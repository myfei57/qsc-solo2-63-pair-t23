"""Versioned topic registry: lifecycle, forwarding plan and reconciliation.

A *family* is the business fact type (``bag.sorted``); each rename cuts a new
*version* of the family (``bag.sorted.v2``). Subscribers stay on whatever
version they know; the bus forwards every published fact across the family's
live versions so old and new subscribers each receive it exactly once.

Why forwarding instead of dual-publish: a publisher that emits the same fact
on two topics gives every consumer subscribed to both versions — and every
wildcard projection such as telemetry or the audit chain — two events with no
shared identity, so counts inflate and cannot be reconciled. Forwarding keeps
one publish, one ``event_id`` and one count; the registry is the single place
that knows which versions are linked, so compatibility is managed here rather
than coordinated across every publisher.

Lifecycle: ``active`` -> ``deprecated`` (successor + hard deadline) ->
``retired``. Deprecation opens the transition window: both directions forward,
new subscriptions are still allowed. Once the clock passes ``not_after`` the
old version is treated as retired — publishes and subscribes are rejected with
:class:`StaleError` — and ``retire`` writes the closing tombstone. Every
transition is journaled (so replay rebuilds the registry) and mirrored into
the audit chain, which is what makes the evolution trail reviewable entry by
entry.
"""

from __future__ import annotations

from datetime import datetime, timezone

from bagsort.audit.chain import AuditChain
from bagsort.clock import Clock
from bagsort.errors import ConflictError, NotFoundError, StaleError, ValidationError
from bagsort.model.entities import TopicTransition, TopicVersion
from bagsort.model.enums import TopicState
from bagsort.store.store import Store

DEADLINE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_JOURNAL_KIND = {
    "registered": "topic.register",
    "deprecated": "topic.deprecate",
    "retired": "topic.retire",
}


def _parse_deadline(value: str) -> datetime:
    try:
        return datetime.strptime(value, DEADLINE_FORMAT).replace(tzinfo=timezone.utc)
    except ValueError as exc:
        raise ValidationError(f"deadline {value!r} must look like YYYY-MM-DDTHH:MM:SSZ") from exc


class TopicRegistry:
    """The catalog of topic versions, their links and their deadlines."""

    def __init__(
        self,
        store: Store,
        clock: Clock,
        audit: AuditChain,
        catalog: tuple[str, ...] = (),
    ) -> None:
        self._store = store
        self._clock = clock
        self._audit = audit
        # The built-in catalog is configuration, not state: it is re-seeded in
        # memory on every start and never journaled. Runtime transitions are.
        self._catalog = {
            name: TopicVersion(topic=name, family=name, version=1) for name in catalog
        }

    # -- read side ---------------------------------------------------------

    def _topics(self) -> dict[str, TopicVersion]:
        """Catalog defaults overlaid with the journaled transitions."""

        return {**self._catalog, **self._store.state().topics}

    def known(self, topic: str) -> bool:
        return topic in self._topics()

    def get(self, topic: str) -> TopicVersion:
        info = self._topics().get(topic)
        if info is None:
            raise NotFoundError(f"topic {topic!r} is not registered")
        return info

    def family_of(self, topic: str) -> str:
        """The family a topic belongs to; unknown names map to themselves."""

        info = self._topics().get(topic)
        return info.family if info else topic

    def families(self) -> dict[str, list[str]]:
        """Every family mapped to its version topics, oldest first."""

        grouped: dict[str, list[TopicVersion]] = {}
        for info in self._topics().values():
            grouped.setdefault(info.family, []).append(info)
        return {
            family: [info.topic for info in sorted(versions, key=lambda item: item.version)]
            for family, versions in sorted(grouped.items())
        }

    def _expired(self, info: TopicVersion) -> bool:
        if info.state != TopicState.DEPRECATED.value or not info.not_after:
            return False
        return self._clock.now() > _parse_deadline(info.not_after)

    def _effective(self, info: TopicVersion) -> str:
        if self._expired(info):
            return TopicState.RETIRED.value
        return info.state

    def describe(self, topic: str) -> dict[str, object]:
        info = self.get(topic)
        payload: dict[str, object] = info.as_dict()
        payload["effective"] = self._effective(info)
        payload["expired"] = self._expired(info)
        return payload

    def as_list(self) -> list[dict[str, object]]:
        return [self.describe(topic) for topic in sorted(self._topics())]

    def linked(self, topic: str) -> list[str]:
        """Live versions of the topic's family, published topic first.

        This is the forwarding plan: one publish is delivered to the
        subscribers of every version in this list, rendered at their version.
        """

        info = self.get(topic)
        versions = [
            item
            for item in self._topics().values()
            if item.family == info.family and self._effective(item) != TopicState.RETIRED.value
        ]
        ordered = [item.topic for item in sorted(versions, key=lambda item: item.version)]
        return [topic] + [name for name in ordered if name != topic]

    def evolution(self) -> list[TopicTransition]:
        """The journaled lifecycle changes, oldest first."""

        return list(self._store.state().topic_evolution)

    # -- validation used by the bus -----------------------------------------

    def validate_publish(self, topic: str) -> None:
        info = self.get(topic)
        effective = self._effective(info)
        if effective == TopicState.RETIRED.value:
            if info.state == TopicState.DEPRECATED.value:
                raise StaleError(
                    f"topic {topic!r} passed its deprecation deadline {info.not_after}"
                )
            raise StaleError(f"topic {topic!r} is retired")

    def validate_subscribe(self, topic: str) -> None:
        self.validate_publish(topic)

    # -- transitions (journaled and audited) ---------------------------------

    def register(self, topic: str) -> TopicVersion:
        name = (topic or "").strip()
        if not name:
            raise ValidationError("topic must not be empty")
        if self.known(name):
            raise ConflictError(f"topic {name!r} is already registered")
        info = TopicVersion(
            topic=name,
            family=name,
            version=1,
            registered_at=self._clock.now_iso(),
        )
        self._commit(info, "registered")
        return info

    def register_version(self, family: str) -> TopicVersion:
        versions = self.families().get(family)
        if versions is None:
            raise NotFoundError(f"topic family {family!r} is not registered")
        number = max(self.get(name).version for name in versions) + 1
        name = f"{family}.v{number}"
        if self.known(name):
            raise ConflictError(f"topic {name!r} is already registered")
        info = TopicVersion(
            topic=name,
            family=family,
            version=number,
            registered_at=self._clock.now_iso(),
        )
        self._commit(info, "registered")
        return info

    def deprecate(self, topic: str, successor: str = "", not_after: str = "") -> TopicVersion:
        info = self.get(topic)
        if info.state == TopicState.DEPRECATED.value:
            raise ConflictError(f"topic {topic!r} is already deprecated")
        if info.state == TopicState.RETIRED.value:
            raise ConflictError(f"topic {topic!r} is retired")
        deadline = (not_after or "").strip()
        if not deadline:
            raise ValidationError("a deprecation needs a deadline (not_after)")
        if _parse_deadline(deadline) <= self._clock.now():
            raise ValidationError("deprecation deadline must be in the future")
        heir = (successor or "").strip()
        if heir:
            target = self.get(heir)
            if target.topic == info.topic:
                raise ValidationError("successor must differ from the deprecated topic")
            if target.family != info.family:
                raise ValidationError(f"successor {heir!r} belongs to another family")
            if self._effective(target) != TopicState.ACTIVE.value:
                raise ValidationError(f"successor {heir!r} is not active")
        updated = TopicVersion(
            topic=info.topic,
            family=info.family,
            version=info.version,
            state=TopicState.DEPRECATED.value,
            successor=heir,
            not_after=deadline,
            registered_at=info.registered_at,
        )
        self._commit(updated, "deprecated")
        return updated

    def retire(self, topic: str) -> TopicVersion:
        info = self.get(topic)
        if info.state == TopicState.RETIRED.value:
            raise ConflictError(f"topic {topic!r} is already retired")
        updated = TopicVersion(
            topic=info.topic,
            family=info.family,
            version=info.version,
            state=TopicState.RETIRED.value,
            successor=info.successor,
            not_after=info.not_after,
            registered_at=info.registered_at,
            retired_at=self._clock.now_iso(),
        )
        self._commit(updated, "retired")
        return updated

    def _commit(self, info: TopicVersion, action: str) -> None:
        at = self._clock.now_iso()
        transition = TopicTransition(
            action=action,
            topic=info.topic,
            family=info.family,
            version=info.version,
            successor=info.successor,
            not_after=info.not_after,
            at=at,
        )
        with self._store.session() as session:
            session.state.topics[info.topic] = info
            session.state.topic_evolution.append(transition)
            session.record(
                _JOURNAL_KIND[action],
                {"info": info.as_dict(), "transition": transition.as_dict()},
                at,
            )
        self._audit.record(
            f"topic_{action}",
            info.topic,
            "audit",
            {
                "family": info.family,
                "version": info.version,
                "successor": info.successor,
                "not_after": info.not_after,
            },
            at=at,
        )

    # -- cross-checks ---------------------------------------------------------

    def reconcile(self, bus, telemetry, audit, kinds: dict[str, str]) -> dict[str, object]:
        """Cross-check the registry against subscriptions, counters and the trail.

        ``kinds`` maps a family to the audit kind its events produce. Every
        check compares two independently maintained views of the same facts;
        any disagreement lands in the report and flips ``ok`` to false.
        """

        topics = self._topics()
        subscribed = bus.topics()
        unknown_subscriptions = sorted(name for name in subscribed if name not in topics)
        dead_subscriptions = sorted(
            name
            for name in subscribed
            if name in topics and self._effective(topics[name]) == TopicState.RETIRED.value
        )
        counters = telemetry.snapshot()
        unknown_counters = sorted(name for name in counters if name not in topics)
        audit_kinds = audit.by_kind()
        counter_mismatches = []
        for family, names in self.families().items():
            kind = kinds.get(family)
            if kind is None:
                continue
            events = sum(counters.get(name, 0) for name in names)
            audited = audit_kinds.get(kind, 0)
            if events != audited:
                counter_mismatches.append(
                    {
                        "family": family,
                        "kind": kind,
                        "events": events,
                        "audited": audited,
                    }
                )
        audited_transitions = sum(
            count for kind, count in audit_kinds.items() if kind.startswith("topic_")
        )
        evolution = self.evolution()
        evolution_mismatch = (
            {"journal": len(evolution), "audit": audited_transitions}
            if len(evolution) != audited_transitions
            else None
        )
        expired = sorted(info.topic for info in topics.values() if self._expired(info))
        ok = not (
            unknown_subscriptions
            or dead_subscriptions
            or unknown_counters
            or counter_mismatches
            or evolution_mismatch
            or expired
        )
        return {
            "ok": ok,
            "unknown_subscriptions": unknown_subscriptions,
            "dead_subscriptions": dead_subscriptions,
            "unknown_counters": unknown_counters,
            "counter_mismatches": counter_mismatches,
            "evolution_mismatch": evolution_mismatch,
            "expired": expired,
            "topics": len(topics),
            "subscriptions": len(subscribed),
        }
