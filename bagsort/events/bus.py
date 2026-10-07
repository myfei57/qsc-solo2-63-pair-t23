"""Synchronous in-process event bus with versioned topic governance.

Subsystems publish *business facts*. Without a :class:`TopicRegistry` the bus
keeps its original flat behaviour, so standalone use stays simple. With a
registry attached via :meth:`EventBus.attach_registry`, publishes become
governed:

* the published topic must be registered and not retired;
* old and new subscribers both receive a fact during the transition window,
  either by *dual* emission (both topic names are delivered) or *forwarding*
  (old topic traffic is redirected to the current topic name);
* every delivery carries the same ``fact_id`` and one fact reaches any given
  handler at most once, so republishing the same fact, or a handler sitting on
  both names of a dual family, can never be double counted;
* wildcard subscribers see the canonical (current) variant exactly once.

Delivery stays deterministic: handlers run in registration order, nothing is
queued across threads.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import count
from typing import Callable

from bagsort.errors import ConflictError
from bagsort.topics.registry import DUAL, FORWARD, RETIRED, TopicRegistry


@dataclass(frozen=True)
class Event:
    """One variant of one business fact."""

    topic: str
    payload: dict[str, object] = field(default_factory=dict)
    fact_id: str = ""
    family: str = ""
    version: int = 0
    canonical: bool = True

    def as_dict(self) -> dict[str, object]:
        return {
            "topic": self.topic,
            "payload": dict(self.payload),
            "fact_id": self.fact_id,
            "family": self.family,
            "version": self.version,
            "canonical": self.canonical,
        }


Subscriber = Callable[[Event], None]


class EventBus:
    """Topic based publish/subscribe with a bounded history for inspection."""

    def __init__(self, history_limit: int = 256) -> None:
        self._handlers: dict[str, list[Subscriber]] = defaultdict(list)
        self._history: list[Event] = []
        self._limit = history_limit
        self._facts = 0
        self._deliveries = 0
        self._registry: TopicRegistry | None = None
        # fact ids already accepted per family, so republish is a no-op
        self._seen: dict[str, set[str]] = defaultdict(set)
        # accepted facts per family; unbounded counter for reconciliation
        self._fact_counts: dict[str, int] = defaultdict(int)
        # (handler identity, fact id) pairs already delivered, so one handler
        # listening on both names of a dual family is still called once
        self._delivered_to: set[tuple[object, str]] = set()
        # family-wide governed handlers (e.g. audit): attached once, applied to
        # every concrete version of the family including ones introduced later
        self._family_handlers: dict[str, list[Subscriber]] = defaultdict(list)
        self._auto = count(1)

    # -- governance --------------------------------------------------------

    def attach_registry(self, registry: TopicRegistry) -> None:
        self._registry = registry

    @property
    def registry(self) -> TopicRegistry | None:
        return self._registry

    def facts(self) -> int:
        """Distinct business facts accepted, regardless of variant fan-out."""

        return self._facts

    def fact_counts(self) -> dict[str, int]:
        """Accepted business facts per family (governed) or topic (ungoverned)."""

        return dict(sorted(self._fact_counts.items()))

    # -- subscriptions -----------------------------------------------------

    def subscribe(self, topic: str, handler: Subscriber) -> None:
        if self._registry is not None and topic != "*":
            row = self._registry.topic(topic)
            if row["status"] == RETIRED:
                raise ConflictError(
                    f"cannot subscribe to {topic!r}: the topic was retired at "
                    f"{row['retired_at']}; migrate to {row['replaced_by']!r}"
                )
        self._handlers[topic].append(handler)

    def subscribe_family(self, family: str, handler: Subscriber) -> None:
        """Governed subscription to every version of a family, present or future.

        Versions introduced after this call still reach the handler, so system
        projections such as the audit trail never miss a newly registered topic.
        """

        self._family_handlers[family].append(handler)

    def subscribers(self, topic: str) -> int:
        return len(self._handlers.get(topic, ()))

    # -- variant fan-out ---------------------------------------------------

    def _variants_for(self, topic: str) -> list[tuple[str, bool, tuple[str, ...]]]:
        """Concrete delivery targets for a publish.

        Each entry is ``(delivery_topic, is_canonical, alias_sources)``. Under
        forwarding, traffic published on an old name is delivered once on the
        current name but also fans in the handlers still subscribed to the old
        name; the per-handler fact dedup keeps the delivery single even when a
        handler sits on both.
        """

        registry = self._registry
        assert registry is not None
        row = registry.topic(topic)
        if row["status"] == RETIRED:
            raise ConflictError(
                f"topic {topic!r} was retired at {row['retired_at']}; "
                f"publish {row['replaced_by']!r} instead"
            )
        family = str(row["family"])
        current = registry.family(family)
        current_name = str(current["topic"])
        mode = family_mode(registry, family)

        if registry.is_current(topic):
            # Current name: dual mode also reaches live old names still in
            # window; forwarding only ever goes old -> new, so it never fans
            # back to deprecated names.
            if mode != DUAL:
                return [(topic, True, ())]
            pairs: list[tuple[str, bool, tuple[str, ...]]] = []
            for version_row in registry.family_rows(family):
                if version_row["status"] in {"active", "deprecated"}:
                    name = str(version_row["topic"])
                    pairs.append((name, name == current_name, ()))
            return pairs or [(topic, True, ())]

        # Publish landed on an old, deprecated name during its window.
        if mode == FORWARD:
            return [(current_name, True, (topic,))]
        return [(topic, False, ()), (current_name, True, ())]

    # -- publishing --------------------------------------------------------

    def publish(self, topic: str, fact_id: str = "", **payload: object) -> Event:
        """Publish one business fact, fanning out per the family policy."""

        if self._registry is None:
            return self._legacy_publish(topic, payload)

        row = self._registry.resolve(topic)
        family = str(row["family"])
        fid = fact_id or f"{family}#auto-{next(self._auto)}"
        if fid in self._seen[family]:
            # Idempotent replay: the fact was already accepted and delivered.
            return self._canonical_of(family, fid, payload)
        self._seen[family].add(fid)
        self._facts += 1
        self._fact_counts[family] += 1

        events: list[Event] = []
        for variant_topic, canonical, aliases in self._variants_for(topic):
            variant_row = self._registry.topic(variant_topic)
            event = Event(
                topic=variant_topic,
                payload=dict(payload),
                fact_id=fid,
                family=family,
                version=int(variant_row["version"]),
                canonical=canonical,
            )
            self._fan_out(event, aliases)
            events.append(event)

        canonical_event = next(event for event in events if event.canonical)
        # Wildcard subscribers observe the canonical variant exactly once.
        for handler in tuple(self._handlers.get("*", ())):
            self._invoke(handler, canonical_event)
        self._remember(canonical_event)
        return canonical_event

    def publish_fact(self, topic: str, fact_id: str = "", **payload: object) -> Event:
        """Explicit alias for callers that name the idempotency key themselves."""

        return self.publish(topic, fact_id=fact_id, **payload)

    def _fan_out(self, event: Event, aliases: tuple[str, ...] = ()) -> None:
        """Deliver to topic handlers, forwarded aliases and family handlers."""

        names = (event.topic, *aliases)
        seen_handlers: set[object] = set()
        for name in names:
            for handler in tuple(self._handlers.get(name, ())):
                marker = id(handler)
                if marker in seen_handlers:
                    continue
                seen_handlers.add(marker)
                self._invoke(handler, event)
        if event.family:
            for handler in tuple(self._family_handlers.get(event.family, ())):
                self._invoke(handler, event)

    def _invoke(self, handler: Subscriber, event: Event) -> None:
        key = (id(handler), event.fact_id)
        if key in self._delivered_to:
            return
        self._delivered_to.add(key)
        handler(event)
        self._deliveries += 1

    def _legacy_publish(self, topic: str, payload: dict[str, object]) -> Event:
        event = Event(topic=topic, payload=dict(payload))
        for handler in tuple(self._handlers.get(topic, ())):
            handler(event)
        for handler in tuple(self._handlers.get("*", ())):
            handler(event)
        self._remember(event)
        self._facts += 1
        self._fact_counts[topic] += 1
        self._deliveries += 1
        return event

    def _remember(self, event: Event) -> None:
        self._history.append(event)
        if len(self._history) > self._limit:
            self._history = self._history[-self._limit :]

    def _canonical_of(
        self, family: str, fact_id: str, payload: dict[str, object]
    ) -> Event:
        for event in reversed(self._history):
            if event.fact_id == fact_id:
                return event
        # History was trimmed; return a synthetic canonical view, undelivered.
        current = self._registry.family(family) if self._registry else None
        return Event(
            topic=str(current["topic"]) if current else family,
            payload=dict(payload),
            fact_id=fact_id,
            family=family,
            version=int(current["version"]) if current else 0,
        )

    # -- introspection -----------------------------------------------------

    def delivered(self) -> int:
        return self._deliveries

    def history(self, topic: str | None = None) -> list[Event]:
        if topic is None:
            return list(self._history)
        return [event for event in self._history if event.topic == topic]

    def topics(self) -> list[str]:
        return sorted(set(self._handlers) - {"*"})


def family_mode(registry: TopicRegistry, family: str) -> str:
    """Compatibility mode declared by the family's in-window old version."""

    for version_row in registry.family_rows(family):
        if version_row["status"] == "deprecated" and version_row.get("compatibility"):
            return str(version_row["compatibility"])
    return DUAL
