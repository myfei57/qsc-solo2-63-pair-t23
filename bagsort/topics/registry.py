"""Versioned topic registry: registration, lineage, deprecation deadlines.

The registry owns three things:

* ``register_topic`` / ``introduce_version`` add concrete topic names and record
  which family and version they belong to;
* ``deprecate`` opens a *transition window* with an explicit sunset deadline and
  a family compatibility mode (``dual`` publishes both versions, ``forward``
  redirects old topic traffic to the current one);
* ``retire`` and the clock driven ``sweep`` end the window, after which the old
  topic rejects publishes and new subscriptions.

Every concrete version is stored separately, so old and new subscribers can be
served at the same time. Every mutation writes the folded topic rows and a
linked :class:`~bagsort.topics.governance.GovernanceRecord` in one store
session, so the registry table and the evolution history can never disagree.
"""

from __future__ import annotations

import re
from typing import Any

from bagsort.clock import Clock
from bagsort.errors import ConflictError, NotFoundError, ValidationError
from bagsort.store.store import Store
from .governance import GovernanceLog, GovernanceRecord

# lifecycle states
ACTIVE = "active"
DEPRECATED = "deprecated"
RETIRED = "retired"

# transition compatibility modes
DUAL = "dual"
FORWARD = "forward"

_VERSION_RE = re.compile(r"^(?P<family>.+)\.v(?P<version>[1-9][0-9]*)$")
_MODES = {DUAL, FORWARD}


class TopicRegistry:
    """Authoritative table of versioned topics and their lifecycle."""

    def __init__(self, store: Store, clock: Clock) -> None:
        self._store = store
        self._clock = clock
        self._log = GovernanceLog(store, clock)

    # -- lookups -----------------------------------------------------------

    def _rows(self) -> dict[str, dict[str, Any]]:
        return self._store.state().topics

    def list(self) -> list[dict[str, Any]]:
        return [dict(row) for _, row in sorted(self._rows().items())]

    def topic(self, name: str) -> dict[str, Any]:
        row = self._rows().get(name)
        if row is None:
            family, _ = _parse(name)
            raise NotFoundError(_missing_hint(name, family, self._rows()))
        return dict(row)

    def family_rows(self, family: str) -> list[dict[str, Any]]:
        rows = [dict(row) for row in self._rows().values() if row["family"] == family]
        if not rows:
            raise NotFoundError(f"unknown topic family {family!r}")
        return sorted(rows, key=lambda row: int(row["version"]))

    def family(self, family: str) -> dict[str, Any]:
        """The highest version row of a family."""

        return self.family_rows(family)[-1]

    def resolve(self, name: str) -> dict[str, Any]:
        """Current row of the family a concrete topic name belongs to."""

        return self.family(_family_of(name))

    def is_current(self, name: str) -> bool:
        parsed = _VERSION_RE.match(name)
        if parsed is None:
            return False
        try:
            return self.family(parsed.group("family"))["topic"] == name
        except NotFoundError:
            return False

    def status_of(self, name: str) -> str:
        return self.topic(name)["status"]

    def current(self) -> list[dict[str, Any]]:
        return [self.family(family) for family in self.families()]

    def by_status(self, status: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self._rows().values() if row["status"] == status]

    def families(self) -> list[str]:
        return sorted({row["family"] for row in self._rows().values()})

    def lineage(self, family: str) -> list[dict[str, Any]]:
        """Every version of a family, oldest first."""

        return self.family_rows(family)

    # -- governance --------------------------------------------------------

    def register_topic(
        self,
        name: str,
        *,
        replaces: str = "",
        at: str | None = None,
    ) -> dict[str, Any]:
        """Register the first or next version of a family.

        ``replaces`` names the previous concrete topic. When given, the previous
        version must still be active; the caller then opens its transition
        window with :meth:`deprecate`.
        """

        family, version = _parse(name)
        if name in self._rows():
            raise ConflictError(f"topic {name!r} is already registered")
        old: dict[str, Any] | None = None
        if replaces:
            old = self.topic(replaces)
            if old["family"] != family:
                raise ValidationError(
                    f"{replaces!r} belongs to family {old['family']!r}, not {family!r}"
                )
            if version <= int(old["version"]):
                raise ConflictError(
                    f"new version {version} must be greater than {old['version']}"
                )
            if old["status"] != ACTIVE:
                raise ConflictError(
                    f"only an active topic can be replaced; {replaces!r} is {old['status']}"
                )
        else:
            same_family = [
                row for row in self._rows().values() if row["family"] == family
            ]
            if same_family:
                current = max(same_family, key=lambda row: int(row["version"]))
                raise ConflictError(
                    f"family {family!r} already has {current['topic']!r}; "
                    f"pass replaces= to introduce a new version"
                )
        now_iso = at or self._clock.now_iso()
        row = {
            "family": family,
            "topic": name,
            "version": version,
            "status": ACTIVE,
            "introduced_at": now_iso,
            "deprecated_at": "",
            "sunset_at": "",
            "retired_at": "",
            "compatibility": "",
            "replaces": replaces,
            "replaced_by": "",
        }
        with self._store.session() as session:
            session.record("topic.put", {"family": family, "topic": row}, now_iso)
            if old is not None:
                updated = dict(old)
                updated["replaced_by"] = name
                session.record(
                    "topic.put", {"family": family, "topic": updated}, now_iso
                )
            self._log.append(
                session,
                "register",
                family,
                name,
                version,
                {"replaces": replaces},
                at=now_iso,
            )
        return self.family(family)

    def introduce_version(
        self,
        name: str,
        previous: str,
        *,
        compatibility: str = DUAL,
        sunset_at: str = "",
        at: str | None = None,
    ) -> dict[str, Any]:
        """Register ``name`` and immediately open ``previous``'s window.

        This is the operator facing one-shot upgrade: a new version plus an old
        version with an explicit deadline and a chosen compatibility mode.
        """

        if compatibility not in _MODES:
            raise ValidationError(f"compatibility must be one of {sorted(_MODES)}")
        self.register_topic(name, replaces=previous, at=at)
        if sunset_at:
            self.deprecate(
                previous,
                sunset_at=sunset_at,
                compatibility=compatibility,
                at=at,
            )
        return self.resolve(name)

    def deprecate(
        self,
        name: str,
        *,
        sunset_at: str,
        compatibility: str = DUAL,
        at: str | None = None,
    ) -> dict[str, Any]:
        """Open the transition window with an explicit last-accept deadline."""

        if compatibility not in _MODES:
            raise ValidationError(f"compatibility must be one of {sorted(_MODES)}")
        if not sunset_at:
            raise ValidationError("deprecation requires an explicit sunset_at deadline")
        old = self.topic(name)
        if old["status"] != ACTIVE:
            raise ConflictError(f"only an active topic can be deprecated; {name!r} is not active")
        now_iso = at or self._clock.now_iso()
        if sunset_at <= now_iso:
            raise ValidationError("sunset_at must be in the future")
        if not old["replaced_by"]:
            raise ConflictError(
                f"cannot deprecate {name!r}: no newer version has replaced it yet"
            )
        new_row = self.topic(old["replaced_by"])
        row = dict(old)
        row["status"] = DEPRECATED
        row["deprecated_at"] = now_iso
        row["sunset_at"] = sunset_at
        row["compatibility"] = compatibility
        with self._store.session() as session:
            session.record("topic.put", {"family": row["family"], "topic": row}, now_iso)
            self._log.append(
                session,
                "deprecate",
                str(row["family"]),
                name,
                int(row["version"]),
                {
                    "sunset_at": sunset_at,
                    "compatibility": compatibility,
                    "replaced_by": new_row["topic"],
                },
                at=now_iso,
            )
        return self.resolve(name)

    def retire(self, name: str, *, at: str | None = None) -> dict[str, Any]:
        """End the transition window explicitly; the topic now rejects traffic."""

        old = self.topic(name)
        if old["status"] == RETIRED:
            raise ConflictError(f"{name!r} is already retired")
        if old["status"] != DEPRECATED:
            raise ConflictError(
                f"only a deprecated topic can be retired; {name!r} is still active"
            )
        now_iso = at or self._clock.now_iso()
        return self._retire_row(old, now_iso, "retire")

    def sweep(self, *, at: str | None = None) -> list[str]:
        """Retire every deprecated topic whose sunset deadline has passed."""

        now_iso = at or self._clock.now_iso()
        due = [
            row
            for row in self.by_status(DEPRECATED)
            if row["sunset_at"] and row["sunset_at"] <= now_iso
        ]
        retired: list[str] = []
        for row in due:
            self._retire_row(row, now_iso, "sunset")
            retired.append(str(row["topic"]))
        return retired

    def _retire_row(
        self, old: dict[str, Any], now_iso: str, action: str
    ) -> dict[str, Any]:
        row = dict(old)
        row["status"] = RETIRED
        row["retired_at"] = now_iso
        detail: dict[str, Any] = {"after": action}
        if row["sunset_at"]:
            detail["sunset_at"] = row["sunset_at"]
        with self._store.session() as session:
            session.record("topic.put", {"family": row["family"], "topic": row}, now_iso)
            self._log.append(
                session,
                "retire",
                str(row["family"]),
                str(row["topic"]),
                int(row["version"]),
                detail,
                at=now_iso,
            )
        return self.family(str(row["family"]))

    # -- history -----------------------------------------------------------

    def history(self, family: str | None = None) -> list[GovernanceRecord]:
        return self._log.entries() if family is None else self._log.for_family(family)

    def verify_history(self) -> bool:
        return self._log.verify()

    # -- bootstrap ---------------------------------------------------------

    def bootstrap(self, name: str) -> None:
        """Install a canonical first version on a fresh store, no watermark.

        Used for the built in topic catalog so a new plant carries registered
        topics without consuming a journal commit. Governance records emitted
        here are bootstrap entries too and still chain into later mutations.
        """

        family, version = _parse(name)
        if name in self._rows():
            return
        now_iso = self._clock.now_iso()
        row = {
            "family": family,
            "topic": name,
            "version": version,
            "status": ACTIVE,
            "introduced_at": now_iso,
            "deprecated_at": "",
            "sunset_at": "",
            "retired_at": "",
            "compatibility": "",
            "replaces": "",
            "replaced_by": "",
        }
        record = self._log.build(
            "register", family, name, version, {"replaces": "", "bootstrap": True}, at=now_iso
        )
        self._store.journal.bootstrap(
            "topic.put", {"family": family, "topic": row}, now_iso
        )
        self._store.journal.bootstrap(
            "topic.governance.append", {"entry": record.as_dict()}, now_iso
        )
        self._store.invalidate()


def _parse(name: str) -> tuple[str, int]:
    parsed = _VERSION_RE.match(name.strip())
    if parsed is None:
        raise ValidationError(
            f"topic {name!r} must be versioned as '<family>.v<integer>', e.g. bag.sorted.v2"
        )
    return parsed.group("family"), int(parsed.group("version"))


def _family_of(name: str) -> str:
    family, _ = _parse(name)
    return family


def _missing_hint(name: str, family: str, rows: dict[str, dict[str, Any]]) -> str:
    current = [row for row in rows.values() if row["family"] == family]
    if current:
        newest = max(current, key=lambda row: int(row["version"]))
        return (
            f"unknown topic {name!r}; family {family!r} currently serves "
            f"{newest['topic']!r}"
        )
    return f"unknown topic {name!r}"
