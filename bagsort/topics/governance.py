"""Hash linked ledger of topic governance decisions.

Every registration, deprecation and retirement appends one
:class:`GovernanceRecord`. Each record hashes its predecessor, so the evolution
history is append-only: deleting or rewriting a step breaks every digest after
it. Records are journaled through the store, which makes the whole history
survive restart and replay.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from typing import Any, TYPE_CHECKING

from bagsort.clock import Clock
from bagsort.store.store import Store

if TYPE_CHECKING:  # pragma: no cover - typing only
    from bagsort.store.store import Session

GENESIS = "0" * 64


@dataclass(frozen=True)
class GovernanceRecord:
    """One immutable step in a topic family's evolution."""

    sequence: int
    action: str
    family: str
    topic: str
    version: int
    detail: dict[str, Any]
    at: str
    prev_digest: str
    digest: str = ""

    def as_dict(self) -> dict[str, Any]:
        return {
            "sequence": self.sequence,
            "action": self.action,
            "family": self.family,
            "topic": self.topic,
            "version": self.version,
            "detail": dict(self.detail),
            "at": self.at,
            "prev_digest": self.prev_digest,
            "digest": self.digest,
        }

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "GovernanceRecord":
        return cls(
            sequence=int(raw["sequence"]),
            action=str(raw["action"]),
            family=str(raw["family"]),
            topic=str(raw["topic"]),
            version=int(raw["version"]),
            detail=dict(raw.get("detail", {})),
            at=str(raw.get("at", "")),
            prev_digest=str(raw.get("prev_digest", GENESIS)),
            digest=str(raw.get("digest", "")),
        )


def digest_of(record: GovernanceRecord) -> str:
    """Digest over the fields that must never change after the fact."""

    material = json.dumps(
        [
            record.sequence,
            record.action,
            record.family,
            record.topic,
            record.version,
            record.detail,
            record.prev_digest,
            record.at,
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


class GovernanceLog:
    """Append-only history of topic registry mutations."""

    def __init__(self, store: Store, clock: Clock) -> None:
        self._store = store
        self._clock = clock

    def entries(self) -> list[GovernanceRecord]:
        return [GovernanceRecord.from_dict(row) for row in self._store.state().topic_governance]

    def count(self) -> int:
        return len(self._store.state().topic_governance)

    def last(self) -> GovernanceRecord | None:
        rows = self._store.state().topic_governance
        return GovernanceRecord.from_dict(rows[-1]) if rows else None

    def last_digest(self) -> str:
        last = self.last()
        return last.digest if last is not None else GENESIS

    def for_family(self, family: str) -> list[GovernanceRecord]:
        return [row for row in self.entries() if row.family == family]

    def append(
        self,
        session: "Session",
        action: str,
        family: str,
        topic: str,
        version: int,
        detail: dict[str, Any] | None = None,
        at: str | None = None,
    ) -> GovernanceRecord:
        """Build the linked record and stage it in the open write session."""

        pending = GovernanceRecord(
            sequence=self.count() + 1,
            action=action,
            family=family,
            topic=topic,
            version=version,
            detail=dict(detail or {}),
            at=at or self._clock.now_iso(),
            prev_digest=self.last_digest(),
        )
        record = replace(pending, digest=digest_of(pending))
        session.record(
            "topic.governance.append",
            {"entry": record.as_dict()},
            record.at,
        )
        return record

    def build(
        self,
        action: str,
        family: str,
        topic: str,
        version: int,
        detail: dict[str, Any] | None = None,
        at: str | None = None,
    ) -> GovernanceRecord:
        """Build a linked record without staging it in a session (bootstrap)."""

        pending = GovernanceRecord(
            sequence=self.count() + 1,
            action=action,
            family=family,
            topic=topic,
            version=version,
            detail=dict(detail or {}),
            at=at or self._clock.now_iso(),
            prev_digest=self.last_digest(),
        )
        return replace(pending, digest=digest_of(pending))

    def verify(self) -> bool:
        """Recompute the chain and confirm every link still matches."""

        previous = GENESIS
        for row in self.entries():
            if row.prev_digest != previous:
                return False
            if digest_of(row) != row.digest:
                return False
            previous = row.digest
        return True
