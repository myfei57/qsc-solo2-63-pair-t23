"""Cross check topic registration, subscriptions and fact counters.

The registry says which topics should carry traffic, the bus says who is
actually subscribed and how many facts each family accepted, and telemetry
says how many facts were counted. A family is only consistent when the three
views agree on the number of business facts; warnings surface subscribers
hanging on deprecated topics past their window or counters that disagree.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from bagsort.console.telemetry import Telemetry
from bagsort.events.bus import EventBus
from bagsort.topics.registry import DEPRECATED, RETIRED, TopicRegistry


@dataclass
class FamilyView:
    family: str
    current_topic: str
    status: str
    subscribers: dict[str, int]
    facts_published: int
    facts_counted: int
    consistent: bool
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "current_topic": self.current_topic,
            "status": self.status,
            "subscribers": dict(sorted(self.subscribers.items())),
            "facts_published": self.facts_published,
            "facts_counted": self.facts_counted,
            "consistent": self.consistent,
            "notes": list(self.notes),
        }


@dataclass
class Reconciliation:
    families: list[FamilyView]
    consistent: bool

    def as_dict(self) -> dict[str, Any]:
        return {
            "consistent": self.consistent,
            "facts_total": sum(view.facts_published for view in self.families),
            "counted_total": sum(view.facts_counted for view in self.families),
            "families": [view.as_dict() for view in self.families],
        }


def reconcile(
    registry: TopicRegistry,
    bus: EventBus,
    telemetry: Telemetry,
    *,
    now_iso: str = "",
) -> Reconciliation:
    """Build one view per registered family and flag any disagreement."""

    counted = telemetry.fact_counts()
    published = bus.fact_counts()
    views: list[FamilyView] = []
    for family in registry.families():
        rows = registry.family_rows(family)
        current = rows[-1]
        subscribers = {
            str(row["topic"]): bus.subscribers(str(row["topic"]))
            for row in rows
            if bus.subscribers(str(row["topic"]))
        }
        facts_published = published.get(family, 0)
        facts_counted = counted.get(str(current["topic"]), 0)
        notes: list[str] = []
        for row in rows:
            topic = str(row["topic"])
            status = str(row["status"])
            subs = bus.subscribers(topic)
            if status == DEPRECATED and subs:
                deadline = str(row.get("sunset_at", ""))
                overdue = bool(now_iso and deadline and deadline <= now_iso)
                notes.append(
                    f"{subs} subscriber(s) still on deprecated {topic} "
                    f"(sunset {deadline or 'unset'}"
                    + (", deadline passed" if overdue else "")
                    + ")"
                )
            if status == RETIRED and subs:
                notes.append(f"{subs} stale subscription(s) on retired {topic}")
        consistent = facts_published == facts_counted
        if not consistent:
            notes.append(
                f"fact count mismatch: bus saw {facts_published}, "
                f"telemetry counted {facts_counted}"
            )
        views.append(
            FamilyView(
                family=family,
                current_topic=str(current["topic"]),
                status=str(current["status"]),
                subscribers=subscribers,
                facts_published=facts_published,
                facts_counted=facts_counted,
                consistent=consistent,
                notes=notes,
            )
        )
    return Reconciliation(families=views, consistent=all(view.consistent for view in views))
