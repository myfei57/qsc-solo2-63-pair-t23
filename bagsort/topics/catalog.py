"""Canonical, versioned topic names published by the plant.

The first incarnation of every topic is ``...v1``. When a topic is renamed or
its payload changes, introduce ``v2`` through the registry instead of editing
the string here: old subscribers keep receiving v1 facts until its sunset
deadline, and the governance log records the whole evolution.
"""

from __future__ import annotations

BAG_CHECKED_IN = "bag.checked_in.v1"
BAG_SORTED = "bag.sorted.v1"
BAG_DIVERTED = "bag.diverted.v1"
BAG_RECHECK_ENQUEUED = "bag.recheck_enqueued.v1"
BAG_REMAPPED = "bag.remapped.v1"
FLIGHT_CLOSED = "flight.closed.v1"
CHUTE_ASSIGNED = "chute.assigned.v1"
BELT_SWITCHED = "belt.switched.v1"

# Every fact the plant publishes, seeded into the topic registry at build.
CANONICAL_TOPICS: tuple[str, ...] = (
    BAG_CHECKED_IN,
    BAG_SORTED,
    BAG_DIVERTED,
    BAG_RECHECK_ENQUEUED,
    BAG_REMAPPED,
    FLIGHT_CLOSED,
    CHUTE_ASSIGNED,
    BELT_SWITCHED,
)

# Audit subscriber topic -> (audit kind, handling stage).
AUDIT_STAGE: dict[str, tuple[str, str]] = {
    BAG_CHECKED_IN: ("checkin", "checkin"),
    BAG_SORTED: ("sort", "sorter"),
    BAG_DIVERTED: ("diversion", "sorter"),
    BAG_RECHECK_ENQUEUED: ("recheck", "recheck"),
    FLIGHT_CLOSED: ("flight_close", "audit"),
    CHUTE_ASSIGNED: ("chute_assign", "chute"),
    BELT_SWITCHED: ("belt_switch", "belt"),
    BAG_REMAPPED: ("remap", "checkin"),
}
