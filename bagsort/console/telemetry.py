"""Counts of published domain facts, kept as a projection of the event bus.

A *business fact* is counted exactly once even when compatibility fan-out
delivers it under more than one topic version: the counter is keyed by the
canonical topic of the fact family, never by the variant name. ``observe`` on a
raw topic name remains for ungoverned buses.
"""

from __future__ import annotations


class Telemetry:
    """How many of each kind of thing the plant did this process lifetime."""

    def __init__(self) -> None:
        self._counters: dict[str, int] = {}
        self._facts: dict[str, int] = {}

    def observe(self, topic: str) -> None:
        self._counters[topic] = self._counters.get(topic, 0) + 1

    def observe_fact(self, canonical_topic: str) -> None:
        self._facts[canonical_topic] = self._facts.get(canonical_topic, 0) + 1

    def total(self) -> int:
        return sum(self._facts.values()) if self._facts else sum(self._counters.values())

    def snapshot(self) -> dict[str, int]:
        return dict(sorted(self._facts.items() if self._facts else self._counters.items()))

    def fact_counts(self) -> dict[str, int]:
        return dict(sorted(self._facts.items()))

    def variant_counts(self) -> dict[str, int]:
        return dict(sorted(self._counters.items()))

    def reset(self) -> None:
        self._counters = {}
        self._facts = {}
