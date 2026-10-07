"""Synchronous in-process event bus with versioned topic forwarding.

Subsystems publish what happened; projections such as the audit trail and the
telemetry counters subscribe. Delivery is deterministic because subscribers run
in registration order and nothing is queued across threads.

When a :class:`~bagsort.events.registry.TopicRegistry` is attached, topics are
versioned families. Publishing to any live version of a family reaches the
subscribers of every other live version: the bus *forwards* the event across
the family instead of asking publishers to dual-publish. Every forwarded view
carries the same ``event_id`` — the business fact's identity — and a handler
is invoked at most once per publish, so a consumer subscribed to two versions
of one family is not double-counted. Forwarding is one hop: views are handed
to subscribers directly and never re-published onto the bus, which is why no
loop or duplicate delivery can form. One publish is one business fact:
``delivered`` and the telemetry counters move exactly once.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable

from bagsort.ids import IdFactory


@dataclass(frozen=True)
class Event:
    """One domain event."""

    topic: str
    payload: dict[str, object] = field(default_factory=dict)
    event_id: str = ""

    def view(self, topic: str) -> "Event":
        """The same business fact addressed to another version of the family."""

        return Event(topic=topic, payload=self.payload, event_id=self.event_id)

    def as_dict(self) -> dict[str, object]:
        return {
            "topic": self.topic,
            "payload": dict(self.payload),
            "event_id": self.event_id,
        }


Subscriber = Callable[[Event], None]


class EventBus:
    """Topic based publish/subscribe with a bounded history for inspection."""

    def __init__(self, history_limit: int = 256, registry=None) -> None:
        self._handlers: dict[str, list[Subscriber]] = defaultdict(list)
        self._history: list[Event] = []
        self._limit = history_limit
        self._delivered = 0
        self._registry = registry
        self._ids = IdFactory("evt")

    def subscribe(self, topic: str, handler: Subscriber) -> None:
        if self._registry is not None and topic != "*":
            self._registry.validate_subscribe(topic)
        self._handlers[topic].append(handler)

    def unsubscribe_all(self, topic: str) -> int:
        """Drop every handler on ``topic``; returns how many were removed."""

        return len(self._handlers.pop(topic, ()))

    def subscribers(self, topic: str) -> int:
        return len(self._handlers.get(topic, ()))

    def publish(self, topic: str, **payload: object) -> Event:
        if self._registry is not None:
            self._registry.validate_publish(topic)
        event = Event(topic=topic, payload=dict(payload), event_id=self._ids.next())
        topics = [topic]
        if self._registry is not None:
            topics = self._registry.linked(topic)
        seen: set[int] = set()
        for name in topics:
            view = event if name == topic else event.view(name)
            for handler in tuple(self._handlers.get(name, ())):
                marker = id(handler)
                if marker in seen:
                    continue
                seen.add(marker)
                handler(view)
        for handler in tuple(self._handlers.get("*", ())):
            handler(event)
        self._history.append(event)
        if len(self._history) > self._limit:
            self._history = self._history[-self._limit :]
        self._delivered += 1
        return event

    def delivered(self) -> int:
        return self._delivered

    def history(self, topic: str | None = None) -> list[Event]:
        if topic is None:
            return list(self._history)
        return [event for event in self._history if event.topic == topic]

    def topics(self) -> list[str]:
        return sorted(set(self._handlers) - {"*"})
