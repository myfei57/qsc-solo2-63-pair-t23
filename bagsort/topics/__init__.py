"""Versioned topic governance.

A *topic* has a concrete name (``bag.sorted.v1``), an integer version and a
*family* (``bag.sorted``) that ties renamed versions of the same business fact
together. The :class:`TopicRegistry` is the single source of truth for which
topics exist, who replaces whom, and when a deprecated topic stops accepting
traffic. Every change is journaled and hashed into the governance chain, so the
evolution history can be replayed and tamper checked.

Import :mod:`bagsort.topics.reconcile` and :mod:`bagsort.topics.governance`
directly; they are kept out of the package initialiser to avoid an import cycle
with the event bus.
"""

from __future__ import annotations

from .registry import (
    ACTIVE,
    DEPRECATED,
    DUAL,
    FORWARD,
    RETIRED,
    TopicRegistry,
)

__all__ = [
    "ACTIVE",
    "DEPRECATED",
    "RETIRED",
    "DUAL",
    "FORWARD",
    "TopicRegistry",
]
