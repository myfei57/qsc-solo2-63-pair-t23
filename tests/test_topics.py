"""Versioned topics: registry, forwarding, transition windows and reconcile."""

from __future__ import annotations

import json

import pytest

from bagsort.app import DOMAIN_TOPICS, Plant, _AuditSubscriber
from bagsort.clock import Clock
from bagsort.errors import ConflictError, NotFoundError, StaleError, ValidationError
from bagsort.store.store import Store
from tests.support import check_in

DEADLINE = "2024-01-01T01:00:00Z"
LATER = "2024-01-01T02:00:00Z"


def call(server, method: str, path: str, payload=None, query=None):
    response = server.call(method, path, payload, query)
    if response.content_type.startswith("application/json"):
        return response.status, json.loads(response.body)
    return response.status, response.body.decode("utf-8")


# ---------------------------------------------------------------------------
# registration
# ---------------------------------------------------------------------------


def test_catalog_topics_are_registered_active(plant):
    topics = {row["topic"]: row for row in plant.topics.as_list()}
    assert set(topics) == set(DOMAIN_TOPICS)
    assert topics["bag.sorted"]["state"] == "active"
    assert topics["bag.sorted"]["version"] == 1
    assert topics["bag.sorted"]["effective"] == "active"


def test_register_new_topic_family(plant):
    info = plant.register_topic("belt.jammed")
    assert (info.topic, info.family, info.version) == ("belt.jammed", "belt.jammed", 1)
    assert plant.topics.get("belt.jammed").state == "active"
    with pytest.raises(ConflictError):
        plant.register_topic("belt.jammed")
    with pytest.raises(ValidationError):
        plant.register_topic("   ")


def test_register_version_bumps_the_name(plant):
    v2 = plant.register_topic_version("bag.sorted")
    v3 = plant.register_topic_version("bag.sorted")
    assert (v2.topic, v2.version) == ("bag.sorted.v2", 2)
    assert (v3.topic, v3.version) == ("bag.sorted.v3", 3)
    assert plant.topics.families()["bag.sorted"] == [
        "bag.sorted",
        "bag.sorted.v2",
        "bag.sorted.v3",
    ]
    with pytest.raises(NotFoundError):
        plant.register_topic_version("no.such.family")


def test_unregistered_topics_are_rejected_by_the_bus(plant):
    with pytest.raises(NotFoundError):
        plant.bus.publish("ghost.topic", bag_id="T1-1")
    with pytest.raises(NotFoundError):
        plant.bus.subscribe("ghost.topic", lambda event: None)


# ---------------------------------------------------------------------------
# deprecation and the transition window
# ---------------------------------------------------------------------------


def test_deprecate_requires_a_future_deadline(plant):
    plant.register_topic_version("bag.sorted")
    with pytest.raises(ValidationError):
        plant.deprecate_topic("bag.sorted", "bag.sorted.v2", "")
    with pytest.raises(ValidationError):
        plant.deprecate_topic("bag.sorted", "bag.sorted.v2", "not-a-date")
    with pytest.raises(ValidationError):
        plant.deprecate_topic("bag.sorted", "bag.sorted.v2", "2023-12-31T23:00:00Z")


def test_deprecate_validates_the_successor(plant):
    plant.register_topic_version("bag.sorted")
    with pytest.raises(NotFoundError):
        plant.deprecate_topic("bag.sorted", "bag.sorted.v9", DEADLINE)
    with pytest.raises(ValidationError):
        plant.deprecate_topic("bag.sorted", "bag.checked_in", DEADLINE)
    with pytest.raises(ValidationError):
        plant.deprecate_topic("bag.sorted", "bag.sorted", DEADLINE)
    info = plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    assert info.state == "deprecated"
    assert info.successor == "bag.sorted.v2"
    assert info.not_after == DEADLINE
    with pytest.raises(ConflictError):
        plant.deprecate_topic("bag.sorted", "bag.sorted.v2", LATER)


def test_forwarding_reaches_legacy_and_new_subscribers(plant):
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    seen: list[tuple[str, str, str]] = []
    plant.bus.subscribe("bag.sorted", lambda e: seen.append(("legacy", e.topic, e.event_id)))
    plant.bus.subscribe("bag.sorted.v2", lambda e: seen.append(("new", e.topic, e.event_id)))

    # A legacy publisher still on the old name reaches the new subscriber...
    plant.bus.publish("bag.sorted", bag_id="T1-1")
    # ...and an upgraded publisher reaches the legacy subscriber. The
    # published version's own subscribers always run first.
    plant.bus.publish("bag.sorted.v2", bag_id="T1-2")

    assert seen == [
        ("legacy", "bag.sorted", "evt-000001"),
        ("new", "bag.sorted.v2", "evt-000001"),
        ("new", "bag.sorted.v2", "evt-000002"),
        ("legacy", "bag.sorted", "evt-000002"),
    ]


def test_handler_subscribed_to_both_versions_is_called_once(plant):
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    calls: list[str] = []

    def handler(event):
        calls.append(event.event_id)

    plant.bus.subscribe("bag.sorted", handler)
    plant.bus.subscribe("bag.sorted.v2", handler)
    plant.bus.publish("bag.sorted.v2", bag_id="T1-1")
    assert calls == ["evt-000001"]


def test_forwarding_never_republishes_or_double_counts(plant):
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    plant.bus.publish("bag.sorted.v2", bag_id="T1-1")
    assert plant.bus.delivered() == 1
    assert len(plant.bus.history()) == 1
    assert plant.telemetry.total() == 1
    assert plant.telemetry.snapshot() == {"bag.sorted.v2": 1}


def test_version_compatibility_and_count_consistency_hold_together(plant):
    # The topic is renamed twice; subscribers remain on v1 and v2.
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted.v2", "bag.sorted.v3", LATER)
    on_v1: list[str] = []
    on_v2: list[str] = []
    plant.bus.subscribe("bag.sorted", lambda e: on_v1.append(e.event_id))
    plant.bus.subscribe("bag.sorted.v2", lambda e: on_v2.append(e.event_id))

    plant.bus.publish("bag.sorted.v3", bag_id="T1-9")

    # Both legacy subscribers received the fact, rendered at their version.
    assert on_v1 == ["evt-000001"]
    assert on_v2 == ["evt-000001"]
    # The fact is counted exactly once, under the name it was published as.
    assert plant.telemetry.total() == 1
    assert plant.telemetry.snapshot() == {"bag.sorted.v3": 1}
    # The family rollup merges every version into one business count.
    assert plant.topic_family_counts()["bag.sorted"] == 1
    # The audit projection (still on the v1 name) recorded it once.
    assert plant.audit.by_kind()["sort"] == 1
    assert plant.reconcile_topics()["ok"] is True


def test_audit_projection_can_migrate_to_the_new_version(plant, flight):
    plant.register_topic_version("flight.closed")
    plant.deprecate_topic("flight.closed", "flight.closed.v2", DEADLINE)
    plant.bus.unsubscribe_all("flight.closed")
    plant.bus.subscribe("flight.closed.v2", _AuditSubscriber(plant.audit, plant.topics))
    plant.close_flight(flight.id)  # the plant still publishes the v1 name
    assert plant.audit.by_kind()["flight_close"] == 1
    assert plant.reconcile_topics()["ok"] is True


# ---------------------------------------------------------------------------
# deadline expiry and retirement
# ---------------------------------------------------------------------------


def test_expired_deprecated_topic_is_rejected(plant):
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    plant.clock.advance(3601)
    with pytest.raises(StaleError, match="deprecation deadline"):
        plant.bus.publish("bag.sorted", bag_id="T1-1")
    with pytest.raises(StaleError, match="deprecation deadline"):
        plant.bus.subscribe("bag.sorted", lambda event: None)
    # The expired version is also out of the forwarding plan.
    seen: list[str] = []
    plant.bus.subscribe("bag.sorted.v2", lambda e: seen.append(e.event_id))
    plant.bus.publish("bag.sorted.v2", bag_id="T1-2")
    assert seen == ["evt-000001"]
    assert plant.topics.describe("bag.sorted")["effective"] == "retired"


def test_retire_closes_the_topic_and_clears_subscriptions(plant):
    plant.register_topic("belt.jammed")
    plant.bus.subscribe("belt.jammed", lambda event: None)
    info = plant.retire_topic("belt.jammed")
    assert info.state == "retired"
    assert info.retired_at == plant.clock.now_iso()
    assert plant.bus.topics() == sorted(DOMAIN_TOPICS)
    with pytest.raises(StaleError, match="retired"):
        plant.bus.publish("belt.jammed")
    with pytest.raises(StaleError, match="retired"):
        plant.bus.subscribe("belt.jammed", lambda event: None)
    with pytest.raises(ConflictError):
        plant.retire_topic("belt.jammed")


def test_evolution_is_recorded_entry_by_entry(plant):
    plant.register_topic("belt.jammed")
    plant.register_topic_version("belt.jammed")
    plant.deprecate_topic("belt.jammed", "belt.jammed.v2", DEADLINE)
    plant.clock.advance(3601)
    plant.retire_topic("belt.jammed")

    evolution = [entry.as_dict() for entry in plant.topic_evolution()]
    assert [entry["action"] for entry in evolution] == [
        "registered",
        "registered",
        "deprecated",
        "retired",
    ]
    assert evolution[2]["successor"] == "belt.jammed.v2"
    assert evolution[2]["not_after"] == DEADLINE
    assert evolution[3]["topic"] == "belt.jammed"

    kinds = plant.audit.by_kind()
    assert kinds["topic_registered"] == 2
    assert kinds["topic_deprecated"] == 1
    assert kinds["topic_retired"] == 1
    assert plant.audit.verify() is True
    assert plant.reconcile_topics()["ok"] is True


def test_registry_survives_a_restart(tmp_path):
    path = str(tmp_path / "state.json")
    plant = Plant.build(Store(path), clock=Clock())
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    plant.store.persist()

    rebuilt = Plant.build(Store.open(path), clock=Clock())
    assert rebuilt.topics.get("bag.sorted").state == "deprecated"
    assert rebuilt.topics.get("bag.sorted.v2").state == "active"
    assert [entry.action for entry in rebuilt.topic_evolution()] == [
        "registered",
        "deprecated",
    ]
    # Forwarding still works across the restart boundary.
    seen: list[str] = []
    rebuilt.bus.subscribe("bag.sorted", lambda e: seen.append(e.event_id))
    rebuilt.bus.publish("bag.sorted.v2", bag_id="T1-1")
    assert seen == ["evt-000001"]
    assert rebuilt.reconcile_topics()["ok"] is True


# ---------------------------------------------------------------------------
# reconciliation
# ---------------------------------------------------------------------------


def test_reconcile_flags_legacy_subscribers_after_the_deadline(plant):
    plant.register_topic_version("bag.sorted")
    plant.deprecate_topic("bag.sorted", "bag.sorted.v2", DEADLINE)
    plant.clock.advance(3601)
    report = plant.reconcile_topics()
    assert report["ok"] is False
    assert report["expired"] == ["bag.sorted"]
    # The audit projection is still hanging on the old name.
    assert "bag.sorted" in report["dead_subscriptions"]
    # Retiring closes the window and drops the stale subscription.
    plant.retire_topic("bag.sorted")
    assert plant.reconcile_topics()["ok"] is True


def test_reconcile_detects_counter_drift(plant, flight):
    check_in(plant, flight.id, 1)
    assert plant.reconcile_topics()["ok"] is True
    plant.telemetry.observe("bag.sorted")  # a count with no event behind it
    report = plant.reconcile_topics()
    assert report["ok"] is False
    assert report["counter_mismatches"] == [
        {"family": "bag.sorted", "kind": "sort", "events": 1, "audited": 0}
    ]


def test_reconcile_detects_unknown_counters(plant):
    plant.telemetry.observe("ghost.topic")
    report = plant.reconcile_topics()
    assert report["ok"] is False
    assert report["unknown_counters"] == ["ghost.topic"]


# ---------------------------------------------------------------------------
# console endpoints
# ---------------------------------------------------------------------------


def test_topic_endpoints(server):
    status, topics = call(server, "GET", "/api/topics")
    assert status == 200
    assert len(topics) == len(DOMAIN_TOPICS)

    status, info = call(server, "POST", "/api/topics/version", {"family": "bag.sorted"})
    assert (status, info["topic"], info["version"]) == (201, "bag.sorted.v2", 2)

    status, info = call(
        server,
        "POST",
        "/api/topics/deprecate",
        {"topic": "bag.sorted", "successor": "bag.sorted.v2", "not_after": DEADLINE},
    )
    assert (status, info["state"]) == (200, "deprecated")

    status, detail = call(server, "GET", "/api/topics/bag.sorted")
    assert status == 200
    assert detail["effective"] == "deprecated"
    assert detail["expired"] is False
    assert detail["successor"] == "bag.sorted.v2"

    status, evolution = call(server, "GET", "/api/topics/evolution")
    assert status == 200
    assert [entry["action"] for entry in evolution] == ["registered", "deprecated"]

    status, report = call(server, "GET", "/api/topics/reconcile")
    assert status == 200 and report["ok"] is True

    server.plant.clock.advance(3601)
    status, report = call(server, "GET", "/api/topics/reconcile")
    assert report["ok"] is False
    assert report["expired"] == ["bag.sorted"]

    status, info = call(server, "POST", "/api/topics/retire", {"topic": "bag.sorted"})
    assert status == 200 and info["state"] == "retired"
    status, report = call(server, "GET", "/api/topics/reconcile")
    assert report["ok"] is True


def test_topic_endpoint_errors(server):
    assert call(server, "POST", "/api/topics/register", {"topic": " "})[0] == 400
    assert call(server, "POST", "/api/topics/register", {"topic": "bag.sorted"})[0] == 409
    assert call(server, "POST", "/api/topics/version", {"family": "nope"})[0] == 404
    assert call(server, "GET", "/api/topics/nope")[0] == 404
    assert call(server, "POST", "/api/topics/deprecate", {"topic": "bag.sorted"})[0] == 400
    assert call(server, "POST", "/api/topics/retire", {"topic": "nope"})[0] == 404


def test_retired_topic_rejects_the_plants_own_publish_path(server):
    status, flight = call(
        server, "POST", "/api/flights", {"code": "CA1234", "carousel": "CR1", "chute": "A"}
    )
    assert status == 201
    flight_id = flight["id"]
    call(server, "POST", "/api/topics/version", {"family": "bag.sorted"})
    call(
        server,
        "POST",
        "/api/topics/deprecate",
        {"topic": "bag.sorted", "successor": "bag.sorted.v2", "not_after": DEADLINE},
    )
    server.plant.clock.advance(3601)
    call(server, "POST", "/api/topics/retire", {"topic": "bag.sorted"})

    status, bag = call(
        server, "POST", "/api/bags/checkin", {"barcode": "9001", "flight_id": flight_id}
    )
    assert status == 201
    status, body = call(
        server, "POST", "/api/sort", {"bag_id": bag["id"], "chute_id": "T1-CHA"}
    )
    assert status == 409
    assert "retired" in body["error"]
    # The rejected publish left no trace in the projections.
    assert "bag.sorted" not in server.plant.telemetry.snapshot()
    assert "sort" not in server.plant.audit.by_kind()


def test_telemetry_endpoint_reports_family_rollups(server):
    call(server, "POST", "/api/topics/version", {"family": "bag.sorted"})
    call(
        server,
        "POST",
        "/api/topics/deprecate",
        {"topic": "bag.sorted", "successor": "bag.sorted.v2", "not_after": DEADLINE},
    )
    server.plant.bus.publish("bag.sorted.v2", bag_id="T1-1")
    status, payload = call(server, "GET", "/api/telemetry")
    assert status == 200
    assert payload["total"] == 1
    assert payload["topics"] == {"bag.sorted.v2": 1}
    assert payload["families"]["bag.sorted"] == 1
