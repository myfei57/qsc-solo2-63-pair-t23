"""Versioned topic governance: registration, windows, rejection, audit."""

from __future__ import annotations

import pytest

from bagsort.errors import ConflictError, NotFoundError, ValidationError
from bagsort.events.bus import Event
from bagsort.store.store import Store
from bagsort.topics.registry import DUAL, FORWARD
from bagsort.app import Plant

V1 = "bag.sorted.v1"
V2 = "bag.sorted.v2"
V3 = "bag.sorted.v3"
SUNSET = "2024-02-01T00:00:00Z"


def _topics(plant):
    return plant.topics


def test_seed_topics_are_registered_without_consuming_the_watermark(plant):
    assert plant.store.watermark == 0
    current = {row["topic"] for row in plant.topics.current()}
    assert V1 in current
    assert plant.topics.verify_history() is True
    bootstrap = [r for r in plant.topics.history() if r.detail.get("bootstrap")]
    assert len(bootstrap) == 8


def test_topic_names_must_be_versioned(plant):
    with pytest.raises(ValidationError):
        plant.topics.register_topic("bag.sorted")
    with pytest.raises(ValidationError):
        plant.topics.register_topic("bag.sorted.v0")


def test_registering_a_family_twice_without_replaces_conflicts(plant):
    with pytest.raises(ConflictError):
        plant.topics.register_topic(V2)


def test_new_version_must_be_higher_and_replace_an_active_topic(plant):
    plant.topics.register_topic(V2, replaces=V1)
    plant.topics.deprecate(V1, sunset_at=SUNSET, compatibility=DUAL)
    # v1 is deprecated now, not active, so it can no longer be replaced
    with pytest.raises(ConflictError):
        plant.topics.register_topic(V3, replaces=V1)
    # a lower version number than the current successor is rejected
    plant.topics.register_topic("bag.diverted.v2", replaces="bag.diverted.v1")
    with pytest.raises(ConflictError):
        plant.topics.register_topic("bag.diverted.v1", replaces="bag.diverted.v2")


def test_deprecation_requires_a_successor_and_a_future_deadline(plant):
    plant.topics.register_topic(V2, replaces=V1)
    with pytest.raises(ValidationError):
        plant.topics.deprecate(V1, sunset_at="")
    with pytest.raises(ValidationError):
        plant.topics.deprecate(V1, sunset_at="2023-12-31T00:00:00Z")
    # a topic with no successor (no introduce happened) cannot be deprecated
    with pytest.raises(ConflictError):
        plant.topics.deprecate(
            "bag.checked_in.v1", sunset_at=SUNSET, compatibility=DUAL
        )


def test_introduce_version_opens_the_window(plant):
    row = plant.topics.introduce_version(
        V2, V1, compatibility=DUAL, sunset_at=SUNSET
    )
    assert row["topic"] == V2
    old = plant.topics.topic(V1)
    assert old["status"] == "deprecated"
    assert old["sunset_at"] == SUNSET
    assert old["compatibility"] == DUAL
    assert old["replaced_by"] == V2
    assert [r.action for r in plant.topics.history("bag.sorted")] == [
        "register",
        "register",
        "deprecate",
    ]


def test_dual_emission_reaches_old_and_new_subscribers(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    old, new = [], []
    plant.bus.subscribe(V1, lambda event: old.append(event.fact_id))
    plant.bus.subscribe(V2, lambda event: new.append(event.fact_id))

    plant.bus.publish(V2, "fact-1", bag_id="B1")
    plant.bus.publish(V1, "fact-2", bag_id="B2")

    assert old == ["fact-1", "fact-2"]
    assert new == ["fact-1", "fact-2"]


def test_dual_mode_counts_one_fact_not_two(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.bus.subscribe(V1, lambda event: None)
    plant.bus.subscribe(V2, lambda event: None)
    plant.bus.publish(V2, "fact-1", bag_id="B1")

    assert plant.bus.facts() == 1
    assert plant.telemetry.fact_counts()[V2] == 1
    assert plant.reconcile_topics().consistent is True


def test_a_handler_on_both_names_receives_one_fact_once(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    seen = []
    handler = lambda event: seen.append(event.fact_id)
    plant.bus.subscribe(V1, handler)
    plant.bus.subscribe(V2, handler)
    plant.bus.publish(V2, "fact-1", bag_id="B1")
    assert seen == ["fact-1"]


def test_republishing_the_same_fact_id_is_a_noop(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    seen = []
    plant.bus.subscribe(V1, lambda event: seen.append(event.fact_id))
    plant.bus.subscribe(V2, lambda event: seen.append(event.fact_id))

    plant.bus.publish(V2, "fact-1", bag_id="B1")
    facts_before = plant.bus.facts()
    plant.bus.publish(V2, "fact-1", bag_id="B1")

    assert plant.bus.facts() == facts_before
    assert seen == ["fact-1", "fact-1"]  # one per subscriber, none redelivered
    assert plant.telemetry.fact_counts()[V2] == 1


def test_forward_mode_redirects_old_traffic_to_the_current_name(plant):
    plant.topics.introduce_version(V2, V1, compatibility=FORWARD, sunset_at=SUNSET)
    old, new = [], []
    plant.bus.subscribe(V1, lambda event: old.append((event.topic, event.fact_id)))
    plant.bus.subscribe(V2, lambda event: new.append((event.topic, event.fact_id)))

    plant.bus.publish(V1, "fact-1", bag_id="B1")

    assert old == [(V2, "fact-1")]
    assert new == [(V2, "fact-1")]
    assert plant.bus.facts() == 1


def test_forward_mode_does_not_fan_new_traffic_back_to_old_names(plant):
    plant.topics.introduce_version(V2, V1, compatibility=FORWARD, sunset_at=SUNSET)
    old = []
    plant.bus.subscribe(V1, lambda event: old.append(event.fact_id))
    plant.bus.subscribe(V2, lambda event: None)
    plant.bus.publish(V2, "fact-1", bag_id="B1")
    assert old == []


def test_sweep_retires_topics_past_their_deadline(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.clock.advance(40 * 24 * 3600)
    assert plant.topics.sweep() == [V1]
    assert plant.topics.topic(V1)["status"] == "retired"
    record = plant.topics.history("bag.sorted")[-1]
    assert record.action == "retire"
    assert record.detail["after"] == "sunset"


def test_retired_topic_rejects_publish_and_subscription(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.clock.advance(40 * 24 * 3600)
    plant.topics.sweep()

    with pytest.raises(ConflictError):
        plant.bus.publish(V1, "fact-x", bag_id="B9")
    with pytest.raises(ConflictError):
        plant.bus.subscribe(V1, lambda event: None)


def test_old_subscribers_stop_receiving_after_retirement_new_ones_keep_going(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    old, new = [], []
    plant.bus.subscribe(V1, lambda event: old.append(event.fact_id))
    plant.bus.subscribe(V2, lambda event: new.append(event.fact_id))
    plant.bus.publish(V2, "during-window", bag_id="B1")
    plant.clock.advance(40 * 24 * 3600)
    plant.topics.sweep()
    plant.bus.publish(V2, "after-window", bag_id="B2")
    assert old == ["during-window"]
    assert new == ["during-window", "after-window"]


def test_manual_retire_matches_the_swept_result(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.clock.advance(40 * 24 * 3600)
    plant.topics.retire(V1)
    assert plant.topics.topic(V1)["status"] == "retired"
    with pytest.raises(ConflictError):
        plant.topics.retire(V1)


def test_publishing_an_unregistered_topic_is_not_found(plant):
    with pytest.raises(NotFoundError):
        plant.bus.publish("ghost.v1", "g", x=1)


def test_reconcile_flags_stale_subscriptions_and_count_mismatch(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.bus.subscribe(V1, lambda event: None)
    plant.bus.publish(V2, "fact-1", bag_id="B1")
    plant.clock.advance(40 * 24 * 3600)
    plant.topics.sweep()

    result = plant.reconcile_topics()
    assert result.consistent is True
    view = next(family for family in result.families if family.family == "bag.sorted")
    assert any("stale subscription" in note for note in view.notes)


def test_governance_history_survives_replay(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.clock.advance(40 * 24 * 3600)
    plant.topics.sweep()
    plant.store.persist()

    reopened = Plant.build(Store.open(plant.store.path), clock=plant.clock)
    assert reopened.topics.topic(V1)["status"] == "retired"
    assert [
        (record.action, record.topic)
        for record in reopened.topics.history("bag.sorted")
    ] == [
        ("register", V1),
        ("register", V2),
        ("deprecate", V1),
        ("retire", V1),
    ]
    assert reopened.topics.verify_history() is True


def test_governance_chain_detects_tampering(plant):
    state = plant.store.state()
    record = state.topic_governance[0]
    tampered = dict(record)
    tampered["action"] = "rewritten"
    plant.store.state().topic_governance[0] = tampered
    assert plant.topics.verify_history() is False


def test_wildcard_subscriber_sees_one_canonical_variant_per_fact(plant):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    seen: list[Event] = []
    plant.bus.subscribe("*", lambda event: seen.append(event))
    plant.bus.publish(V2, "fact-1", bag_id="B1")
    assert len(seen) == 1
    assert seen[0].topic == V2 and seen[0].canonical is True


def test_audit_writes_one_entry_per_fact_under_dual_emission(plant, flight):
    plant.topics.introduce_version(V2, V1, compatibility=DUAL, sunset_at=SUNSET)
    plant.bus.subscribe(V1, lambda event: None)
    plant.check_in("BAG0001", flight.id)
    sort_kinds = [entry for entry in plant.audit.entries() if entry.kind == "checkin"]
    assert len(sort_kinds) == 1
    assert sort_kinds[0].detail["fact_id"].startswith("bag.checked_in#")
    assert plant.audit.verify() is True
