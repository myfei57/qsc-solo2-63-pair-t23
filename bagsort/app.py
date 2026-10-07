"""Assembly of the handling line.

Every subsystem is constructed here and every operator visible action is a
method on :class:`Plant`, which is what keeps the console handlers thin and
lets the CLI drive the same operations the HTTP API exposes.
"""

from __future__ import annotations

from dataclasses import dataclass

from bagsort.audit.chain import AuditChain
from bagsort.belt.belt import Belt
from bagsort.checkin.desk import Desk
from bagsort.clock import Clock
from bagsort.errors import ConflictError
from bagsort.events.bus import EventBus
from bagsort.flight.book import FlightBook
from bagsort.flight.close import FlightCloser
from bagsort.flight.mapping import BagMapping
from bagsort.flight.schedule import ScheduleBoard
from bagsort.ids import IdFactory
from bagsort.inject.injector import Injector
from bagsort.model.entities import Bag, Flight, Reading, SortRecord
from bagsort.model.errors import unknown_bag
from bagsort.ns.chutes import ChuteRegistry
from bagsort.ns.namespace import Namespace
from bagsort.ns.topology import handling_line
from bagsort.quota.quota import QuotaBook
from bagsort.recheck.queue import RecheckQueue
from bagsort.reporting.report import PlantReport
from bagsort.sequence.verdict import VerdictTracker
from bagsort.sorter.sorter import Sorter
from bagsort.store.store import Store
from bagsort.tag.history import ReadHistory
from bagsort.tag.reader import TagReader
from bagsort.chute.assigner import ChuteAssigner
from bagsort.chute.history import AssignmentHistory
from bagsort.console.metrics import Metrics
from bagsort.console.telemetry import Telemetry
from bagsort.topics.catalog import (
    AUDIT_STAGE,
    CANONICAL_TOPICS,
    BAG_CHECKED_IN,
    BAG_SORTED,
    BAG_DIVERTED,
    BAG_RECHECK_ENQUEUED,
    BAG_REMAPPED,
    FLIGHT_CLOSED,
    CHUTE_ASSIGNED,
    BELT_SWITCHED,
)
from bagsort.topics.reconcile import reconcile
from bagsort.topics.registry import TopicRegistry

DEFAULT_TERMINAL = "T1"


@dataclass
class Plant:
    """The wired plant plus the operator level operations the console calls."""

    store: Store
    clock: Clock
    namespace: Namespace
    registry: ChuteRegistry
    bus: EventBus
    topics: TopicRegistry
    telemetry: Telemetry
    metrics: Metrics
    reader: TagReader
    reads: ReadHistory
    book: FlightBook
    mapping: BagMapping
    schedule: ScheduleBoard
    closer: FlightCloser
    chutes: ChuteAssigner
    chute_history: AssignmentHistory
    sorter: Sorter
    injector: Injector
    desk: Desk
    recheck: RecheckQueue
    belt: Belt
    quota: QuotaBook
    audit: AuditChain
    report: PlantReport

    @property
    def topology(self):
        return handling_line()

    # -- construction ------------------------------------------------------

    @classmethod
    def build(
        cls,
        store: Store,
        terminal: str = DEFAULT_TERMINAL,
        clock: Clock | None = None,
    ) -> "Plant":
        clock = clock or Clock()
        namespace = Namespace(terminal)
        registry = ChuteRegistry()
        bus = EventBus()
        topics = TopicRegistry(store, clock)
        # Bootstrap the built in catalog on a fresh store: the entries replay
        # like any other but do not consume an operator commit.
        for name in CANONICAL_TOPICS:
            topics.bootstrap(name)
        bus.attach_registry(topics)
        telemetry = Telemetry()
        metrics = Metrics()
        bus.subscribe("*", lambda event: telemetry.observe_fact(_canonical_topic(topics, event)))

        reader = TagReader(store, namespace, clock)
        reads = ReadHistory(store)
        book = FlightBook(store, clock)
        mapping = BagMapping(store, clock)
        schedule = ScheduleBoard(store, clock)
        chutes = ChuteAssigner(store, clock)
        chute_history = AssignmentHistory(store)
        sorter = Sorter(store, mapping, chutes, clock, VerdictTracker())
        injector = Injector(store, book, sorter, clock)
        quota = QuotaBook(store, clock)
        desk = Desk(store, reader, book, mapping, injector, quota, clock, handling_line())
        closer = FlightCloser(book)
        recheck = RecheckQueue(store, book, clock)
        belt = Belt(store, clock, IdFactory("cmd"))

        audit = AuditChain(store, clock, handling_line())
        audit_subscriber = _AuditSubscriber(audit)
        # One family subscription covers every concrete version, including ones
        # introduced later via the console and retired ones replayed at restart.
        for family in AUDIT_STAGE_BY_FAMILY:
            bus.subscribe_family(family, audit_subscriber)

        report = PlantReport(
            store, book, chutes, belt, quota, audit, namespace, handling_line()
        )
        return cls(
            store=store,
            clock=clock,
            namespace=namespace,
            registry=registry,
            bus=bus,
            topics=topics,
            telemetry=telemetry,
            metrics=metrics,
            reader=reader,
            reads=reads,
            book=book,
            mapping=mapping,
            schedule=schedule,
            closer=closer,
            chutes=chutes,
            chute_history=chute_history,
            sorter=sorter,
            injector=injector,
            desk=desk,
            recheck=recheck,
            belt=belt,
            quota=quota,
            audit=audit,
            report=report,
        )

    # -- operator operations ----------------------------------------------

    def register_flight(self, code: str, carousel: str = "", chute: str = "") -> Flight:
        flight = Flight(
            id=self.namespace.flight_id(code),
            code=code.strip().upper(),
            terminal=self.namespace.terminal,
            carousel=self.namespace.carousel_id(carousel) if carousel else "",
        )
        self.book.register(flight)
        if chute:
            self.assign_chute(flight.id, self.namespace.chute_id(chute))
        return self.book.get(flight.id)

    def assign_chute(self, flight_id: str, chute_id: str):
        assignment = self.chutes.assign(flight_id, chute_id)
        self.bus.publish(
            CHUTE_ASSIGNED, f"chute.assigned#{assignment.generation}:{flight_id}",
            flight_id=flight_id, chute_id=chute_id,
        )
        return assignment

    def read_barcode(self, barcode: str) -> Reading:
        return self.reader.read(barcode)

    def check_in(self, barcode: str, flight_id: str) -> Bag:
        bag = self.desk.check_in(barcode, flight_id)
        self.bus.publish(
            BAG_CHECKED_IN, f"bag.checked_in#{bag.id}",
            bag_id=bag.id, flight_id=flight_id,
        )
        return bag

    def cancel_check_in(self, bag_id: str) -> Bag:
        bag = self.desk.cancel(bag_id)
        self.bus.publish(
            BAG_DIVERTED, f"bag.diverted#cancel:{bag_id}",
            bag_id=bag_id, reason="checkin_cancelled",
        )
        return bag

    def bag(self, bag_id: str) -> Bag:
        bag = self.store.state().bags.get(bag_id)
        if bag is None:
            raise unknown_bag(bag_id)
        return bag

    def push_bag(self, bag_id: str, chute_id: str) -> SortRecord:
        bag = self.bag(bag_id)
        record = self.injector.push(bag, chute_id)
        self.bus.publish(
            BAG_SORTED, f"bag.sorted#{bag.id}:{record.sequence}",
            bag_id=bag.id, chute_id=record.chute_id,
        )
        return record

    def retry_bag(self, bag_id: str) -> bool:
        bag = self.bag(bag_id)
        sorted_now = self.injector.retry(bag)
        if sorted_now:
            self.bus.publish(
                BAG_SORTED, f"bag.sorted.retry#{bag.id}:{bag.sequence}",
                bag_id=bag.id, retry=True,
            )
        return sorted_now

    def divert_bag(self, bag_id: str) -> Bag:
        bag = self.bag(bag_id)
        updated = self.sorter.divert(bag)
        self.bus.publish(BAG_DIVERTED, f"bag.diverted#{bag_id}", bag_id=bag_id)
        return updated

    def remap_bag(self, bag_id: str, flight_id: str) -> Bag:
        self.book.require_open(flight_id)
        self.mapping.remap(bag_id, flight_id)
        self.bus.publish(
            BAG_REMAPPED, f"bag.remapped#{bag_id}:{flight_id}",
            bag_id=bag_id, flight_id=flight_id,
        )
        return self.bag(bag_id)

    def revise_schedule(self, flight_id: str, cutoff_at: str):
        return self.schedule.revise(flight_id, cutoff_at)

    def close_flight(self, flight_id: str) -> Flight:
        flight = self.closer.close(flight_id, self.injector)
        self.bus.publish(FLIGHT_CLOSED, f"flight.closed#{flight_id}", flight_id=flight_id)
        return flight

    def enqueue_recheck(self, bag_id: str) -> Bag:
        bag = self.bag(bag_id)
        self.recheck.enqueue(bag)
        self.bus.publish(
            BAG_RECHECK_ENQUEUED, f"bag.recheck_enqueued#{bag_id}", bag_id=bag_id
        )
        return bag

    def process_recheck(self) -> Bag | None:
        return self.recheck.dequeue()

    def switch_belt(self, carousel: str, batch: int) -> int:
        cleared = self.belt.switch(carousel, batch)
        self.bus.publish(
            BELT_SWITCHED, f"belt.switched#{carousel}:{batch}",
            carousel=carousel, batch=batch,
        )
        return cleared

    def rotate_belt(self) -> int:
        return self.belt.rotate()

    def append_transfer(self, flight_id: str, carousel: str, batch: int):
        return self.belt.append_transfer(flight_id, carousel, batch)

    def set_quota(self, flight_id: str, limit: int) -> int:
        return self.quota.set_limit(flight_id, limit)

    def verify(self) -> bool:
        return self.audit.verify()

    def reconcile_topics(self, *, now_iso: str = ""):
        """Cross check registry, bus subscriptions and fact counters."""

        return reconcile(
            self.topics,
            self.bus,
            self.telemetry,
            now_iso=now_iso or self.clock.now_iso(),
        )

    def sweep_topics(self) -> list[str]:
        """Retire deprecated topics whose sunset deadline has passed."""

        return self.topics.sweep()


def _canonical_topic(topics: TopicRegistry, event) -> str:
    """Counter key: the current topic name of a fact's family."""

    try:
        return topics.family(event.family)["topic"] if event.family else event.topic
    except Exception:
        return event.topic


class _AuditSubscriber:
    """Turns published operations into linked audit entries.

    The mapping is keyed by family, so during dual emission the same fact
    arriving under an old version still audits with the right kind and stage.
    Delivery is deduplicated per ``fact_id`` on the bus, which guarantees this
    subscriber writes one entry per business fact.
    """

    def __init__(self, audit: AuditChain) -> None:
        self._audit = audit

    def __call__(self, event) -> None:
        family = event.family or _family_of(event.topic)
        mapping = AUDIT_STAGE_BY_FAMILY.get(family)
        if mapping is None:
            raise ConflictError(f"no audit mapping for topic {event.topic!r}")
        kind, stage = mapping
        subject = str(
            event.payload.get("bag_id")
            or event.payload.get("flight_id")
            or event.payload.get("carousel")
            or ""
        )
        detail = {
            key: value
            for key, value in event.payload.items()
            if key not in {"bag_id", "flight_id"}
        }
        detail["topic"] = event.topic
        detail["fact_id"] = event.fact_id
        self._audit.record(kind, subject, stage, detail)


def _family_of(topic: str) -> str:
    return topic.rsplit(".v", 1)[0]


AUDIT_STAGE_BY_FAMILY = {
    topic.rsplit(".v", 1)[0]: mapping for topic, mapping in AUDIT_STAGE.items()
}
