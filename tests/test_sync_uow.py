from collections.abc import Iterable
from dataclasses import dataclass, field
from unittest.mock import Mock

import pytest

from uow import EntityConfig, InstrumentationRegistry, ListOf, SingleOf
from uow.sync import GenericDataMapper, UnitOfWork


@dataclass
class SyncOrderItem:
    id: int | None
    product: str
    qty: int


@dataclass
class SyncDelivery:
    id: int | None
    address: str


@dataclass
class SyncOrder:
    id: int | None
    customer: str
    items: list[SyncOrderItem] = field(default_factory=list)
    delivery: SyncDelivery | None = None


class SyncOrderMapper(GenericDataMapper[SyncOrder]):
    def __init__(self, connection: object) -> None:
        self.saved: list[SyncOrder] = []
        self.updated: list[SyncOrder] = []
        self.deleted: list[SyncOrder] = []

    def save(self, entities: Iterable[SyncOrder]) -> None:
        self.saved.extend(entities)

    def update(self, entities: Iterable[SyncOrder]) -> None:
        self.updated.extend(entities)

    def delete(self, entities: Iterable[SyncOrder]) -> None:
        self.deleted.extend(entities)


class SyncItemMapper(GenericDataMapper[SyncOrderItem]):
    def __init__(self, connection: object) -> None:
        self.saved: list[SyncOrderItem] = []
        self.updated: list[SyncOrderItem] = []
        self.deleted: list[SyncOrderItem] = []

    def save(self, entities: Iterable[SyncOrderItem]) -> None:
        self.saved.extend(entities)

    def update(self, entities: Iterable[SyncOrderItem]) -> None:
        self.updated.extend(entities)

    def delete(self, entities: Iterable[SyncOrderItem]) -> None:
        self.deleted.extend(entities)


class SyncDeliveryMapper(GenericDataMapper[SyncDelivery]):
    def __init__(self, connection: object) -> None:
        self.saved: list[SyncDelivery] = []
        self.updated: list[SyncDelivery] = []
        self.deleted: list[SyncDelivery] = []

    def save(self, entities: Iterable[SyncDelivery]) -> None:
        self.saved.extend(entities)

    def update(self, entities: Iterable[SyncDelivery]) -> None:
        self.updated.extend(entities)

    def delete(self, entities: Iterable[SyncDelivery]) -> None:
        self.deleted.extend(entities)


class FailingOrderMapper(SyncOrderMapper):
    def update(self, entities: Iterable[SyncOrder]) -> None:
        raise RuntimeError("mapper failed")


@pytest.fixture
def sync_connection() -> Mock:
    return Mock()


@pytest.fixture
def sync_registry() -> InstrumentationRegistry:
    registry = InstrumentationRegistry()
    registry.register(
        EntityConfig(
            entity_type=SyncOrder,
            identity_key=("id",),
            mapper_type=SyncOrderMapper,
            children={
                "items": ListOf(SyncOrderItem),
                "delivery": SingleOf(SyncDelivery),
            },
            depends_on=[],
        ),
    )
    registry.register(
        EntityConfig(
            entity_type=SyncOrderItem,
            identity_key=("id",),
            mapper_type=SyncItemMapper,
            children={},
            depends_on=[SyncOrder],
        ),
    )
    registry.register(
        EntityConfig(
            entity_type=SyncDelivery,
            identity_key=("id",),
            mapper_type=SyncDeliveryMapper,
            children={},
            depends_on=[SyncOrder],
        ),
    )
    return registry


@pytest.fixture
def sync_uow(
    sync_connection: Mock,
    sync_registry: InstrumentationRegistry,
) -> UnitOfWork:
    return UnitOfWork(sync_connection, sync_registry)


def test_commit_calls_sync_mapper_and_connection(
    sync_uow: UnitOfWork,
    sync_connection: Mock,
) -> None:
    order = SyncOrder(id=1, customer="Alice")
    sync_uow.register_clean(order)
    order.customer = "Bob"

    sync_uow.commit()

    mapper = sync_uow._mappers[SyncOrder]
    assert isinstance(mapper, SyncOrderMapper)
    assert order in mapper.updated
    sync_connection.commit.assert_called_once_with()
    sync_connection.rollback.assert_not_called()


def test_flush_calls_mapper_without_commit(
    sync_uow: UnitOfWork,
    sync_connection: Mock,
) -> None:
    order = SyncOrder(id=None, customer="Alice")
    sync_uow.register_new(order)

    sync_uow.flush()

    mapper = sync_uow._mappers[SyncOrder]
    assert isinstance(mapper, SyncOrderMapper)
    assert order in mapper.saved
    sync_connection.commit.assert_not_called()
    sync_connection.rollback.assert_not_called()
    assert sync_uow._build_operations() == []


def test_rollback_detaches_tracked_state(
    sync_uow: UnitOfWork,
    sync_connection: Mock,
) -> None:
    order = SyncOrder(id=1, customer="Alice")
    sync_uow.register_clean(order)

    sync_uow.rollback()

    sync_connection.rollback.assert_called_once_with()
    assert sync_uow._entries == {}
    assert sync_uow._mappers == {}


def test_commit_rolls_back_detaches_and_reraises(
    sync_connection: Mock,
) -> None:
    registry = InstrumentationRegistry()
    registry.register(
        EntityConfig(
            entity_type=SyncOrder,
            identity_key=("id",),
            mapper_type=FailingOrderMapper,
            children={},
            depends_on=[],
        ),
    )
    uow = UnitOfWork(sync_connection, registry)
    order = SyncOrder(id=1, customer="Alice")
    uow.register_clean(order)
    order.customer = "Bob"

    with pytest.raises(RuntimeError, match="mapper failed"):
        uow.commit()

    sync_connection.rollback.assert_called_once_with()
    sync_connection.commit.assert_not_called()
    assert uow._entries == {}
    assert uow._mappers == {}


def test_full_cycle_uses_sync_save_update_and_delete(
    sync_uow: UnitOfWork,
) -> None:
    item = SyncOrderItem(id=10, product="Widget", qty=5)
    delivery = SyncDelivery(id=20, address="123 Main")
    order = SyncOrder(
        id=1,
        customer="Alice",
        items=[item],
        delivery=delivery,
    )
    sync_uow.register_clean(order)

    order.customer = "Bob"
    new_item = SyncOrderItem(id=None, product="Gadget", qty=1)
    order.items.append(new_item)
    item.qty = 10
    new_delivery = SyncDelivery(id=None, address="456 Oak")
    order.delivery = new_delivery

    sync_uow.commit()

    order_mapper = sync_uow._mappers[SyncOrder]
    item_mapper = sync_uow._mappers[SyncOrderItem]
    delivery_mapper = sync_uow._mappers[SyncDelivery]

    assert isinstance(order_mapper, SyncOrderMapper)
    assert isinstance(item_mapper, SyncItemMapper)
    assert isinstance(delivery_mapper, SyncDeliveryMapper)
    assert order in order_mapper.updated
    assert item in item_mapper.updated
    assert new_item in item_mapper.saved
    assert delivery in delivery_mapper.deleted
    assert new_delivery in delivery_mapper.saved
