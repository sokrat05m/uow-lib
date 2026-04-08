from collections.abc import Iterable
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest

from uow import (
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    ListOf,
    SingleOf,
    UnitOfWork,
)


# ── Shared domain objects ────────────────────────────────────────


@dataclass
class OrderItem:
    id: int | None
    product: str
    qty: int


@dataclass
class Delivery:
    id: int | None
    address: str


@dataclass
class Order:
    id: int | None
    customer: str
    items: list[OrderItem] = field(default_factory=list)
    delivery: Delivery | None = None


# ── Fake mappers ─────────────────────────────────────────────────


class FakeOrderMapper(GenericDataMapper[Order]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Order] = []
        self.updated: list[Order] = []
        self.deleted: list[Order] = []

    async def save(self, entities: Iterable[Order]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Order]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Order]) -> None:
        self.deleted.extend(entities)


class FakeItemMapper(GenericDataMapper[OrderItem]):
    def __init__(self, connection: object) -> None:
        self.saved: list[OrderItem] = []
        self.updated: list[OrderItem] = []
        self.deleted: list[OrderItem] = []

    async def save(self, entities: Iterable[OrderItem]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[OrderItem]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[OrderItem]) -> None:
        self.deleted.extend(entities)


class FakeDeliveryMapper(GenericDataMapper[Delivery]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Delivery] = []
        self.updated: list[Delivery] = []
        self.deleted: list[Delivery] = []

    async def save(self, entities: Iterable[Delivery]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Delivery]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Delivery]) -> None:
        self.deleted.extend(entities)


# ── Shared fixtures ──────────────────────────────────────────────


@pytest.fixture
def fake_connection() -> AsyncMock:
    return AsyncMock()


@pytest.fixture
def order_registry() -> InstrumentationRegistry:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Order,
            identity_key=("id",),
            mapper_type=FakeOrderMapper,
            children={
                "items": ListOf(OrderItem),
                "delivery": SingleOf(Delivery),
            },
            depends_on=[],
        )
    )
    reg.register(
        EntityConfig(
            entity_type=OrderItem,
            identity_key=("id",),
            mapper_type=FakeItemMapper,
            children={},
            depends_on=[Order],
        )
    )
    reg.register(
        EntityConfig(
            entity_type=Delivery,
            identity_key=("id",),
            mapper_type=FakeDeliveryMapper,
            children={},
            depends_on=[Order],
        )
    )
    return reg


@pytest.fixture
def order_uow(
    fake_connection: AsyncMock,
    order_registry: InstrumentationRegistry,
) -> UnitOfWork:
    return UnitOfWork(fake_connection, order_registry)
