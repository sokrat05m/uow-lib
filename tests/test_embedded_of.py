from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from uow import (
    GenericDataMapper,
    InstrumentationRegistry,
    EntityConfig,
    EmbeddedOf,
    UnitOfWork,
)


@dataclass(frozen=True)
class Address:
    street: str
    city: str


@dataclass
class Customer:
    id: int | None
    name: str
    address: Address | None = None


class FakeCustomerMapper(GenericDataMapper[Customer]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Customer] = []
        self.updated: list[Customer] = []
        self.deleted: list[Customer] = []

    async def save(self, entities: Iterable[Customer]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Customer]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Customer]) -> None:
        self.deleted.extend(entities)


@pytest.fixture
def registry() -> InstrumentationRegistry:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Customer,
            identity_key=("id",),
            mapper_type=FakeCustomerMapper,
            children={"address": EmbeddedOf(Address)},
        )
    )
    return reg


@pytest.fixture
def uow(fake_connection: AsyncMock, registry: InstrumentationRegistry) -> UnitOfWork:
    return UnitOfWork(fake_connection, registry)


class TestEmbeddedOfRegistration:
    def test_rejects_non_frozen_dataclass(self) -> None:
        @dataclass
        class MutableVO:
            value: str

        reg = InstrumentationRegistry()
        with pytest.raises(TypeError, match="must be a frozen dataclass"):
            reg.register(
                EntityConfig(
                    entity_type=Customer,
                    identity_key=("id",),
                    mapper_type=FakeCustomerMapper,
                    children={"address": EmbeddedOf(MutableVO)},
                )
            )

    def test_rejects_plain_class(self) -> None:
        class PlainVO:
            pass

        reg = InstrumentationRegistry()
        with pytest.raises(TypeError, match="must be a frozen dataclass"):
            reg.register(
                EntityConfig(
                    entity_type=Customer,
                    identity_key=("id",),
                    mapper_type=FakeCustomerMapper,
                    children={"address": EmbeddedOf(PlainVO)},
                )
            )

    def test_accepts_frozen_dataclass(self) -> None:
        reg = InstrumentationRegistry()
        reg.register(
            EntityConfig(
                entity_type=Customer,
                identity_key=("id",),
                mapper_type=FakeCustomerMapper,
                children={"address": EmbeddedOf(Address)},
            )
        )
        assert reg.get(Customer) is not None


class TestEmbeddedOfTracking:
    def test_replace_vo_marks_parent_dirty(self, uow: UnitOfWork) -> None:
        customer = Customer(id=1, name="Alice", address=Address("Main St", "NYC"))
        uow.register_clean(customer)

        customer.address = Address("Oak Ave", "LA")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
        assert ops[0][1] is Customer

    def test_set_vo_to_none_marks_dirty(self, uow: UnitOfWork) -> None:
        customer = Customer(id=1, name="Alice", address=Address("Main St", "NYC"))
        uow.register_clean(customer)

        customer.address = None

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_set_vo_from_none_marks_dirty(self, uow: UnitOfWork) -> None:
        customer = Customer(id=1, name="Alice", address=None)
        uow.register_clean(customer)

        customer.address = Address("Main St", "NYC")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_no_vo_change_no_update(self, uow: UnitOfWork) -> None:
        customer = Customer(id=1, name="Alice", address=Address("Main St", "NYC"))
        uow.register_clean(customer)

        ops = uow._build_operations()
        assert ops == []

    def test_vo_not_registered_as_separate_entity(self, uow: UnitOfWork) -> None:
        addr = Address("Main St", "NYC")
        customer = Customer(id=1, name="Alice", address=addr)
        uow.register_clean(customer)

        assert id(addr) not in uow._entries

    def test_new_entity_with_vo_produces_single_insert(self, uow: UnitOfWork) -> None:
        customer = Customer(id=None, name="Alice", address=Address("Main St", "NYC"))
        uow.register_new(customer)

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "insert"
        assert ops[0][1] is Customer

    @pytest.mark.asyncio
    async def test_commit_with_vo_replacement(self, uow: UnitOfWork) -> None:
        customer = Customer(id=1, name="Alice", address=Address("Main St", "NYC"))
        uow.register_clean(customer)

        customer.address = Address("Oak Ave", "LA")
        await uow.commit()

        mapper = uow._mappers[Customer]
        assert isinstance(mapper, FakeCustomerMapper)
        assert customer in mapper.updated

        ops = uow._build_operations()
        assert ops == []
