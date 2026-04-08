from collections.abc import Iterable
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest

from uow import GenericDataMapper, InstrumentationRegistry, EntityConfig, UnitOfWork


@dataclass(frozen=True)
class Address:
    street: str
    city: str


@dataclass
class Company:
    id: int | None
    name: str
    addresses: list[Address] = field(default_factory=list)


class FakeCompanyMapper(GenericDataMapper[Company]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Company] = []
        self.updated: list[Company] = []
        self.deleted: list[Company] = []

    async def save(self, entities: Iterable[Company]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Company]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Company]) -> None:
        self.deleted.extend(entities)


@pytest.fixture
def registry() -> InstrumentationRegistry:
    from uow.instrumentation import CollectionOfEmbedded

    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Company,
            identity_key=("id",),
            mapper_type=FakeCompanyMapper,
            children={"addresses": CollectionOfEmbedded(Address)},
        )
    )
    return reg


@pytest.fixture
def uow(
    fake_connection: AsyncMock,
    registry: InstrumentationRegistry,
) -> UnitOfWork:
    return UnitOfWork(fake_connection, registry)


class TestCollectionOfEmbeddedTracking:
    def test_append_marks_dirty(self, uow: UnitOfWork) -> None:
        company = Company(id=1, name="Corp", addresses=[Address("Main", "NYC")])
        uow.register_clean(company)

        company.addresses.append(Address("Oak", "LA"))

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
        assert ops[0][1] is Company

    def test_no_separate_entity_registered(
        self,
        uow: UnitOfWork,
    ) -> None:
        addr = Address("Main", "NYC")
        company = Company(id=1, name="Corp", addresses=[addr])
        uow.register_clean(company)

        assert id(addr) not in uow._entries

    def test_rejects_non_frozen_dataclass(self) -> None:
        from uow.instrumentation import CollectionOfEmbedded

        @dataclass
        class MutableVO:
            value: str

        reg = InstrumentationRegistry()
        with pytest.raises(TypeError, match="must be a frozen dataclass"):
            reg.register(
                EntityConfig(
                    entity_type=Company,
                    identity_key=("id",),
                    mapper_type=FakeCompanyMapper,
                    children={"addresses": CollectionOfEmbedded(MutableVO)},
                )
            )
