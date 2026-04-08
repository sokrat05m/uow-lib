from collections.abc import Iterable
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest

from uow import GenericDataMapper, InstrumentationRegistry, EntityConfig, UnitOfWork


@dataclass
class Aggregate:
    id: int | None
    name: str
    _events: list[str] = field(default_factory=list)


class FakeAggregateMapper(GenericDataMapper[Aggregate]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Aggregate] = []
        self.updated: list[Aggregate] = []
        self.deleted: list[Aggregate] = []

    async def save(self, entities: Iterable[Aggregate]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Aggregate]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Aggregate]) -> None:
        self.deleted.extend(entities)


@pytest.fixture
def registry() -> InstrumentationRegistry:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Aggregate,
            identity_key=("id",),
            mapper_type=FakeAggregateMapper,
            exclude_from_tracking=frozenset({"_events"}),
        )
    )
    return reg


@pytest.fixture
def uow(fake_connection: AsyncMock, registry: InstrumentationRegistry) -> UnitOfWork:
    return UnitOfWork(fake_connection, registry)


class TestExcludeFromTracking:
    def test_excluded_field_not_in_tracked_attrs(self) -> None:
        config = EntityConfig(
            entity_type=Aggregate,
            identity_key=("id",),
            mapper_type=FakeAggregateMapper,
            exclude_from_tracking=frozenset({"_events"}),
        )
        assert "_events" not in config.tracked_attrs
        assert "name" in config.tracked_attrs
        assert "id" in config.tracked_attrs

    def test_excluded_field_change_does_not_produce_update(
        self,
        uow: UnitOfWork,
    ) -> None:
        agg = Aggregate(id=1, name="root")
        uow.register_clean(agg)

        agg._events.append("something_happened")
        agg._events = ["replaced"]

        ops = uow._build_operations()
        assert ops == []

    def test_non_excluded_field_still_tracked(
        self,
        uow: UnitOfWork,
    ) -> None:
        agg = Aggregate(id=1, name="root")
        uow.register_clean(agg)

        agg.name = "updated"

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_mixed_changes_only_tracks_non_excluded(
        self,
        uow: UnitOfWork,
    ) -> None:
        agg = Aggregate(id=1, name="root")
        uow.register_clean(agg)

        agg._events = ["event1"]
        agg.name = "updated"

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
