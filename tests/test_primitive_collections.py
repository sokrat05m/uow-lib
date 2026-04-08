from collections.abc import Iterable
from dataclasses import dataclass, field
from unittest.mock import AsyncMock

import pytest

from uow import GenericDataMapper, InstrumentationRegistry, EntityConfig, UnitOfWork


@dataclass
class Profile:
    id: int | None
    name: str
    tags: list[str] = field(default_factory=list)
    roles: set[str] = field(default_factory=set)
    metadata: dict[str, str] = field(default_factory=dict)


class FakeProfileMapper(GenericDataMapper[Profile]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Profile] = []
        self.updated: list[Profile] = []
        self.deleted: list[Profile] = []

    async def save(self, entities: Iterable[Profile]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Profile]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Profile]) -> None:
        self.deleted.extend(entities)


@pytest.fixture
def registry() -> InstrumentationRegistry:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Profile,
            identity_key=("id",),
            mapper_type=FakeProfileMapper,
        )
    )
    return reg


@pytest.fixture
def uow(fake_connection: AsyncMock, registry: InstrumentationRegistry) -> UnitOfWork:
    return UnitOfWork(fake_connection, registry)


class TestPrimitiveCollectionTracking:
    def test_list_append_marks_dirty(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", tags=["a"])
        uow.register_clean(profile)

        profile.tags.append("b")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
        assert ops[0][1] is Profile

    def test_set_add_marks_dirty(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", roles={"admin"})
        uow.register_clean(profile)

        profile.roles.add("editor")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_dict_setitem_marks_dirty(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", metadata={"k": "v"})
        uow.register_clean(profile)

        profile.metadata["k2"] = "v2"

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_no_mutation_no_update(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", tags=["a"])
        uow.register_clean(profile)

        ops = uow._build_operations()
        assert ops == []

    @pytest.mark.asyncio
    async def test_dirty_after_replace_and_commit(
        self,
        uow: UnitOfWork,
    ) -> None:
        profile = Profile(id=1, name="Alice", tags=["a"])
        uow.register_clean(profile)

        profile.tags = ["b", "c"]
        await uow.commit()

        profile.tags.append("d")
        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_list_remove_marks_dirty(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", tags=["a", "b"])
        uow.register_clean(profile)

        profile.tags.remove("a")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    def test_dict_pop_marks_dirty(self, uow: UnitOfWork) -> None:
        profile = Profile(id=1, name="Alice", metadata={"k": "v"})
        uow.register_clean(profile)

        profile.metadata.pop("k")

        ops = uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
