from collections.abc import Iterable
from dataclasses import dataclass
from typing import Hashable, cast
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from uow import (
    DuplicateEntityError,
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    UnitOfWork,
)


# ── Value-object IDs ────────────────────────────────────────────


@dataclass(frozen=True)
class UUIDId:
    value: UUID

    def __eq__(self, other: object) -> bool:
        if type(self) == type(other):
            return self.value == cast(UUIDId, other).value
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.value)


class UserId(UUIDId): ...


class ProjectId(UUIDId): ...


# ── Entities ────────────────────────────────────────────────────


class DomainEntity[EntityID: Hashable]:
    entity_id: EntityID

    def __eq__(self, other: object) -> bool:
        if isinstance(other, DomainEntity):
            return bool(other.entity_id == self.entity_id)
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self.entity_id)


class User(DomainEntity[UserId]):
    full_name: str

    def __init__(self, entity_id: UserId, full_name: str) -> None:
        self.entity_id = entity_id
        self.full_name = full_name


class Project(DomainEntity[ProjectId]):
    title: str

    def __init__(self, entity_id: ProjectId, title: str) -> None:
        self.entity_id = entity_id
        self.title = title


# ── Fake mappers ────────────────────────────────────────────────


class FakeUserMapper(GenericDataMapper[User]):
    def __init__(self, connection: object) -> None:
        self.saved: list[User] = []
        self.updated: list[User] = []
        self.deleted: list[User] = []

    async def save(self, entities: Iterable[User]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[User]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[User]) -> None:
        self.deleted.extend(entities)


class FakeProjectMapper(GenericDataMapper[Project]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Project] = []
        self.updated: list[Project] = []
        self.deleted: list[Project] = []

    async def save(self, entities: Iterable[Project]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Project]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Project]) -> None:
        self.deleted.extend(entities)


# ── Fixtures ────────────────────────────────────────────────────


@pytest.fixture
def user_uow() -> UnitOfWork:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=User,
            identity_key=("entity_id",),
            mapper_type=FakeUserMapper,
        )
    )
    return UnitOfWork(AsyncMock(), reg)


@pytest.fixture
def multi_uow() -> UnitOfWork:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=User,
            identity_key=("entity_id",),
            mapper_type=FakeUserMapper,
        )
    )
    reg.register(
        EntityConfig(
            entity_type=Project,
            identity_key=("entity_id",),
            mapper_type=FakeProjectMapper,
        )
    )
    return UnitOfWork(AsyncMock(), reg)


# ── Tests ───────────────────────────────────────────────────────


class TestDataclassIdClean:
    def test_no_changes_no_update(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)

        ops = user_uow._build_operations()
        assert ops == []

    def test_scalar_change_produces_update(
        self, user_uow: UnitOfWork
    ) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)

        user.full_name = "Bob"

        ops = user_uow._build_operations()
        assert len(ops) == 1
        op_type, entity_type, entities = ops[0]
        assert op_type.value == "update"
        assert entity_type is User
        assert entities == [user]

    def test_delete_produces_delete(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)

        user_uow.register_deleted(user)

        ops = user_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "delete"


class TestDataclassIdNew:
    def test_new_entity_produces_insert(
        self, user_uow: UnitOfWork
    ) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_new(user)

        ops = user_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "insert"
        assert ops[0][1] is User


class TestDataclassIdIdentityMap:
    def test_duplicate_identity_raises(self, user_uow: UnitOfWork) -> None:
        uid = UserId(uuid4())
        user1 = User(entity_id=uid, full_name="Alice")
        user2 = User(entity_id=uid, full_name="Bob")

        user_uow.register_clean(user1)

        with pytest.raises(DuplicateEntityError):
            user_uow.register_clean(user2)

    def test_different_ids_coexist(self, user_uow: UnitOfWork) -> None:
        user1 = User(entity_id=UserId(uuid4()), full_name="Alice")
        user2 = User(entity_id=UserId(uuid4()), full_name="Bob")

        user_uow.register_clean(user1)
        user_uow.register_clean(user2)

        ops = user_uow._build_operations()
        assert ops == []

    def test_same_uuid_different_id_types_are_independent(
        self, multi_uow: UnitOfWork
    ) -> None:
        raw_uuid = uuid4()
        user = User(entity_id=UserId(raw_uuid), full_name="Alice")
        project = Project(entity_id=ProjectId(raw_uuid), title="Alpha")

        multi_uow.register_clean(user)
        multi_uow.register_clean(project)

        ops = multi_uow._build_operations()
        assert ops == []


class TestDataclassIdCommit:
    @pytest.mark.asyncio
    async def test_commit_update(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)
        user.full_name = "Bob"

        await user_uow.commit()

        mapper = user_uow._mappers[User]
        assert isinstance(mapper, FakeUserMapper)
        assert user in mapper.updated

    @pytest.mark.asyncio
    async def test_commit_insert(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_new(user)

        await user_uow.commit()

        mapper = user_uow._mappers[User]
        assert isinstance(mapper, FakeUserMapper)
        assert user in mapper.saved

    @pytest.mark.asyncio
    async def test_commit_cleans_state(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)
        user.full_name = "Bob"

        await user_uow.commit()

        ops = user_uow._build_operations()
        assert ops == []

    @pytest.mark.asyncio
    async def test_second_mutation_after_commit(
        self, user_uow: UnitOfWork
    ) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)
        user.full_name = "Bob"

        await user_uow.commit()

        user.full_name = "Charlie"

        ops = user_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"

    @pytest.mark.asyncio
    async def test_commit_delete(self, user_uow: UnitOfWork) -> None:
        user = User(entity_id=UserId(uuid4()), full_name="Alice")
        user_uow.register_clean(user)
        user_uow.register_deleted(user)

        await user_uow.commit()

        mapper = user_uow._mappers[User]
        assert isinstance(mapper, FakeUserMapper)
        assert user in mapper.deleted
