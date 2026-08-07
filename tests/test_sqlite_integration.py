import sqlite3
from collections.abc import AsyncIterator, Iterable, Iterator
from dataclasses import dataclass
from typing import cast

import aiosqlite
import pytest

from uow import EntityConfig, InstrumentationRegistry
from uow.asyncio import (
    Connection as AsyncConnection,
    GenericDataMapper as AsyncGenericDataMapper,
    UnitOfWork as AsyncUnitOfWork,
)
from uow.sync import (
    Connection as SyncConnection,
    GenericDataMapper as SyncGenericDataMapper,
    UnitOfWork as SyncUnitOfWork,
)


@dataclass
class Todo:
    id: int | None
    title: str
    is_done: bool = False


class SyncSQLiteConnection(SyncConnection):
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()


class SyncTodoMapper(SyncGenericDataMapper[Todo]):
    def __init__(self, connection: object) -> None:
        self._db = cast("SyncSQLiteConnection", connection).db

    def save(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            cursor = self._db.execute(
                "insert into todo (title, is_done) values (?, ?)",
                (entity.title, int(entity.is_done)),
            )
            entity.id = cursor.lastrowid
            cursor.close()

    def update(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            self._db.execute(
                "update todo set title = ?, is_done = ? where id = ?",
                (entity.title, int(entity.is_done), entity.id),
            )

    def delete(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            self._db.execute("delete from todo where id = ?", (entity.id,))


class FailingSyncTodoMapper(SyncTodoMapper):
    def update(self, entities: Iterable[Todo]) -> None:
        super().update(entities)
        raise RuntimeError("sync mapper failed")


class AsyncSQLiteConnection(AsyncConnection):
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db

    async def commit(self) -> None:
        await self.db.commit()

    async def rollback(self) -> None:
        await self.db.rollback()


class AsyncTodoMapper(AsyncGenericDataMapper[Todo]):
    def __init__(self, connection: object) -> None:
        self._db = cast("AsyncSQLiteConnection", connection).db

    async def save(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            cursor = await self._db.execute(
                "insert into todo (title, is_done) values (?, ?)",
                (entity.title, int(entity.is_done)),
            )
            entity.id = cursor.lastrowid
            await cursor.close()

    async def update(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            await self._db.execute(
                "update todo set title = ?, is_done = ? where id = ?",
                (entity.title, int(entity.is_done), entity.id),
            )

    async def delete(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            await self._db.execute(
                "delete from todo where id = ?",
                (entity.id,),
            )


class FailingAsyncTodoMapper(AsyncTodoMapper):
    async def update(self, entities: Iterable[Todo]) -> None:
        await super().update(entities)
        raise RuntimeError("async mapper failed")


def build_registry(
    mapper_type: type[SyncGenericDataMapper[Todo]]
    | type[AsyncGenericDataMapper[Todo]],
) -> InstrumentationRegistry:
    registry = InstrumentationRegistry()
    registry.register(
        EntityConfig(
            entity_type=Todo,
            identity_key=("id",),
            mapper_type=mapper_type,
            children={},
            depends_on=[],
        ),
    )
    return registry


def init_sync_schema(db: sqlite3.Connection) -> None:
    db.execute(
        """
        create table todo (
            id integer primary key autoincrement,
            title text not null,
            is_done integer not null
        )
        """,
    )
    db.commit()


def fetch_sync_todos(db: sqlite3.Connection) -> list[Todo]:
    cursor = db.execute("select id, title, is_done from todo order by id")
    rows = cursor.fetchall()
    cursor.close()
    return [
        Todo(id=row[0], title=row[1], is_done=bool(row[2])) for row in rows
    ]


async def init_async_schema(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        create table todo (
            id integer primary key autoincrement,
            title text not null,
            is_done integer not null
        )
        """,
    )
    await db.commit()


async def fetch_async_todos(db: aiosqlite.Connection) -> list[Todo]:
    cursor = await db.execute(
        "select id, title, is_done from todo order by id",
    )
    rows = await cursor.fetchall()
    await cursor.close()
    return [
        Todo(id=row[0], title=row[1], is_done=bool(row[2])) for row in rows
    ]


@pytest.fixture
def sync_db() -> Iterator[sqlite3.Connection]:
    with sqlite3.connect(":memory:") as db:
        init_sync_schema(db)
        yield db


@pytest.fixture
def sync_connection(sync_db: sqlite3.Connection) -> SyncSQLiteConnection:
    return SyncSQLiteConnection(sync_db)


@pytest.fixture
def sync_uow(sync_connection: SyncSQLiteConnection) -> SyncUnitOfWork:
    return SyncUnitOfWork(sync_connection, build_registry(SyncTodoMapper))


@pytest.fixture
def failing_sync_uow(
    sync_connection: SyncSQLiteConnection,
) -> SyncUnitOfWork:
    return SyncUnitOfWork(
        sync_connection,
        build_registry(FailingSyncTodoMapper),
    )


@pytest.fixture
async def async_db() -> AsyncIterator[aiosqlite.Connection]:
    async with aiosqlite.connect(":memory:") as db:
        await init_async_schema(db)
        yield db


@pytest.fixture
def async_connection(async_db: aiosqlite.Connection) -> AsyncSQLiteConnection:
    return AsyncSQLiteConnection(async_db)


@pytest.fixture
def async_uow(async_connection: AsyncSQLiteConnection) -> AsyncUnitOfWork:
    return AsyncUnitOfWork(async_connection, build_registry(AsyncTodoMapper))


@pytest.fixture
def failing_async_uow(
    async_connection: AsyncSQLiteConnection,
) -> AsyncUnitOfWork:
    return AsyncUnitOfWork(
        async_connection,
        build_registry(FailingAsyncTodoMapper),
    )


def test_sync_sqlite_commit_persists_insert_update_and_delete(
    sync_db: sqlite3.Connection,
    sync_uow: SyncUnitOfWork,
) -> None:
    todo = Todo(id=None, title="write sqlite integration test")
    sync_uow.register_new(todo)
    sync_uow.commit()

    assert todo.id == 1
    assert fetch_sync_todos(sync_db) == [todo]

    sync_uow.register_clean(todo)
    todo.title = "ship sqlite integration test"
    todo.is_done = True
    sync_uow.commit()

    assert fetch_sync_todos(sync_db) == [todo]

    sync_uow.register_deleted(todo)
    sync_uow.commit()

    assert fetch_sync_todos(sync_db) == []


def test_sync_sqlite_commit_rolls_back_failed_mapper_update(
    sync_db: sqlite3.Connection,
    failing_sync_uow: SyncUnitOfWork,
) -> None:
    sync_db.execute(
        "insert into todo (title, is_done) values (?, ?)",
        ("kept by rollback", 0),
    )
    sync_db.commit()
    todo = Todo(id=1, title="kept by rollback")

    failing_sync_uow.register_clean(todo)
    todo.title = "rolled back"

    with pytest.raises(RuntimeError, match="sync mapper failed"):
        failing_sync_uow.commit()

    assert fetch_sync_todos(sync_db) == [Todo(id=1, title="kept by rollback")]
    assert failing_sync_uow._entries == {}
    assert failing_sync_uow._mappers == {}


@pytest.mark.asyncio
async def test_async_sqlite_commit_persists_insert_update_and_delete(
    async_db: aiosqlite.Connection,
    async_uow: AsyncUnitOfWork,
) -> None:
    todo = Todo(id=None, title="write aiosqlite integration test")
    async_uow.register_new(todo)
    await async_uow.commit()

    assert todo.id == 1
    assert await fetch_async_todos(async_db) == [todo]

    async_uow.register_clean(todo)
    todo.title = "ship aiosqlite integration test"
    todo.is_done = True
    await async_uow.commit()

    assert await fetch_async_todos(async_db) == [todo]

    async_uow.register_deleted(todo)
    await async_uow.commit()

    assert await fetch_async_todos(async_db) == []


@pytest.mark.asyncio
async def test_async_sqlite_commit_rolls_back_failed_mapper_update(
    async_db: aiosqlite.Connection,
    failing_async_uow: AsyncUnitOfWork,
) -> None:
    await async_db.execute(
        "insert into todo (title, is_done) values (?, ?)",
        ("kept by rollback", 0),
    )
    await async_db.commit()
    todo = Todo(id=1, title="kept by rollback")

    failing_async_uow.register_clean(todo)
    todo.title = "rolled back"

    with pytest.raises(RuntimeError, match="async mapper failed"):
        await failing_async_uow.commit()

    assert await fetch_async_todos(async_db) == [
        Todo(id=1, title="kept by rollback"),
    ]
    assert failing_async_uow._entries == {}
    assert failing_async_uow._mappers == {}
