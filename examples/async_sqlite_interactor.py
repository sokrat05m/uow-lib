# Install aiosqlite separately to run this example:
#   uv pip install aiosqlite

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

import aiosqlite

from uow import EntityConfig, InstrumentationRegistry
from uow.asyncio import Connection, GenericDataMapper, UnitOfWork


@dataclass
class Todo:
    id: int | None
    title: str
    is_done: bool = False


class SQLiteConnection(Connection):
    def __init__(self, db: aiosqlite.Connection) -> None:
        self.db = db

    async def commit(self) -> None:
        await self.db.commit()

    async def rollback(self) -> None:
        await self.db.rollback()


class TodoMapper(GenericDataMapper[Todo]):
    def __init__(self, connection: object) -> None:
        self._db = cast("SQLiteConnection", connection).db

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


class TodoGateway:
    def __init__(self, connection: SQLiteConnection) -> None:
        self._db = connection.db

    async def list_todos(self) -> list[Todo]:
        cursor = await self._db.execute(
            "select id, title, is_done from todo order by id",
        )
        rows = await cursor.fetchall()
        await cursor.close()
        return [
            Todo(id=row[0], title=row[1], is_done=bool(row[2])) for row in rows
        ]


class CreateTodoInteractor:
    def __init__(
        self,
        gateway: TodoGateway,
        uow: UnitOfWork,
    ) -> None:
        self._gateway = gateway
        self._uow = uow

    async def __call__(self, title: str) -> list[Todo]:
        todo = Todo(id=None, title=title)
        self._uow.register_new(todo)
        await self._uow.commit()
        return await self._gateway.list_todos()


def build_registry() -> InstrumentationRegistry:
    registry = InstrumentationRegistry()
    registry.register(
        EntityConfig(
            entity_type=Todo,
            identity_key=("id",),
            mapper_type=TodoMapper,
            children={},
            depends_on=[],
        ),
    )
    return registry


async def init_schema(db: aiosqlite.Connection) -> None:
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


async def main() -> None:
    async with aiosqlite.connect(":memory:") as db:
        await init_schema(db)

        connection = SQLiteConnection(db)
        registry = build_registry()
        uow = UnitOfWork(connection, registry)
        gateway = TodoGateway(connection)
        interactor = CreateTodoInteractor(gateway, uow)

        todos = await interactor("write async sqlite MRE")
        for todo in todos:
            print(todo)  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
