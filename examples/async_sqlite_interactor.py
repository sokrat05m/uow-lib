# Install aiosqlite separately to run this example:
#   uv pip install aiosqlite

import asyncio
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import cast
from uuid import UUID, uuid4

import aiosqlite

from uow import EntityConfig, InstrumentationRegistry, ListOf
from uow.asyncio import Connection, GenericDataMapper, UnitOfWork


@dataclass
class TodoNote:
    body: str
    id: UUID = field(default_factory=uuid4)
    todo_id: UUID | None = None


@dataclass
class Todo:
    title: str
    id: UUID = field(default_factory=uuid4)
    is_done: bool = False
    notes: list[TodoNote] = field(default_factory=list)


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
                "insert into todo (id, title, is_done) values (?, ?, ?)",
                (str(entity.id), entity.title, int(entity.is_done)),
            )
            await cursor.close()

    async def update(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            await self._db.execute(
                "update todo set title = ?, is_done = ? where id = ?",
                (entity.title, int(entity.is_done), str(entity.id)),
            )

    async def delete(self, entities: Iterable[Todo]) -> None:
        for entity in entities:
            await self._db.execute(
                "delete from todo where id = ?",
                (str(entity.id),),
            )


class TodoNoteMapper(GenericDataMapper[TodoNote]):
    def __init__(self, connection: object) -> None:
        self._db = cast("SQLiteConnection", connection).db

    async def save(self, entities: Iterable[TodoNote]) -> None:
        for entity in entities:
            cursor = await self._db.execute(
                "insert into todo_note (id, todo_id, body) values (?, ?, ?)",
                (
                    str(entity.id),
                    str(entity.todo_id) if entity.todo_id else None,
                    entity.body,
                ),
            )
            await cursor.close()

    async def update(self, entities: Iterable[TodoNote]) -> None:
        for entity in entities:
            await self._db.execute(
                "update todo_note set todo_id = ?, body = ? where id = ?",
                (
                    str(entity.todo_id) if entity.todo_id else None,
                    entity.body,
                    str(entity.id),
                ),
            )

    async def delete(self, entities: Iterable[TodoNote]) -> None:
        for entity in entities:
            await self._db.execute(
                "delete from todo_note where id = ?",
                (str(entity.id),),
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
        todos: list[Todo] = []
        for row in rows:
            todo = Todo(id=UUID(row[0]), title=row[1], is_done=bool(row[2]))
            notes_cursor = await self._db.execute(
                """
                select id, todo_id, body
                from todo_note
                where todo_id = ?
                order by id
                """,
                (str(todo.id),),
            )
            todo.notes = [
                TodoNote(id=UUID(note[0]), todo_id=UUID(note[1]), body=note[2])
                for note in await notes_cursor.fetchall()
            ]
            await notes_cursor.close()
            todos.append(todo)
        return todos


class CreateTodoInteractor:
    def __init__(
        self,
        gateway: TodoGateway,
        uow: UnitOfWork,
    ) -> None:
        self._gateway = gateway
        self._uow = uow

    async def __call__(self, title: str, note_body: str) -> list[Todo]:
        todo = Todo(
            title=title,
            notes=[TodoNote(body=note_body)],
        )
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
            children={"notes": ListOf(TodoNote, parent_key="todo_id")},
            depends_on=[],
        ),
    )
    registry.register(
        EntityConfig(
            entity_type=TodoNote,
            identity_key=("id",),
            mapper_type=TodoNoteMapper,
            children={},
            depends_on=[Todo],
        ),
    )
    return registry


async def init_schema(db: aiosqlite.Connection) -> None:
    await db.execute(
        """
        create table todo (
            id text primary key,
            title text not null,
            is_done integer not null
        )
        """,
    )
    await db.execute(
        """
        create table todo_note (
            id text primary key,
            todo_id text not null references todo(id) on delete cascade,
            body text not null
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

        todos = await interactor(
            "write async sqlite MRE",
            "keep the example tiny",
        )
        for todo in todos:
            print(todo)  # noqa: T201


if __name__ == "__main__":
    asyncio.run(main())
