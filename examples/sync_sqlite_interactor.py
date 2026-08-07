import sqlite3
from collections.abc import Iterable
from dataclasses import dataclass
from typing import cast

from uow import EntityConfig, InstrumentationRegistry
from uow.sync import Connection, GenericDataMapper, UnitOfWork


@dataclass
class Todo:
    id: int | None
    title: str
    is_done: bool = False


class SQLiteConnection(Connection):
    def __init__(self, db: sqlite3.Connection) -> None:
        self.db = db

    def commit(self) -> None:
        self.db.commit()

    def rollback(self) -> None:
        self.db.rollback()


class TodoMapper(GenericDataMapper[Todo]):
    def __init__(self, connection: object) -> None:
        self._db = cast("SQLiteConnection", connection).db

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
            self._db.execute(
                "delete from todo where id = ?",
                (entity.id,),
            )


class TodoGateway:
    def __init__(self, connection: SQLiteConnection) -> None:
        self._db = connection.db

    def list_todos(self) -> list[Todo]:
        cursor = self._db.execute(
            "select id, title, is_done from todo order by id",
        )
        rows = cursor.fetchall()
        cursor.close()
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

    def __call__(self, title: str) -> list[Todo]:
        todo = Todo(id=None, title=title)
        self._uow.register_new(todo)
        self._uow.commit()
        return self._gateway.list_todos()


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


def init_schema(db: sqlite3.Connection) -> None:
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


def main() -> None:
    with sqlite3.connect(":memory:") as db:
        init_schema(db)

        connection = SQLiteConnection(db)
        registry = build_registry()
        uow = UnitOfWork(connection, registry)
        gateway = TodoGateway(connection)
        interactor = CreateTodoInteractor(gateway, uow)

        todos = interactor("write sync sqlite MRE")
        for todo in todos:
            print(todo)  # noqa: T201


if __name__ == "__main__":
    main()
