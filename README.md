# uow-lib

Generic, backend-agnostic implementation of the
[Unit of Work](https://martinfowler.com/eaaCatalog/unitOfWork.html) pattern for Python 3.12+.

The library tracks entity lifecycle, automatically detects mutations, and
coordinates persistence through user-defined data mappers — no ORM required.

## Installation

```bash
pip install uow-lib
```

## Quick start

```python
from dataclasses import dataclass, field
from uow import (
    Connection,
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    ListOf,
    UnitOfWork,
)


# 1. Define your entities
@dataclass
class OrderItem:
    id: int | None
    product: str
    qty: int


@dataclass
class Order:
    id: int | None
    customer: str
    items: list[OrderItem] = field(default_factory=list)


# 2. Implement data mappers (one per entity type)
class OrderMapper:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    async def save(self, entities):
        ...  # INSERT into the database

    async def update(self, entities):
        ...  # UPDATE in the database

    async def delete(self, entities):
        ...  # DELETE from the database


class OrderItemMapper:
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    async def save(self, entities): ...
    async def update(self, entities): ...
    async def delete(self, entities): ...


# 3. Register entity configurations
registry = InstrumentationRegistry()
registry.register(EntityConfig(
    entity_type=Order,
    identity_key=("id",),
    mapper_type=OrderMapper,
    children={"items": ListOf(OrderItem)},
))
registry.register(EntityConfig(
    entity_type=OrderItem,
    identity_key=("id",),
    mapper_type=OrderItemMapper,
    depends_on=[Order],
))

# 4. Use the Unit of Work
async def create_order(conn: Connection) -> None:
    uow = UnitOfWork(conn, registry)

    order = Order(id=None, customer="Alice", items=[
        OrderItem(id=None, product="Widget", qty=3),
    ])
    uow.register_new(order)       # order + items tracked as NEW
    await uow.commit()            # calls OrderMapper.save, then OrderItemMapper.save

async def update_order(conn: Connection, order: Order) -> None:
    uow = UnitOfWork(conn, registry)
    uow.register_clean(order)     # track existing entity

    order.customer = "Bob"        # change detected automatically
    order.items.append(           # new child auto-registered as NEW
        OrderItem(id=None, product="Gadget", qty=1),
    )
    await uow.commit()            # UPDATE order, INSERT new item
```

## Features

### Automatic change tracking

After `register_clean`, the library instruments entity classes to intercept
`__setattr__`. Any mutation to a tracked attribute marks the entity as dirty —
no manual flags needed.

```python
uow.register_clean(order)
order.customer = "Bob"   # automatically detected, will trigger UPDATE on flush
```

### Child relationship types

Describe entity graphs declaratively via `children` in `EntityConfig`:

| Spec                      | Description                                  |
|---------------------------|----------------------------------------------|
| `ListOf(ChildType)`       | One-to-many list, wrapped in `TrackedList`   |
| `SetOf(ChildType)`        | One-to-many set, wrapped in `TrackedSet`     |
| `SingleOf(ChildType)`     | One-to-one reference                         |
| `EmbeddedOf(VOType)`      | Immutable value object (frozen dataclass)    |
| `CollectionOfEmbedded(VOType)` | List of immutable value objects          |

**Entity children** (`ListOf`, `SetOf`, `SingleOf`) are tracked and persisted
individually. Adding a child to a tracked collection registers it as NEW;
removing one marks it as DELETED. Replacing a `SingleOf` reference deletes the
old child and inserts the new one.

**Embedded value objects** (`EmbeddedOf`, `CollectionOfEmbedded`) are not
separate entities. Changes to them mark the *parent* entity as dirty.
`EmbeddedOf` requires a frozen dataclass:

```python
from dataclasses import dataclass
from uow import EmbeddedOf

@dataclass(frozen=True)
class Address:
    street: str
    city: str

registry.register(EntityConfig(
    entity_type=Customer,
    identity_key=("id",),
    mapper_type=CustomerMapper,
    children={"address": EmbeddedOf(Address)},
))
```

### Dirty primitive collections

Plain `list`, `set`, and `dict` attributes that aren't entity children are
automatically wrapped in mutation-aware proxies (`DirtyList`, `DirtySet`,
`DirtyDict`). Mutations mark the parent entity as dirty:

```python
profile.tags.append("new-tag")       # DirtyList  -> parent marked dirty
profile.roles.add("editor")          # DirtySet   -> parent marked dirty
profile.metadata["key"] = "value"    # DirtyDict  -> parent marked dirty
```

### Lazy child materialization

Collections registered via `register_clean` use lazy materialization — children
are not registered with the UoW until the collection is first accessed.
This avoids unnecessary work when loading large entity graphs.

### Dependency-aware flush ordering

Specify `depends_on` in `EntityConfig` to control persistence order.
The library uses topological sort (Kahn's algorithm) to ensure:

- **Inserts**: parents before children (by dependency depth)
- **Deletes**: children before parents (reversed)
- **Updates**: stable registration order

Circular dependencies raise `CyclicDependencyError`.

### Identity map

The built-in `IdentityMap` guarantees at most one in-memory instance per
entity identity `(type, key)`. Attempting to register two different objects
with the same identity raises `DuplicateEntityError`.

### Transactional semantics

| Method     | Behavior                                               |
|------------|--------------------------------------------------------|
| `flush()`  | Detect changes and call mapper operations; rollback on error |
| `commit()` | Flush + `connection.commit()`; rollback on error       |
| `rollback()` | `connection.rollback()` and detach all entities     |

### Backend agnostic

Persistence is defined through two protocols — implement them for any database:

```python
class Connection(Protocol):
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...

class GenericDataMapper[T](Protocol):
    async def save(self, entities: Iterable[T]) -> None: ...
    async def update(self, entities: Iterable[T]) -> None: ...
    async def delete(self, entities: Iterable[T]) -> None: ...
```

Works with asyncpg, aiosqlite, databases, or any async connection that
satisfies the `Connection` protocol.

### Excluding fields from tracking

Use `exclude_from_tracking` to prevent internal attributes (e.g., domain
events) from triggering updates:

```python
EntityConfig(
    entity_type=Aggregate,
    identity_key=("id",),
    mapper_type=AggregateMapper,
    exclude_from_tracking=frozenset({"_events"}),
)
```

## API reference

### Core classes

- **`UnitOfWork(connection, registry)`** — main entry point. Methods:
  `register_new`, `register_clean`, `register_deleted`, `flush`, `commit`,
  `rollback`.
- **`InstrumentationRegistry`** — registry for `EntityConfig` objects.
  Call `register(config)` for each entity type.
- **`EntityConfig`** — declares entity type, identity key, mapper type,
  children, dependencies, and excluded fields.

### Child specs

`ListOf`, `SetOf`, `SingleOf`, `EmbeddedOf`, `CollectionOfEmbedded`

### Collections

`TrackedList`, `TrackedSet` — collection wrappers that fire callbacks on add/remove.

### Protocols

`Connection`, `GenericDataMapper[T]`

### Exceptions

| Exception                 | When                                          |
|---------------------------|-----------------------------------------------|
| `UoWError`                | Base exception                                |
| `UnregisteredEntityError` | Entity type has no registered `EntityConfig`  |
| `DuplicateEntityError`    | Two objects share the same identity            |
| `UntrackedEntityError`    | Operation on entity not tracked by this UoW   |
| `CyclicDependencyError`   | `depends_on` graph contains a cycle           |

## License

MIT
