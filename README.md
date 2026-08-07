# uow-lib

Generic, backend-agnostic implementation of the
[Unit of Work](https://martinfowler.com/eaaCatalog/unitOfWork.html) pattern
for Python 3.12+.

The library tracks entity lifecycle, detects mutations automatically, and
coordinates persistence through user-defined data mappers. It does not require
an ORM and works with both dataclasses and regular Python classes.

## Installation

```bash
pip install uow-lib
```

## Quick Start

```python
from dataclasses import dataclass, field
from collections.abc import Iterable

from uow import (
    Connection,
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    ListOf,
    UnitOfWork,
)


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


class OrderMapper(GenericDataMapper[Order]):
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    async def save(self, entities: Iterable[Order]) -> None: ...

    async def update(self, entities: Iterable[Order]) -> None: ...

    async def delete(self, entities: Iterable[Order]) -> None: ...


class OrderItemMapper(GenericDataMapper[OrderItem]):
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    async def save(self, entities: Iterable[OrderItem]) -> None: ...

    async def update(self, entities: Iterable[OrderItem]) -> None: ...

    async def delete(self, entities: Iterable[OrderItem]) -> None: ...


registry = InstrumentationRegistry()
registry.register(
    EntityConfig(
        entity_type=Order,
        identity_key=("id",),
        mapper_type=OrderMapper,
        children={"items": ListOf(OrderItem)},
    )
)
registry.register(
    EntityConfig(
        entity_type=OrderItem,
        identity_key=("id",),
        mapper_type=OrderItemMapper,
        depends_on=[Order],
    )
)


async def create_order(conn: Connection) -> None:
    uow = UnitOfWork(conn, registry)

    order = Order(
        id=None,
        customer="Alice",
        items=[OrderItem(id=None, product="Widget", qty=3)],
    )
    uow.register_new(order)
    await uow.commit()


async def update_order(conn: Connection, order: Order) -> None:
    uow = UnitOfWork(conn, registry)
    uow.register_clean(order)

    order.customer = "Bob"
    order.items.append(OrderItem(id=None, product="Gadget", qty=1))

    await uow.commit()
```

## Sync API

For synchronous database drivers, import the sync protocols and Unit of Work
from `uow.sync`.

```python
from collections.abc import Iterable

from uow import EntityConfig, InstrumentationRegistry
from uow.sync import Connection, GenericDataMapper, UnitOfWork


class OrderMapper(GenericDataMapper[Order]):
    def __init__(self, conn: Connection) -> None:
        self.conn = conn

    def save(self, entities: Iterable[Order]) -> None: ...

    def update(self, entities: Iterable[Order]) -> None: ...

    def delete(self, entities: Iterable[Order]) -> None: ...


registry = InstrumentationRegistry()
registry.register(
    EntityConfig(
        entity_type=Order,
        identity_key=("id",),
        mapper_type=OrderMapper,
    )
)


def create_order(conn: Connection) -> None:
    uow = UnitOfWork(conn, registry)

    order = Order(id=None, customer="Alice")
    uow.register_new(order)
    uow.commit()
```

The sync `UnitOfWork` exposes the same lifecycle methods as the async one:

| Method | Behavior |
|--------|----------|
| `flush()` | Detects changes and calls sync mapper operations |
| `commit()` | `flush()` plus `connection.commit()` |
| `rollback()` | Calls `connection.rollback()` and detaches all tracked state |

## Core Concepts

### Registering entities

- `register_new(entity)` marks an entity for insert.
- `register_clean(entity)` starts tracking an already-persisted entity.
- `register_deleted(entity)` marks a tracked entity for delete.

New child entities discovered through configured relationships are registered
automatically.

### Automatic change tracking

After `register_clean`, the library instruments the entity class and tracks
assignments to configured attributes through `__setattr__`.

```python
uow.register_clean(order)
order.customer = "Bob"  # UPDATE will be emitted on flush
```

This works for:

- dataclasses
- regular classes with attributes assigned in `__init__`
- inherited attributes on regular classes
- private attributes such as `_name`

### Child relationship types

Describe entity graphs declaratively in `EntityConfig.children`:

| Spec | Description |
|------|-------------|
| `ListOf(ChildType, parent_key=None)` | One-to-many list, wrapped in `TrackedList` |
| `SetOf(ChildType, parent_key=None)` | One-to-many set, wrapped in `TrackedSet` |
| `SingleOf(ChildType, parent_key=None)` | One-to-one child entity |
| `EmbeddedOf(ValueObjectType)` | Single embedded value object |
| `CollectionOfEmbedded(ValueObjectType)` | Collection of embedded value objects |

Entity children (`ListOf`, `SetOf`, `SingleOf`) are persisted individually.

- Adding a child registers it as `NEW`.
- Removing an existing child marks it as `DELETED`.
- Replacing a `SingleOf` deletes the old child and inserts the new one.
- Replacing a whole `ListOf` or `SetOf` collection performs the same diff:
  removed children become `DELETED`, new children become `NEW`.

### Parent key propagation

`ListOf`, `SetOf`, and `SingleOf` accept `parent_key` to copy the parent
identity onto child entities automatically.

```python
registry.register(
    EntityConfig(
        entity_type=Post,
        identity_key=("id",),
        mapper_type=PostMapper,
        children={
            "comments": ListOf(Comment, parent_key="post_id"),
            "detail": SingleOf(PostDetail, parent_key="post_id"),
        },
    )
)
```

`parent_key` is applied when:

- registering a new aggregate
- registering a clean aggregate with `SingleOf` children
- appending/inserting/extending/adding children to tracked collections
- replacing a `SingleOf`, `ListOf`, or `SetOf` relationship

Current limitation: `parent_key` copies only the first field from the parent's
`identity_key`. If the parent identity is composite, automatic propagation is
not enough on its own.

### Embedded value objects

`EmbeddedOf` and `CollectionOfEmbedded` treat values as part of the parent
entity. They are not tracked or persisted as separate entities.

- replacing an embedded value marks the parent dirty
- mutating an embedded collection marks the parent dirty
- embedded value object types must be frozen dataclasses

```python
from dataclasses import dataclass
from uow import CollectionOfEmbedded, EmbeddedOf


@dataclass(frozen=True)
class Address:
    street: str
    city: str


registry.register(
    EntityConfig(
        entity_type=Customer,
        identity_key=("id",),
        mapper_type=CustomerMapper,
        children={
            "address": EmbeddedOf(Address),
            "previous_addresses": CollectionOfEmbedded(Address),
        },
    )
)
```

### Dirty primitive collections

Plain `list`, `set`, and `dict` attributes that are not entity relationships
are wrapped in mutation-aware proxies.

```python
profile.tags.append("new-tag")
profile.roles.add("editor")
profile.metadata["key"] = "value"
```

Those mutations mark the parent entity dirty and result in `UPDATE`.

### Lazy child materialization

Collections configured with `ListOf` and `SetOf` are materialized lazily when
an entity is registered with `register_clean`.

- the collection is wrapped immediately
- children inside it are registered with the UoW only on first access or first
  collection mutation

This avoids eagerly traversing large loaded graphs. `SingleOf` children are not
lazy; they are registered during `register_clean`.

### Identity map

The built-in identity map guarantees at most one tracked instance per entity
identity `(type, key)`.

- registering two different tracked objects with the same non-empty identity
  raises `DuplicateEntityError`
- all-`None` identities are treated as empty and are not inserted into the
  identity map until after a successful flush/commit cycle
- entity type is part of the key, so different entity classes may reuse the
  same underlying ID value safely

### Dependency-aware flush ordering

Use `depends_on` in `EntityConfig` to express cross-entity ordering.

The library computes dependency depth and orders operations as follows:

- `INSERT`: parents before dependents
- `UPDATE`: shallower dependencies before deeper ones
- `DELETE`: dependents before parents

Within the same dependency depth, the original first-seen order is preserved.
Circular dependency graphs raise `CyclicDependencyError`.

### Transactional semantics

| Method | Behavior |
|--------|----------|
| `flush()` | Detects changes, calls mapper operations, and leaves the connection open |
| `commit()` | `flush()` plus `connection.commit()` |
| `rollback()` | Calls `connection.rollback()` and detaches all tracked state |

If `flush()` or `commit()` raises for any reason, the Unit of Work:

- calls `connection.rollback()`
- detaches all tracked entities
- clears installed mappers and identity-map state

After such a failure, entities must be registered again before reuse.

### Backend agnostic

Persistence is defined through two protocols:

```python
from collections.abc import Iterable
from typing import Protocol


class Connection(Protocol):
    async def commit(self) -> None: ...
    async def rollback(self) -> None: ...


class GenericDataMapper[T](Protocol):
    async def save(self, entities: Iterable[T]) -> None: ...
    async def update(self, entities: Iterable[T]) -> None: ...
    async def delete(self, entities: Iterable[T]) -> None: ...
```

Any async database adapter can be used as long as it satisfies those protocols.
For synchronous adapters, use the matching protocols from `uow.sync`; their
methods have the same names but are regular `def` methods instead of
`async def`.

### Excluding fields from tracking

Use `exclude_from_tracking` for attributes that should never trigger updates,
for example domain-event buffers or other internal state.

```python
EntityConfig(
    entity_type=Aggregate,
    identity_key=("id",),
    mapper_type=AggregateMapper,
    exclude_from_tracking=frozenset({"_events"}),
)
```

## API Reference

### Main classes

- `UnitOfWork(connection, registry)`
- `InstrumentationRegistry`
- `EntityConfig`

### Relationship specs

- `ListOf`
- `SetOf`
- `SingleOf`
- `EmbeddedOf`
- `CollectionOfEmbedded`

### Collection wrappers

- `TrackedList`
- `TrackedSet`

### Protocols

- `Connection`
- `GenericDataMapper[T]`

### Exceptions

| Exception | Meaning |
|-----------|---------|
| `UoWError` | Base exception |
| `UnregisteredEntityError` | No `EntityConfig` registered for entity type |
| `DuplicateEntityError` | Another tracked object already uses the same identity |
| `UntrackedEntityError` | Operation requires an entity tracked by this Unit of Work |
| `CyclicDependencyError` | `depends_on` contains a cycle |

## Development

Run checks locally:

```bash
pytest -q
python -m mypy src tests
```

## Limitations

- `parent_key` copies only the first field of the parent `identity_key`
- entity child relationships support `list`, `set`, and single references
- embedded value objects must be frozen dataclasses
- change tracking is assignment-based; if custom descriptors or metaclass tricks
  bypass normal attribute writes, they may bypass tracking as well

## License

MIT
