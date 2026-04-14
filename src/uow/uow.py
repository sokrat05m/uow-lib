import enum
from collections import defaultdict
from typing import Any, Callable

from uow.collections import (
    DirtyDict,
    DirtyList,
    DirtySet,
    TrackedList,
    TrackedSet,
)
from uow.exceptions import UntrackedEntityError
from uow.flush import OpType, sort_operations
from uow.identity import IdentityMap
from uow.instrumentation import (
    EntityConfig,
    InstrumentationRegistry,
    ListOf,
    SetOf,
    SingleOf,
)
from uow.mapper import Connection, GenericDataMapper
from uow.tracking import ChangeTracker, _TRACKER_ATTR

_DIRTY_WRAPPERS: dict[
    type, type[DirtyList[Any]] | type[DirtySet[Any]] | type[DirtyDict[Any, Any]]
] = {
    list: DirtyList,
    set: DirtySet,
    dict: DirtyDict,
}


class _EntityState(enum.Enum):
    NEW = "new"
    CLEAN = "clean"
    DELETED = "deleted"


class _TrackedEntry:
    __slots__ = ("entity", "state", "tracker", "config", "single_children")

    def __init__(
        self,
        entity: object,
        state: _EntityState,
        tracker: ChangeTracker | None,
        config: EntityConfig,
        single_children: dict[str, object | None],
    ) -> None:
        self.entity = entity
        self.state = state
        self.tracker = tracker
        self.config = config
        self.single_children = single_children


class UnitOfWork:
    def __init__(
        self,
        connection: Connection,
        registry: InstrumentationRegistry,
    ) -> None:
        self._connection = connection
        self._registry = registry
        self._identity_map = IdentityMap()
        self._entries: dict[int, _TrackedEntry] = {}
        self._mappers: dict[type, GenericDataMapper[Any]] = {}

    def register_new(self, entity: object) -> None:
        if id(entity) in self._entries:
            return
        config = self._registry.get(type(entity))
        entry = _TrackedEntry(
            entity,
            _EntityState.NEW,
            tracker=None,
            config=config,
            single_children={},
        )
        self._entries[id(entity)] = entry
        self._wrap_child_collections(entity, config)
        self._wrap_dirty_collections(entity, config)
        self._register_all_children_new(entity, config)

    def register_clean(self, entity: object) -> None:
        if id(entity) in self._entries:
            return
        config = self._registry.get(type(entity))
        identity = self._get_identity(entity, config)

        if not self._is_identity_empty(identity):
            self._identity_map.put(type(entity), identity, entity)

        single_children = self._snapshot_single_children(entity, config)
        tracker = ChangeTracker(entity, config.tracked_attrs)
        entry = _TrackedEntry(
            entity,
            _EntityState.CLEAN,
            tracker=tracker,
            config=config,
            single_children=single_children,
        )
        self._entries[id(entity)] = entry
        tracker.install()
        self._wrap_child_collections_lazy(entity, config)
        self._wrap_dirty_collections(entity, config)
        self._register_single_children_clean(entity, config)

    def register_deleted(self, entity: object) -> None:
        entry = self._entries.get(id(entity))
        if entry is None:
            raise UntrackedEntityError(
                f"{type(entity).__name__} is not tracked by this UoW"
            )
        entry.state = _EntityState.DELETED
        self._mark_children_deleted(entity, entry.config)

    async def flush(self) -> None:
        try:
            await self._flush()
        except Exception:
            await self._connection.rollback()
            self._detach_all()
            raise
        self._post_flush_cleanup()

    async def commit(self) -> None:
        try:
            await self._flush()
            await self._connection.commit()
        except Exception:
            await self._connection.rollback()
            raise
        self._post_flush_cleanup()

    async def rollback(self) -> None:
        await self._connection.rollback()
        self._detach_all()

    async def _flush(self) -> None:
        operations = self._build_operations()
        ordered = sort_operations(self._registry, operations)

        for op_type, entity_type, entities in ordered:
            mapper = self._get_mapper(entity_type)
            if op_type is OpType.INSERT:
                await mapper.save(entities)
            elif op_type is OpType.UPDATE:
                await mapper.update(entities)
            elif op_type is OpType.DELETE:
                await mapper.delete(entities)

    def _get_mapper(self, entity_type: type) -> GenericDataMapper[Any]:
        if entity_type not in self._mappers:
            config = self._registry.get(entity_type)
            self._mappers[entity_type] = config.mapper_type(self._connection)  # type: ignore[call-arg]
        return self._mappers[entity_type]

    def _build_operations(self) -> list[tuple[OpType, type, list[object]]]:
        self._detect_single_replacements()

        groups: dict[tuple[OpType, type], list[object]] = defaultdict(list)

        for entry in self._entries.values():
            entity_type = entry.config.entity_type

            if entry.state is _EntityState.NEW:
                groups[(OpType.INSERT, entity_type)].append(entry.entity)
            elif entry.state is _EntityState.DELETED:
                groups[(OpType.DELETE, entity_type)].append(entry.entity)
            elif entry.state is _EntityState.CLEAN:
                if entry.tracker and entry.tracker.is_dirty:
                    groups[(OpType.UPDATE, entity_type)].append(entry.entity)

        return [(op, et, ents) for (op, et), ents in groups.items()]

    def _detect_single_replacements(self) -> None:
        for entry in list(self._entries.values()):
            if entry.tracker is None or entry.state is _EntityState.DELETED:
                continue

            for attr_name in entry.single_children:
                if attr_name not in entry.tracker.get_dirty_fields():
                    continue

                old_value = entry.single_children[attr_name]
                new_value = getattr(entry.entity, attr_name, None)

                if old_value is not None and id(old_value) in self._entries:
                    self._entries[id(old_value)].state = _EntityState.DELETED

                if new_value is not None and id(new_value) not in self._entries:
                    child_spec = entry.config.children[attr_name]
                    if isinstance(child_spec, SingleOf):
                        self._set_parent_key(entry.entity, new_value, child_spec, entry.config)
                    self.register_new(new_value)

    # ── Collection wrapping ────────────────────────────────────────

    @staticmethod
    def _make_dirty_callback(entity: object, attr_name: str) -> Callable[[], None]:
        def on_change() -> None:
            tracker: ChangeTracker | None = entity.__dict__.get(_TRACKER_ATTR)
            if tracker is not None:
                tracker._dirty_fields.add(attr_name)

        return on_change

    @staticmethod
    def _wrap_as_dirty(
        entity: object,
        attr_name: str,
        value: object,
        on_change: Callable[[], None],
    ) -> bool:
        wrapper_type = _DIRTY_WRAPPERS.get(type(value))
        if wrapper_type is None:
            return False
        wrapped = wrapper_type(value, on_change)  # type: ignore[arg-type]
        object.__setattr__(entity, attr_name, wrapped)
        return True

    def _wrap_child_collections(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue

            if isinstance(child_spec, ListOf) and not isinstance(
                child_value, TrackedList
            ):
                tracked_list = TrackedList(
                    child_value,
                    on_add=lambda item, p=entity, cs=child_spec, cfg=config: self._on_child_added(item, p, cs, cfg),  # type: ignore[misc]
                    on_remove=lambda item: self._on_child_removed(item),
                )
                object.__setattr__(entity, attr_name, tracked_list)

            elif isinstance(child_spec, SetOf) and not isinstance(
                child_value, TrackedSet
            ):
                tracked_set = TrackedSet(
                    child_value,
                    on_add=lambda item, p=entity, cs=child_spec, cfg=config: self._on_child_added(item, p, cs, cfg),  # type: ignore[misc]
                    on_remove=lambda item: self._on_child_removed(item),
                )
                object.__setattr__(entity, attr_name, tracked_set)

    def _wrap_child_collections_lazy(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue

            if isinstance(child_spec, ListOf) and not isinstance(
                child_value, TrackedList
            ):
                tracked_list = TrackedList(
                    child_value,
                    on_add=lambda item, p=entity, cs=child_spec, cfg=config: self._on_child_added(item, p, cs, cfg),  # type: ignore[misc]
                    on_remove=lambda item: self._on_child_removed(item),
                    on_materialize=lambda e=entity, a=attr_name: self._register_collection_children_clean(e, a),  # type: ignore[misc]
                )
                object.__setattr__(entity, attr_name, tracked_list)

            elif isinstance(child_spec, SetOf) and not isinstance(
                child_value, TrackedSet
            ):
                tracked_set = TrackedSet(
                    child_value,
                    on_add=lambda item, p=entity, cs=child_spec, cfg=config: self._on_child_added(item, p, cs, cfg),  # type: ignore[misc]
                    on_remove=lambda item: self._on_child_removed(item),
                    on_materialize=lambda e=entity, a=attr_name: self._register_collection_children_clean(e, a),  # type: ignore[misc]
                )
                object.__setattr__(entity, attr_name, tracked_set)

    def _wrap_dirty_collections(
        self,
        entity: object,
        config: EntityConfig,
    ) -> None:
        entity_collection_attrs = {
            name
            for name, spec in config.children.items()
            if isinstance(spec, (ListOf, SetOf))
        }
        for attr_name in config.tracked_attrs:
            if attr_name in entity_collection_attrs:
                continue
            value = getattr(entity, attr_name, None)
            if value is None:
                continue
            self._wrap_as_dirty(
                entity,
                attr_name,
                value,
                self._make_dirty_callback(entity, attr_name),
            )

    @staticmethod
    def _set_parent_key(
        parent: object,
        child: object,
        child_spec: ListOf | SetOf | SingleOf,
        parent_config: EntityConfig,
    ) -> None:
        if child_spec.parent_key is None:
            return
        parent_id = getattr(parent, parent_config.identity_key[0])
        object.__setattr__(child, child_spec.parent_key, parent_id)

    def _on_child_added(
        self,
        item: object,
        parent: object,
        child_spec: ListOf | SetOf | SingleOf,
        parent_config: EntityConfig,
    ) -> None:
        self._set_parent_key(parent, item, child_spec, parent_config)
        if id(item) not in self._entries:
            self.register_new(item)

    def _on_child_removed(self, item: object) -> None:
        entry = self._entries.get(id(item))
        if entry is None:
            self.register_clean(item)
            entry = self._entries[id(item)]
        entry.state = _EntityState.DELETED

    def _register_all_children_new(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue

            if isinstance(child_spec, SingleOf):
                self._set_parent_key(entity, child_value, child_spec, config)
                self.register_new(child_value)
            elif isinstance(child_spec, (ListOf, SetOf)):
                for child in child_value:
                    self._set_parent_key(entity, child, child_spec, config)
                    self.register_new(child)

    def _register_single_children_clean(
        self, entity: object, config: EntityConfig
    ) -> None:
        for attr_name, child_spec in config.children.items():
            if not isinstance(child_spec, SingleOf):
                continue
            child_value = getattr(entity, attr_name, None)
            if child_value is not None:
                self._set_parent_key(entity, child_value, child_spec, config)
                self.register_clean(child_value)

    def _register_collection_children_clean(self, entity: object, attr_name: str) -> None:
        collection = getattr(entity, attr_name, None)
        if collection is None:
            return
        for child in collection:
            self.register_clean(child)

    def _mark_children_deleted(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue

            if isinstance(child_spec, SingleOf):
                entry = self._entries.get(id(child_value))
                if entry is not None:
                    entry.state = _EntityState.DELETED
            elif isinstance(child_spec, (ListOf, SetOf)):
                for child in child_value:
                    entry = self._entries.get(id(child))
                    if entry is not None:
                        entry.state = _EntityState.DELETED

    # ── Helpers ───────────────────────────────────────────────────

    @staticmethod
    def _snapshot_single_children(
        entity: object,
        config: EntityConfig,
    ) -> dict[str, object | None]:
        result: dict[str, object | None] = {}
        for attr_name, child_spec in config.children.items():
            if isinstance(child_spec, SingleOf):
                result[attr_name] = getattr(entity, attr_name, None)
        return result

    def _get_identity(self, entity: object, config: EntityConfig) -> tuple[object, ...]:
        return tuple(getattr(entity, attr) for attr in config.identity_key)

    @staticmethod
    def _is_identity_empty(identity: tuple[object, ...]) -> bool:
        return all(v is None for v in identity)

    def _post_flush_cleanup(self) -> None:
        to_remove: list[int] = []

        for eid, entry in self._entries.items():
            if entry.state is _EntityState.DELETED:
                if entry.tracker:
                    entry.tracker.uninstall()
                identity = self._get_identity(entry.entity, entry.config)
                if not self._is_identity_empty(identity):
                    self._identity_map.remove(type(entry.entity), identity)
                to_remove.append(eid)
            else:
                entry.state = _EntityState.CLEAN
                entry.single_children = self._snapshot_single_children(
                    entry.entity,
                    entry.config,
                )
                if entry.tracker:
                    entry.tracker.reset()
                else:
                    tracker = ChangeTracker(entry.entity, entry.config.tracked_attrs)
                    tracker.install()
                    entry.tracker = tracker
                    identity = self._get_identity(entry.entity, entry.config)
                    if not self._is_identity_empty(identity):
                        self._identity_map.put(
                            type(entry.entity), identity, entry.entity
                        )
                self._wrap_child_collections_lazy(entry.entity, entry.config)
                self._wrap_dirty_collections(entry.entity, entry.config)

        for eid in to_remove:
            del self._entries[eid]

    def _detach_all(self) -> None:
        for entry in self._entries.values():
            if entry.tracker:
                entry.tracker.uninstall()
        self._entries.clear()
        self._identity_map.clear()
        self._mappers.clear()
