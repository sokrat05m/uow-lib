from collections import defaultdict
from typing import Any

from uow._entry import _EntityState, _TrackedEntry
from uow.children import ChildTracker
from uow.exceptions import UntrackedEntityError
from uow.flush import OpType, sort_operations
from uow.identity import IdentityMap
from uow.instrumentation import EntityConfig, InstrumentationRegistry, SingleOf
from uow.mapper import Connection, GenericDataMapper
from uow.tracking import ChangeTracker
from uow.wrapping import CollectionInstrumentor


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
        self._children = ChildTracker(
            self._entries,
            self.register_new,
            self.register_clean,
        )
        self._instrumentor = CollectionInstrumentor(self._children)

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
        self._instrumentor.wrap_eager(entity, config)
        self._children.register_all_new(entity, config)

    def register_clean(self, entity: object) -> None:
        if id(entity) in self._entries:
            return
        config = self._registry.get(type(entity))
        identity = self._get_identity(entity, config)

        if not self._is_identity_empty(identity):
            self._identity_map.put(type(entity), identity, entity)

        single_children = ChildTracker.snapshot_singles(entity, config)
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
        self._instrumentor.wrap_lazy(entity, config)
        self._children.register_singles_clean(entity, config)

    def register_deleted(self, entity: object) -> None:
        entry = self._entries.get(id(entity))
        if entry is None:
            raise UntrackedEntityError(
                f"{type(entity).__name__} is not tracked by this UoW"
            )
        entry.state = _EntityState.DELETED
        self._children.mark_deleted(entity, entry.config)

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

    # ── Flush internals ──────────────────────────────────────────

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
                        ChildTracker.set_parent_key(
                            entry.entity, new_value, child_spec, entry.config
                        )
                    self.register_new(new_value)

    # ── Helpers ──────────────────────────────────────────────────

    @staticmethod
    def _get_identity(entity: object, config: EntityConfig) -> tuple[object, ...]:
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
                entry.single_children = ChildTracker.snapshot_singles(
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
                self._instrumentor.wrap_lazy(entry.entity, entry.config)

        for eid in to_remove:
            del self._entries[eid]

    def _detach_all(self) -> None:
        for entry in self._entries.values():
            if entry.tracker:
                entry.tracker.uninstall()
        self._entries.clear()
        self._identity_map.clear()
        self._mappers.clear()
