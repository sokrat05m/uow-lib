from collections.abc import Iterable
from typing import Callable

from uow._entry import _EntityState, _TrackedEntry
from uow.instrumentation import EntityConfig, ListOf, SetOf, SingleOf


class ChildTracker:
    def __init__(
        self,
        entries: dict[int, _TrackedEntry],
        register_new: Callable[[object], None],
        register_clean: Callable[[object], None],
    ) -> None:
        self._entries = entries
        self._register_new = register_new
        self._register_clean = register_clean

    @staticmethod
    def set_parent_key(
        parent: object,
        child: object,
        child_spec: ListOf | SetOf | SingleOf,
        parent_config: EntityConfig,
    ) -> None:
        if child_spec.parent_key is None:
            return
        parent_id = getattr(parent, parent_config.identity_key[0])
        object.__setattr__(child, child_spec.parent_key, parent_id)

    def on_added(
        self,
        item: object,
        parent: object,
        child_spec: ListOf | SetOf | SingleOf,
        parent_config: EntityConfig,
    ) -> None:
        self.set_parent_key(parent, item, child_spec, parent_config)
        if id(item) not in self._entries:
            self._register_new(item)

    def on_removed(self, item: object) -> None:
        entry = self._entries.get(id(item))
        if entry is None:
            self._register_clean(item)
            entry = self._entries[id(item)]
        if entry.state is _EntityState.NEW:
            self._discard_new(item)
            return
        entry.state = _EntityState.DELETED

    def _discard_new(self, entity: object) -> None:
        entry = self._entries.get(id(entity))
        if entry is None:
            return

        for attr_name, child_spec in entry.config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue
            if isinstance(child_spec, SingleOf):
                self._discard_new(child_value)
                continue
            if isinstance(child_spec, (ListOf, SetOf)):
                for child in child_value:
                    self._discard_new(child)

        if entry.tracker is not None:
            entry.tracker.uninstall()
        del self._entries[id(entity)]

    def register_all_new(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue
            if isinstance(child_spec, SingleOf):
                self.set_parent_key(entity, child_value, child_spec, config)
                self._register_new(child_value)
            elif isinstance(child_spec, (ListOf, SetOf)):
                for child in child_value:
                    self.set_parent_key(entity, child, child_spec, config)
                    self._register_new(child)

    def register_singles_clean(self, entity: object, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            if not isinstance(child_spec, SingleOf):
                continue
            child_value = getattr(entity, attr_name, None)
            if child_value is not None:
                self.set_parent_key(entity, child_value, child_spec, config)
                self._register_clean(child_value)

    def register_collection_clean(
        self,
        collection: Iterable[object] | None,
    ) -> None:
        if collection is None:
            return
        for child in collection:
            self._register_clean(child)

    def mark_deleted(self, entity: object, config: EntityConfig) -> None:
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

    @staticmethod
    def snapshot_singles(
        entity: object,
        config: EntityConfig,
    ) -> dict[str, object | None]:
        result: dict[str, object | None] = {}
        for attr_name, child_spec in config.children.items():
            if isinstance(child_spec, SingleOf):
                result[attr_name] = getattr(entity, attr_name, None)
        return result

    @staticmethod
    def snapshot_collection_refs(
        entity: object,
        config: EntityConfig,
    ) -> dict[str, Iterable[object] | None]:
        result: dict[str, Iterable[object] | None] = {}
        for attr_name, child_spec in config.children.items():
            if not isinstance(child_spec, (ListOf, SetOf)):
                continue
            result[attr_name] = getattr(entity, attr_name, None)
        return result
