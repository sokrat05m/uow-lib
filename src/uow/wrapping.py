import functools
from typing import Any, Callable

from uow.children import ChildTracker
from uow.collections import DirtyDict, DirtyList, DirtySet, TrackedList, TrackedSet
from uow.instrumentation import EntityConfig, ListOf, SetOf
from uow.tracking import ChangeTracker, _TRACKER_ATTR

_DIRTY_WRAPPERS: dict[
    type, type[DirtyList[Any]] | type[DirtySet[Any]] | type[DirtyDict[Any, Any]]
] = {
    list: DirtyList,
    set: DirtySet,
    dict: DirtyDict,
}


def _fire_dirty(entity: object, attr_name: str) -> None:
    tracker: ChangeTracker | None = entity.__dict__.get(_TRACKER_ATTR)
    if tracker is not None:
        tracker._dirty_fields.add(attr_name)


class CollectionInstrumentor:
    def __init__(self, children: ChildTracker) -> None:
        self._children = children

    def wrap_eager(self, entity: object, config: EntityConfig) -> None:
        self._wrap_tracked(entity, config, lazy=False)
        self._wrap_dirty(entity, config)

    def wrap_lazy(self, entity: object, config: EntityConfig) -> None:
        self._wrap_tracked(entity, config, lazy=True)
        self._wrap_dirty(entity, config)

    def _wrap_tracked(
        self,
        entity: object,
        config: EntityConfig,
        *,
        lazy: bool,
    ) -> None:
        children = self._children
        for attr_name, child_spec in config.children.items():
            child_value = getattr(entity, attr_name, None)
            if child_value is None:
                continue

            on_add = lambda item, p=entity, cs=child_spec, cfg=config: children.on_added(item, p, cs, cfg)
            on_remove = lambda item: children.on_removed(item)
            on_materialize: Callable[[], None] | None = None
            if lazy:
                on_materialize = lambda e=entity, a=attr_name: children.register_collection_clean(e, a)  # type: ignore[misc]

            if isinstance(child_spec, ListOf) and not isinstance(
                child_value, TrackedList
            ):
                tracked_list = TrackedList(
                    child_value,
                    on_add=on_add,
                    on_remove=on_remove,
                    on_materialize=on_materialize,
                )
                object.__setattr__(entity, attr_name, tracked_list)
            elif isinstance(child_spec, SetOf) and not isinstance(
                child_value, TrackedSet
            ):
                tracked_set = TrackedSet(
                    child_value,
                    on_add=on_add,
                    on_remove=on_remove,
                    on_materialize=on_materialize,
                )
                object.__setattr__(entity, attr_name, tracked_set)

    @staticmethod
    def _wrap_dirty(entity: object, config: EntityConfig) -> None:
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
            wrapper_type = _DIRTY_WRAPPERS.get(type(value))
            if wrapper_type is None:
                continue
            on_change = functools.partial(_fire_dirty, entity, attr_name)
            wrapped = wrapper_type(value, on_change)
            object.__setattr__(entity, attr_name, wrapped)
