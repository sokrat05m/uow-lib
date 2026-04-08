from typing import Any, Iterable, Callable, Mapping, Protocol, SupportsIndex


class HasMaterialize(Protocol):
    _on_materialize: Callable[[], None] | None


def ensure_materialized(self: HasMaterialize) -> None:
    cb = self._on_materialize
    if cb is not None:
        self._on_materialize = None
        cb()


class DirtyList[T](list[T]):
    def __init__(
        self,
        initial: Iterable[T],
        on_change: Callable[[], None],
    ) -> None:
        super().__init__(initial)
        self._on_change = on_change

    def append(self, item: T) -> None:
        super().append(item)
        self._on_change()

    def extend(self, items: Iterable[T]) -> None:
        super().extend(items)
        self._on_change()

    def insert(self, index: SupportsIndex, item: T) -> None:
        super().insert(index, item)
        self._on_change()

    def remove(self, item: T) -> None:
        super().remove(item)
        self._on_change()

    def pop(self, index: SupportsIndex = -1) -> T:
        item = super().pop(index)
        self._on_change()
        return item

    def clear(self) -> None:
        super().clear()
        self._on_change()

    def __setitem__(self, index: SupportsIndex | slice, value: Any) -> None:
        super().__setitem__(index, value)
        self._on_change()

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        super().__delitem__(index)
        self._on_change()

    def __iadd__(self, other: Iterable[T]) -> "DirtyList[T]":  # type: ignore[override, misc]
        super().__iadd__(other)
        self._on_change()
        return self


class DirtySet[T](set[T]):
    def __init__(
        self,
        initial: Iterable[T],
        on_change: Callable[[], None],
    ) -> None:
        super().__init__(initial)
        self._on_change = on_change

    def add(self, item: T) -> None:
        super().add(item)
        self._on_change()

    def discard(self, item: T) -> None:
        if item in self:
            super().discard(item)
            self._on_change()

    def remove(self, item: T) -> None:
        super().remove(item)
        self._on_change()

    def pop(self) -> T:
        item = super().pop()
        self._on_change()
        return item

    def clear(self) -> None:
        super().clear()
        self._on_change()

    def __ior__(self, other: Iterable[T]) -> "DirtySet[T]":  # type: ignore[override, misc]
        for item in other:
            super().add(item)
        self._on_change()
        return self

    def __isub__(self, other: Iterable[T]) -> "DirtySet[T]":  # type: ignore[override, misc]
        for item in other:
            super().discard(item)
        self._on_change()
        return self


class DirtyDict[K, V](dict[K, V]):
    def __init__(
        self,
        initial: Mapping[K, V] | Iterable[tuple[K, V]],
        on_change: Callable[[], None],
    ) -> None:
        super().__init__(initial)
        self._on_change = on_change

    def __setitem__(self, key: K, value: V) -> None:
        super().__setitem__(key, value)
        self._on_change()

    def __delitem__(self, key: K) -> None:
        super().__delitem__(key)
        self._on_change()

    def pop(self, key: K, *args: Any) -> V:
        result: V = super().pop(key, *args)
        self._on_change()
        return result

    def update(self, m: Mapping[K, V] | Iterable[tuple[K, V]] = (), **kwargs: Any) -> None:  # type: ignore[override]
        super().update(m, **kwargs)
        self._on_change()

    def setdefault(self, key: K, default: V = None) -> V:  # type: ignore[assignment]
        if key not in self:
            super().__setitem__(key, default)
            self._on_change()
            return default
        return self[key]

    def clear(self) -> None:
        super().clear()
        self._on_change()


class TrackedList[T](list[T]):
    def __init__(
        self,
        initial: Iterable[T],
        on_add: Callable[[T], None],
        on_remove: Callable[[T], None],
        on_materialize: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(initial)
        self._on_add = on_add
        self._on_remove = on_remove
        self._on_materialize = on_materialize

    def __getitem__(self, index: SupportsIndex | slice) -> Any:
        ensure_materialized(self)
        return super().__getitem__(index)

    def __iter__(self) -> Any:
        ensure_materialized(self)
        return super().__iter__()

    def append(self, item: T) -> None:
        ensure_materialized(self)
        super().append(item)
        self._on_add(item)

    def extend(self, items: Iterable[T]) -> None:
        ensure_materialized(self)
        items_list = list(items)
        super().extend(items_list)
        for item in items_list:
            self._on_add(item)

    def insert(self, index: SupportsIndex, item: T) -> None:
        ensure_materialized(self)
        super().insert(index, item)
        self._on_add(item)

    def remove(self, item: T) -> None:
        ensure_materialized(self)
        super().remove(item)
        self._on_remove(item)

    def pop(self, index: SupportsIndex = -1) -> T:
        ensure_materialized(self)
        item = super().pop(index)
        self._on_remove(item)
        return item

    def clear(self) -> None:
        ensure_materialized(self)
        old = list(self)
        super().clear()
        for item in old:
            self._on_remove(item)

    def __setitem__(self, index: SupportsIndex | slice, value: Any) -> None:
        ensure_materialized(self)
        if isinstance(index, slice):
            old_items = list.__getitem__(self, index)
            super().__setitem__(index, value)
            for old in old_items:
                self._on_remove(old)
            for new in value:
                self._on_add(new)
        else:
            old = list.__getitem__(self, index)
            super().__setitem__(index, value)
            self._on_remove(old)
            self._on_add(value)

    def __delitem__(self, index: SupportsIndex | slice) -> None:
        ensure_materialized(self)
        if isinstance(index, slice):
            old_items = list.__getitem__(self, index)
            super().__delitem__(index)
            for old in old_items:
                self._on_remove(old)
        else:
            old = list.__getitem__(self, index)
            super().__delitem__(index)
            self._on_remove(old)

    def __iadd__(self, other: Iterable[T]) -> "TrackedList[T]":  # type: ignore[override, misc]
        ensure_materialized(self)
        items = list(other)
        super().__iadd__(items)
        for item in items:
            self._on_add(item)
        return self


class TrackedSet[T](set[T]):
    def __init__(
        self,
        initial: Iterable[T],
        on_add: Callable[[T], None],
        on_remove: Callable[[T], None],
        on_materialize: Callable[[], None] | None = None,
    ) -> None:
        super().__init__(initial)
        self._on_add = on_add
        self._on_remove = on_remove
        self._on_materialize = on_materialize

    def __iter__(self) -> Any:
        ensure_materialized(self)
        return super().__iter__()

    def add(self, item: T) -> None:
        ensure_materialized(self)
        if item not in self:
            super().add(item)
            self._on_add(item)

    def discard(self, item: T) -> None:
        ensure_materialized(self)
        if item in self:
            super().discard(item)
            self._on_remove(item)

    def remove(self, item: T) -> None:
        ensure_materialized(self)
        super().remove(item)
        self._on_remove(item)

    def pop(self) -> T:
        ensure_materialized(self)
        item = super().pop()
        self._on_remove(item)
        return item

    def clear(self) -> None:
        ensure_materialized(self)
        old = set(self)
        super().clear()
        for item in old:
            self._on_remove(item)

    def __ior__(self, other: Iterable[T]) -> "TrackedSet[T]":  # type: ignore[override, misc]
        ensure_materialized(self)
        for item in other:
            self.add(item)
        return self

    def __isub__(self, other: Iterable[T]) -> "TrackedSet[T]":  # type: ignore[override, misc]
        ensure_materialized(self)
        for item in other:
            self.discard(item)
        return self
