_TRACKER_ATTR = "_uow_tracker_"
_ORIGINAL_SETATTR = "_uow_original_setattr_"


class ChangeTracker:
    __slots__ = (
        "_entity",
        "_tracked_attrs",
        "_dirty_fields",
        "_original_class",
    )

    def __init__(self, entity: object, tracked_attrs: frozenset[str]) -> None:
        self._entity = entity
        self._tracked_attrs = tracked_attrs
        self._dirty_fields: set[str] = set()
        self._original_class: type = type(entity)

    def install(self) -> None:
        object.__setattr__(self._entity, _TRACKER_ATTR, self)
        _patch_class(self._original_class)

    def uninstall(self) -> None:
        try:
            object.__delattr__(self._entity, _TRACKER_ATTR)
        except AttributeError:
            pass

    @property
    def is_dirty(self) -> bool:
        return len(self._dirty_fields) > 0

    def get_dirty_fields(self) -> frozenset[str]:
        return frozenset(self._dirty_fields)

    def discard_dirty_field(self, name: str) -> None:
        self._dirty_fields.discard(name)

    def reset(self) -> None:
        self._dirty_fields.clear()


def _tracking_setattr(self: object, name: str, value: object) -> None:
    if name != _TRACKER_ATTR:
        instance_dict = object.__getattribute__(self, "__dict__")
        tracker: ChangeTracker | None = instance_dict.get(_TRACKER_ATTR)
        if tracker is not None and name in tracker._tracked_attrs:
            tracker._dirty_fields.add(name)
    original_setattr = type.__getattribute__(type(self), _ORIGINAL_SETATTR)
    if original_setattr is not None:
        original_setattr(self, name, value)
    else:
        object.__setattr__(self, name, value)


def _patch_class(cls: type) -> None:
    if _ORIGINAL_SETATTR in cls.__dict__:
        return

    original_setattr = cls.__dict__.get("__setattr__")
    type.__setattr__(cls, _ORIGINAL_SETATTR, original_setattr)
    cls.__setattr__ = _tracking_setattr  # type: ignore[assignment]
