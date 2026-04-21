import enum
from collections.abc import Iterable

from uow.instrumentation import EntityConfig
from uow.tracking import ChangeTracker


class _EntityState(enum.Enum):
    NEW = "new"
    CLEAN = "clean"
    DELETED = "deleted"


class _TrackedEntry:
    __slots__ = (
        "entity",
        "state",
        "tracker",
        "config",
        "single_children",
        "collection_refs",
    )

    def __init__(
        self,
        entity: object,
        state: _EntityState,
        tracker: ChangeTracker | None,
        config: EntityConfig,
        single_children: dict[str, object | None],
        collection_refs: dict[str, Iterable[object] | None],
    ) -> None:
        self.entity = entity
        self.state = state
        self.tracker = tracker
        self.config = config
        self.single_children = single_children
        self.collection_refs = collection_refs
