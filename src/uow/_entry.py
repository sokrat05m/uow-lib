import enum

from uow.instrumentation import EntityConfig
from uow.tracking import ChangeTracker


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
