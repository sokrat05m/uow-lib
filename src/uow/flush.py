import enum
from collections import deque

from uow import InstrumentationRegistry
from uow.exceptions import CyclicDependencyError


class OpType(enum.Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"


def _depth_sort_key(
    depth: dict[type, int],
    item: tuple[OpType, type, list[object]],
) -> int:
    return depth.get(item[1], 0)


def sort_operations(
    registry: InstrumentationRegistry,
    operations: list[tuple[OpType, type, list[object]]],
) -> list[tuple[OpType, type, list[object]]]:
    depth = _compute_depth_levels(registry)

    inserts = [op for op in operations if op[0] is OpType.INSERT]
    updates = [op for op in operations if op[0] is OpType.UPDATE]
    deletes = [op for op in operations if op[0] is OpType.DELETE]

    key = lambda item: _depth_sort_key(depth, item)
    inserts.sort(key=key)
    updates.sort(key=key)
    deletes.sort(key=key, reverse=True)

    return inserts + updates + deletes


def _compute_depth_levels(registry: InstrumentationRegistry) -> dict[type, int]:
    configs = registry.all_configs()
    in_degree: dict[type, int] = {t: 0 for t in configs}
    adjacency: dict[type, list[type]] = {t: [] for t in configs}

    for entity_type, config in configs.items():
        for dep in config.depends_on:
            if dep in adjacency:
                adjacency[dep].append(entity_type)
                in_degree[entity_type] += 1

    queue = deque(t for t, d in in_degree.items() if d == 0)
    depth: dict[type, int] = {t: 0 for t in queue}
    visited = 0

    while queue:
        node = queue.popleft()
        visited += 1
        for neighbor in adjacency[node]:
            in_degree[neighbor] -= 1
            depth[neighbor] = max(depth.get(neighbor, 0), depth[node] + 1)
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if visited != len(configs):
        raise CyclicDependencyError(
            "Circular dependency detected among entity configs"
        )
    return depth
