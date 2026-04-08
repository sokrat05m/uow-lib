from uow.exceptions import DuplicateEntityError


class IdentityMap:
    def __init__(self) -> None:
        self._map: dict[tuple[type, tuple[object, ...]], object] = {}

    def get(self, entity_type: type, key: tuple[object, ...]) -> object | None:
        return self._map.get((entity_type, key))

    def put(self, entity_type: type, key: tuple[object, ...], entity: object) -> None:
        map_key = (entity_type, key)
        if map_key in self._map:
            raise DuplicateEntityError(
                f"{entity_type.__name__} with key {key} already registered"
            )
        self._map[map_key] = entity

    def remove(self, entity_type: type, key: tuple[object, ...]) -> None:
        self._map.pop((entity_type, key), None)

    def clear(self) -> None:
        self._map.clear()
