import ast
import inspect
import textwrap
from dataclasses import dataclass, field
from typing import Any

from uow import UnregisteredEntityError
from uow.mapper import GenericDataMapper


@dataclass(frozen=True)
class ListOf:
    entity_type: type
    parent_key: str | None = None


@dataclass(frozen=True)
class SingleOf:
    entity_type: type
    parent_key: str | None = None


@dataclass(frozen=True)
class SetOf:
    entity_type: type
    parent_key: str | None = None


@dataclass(frozen=True)
class EmbeddedOf:
    value_object_type: type


@dataclass(frozen=True)
class CollectionOfEmbedded:
    value_object_type: type


type ChildSpec = ListOf | SingleOf | SetOf | EmbeddedOf | CollectionOfEmbedded


def _extract_init_attrs(cls: type) -> set[str]:
    init = cls.__dict__.get("__init__")
    if init is None or not inspect.isfunction(init):
        return set()
    try:
        source = textwrap.dedent(inspect.getsource(init))
    except OSError:
        return set()
    tree = ast.parse(source)
    attrs: set[str] = set()
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Assign)
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Attribute)
            and isinstance(node.targets[0].value, ast.Name)
            and node.targets[0].value.id == "self"
        ):
            attrs.add(node.targets[0].attr)
    return attrs


@dataclass(frozen=True)
class EntityConfig:
    entity_type: type
    identity_key: tuple[str, ...]
    mapper_type: type[GenericDataMapper[Any]]
    children: dict[str, ChildSpec] = field(default_factory=dict)
    depends_on: list[type] = field(default_factory=list)
    exclude_from_tracking: frozenset[str] = field(default_factory=frozenset)

    @property
    def tracked_attrs(self) -> frozenset[str]:
        all_attrs: set[str] = set()
        if hasattr(self.entity_type, "__dataclass_fields__"):
            all_attrs = set(self.entity_type.__dataclass_fields__.keys())
        else:
            for cls in reversed(self.entity_type.__mro__):
                if hasattr(cls, "__annotations__"):
                    all_attrs.update(cls.__annotations__.keys())
                all_attrs.update(_extract_init_attrs(cls))
        return frozenset(all_attrs - self.exclude_from_tracking)


class InstrumentationRegistry:
    def __init__(self) -> None:
        self._configs: dict[type, EntityConfig] = {}

    def register(self, config: EntityConfig) -> None:
        for attr_name, child_spec in config.children.items():
            if isinstance(child_spec, (EmbeddedOf, CollectionOfEmbedded)):
                self._validate_embedded(child_spec.value_object_type, attr_name)
        self._configs[config.entity_type] = config

    @staticmethod
    def _validate_embedded(vo_type: type, attr_name: str) -> None:
        params = getattr(vo_type, "__dataclass_params__", None)
        if params is None or not params.frozen:
            raise TypeError(
                f"EmbeddedOf('{attr_name}'): {vo_type.__name__} must be a frozen dataclass"
            )

    def get(self, entity_type: type) -> EntityConfig:
        try:
            return self._configs[entity_type]
        except KeyError:
            raise UnregisteredEntityError(
                f"No config registered for {entity_type.__name__}"
            )

    def all_configs(self) -> dict[type, EntityConfig]:
        return self._configs
