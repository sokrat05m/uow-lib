from typing import Any

from uow.base_uow import BaseUnitOfWork
from uow.flush import OpType, sort_operations
from uow.instrumentation import InstrumentationRegistry
from uow.sync.mapper import Connection, GenericDataMapper


class UnitOfWork(BaseUnitOfWork[Connection, GenericDataMapper[Any]]):
    def __init__(
        self,
        connection: Connection,
        registry: InstrumentationRegistry,
    ) -> None:
        super().__init__(connection, registry)

    def flush(self) -> None:
        try:
            self._flush()
            self._post_flush_cleanup()
        except Exception:
            self._rollback_and_detach()
            raise

    def commit(self) -> None:
        try:
            self._flush()
            self._post_flush_cleanup()
            self._connection.commit()
        except Exception:
            self._rollback_and_detach()
            raise

    def rollback(self) -> None:
        self._rollback_and_detach()

    def _flush(self) -> None:
        operations = self._build_operations()
        ordered = sort_operations(self._registry, operations)

        for op_type, entity_type, entities in ordered:
            mapper = self._get_mapper(entity_type)
            if op_type is OpType.INSERT:
                mapper.save(entities)
            elif op_type is OpType.UPDATE:
                mapper.update(entities)
            elif op_type is OpType.DELETE:
                mapper.delete(entities)

    def _rollback_and_detach(self) -> None:
        try:
            self._connection.rollback()
        finally:
            self._detach_all()
