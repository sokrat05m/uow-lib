from typing import Any

from uow.asyncio.mapper import Connection, GenericDataMapper
from uow.base_uow import BaseUnitOfWork
from uow.flush import OpType, sort_operations
from uow.instrumentation import InstrumentationRegistry


class UnitOfWork(BaseUnitOfWork[Connection, GenericDataMapper[Any]]):
    def __init__(
        self,
        connection: Connection,
        registry: InstrumentationRegistry,
    ) -> None:
        super().__init__(connection, registry)

    async def flush(self) -> None:
        try:
            await self._flush()
            self._post_flush_cleanup()
        except Exception:
            await self._rollback_and_detach()
            raise

    async def commit(self) -> None:
        try:
            await self._flush()
            self._post_flush_cleanup()
            await self._connection.commit()
        except Exception:
            await self._rollback_and_detach()
            raise

    async def rollback(self) -> None:
        await self._rollback_and_detach()

    async def _flush(self) -> None:
        operations = self._build_operations()
        ordered = sort_operations(self._registry, operations)

        for op_type, entity_type, entities in ordered:
            mapper = self._get_mapper(entity_type)
            if op_type is OpType.INSERT:
                await mapper.save(entities)
            elif op_type is OpType.UPDATE:
                await mapper.update(entities)
            elif op_type is OpType.DELETE:
                await mapper.delete(entities)

    async def _rollback_and_detach(self) -> None:
        try:
            await self._connection.rollback()
        finally:
            self._detach_all()
