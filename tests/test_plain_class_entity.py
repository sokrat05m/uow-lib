from collections.abc import Iterable
from unittest.mock import AsyncMock

import pytest

from uow import (
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    UnitOfWork,
)


class Account:
    account_id: int | None
    name: str

    def __init__(self, account_id: int | None, name: str) -> None:
        self.account_id = account_id
        self.name = name


class BaseEntity:
    entity_id: int | None

    def __init__(self, entity_id: int | None) -> None:
        self.entity_id = entity_id


class Employee(BaseEntity):
    department: str

    def __init__(
        self, entity_id: int | None, department: str
    ) -> None:
        super().__init__(entity_id)
        self.department = department


class FakeAccountMapper(GenericDataMapper[Account]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Account] = []
        self.updated: list[Account] = []
        self.deleted: list[Account] = []

    async def save(self, entities: Iterable[Account]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Account]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Account]) -> None:
        self.deleted.extend(entities)


class FakeEmployeeMapper(GenericDataMapper[Employee]):
    def __init__(self, connection: object) -> None:
        self.saved: list[Employee] = []
        self.updated: list[Employee] = []
        self.deleted: list[Employee] = []

    async def save(self, entities: Iterable[Employee]) -> None:
        self.saved.extend(entities)

    async def update(self, entities: Iterable[Employee]) -> None:
        self.updated.extend(entities)

    async def delete(self, entities: Iterable[Employee]) -> None:
        self.deleted.extend(entities)


@pytest.fixture
def account_uow() -> UnitOfWork:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Account,
            identity_key=("account_id",),
            mapper_type=FakeAccountMapper,
        )
    )
    return UnitOfWork(AsyncMock(), reg)


@pytest.fixture
def employee_uow() -> UnitOfWork:
    reg = InstrumentationRegistry()
    reg.register(
        EntityConfig(
            entity_type=Employee,
            identity_key=("entity_id",),
            mapper_type=FakeEmployeeMapper,
        )
    )
    return UnitOfWork(AsyncMock(), reg)


class TestPlainClassEntity:
    def test_no_changes_no_update(self, account_uow: UnitOfWork) -> None:
        account = Account(account_id=1, name="Alice")
        account_uow.register_clean(account)

        ops = account_uow._build_operations()
        assert ops == []

    def test_scalar_change_produces_update(
        self, account_uow: UnitOfWork
    ) -> None:
        account = Account(account_id=1, name="Alice")
        account_uow.register_clean(account)

        account.name = "Bob"

        ops = account_uow._build_operations()
        assert len(ops) == 1
        op_type, entity_type, entities = ops[0]
        assert op_type.value == "update"
        assert entity_type is Account
        assert entities == [account]

    def test_new_entity_produces_insert(
        self, account_uow: UnitOfWork
    ) -> None:
        account = Account(account_id=None, name="Alice")
        account_uow.register_new(account)

        ops = account_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "insert"
        assert ops[0][1] is Account

    def test_delete_produces_delete(self, account_uow: UnitOfWork) -> None:
        account = Account(account_id=1, name="Alice")
        account_uow.register_clean(account)

        account_uow.register_deleted(account)

        ops = account_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "delete"

    @pytest.mark.asyncio
    async def test_commit_calls_mapper(
        self, account_uow: UnitOfWork
    ) -> None:
        account = Account(account_id=1, name="Alice")
        account_uow.register_clean(account)
        account.name = "Bob"

        await account_uow.commit()

        mapper = account_uow._mappers[Account]
        assert isinstance(mapper, FakeAccountMapper)
        assert account in mapper.updated

    @pytest.mark.asyncio
    async def test_commit_cleans_state(
        self, account_uow: UnitOfWork
    ) -> None:
        account = Account(account_id=1, name="Alice")
        account_uow.register_clean(account)
        account.name = "Bob"

        await account_uow.commit()

        ops = account_uow._build_operations()
        assert ops == []


class TestInheritedPlainClassEntity:
    def test_no_changes_no_update(self, employee_uow: UnitOfWork) -> None:
        emp = Employee(entity_id=1, department="Engineering")
        employee_uow.register_clean(emp)

        ops = employee_uow._build_operations()
        assert ops == []

    def test_own_field_change_produces_update(
        self, employee_uow: UnitOfWork
    ) -> None:
        emp = Employee(entity_id=1, department="Engineering")
        employee_uow.register_clean(emp)

        emp.department = "Sales"

        ops = employee_uow._build_operations()
        assert len(ops) == 1
        op_type, entity_type, entities = ops[0]
        assert op_type.value == "update"
        assert entity_type is Employee
        assert entities == [emp]

    def test_inherited_field_change_produces_update(
        self, employee_uow: UnitOfWork
    ) -> None:
        emp = Employee(entity_id=1, department="Engineering")
        employee_uow.register_clean(emp)

        emp.entity_id = 2

        ops = employee_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "update"
        assert ops[0][1] is Employee

    def test_new_entity_produces_insert(
        self, employee_uow: UnitOfWork
    ) -> None:
        emp = Employee(entity_id=None, department="Engineering")
        employee_uow.register_new(emp)

        ops = employee_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "insert"
        assert ops[0][1] is Employee

    @pytest.mark.asyncio
    async def test_commit_calls_mapper(
        self, employee_uow: UnitOfWork
    ) -> None:
        emp = Employee(entity_id=1, department="Engineering")
        employee_uow.register_clean(emp)
        emp.department = "Sales"

        await employee_uow.commit()

        mapper = employee_uow._mappers[Employee]
        assert isinstance(mapper, FakeEmployeeMapper)
        assert emp in mapper.updated
