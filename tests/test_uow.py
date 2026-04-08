from collections.abc import Iterable
from dataclasses import dataclass
from unittest.mock import AsyncMock

import pytest

from uow import (
    EntityConfig,
    GenericDataMapper,
    InstrumentationRegistry,
    UnitOfWork,
)
from uow.flush import sort_operations

from conftest import (
    Delivery,
    FakeDeliveryMapper,
    FakeItemMapper,
    FakeOrderMapper,
    Order,
    OrderItem,
)


class TestRegisterClean:
    def test_no_changes_no_update(self, order_uow: UnitOfWork) -> None:
        order = Order(id=1, customer="Alice", items=[], delivery=None)
        order_uow.register_clean(order)

        ops = order_uow._build_operations()
        assert ops == []

    def test_scalar_change_produces_update(self, order_uow: UnitOfWork) -> None:
        order = Order(id=1, customer="Alice", items=[], delivery=None)
        order_uow.register_clean(order)

        order.customer = "Bob"

        ops = order_uow._build_operations()
        assert len(ops) == 1
        op_type, entity_type, entities = ops[0]
        assert op_type.value == "update"
        assert entity_type is Order
        assert entities == [order]


class TestRegisterNew:
    def test_new_entity_produces_insert(self, order_uow: UnitOfWork) -> None:
        order = Order(id=None, customer="Alice", items=[], delivery=None)
        order_uow.register_new(order)

        ops = order_uow._build_operations()
        assert len(ops) == 1
        assert ops[0][0].value == "insert"

    def test_new_with_children(self, order_uow: UnitOfWork) -> None:
        item = OrderItem(id=None, product="X", qty=1)
        order = Order(id=None, customer="Alice", items=[item], delivery=None)
        order_uow.register_new(order)

        ops = order_uow._build_operations()
        types_to_insert = {op[1] for op in ops if op[0].value == "insert"}
        assert Order in types_to_insert
        assert OrderItem in types_to_insert


class TestListTracking:
    def test_append_to_list_registers_new(self, order_uow: UnitOfWork) -> None:
        order = Order(id=1, customer="Alice", items=[], delivery=None)
        order_uow.register_clean(order)

        new_item = OrderItem(id=None, product="Y", qty=2)
        order.items.append(new_item)

        ops = order_uow._build_operations()
        insert_ops = [op for op in ops if op[0].value == "insert"]
        assert len(insert_ops) == 1
        assert insert_ops[0][1] is OrderItem
        assert insert_ops[0][2] == [new_item]

    def test_remove_from_list_marks_deleted(self, order_uow: UnitOfWork) -> None:
        item = OrderItem(id=10, product="Z", qty=1)
        order = Order(id=1, customer="Alice", items=[item], delivery=None)
        order_uow.register_clean(order)

        order.items.remove(item)

        ops = order_uow._build_operations()
        delete_ops = [op for op in ops if op[0].value == "delete"]
        assert len(delete_ops) == 1
        assert delete_ops[0][1] is OrderItem


class TestSingleOfReplacement:
    def test_replace_single_child(self, order_uow: UnitOfWork) -> None:
        old_delivery = Delivery(id=1, address="Old St")
        order = Order(id=1, customer="Alice", items=[], delivery=old_delivery)
        order_uow.register_clean(order)

        new_delivery = Delivery(id=None, address="New St")
        order.delivery = new_delivery

        ops = order_uow._build_operations()
        delete_ops = [op for op in ops if op[0].value == "delete"]
        insert_ops = [op for op in ops if op[0].value == "insert"]

        assert any(op[1] is Delivery for op in delete_ops)
        assert any(op[1] is Delivery for op in insert_ops)


class TestFlushOrder:
    def test_independent_types_preserve_registration_order(self) -> None:
        @dataclass
        class User:
            id: int | None
            name: str

        @dataclass
        class Subscription:
            id: int | None
            user_id: int | None

        class FakeUserMapper(GenericDataMapper[User]):
            def __init__(self, conn: object) -> None: ...
            async def save(self, entities: Iterable[User]) -> None: ...
            async def update(self, entities: Iterable[User]) -> None: ...
            async def delete(self, entities: Iterable[User]) -> None: ...

        class FakeSubMapper(GenericDataMapper[Subscription]):
            def __init__(self, conn: object) -> None: ...
            async def save(self, entities: Iterable[Subscription]) -> None: ...
            async def update(self, entities: Iterable[Subscription]) -> None: ...
            async def delete(self, entities: Iterable[Subscription]) -> None: ...

        reg = InstrumentationRegistry()
        reg.register(
            EntityConfig(
                entity_type=User,
                identity_key=("id",),
                mapper_type=FakeUserMapper,
                children={},
                depends_on=[],
            )
        )
        reg.register(
            EntityConfig(
                entity_type=Subscription,
                identity_key=("id",),
                mapper_type=FakeSubMapper,
                children={},
                depends_on=[],
            )
        )

        conn = AsyncMock()
        uow = UnitOfWork(conn, reg)

        user = User(id=None, name="Alice")
        sub = Subscription(id=None, user_id=None)

        uow.register_new(user)
        uow.register_new(sub)

        ops = uow._build_operations()
        ordered = sort_operations(reg, ops)

        insert_types = [op[1] for op in ordered if op[0].value == "insert"]
        assert insert_types == [User, Subscription]

    def test_reverse_registration_order_respected(self) -> None:
        @dataclass
        class User:
            id: int | None
            name: str

        @dataclass
        class Subscription:
            id: int | None
            user_id: int | None

        class FakeUserMapper(GenericDataMapper[User]):
            def __init__(self, conn: object) -> None: ...
            async def save(self, entities: Iterable[User]) -> None: ...
            async def update(self, entities: Iterable[User]) -> None: ...
            async def delete(self, entities: Iterable[User]) -> None: ...

        class FakeSubMapper(GenericDataMapper[Subscription]):
            def __init__(self, conn: object) -> None: ...
            async def save(self, entities: Iterable[Subscription]) -> None: ...
            async def update(self, entities: Iterable[Subscription]) -> None: ...
            async def delete(self, entities: Iterable[Subscription]) -> None: ...

        reg = InstrumentationRegistry()
        reg.register(
            EntityConfig(
                entity_type=User,
                identity_key=("id",),
                mapper_type=FakeUserMapper,
                children={},
                depends_on=[],
            )
        )
        reg.register(
            EntityConfig(
                entity_type=Subscription,
                identity_key=("id",),
                mapper_type=FakeSubMapper,
                children={},
                depends_on=[],
            )
        )

        conn = AsyncMock()
        uow = UnitOfWork(conn, reg)

        user = User(id=None, name="Alice")
        sub = Subscription(id=None, user_id=None)

        uow.register_new(sub)
        uow.register_new(user)

        ops = uow._build_operations()
        ordered = sort_operations(reg, ops)

        insert_types = [op[1] for op in ordered if op[0].value == "insert"]
        assert insert_types == [Subscription, User]


class TestCommit:
    @pytest.mark.asyncio
    async def test_commit_calls_mappers(self, order_uow: UnitOfWork) -> None:
        order = Order(id=1, customer="Alice", items=[], delivery=None)
        order_uow.register_clean(order)
        order.customer = "Bob"

        await order_uow.commit()

        mapper = order_uow._mappers[Order]
        assert isinstance(mapper, FakeOrderMapper)
        assert order in mapper.updated

    @pytest.mark.asyncio
    async def test_commit_cleans_state(self, order_uow: UnitOfWork) -> None:
        order = Order(id=1, customer="Alice", items=[], delivery=None)
        order_uow.register_clean(order)
        order.customer = "Bob"

        await order_uow.commit()

        ops = order_uow._build_operations()
        assert ops == []

    @pytest.mark.asyncio
    async def test_full_cycle(self, order_uow: UnitOfWork) -> None:
        item = OrderItem(id=10, product="Widget", qty=5)
        delivery = Delivery(id=20, address="123 Main")
        order = Order(id=1, customer="Alice", items=[item], delivery=delivery)
        order_uow.register_clean(order)

        order.customer = "Bob"
        new_item = OrderItem(id=None, product="Gadget", qty=1)
        order.items.append(new_item)
        item.qty = 10
        new_delivery = Delivery(id=None, address="456 Oak")
        order.delivery = new_delivery

        await order_uow.commit()

        order_mapper = order_uow._mappers[Order]
        item_mapper = order_uow._mappers[OrderItem]
        delivery_mapper = order_uow._mappers[Delivery]

        assert isinstance(order_mapper, FakeOrderMapper)
        assert isinstance(item_mapper, FakeItemMapper)
        assert isinstance(delivery_mapper, FakeDeliveryMapper)

        assert order in order_mapper.updated
        assert item in item_mapper.updated
        assert new_item in item_mapper.saved
        assert delivery in delivery_mapper.deleted
        assert new_delivery in delivery_mapper.saved
