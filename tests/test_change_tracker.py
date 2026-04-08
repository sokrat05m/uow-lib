from dataclasses import dataclass

from uow.tracking import ChangeTracker


@dataclass
class Item:
    id: int | None
    product: str
    qty: int


class TestChangeTracker:
    def test_install_and_dirty(self) -> None:
        item = Item(id=1, product="A", qty=5)
        tracker = ChangeTracker(item, frozenset({"product", "qty"}))
        tracker.install()

        assert not tracker.is_dirty

        item.product = "B"
        assert tracker.is_dirty
        assert tracker.get_dirty_fields() == frozenset({"product"})

    def test_multiple_fields_dirty(self) -> None:
        item = Item(id=1, product="A", qty=5)
        tracker = ChangeTracker(item, frozenset({"product", "qty"}))
        tracker.install()

        item.product = "B"
        item.qty = 10
        assert tracker.get_dirty_fields() == frozenset({"product", "qty"})

    def test_type_unchanged_after_install(self) -> None:
        item = Item(id=1, product="A", qty=5)
        original_class = type(item)
        tracker = ChangeTracker(item, frozenset({"product"}))
        tracker.install()

        assert type(item) is original_class

    def test_uninstall_stops_tracking(self) -> None:
        item = Item(id=1, product="A", qty=5)
        tracker = ChangeTracker(item, frozenset({"product"}))
        tracker.install()
        tracker.uninstall()

        item.product = "B"
        assert not tracker.is_dirty

    def test_reset_clears_dirty(self) -> None:
        item = Item(id=1, product="A", qty=5)
        tracker = ChangeTracker(item, frozenset({"product"}))
        tracker.install()

        item.product = "B"
        assert tracker.is_dirty

        tracker.reset()
        assert not tracker.is_dirty
