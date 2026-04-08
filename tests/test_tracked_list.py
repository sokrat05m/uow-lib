from uow.collections import TrackedList


class TestTrackedList:
    def test_append(self) -> None:
        added: list[int] = []
        removed: list[int] = []
        tl: TrackedList[int] = TrackedList(
            [1, 2], on_add=added.append, on_remove=removed.append
        )

        tl.append(3)
        assert list(tl) == [1, 2, 3]
        assert added == [3]

    def test_remove(self) -> None:
        added: list[int] = []
        removed: list[int] = []
        tl: TrackedList[int] = TrackedList(
            [1, 2, 3], on_add=added.append, on_remove=removed.append
        )

        tl.remove(2)
        assert list(tl) == [1, 3]
        assert removed == [2]

    def test_pop(self) -> None:
        removed: list[int] = []
        tl: TrackedList[int] = TrackedList(
            [1, 2, 3], on_add=lambda x: None, on_remove=removed.append
        )

        val = tl.pop()
        assert val == 3
        assert removed == [3]

    def test_setitem(self) -> None:
        added: list[int] = []
        removed: list[int] = []
        tl: TrackedList[int] = TrackedList(
            [1, 2, 3], on_add=added.append, on_remove=removed.append
        )

        tl[1] = 20
        assert list(tl) == [1, 20, 3]
        assert added == [20]
        assert removed == [2]

    def test_clear(self) -> None:
        removed: list[int] = []
        tl: TrackedList[int] = TrackedList(
            [1, 2], on_add=lambda x: None, on_remove=removed.append
        )

        tl.clear()
        assert list(tl) == []
        assert set(removed) == {1, 2}
