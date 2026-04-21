from dataclasses import dataclass, field
from collections.abc import Iterable
from unittest.mock import AsyncMock

from uow import EntityConfig, GenericDataMapper, InstrumentationRegistry, ListOf, SetOf, UnitOfWork


@dataclass
class Comment:
    id: int | None
    text: str


@dataclass(unsafe_hash=True)
class Tag:
    id: int | None
    label: str


@dataclass
class Post:
    id: int | None
    comments: list[Comment] = field(default_factory=list)
    tags: set[Tag] = field(default_factory=set)


class FakePostMapper(GenericDataMapper[Post]):
    def __init__(self, connection: object) -> None: ...
    async def save(self, entities: Iterable[Post]) -> None: ...
    async def update(self, entities: Iterable[Post]) -> None: ...
    async def delete(self, entities: Iterable[Post]) -> None: ...


class FakeCommentMapper(GenericDataMapper[Comment]):
    def __init__(self, connection: object) -> None: ...
    async def save(self, entities: Iterable[Comment]) -> None: ...
    async def update(self, entities: Iterable[Comment]) -> None: ...
    async def delete(self, entities: Iterable[Comment]) -> None: ...


class FakeTagMapper(GenericDataMapper[Tag]):
    def __init__(self, connection: object) -> None: ...
    async def save(self, entities: Iterable[Tag]) -> None: ...
    async def update(self, entities: Iterable[Tag]) -> None: ...
    async def delete(self, entities: Iterable[Tag]) -> None: ...


def build_uow() -> UnitOfWork:
    registry = InstrumentationRegistry()
    registry.register(
        EntityConfig(
            entity_type=Post,
            identity_key=("id",),
            mapper_type=FakePostMapper,
            children={
                "comments": ListOf(Comment, parent_key="post_id"),
                "tags": SetOf(Tag, parent_key="post_id"),
            },
        )
    )
    registry.register(
        EntityConfig(
            entity_type=Comment,
            identity_key=("id",),
            mapper_type=FakeCommentMapper,
            depends_on=[Post],
        )
    )
    registry.register(
        EntityConfig(
            entity_type=Tag,
            identity_key=("id",),
            mapper_type=FakeTagMapper,
            depends_on=[Post],
        )
    )
    return UnitOfWork(AsyncMock(), registry)


class TestCollectionReplacement:
    def test_replace_list_marks_removed_and_added_children(self) -> None:
        uow = build_uow()
        old_comment = Comment(id=1, text="old")
        post = Post(id=10, comments=[old_comment])
        uow.register_clean(post)

        new_comment = Comment(id=None, text="new")
        post.comments = [new_comment]

        ops = uow._build_operations()
        delete_ops = [op for op in ops if op[0].value == "delete"]
        insert_ops = [op for op in ops if op[0].value == "insert"]

        assert any(op[1] is Comment and old_comment in op[2] for op in delete_ops)
        assert any(op[1] is Comment and new_comment in op[2] for op in insert_ops)
        assert new_comment.post_id == 10  # type: ignore[attr-defined]
        later = Comment(id=None, text="later")
        post.comments.append(later)

        ops = uow._build_operations()
        assert any(op[1] is Comment and later in op[2] for op in ops if op[0].value == "insert")

    def test_replace_set_marks_removed_and_added_children(self) -> None:
        uow = build_uow()
        old_tag = Tag(id=1, label="old")
        post = Post(id=10, tags={old_tag})
        uow.register_clean(post)

        new_tag = Tag(id=None, label="new")
        post.tags = {new_tag}

        ops = uow._build_operations()
        delete_ops = [op for op in ops if op[0].value == "delete"]
        insert_ops = [op for op in ops if op[0].value == "insert"]

        assert any(op[1] is Tag and old_tag in op[2] for op in delete_ops)
        assert any(op[1] is Tag and new_tag in op[2] for op in insert_ops)
        assert new_tag.post_id == 10  # type: ignore[attr-defined]
        later = Tag(id=None, label="later")
        post.tags.add(later)

        ops = uow._build_operations()
        assert any(op[1] is Tag and later in op[2] for op in ops if op[0].value == "insert")

    def test_replace_after_append_cancels_pending_insert(self) -> None:
        uow = build_uow()
        post = Post(id=10)
        uow.register_clean(post)

        transient = Comment(id=None, text="transient")
        post.comments.append(transient)
        post.comments = []

        ops = uow._build_operations()
        insert_ops = [op for op in ops if op[0].value == "insert" and op[1] is Comment]

        assert insert_ops == []
