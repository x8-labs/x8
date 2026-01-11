from __future__ import annotations

from typing import Any

from x8.core import DataModel, Response, operation
from x8.ql import Expression, OrderBy, Select, Update
from x8.storage._common import (
    CollectionResult,
    Index,
    IndexResult,
    StoreComponent,
)

from ._models import (
    SearchBatch,
    SearchCollectionConfig,
    SearchItem,
    SearchKey,
    SearchList,
)


class SearchStore(StoreComponent):
    collection: str | None

    def __init__(
        self,
        collection: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            collection:
                Default collection name.
        """
        self.collection = collection
        super().__init__(**kwargs)

    @operation()
    def create_collection(
        self,
        collection: str | None = None,
        config: dict | SearchCollectionConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Create collection.

        Args:
            collection:
                Collection name.
            config:
                Collection config.
            where:
                Condition expression.

        Returns:
            Collection result.
        """
        ...

    @operation()
    def drop_collection(
        self,
        collection: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Drop collection.

        Args:
            collection:
                Collection name.
            where:
                Condition expression.

        Returns:
            Collection result.
        """
        ...

    @operation()
    def list_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List collections.

        Returns:
            List of collection names.
        """
        ...

    @operation()
    def has_collection(
        self,
        collection: str | None = None,
    ) -> Response[bool]:
        """Check if collection exists.

        Args:
            collection:
                Collection name.

        Returns:
            A value indicating whether collection exists.
        """
        ...

    @operation()
    def create_index(
        self,
        index: dict | Index,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Create index.

        Args:
            index:
                Index to create.
            collection:
                Collection name.

        Returns:
            Index result.
        """
        ...

    @operation()
    def drop_index(
        self,
        index: dict | Index | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Drop index.

        Args:
            index:
                Index to drop.
            collection:
                Collection name.

        Returns:
            Index result.
        """
        ...

    @operation()
    def list_indexes(
        self,
        collection: str | None = None,
    ) -> Response[list[Index]]:
        """List indexes.

        Args:
            collection:
                Collection name.

        Returns:
            List of indexes.
        """
        ...

    @operation()
    def get(
        self,
        key: str | dict | SearchKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Get document.

        Args:
            key:
                Document key.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            NotFoundError:
                Item not found.
        """
        ...

    @operation()
    def put(
        self,
        value: dict[str, Any] | DataModel,
        key: str | dict | SearchKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Put document.

        Args:
            value:
                Document value.
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    def update(
        self,
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Update document.

        Args:
            key:
                Document key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether updated document is returned.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            NotFoundError:
                Document not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    def delete(
        self,
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete document.

        Args:
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            None.

        Raises:
            NotFoundError:
                Document not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    def query(
        self,
        search: str | Expression | None = None,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[SearchList]:
        """Query documents.

        Args:
            search:
                Search expression.
            select:
                Select expression.
            where:
                Condition expression.
            order_by:
                Order by expression.
            limit:
                Query limit.
            offset:
                Query offset.
            collection:
                Collection name.

        Returns:
            Document list with items.
        """
        ...

    @operation()
    def count(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count documents.

        Args:
            search:
                Search expression.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Count of documents.
        """
        ...

    @operation()
    def batch(
        self,
        batch: dict | SearchBatch,
        **kwargs,
    ) -> Response[list[Any]]:
        """Execute batch.

        Args:
            batch:
                Batch operation.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close client.

        Returns:
            None.
        """
        ...

    @operation()
    async def acreate_collection(
        self,
        collection: str | None = None,
        config: dict | SearchCollectionConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Create collection.

        Args:
            collection:
                Collection name.
            config:
                Collection config.
            where:
                Condition expression.

        Returns:
            Collection result.
        """
        ...

    @operation()
    async def adrop_collection(
        self,
        collection: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Drop collection.

        Args:
            collection:
                Collection name.
            where:
                Condition expression.

        Returns:
            Collection result.
        """
        ...

    @operation()
    async def alist_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List collections.

        Returns:
            List of collection names.
        """
        ...

    @operation()
    async def ahas_collection(
        self, collection: str | None = None
    ) -> Response[bool]:
        """Check if collection exists.

        Args:
            collection:
                Collection name.

        Returns:
            A value indicating whether collection exists.
        """
        ...

    @operation()
    async def acreate_index(
        self,
        index: dict | Index,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Create index.

        Args:
            index:
                Index to create.
            collection:
                Collection name.

        Returns:
            Index result.
        """
        ...

    @operation()
    async def adrop_index(
        self,
        index: dict | Index | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Drop index.

        Args:
            index:
                Index to drop.
            collection:
                Collection name.

        Returns:
            Index result.
        """
        ...

    @operation()
    async def alist_indexes(
        self,
        collection: str | None = None,
    ) -> Response[list[Index]]:
        """List indexes.

        Args:
            collection:
                Collection name.
        """
        ...

    @operation()
    async def aget(
        self,
        key: str | dict | SearchKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Get document.

        Args:
            key:
                Document key.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            NotFoundError:
                Document not found.
        """
        ...

    @operation()
    async def aput(
        self,
        value: dict[str, Any] | DataModel,
        key: str | dict | SearchKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Put document.

        Args:
            value:
                Document.
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    async def aupdate(
        self,
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        """Update document.

        Args:
            key:
                Document key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether updated document is returned.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            NotFoundError:
                Document not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    async def adelete(
        self,
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete document.

        Args:
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            None.

        Raises:
            NotFoundError:
                Document not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    async def aquery(
        self,
        search: str | Expression | None = None,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[SearchList]:
        """Query documents.

        Args:
            search:
                Search expression.
            select:
                Select expression.
            where:
                Condition expression.
            order_by:
                Order by expression.
            limit:
                Query limit.
            offset:
                Query offset.
            collection:
                Collection name.

        Returns:
            Document list with items.
        """
        ...

    @operation()
    async def acount(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count documents.

        Args:
            search:
                Search expression.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Count of documents.
        """
        ...

    @operation()
    async def abatch(
        self,
        batch: dict | SearchBatch,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute batch.

        Args:
            batch:
                Batch operation.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close async client.

        Returns:
            None.
        """
        ...
