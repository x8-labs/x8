from __future__ import annotations

from typing import Any

from x8.core import Response, operation
from x8.ql import Expression, OrderBy, Select
from x8.storage._common import CollectionResult, StoreComponent

from ._models import (
    VectorBatch,
    VectorCollectionConfig,
    VectorItem,
    VectorKey,
    VectorList,
    VectorTransaction,
    VectorValue,
)


class VectorStore(StoreComponent):
    collection: str | None

    def __init__(self, collection: str | None = None, **kwargs):
        """_summary_

        Args:
            collection:
                Collection name.
        """
        self.collection = collection
        super().__init__(**kwargs)

    @operation()
    def create_collection(
        self,
        collection: str | None = None,
        config: dict | VectorCollectionConfig | None = None,
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
            Collection operation result.
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
            Collection operation result.
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
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if the collection exists.

        Args:
            collection:
                Collection name.

        Returns:
            A value indicating whether the collection exists.
        """
        ...

    @operation()
    def get(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Get vector.

        Args:
            key:
                Vector key.
            collection:
                Collection name.

        Returns:
            Vector item.

        Raises:
            NotFoundError:
                Vector not found.
        """
        ...

    @operation()
    def put(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        metadata: dict | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Put vector.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            metadata:
                Custom metadata.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    def update(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Update vector value.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    def update_metadata(
        self,
        key: str | dict | VectorKey,
        metadata: dict | None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Update vector metadata.

        Args:
            key:
                Vector key.
            metadata:
                Vector metadata.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    def delete(
        self,
        key: str | dict | VectorKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete vector.

        Args:
            key:
                Vector key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            None.

        Raises:
            NotFoundError:
                Vector not found.
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
    ) -> Response[VectorList]:
        """Query vectors.

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
            List of vectors.
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
        """Count vectors.

        Args:
            search:
                Search expression.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Vector count.
        """
        ...

    @operation()
    def batch(
        self,
        batch: dict | VectorBatch,
        **kwargs,
    ) -> Response[list[Any]]:
        """Batch operation.

        Args:
            batch:
                Vector batch.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    def transact(
        self,
        transaction: dict | VectorTransaction,
        **kwargs,
    ) -> Response[list[Any]]:
        """Execute transaction.

        Args:
            transaction:
                Vector transaction.

        Returns:
            Transaction results.

        Raises:
            ConflictError:
                Transaction failed.
        """
        ...

    @operation()
    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close the sync client.

        Returns:
            None.
        """
        ...

    @operation()
    async def acreate_collection(
        self,
        collection: str | None = None,
        config: dict | VectorCollectionConfig | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Create collection.

        Args:
            collection:
                Collection name.
            config:
                Collection config.

        Returns:
            Collection operation result.
        """
        ...

    @operation()
    async def adrop_collection(
        self,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        """Drop collection.

        Args:
            collection:
                Collection name.

        Returns:
            Collection operation result.
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
        self,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if the collection exists.

        Args:
            collection:
                Collection name.

        Returns:
            A value indicating whether the collection exists.
        """
        ...

    @operation()
    async def aget(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Get vector.

        Args:
            key:
                Vector key.
            collection:
                Collection name.

        Returns:
            Vector item.

        Raises:
            NotFoundError:
                Vector not found.
        """
        ...

    @operation()
    async def aput(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        metadata: dict | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Put vector.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            metadata:
                Custom metadata.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    async def aupdate(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Update vector value.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    async def aupdate_metadata(
        self,
        key: str | dict | VectorKey,
        metadata: dict | None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[VectorItem]:
        """Update vector metadata.

        Args:
            key:
                Vector key.
            metadata:
                Vector metadata.
            collection:
                Collection name.

        Returns:
            Vector item.
        """
        ...

    @operation()
    async def adelete(
        self,
        key: str | dict | VectorKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete vector.

        Args:
            key:
                Vector key.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            None.

        Raises:
            NotFoundError:
                Vector not found.
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
    ) -> Response[VectorList]:
        """Query vectors.

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
            List of vectors.
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
        """Count vectors.

        Args:
            search:
                Search expression.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Vector count.
        """
        ...

    @operation()
    async def abatch(
        self,
        batch: dict | VectorBatch,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Batch operation.

        Args:
            batch:
                Vector batch.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    async def atransact(
        self,
        transaction: dict | VectorTransaction,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute transaction.

        Args:
            transaction:
                Vector transaction.

        Returns:
            Transaction results.

        Raises:
            ConflictError:
                Transaction failed.
        """
        ...

    @operation()
    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close the async client.

        Returns:
            None.
        """
        ...
