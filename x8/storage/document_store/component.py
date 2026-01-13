"""
Document Store
"""

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
    DocumentBatch,
    DocumentCollectionConfig,
    DocumentItem,
    DocumentKey,
    DocumentKeyType,
    DocumentList,
    DocumentTransaction,
)


class DocumentStore(StoreComponent):
    collection: str | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None

    def __init__(
        self,
        collection: str | None = None,
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = "pk",
        **kwargs,
    ):
        """Initialize.

        Args:
            collection:
                Default collection name.
            id_map_field:
                Field in the document to map into id.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            pk_map_field:
                Field in the document to map into pk.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
        """
        self.collection = collection
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field

        super().__init__(**kwargs)

    @operation()
    def create_collection(
        self,
        collection: str | None = None,
        config: dict | DocumentCollectionConfig | None = None,
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
        raise NotImplementedError

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
        raise NotImplementedError

    @operation()
    def list_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List collections.

        Returns:
            List of collection names.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @operation()
    def create_index(
        self,
        index: dict | Index,
        where: str | Expression | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Create index.

        Args:
            index:
                Index to create.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Index operation result.
        """
        raise NotImplementedError

    @operation()
    def drop_index(
        self,
        index: dict | Index | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Drop index.

        Args:
            index:
                Index to drop.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Index operation result.
        """
        raise NotImplementedError

    @operation()
    def list_indexes(
        self,
        collection: str | None = None,
    ) -> Response[list[Index]]:
        """List indexes.

        Args:
            collection:
                Collection name.
        """
        raise NotImplementedError

    @operation()
    def get(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
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
        raise NotImplementedError

    @operation()
    def put(
        self,
        value: dict[str, Any] | DataModel,
        key: DocumentKeyType | dict | DocumentKey | None = None,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
        """Put document.

        Args:
            value:
                Document.
            key:
                Document key.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old document ("old"), new document ("new"),
                or None is returned.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            PreconditionFailedError:
                Condition failed.
        """
        raise NotImplementedError

    @operation()
    def update(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
        """Update document.

        Args:
            key:
                Document key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old document ("old"), new document ("new"),
                or None is returned.
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
        raise NotImplementedError

    @operation()
    def delete(
        self,
        key: DocumentKeyType | dict | DocumentKey,
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
        raise NotImplementedError

    @operation()
    def query(
        self,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[DocumentList]:
        """Query documents.

        Args:
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
        raise NotImplementedError

    @operation()
    def count(
        self,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count documents.

        Args:
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Count of documents.
        """
        raise NotImplementedError

    @operation()
    def batch(
        self,
        batch: dict | DocumentBatch,
        **kwargs: Any,
    ) -> Response[list[DocumentItem | None]]:
        """Execute batch.

        Args:
            batch:
                Document batch.

        Returns:
            Batch operation results.
        """
        raise NotImplementedError

    @operation()
    def transact(
        self,
        transaction: dict | DocumentTransaction,
        **kwargs: Any,
    ) -> Response[list[DocumentItem | None]]:
        """Execute transaction.

        Args:
            transaction:
                Document transaction.

        Returns:
            Transaction results.

        Raises:
            ConflictError:
                Transaction failed.
        """
        raise NotImplementedError

    @operation()
    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close client.

        Returns:
            None.
        """
        raise NotImplementedError

    @operation()
    async def acreate_collection(
        self,
        collection: str | None = None,
        config: dict | DocumentCollectionConfig | None = None,
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
        raise NotImplementedError

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
            Collection operation result.
        """
        raise NotImplementedError

    @operation()
    async def alist_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List collections.

        Returns:
            List of collection names.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @operation()
    async def acreate_index(
        self,
        index: dict | Index,
        where: str | Expression | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Create index.

        Args:
            index:
                Index to create.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Index operation result.
        """
        raise NotImplementedError

    @operation()
    async def adrop_index(
        self,
        index: dict | Index | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
    ) -> Response[IndexResult]:
        """Drop index.

        Args:
            index:
                Index to drop.
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Index operation result.
        """
        raise NotImplementedError

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
        raise NotImplementedError

    @operation()
    async def aget(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
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
        raise NotImplementedError

    @operation()
    async def aput(
        self,
        value: dict[str, Any] | DataModel,
        key: DocumentKeyType | dict | DocumentKey | None = None,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
        """Put document.

        Args:
            value:
                Document.
            key:
                Document key.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old document ("old"), new document ("new"),
                or None is returned.
            collection:
                Collection name.

        Returns:
            Document item.

        Raises:
            PreconditionFailedError:
                Condition failed.
        """
        raise NotImplementedError

    @operation()
    async def aupdate(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[DocumentItem]:
        """Update document.

        Args:
            key:
                Document key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old document ("old"), new document ("new"),
                or None is returned.
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
        raise NotImplementedError

    @operation()
    async def adelete(
        self,
        key: DocumentKeyType | dict | DocumentKey,
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
        raise NotImplementedError

    @operation()
    async def aquery(
        self,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[DocumentList]:
        """Query documents.

        Args:
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
        raise NotImplementedError

    @operation()
    async def acount(
        self,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count documents.

        Args:
            where:
                Condition expression.
            collection:
                Collection name.

        Returns:
            Count of documents.
        """
        raise NotImplementedError

    @operation()
    async def abatch(
        self,
        batch: dict | DocumentBatch,
        **kwargs: Any,
    ) -> Response[list[DocumentItem | None]]:
        """Execute batch.

        Args:
            batch:
                Document batch.

        Returns:
            Batch operation results.
        """
        raise NotImplementedError

    @operation()
    async def atransact(
        self,
        transaction: dict | DocumentTransaction,
        **kwargs: Any,
    ) -> Response[list[DocumentItem | None]]:
        """Execute transaction.

        Args:
            transaction:
                Document transaction.

        Returns:
            Transaction results.

        Raises:
            ConflictError:
                Transaction failed.
        """
        raise NotImplementedError

    @operation()
    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        """Close async client.

        Returns:
            None.
        """
        raise NotImplementedError
