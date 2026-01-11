from __future__ import annotations

from enum import Enum
from typing import Any, Union

from x8.core import DataModel, Operation
from x8.ql import Expression, Update
from x8.storage._common import Index, StoreOperation

DocumentKeyType = Union[str, int, float, bool]


class DocumentFieldType(str, Enum):
    """Document field type."""

    STRING = "string"
    NUMBER = "number"
    BOOLEAN = "boolean"
    OBJECT = "object"
    ARRAY = "array"
    NULL = "null"


class DocumentKey(DataModel):
    """Document key.

    Attributes:
        id: Document id.
        pk: Partition key.
    """

    id: DocumentKeyType
    pk: DocumentKeyType | None = None


class DocumentProperties(DataModel):
    """Document properties.

    Attributes:
        etag: Document ETag.
    """

    etag: str | None = None


class DocumentItem(DataModel):
    """Document item.

    Attributes:
        key: Document key.
        value: Document value.
        properties: Document properties.
    """

    key: DocumentKey
    value: dict[str, Any] | None = None
    properties: DocumentProperties | None = None


class DocumentList(DataModel):
    """Document list.

    Attributes:
        items: List of documents.
    """

    items: list[DocumentItem]


class DocumentCollectionConfig(DataModel):
    """Document collection config.

    Attributes:
        id_field: Id field name.
        id_type: Id field type.
        pk_field: Partition key field name.
        pk_type: Partition key field type.
        value_field: Value field name.
        value_type: Value type.
        indexes: List of indexes.
        nconfig: Native config parameters.
    """

    id_field: str | None = None
    id_type: DocumentFieldType | None = None
    pk_field: str | None = None
    pk_type: DocumentFieldType | None = None
    value_field: str | None = None
    value_type: str | None = None
    indexes: list[Index] | None = None
    nconfig: dict[str, Any] | None = None


class DocumentQueryConfig(DataModel):
    """Query config.

    Attributes:
        paging: A value indicating whether the results should be paged.
        page_size: Page size.
    """

    paging: bool | None = False
    page_size: int | None = None


class DocumentBatch(DataModel):
    """Document batch.

    Attributes:
        operations: List of batch operations.
    """

    operations: list[Operation] = []

    def get(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentBatch:
        """Get operation.

        Args:
            key:
                Document key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        value: dict[str, Any] | DataModel,
        key: DocumentKeyType | dict | DocumentKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentBatch:
        """Put operation.

        Args:
            value:
                Document value.
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def update(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentBatch:
        """Update operation.

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
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.UPDATE, args=locals())
        )
        return self

    def delete(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentBatch:
        """Delete operation.

        Args:
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self


class DocumentTransaction(DataModel):
    """Document transaction.

    Attributes:
        operations: List of transaction operations.
    """

    operations: list[Operation] = []

    def get(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentTransaction:
        """Get operation.

        Args:
            key:
                Document key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        value: dict[str, Any] | DataModel,
        key: DocumentKeyType | dict | DocumentKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentTransaction:
        """Put operation.

        Args:
            value:
                Document value.
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def update(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentTransaction:
        """Update operation.

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
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.UPDATE, args=locals())
        )
        return self

    def delete(
        self,
        key: DocumentKeyType | dict | DocumentKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> DocumentTransaction:
        """Delete operation.

        Args:
            key:
                Document key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self
