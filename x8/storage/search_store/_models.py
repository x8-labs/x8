from __future__ import annotations

from enum import Enum
from typing import Any

from x8.core import DataModel, Operation
from x8.ql import Expression, Update
from x8.storage._common import Index, StoreOperation


class SearchFieldType(str, Enum):
    # String/Text
    TEXT = "text"  # full-text search (ES text, Azure searchable string)
    STRING = "string"  # keyword/exact match

    # Numeric (generic + specific)
    NUMBER = "number"  # generic number
    INTEGER = "integer"  # 32-bit integer
    LONG = "long"  # 64-bit integer
    FLOAT = "float"  # float (single precision)
    DOUBLE = "double"  # double precision float
    BYTE = "byte"  # 8-bit integer

    # Other primitives
    BOOLEAN = "boolean"
    DATE = "date"

    # Spatial
    GEO_POINT = "geo_point"
    GEO_SHAPE = "geo_shape"

    # Structures
    OBJECT = "object"
    ARRAY = "array"

    # Vectors
    VECTOR = "vector"  # dense vector
    SPARSE_VECTOR = "sparse_vector"  # sparse vector

    # Explicit null
    NULL = "null"


class SearchKey(DataModel):
    """Search key."""

    id: str
    """Content id."""


class SearchProperties(DataModel):
    """Search properties."""

    etag: str | None = None
    """Content ETag."""

    score: float | None = None
    """Match score."""


class SearchItem(DataModel):
    """Search item."""

    key: SearchKey
    """Search key."""

    value: dict[str, Any] | None = None
    """Content value."""

    properties: SearchProperties | None = None
    """Search properties."""


class SearchList(DataModel):
    """Search list."""

    items: list[SearchItem]
    """List of search items."""


class SearchCollectionConfig(DataModel):
    """Search collection config."""

    indexes: list[Index] | None = None
    """Indexes."""

    nconfig: dict | None = None
    """Native config parameters."""


class SearchQueryConfig(DataModel):
    """Query config."""

    paging: bool | None = False
    """A value indicating whether the results should be paged."""

    page_size: int | None = None
    """Page size."""


class SearchBatch(DataModel):
    """Search batch.

    Attributes:
        operations: List of batch operations.
    """

    operations: list[Operation] = []

    def get(
        self,
        key: str | dict | SearchKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> SearchBatch:
        """Get operation.

        Args:
            key:
                Search key.
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
        key: str | dict | SearchBatch | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> SearchBatch:
        """Put operation.

        Args:
            value:
                Search value.
            key:
                Search key.
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
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> SearchBatch:
        """Update operation.

        Args:
            key:
                Search key.
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
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> SearchBatch:
        """Delete operation.

        Args:
            key:
                Search key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self
