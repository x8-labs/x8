from __future__ import annotations

from typing import Any

from x8.core import DataModel, Operation
from x8.storage._common import SparseVectorIndex, StoreOperation, VectorIndex


class VectorSearchArgs(DataModel):
    """Vector search args."""

    vector: list[float] | None = None
    """Vector floats."""

    sparse_vector: dict[int, float] | None = None
    """Sparse vector dictionary."""

    field: str | None = None
    """Vector field name."""


class VectorValue(DataModel):
    """Vector value."""

    vector: list[float] | None = None
    """Vector floats."""

    sparse_vector: dict[int, float] | None = None
    """Sparse vector dictionary."""

    content: str | None = None
    """Text context."""


class VectorKey(DataModel):
    """Vector key."""

    id: str
    """Vector id."""


class VectorProperties(DataModel):
    """Vector properties."""

    score: float | None = None
    """Match score."""


class VectorItem(DataModel):
    """Vector item."""

    key: VectorKey
    """Vector key."""

    value: VectorValue | None = None
    """Vector value."""

    metadata: dict[str, Any] | None = None
    """Vector metadata."""

    properties: VectorProperties | None = None
    """Vector properties."""


class VectorList(DataModel):
    """Vector list."""

    items: list[VectorItem]
    """List of vector items."""


class VectorCollectionConfig(DataModel):
    """Vector collection config."""

    vector_index: VectorIndex | None = None
    """Vector index."""

    sparse_vector_index: SparseVectorIndex | None = None
    """Sparse vector index."""

    nconfig: dict[str, Any] | None = None
    """Native config."""


class VectorBatch(DataModel):
    """Vector batch.

    Attributes:
        operations: List of batch operations.
    """

    operations: list[Operation] = []

    def get(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorBatch:
        """Get vector operation.

        Args:
            key:
                Vector key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        metadata: dict | None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorBatch:
        """Put vector operation.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            metadata:
                Custom metadata.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def delete(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorBatch:
        """Delete vector operation.

        Args:
            key:
                Vector key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self


class VectorTransaction(DataModel):
    """Vector transaction.

    Attributes:
        operations: List of transaction operations.
    """

    operations: list[Operation] = []

    def get(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorTransaction:
        """Get vector operation.

        Args:
            key:
                Vector key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        key: str | dict | VectorKey,
        value: list[float] | dict | VectorValue,
        metadata: dict | None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorTransaction:
        """Put vector operation.

        Args:
            key:
                Vector key.
            value:
                Vector value.
            metadata:
                Custom metadata.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def delete(
        self,
        key: str | dict | VectorKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> VectorTransaction:
        """Delete vector operation.

        Args:
            key:
                Vector key.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self
