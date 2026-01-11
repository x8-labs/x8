from __future__ import annotations

from enum import Enum
from typing import Any

from x8.core import DataModel, Operation
from x8.ql import Expression
from x8.storage._common import StoreOperation


class ObjectStoreClass(str, Enum):
    """Object Store class."""

    HOT = "hot"
    COOL = "cool"
    COLD = "cold"
    ARCHIVE = "archive"


class ObjectKey(DataModel):
    """Object key."""

    id: str
    """Object id."""

    version: str | None = None
    """Object version."""


class ObjectProperties(DataModel):
    """Object properties."""

    cache_control: str | None = None
    """Cache control."""

    content_disposition: str | None = None
    """Content disposition."""

    content_encoding: str | None = None
    """Content encoding."""

    content_language: str | None = None
    """Content language."""

    content_length: int | None = None
    """Content length."""

    content_md5: str | None = None
    """Content md5."""

    content_type: str | None = None
    """Content type."""

    crc32c: str | None = None
    """Crc32c value."""

    expires: float | None = None
    """Expiration time."""

    last_modified: float | None = None
    """Last modified time."""

    etag: str | None = None
    """Etag value."""

    storage_class: str | None = None
    """Storage class."""


class ObjectVersion(DataModel):
    """Object version."""

    version: str | None
    """Version id."""

    properties: ObjectProperties | None = None
    """Object properties."""

    metadata: dict | None = None
    """Custom metadata."""

    latest: bool | None = None
    """A value indicating whether this is the latest version."""


class ObjectItem(DataModel):
    """Object item."""

    key: ObjectKey
    """Object key."""

    value: bytes | None = None
    """Object value."""

    metadata: dict | None = None
    """Object metadata."""

    properties: ObjectProperties | None = None
    """Object properties."""

    versions: list[ObjectVersion] | None = None
    """Object vesions."""

    url: str | None = None
    """Object url."""


class ObjectList(DataModel):
    """Object list."""

    items: list[ObjectItem]
    """List of items."""

    continuation: str | None = None
    """Continuation token."""

    prefixes: list[str] | None = None
    """List of prefixes."""


class ObjectCollectionConfig(DataModel):
    """Object collection config."""

    acl: str | None = None
    """Access control."""

    versioned: bool | None = None
    """A value indicating whether the store is versioned."""

    nconfig: dict[str, Any] | None = None
    """Native config for providers."""


class ObjectTransferConfig(DataModel):
    """Transfer config."""

    multipart: bool | None = False
    """A value indicating whether the transfer should be multi-part."""

    chunksize: int | None = None
    """Chunk size for multi-part transfer."""

    concurrency: int | None = None
    """Number of concurrent transfer."""

    nconfig: dict[str, Any] | None = None
    """Native config for providers."""


class ObjectSource(DataModel):
    """Object source."""

    id: str
    """Object id."""

    version: str | None = None
    """Object version."""

    collection: str | None = None
    """Collection name."""


class ObjectQueryConfig(DataModel):
    """Query config."""

    paging: bool | None = False
    """A value indicating whether the results should be paged."""

    page_size: int | None = None
    """Page size."""


class ObjectBatch(DataModel):
    """Object batch.

    Attributes:
        operations: List of batch operations.
    """

    operations: list[Operation] = []

    def delete(
        self,
        key: str | dict | ObjectKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> ObjectBatch:
        """Delete operation.

        Args:
            key:
                Object key.
            where:
                Condition expression.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self
