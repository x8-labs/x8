from __future__ import annotations

from typing import Any, Union

from x8.core import DataModel, Operation
from x8.ql import Expression, Update
from x8.storage._common import StoreOperation

KeyValueKeyType = Union[str, bytes, memoryview]
KeyValueValueType = Union[str, int, float, bytes, memoryview]


class KeyValueKey(DataModel):
    """Key Value key."""

    id: KeyValueKeyType
    """Key Value id.
    """


class KeyValueItem(DataModel):
    """Config item."""

    key: KeyValueKey
    """Key.
    """

    value: KeyValueValueType | None = None
    """Value.
    """

    properties: KeyValueProperties
    """Key value properties.
    """


class KeyValueProperties(DataModel):
    """Key Value properties.

    Attributes:
        etag: Key Value ETag.
    """

    etag: str | None = None


class KeyValueList(DataModel):
    """Key Value item list."""

    items: list[KeyValueItem]
    """List of Key Value items."""

    continuation: str | None = None
    """Continuation token."""


class KeyValueQueryConfig(DataModel):
    """Query config."""

    paging: bool | None = False
    """A value indicating whether the results should be paged."""

    page_size: int | None = None
    """Page size."""


class KeyValueBatch(DataModel):
    """Key value batch.

    Attributes:
        operations: List of batch operations.
    """

    operations: list[Operation] = []

    def exists(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
    ) -> KeyValueBatch:
        """Check if key exists value.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.EXISTS, args=locals())
        )
        return self

    def get(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueBatch:
        """Get value.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        value: KeyValueValueType,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueBatch:
        """Put value.

        Args:
            key: Key.
            value: Value.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def delete(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueBatch:
        """Delete key.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self


class KeyValueTransaction(DataModel):
    """Key value transaction.

    Attributes:
        operations: List of transaction operations.
    """

    operations: list[Operation] = []

    def exists(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
    ) -> KeyValueTransaction:
        """Check if key exists value.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.EXISTS, args=locals())
        )
        return self

    def get(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueTransaction:
        """Get value.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.GET, args=locals())
        )
        return self

    def put(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        value: KeyValueValueType,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueTransaction:
        """Put value.

        Args:
            key: Key.
            value: Value.
            where: Condition expression.
            returning:
                A value indicating whether old or new value is returned.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.PUT, args=locals())
        )
        return self

    def update(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueTransaction:
        """Update value.

        Args:
            key:
                Key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether updated value is returned.
            collection:
                Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.UPDATE, args=locals())
        )
        return self

    def delete(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> KeyValueTransaction:
        """Delete key.

        Args:
            key: Key.
            collection: Collection name.
        """
        self.operations.append(
            Operation.normalize(name=StoreOperation.DELETE, args=locals())
        )
        return self
