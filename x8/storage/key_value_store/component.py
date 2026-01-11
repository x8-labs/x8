from typing import Any, Literal

from x8.core import Response, operation
from x8.ql import Expression, Update
from x8.storage._common import StoreComponent

from ._models import (
    KeyValueBatch,
    KeyValueItem,
    KeyValueKey,
    KeyValueKeyType,
    KeyValueList,
    KeyValueQueryConfig,
    KeyValueTransaction,
    KeyValueValueType,
)


class KeyValueStore(StoreComponent):
    type: str | None
    collection: str | None

    def __init__(
        self,
        type: Literal["binary", "string"] = "binary",
        collection: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            type:
                Value type (binary or string).
            collection:
                Default collection name.
        """
        self.type = type
        self.collection = collection

        super().__init__(**kwargs)

    @operation()
    def exists(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
    ) -> Response[bool]:
        """Check if key exists value.

        Args:
            key: Key.
            collection: Collection name.

        Returns:
            A value indicating whether key exists.
        """

    @operation()
    def get(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        start: int | None = None,
        end: int | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Get value.

        Args:
            key: Key.
            start: Start location for range request.
            end: End location for range request.
            collection: Collection name.

        Returns:
            Key value item with value.

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    def put(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        value: KeyValueValueType,
        where: str | Expression | None = None,
        expiry: int | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Put value.

        Args:
            key: Key.
            value: Value.
            where: Condition expression.
            expiry: Expiry in milliseconds.
            returning:
                A value indicating whether
                old value ("old"), new value ("new"),
                or None is returned.
            collection: Collection name.

        Returns:
            Key value item.
        """
        ...

    @operation()
    def update(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Update value.

        Args:
            key:
                Key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old value ("old"), new value ("new"),
                or None is returned.
            collection:
                Collection name.

        Returns:
            Key value item.

        Raises:
            NotFoundError:
                Key not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    def delete(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete key.

        Args:
            key: Key.
            collection: Collection name.

        Returns:
           None

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    def query(
        self,
        where: str | Expression | None = None,
        limit: int | None = None,
        continuation: str | None = None,
        config: dict | KeyValueQueryConfig | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[KeyValueList]:
        """Query items.

        Args:
            where: Condition expression.
            limit: Query limit.
            continuation: Continuation token.
            config: Query config.
            collection: Collection name.

        Returns:
            List of key value items.
        """
        ...

    @operation()
    def count(
        self,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count items.

        Args:
            where: Condition expression.
            collection: Collection name.

        Returns:
            Count of key value items.
        """
        ...

    @operation()
    def batch(
        self,
        batch: dict | KeyValueBatch,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute batch.

        Args:
            batch:
                Key value batch.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    def transact(
        self,
        transaction: dict | KeyValueTransaction,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute transaction.

        Args:
            transaction:
                Key value transaction.

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
        """Close the client.

        Returns:
            None.
        """
        ...

    @operation()
    async def aexists(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
    ) -> Response[bool]:
        """Check if key exists value.

        Args:
            key: Key.
            collection: Collection name.

        Returns:
            A value indicating whether key exists.
        """

    @operation()
    async def aget(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Get value.

        Args:
            key: Key.
            collection: Collection name.

        Returns:
            Key value item with value.

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    async def aput(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        value: KeyValueValueType,
        where: str | Expression | None = None,
        expiry: int | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Put value.

        Args:
            key: Key.
            value: Value.
            where: Condition expression.
            expiry: Expiry in milliseconds.
            returning:
                A value indicating whether
                old value ("old"), new value ("new"),
                or None is returned.
            collection: Collection name.

        Returns:
            Key value item.
        """
        ...

    @operation()
    async def aupdate(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[KeyValueItem]:
        """Update value.

        Args:
            key:
                Key.
            set:
                Update expression.
            where:
                Condition expression.
            returning:
                A value indicating whether
                old value ("old"), new value ("new"),
                or None is returned.
            collection:
                Collection name.

        Returns:
            Key value item.

        Raises:
            NotFoundError:
                Key not found.
            PreconditionFailedError:
                Condition failed.
        """
        ...

    @operation()
    async def adelete(
        self,
        key: KeyValueKeyType | dict | KeyValueKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete key.

        Args:
            key: Key.
            collection: Collection name.

        Returns:
           None

        Raises:
            NotFoundError: Key not found.
        """
        ...

    @operation()
    async def aquery(
        self,
        where: str | Expression | None = None,
        limit: int | None = None,
        continuation: str | None = None,
        config: dict | KeyValueQueryConfig | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[KeyValueList]:
        """Query items.

        Args:
            where: Condition expression.
            limit: Query limit.
            continuation: Continuation token.
            config: Query config.
            collection: Collection name.

        Returns:
            List of key value items.
        """
        ...

    @operation()
    async def acount(
        self,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        """Count items.

        Args:
            where: Condition expression.
            collection: Collection name.

        Returns:
            Count of key value items.
        """
        ...

    @operation()
    async def abatch(
        self,
        batch: dict | KeyValueBatch,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute batch.

        Args:
            batch:
                Key value batch.

        Returns:
            Batch operation results.
        """
        ...

    @operation()
    async def atransact(
        self,
        transaction: dict | KeyValueTransaction,
        **kwargs: Any,
    ) -> Response[list[Any]]:
        """Execute transaction.

        Args:
            transaction:
                Key value transaction.

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
