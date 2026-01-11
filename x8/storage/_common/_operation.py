from __future__ import annotations

from typing import Any

from x8.core import Operation
from x8.ql import Collection, Expression, OrderBy, Select, Update, Value


class StoreOperation:
    EXISTS = "exists"
    GET = "get"
    PUT = "put"
    UPDATE = "update"
    DELETE = "delete"
    QUERY = "query"
    COUNT = "count"
    BATCH = "batch"
    TRANSACT = "transact"
    COPY = "copy"
    GENERATE = "generate"
    WATCH = "watch"
    CLOSE = "close"
    GET_METADATA = "get_metadata"
    GET_PROPERTIES = "get_properties"
    GET_VERSIONS = "get_versions"
    UPDATE_METADATA = "update_metadata"

    CREATE_COLLECTION = "create_collection"
    DROP_COLLECTION = "drop_collection"
    LIST_COLLECTIONS = "list_collections"
    HAS_COLLECTION = "has_collection"

    CREATE_INDEX = "create_index"
    DROP_INDEX = "drop_index"
    LIST_INDEXES = "list_indexes"

    EXECUTE = "__execute__"

    @staticmethod
    def execute(
        statement: str,
        params: dict[str, Any] | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.EXECUTE, locals())

    @staticmethod
    def create_collection(
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.CREATE_COLLECTION, locals())

    @staticmethod
    def drop_collection(
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.DROP_COLLECTION, locals())

    @staticmethod
    def list_collections(
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.LIST_COLLECTIONS, locals())

    @staticmethod
    def exists(
        key: Any = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.EXISTS, locals())

    @staticmethod
    def get(
        key: Any = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.GET, locals())

    @staticmethod
    def get_properties(
        key: Any = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.GET_PROPERTIES, locals())

    @staticmethod
    def get_metadata(
        key: Any = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.GET_METADATA, locals())

    @staticmethod
    def put(
        key: Any = None,
        value: Value | None = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.PUT, locals())

    @staticmethod
    def update(
        key: Any = None,
        set: str | Update | None = None,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.UPDATE, locals())

    @staticmethod
    def delete(
        key: Any = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.DELETE, locals())

    @staticmethod
    def query(
        select: str | Select | None = None,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.QUERY, locals())

    @staticmethod
    def count(
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.COUNT, locals())

    @staticmethod
    def batch(
        batch: dict | Any,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.BATCH, locals())

    @staticmethod
    def transact(
        transaction: dict | Any,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.TRANSACT, locals())

    @staticmethod
    def copy(
        key: Any = None,
        source: Value = None,
        where: str | Expression | None = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.COPY, locals())

    @staticmethod
    def generate(
        key: Any = None,
        collection: str | Collection | None = None,
        **kwargs,
    ) -> Operation:
        return Operation.normalize(StoreOperation.GENERATE, locals())

    @staticmethod
    def close(**kwargs) -> Operation:
        return Operation.normalize(StoreOperation.CLOSE, locals())
