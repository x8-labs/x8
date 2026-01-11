from typing import Any

from x8.core.exceptions import BadRequestError
from x8.storage._common import StoreOperationParser

from ._models import (
    KeyValueItem,
    KeyValueKey,
    KeyValueKeyType,
    KeyValueProperties,
    KeyValueQueryConfig,
    KeyValueValueType,
)


def get_collection_name(
    provider: Any,
    op_parser: StoreOperationParser,
) -> str | None:
    collection = (
        op_parser.get_collection_name()
        or provider.collection
        or provider.__component__.collection
    )
    return collection


def get_id(key: str, collection: str | None) -> str:
    prefix = get_collection_prefix(collection)
    if not prefix:
        return key
    start = len(prefix)
    return key[start:]


def get_collection_prefix(collection: str | None) -> str | None:
    if collection:
        return f"{collection}:"
    return None


def get_collection_key(collection: str | None, id: str) -> str:
    if collection is None:
        return id
    return f"{collection}:{id}"


def build_item(
    id: KeyValueKeyType,
    value: KeyValueValueType | None = None,
    etag: str | None = None,
) -> KeyValueItem:
    key = KeyValueKey(id=id)
    return KeyValueItem(
        key=key,
        value=value,
        properties=KeyValueProperties(etag=etag),
    )


def get_query_config(op_parser: StoreOperationParser):
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return KeyValueQueryConfig.from_dict(config)
    if isinstance(config, KeyValueQueryConfig):
        return config
    raise BadRequestError("Query config format error")


def convert_value(value: Any, type: str) -> Any:
    if not value:
        return None
    if type == "binary":
        if isinstance(value, bytes):
            return value
        elif isinstance(value, str):
            return value.encode()
        else:
            return str(value).encode()
    elif type == "string":
        if isinstance(value, bytes):
            return value.decode()
        elif isinstance(value, str):
            return value
        else:
            return str(value)
    return value
