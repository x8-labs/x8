from typing import Any

from x8.core.exceptions import BadRequestError
from x8.storage._common import ItemProcessor, StoreOperationParser

from ._models import (
    DocumentCollectionConfig,
    DocumentItem,
    DocumentKey,
    DocumentList,
    DocumentProperties,
)


def get_collection_config(op_parser: StoreOperationParser):
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return DocumentCollectionConfig.from_dict(config)
    if isinstance(config, DocumentCollectionConfig):
        return config
    raise BadRequestError("Collection config format error")


def build_item_from_value(
    processor: ItemProcessor,
    value: dict,
    include_value: bool = False,
) -> DocumentItem:
    id = processor.get_id_from_value(value=value)
    pk = processor.get_pk_from_value(value=value)
    etag = processor.get_etag_from_value(value=value)
    key = DocumentKey(id=id, pk=pk)
    properties = None
    if etag is not None:
        properties = DocumentProperties(etag=etag)
    val = None
    if include_value:
        val = processor.suppress_fields_if_needed(value=value)
    return DocumentItem(key=key, value=val, properties=properties)


def build_item_from_parts(
    processor: ItemProcessor, key: Any, etag: Any = None
) -> DocumentItem:
    id = processor.get_id_from_key(key)
    pk = processor.get_pk_from_key(key)
    key = DocumentKey(id=id, pk=pk)
    properties = None
    if etag is not None:
        properties = DocumentProperties(etag=etag)
    return DocumentItem(key=key, properties=properties)


def build_query_result(items: list[DocumentItem]):
    return DocumentList(items=items)
