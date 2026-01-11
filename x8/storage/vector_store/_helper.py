from x8.core.exceptions import BadRequestError
from x8.storage._common import (
    Attribute,
    SpecialAttribute,
    StoreOperationParser,
)

from ._models import VectorCollectionConfig


def get_collection_config(op_parser: StoreOperationParser):
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return VectorCollectionConfig.from_dict(config)
    if isinstance(config, VectorCollectionConfig):
        return config
    raise BadRequestError("Collection config format error")


def is_value_field(field: str) -> bool:
    return field == Attribute.VALUE or field == SpecialAttribute.VALUE


def is_metadata_field(field: str) -> bool:
    return field == Attribute.METADATA or field == SpecialAttribute.METADATA
