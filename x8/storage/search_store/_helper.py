from typing import Any

from x8.core import DataModel
from x8.core.exceptions import BadRequestError
from x8.storage._common import ItemProcessor

from ._models import SearchCollectionConfig, SearchKey


class Helper:
    @staticmethod
    def get_collection_config(
        config: dict | SearchCollectionConfig | None,
    ) -> SearchCollectionConfig | None:
        if config is None:
            return None
        if isinstance(config, dict):
            return SearchCollectionConfig.from_dict(config)
        if isinstance(config, SearchCollectionConfig):
            return config
        raise BadRequestError("Collection config format error")

    @staticmethod
    def get_value(value: dict[str, Any] | DataModel) -> dict[str, Any]:
        if isinstance(value, DataModel):
            return value.to_dict()
        return value

    @staticmethod
    def get_id(
        processor: ItemProcessor,
        key: str | dict | SearchKey | None = None,
        value: dict[str, Any] | DataModel | None = None,
    ):
        if key:
            if isinstance(key, SearchKey):
                key = SearchKey.to_dict(key)
            return processor.get_id_from_key(key)
        if value:
            return processor.get_id_from_value(Helper.get_value(value))
        raise BadRequestError("Either key or value must be provided to get id")
