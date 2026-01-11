__all__ = ["DataModel", "DataModelField", "Empty", "get_origin", "get_args"]

from typing import Any, Self

from pydantic import BaseModel, ConfigDict, Field
from pydantic._internal._generics import get_args as get_pydantic_get_args
from pydantic._internal._generics import get_origin as get_pydantic_origin
from pydantic_core import PydanticUndefined


def get_origin(v: Any):
    return get_pydantic_origin(v)


def get_args(v: Any):
    return get_pydantic_get_args(v)


class DataModel(BaseModel):
    """Data model."""

    model_config = ConfigDict(arbitrary_types_allowed=True, extra="ignore")

    def to_dict(self):
        return self.model_dump()

    def to_json(self, indent: int | None = None):
        return self.model_dump_json(indent=indent)

    def copy(self, deep: bool = False, **kwargs):
        return self.model_copy(deep=deep, **kwargs)

    @classmethod
    def from_dict(cls, obj: dict | None) -> Self:
        return cls.model_validate(obj)

    @classmethod
    def from_json(cls, json: str) -> Self:
        return cls.model_validate_json(json)

    @classmethod
    def get_default_value(cls, name: str) -> Any:
        field = cls.model_fields.get(name)
        if field:
            if field.default is PydanticUndefined:
                return Empty
            return field.default
        raise ValueError(f"Attribute {name} not found in model")


class Empty(DataModel):
    """Empty value."""

    pass


def DataModelField(
    alias: str | None = None,
    exclude: bool | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    **kwargs,
) -> Any:
    return Field(
        alias=alias,
        exclude=exclude,
        min_length=min_length,
        max_length=max_length,
        **kwargs,
    )
