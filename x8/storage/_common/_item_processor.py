import json
import uuid
from typing import Any

from x8.ql import Update, Value

from ._attributes import Attribute, KeyAttribute, SpecialAttribute


def root(attribute: str) -> str:
    return f"${attribute}"


def unroot(attribute: str) -> str:
    if attribute[0] == "$":
        return attribute[1:]
    return attribute


class ItemProcessor:
    id_embed_field: str | None
    pk_embed_field: str | None
    etag_embed_field: str | None
    id_map_field: str | None
    pk_map_field: str | None
    score_resolve_field: str | None
    local_etag: bool | None
    suppress_fields: list[str] | None
    field_types: dict | None

    def __init__(
        self,
        id_embed_field: str | None = None,
        pk_embed_field: str | None = None,
        etag_embed_field: str | None = None,
        id_map_field: str | None = None,
        pk_map_field: str | None = None,
        score_resolve_field: str | None = None,
        local_etag: bool | None = False,
        suppress_fields: list[str] | None = None,
        field_types: dict | None = None,
        **kwargs,
    ):
        self.id_embed_field = id_embed_field
        self.pk_embed_field = pk_embed_field
        self.etag_embed_field = etag_embed_field
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.score_resolve_field = score_resolve_field
        self.local_etag = local_etag
        self.suppress_fields = suppress_fields
        self.field_types = field_types

    def suppress_fields_if_needed(self, value: dict) -> dict:
        if self.suppress_fields is not None:
            return {
                k: v for k, v in value.items() if k not in self.suppress_fields
            }
        return value

    def add_embed_fields(
        self,
        value: dict,
        key: Value | None = None,
    ) -> dict:
        value_copy = None
        if key is not None:
            value_copy = value.copy()
            if self.id_embed_field is not None:
                value_copy[self.id_embed_field] = self.get_id_from_key(key=key)
            if (
                self.pk_embed_field is not None
                and self.id_embed_field != self.pk_embed_field
            ):
                value_copy[self.pk_embed_field] = self.get_pk_from_key(key=key)
            if (
                self.id_map_field is not None
                and self.id_map_field not in value
            ):
                value_copy[self.id_map_field] = self.get_id_from_key(key=key)
            if (
                self.pk_map_field is not None
                and self.pk_map_field not in value
            ):
                value_copy[self.pk_map_field] = self.get_pk_from_key(key=key)
        elif self.id_map_field is not None or self.pk_map_field is not None:
            value_copy = value.copy()
            if (
                self.id_embed_field is not None
                and self.id_map_field is not None
            ):
                value_copy[self.id_embed_field] = value[self.id_map_field]
            if (
                self.pk_embed_field is not None
                and self.pk_map_field is not None
                and self.id_embed_field != self.pk_embed_field
            ):
                value_copy[self.pk_embed_field] = value[self.pk_map_field]
        if self.local_etag and self.etag_embed_field is not None:
            if value_copy is None:
                value_copy = value.copy()
            value_copy[self.etag_embed_field] = self.generate_etag()
        if value_copy is not None:
            return value_copy
        return value

    def get_id_from_key(self, key: Value) -> Any:
        if isinstance(key, dict):
            if self.id_embed_field is not None and self.id_embed_field in key:
                return key[self.id_embed_field]
            if self.id_map_field is not None and self.id_map_field in key:
                return key[self.id_map_field]
            if SpecialAttribute.ID in key:
                return key[SpecialAttribute.ID]
            if KeyAttribute.ID in key:
                return key[KeyAttribute.ID]
        elif isinstance(key, (str | int | float | bool)):
            return key
        return None

    def get_pk_from_key(self, key: Value) -> Any:
        if isinstance(key, dict):
            if self.pk_embed_field is not None and self.pk_embed_field in key:
                return key[self.pk_embed_field]
            if self.pk_map_field is not None and self.pk_map_field in key:
                return key[self.pk_map_field]
            if SpecialAttribute.PK in key:
                return key[SpecialAttribute.PK]
            if KeyAttribute.PK in key:
                return key[KeyAttribute.PK]
        elif isinstance(key, (str | int | float | bool)):
            return key
        return None

    def get_id_pk_from_key(self, key: Value) -> tuple:
        return self.get_id_from_key(key=key), self.get_pk_from_key(key=key)

    def get_id_from_value(self, value: dict) -> Any:
        if self.id_embed_field is not None and self.id_embed_field in value:
            return value[self.id_embed_field]
        if self.id_map_field is not None and self.id_map_field in value:
            return value[self.id_map_field]
        return None

    def get_pk_from_value(self, value: dict) -> Any:
        if self.pk_embed_field is not None and self.pk_embed_field in value:
            return value[self.pk_embed_field]
        if self.pk_map_field is not None and self.pk_map_field in value:
            return value[self.pk_map_field]
        return None

    def get_id_pk_from_value(self, value: dict) -> tuple:
        return self.get_id_from_value(value=value), self.get_pk_from_value(
            value=value
        )

    def get_key_from_key(self, key: Value) -> dict:
        rkey = {}
        id = self.get_id_from_key(key=key)
        if id is not None and self.id_embed_field is not None:
            rkey[self.id_embed_field] = id
        elif id is not None and self.id_map_field is not None:
            rkey[self.id_map_field] = id
        pk = self.get_pk_from_key(key=key)
        if (
            pk is not None
            and self.pk_embed_field is not None
            and self.id_embed_field != self.pk_embed_field
        ):
            rkey[self.pk_embed_field] = pk
        elif (
            pk is not None
            and self.pk_map_field is not None
            and self.id_map_field != self.pk_map_field
        ):
            rkey[self.pk_map_field] = pk
        return rkey

    def get_key_from_value(self, value: dict) -> dict:
        key = {}
        id = self.get_id_from_value(value=value)
        if id is not None and self.id_embed_field is not None:
            key[self.id_embed_field] = id
        elif id is not None and self.id_map_field is not None:
            key[self.id_map_field] = id
        pk = self.get_pk_from_value(value=value)
        if (
            pk is not None
            and self.pk_embed_field is not None
            and self.id_embed_field != self.pk_embed_field
        ):
            key[self.pk_embed_field] = pk
        elif (
            pk is not None
            and self.pk_map_field is not None
            and self.id_map_field != self.pk_map_field
        ):
            key[self.pk_map_field] = pk
        return key

    def get_normalized_key_from_key(self, key: Value) -> dict:
        rkey = {}
        id = self.get_id_from_key(key=key)
        if id is not None:
            rkey[KeyAttribute.ID] = id
        pk = self.get_pk_from_key(key=key)
        if pk is not None:
            rkey[KeyAttribute.PK] = pk
        return rkey

    def get_normalized_key_from_value(self, value: dict) -> dict:
        key = {}
        id = self.get_id_from_value(value=value)
        if id is not None:
            key[KeyAttribute.ID] = id
        pk = self.get_pk_from_value(value=value)
        if pk is not None:
            key[KeyAttribute.PK] = pk
        return key

    def get_etag_from_value(self, value: dict) -> str | None:
        if (
            self.etag_embed_field is not None
            and self.etag_embed_field in value
        ):
            return value[self.etag_embed_field]
        return None

    def needs_local_etag(self) -> bool:
        if self.local_etag is None:
            return False
        return self.local_etag and self.etag_embed_field is not None

    def add_etag_update(self, update: Update, etag: str) -> Update:
        if self.local_etag and self.etag_embed_field is not None:
            update.put(self.etag_embed_field, etag)
        return update

    def generate_etag(self) -> str:
        return str(uuid.uuid4())

    def convert_type(self, field: str, value: Any) -> Any:
        if self.field_types is None:
            return value
        if field not in self.field_types:
            return value
        type = self.field_types[field]
        if type == "object":
            return json.loads(value)
        if type == "array":
            return json.loads(value)
        if type == "integer":
            return int(value)
        if type == "float":
            return float(value)
        if type == "number":
            try:
                return int(value)
            except ValueError:
                return float(value)
        if type == "string":
            return str(value)
        if type == "boolean":
            if isinstance(value, str):
                value_lower = value.lower()
                if value_lower in ["0", "false"]:
                    return False
                elif value_lower in ["1", "true"]:
                    return True
            return bool(value)
        if type == "string|null":
            if value == "null":
                return None
            return str(value)
        return value

    def resolve_field(self, field: str) -> str:
        if field == SpecialAttribute.ID and self.id_map_field is not None:
            return self.id_map_field
        if field == SpecialAttribute.PK and self.pk_map_field is not None:
            return self.pk_map_field
        if field == SpecialAttribute.ID and self.id_embed_field is not None:
            return self.id_embed_field
        if field == SpecialAttribute.PK and self.pk_embed_field is not None:
            return self.pk_embed_field
        if (
            field == SpecialAttribute.ETAG
            and self.etag_embed_field is not None
        ):
            return self.etag_embed_field
        if (
            field == SpecialAttribute.SCORE
            and self.score_resolve_field is not None
        ):
            return self.score_resolve_field
        return field

    def resolve_attribute_field(self, field: str, attribute: str) -> str:
        if field.startswith(f"${attribute}."):
            return field.replace(f"${attribute}.", "")
        return field

    def resolve_metadata_field(self, field: str) -> str:
        return self.resolve_attribute_field(field, Attribute.METADATA)

    def resolve_root_field(self, field: str) -> str:
        if field == SpecialAttribute.ID:
            return "key.id"
        if field == SpecialAttribute.LABEL:
            return "key.label"
        if field == SpecialAttribute.SCORE:
            return "properties.score"
        if field[0] == "$":
            return field[1:]
        return f"value.{field}"
