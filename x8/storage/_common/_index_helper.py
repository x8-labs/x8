import hashlib
import re

from ._models import (
    ArrayIndex,
    AscIndex,
    CompositeIndex,
    DescIndex,
    ExcludeIndex,
    FieldIndex,
    GeospatialIndex,
    HashIndex,
    Index,
    IndexStatus,
    RangeIndex,
    SparseVectorIndex,
    TextIndex,
    VectorIndex,
    WildcardIndex,
)


class IndexHelper:
    @staticmethod
    def check_index_status(
        indexes: list, index: Index
    ) -> tuple[IndexStatus, Index | None]:
        matched_index = IndexHelper.match_index(indexes, index, False)
        if matched_index:
            return IndexStatus.EXISTS, matched_index
        matched_index = IndexHelper.match_index(indexes, index, True)
        if matched_index:
            return IndexStatus.COVERED, matched_index
        return IndexStatus.NOT_EXISTS, None

    @staticmethod
    def get_composite_types_from_name(name: str) -> list[str]:
        splits = name.split("_")
        if (
            len(splits) > 3
            and splits[1] == "composite"
            and splits[2].isnumeric()
        ):
            n = int(splits[2])
            start = 3
            end = 3 + n
            return splits[start:end]
        return []

    @staticmethod
    def get_type_from_name(name: str) -> str | None:
        splits = name.split("_")
        if len(splits) > 1:
            return splits[1]
        return None

    @staticmethod
    def hash_fields(fields: list[str]):
        concatenated = ",".join(fields)
        hash_object = hashlib.sha256(concatenated.encode("utf-8"))
        short_hash = hash_object.hexdigest()[:8]
        return short_hash

    @staticmethod
    def is_index(
        index: Index,
        match_index: Index,
        superset_match: bool = False,
    ) -> bool:
        if superset_match:
            if index.type == "asc" and match_index.type == "hash":
                pass
            elif index.type == "desc" and match_index.type == "hash":
                pass
            elif index.type == "range" and match_index.type == "asc":
                pass
            elif index.type == "range" and match_index.type == "desc":
                pass
            elif index.type == "field" and match_index.type == "hash":
                pass
            elif index.type == "field" and match_index.type == "asc":
                pass
            elif index.type == "field" and match_index.type == "desc":
                pass
            elif index.type != match_index.type:
                return False
        else:
            if index.type != match_index.type:
                return False
        if isinstance(index, CompositeIndex) and isinstance(
            match_index, CompositeIndex
        ):
            if len(index.fields) != len(match_index.fields):
                return False
            for a, b in zip(index.fields, match_index.fields):
                if not IndexHelper.is_index(a, b, superset_match):
                    return False
            return True
        else:
            if getattr(index, "field") != getattr(match_index, "field"):
                return False
            if (index, WildcardIndex) and isinstance(
                match_index, WildcardIndex
            ):
                if sorted(getattr(index, "excluded")) != sorted(
                    getattr(match_index, "excluded")
                ):
                    return False
        return True

    @staticmethod
    def match_index(
        indexes: list[Index],
        match_index: Index,
        superset_match: bool = False,
    ) -> Index | None:
        for index in indexes:
            if IndexHelper.is_index(index, match_index, superset_match):
                return index
        return None

    @staticmethod
    def convert_index_name(
        index: Index,
        collection: str | None = None,
        prefix: str | None = "idx",
    ) -> str:
        if index.name:
            return index.name
        if isinstance(
            index,
            (
                RangeIndex,
                HashIndex,
                FieldIndex,
                AscIndex,
                DescIndex,
                GeospatialIndex,
                VectorIndex,
                TextIndex,
                ArrayIndex,
                SparseVectorIndex,
                ExcludeIndex,
            ),
        ):
            field = index.field.replace(".", "_")
            field = re.sub(r"\[\d+\]", "", field)
            name = f"{index.type}_{field}"
        elif isinstance(index, CompositeIndex):
            name = f"{index.type}_{len(index.fields)}"
            for part in index.fields:
                name = f"{name}_{part.type}"
            field_hash = IndexHelper.hash_fields(
                [part.field for part in index.fields]
            )
            name = f"{name}_{field_hash}"
        elif isinstance(index, WildcardIndex):
            field = index.field.replace(".", "_").replace("*", "")
            field = re.sub(r"\[\d+\]", "", field)
            name = f"{index.type}_{field}"
            name = name.rstrip("_")
        else:
            name = "idx"
        if prefix:
            name = f"{prefix}_{name}"
        if collection:
            name = f"{name}_{collection}"
        return name
