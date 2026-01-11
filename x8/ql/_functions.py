from typing import Literal

from ._models import Field, Function, GeoPoint, Value

TextSearchQueryType = Literal[
    "simple",
    "full",
    "phrase",
    "phrase_prefix",
    "prefix",
    "best_fields",
    "most_fields",
    "cross_fields",
]

TextSearchMatchMode = Literal[
    "and",
    "or",
]


class QueryFunctionName:
    EXISTS = "exists"
    NOT_EXISTS = "not_exists"

    IS_DEFINED = "is_defined"
    IS_NOT_DEFINED = "is_not_defined"
    IS_TYPE = "is_type"

    LENGTH = "length"
    CONTAINS = "contains"
    STARTS_WITH = "starts_with"
    ENDS_WITH = "ends_with"

    ARRAY_LENGTH = "array_length"
    ARRAY_CONTAINS = "array_contains"
    ARRAY_CONTAINS_ANY = "array_contains_any"

    STARTS_WITH_DELIMITED = "starts_with_delimited"

    RANDOM = "random"
    NOW = "now"

    VECTOR_SEARCH = "vector_search"
    SPARSE_VECTOR_SEARCH = "sparse_vector_search"
    HYBRID_VECTOR_SEARCH = "hybrid_vector_search"
    TEXT_SEARCH = "text_search"
    HYBRID_TEXT_SEARCH = "hybrid_text_search"

    GEO_SEARCH_DISTANCE = "geo_search_distance"
    GEO_SEARCH_POLYGON = "geo_search_polygon"
    GEO_SEARCH_BBOX = "geo_search_bbox"


class QueryFunction:
    @staticmethod
    def exists() -> Function:
        return Function(name=QueryFunctionName.EXISTS)

    @staticmethod
    def not_exists() -> Function:
        return Function(name=QueryFunctionName.NOT_EXISTS)

    @staticmethod
    def is_defined(field: str) -> Function:
        return Function(
            name=QueryFunctionName.IS_DEFINED, args=[Field(path=field)]
        )

    @staticmethod
    def is_not_defined(field: str) -> Function:
        return Function(
            name=QueryFunctionName.IS_NOT_DEFINED, args=[Field(path=field)]
        )

    @staticmethod
    def is_type(field: str, type: str) -> Function:
        return Function(
            name=QueryFunctionName.IS_TYPE,
            args=[
                Field(path=field),
                type,
            ],
        )

    @staticmethod
    def starts_with(field: str, value: str) -> Function:
        return Function(
            name=QueryFunctionName.STARTS_WITH,
            args=[
                Field(path=field),
                value,
            ],
        )

    @staticmethod
    def ends_with(field: str, value: str) -> Function:
        return Function(
            name=QueryFunctionName.ENDS_WITH,
            args=[
                Field(path=field),
                value,
            ],
        )

    @staticmethod
    def contains(field: str, value: str) -> Function:
        return Function(
            name=QueryFunctionName.CONTAINS,
            args=[
                Field(path=field),
                value,
            ],
        )

    @staticmethod
    def length(field: str) -> Function:
        return Function(
            name=QueryFunctionName.LENGTH, args=[Field(path=field)]
        )

    @staticmethod
    def array_contains(field: str, value: Value) -> Function:
        return Function(
            name=QueryFunctionName.ARRAY_CONTAINS,
            args=[
                Field(path=field),
                value,
            ],
        )

    @staticmethod
    def array_contains_any(field: str, value: list[Value]) -> Function:
        return Function(
            name=QueryFunctionName.ARRAY_CONTAINS_ANY,
            args=[
                Field(path=field),
                value,
            ],
        )

    @staticmethod
    def array_length(field: str) -> Function:
        return Function(
            name=QueryFunctionName.ARRAY_LENGTH, args=[Field(path=field)]
        )

    @staticmethod
    def starts_with_delimited(
        field: str,
        value: str,
        delimiter: str,
    ) -> Function:
        return Function(
            name=QueryFunctionName.STARTS_WITH_DELIMITED,
            args=[Field(path=field), value, delimiter],
        )

    @staticmethod
    def random() -> Function:
        return Function(name=QueryFunctionName.RANDOM)

    @staticmethod
    def now() -> Function:
        return Function(name=QueryFunctionName.NOW)

    @staticmethod
    def vector_search_old(
        vector: list[float] | None = None,
        sparse_vector: dict[int, float] | None = None,
        field: str | None = None,
    ):
        return Function(
            name=QueryFunctionName.VECTOR_SEARCH,
            named_args=dict(
                vector=vector, sparse_vector=sparse_vector, field=field
            ),
        )

    @staticmethod
    def vector_search(
        vector: list[float] | None = None,
        field: str | None = None,
        k: int | None = None,
        num_candidates: int | None = None,
    ) -> Function:
        return Function(
            name=QueryFunctionName.VECTOR_SEARCH,
            named_args=dict(
                vector=vector,
                field=field,
                k=k,
                num_candidates=num_candidates,
            ),
        )

    @staticmethod
    def sparse_vector_search(
        sparse_vector: dict[int, float] | None = None, field: str | None = None
    ) -> Function:
        return Function(
            name=QueryFunctionName.SPARSE_VECTOR_SEARCH,
            named_args=dict(
                sparse_vector=sparse_vector,
                field=field,
            ),
        )

    @staticmethod
    def hybrid_vector_search(
        # dense vector
        vector: list[float] | None = None,
        vector_field: str | None = None,
        k: int | None = None,
        num_candidates: int | None = None,
        # sparse vector
        sparse_vector: dict[int, float] | None = None,
        sparse_vector_field: str | None = None,
        # fusion
        hybrid_mode: (
            Literal["add", "multiply", "max", "min", "rrf"] | None
        ) = None,
        vector_weight: float | None = None,
        sparse_vector_weight: float | None = None,
    ) -> Function:
        return Function(
            name=QueryFunctionName.HYBRID_VECTOR_SEARCH,
            named_args=dict(
                # dense
                vector=vector,
                vector_field=vector_field,
                k=k,
                num_candidates=num_candidates,
                # sparse
                sparse_vector=sparse_vector,
                sparse_vector_field=sparse_vector_field,
                # fusion
                hybrid_mode=hybrid_mode,
                vector_weight=vector_weight,
                sparse_vector_weight=sparse_vector_weight,
            ),
        )

    @staticmethod
    def text_search(
        query: str,
        fields: list[str] | None = None,
        match_mode: TextSearchMatchMode | None = None,
        query_type: TextSearchQueryType | None = None,
        fuzziness: int | str | None = None,
        minimum_should_match: int | str | None = None,
        analyzer: str | None = None,
        boost: dict[str, float] | None = None,
    ) -> Function:
        return Function(
            name=QueryFunctionName.TEXT_SEARCH,
            named_args=dict(
                query=query,
                fields=fields,
                match_mode=match_mode,
                query_type=query_type,
                fuzziness=fuzziness,
                minimum_should_match=minimum_should_match,
                analyzer=analyzer,
                boost=boost,
            ),
        )

    @staticmethod
    def hybrid_text_search(
        # text
        query: str | None = None,
        fields: list[str] | None = None,
        match_mode: TextSearchMatchMode | None = None,
        query_type: TextSearchQueryType | None = None,
        fuzziness: int | str | None = None,
        minimum_should_match: int | str | None = None,
        analyzer: str | None = None,
        boost: dict[str, float] | None = None,
        # vector
        vector: list[float] | None = None,
        vector_field: str | None = None,
        k: int | None = None,
        num_candidates: int | None = None,
        # sparse vector
        sparse_vector: dict[int, float] | None = None,
        sparse_vector_field: str | None = None,
        # fusion
        hybrid_mode: (
            Literal["add", "multiply", "max", "min", "rrf"] | None
        ) = None,
        text_weight: float | None = None,
        vector_weight: float | None = None,
    ) -> Function:
        return Function(
            name=QueryFunctionName.HYBRID_TEXT_SEARCH,
            named_args=dict(
                query=query,
                fields=fields,
                term_match_mode=match_mode,
                query_type=query_type,
                fuzziness=fuzziness,
                minimum_should_match=minimum_should_match,
                analyzer=analyzer,
                boost=boost,
                vector=vector,
                vector_field=vector_field,
                k=k,
                num_candidates=num_candidates,
                sparse_vector=sparse_vector,
                sparse_vector_field=sparse_vector_field,
                hybrid_mode=hybrid_mode,
                text_weight=text_weight,
                vector_weight=vector_weight,
            ),
        )

    @staticmethod
    def geo_search_distance(
        field: str,
        center: dict | GeoPoint,
        distance: float,
        unit: Literal["m", "km", "mi"] = "m",
    ) -> Function:
        return Function(
            name=QueryFunctionName.GEO_SEARCH_DISTANCE,
            named_args=dict(
                field=Field(path=field),
                center=(
                    center
                    if isinstance(center, dict)
                    else {"lat": center.lat, "lon": center.lon}
                ),
                distance=distance,
                unit=unit,
            ),
        )

    @staticmethod
    def geo_search_polygon(
        field: str,
        points: list[dict] | list[GeoPoint],
    ) -> Function:
        return Function(
            name=QueryFunctionName.GEO_SEARCH_POLYGON,
            named_args=dict(
                field=Field(path=field),
                points=[
                    p if isinstance(p, dict) else {"lat": p.lat, "lon": p.lon}
                    for p in points
                ],
            ),
        )

    @staticmethod
    def geo_search_bbox(
        field: str,
        top_left: dict | GeoPoint,
        bottom_right: dict | GeoPoint,
    ) -> Function:
        return Function(
            name=QueryFunctionName.GEO_SEARCH_BBOX,
            named_args=dict(
                field=Field(path=field),
                top_left=(
                    top_left
                    if isinstance(top_left, dict)
                    else {"lat": top_left.lat, "lon": top_left.lon}
                ),
                bottom_right=(
                    bottom_right
                    if isinstance(bottom_right, dict)
                    else {
                        "lat": bottom_right.lat,
                        "lon": bottom_right.lon,
                    }
                ),
            ),
        )
