from enum import Enum
from typing import Any, Literal, Union

from ...core import DataModel


class MatchCondition(DataModel):
    """Match condition."""

    exists: bool | None = None
    """Check item exists."""

    if_match: str | None = None
    """If match."""

    if_none_match: str | None = None
    """If none match."""

    if_modified_since: float | None = None
    """If modified since."""

    if_unmodified_since: float | None = None
    """If unmodified since."""

    if_version_match: str | None = None
    """If version match."""

    if_version_not_match: str | None = None
    """If version not match."""


class BaseIndex(DataModel):
    """Store index."""

    type: str
    """Index type."""

    name: str | None = None
    """Index name."""

    nconfig: Any | None = None
    """Native config options."""


class WildcardIndex(BaseIndex):
    """Wildcard index."""

    field: str = "*"
    """Wilcard path."""

    excluded: list[str] = []
    """Excluded paths."""

    type: Literal["wildcard"] = "wildcard"
    """Index type."""


class FieldIndex(BaseIndex):
    """Field index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["field"] = "field"
    """Index type."""


class ExcludeIndex(BaseIndex):
    """Exclude index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["exclude"] = "exclude"


class RangeIndex(BaseIndex):
    """Field index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["range"] = "range"
    """Index type."""


class HashIndex(BaseIndex):
    """Field index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["hash"] = "hash"
    """Index type."""


class AscIndex(BaseIndex):
    """Ascending index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["asc"] = "asc"
    """Index type."""


class DescIndex(BaseIndex):
    """Descending index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["desc"] = "desc"
    """Index type."""


class ArrayIndex(BaseIndex):
    """Array index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["array"] = "array"
    """Index type."""


class TTLIndex(BaseIndex):
    """TTL index."""

    field: str
    """Index field."""

    type: Literal["ttl"] = "ttl"
    """Index type."""


class GeospatialFieldType(str, Enum):
    """Geospatial field type."""

    POINT = "point"
    """Point."""

    SHAPE = "shape"
    """Shape."""


class GeospatialIndex(BaseIndex):
    """Geospatial index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    type: Literal["geospatial"] = "geospatial"
    """Index type."""


class TextSimilarityAlgorithm(str, Enum):
    """Text scoring algorithm for text index."""

    BM25 = "BM25"
    """BM25 algorithm."""

    BOOLEAN = "boolean"
    """Boolean similarity."""


class TextIndex(BaseIndex):
    """Text index."""

    field: str
    """Index field."""

    field_type: str | None = None
    """Field type."""

    variant: str | None = None
    """Text index variant."""

    similarity: TextSimilarityAlgorithm | None = None
    """Text scoring algorithm."""

    type: Literal["text"] = "text"
    """Index type."""


class VectorIndexMetric(str, Enum):
    """Vector index metric."""

    DOT_PRODUCT = "dot_product"
    COSINE = "cosine"
    EUCLIDEAN = "euclidean"
    MANHATTAN = "manhattan"
    HAMMING = "hamming"
    MAX_INNER_PRODUCT = "max_inner_product"


class VectorIndexStructure(str, Enum):
    """Vector index structure."""

    FLAT = "flat"
    HNSW = "hnsw"
    DISKANN = "diskann"

    INT8_HNSW = "int8_hnsw"
    INT4_HNSW = "int4_hnsw"
    BQ_HNSW = "bbq_hnsw"

    INT8_FLAT = "int8_flat"
    INT4_FLAT = "int4_flat"
    BQ_FLAT = "bbq_flat"
    QUANTIZED_FLAT = "quantized_flat"


class VectorIndex(BaseIndex):
    """Vector index."""

    field: str = "vector"
    """Index field name."""

    field_type: str | None = None
    """Field type."""

    dimension: int = 4
    """Vector dimension."""

    metric: VectorIndexMetric = VectorIndexMetric.DOT_PRODUCT
    """Index metric."""

    structure: VectorIndexStructure | None = None
    """Vector index structure."""

    m: int | None = None
    """The number of neighbors each node will be
    connected to in the HNSW graph."""

    ef_construction: int | None = None
    """The number of candidates to track while assembling
    the list of nearest neighbors for each new node. """

    ef_runtime: int | None = None
    """	Max top candidates during KNN search.
    Higher values increase accuracy,
    but also increase search latency."""

    epsilon: float | None = None
    """Relative factor that sets the boundaries in which a
    range query may search for candidates. That is, vector candidates
    whose distance from the query vector is radius * (1 + EPSILON) are
    potentially scanned, allowing more extensive search and more
    accurate results, at the expense of run time."""

    confidence_interval: float | None = None
    """The confidence interval to use when quantizing the vectors."""

    partitions: int | None = None
    """Partitions for IVF flat."""

    type: Literal["vector"] = "vector"
    """Index type."""


class SparseVectorIndex(BaseIndex):
    """Sparse vector index."""

    field: str = "sparse_vector"
    """Sparse vector field name."""

    field_type: str | None = None
    """Field type."""

    type: Literal["sparse_vector"] = "sparse_vector"
    """Index type."""


class CompositeIndex(BaseIndex):
    """Composite index."""

    fields: list[
        FieldIndex
        | HashIndex
        | AscIndex
        | DescIndex
        | RangeIndex
        | ArrayIndex
        | GeospatialIndex
        | TextIndex
        | VectorIndex
        | SparseVectorIndex
    ]
    """Composite index fields."""

    type: Literal["composite"] = "composite"
    """Index type."""


class RankMethod(str, Enum):
    LOG = "log"
    SATURATION = "saturation"
    LINEAR = "linear"
    FRESHNESS = "freshness"  # for Azure etc.


class RankIndex(BaseIndex):
    field: str
    method: RankMethod = RankMethod.LOG
    weight: float | None = None
    range_start: float | None = None
    range_end: float | None = None
    type: Literal["rank"] = "rank"


class IndexType:
    FIELD = "field"
    COMPOSITE = "composite"
    ASC = "asc"
    DESC = "desc"
    ARRAY = "array"
    EXCLUDE = "exclude"
    TTL = "ttl"
    GEOSPATIAL = "geospatial"
    TEXT = "text"
    VECTOR = "vector"
    SPARSE_VECTOR = "sparse_vector"
    RANGE = "range"
    HASH = "hash"
    WILDCARD = "wildcard"
    RANK = "rank"


Index = Union[
    FieldIndex,
    CompositeIndex,
    AscIndex,
    DescIndex,
    ArrayIndex,
    TTLIndex,
    GeospatialIndex,
    TextIndex,
    VectorIndex,
    SparseVectorIndex,
    RangeIndex,
    HashIndex,
    ExcludeIndex,
    WildcardIndex,
    BaseIndex,
]


class IndexStatus(str, Enum):
    """Index status."""

    CREATED = "created"
    EXISTS = "exists"
    COVERED = "covered"

    DROPPED = "dropped"
    NOT_EXISTS = "not_exists"

    NOT_SUPPORTED = "not_supported"
    ERROR = "error"


class IndexResult(DataModel):
    """Index result."""

    status: IndexStatus
    """Index status."""

    index: Index | None = None
    """Index."""

    error: str | None = None
    """Error string."""


class CollectionStatus(str, Enum):
    """Collection status"""

    CREATED = "created"
    EXISTS = "exists"

    DROPPED = "dropped"
    NOT_EXISTS = "not_exists"


class CollectionResult(DataModel):
    """Collection result."""

    status: CollectionStatus
    """Collection status."""

    indexes: list[IndexResult] = []
    """Index results."""


class FieldDefinition(DataModel):
    """Field definition."""

    attribute: str | None
    """Attribute name."""

    field: str
    """Field name."""

    field_type: str | None
    """Field type."""
