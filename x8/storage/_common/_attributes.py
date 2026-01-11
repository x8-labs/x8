class Attribute:
    KEY = "key"
    VALUE = "value"
    METADATA = "metadata"
    PROPERTIES = "properties"
    VERSIONS = "versions"


class KeyAttribute:
    ID = "id"
    PK = "pk"
    VERSION = "version"
    LABEL = "label"


class UpdateAttribute:
    VALUE = "$value"
    METADATA = "$metadata"
    PROPERTIES = "$properties"


class SpecialAttribute:
    ID = "$id"
    PK = "$pk"
    VERSION = "$version"
    LABEL = "$label"
    ETAG = "$etag"
    MODIFIED = "$modified"
    SCORE = "$score"
    VALUE = "$value"
    METADATA = "$metadata"
    PROPERTIES = "$properties"
