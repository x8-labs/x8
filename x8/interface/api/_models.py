from enum import Enum
from typing import Literal

from x8.core import Component, DataModel


class APIInfo(DataModel):
    """API Info.

    Attributes:
        host: Host address.
        port: Host port.
        ssl: SSL enabled.
    """

    host: str
    port: int | None
    ssl: bool = False


class APIAuthType(str, Enum):
    """API Auth Type Enum.

    Attributes:
        API_KEY: API Key authentication.
        BEARER_TOKEN: Bearer token (JWT) authentication.
        BASIC: Basic authentication.
    """

    API_KEY = "api_key"
    BEARER_TOKEN = "bearer_token"
    BASIC = "basic"


class APIKeyAuthConfig(DataModel):
    """API Key Auth Config.

    Attributes:
        header: Header name for API key.
        query: Query parameter name for API key.
    """

    header: str | None = None
    query: str | None = None


class APIAuth(DataModel):
    """Auth Mapping Info.

    Attributes:
        type: Auth type.
        component: Auth component.
        config: Additional config for auth.
    """

    type: APIAuthType
    component: Component | None = None
    config: APIKeyAuthConfig | None = None


class ArgSourceType(str, Enum):
    """Arg Source Type Enum.

    Attributes:
        BODY: Field from request body.
        QUERY: Field from query parameters.
        HEADER: Field from request headers.
        PATH: Field from URL path.
        AUTH: Field from auth validation.
        AUTH_USER: Field from authenticated user info.
    """

    BODY = "body"
    QUERY = "query"
    HEADER = "header"
    PATH = "path"
    AUTH = "auth"
    AUTH_USER = "auth_user"


class ArgSource(DataModel):
    """Argument Source.

    Attributes:
        type: Source type.
        field: Field name for the source.
    """

    type: ArgSourceType
    field: str | None = None


class ArgMapping(DataModel):
    """Argument Mapping Info.

    Attributes:
        name:
            Argument name.
        source:
            Argument source.
    """

    name: str
    source: ArgSource


class ResponseMapping(DataModel):
    """Response Mapping Info.

    Attributes:
        body: Body mapping field.
        headers: Headers field mapping.
        media_type: Media type.
    """

    body: str | dict[str, str] | None = None
    headers: dict[str, str] | None = None
    media_type: str | None = None


class OperationMapping(DataModel):
    """Operation Mapping Info.

    Attributes:
        name: Operation name.
        path: URL path.
        method: Http method.
        status: Status code.
        args: Arg mappings.
        auth: Auth component.
        supress: Suppress operation in API.
    """

    name: str
    path: str | None = None
    method: str | None = None
    status: int | None = None
    response: ResponseMapping | None = None
    args: list[ArgMapping] = []
    auth: APIAuth | None = None
    supress: bool = False


class ComponentMapping(DataModel):
    """Component Mapping Info.

    Attributes:
        component: Component.
        prefix: URL prefix.
        tags: Tags for the component.
        args: Common arg mappings across operations.
        auth: Auth component.
        sync_async_resolution: If both sync and async methods
            are available (with async methods prefixed with 'a'),
            how to resolve them. Can be "async", "sync", or "both".
        remove_async_prefix: If True, removes 'a' prefix from async methods
            when naming operations.
        predict_http_method: If True, predicts HTTP method
            based on operation name.
        operations: Operation mappings.
    """

    component: Component
    prefix: str | None = None
    tags: list[str] | str | None = None
    args: list[ArgMapping] = []
    auth: APIAuth | None = None
    sync_async_resolution: Literal["async", "sync", "both"] = "async"
    remove_async_prefix: bool = True
    predict_http_method: bool = False
    operations: list[OperationMapping] = []
