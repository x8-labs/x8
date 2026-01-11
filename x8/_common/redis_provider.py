from x8.core import Provider
from x8.core.exceptions import BadRequestError


class RedisProvider(Provider):
    url: str | None
    host: str | None
    port: int | None
    db: int
    username: str | None
    password: str | None
    options: dict | None

    def __init__(
        self,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        options: dict | None = None,
        **kwargs
    ):
        self.url = url
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.options = options
        super().__init__(**kwargs)

    def _get_client_and_lib(self, decode_responses: bool = True):
        import redis

        lib = redis
        roptions = self.options if self.options else {}
        if self.url is not None:
            client = redis.from_url(
                self.url, decode_responses=decode_responses, **roptions
            )
        elif self.host is not None and self.port is not None:
            client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                username=self.username,
                password=self.password,
                decode_responses=decode_responses,
                **roptions,
            )
        else:
            raise BadRequestError(
                "Redis initialization needs url or host and port"
            )
        return client, lib

    def _aget_client_and_lib(self, decode_responses: bool = True):
        import redis.asyncio as redis

        lib = redis
        roptions = self.options if self.options else {}
        if self.url is not None:
            client = redis.from_url(
                self.url, decode_responses=decode_responses, **roptions
            )
        elif self.host is not None and self.port is not None:
            client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                username=self.username,
                password=self.password,
                decode_responses=decode_responses,
                **roptions,
            )
        else:
            raise BadRequestError(
                "Redis initialization needs url or host and port"
            )
        return client, lib
