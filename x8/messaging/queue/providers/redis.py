from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.redis import RedisBase

from .._feature import QueueFeature


class Redis(RedisBase):
    def __init__(
        self,
        queue: str | None = None,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: str | int = 0,
        username: str | None = None,
        password: str | None = None,
        options: dict | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            queue:
                Queue name.
            url:
                Redis URL.
            host:
                Redis host.
            port:
                Redis port.
            db:
                Redis database. Defaults to 0.
            username:
                Redis username.
            password:
                Redis password.
            options:
                Native parameters to Redis client.
        """
        super().__init__(
            mode=MessagingMode.QUEUE,
            queue=queue,
            url=url,
            host=host,
            port=port,
            db=db,
            username=username,
            password=password,
            options=options,
            nparams=nparams,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return feature not in [
            QueueFeature.BUILTIN_DLQ,
            QueueFeature.CONFIGURABLE_DLQ,
        ]
