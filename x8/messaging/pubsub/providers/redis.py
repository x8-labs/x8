from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.redis import RedisBase

from .._feature import PubSubFeature


class Redis(RedisBase):
    def __init__(
        self,
        topic: str | None = None,
        subscription: str | None = None,
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
            topic:
                Topic name.
            subscription:
                Subscription name.
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
            mode=MessagingMode.PUBSUB,
            topic=topic,
            subscription=subscription,
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
            PubSubFeature.BUILTIN_DLQ,
            PubSubFeature.CONFIGURABLE_DLQ,
        ]
