__all__ = ["Memory"]

from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.sqlite import SQLiteBase


class Memory(SQLiteBase):
    def __init__(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        lock_duration: float = 30,
        poll_interval: float = 0.5,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            lock_duration:
                Lock duration in seconds. Defaults to 30.
            poll_interval:
                Poll interval in seconds. Defaults to 0.5.
            nparams:
                Native parameters to SQLite client.
        """
        super().__init__(
            mode=MessagingMode.PUBSUB,
            database=":memory:",
            topic=topic,
            subscription=subscription,
            lock_duration=lock_duration,
            poll_interval=poll_interval,
            nparams=nparams,
            **kwargs,
        )
