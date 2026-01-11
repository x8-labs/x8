__all__ = ["PostgreSQL"]

from typing import Any

from x8.messaging._common import MessagingMode
from x8.messaging._common.postgresql import PostgreSQLBase

from .._feature import PubSubFeature


class PostgreSQL(PostgreSQLBase):
    def __init__(
        self,
        connection_string: str,
        topic: str | None = None,
        subscription: str | None = None,
        message_table: str = "message",
        metadata_table: str = "metadata",
        lock_duration: float = 30,
        poll_interval: float = 0.5,
        nparams: dict[str, Any] = dict(),
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            connection_string:
                PostgreSQL connection string.
            topic:
                Topic name.
            subscription:
                Subscription name.
            message_table:
                PostgreSQL table name for messages.
            metadata_table:
                PostgreSQL table name for metadata.
            lock_duration:
                Lock duration in seconds. Defaults to 30.
            poll_interval:
                Poll interval in seconds. Defaults to 0.5.
            nparams:
                Native parameters to PostgreSQL client.
        """
        super().__init__(
            mode=MessagingMode.PUBSUB,
            connection_string=connection_string,
            topic=topic,
            subscription=subscription,
            message_table=message_table,
            metadata_table=metadata_table,
            lock_duration=lock_duration,
            poll_interval=poll_interval,
            nparams=nparams,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return feature not in [
            PubSubFeature.BUILTIN_DLQ,
            PubSubFeature.CONFIGURABLE_DLQ,
        ]
