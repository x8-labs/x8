from __future__ import annotations

from enum import Enum
from typing import Any, Union

from x8.core import DataModel, Operation

from ._operation import MessagingOperation


class MessageKey(DataModel):
    """Message key."""

    id: str | None = None
    """Message id."""

    nref: Any = None
    """Reference to the underlying native message handle."""


class MessageProperties(DataModel):
    """Message properties."""

    message_id: str | None = None
    """Message id for deduplication"""

    group_id: str | None = None
    """Id for grouping messages.
    Messages with the same id are delivered in order."""

    content_type: str | None = None
    """Content type of the message."""

    enqueued_time: float | None = None
    """Enqueued time in utc."""

    delivery_count: int | None = None
    """Delivery count."""


class MessageItem(DataModel):
    """Message item."""

    key: MessageKey | None = None
    """Message key."""

    value: MessageValueType | None = None
    """Message value."""

    metadata: dict | None = None
    """Message metadata."""

    properties: MessageProperties | None = None
    """Message properties."""


class MessagePutConfig(DataModel):
    """Message put config."""

    delay: int | None = None
    """Scheduled delay time to send."""


class MessagePullConfig(DataModel):
    """Message get config."""

    max_count: int | None = 1
    """Maximum number of messages in batch. Defaults to 1."""

    max_wait_time: float | None = None
    "Maximum wait time in seconds. Defaults to None."

    visibility_timeout: float | None = None
    """Visibility timeout in seconds. Defaults to None."""


class MessageBatch(DataModel):
    operations: list[Operation] = []

    def put(
        self,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: dict | MessageProperties | None = None,
        config: dict | MessagePutConfig | None = None,
        **kwargs: Any,
    ) -> MessageBatch:
        """Put message in the queue.

        Args:
            value:
                Message value.
            metadata:
                Message metadata.
            properties:
                Message properties.
            config:
                Put config.
        """
        self.operations.append(
            Operation.normalize(name=MessagingOperation.PUT, args=locals())
        )
        return self


MessageValueType = Union[str, bytes, dict[str, Any]]


class QueueConfig(DataModel):
    """Queue config."""

    visibility_timeout: float | None = None
    """Visibility timeout in seconds."""

    ttl: float | None = None
    """Time to live in seconds."""

    max_delivery_count: int | None = None
    """Maximum delivery count."""

    dlq_nref: Any = None
    """Reference to the underlying native DLQ handle."""

    fifo: bool | None = None
    """Whether the queue is FIFO."""

    nconfig: dict[str, Any] | None = None
    """Native config for providers."""


class TopicConfig(DataModel):
    """Topic config."""

    ttl: float | None = None
    """Time to live in seconds."""

    nconfig: dict[str, Any] | None = None
    """Native config for providers."""


class SubscriptionConfig(DataModel):
    """Subscription config."""

    visibility_timeout: float | None = None
    """Visibility timeout in seconds."""

    ttl: float | None = None
    """Time to live in seconds."""

    max_delivery_count: int | None = None
    """Maximum delivery count."""

    dlq_nref: Any = None
    """Reference to the underlying native DLQ handle."""

    fifo: bool | None = None
    """Whether the queue is FIFO."""

    nconfig: dict[str, Any] | None = None
    """Native config for providers."""


class QueueInfo(DataModel):
    """Queue info."""

    name: str | None = None
    """Queue name."""

    active_message_count: int | None = None
    """Number of active messages."""

    inflight_message_count: int | None = None
    """Number of inflight messages."""

    scheduled_message_count: int | None = None
    """Number of scheduled messages."""

    config: QueueConfig | None = None
    """Queue config."""

    nref: Any = None
    """Reference to the underlying native queue."""


class TopicInfo(DataModel):
    """Topic info."""

    name: str | None = None
    """Topic name."""

    subscription_count: int | None = None
    """Number of subscriptions."""

    scheduled_message_count: int | None = None
    """Number of scheduled messages."""

    config: TopicConfig | None = None
    """Topic config."""

    nref: Any = None
    """Reference to the underlying native topic handle."""


class SubscriptionInfo(DataModel):
    """Subscription info."""

    name: str | None = None
    """Subscription name."""

    topic: str | None = None
    """Topic name."""

    active_message_count: int | None = None
    """Number of active messages."""

    inflight_message_count: int | None = None
    """Number of inflight messages."""

    scheduled_message_count: int | None = None
    """Number of scheduled messages."""

    config: SubscriptionConfig | None = None
    """Subscription config."""

    nref: Any = None
    """Reference to the underlying native subscription handle."""


class MessagingMode(str, Enum):
    QUEUE = "queue"
    PUBSUB = "pubsub"


DEFAULT_SUBSCRIPTION_NAME = "default"
