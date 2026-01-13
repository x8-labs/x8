from typing import Any

from x8.core import Component, Response, operation
from x8.messaging._common import (
    MessageBatch,
    MessageItem,
    MessageKey,
    MessageProperties,
    MessagePullConfig,
    MessagePutConfig,
    MessageValueType,
    QueueConfig,
    QueueInfo,
)
from x8.ql import Expression


class Queue(Component):
    queue: str | None

    def __init__(
        self,
        queue: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            queue:
                Queue name.
        """
        self.queue = queue
        super().__init__(**kwargs)

    @operation()
    def create_queue(
        self,
        queue: str | None = None,
        config: dict | QueueConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create queue.

        Args:
            queue:
                Queue name.
            config:
                Queue config.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def update_queue(
        self,
        queue: str | None = None,
        config: dict | QueueConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update queue.

        Args:
            queue:
                Queue name.
            config:
                Queue config.
        """
        raise NotImplementedError

    @operation()
    def drop_queue(
        self,
        queue: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Drop queue.

        Args:
            queue:
                Queue name.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def list_queues(self, **kwargs: Any) -> Response[list[str]]:
        """List queues.

        Returns:
            List of queue names.
        """
        raise NotImplementedError

    @operation()
    def has_queue(
        self,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if queue exists.

        Args:
            queue:
                Queue name.
        Returns:
            True if queue exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    def get_queue(
        self,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[QueueInfo]:
        """Get queue info.

        Args:
            queue:
                Queue name.

        Returns:
            Queue info.
        """
        raise NotImplementedError

    @operation()
    def put(
        self,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: dict | MessageProperties | None = None,
        config: dict | MessagePutConfig | None = None,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
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
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    def batch(
        self,
        batch: dict | MessageBatch,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Put batch of message in the queue.

        Args:
            batch: Message batch.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    def pull(
        self,
        config: dict | MessagePullConfig | None = None,
        queue: str | None = None,
    ) -> Response[list[MessageItem]]:
        """Pull messages from the queue.

        Args:
            config:
                Pull config. Defaults to None.
            queue:
                Queue name.

        Returns:
            Pulled message items.
        """
        raise NotImplementedError

    @operation()
    def ack(
        self,
        key: dict | MessageKey,
        queue: str | None = None,
    ) -> Response[None]:
        """Acknowledge message.

        Args:
            key:
                Message key.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    def nack(
        self,
        key: dict | MessageKey,
        queue: str | None = None,
    ) -> Response[None]:
        """Abandon message.

        Args:
            key:
                Message key.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    def extend(
        self,
        key: dict | MessageKey,
        timeout: int | None = None,
        queue: str | None = None,
    ) -> Response[MessageItem]:
        """Extend message timeout.

        Args:
            key:
                Message key.
            timeout:
                Number of seconds to extend the message timeout.
            queue:
                Queue name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    def purge(
        self,
        config: dict | MessagePullConfig | None = None,
        queue: str | None = None,
    ) -> Response[None]:
        """Purge all messages in the queue.

        Args:
            config:
                Pull config.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    def close(
        self,
        queue: str | None = None,
    ) -> Response[None]:
        """Close the queue and the client.

        Args:
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def acreate_queue(
        self,
        queue: str | None = None,
        config: dict | QueueConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create queue.

        Args:
            queue:
                Queue name.
            config:
                Queue config.
        """
        raise NotImplementedError

    @operation()
    async def aupdate_queue(
        self,
        queue: str | None = None,
        config: dict | QueueConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update queue.

        Args:
            queue:
                Queue name.
            config:
                Queue config.
        """
        raise NotImplementedError

    @operation()
    async def adrop_queue(
        self, queue: str | None = None, **kwargs: Any
    ) -> Response[None]:
        """Drop queue.

        Args:
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def alist_queues(self, **kwargs: Any) -> Response[list[str]]:
        """List queues.

        Returns:
            List of queue names.
        """
        raise NotImplementedError

    @operation()
    async def ahas_queue(
        self,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if queue exists.

        Args:
            queue:
                Queue name.
        Returns:
            True if queue exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    async def aget_queue(
        self,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[QueueInfo]:
        """Get queue info.

        Args:
            queue:
                Queue name.

        Returns:
            Queue info.
        """
        raise NotImplementedError

    @operation()
    async def aput(
        self,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: dict | MessageProperties | None = None,
        config: dict | MessagePutConfig | None = None,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
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
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def abatch(
        self,
        batch: dict | MessageBatch,
        queue: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Put batch of message in the queue.

        Args:
            batch: Message batch.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def apull(
        self,
        config: dict | MessagePullConfig | None = None,
        queue: str | None = None,
    ) -> Response[list[MessageItem]]:
        """Pull messages from the queue.

        Args:
            config:
                Pull config. Defaults to None.
            queue:
                Queue name.

        Returns:
            Pulled message items.
        """
        raise NotImplementedError

    @operation()
    async def aack(
        self,
        key: dict | MessageKey,
        queue: str | None = None,
    ) -> Response[None]:
        """Acknowledge message.

        Args:
            key:
                Message key.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def anack(
        self,
        key: dict | MessageKey,
        queue: str | None = None,
    ) -> Response[None]:
        """Abandon message.

        Args:
            key:
                Message key.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def aextend(
        self,
        key: dict | MessageKey,
        timeout: int | None = None,
        queue: str | None = None,
    ) -> Response[MessageItem]:
        """Extend message timeout.

        Args:
            key:
                Message key.
            timeout:
                Number of seconds to extend the message timeout.
            queue:
                Queue name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    async def apurge(
        self,
        config: dict | MessagePullConfig | None = None,
        queue: str | None = None,
    ) -> Response[None]:
        """Purge all messages in the queue.

        Args:
            config:
                Pull config.
            queue:
                Queue name.
        """
        raise NotImplementedError

    @operation()
    async def aclose(
        self,
        queue: str | None = None,
    ) -> Response[None]:
        """Close the queue and the client.

        Args:
            queue:
                Queue name.
        """
        raise NotImplementedError
