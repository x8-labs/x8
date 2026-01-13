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
    SubscriptionConfig,
    SubscriptionInfo,
    TopicConfig,
    TopicInfo,
)
from x8.ql import Expression


class PubSub(Component):
    topic: str | None
    subscription: str | None

    def __init__(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        self.topic = topic
        self.subscription = subscription
        super().__init__(**kwargs)

    @operation()
    def create_topic(
        self,
        topic: str | None = None,
        config: dict | TopicConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create topic.

        Args:
            topic:
                Topic name.
            config:
                Topic config.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def update_topic(
        self,
        topic: str | None = None,
        config: dict | TopicConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update topic.

        Args:
            topic:
                Topic name.
            config:
                Topic config.
        """
        raise NotImplementedError

    @operation()
    def drop_topic(
        self,
        topic: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Drop topic.

        Args:
            topic:
                Topic name.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def list_topics(self, **kwargs: Any) -> Response[list[str]]:
        """List topics.

        Returns:
            List of topic names.
        """
        raise NotImplementedError

    @operation()
    def has_topic(
        self,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if topic exists.

        Args:
            topic:
                Topic name.

        Returns:
            True if topic exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    def get_topic(
        self, topic: str | None = None, **kwargs: Any
    ) -> Response[TopicInfo]:
        """Get topic.

        Args:
            topic:
                Topic name.

        Returns:
            Topic info.
        """
        raise NotImplementedError

    @operation()
    def create_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        config: dict | SubscriptionConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            config:
                Subscription config.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def update_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        config: dict | SubscriptionConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            config:
                Subscription config.
        """
        raise NotImplementedError

    @operation()
    def drop_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Drop subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            where:
                Condition expression.
        """
        raise NotImplementedError

    @operation()
    def list_subscriptions(
        self,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List subscriptions.

        Returns:
            List of subscription names.
        """
        raise NotImplementedError

    @operation()
    def has_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if subscription exists.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            True if subscription exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    def get_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> Response[SubscriptionInfo]:
        """Get subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Subscription info.
        """
        raise NotImplementedError

    @operation()
    def put(
        self,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: dict | MessageProperties | None = None,
        config: dict | MessagePutConfig | None = None,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[MessageItem]:
        """Put message in the topic.

        Args:
            value:
                Message value.
            metadata:
                Message metadata.
            properties:
                Message properties.
            config:
                Put config.
            topic:
                Topic name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    def batch(
        self,
        batch: dict | MessageBatch,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[list[MessageItem]]:
        """Put batch of message in the topic.

        Args:
            batch: Message batch.
            topic:
                Topic name.

        Returns:
            List of message items.
        """
        raise NotImplementedError

    @operation()
    def pull(
        self,
        config: dict | MessagePullConfig | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[list[MessageItem]]:
        """Pull messages from the topic and subscription.

        Args:
            config:
                Pull config. Defaults to None.
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Pulled message items.
        """
        raise NotImplementedError

    @operation()
    def ack(
        self,
        key: dict | MessageKey,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Acknowledge message.

        Args:
            key:
                Message key.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    def nack(
        self,
        key: dict | MessageKey,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Abandon message.

        Args:
            key:
                Message key.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    def extend(
        self,
        key: dict | MessageKey,
        timeout: int | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[MessageItem]:
        """Extend message timeout.

        Args:
            key:
                Message key.
            timeout:
                Number of seconds to extend the message timeout.
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    def purge(
        self,
        config: dict | MessagePullConfig | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Purge all messages in the queue.

        Args:
            config:
                Pull config.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    def close(
        self,
        topic: str | None = None,
    ) -> Response[None]:
        """Close the topic and the client.

        Args:
            topic:
                Topic name.
        """
        raise NotImplementedError

    @operation()
    async def acreate_topic(
        self,
        topic: str | None = None,
        config: dict | TopicConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create topic.

        Args:
            topic:
                Topic name.
            config:
                Topic config.
        """
        raise NotImplementedError

    @operation()
    async def aupdate_topic(
        self,
        topic: str | None = None,
        config: dict | TopicConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update topic.

        Args:
            topic:
                Topic name.
            config:
                Topic config.
        """
        raise NotImplementedError

    @operation()
    async def adrop_topic(
        self, topic: str | None = None, **kwargs: Any
    ) -> Response[None]:
        """Drop topic.

        Args:
            topic:
                Topic name.
        """
        raise NotImplementedError

    @operation()
    async def alist_topics(self, **kwargs: Any) -> Response[list[str]]:
        """List topics.

        Returns:
            List of topic names.
        """
        raise NotImplementedError

    @operation()
    async def ahas_topic(
        self,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if topic exists.

        Args:
            topic:
                Topic name.

        Returns:
            True if topic exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    async def aget_topic(
        self, topic: str | None = None, **kwargs: Any
    ) -> Response[TopicInfo]:
        """Get topic.

        Args:
            topic:
                Topic name.

        Returns:
            Topic info.
        """
        raise NotImplementedError

    @operation()
    async def acreate_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        config: dict | SubscriptionConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Create subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            config:
                Subscription config.
        """
        raise NotImplementedError

    @operation()
    async def aupdate_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        config: dict | SubscriptionConfig | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Update subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
            config:
                Subscription config.
        """
        raise NotImplementedError

    @operation()
    async def adrop_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        """Drop subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    async def alist_subscriptions(
        self,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[list[str]]:
        """List subscriptions.

        Returns:
            List of subscription names.
        """
        raise NotImplementedError

    @operation()
    async def ahas_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> Response[bool]:
        """Check if subscription exists.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            True if subscription exists, False otherwise.
        """
        raise NotImplementedError

    @operation()
    async def aget_subscription(
        self,
        topic: str | None = None,
        subscription: str | None = None,
        **kwargs: Any,
    ) -> Response[SubscriptionInfo]:
        """Get subscription.

        Args:
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Subscription info.
        """
        raise NotImplementedError

    @operation()
    async def aput(
        self,
        value: MessageValueType,
        metadata: dict | None = None,
        properties: dict | MessageProperties | None = None,
        config: dict | MessagePutConfig | None = None,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[MessageItem]:
        """Put message in the topic.

        Args:
            value:
                Message value.
            metadata:
                Message metadata.
            properties:
                Message properties.
            config:
                Put config.
            topic:
                Topic name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    async def abatch(
        self,
        batch: dict | MessageBatch,
        topic: str | None = None,
        **kwargs: Any,
    ) -> Response[list[MessageItem]]:
        """Put batch of message in the topic.

        Args:
            batch: Message batch.
            topic:
                Topic name.

        Returns:
            List of message items.
        """
        raise NotImplementedError

    @operation()
    async def apull(
        self,
        config: dict | MessagePullConfig | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[list[MessageItem]]:
        """Pull messages from the topic and subscription.

        Args:
            config:
                Pull config. Defaults to None.
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Pulled message items.
        """
        raise NotImplementedError

    @operation()
    async def aack(
        self,
        key: dict | MessageKey,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Acknowledge message.

        Args:
            key:
                Message key.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    async def anack(
        self,
        key: dict | MessageKey,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Abandon message.

        Args:
            key:
                Message key.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    async def aextend(
        self,
        key: dict | MessageKey,
        timeout: int | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[MessageItem]:
        """Extend message timeout.

        Args:
            key:
                Message key.
            timeout:
                Number of seconds to extend the message timeout.
            topic:
                Topic name.
            subscription:
                Subscription name.

        Returns:
            Message item.
        """
        raise NotImplementedError

    @operation()
    async def apurge(
        self,
        config: dict | MessagePullConfig | None = None,
        topic: str | None = None,
        subscription: str | None = None,
    ) -> Response[None]:
        """Purge all messages in the queue.

        Args:
            config:
                Pull config.
            topic:
                Topic name.
            subscription:
                Subscription name.
        """
        raise NotImplementedError

    @operation()
    async def aclose(
        self,
        topic: str | None = None,
    ) -> Response[None]:
        """Close the topic and the client.

        Args:
            topic:
                Topic name.
        """
        raise NotImplementedError
