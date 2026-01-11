from x8.core import OperationParser

from ._models import (
    MessageBatch,
    MessageKey,
    MessageProperties,
    MessagePullConfig,
    MessagePutConfig,
    MessageValueType,
    QueueConfig,
    SubscriptionConfig,
    TopicConfig,
)
from ._operation import MessagingOperation


class MessagingOperationParser(OperationParser):
    def get_value(self) -> MessageValueType:
        return self.get_arg("value")

    def get_metadata(self) -> dict | None:
        return self.get_arg("metadata")

    def get_properties(self) -> MessageProperties | None:
        properties = self.get_arg("properties")
        if isinstance(properties, dict):
            return MessageProperties.from_dict(properties)
        return properties

    def get_put_config(self) -> MessagePutConfig | None:
        config = self.get_arg("config")
        if isinstance(config, dict):
            return MessagePutConfig.from_dict(config)
        return config

    def get_pull_config(self) -> MessagePullConfig | None:
        config = self.get_arg("config")
        if isinstance(config, dict):
            return MessagePullConfig.from_dict(config)
        return config

    def get_batch(self) -> MessageBatch:
        batch = self.get_arg("batch")
        if isinstance(batch, dict):
            return MessageBatch.from_dict(batch)
        return batch

    def get_key(self) -> MessageKey:
        key = self.get_arg("key")
        if isinstance(key, dict):
            return MessageKey.from_dict(key)
        return key

    def get_timeout(self) -> int | None:
        return self.get_arg("timeout")

    def get_queue(self) -> str | None:
        return self.get_arg("queue")

    def get_topic(self) -> str | None:
        return self.get_arg("topic")

    def get_subscription(self) -> str | None:
        return self.get_arg("subscription")

    def get_queue_config(self) -> QueueConfig | None:
        config = self.get_arg("config")
        if isinstance(config, dict):
            return QueueConfig.from_dict(config)
        return config

    def get_topic_config(self) -> TopicConfig | None:
        config = self.get_arg("config")
        if isinstance(config, dict):
            return TopicConfig.from_dict(config)
        return config

    def get_subscription_config(self) -> SubscriptionConfig | None:
        config = self.get_arg("config")
        if isinstance(config, dict):
            return SubscriptionConfig.from_dict(config)
        return config

    def is_resource_op(self) -> bool:
        return self.get_op_name() in [
            MessagingOperation.CREATE_QUEUE,
            MessagingOperation.UPDATE_QUEUE,
            MessagingOperation.DROP_QUEUE,
            MessagingOperation.LIST_QUEUES,
            MessagingOperation.HAS_QUEUE,
            MessagingOperation.GET_QUEUE,
            MessagingOperation.CREATE_TOPIC,
            MessagingOperation.UPDATE_TOPIC,
            MessagingOperation.DROP_TOPIC,
            MessagingOperation.LIST_TOPICS,
            MessagingOperation.HAS_TOPIC,
            MessagingOperation.GET_TOPIC,
            MessagingOperation.CREATE_SUBSCRIPTION,
            MessagingOperation.UPDATE_SUBSCRIPTION,
            MessagingOperation.DROP_SUBSCRIPTION,
            MessagingOperation.LIST_SUBSCRIPTIONS,
            MessagingOperation.HAS_SUBSCRIPTION,
            MessagingOperation.GET_SUBSCRIPTION,
            MessagingOperation.CLOSE,
        ]

    def is_sender_op(self):
        return self.get_op_name() in [
            MessagingOperation.PUT,
            MessagingOperation.BATCH,
        ]

    def is_receiver_op(self):
        return self.get_op_name() in [
            MessagingOperation.PULL,
            MessagingOperation.ACK,
            MessagingOperation.NACK,
            MessagingOperation.EXTEND,
            MessagingOperation.PURGE,
        ]
