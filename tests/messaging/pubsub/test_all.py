# type: ignore

import time
from datetime import datetime, timezone

import pytest

from x8.messaging.pubsub import (
    ConflictError,
    MessageBatch,
    MessageItem,
    MessagePullConfig,
    NotFoundError,
    SubscriptionConfig,
    SubscriptionInfo,
    TopicConfig,
    TopicInfo,
)

from ._data import messages
from ._providers import PubSubProvider
from ._sync_and_async_client import PubSubSyncAndAsyncClient

except_config_providers = [
    PubSubProvider.REDIS,
]

except_count_providers = [
    PubSubProvider.GOOGLE_PUBSUB,
]

except_pull_visibility_providers = [
    PubSubProvider.AZURE_SERVICE_BUS,
    PubSubProvider.GOOGLE_PUBSUB,
    PubSubProvider.REDIS,
]

topic_name = "topictest"
subscription_names = ["sub1", "sub2"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_create_drop(provider_type: str, async_call: bool):
    new_topic = f"ntest{str(async_call).lower()}"
    sub1 = f"nsub1{str(async_call).lower()}"
    sub2 = f"nsub2{str(async_call).lower()}"
    topic_config = TopicConfig(ttl=600)
    new_topic_config = TopicConfig(ttl=900)
    subscription_config = SubscriptionConfig(
        visibility_timeout=10,
        ttl=600,
    )
    new_subscription_config = SubscriptionConfig(
        visibility_timeout=20,
        ttl=900,
    )
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    response = await client.list_topics()
    if new_topic in response.result:
        await client.drop_topic(topic=new_topic)

    response = await client.list_topics()
    assert new_topic not in response.result

    response = await client.has_topic(topic=new_topic)
    assert not response.result

    with pytest.raises(NotFoundError):
        await client.get_topic(topic=new_topic)

    await client.create_topic(
        topic=new_topic,
        config=topic_config,
    )

    response = await client.list_topics()
    assert new_topic in response.result

    response = await client.has_topic(topic=new_topic)
    assert response.result

    response = await client.get_topic(topic=new_topic)
    assert_topic_info(
        result=response.result,
        topic=new_topic,
        config=topic_config,
    )

    await client.update_topic(
        topic=new_topic,
        config=new_topic_config,
    )
    response = await client.get_topic(topic=new_topic)
    assert_topic_info(
        result=response.result,
        topic=new_topic,
        config=new_topic_config,
    )

    await client.create_topic(
        topic=new_topic,
    )

    with pytest.raises(ConflictError):
        await client.create_topic(
            topic=new_topic,
            where="not_exists()",
        )

    response = await client.list_subscriptions(topic=new_topic)
    assert sub1 not in response.result
    assert sub2 not in response.result

    response = await client.has_subscription(
        topic=new_topic,
        subscription=sub1,
    )
    assert not response.result

    with pytest.raises(NotFoundError):
        await client.get_subscription(
            topic=new_topic,
            subscription=sub1,
        )

    await client.create_subscription(
        topic=new_topic, subscription=sub1, config=subscription_config
    )
    await client.create_subscription(
        topic=new_topic, subscription=sub2, config=subscription_config
    )
    wait_for_create_subscription(provider_type)
    response = await client.list_subscriptions(topic=new_topic)
    assert sub1 in response.result
    assert sub2 in response.result

    response = await client.has_subscription(
        topic=new_topic,
        subscription=sub1,
    )
    assert response.result

    response = await client.get_subscription(
        topic=new_topic,
        subscription=sub1,
    )
    assert_subscription_info(
        provider_type=provider_type,
        result=response.result,
        topic=new_topic,
        subscription=sub1,
        config=subscription_config,
        count=0,
    )

    await client.update_subscription(
        topic=new_topic,
        subscription=sub1,
        config=new_subscription_config,
    )
    response = await client.get_subscription(
        topic=new_topic,
        subscription=sub1,
    )
    assert_subscription_info(
        provider_type=provider_type,
        result=response.result,
        topic=new_topic,
        subscription=sub1,
        config=new_subscription_config,
        count=0,
    )

    await client.create_subscription(topic=new_topic, subscription=sub1)
    with pytest.raises(ConflictError):
        await client.create_subscription(
            topic=new_topic, subscription=sub1, where="not_exists()"
        )

    await client.drop_subscription(topic=new_topic, subscription=sub1)
    response = await client.list_subscriptions(topic=new_topic)
    assert sub1 not in response.result
    assert sub2 in response.result

    await client.drop_subscription(topic=new_topic, subscription=sub1)
    with pytest.raises(NotFoundError):
        await client.drop_subscription(
            topic=new_topic, subscription=sub1, where="exists()"
        )

    await client.drop_subscription(topic=new_topic, subscription=sub2)

    await client.drop_topic(topic=new_topic)

    response = await client.list_topics()
    assert new_topic not in response.result

    await client.drop_topic(topic=new_topic)

    with pytest.raises(NotFoundError):
        await client.drop_topic(
            topic=new_topic,
            where="exists()",
        )

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_pull(provider_type: str, async_call: bool):
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )

    for subscription_name in subscription_names:
        res = await client.pull(
            config=dict(max_wait_time=1),
            topic=topic_name,
            subscription=subscription_name,
        )
        assert len(res.result) == 0

    for message in messages:
        await client.put(**message, topic=topic_name)
        for subscription_name in subscription_names:
            res = await client.pull(
                config=dict(max_wait_time=5),
                topic=topic_name,
                subscription=subscription_name,
            )
            assert_message(res.result[0], message)
            await client.ack(
                key=res.result[0].key,
                topic=topic_name,
                subscription=subscription_name,
            )

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_batch(provider_type: str, async_call: bool):
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )

    batch = MessageBatch()
    for message in messages:
        batch.put(**message)
    await client.batch(batch=batch, topic=topic_name)

    for subscription_name in subscription_names:
        result = []
        while True:
            res = await client.pull(
                topic=topic_name,
                subscription=subscription_name,
                config=MessagePullConfig(max_wait_time=5),
            )
            for message in res.result:
                result.append(message)
                # If two messages have the same group_id,
                # (ordering_key in Google PubSub)
                # the subsequent message will not be delivered
                # until the first message is acked.
                await client.ack(
                    key=message.key,
                    topic=topic_name,
                    subscription=subscription_name,
                )
            if len(result) == len(messages):
                break
        assert len(result) == len(messages)
        assert_batch(result, messages)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_nack(provider_type: str, async_call: bool):
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )

    message = messages[0]
    await client.put(
        **message,
        topic=topic_name,
    )
    for subscription_name in subscription_names:
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
        )
        assert_message(res.result[0], message)
        await client.nack(
            key=res.result[0].key,
            topic=topic_name,
            subscription=subscription_name,
        )
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
        )
        assert_message(res.result[0], message)
        await client.extend(
            key=res.result[0].key,
            timeout=10,
            topic=topic_name,
            subscription=subscription_name,
        )
        await client.ack(
            key=res.result[0].key,
            topic=topic_name,
            subscription=subscription_name,
        )
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_count(provider_type: str, async_call: bool):
    if provider_type in except_count_providers:
        pytest.skip("Not supported for this provider")
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )

    for message in messages:
        await client.put(**message, topic=topic_name)

    for subscription_name in subscription_names:
        res = await client.get_subscription(
            topic=topic_name, subscription=subscription_name
        )
        assert_count(res.result, len(messages))
        for message in messages:
            res = await client.pull(
                topic=topic_name, subscription=subscription_name
            )
            await client.ack(
                key=res.result[0].key,
                topic=topic_name,
                subscription=subscription_name,
            )

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_visibility(provider_type: str, async_call: bool):
    if provider_type in except_pull_visibility_providers:
        pytest.skip("Not supported for this provider")
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )

    message = messages[0]
    await client.put(**message, topic=topic_name)
    for subscription_name in subscription_names:
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
            config=MessagePullConfig(visibility_timeout=1),
        )
        time.sleep(2)
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
            config=MessagePullConfig(visibility_timeout=10),
        )
        await client.ack(
            key=res.result[0].key,
            topic=topic_name,
            subscription=subscription_name,
        )
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        PubSubProvider.AMAZON_SNS,
        PubSubProvider.AZURE_SERVICE_BUS,
        PubSubProvider.GOOGLE_PUBSUB,
        PubSubProvider.REDIS,
        PubSubProvider.POSTGRESQL,
        PubSubProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_close(provider_type: str, async_call: bool):
    client = PubSubSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_topic_if_needed(provider_type, client)
    for subscription_name in subscription_names:
        await client.purge(
            config=dict(max_count=1, max_wait_time=2),
            topic=topic_name,
            subscription=subscription_name,
        )
    message = messages[0]
    await client.put(**message)
    for subscription_name in subscription_names:
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
        )
        assert_message(res.result[0], message)
        await client.ack(
            key=res.result[0].key,
            topic=topic_name,
            subscription=subscription_name,
        )
    await client.close(topic=topic_name)

    await client.put(**message)
    for subscription_name in subscription_names:
        res = await client.pull(
            topic=topic_name,
            subscription=subscription_name,
        )
        assert_message(res.result[0], message)
        await client.ack(
            key=res.result[0].key,
            topic=topic_name,
            subscription=subscription_name,
        )
    await client.close()


def wait_for_create_subscription(provider_type: str):
    if provider_type == PubSubProvider.AMAZON_SNS:
        time.sleep(5)


def assert_count(result: SubscriptionInfo, count: int):
    assert result.active_message_count >= count


def assert_topic_info(
    result: TopicInfo,
    topic: str,
    config: TopicConfig,
):
    assert result.name == topic
    assert result.nref is not None
    # assert result.config.ttl == config.ttl


def assert_subscription_info(
    provider_type: str,
    result: SubscriptionInfo,
    topic: str,
    subscription: str,
    config: SubscriptionConfig,
    count: int,
):
    assert result.name == subscription
    assert result.topic == topic
    if provider_type not in except_config_providers:
        assert result.config.visibility_timeout == config.visibility_timeout
        assert result.config.ttl == config.ttl
    if provider_type not in except_count_providers:
        assert result.active_message_count == count
    assert result.nref is not None


def assert_batch(result: list[MessageItem], messages: list[dict]):
    for message in messages:
        found = False
        for rmessage in result:
            if (
                rmessage.properties.message_id
                == message["properties"]["message_id"]
            ):
                assert_message(rmessage, message)
                found = True
                break
        if not found:
            raise AssertionError("Message not found")


def assert_message(result: MessageItem, message: dict):
    assert result.value == message["value"]
    assert result.metadata == message["metadata"]
    assert (
        result.properties.content_type == message["properties"]["content_type"]
    )
    assert result.properties.message_id == message["properties"]["message_id"]
    assert result.properties.group_id == message["properties"]["group_id"]
    diff_time_seconds = (
        datetime.now(timezone.utc).timestamp()
        - result.properties.enqueued_time
    )
    assert diff_time_seconds < 100


async def create_topic_if_needed(
    provider_type: str,
    client: PubSubSyncAndAsyncClient,
):
    response = await client.list_topics()
    topics = response.result
    if topic_name not in topics:
        await client.create_topic(
            topic=topic_name, config=TopicConfig(ttl=600)
        )
    response = await client.list_subscriptions()
    subscriptions = response.result
    for subscription_name in subscription_names:
        if subscription_name not in subscriptions:
            await client.create_subscription(
                topic=topic_name,
                subscription=subscription_name,
                config=SubscriptionConfig(visibility_timeout=30),
            )
