# type: ignore

import time
from datetime import datetime, timezone

import pytest

from x8.messaging.queue import (
    ConflictError,
    MessageBatch,
    MessageItem,
    MessagePullConfig,
    NotFoundError,
    QueueConfig,
    QueueInfo,
)

from ._data import messages
from ._providers import QueueProvider
from ._sync_and_async_client import QueueSyncAndAsyncClient

except_config_providers = [
    QueueProvider.AZURE_QUEUE_STORAGE,
    QueueProvider.REDIS,
]

except_count_providers = [
    QueueProvider.GOOGLE_PUBSUB,
]

except_pull_visibility_providers = [
    QueueProvider.AZURE_SERVICE_BUS,
    QueueProvider.GOOGLE_PUBSUB,
    QueueProvider.REDIS,
]

queue_name = "test"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_create_drop(provider_type: str, async_call: bool):
    new_queue = f"ntest{str(async_call).lower()}"
    config = QueueConfig(visibility_timeout=30, ttl=600)
    new_config = QueueConfig(visibility_timeout=15, ttl=900)
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    response = await client.list_queues()
    if new_queue in response.result:
        await client.drop_queue(queue=new_queue)

    response = await client.list_queues()
    assert new_queue not in response.result

    response = await client.has_queue(queue=new_queue)
    assert response.result is False

    with pytest.raises(NotFoundError):
        await client.get_queue(queue=new_queue)

    await client.create_queue(
        queue=new_queue,
        config=config,
    )

    wait_for_create_queue(provider_type)
    response = await client.list_queues()
    assert new_queue in response.result

    response = await client.has_queue(queue=new_queue)
    assert response.result is True

    response = await client.get_queue(queue=new_queue)
    assert_queue_info(provider_type, response.result, new_queue, config, 0)

    await client.update_queue(
        queue=new_queue,
        config=new_config,
    )
    response = await client.get_queue(queue=new_queue)
    assert_queue_info(provider_type, response.result, new_queue, new_config, 0)

    await client.create_queue(
        queue=new_queue,
    )

    with pytest.raises(ConflictError):
        await client.create_queue(queue=new_queue, where="not_exists()")

    await client.drop_queue(queue=new_queue)

    wait_for_drop_queue(provider_type)
    response = await client.list_queues()
    assert new_queue not in response.result

    await client.drop_queue(queue=new_queue)

    with pytest.raises(NotFoundError):
        await client.drop_queue(queue=new_queue, where="exists()")

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_pull(provider_type: str, async_call: bool):
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)

    await client.purge(config=dict(max_count=10, max_wait_time=2))

    res = await client.pull(config=dict(max_wait_time=1))
    assert len(res.result) == 0

    for message in messages:
        await client.put(**message)
        res = await client.pull(config=dict(max_wait_time=5))
        assert_message(res.result[0], message)
        await client.ack(key=res.result[0].key)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_batch(provider_type: str, async_call: bool):
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)

    await client.purge(config=dict(max_count=10, max_wait_time=2))
    batch = MessageBatch()
    for message in messages:
        batch.put(**message)
    await client.batch(batch=batch)
    result = []
    while True:
        res = await client.pull()
        for message in res.result:
            result.append(message)
            await client.ack(key=message.key)
        if len(result) == len(messages):
            break
    assert len(result) == len(messages)
    assert_batch(result, messages)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_nack(provider_type: str, async_call: bool):
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)

    await client.purge(config=dict(max_count=10, max_wait_time=2))
    message = messages[0]
    await client.put(**message)
    res = await client.pull()
    assert_message(res.result[0], message)
    await client.nack(key=res.result[0].key)
    res = await client.pull()
    assert_message(res.result[0], message)
    res = await client.extend(key=res.result[0].key, timeout=10)
    await client.ack(key=res.result.key)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_count(provider_type: str, async_call: bool):
    if provider_type in except_count_providers:
        pytest.skip("Not supported for this provider")
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)

    await client.purge(config=dict(max_count=10, max_wait_time=2))
    message = messages[0]

    for message in messages:
        await client.put(**message)
    response = await client.get_queue()
    assert_count(response.result, len(messages))
    for message in messages:
        res = await client.pull()
        # Azure Queue Storage can return multiple messages
        # even if max_count is set to 1
        for m in res.result:
            await client.ack(key=m.key)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_visibility(provider_type: str, async_call: bool):
    if provider_type in except_pull_visibility_providers:
        pytest.skip("Not supported for this provider")
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)
    await client.purge(config=dict(max_count=10, max_wait_time=2))
    message = messages[0]
    await client.put(**message)
    res = await client.pull(config=MessagePullConfig(visibility_timeout=1))
    time.sleep(2)
    res = await client.pull(config=MessagePullConfig(visibility_timeout=20))
    await client.ack(key=res.result[0].key)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        QueueProvider.AMAZON_SQS,
        QueueProvider.AZURE_SERVICE_BUS,
        QueueProvider.AZURE_QUEUE_STORAGE,
        QueueProvider.GOOGLE_PUBSUB,
        QueueProvider.REDIS,
        QueueProvider.POSTGRESQL,
        QueueProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_close(provider_type: str, async_call: bool):
    client = QueueSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_queue_if_needed(provider_type, client)

    await client.purge(config=dict(max_count=10, max_wait_time=2))
    message = messages[0]

    await client.put(**message)
    res = await client.pull()
    assert_message(res.result[0], message)
    await client.ack(key=res.result[0].key)
    await client.close(queue=queue_name)
    await client.put(**message)
    res = await client.pull()
    assert_message(res.result[0], message)
    await client.ack(key=res.result[0].key)
    await client.close()


def wait_for_create_queue(provider_type: str):
    if provider_type == QueueProvider.AMAZON_SQS:
        time.sleep(60)
    if provider_type == QueueProvider.AZURE_QUEUE_STORAGE:
        time.sleep(20)


def wait_for_drop_queue(provider_type: str):
    if provider_type == QueueProvider.AMAZON_SQS:
        time.sleep(60)


def assert_count(result: QueueInfo, count: int):
    assert result.active_message_count == count


def assert_queue_info(
    provider_type: str,
    result: QueueInfo,
    queue: str,
    config: QueueConfig,
    count: int,
):
    assert result.name == queue
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
    if "content_type" in message["properties"]:
        assert (
            result.properties.content_type
            == message["properties"]["content_type"]
        )
    if "message_id" in message["properties"]:
        assert (
            result.properties.message_id == message["properties"]["message_id"]
        )
    if "group_id" in message["properties"]:
        assert result.properties.group_id == message["properties"]["group_id"]
    diff_time_seconds = (
        datetime.now(timezone.utc).timestamp()
        - result.properties.enqueued_time
    )
    assert diff_time_seconds < 100


async def create_queue_if_needed(
    provider_type: str,
    client: QueueSyncAndAsyncClient,
):
    response = await client.list_queues()
    result = response.result
    if queue_name not in result:
        await client.create_queue(
            queue=queue_name, config=QueueConfig(visibility_timeout=30)
        )
