# type: ignore
import asyncio
import os
import time
from datetime import datetime, timezone

import pytest

from x8.storage.config_store import DEFAULT_LABEL, ConfigItem, NotFoundError

from ._data import configs
from ._providers import ConfigStoreProvider
from ._sync_and_async_client import ConfigStoreSyncAndAsyncClient

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

queries = [
    {
        "query": None,
        "result": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    },
    {
        "query": "starts_with($id, '')",
        "result": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    },
    {
        "query": "starts_with($id, '/root/')",
        "result": [0, 1, 2, 3, 6, 7, 9, 10],
    },
    {
        "query": "starts_with($id, '/root/test1')",
        "result": [0, 1, 6, 9],
    },
    {
        "query": f"$label = '{DEFAULT_LABEL}'",
        "result": [0, 1, 2, 3, 4, 5],
    },
    {
        "query": "$label = 'label1'",
        "result": [6, 7, 8],
    },
    {
        "query": "$label = 'label2'",
        "result": [9, 10, 11],
    },
    {
        "query": f"starts_with($id, '') and $label = '{DEFAULT_LABEL}'",
        "result": [0, 1, 2, 3, 4, 5],
    },
    {
        "query": "starts_with($id, '') and $label = 'label1'",
        "result": [6, 7, 8],
    },
    {
        "query": f"starts_with($id, '/root/') and $label = '{DEFAULT_LABEL}'",
        "result": [0, 1, 2, 3],
    },
    {
        "query": "starts_with($id, '/root/') and $label = 'label2'",
        "result": [9, 10],
    },
    {
        "query": f"""starts_with($id, '/root/test1')
                and $label = '{DEFAULT_LABEL}'""",
        "result": [0, 1],
    },
    {
        "query": "starts_with($id, '/root/test1') and $label = 'label1'",
        "result": [6],
    },
    {
        "query": "starts_with($id, '/root/test2') and $label = 'label2'",
        "result": [10],
    },
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ConfigStoreProvider.AWS_PARAMETER_STORE,
        ConfigStoreProvider.AZURE_APP_CONFIGURATION,
        ConfigStoreProvider.GOOGLE_RUNTIME_CONFIGURATOR,
        ConfigStoreProvider.DS_AMAZON_DYNAMODB,
        ConfigStoreProvider.DS_AZURE_COSMOS_DB,
        ConfigStoreProvider.DS_GOOGLE_FIRESTORE,
        ConfigStoreProvider.DS_MONGODB,
        ConfigStoreProvider.DS_POSTGRESQL,
        ConfigStoreProvider.DS_REDIS,
        ConfigStoreProvider.DS_MEMORY,
        ConfigStoreProvider.DS_SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_get_delete(provider_type: str, async_call: bool):
    config = configs[0]
    key = get_key(config)
    value = config["value"]
    client = ConfigStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_config(provider_type, key, client=client)

    with pytest.raises(NotFoundError):
        await client.get(key=key)

    response = await client.put(key=key, value=value)
    result = response.result
    assert_config_item(result, config)

    await wait_for_put(provider_type)

    response = await client.get(key=key)
    result = response.result
    assert_config_item(result, config)

    config["value"] = "new value"
    response = await client.put(key=key, value=config["value"])
    result = response.result
    assert_config_item(result, config)

    await wait_for_put(provider_type)

    response = await client.get(key=key)
    result = response.result
    assert_config_item(result, config)

    response = await client.delete(key=key)
    result = response.result
    assert result is None

    await wait_for_delete(provider_type)

    with pytest.raises(NotFoundError):
        await client.get(key=key)

    with pytest.raises(NotFoundError):
        await client.delete(key=key)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ConfigStoreProvider.AWS_PARAMETER_STORE,
        ConfigStoreProvider.AZURE_APP_CONFIGURATION,
        ConfigStoreProvider.GOOGLE_RUNTIME_CONFIGURATOR,
        ConfigStoreProvider.DS_AMAZON_DYNAMODB,
        ConfigStoreProvider.DS_AZURE_COSMOS_DB,
        ConfigStoreProvider.DS_GOOGLE_FIRESTORE,
        ConfigStoreProvider.DS_MONGODB,
        ConfigStoreProvider.DS_POSTGRESQL,
        ConfigStoreProvider.DS_REDIS,
        ConfigStoreProvider.DS_MEMORY,
        ConfigStoreProvider.DS_SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query_count(provider_type: str, async_call: bool):
    client = ConfigStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    for config in configs:
        key = get_key(config)
        value = config["value"]
        response = await client.put(key=key, value=value)
        result = response.result
        assert_config_item(result, config)

    await wait_for_put(provider_type)

    for query in queries:
        response = await client.query(query["query"])
        result = response.result
        assert_config_list(result.items, query["result"])
        response = await client.count(query["query"])
        result = response.result
        assert result == len(query["result"])

    for config in configs:
        key = get_key(config)
        await client.delete(key=key)

    await wait_for_delete(provider_type)

    await client.close()


def get_key(config):
    if config["label"] is None:
        return config["id"]
    return {"id": config["id"], "label": config["label"]}


def assert_config_list(items: list[ConfigItem], sorted_index: list[int]):
    assert len(items) == len(sorted_index)
    for i in range(len(sorted_index)):
        assert_config_item(items[i], configs[sorted_index[i]])


def assert_config_item(item: ConfigItem, config):
    assert item.key.id == config["id"]
    if config["label"] is None:
        assert item.key.label == DEFAULT_LABEL
    else:
        assert item.key.label == config["label"]
    assert item.value == config["value"]
    if (
        item.properties is not None
        and item.properties.updated_time is not None
    ):
        diff_time_seconds = (
            datetime.now(timezone.utc).timestamp()
            - item.properties.updated_time
        )
        assert diff_time_seconds < 1800


async def cleanup_config(
    provider_type, key, client: ConfigStoreSyncAndAsyncClient
):
    try:
        await client.delete(key=key)
    except NotFoundError:
        return

    await wait_for_delete(provider_type)


async def wait_for_delete(provider_type):
    if provider_type == ConfigStoreProvider.AWS_PARAMETER_STORE:
        time.sleep(1)


async def wait_for_put(provider_type):
    if provider_type == ConfigStoreProvider.AWS_PARAMETER_STORE:
        time.sleep(5)
