# type: ignore
import asyncio
import os
import time

import pytest

from x8.storage.key_value_store import (
    KeyValueBatch,
    KeyValueItem,
    NotFoundError,
    PreconditionFailedError,
)

from ._data import batch_kvs, kvs, query_kvs
from ._providers import KeyValueStoreProvider
from ._sync_and_async_client import KeyValueStoreSyncAndAsyncClient

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

providers = [
    KeyValueStoreProvider.REDIS,
    KeyValueStoreProvider.REDIS_SIMPLE,
    KeyValueStoreProvider.MEMCACHED,
    KeyValueStoreProvider.MEMORY,
    KeyValueStoreProvider.SQLITE,
    KeyValueStoreProvider.POSTGRESQL,
    KeyValueStoreProvider.DS_AMAZON_DYNAMODB,
    KeyValueStoreProvider.DS_AZURE_COSMOS_DB,
    KeyValueStoreProvider.DS_GOOGLE_FIRESTORE,
    KeyValueStoreProvider.DS_MONGODB,
    KeyValueStoreProvider.OS_AMAZON_S3,
    KeyValueStoreProvider.OS_AZURE_BLOB_STORAGE,
    KeyValueStoreProvider.OS_GOOGLE_CLOUD_STORAGE,
    KeyValueStoreProvider.OS_FILE_SYSTEM,
]

except_incr_float_providers = [
    KeyValueStoreProvider.MEMCACHED,
]
except_put_update_return_etag_providers = [
    KeyValueStoreProvider.MEMCACHED,
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_replace_etag_providers = [
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_delete_etag_providers = [
    KeyValueStoreProvider.MEMCACHED,
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_update_etag_providers = [
    KeyValueStoreProvider.MEMCACHED,
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_batch_get_etag_providers = [
    KeyValueStoreProvider.MEMCACHED,
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_get_etag_providers = [
    KeyValueStoreProvider.REDIS_SIMPLE,
]
except_update_string_providers = [
    KeyValueStoreProvider.REDIS,
    KeyValueStoreProvider.DS_AMAZON_DYNAMODB,
    KeyValueStoreProvider.DS_AZURE_COSMOS_DB,
    KeyValueStoreProvider.DS_GOOGLE_FIRESTORE,
    KeyValueStoreProvider.DS_MONGODB,
    KeyValueStoreProvider.OS_AMAZON_S3,
    KeyValueStoreProvider.OS_AZURE_BLOB_STORAGE,
    KeyValueStoreProvider.OS_GOOGLE_CLOUD_STORAGE,
    KeyValueStoreProvider.OS_FILE_SYSTEM,
]

queries = [
    {
        "where": None,
        "limit": None,
        "result_index": [0, 1, 2, 3, 4],
        "collection": "col1",
    },
    {
        "where": None,
        "limit": None,
        "result_index": [5, 6, 7, 8, 9, 10, 11, 12, 13],
        "collection": "col2",
    },
    {
        "where": None,
        "limit": 5,
        "result_index": [5, 6, 7, 8, 9],
        "collection": "col2",
    },
    {
        "where": "starts_with($id, 'q0')",
        "limit": None,
        "result_index": [5, 6, 7, 8, 9],
        "collection": "col2",
    },
    {
        "where": "contains($id, '2')",
        "limit": None,
        "result_index": [12],
        "collection": "col2",
        "except_providers": [
            KeyValueStoreProvider.DS_AMAZON_DYNAMODB,
            KeyValueStoreProvider.DS_AZURE_COSMOS_DB,
            KeyValueStoreProvider.DS_GOOGLE_FIRESTORE,
            KeyValueStoreProvider.DS_MONGODB,
            KeyValueStoreProvider.OS_AMAZON_S3,
            KeyValueStoreProvider.OS_AZURE_BLOB_STORAGE,
            KeyValueStoreProvider.OS_GOOGLE_CLOUD_STORAGE,
            KeyValueStoreProvider.OS_FILE_SYSTEM,
        ],
    },
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    providers,
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
@pytest.mark.parametrize(
    "type",
    ["binary", "string"],
)
async def test_put_get_delete(provider_type: str, async_call: bool, type: str):
    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type,
        async_call=async_call,
        type=type,
    )
    kv = kvs[0]
    key = get_key(kv)
    value = kv["value"]
    replace_kv = {"id": kv["id"], "value": b"replace value"}
    replace_value = replace_kv["value"]
    replace2_kv = {"id": kv["id"], "value": b"blah blah value"}
    replace2_value = replace2_kv["value"]

    await cleanup_kv(provider_type, key, client)

    # get when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # unconditional delete when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.delete(key=key)

    # check exists when item doesn't exist
    response = await client.exists(key=key)
    result = response.result
    assert result is False

    # unconditional put when item doesn't exist
    response = await client.put(key=key, value=value)
    result = response.result
    assert_put_result(result, kv, provider_type)

    # get when item exists
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, kv, type, provider_type)

    # check exists when item doesn't exists
    response = await client.exists(key=key)
    result = response.result
    assert result is True

    # unconditional put when item exists
    response = await client.put(key=key, value=replace_value)
    result = response.result
    assert_put_result(result, replace_kv, provider_type)
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, replace_kv, type, provider_type)

    # range get
    response = await client.get(key=key, start=2)
    result = response.result
    assert_get_result(result, replace_kv, type, provider_type, start=2)
    response = await client.get(key=key, end=5)
    result = response.result
    assert_get_result(result, replace_kv, type, provider_type, end=5)
    response = await client.get(key=key, start=3, end=8)
    result = response.result
    assert_get_result(result, replace_kv, type, provider_type, start=3, end=8)

    # unconditional delete when item exists
    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    # get when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # conditional put (exists=True) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.put(key=key, value=value, where="exists()")

    # conditional put (exists=False) when item doesn't exist
    response = await client.put(key=key, value=value, where="not_exists()")
    result = response.result
    assert_put_result(result, kv, provider_type)
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, kv, type, provider_type)

    # conditional put (exists=False) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.put(key=key, value=value, where="not_exists()")

    # conditional put (exists=True) when item exists
    put_response = await client.put(
        key=key, value=replace_value, where="exists()"
    )
    put_result = put_response.result
    assert_put_result(put_result, replace_kv, provider_type)

    get_response = await client.get(key=key)
    get_result = get_response.result
    assert_get_result(get_result, replace_kv, type, provider_type)

    # conditional put with bad etag
    if provider_type not in except_replace_etag_providers:
        with pytest.raises(PreconditionFailedError):
            await client.put(
                key=key,
                value=value,
                where=f"$etag='{get_bad_etag(provider_type)}'",
            )

    # conditional put with good etag
    put_response = await client.put(
        key=key,
        value=replace2_value,
        where=f"$etag='{get_result.properties.etag}'",
    )
    put_result = put_response.result
    assert_put_result(put_result, replace2_kv, provider_type)
    if provider_type not in except_get_etag_providers:
        assert get_result.properties.etag != put_result.properties.etag

    get_response = await client.get(key=key)
    get_result = get_response.result
    assert_get_result(get_result, replace2_kv, type, provider_type)

    # conditional delete with bad etag
    if provider_type not in except_delete_etag_providers:
        with pytest.raises(PreconditionFailedError):
            await client.delete(
                key=key,
                where=f"$etag='{get_bad_etag(provider_type)}'",
            )

    # conditional delete with good etag
    response = await client.delete(
        key=key, where=f"$etag='{get_result.properties.etag}'"
    )
    result = response.result
    assert_delete_result(result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    providers,
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_update(provider_type: str, async_call: bool):
    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call, type="binary"
    )
    for kv in kvs:
        await cleanup_kv(provider_type, get_key(kv), client)

    kv = kvs[1]
    key = kv["id"]
    value = kv["value"]

    response = await client.put(key=key, value=value)
    result = response.result
    assert result.value is None

    response = await client.get(key=key)
    result = response.result
    assert value is int(result.value)

    response = await client.put(key=key, value=30, returning="new")
    result = response.result
    assert int(result.value) == 30

    response = await client.update(key=key, set="$value=increment(5)")
    result = response.result
    assert_update_result(result, kv, provider_type)
    assert result.value is None

    response = await client.get(key=key)
    get_result = response.result
    assert int(get_result.value) == 35

    # bad etag update
    if provider_type not in except_update_etag_providers:
        with pytest.raises(PreconditionFailedError):
            response = await client.update(
                key=key,
                set="$value=increment(-10)",
                where="$etag='111'",
                returning="new",
            )

    # good etag update
    response = await client.update(
        key=key,
        set="$value=increment(-10)",
        where=f"$etag='{get_result.properties.etag}'",
        returning="new",
    )
    result = response.result
    assert int(result.value) == 25
    if provider_type not in except_get_etag_providers:
        assert result.properties.etag != get_result.properties.etag

    response = await client.get(key=key)
    result = response.result
    assert int(result.value) == 25

    if provider_type not in except_incr_float_providers:
        response = await client.update(key=key, set="$value=increment(3.5)")
        response = await client.get(key=key)
        result = response.result
        assert float(result.value) == 28.5

        response = await client.update(key=key, set="$value=increment(-20.5)")
        response = await client.get(key=key)
        result = response.result
        assert float(result.value) == 8.0

    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    response = await client.update(key=key, set="$value=increment(5)")
    result = response.result
    assert_update_result(result, kv, provider_type)

    response = await client.get(key=key)
    result = response.result
    assert int(result.value) == 5

    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    await client.close()

    if provider_type in except_update_string_providers:
        return

    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call, type="string"
    )

    kv = kvs[2]
    key = kv["id"]
    value = kv["value"]

    response = await client.put(key=key, value=value)
    response = await client.get(key=key)
    result = response.result
    assert result.value == value

    response = await client.update(key=key, set="$value=append('Universe')")
    response = await client.get(key=key)
    result = response.result
    assert result.value == f"{value}Universe"

    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    providers,
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query(provider_type: str, async_call: bool):
    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call, type="binary"
    )
    for kv in query_kvs:
        try:
            await client.delete(
                key=get_key(kv),
                collection=kv["collection"],
            )
        except NotFoundError:
            pass

    for kv in query_kvs:
        await client.put(
            key=get_key(kv),
            value=get_value(kv),
            collection=kv["collection"],
        )

    for query in queries:
        if (
            "except_providers" in query
            and provider_type in query["except_providers"]
        ):
            continue
        response = await client.query(
            where=query["where"],
            limit=query["limit"],
            collection=query["collection"],
        )
        result = response.result
        if query["limit"]:
            assert len(result.items) >= len(query["result_index"])
        else:
            assert len(result.items) == len(query["result_index"])
        if not query["limit"]:
            response = await client.count(
                where=query["where"],
                collection=query["collection"],
            )
            result = response.result
            assert result == len(query["result_index"])

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    providers,
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
@pytest.mark.parametrize(
    "type",
    ["binary", "string"],
)
async def test_batch(provider_type: str, async_call: bool, type: str):
    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type,
        async_call=async_call,
        type=type,
    )
    for kv in batch_kvs:
        try:
            await client.delete(
                key=get_key(kv),
                collection=kv["collection"],
            )
        except NotFoundError:
            pass

    batch = KeyValueBatch()
    for kv in batch_kvs:
        batch.put(
            key=get_key(kv),
            value=get_value(kv),
            collection=kv["collection"],
        )
    response = await client.batch(batch=batch)
    result = response.result
    for item_result, kv in zip(result, batch_kvs):
        assert_put_result(item_result, kv, provider_type)

    batch = KeyValueBatch()
    for kv in batch_kvs:
        batch.get(
            key=get_key(kv),
            collection=kv["collection"],
        )
    response = await client.batch(batch=batch)
    result = response.result
    for item_result, kv in zip(result, batch_kvs):
        assert_get_result(
            item_result,
            kv,
            type,
            provider_type,
            batch=True,
        )

    batch = KeyValueBatch()
    for kv in batch_kvs:
        batch.delete(
            key=get_key(kv),
            collection=kv["collection"],
        )
    response = await client.batch(batch=batch)
    result = response.result
    for item_result in result:
        assert_delete_result(item_result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    providers,
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_expire(provider_type: str, async_call: bool):
    client = KeyValueStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call, type="binary"
    )

    kv = kvs[0]
    key = get_key(kv)
    value = kv["value"]

    await cleanup_kv(provider_type, key, client)

    # get when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    response = await client.put(key=key, value=value, expiry=3000)
    result = response.result
    assert_put_result(result, kv, provider_type)

    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, kv, type, provider_type)

    time.sleep(4)
    # get when item expired
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # delete when item expired
    with pytest.raises(NotFoundError):
        await client.delete(key=key)

    await client.close()


def get_value(item):
    return item["value"]


def get_key(item):
    return item["id"]


def assert_get_result(
    item: KeyValueItem,
    kv: dict,
    type: str,
    provider_type: str,
    batch: bool = False,
    start: int | None = None,
    end: int | None = None,
):
    assert item.key.id == kv["id"]
    if type == "string":
        check_value = kv["value"].decode()
    else:
        check_value = kv["value"]
    if not start:
        start = 0
    if not end:
        end = len(check_value)
    else:
        end = end + 1
    check_value = check_value[start:end]
    assert item.value == check_value
    if batch and provider_type not in except_batch_get_etag_providers:
        assert item.properties.etag is not None


def assert_put_result(
    item: KeyValueItem,
    kv: dict,
    provider_type: str,
):
    assert item.key.id == kv["id"]
    if provider_type not in except_put_update_return_etag_providers:
        assert item.properties.etag is not None


def assert_update_result(
    item: KeyValueItem,
    kv: dict,
    provider_type: str,
):
    assert item.key.id == kv["id"]
    if provider_type not in except_put_update_return_etag_providers:
        assert item.properties.etag is not None


def assert_delete_result(result):
    assert result is None


def get_bad_etag(provider_type: str):
    if provider_type == KeyValueStoreProvider.MEMCACHED:
        return "111"
    return "CK/Z68ON54sDENED"


async def cleanup_kv(
    provider_type,
    key,
    client: KeyValueStoreSyncAndAsyncClient,
):
    try:
        await client.delete(key=key)
    except NotFoundError:
        return
