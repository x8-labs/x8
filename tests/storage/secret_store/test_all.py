# type: ignore
import asyncio
import os
import time
from datetime import datetime, timezone

import pytest

from x8.core.exceptions import NotSupportedError
from x8.storage.secret_store import (
    NotFoundError,
    PreconditionFailedError,
    SecretItem,
    SecretProperties,
    SecretVersion,
)

from ._data import secrets
from ._providers import SecretStoreProvider
from ._sync_and_async_client import SecretStoreSyncAndAsyncClient

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

queries = [
    "starts_with($id, 'test-id-')",
    "starts_with($id, 'test-id-1') or starts_with($id, 'test-id-2')",
    "starts_with($id, 'test-id-') and $metadata.common-key='common-value'",
    "$metadata.test-1-tag-1-key-1='test-1-tag-1-value-1'",
    """$metadata.test-11-tag-1-key-1='test-11-tag-1-value-1'
        and $metadata.common-key='common-value'""",
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_get(provider_type: str, async_call: bool):
    id = secrets[0]["id"]
    value = secrets[0]["values"][0]
    metadata = secrets[0]["metadata"][0]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_secret(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.get(key=id)

    with pytest.raises(NotFoundError):
        await client.get_metadata(key=id)

    response = await client.put(
        key=id, value=value, metadata=metadata, where="not_exists()"
    )
    result = response.result
    assert_put_result(item=result, id=id)
    version = result.key.version

    with pytest.raises(PreconditionFailedError):
        await client.put(
            key=id,
            value=value,
            metadata=metadata,
            where="not_exists()",
        )

    response = await client.get(key=id)
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    response = await client.get(key={"id": id, "version": version})
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=metadata)

    await client.delete(key=id)

    await wait_for_delete(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.get(key=id)

    with pytest.raises(NotFoundError):
        await client.get_metadata(key=id)

    with pytest.raises(NotFoundError):
        await client.delete(key=id)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put(provider_type: str, async_call: bool):
    id = secrets[0]["id"]
    value = secrets[0]["values"][0]
    metadata = secrets[0]["metadata"][0]
    new_value = secrets[0]["values"][1]
    new_metadata = secrets[0]["metadata"][1]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_secret(id=id, client=client)

    with pytest.raises(PreconditionFailedError):
        await client.put(
            key=id, value=value, metadata=metadata, where="exists()"
        )

    response = await client.put(key=id, value=value, metadata=metadata)
    result = response.result
    assert_put_result(
        item=result,
        id=id,
    )
    version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=metadata)

    response = await client.put(key=id, value=new_value, metadata=new_metadata)
    result = response.result
    assert_put_result(
        item=result,
        id=id,
    )
    new_version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(
        item=result,
        id=id,
        value=new_value,
        version=new_version,
    )

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=new_metadata)

    await client.delete(key=id)
    await wait_for_delete(id=id, client=client)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_update(provider_type: str, async_call: bool):
    id = secrets[0]["id"]
    value = secrets[0]["values"][0]
    metadata = secrets[0]["metadata"][0]
    new_value = secrets[0]["values"][1]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_secret(id=id, client=client)

    response = await client.put(
        key=id, value=value, metadata=metadata, where="not_exists()"
    )
    result = response.result
    assert_put_result(
        item=result,
        id=id,
    )
    version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=metadata)

    response = await client.update(
        key=id,
        value=new_value,
    )
    result = response.result
    assert_put_result(
        item=result,
        id=id,
    )
    new_version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(
        item=result,
        id=id,
        value=new_value,
        version=new_version,
    )

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=metadata)

    await client.delete(key=id)
    await wait_for_delete(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.update(key=id, value=value)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_metadata(provider_type: str, async_call: bool):
    id = secrets[0]["id"]
    value = secrets[0]["values"][0]
    metadata = None  # start with none metadata, other tests test with metadata
    new_metadata = secrets[0]["metadata"][0]
    new_new_metadata = secrets[0]["metadata"][1]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_secret(id=id, client=client)

    response = await client.put(
        key=id, value=value, metadata=metadata, where="not_exists()"
    )
    result = response.result
    assert_put_result(
        item=result,
        id=id,
    )

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=metadata)

    # Update metadata to new metadata
    response = await client.update_metadata(
        key=id,
        metadata=new_metadata,
    )
    result = response.result
    assert_update_metadata_result(item=result, id=id, metadata=new_metadata)

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=new_metadata)

    # Update metadata to new new metadata
    response = await client.update_metadata(
        key=id,
        metadata=new_new_metadata,
    )
    result = response.result
    assert_update_metadata_result(
        item=result, id=id, metadata=new_new_metadata
    )

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=new_new_metadata)

    # Update metadata to None
    response = await client.update_metadata(
        key=id,
        metadata=None,
    )
    result = response.result
    assert_update_metadata_result(item=result, id=id, metadata=None)

    response = await client.get_metadata(key=id)
    result = response.result
    assert_get_metadata_result(item=result, id=id, metadata=None)

    await client.delete(key=id)
    await wait_for_delete(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.update_metadata(
            key=id,
            metadata=new_metadata,
        )
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_versions(provider_type: str, async_call: bool):
    id = secrets[0]["id"]
    metadata = secrets[0]["metadata"][0]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await cleanup_secret(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.get_versions(key=id)

    versions = []
    response = await client.put(
        key=id,
        value=secrets[0]["values"][0],
        metadata=metadata,
        where="not_exists()",
    )
    result = response.result
    versions.append(result.key.version)

    for i in range(1, len(secrets[0]["values"])):
        # Azure Key Vault, SQLite requires less than 1 RPS to
        # maintain differentiated created_time
        # that is used to sort the versions
        time.sleep(1)
        response = await client.update(key=id, value=secrets[0]["values"][i])
        result = response.result
        versions.append(result.key.version)

    response = await client.get_versions(key=id)
    result = response.result
    assert len(result.versions) == len(secrets[0]["values"])
    result.versions.reverse()
    for i in range(len(result.versions)):
        item = result.versions[i]
        assert_secret_version(result.versions[i], version=versions[i])

        get_response = await client.get(
            key={"id": id, "version": item.version}
        )
        get_result = get_response.result

        check_value = secrets[0]["values"][i]
        assert_get_result(
            item=get_result,
            id=id,
            value=check_value,
            version=item.version,
        )

    await client.delete(key=id)
    await wait_for_delete(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.get_versions(key=id)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SecretStoreProvider.AWS_SECRETS_MANAGER,
        SecretStoreProvider.AZURE_KEY_VAULT,
        SecretStoreProvider.GOOGLE_SECRET_MANAGER,
        SecretStoreProvider.HASHICORP_VAULT,
        SecretStoreProvider.DS_AMAZON_DYNAMODB,
        SecretStoreProvider.DS_AZURE_COSMOS_DB,
        SecretStoreProvider.DS_GOOGLE_FIRESTORE,
        SecretStoreProvider.DS_MONGODB,
        SecretStoreProvider.DS_POSTGRESQL,
        SecretStoreProvider.DS_REDIS,
        SecretStoreProvider.DS_SQLITE,
        SecretStoreProvider.DS_MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query(provider_type: str, async_call: bool):
    client = SecretStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    for secret in secrets:
        await cleanup_secret(id=secret["id"], client=client)

    response = await client.query(where=queries[0])
    result = response.result
    assert len(result.items) == 0

    for secret in secrets:
        await client.put(
            key=secret["id"],
            value=secret["values"][0],
            metadata=secret["metadata"][0],
            where="not_exists()",
        )

    # AWS Secrets Manager needs few seconds
    # to propagate secrets to list secrets.
    if provider_type == SecretStoreProvider.AWS_SECRETS_MANAGER:
        time.sleep(5)

    sorted_indexes = [[0, 2, 1], [0, 2, 1], [0, 1], [0], []]
    for k in range(len(queries)):
        # HashiCorp does not return metadata on list secrets operations.
        if k > 1 and provider_type == SecretStoreProvider.HASHICORP_VAULT:
            continue
        query = queries[k]
        sorted_index = sorted_indexes[k]
        response = await client.query(where=query)
        result = response.result
        check_secret_list(
            provider_type=provider_type,
            items=result.items,
            sorted_index=sorted_index,
        )

    response = await client.count(where=queries[0])
    result = response.result
    assert result == len(secrets)

    response = await client.query(where=queries[0])
    result = response.result
    for item in result.items:
        await client.delete(key=item.key)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_env(async_call: bool):
    id = secrets[0]["id"]
    value = secrets[0]["values"][0]
    metadata = secrets[0]["metadata"][0]
    new_value = secrets[0]["values"][1]
    client = SecretStoreSyncAndAsyncClient(
        provider_type=SecretStoreProvider.ENV,
        async_call=async_call,
    )

    await cleanup_secret(id=id, client=client)

    with pytest.raises(NotFoundError):
        await client.get(key=id)

    with pytest.raises(NotFoundError):
        await client.get_versions(key=id)

    response = await client.put(
        key=id, value=value, metadata=metadata, where="not_exists()"
    )
    result = response.result
    assert_put_result(item=result, id=id)
    version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    response = await client.get_versions(key=id)
    result = response.result
    assert len(result.versions) == 1
    assert result.versions[0].version == version

    response = await client.update(key=id, value=new_value)
    result = response.result
    new_version = result.key.version
    assert_put_result(result, id=id)

    response = await client.get(key=id)
    result = response.result
    assert_get_result(
        item=result,
        id=id,
        value=new_value,
        version=new_version,
    )

    response = await client.get_versions(key=id)
    result = response.result
    assert len(result.versions) == 1
    assert result.versions[0].version == new_version

    with pytest.raises(NotSupportedError):
        await client.update_metadata(key=id, metadata=None)

    with pytest.raises(NotSupportedError):
        await client.get_metadata(key=id)

    with pytest.raises(NotSupportedError):
        await client.query()

    await client.delete(key=id)

    response = await client.put(key=id, value=value, metadata=metadata)
    result = response.result
    assert_put_result(result, id=id)
    version = result.key.version

    response = await client.get(key=id)
    result = response.result
    assert_get_result(item=result, id=id, value=value, version=version)

    await client.delete(key=id)

    with pytest.raises(NotFoundError):
        await client.get_versions(key=id)
    await client.close()


def check_secret_list(
    provider_type: str, items: list[SecretItem], sorted_index: list[int]
):
    assert len(items) == len(sorted_index)
    for i in range(len(sorted_index)):
        item = items[i]
        id = secrets[sorted_index[i]]["id"]
        metadata = secrets[sorted_index[i]]["metadata"][0]
        if provider_type == SecretStoreProvider.HASHICORP_VAULT:
            item.metadata = secrets[sorted_index[i]]["metadata"][0]
            item.properties = SecretProperties(
                created_time=datetime.now().astimezone().timestamp()
            )
        assert_secret_item(item=item, id=id, metadata=metadata)


def assert_put_result(item: SecretItem, id: str):
    assert item.key.id == id
    assert item.key.version is not None


def assert_update_metadata_result(item: SecretItem, id: str, metadata: dict):
    assert item.key.id == id
    if metadata is not None:
        assert item.metadata == metadata
    else:
        assert item.metadata == {}


def assert_get_result(
    item: SecretItem,
    id: str,
    value: str,
    version: str,
):
    assert item.key.id == id
    assert item.value == value
    if item.key.version != "latest":
        assert item.key.version == version


def assert_get_metadata_result(item: SecretItem, id: str, metadata: dict):
    assert item.key.id == id
    if metadata is not None:
        assert item.metadata == metadata
    else:
        assert item.metadata == {}
    diff_time_seconds = (
        datetime.now(timezone.utc).timestamp() - item.properties.created_time
    )
    assert diff_time_seconds < 1800


def assert_secret_version(
    item: SecretVersion,
    version: str,
):
    if version != "latest":
        assert item.version == version
    diff_time_seconds = (
        datetime.now(timezone.utc).timestamp() - item.created_time
    )
    assert diff_time_seconds < 1800


def assert_secret_item(item: SecretItem, id: str, metadata: dict):
    assert item.key.id == id
    if metadata is not None:
        assert item.metadata == metadata
    else:
        assert item.metadata == {}
    diff_time_seconds = (
        datetime.now(timezone.utc).timestamp() - item.properties.created_time
    )
    assert diff_time_seconds < 1800


async def cleanup_secret(id: str, client: SecretStoreSyncAndAsyncClient):
    try:
        await client.delete(key=id)
    except NotFoundError:
        return

    await wait_for_delete(id=id, client=client)


async def wait_for_delete(id: str, client: SecretStoreSyncAndAsyncClient):
    if client.provider_type == SecretStoreProvider.AWS_SECRETS_MANAGER:
        while True:
            try:
                await client.get_metadata(
                    key=id, nflags=dict(raise_on_deleted=False)
                )
            except NotFoundError:
                break
            time.sleep(0.5)
    if client.provider_type == SecretStoreProvider.AZURE_KEY_VAULT:
        # Wait for the secret to be destroyed.
        # Azure doesn't provider any way to poll.
        time.sleep(5)
