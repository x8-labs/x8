# type: ignore
import copy
import filecmp
import os
import tempfile
import time
import uuid
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Any

import httpx
import pytest

from x8.storage.object_store import (
    CollectionStatus,
    ConflictError,
    NotFoundError,
    NotModified,
    ObjectBatch,
    ObjectItem,
    PreconditionFailedError,
)

from ._data import objects, query_objects
from ._providers import ObjectStoreProvider
from ._sync_and_async_client import ObjectStoreSyncAndAsyncClient

except_if_modified_providers = [ObjectStoreProvider.GOOGLE_CLOUD_STORAGE]
except_multipart_condition_providers = [ObjectStoreProvider.AMAZON_S3]

queries = [
    {
        "where": None,
        "limit": None,
        "result_index": [13, 9, 10, 4, 5, 6, 2, 3, 7, 8, 0, 1, 11, 12],
        "should_count": True,
    },
    {
        "where": None,
        "limit": 5,
        "result_index": [13, 9, 10, 4, 5],
    },
    {
        "where": "starts_with($id, 'data/')",
        "limit": None,
        "result_index": [4, 5, 6, 2, 3, 7, 8],
        "should_count": True,
    },
    {
        "where": "starts_with($id, 'data/')",
        "limit": 3,
        "result_index": [4, 5, 6],
    },
    {
        "where": "starts_with($id, 't')",
        "limit": None,
        "result_index": [0, 1, 11, 12],
        "should_count": True,
    },
    {
        "where": "starts_with($id, null)",
        "limit": None,
        "result_index": [13, 9, 10, 4, 5, 6, 2, 3, 7, 8, 0, 1, 11, 12],
    },
    {
        "where": "starts_with_delimited($id, null, '/')",
        "limit": None,
        "result_index": [13, 0, 1],
        "should_count": True,
        "prefixes": ["abc/", "data/", "tzyx/"],
    },
    {
        "where": "starts_with_delimited($id, 'data/', '/')",
        "limit": None,
        "result_index": [2, 3],
        "should_count": True,
        "prefixes": ["data/ab/", "data/cd/", "data/xy/"],
    },
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_collection(provider_type: str, async_call: bool):
    new_collection = f"x8-ntest-{async_call}".lower()
    item = objects[3]
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    response = await client.list_collections()
    result = response.result
    if new_collection in result:
        await client.delete(key=get_key(item), collection=new_collection)
        await client.drop_collection(collection=new_collection)
        await wait_for_drop(provider_type)

    response = await client.list_collections()
    result = response.result
    assert new_collection not in result

    response = await client.has_collection(new_collection)
    result = response.result
    assert result is False

    response = await client.create_collection(collection=new_collection)
    result = response.result
    assert result.status == CollectionStatus.CREATED

    response = await client.create_collection(collection=new_collection)
    result = response.result
    assert result.status == CollectionStatus.EXISTS
    with pytest.raises(ConflictError):
        await client.create_collection(
            collection=new_collection, where="not_exists()"
        )

    response = await client.put(
        key=get_key(item),
        value=item["value"],
        properties=item["properties"],
        metadata=item["metadata"],
        collection=new_collection,
    )
    result = response.result
    assert_put_result(result, item, versioned=False)
    response = await client.get(key=get_key(item), collection=new_collection)
    result = response.result
    assert_get_result(result, item, versioned=False)
    await client.delete(key=get_key(item), collection=new_collection)

    response = await client.list_collections()
    result = response.result
    assert new_collection in result

    response = await client.has_collection(new_collection)
    result = response.result
    assert result is True

    response = await client.drop_collection(new_collection)
    result = response.result
    assert result.status == CollectionStatus.DROPPED
    await wait_for_drop(provider_type)

    response = await client.list_collections()
    result = response.result
    assert new_collection not in result

    response = await client.drop_collection(
        collection=new_collection,
    )
    result = response.result
    assert result.status == CollectionStatus.NOT_EXISTS
    with pytest.raises(NotFoundError):
        await client.drop_collection(
            collection=new_collection, where="exists()"
        )

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_get_delete(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    for object in objects:
        await cleanup_object(provider_type, get_key(object), client)
    for object in objects:
        with pytest.raises(NotFoundError):
            await client.get_properties(key=get_key(object))
    for multipart in [False, True]:
        for object in objects:
            in_file = None
            in_stream = None
            config = None
            if "file" in object:
                in_file = tempfile.NamedTemporaryFile(delete=False).name
            if "stream" in object:
                in_stream = BytesIO()
            if multipart:
                config = {"multipart": True}
            with pytest.raises(NotFoundError):
                await client.get(
                    key=get_key(object),
                    file=in_file,
                    stream=in_stream,
                    config=config,
                )
            if in_file is not None:
                try:
                    os.remove(in_file)
                except Exception:
                    pass
    for multipart in [False, True]:
        for object in objects:
            key = get_key(object)
            in_file = object.get("file", None)
            in_stream_file = None
            in_stream = None
            value = object.get("value", None)
            metadata = object.get("metadata", None)
            properties = object.get("properties", None)
            config = None
            if in_file is not None:
                in_file = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "data",
                    in_file,
                )
            if "stream" in object:
                in_stream_file = os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "data",
                    object["stream"],
                )
                in_stream = open(in_stream_file, "rb")
            if multipart:
                config = {"multipart": True}
            response = await client.put(
                key=key,
                value=value,
                file=in_file,
                stream=in_stream,
                metadata=metadata,
                properties=properties,
                config=config,
            )
            result = response.result
            assert_put_result(result, object, multipart)
            if in_stream is not None:
                in_stream.close()

            out_file = None
            out_stream = None
            if "stream" in object:
                out_stream = BytesIO()
            if "file" in object:
                out_file = tempfile.NamedTemporaryFile(delete=False).name
            response = await client.get(
                key=key, file=out_file, stream=out_stream, config=config
            )
            result = response.result
            assert_get_result(
                result,
                object,
                in_file,
                in_stream_file,
                out_file,
                out_stream,
                multipart,
            )
            if out_file is not None:
                os.remove(out_file)
    for object in objects:
        response = await client.get_properties(key=get_key(object))
        result = response.result
        assert_properties_metadata(result, object)
    for object in objects:
        response = await client.delete(key=get_key(object))
        result = response.result
        assert result is None
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_range(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[0]
    key = get_key(object)
    value = object["value"]
    await cleanup_object(provider_type, key, client)

    response = await client.put(
        key=key,
        value=value,
    )
    result = response.result
    assert_put_result(response.result, object, False)
    response = await client.get(key=key, start=3)
    result = response.result
    assert result.value == value[3:]
    response = await client.get(key=key, start=3, end=7)
    result = response.result
    assert result.value == value[3:8]
    response = await client.get(key=key, end=7)
    result = response.result
    assert result.value == value[:8]
    assert result.key.id == object["id"]
    assert result.key.version is not None
    assert result.properties.etag is not None

    await client.delete(key=key)
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_etag(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[0]
    key = get_key(object)
    value = object["value"]
    replace_value = b"Hello World Two"
    replace_value2 = b"Hello World Three"
    await cleanup_object(provider_type, key, client)

    # bad conditional put from item doesn't exist
    with pytest.raises(PreconditionFailedError):
        response = await client.put(key=key, value=value, where="exists()")

    # good conditional put from item doesn't exist
    response = await client.put(key=key, value=value, where="not_exists()")
    put_result = response.result
    assert_put_result(put_result, object, False)

    response = await client.get(key=key)
    get_result = response.result
    assert_get_result(get_result, object)
    assert put_result.properties.etag == get_result.properties.etag

    response = await client.get_properties(key=key)
    get_properties_result = response.result
    assert_properties_metadata(get_properties_result, object)
    assert get_result.properties.etag == get_properties_result.properties.etag

    # bad conditional put when item exists
    with pytest.raises(PreconditionFailedError):
        response = await client.put(
            key=key, value=replace_value, where="not_exists()"
        )

    # bad etag put
    # gcs etag as it validates the format, others don't
    with pytest.raises(PreconditionFailedError):
        response = await client.put(
            key=key,
            value=replace_value,
            where=f"$etag='{get_bad_etag(provider_type)}'",
        )

    # good etag put
    response = await client.put(
        key=key,
        value=replace_value,
        where=f"$etag='{get_result.properties.etag}'",
    )
    put_result = response.result
    assert put_result.properties.etag != get_result.properties.etag

    # good conditional put when item exists
    response = await client.put(
        key=key,
        value=replace_value2,
        where="exists()",
    )
    put_result = response.result

    # bad etag update
    # gcs etag as it validates the format, others don't
    with pytest.raises(PreconditionFailedError):
        response = await client.update(
            key=key,
            metadata={"a": "b"},
            where=f"$etag='{get_bad_etag(provider_type)}'",
        )

    # good etag update
    response = await client.update(
        key=key,
        metadata={"a": "b"},
        where=f"$etag='{put_result.properties.etag}'",
    )
    update_result = response.result

    # bad etag delete
    with pytest.raises(PreconditionFailedError):
        response = await client.delete(
            key=key,
            where=f"$etag='{get_bad_etag(provider_type)}'",
        )

    # good etag delete
    response = await client.delete(
        key=key, where=f"$etag='{update_result.properties.etag}'"
    )

    with pytest.raises(NotFoundError):
        response = await client.delete(key=key)

    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_generate(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[0]
    key = get_key(object)
    value = object["value"]
    await cleanup_object(provider_type, key, client)

    response = await client.put(
        key=key,
        value=value,
    )
    result = response.result
    assert_put_result(result, object, False)
    response = await client.generate(key=key, method="GET", expiry=300 * 1000)
    result = response.result
    if provider_type == ObjectStoreProvider.FILE_SYSTEM:
        from urllib.parse import unquote, urlparse

        parsed_url = urlparse(result.url)
        file_path = unquote(parsed_url.path.lstrip("/"))
        file_path = file_path.replace("/", "\\")
        with open(file_path, "rb") as f:
            value = f.read()
    else:
        r = httpx.get(result.url)
        value = r.content
    assert result.key.id == object["id"]
    assert object["value"] == value

    await client.delete(key=key)
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_copy(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[3]
    key = get_key(object)
    await cleanup_object(provider_type, key, client)

    copy_key = "copied.txt"
    copy_object = object
    copy_object["id"] = copy_key

    with pytest.raises(NotFoundError):
        await client.copy(
            source=key,
            key=copy_key,
        )

    response = await client.put(
        key=key,
        value=object["value"],
        metadata=object["metadata"],
        properties=object["properties"],
    )
    result = response.result
    with pytest.raises(PreconditionFailedError):
        await client.copy(
            source=key,
            key=copy_key,
            where=f"$etag='{get_bad_etag(provider_type)}'",
        )

    response = await client.copy(source=key, key=copy_key)
    result = response.result

    response = await client.get_properties(key=copy_key)
    result = response.result

    assert_properties_metadata(result, copy_object)
    etag = result.properties.etag

    response = await client.get(key=copy_key)
    result = response.result
    assert_get_result(result, copy_object)

    response = await client.copy(
        source=key, key=copy_key, where=f"$etag='{etag}'"
    )

    await client.delete(key={"id": copy_key, "version": "*"})
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_update(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[3]
    key = get_key(object)
    await cleanup_object(provider_type, key, client)

    update_object = copy.deepcopy(object)
    update_object["metadata"] = {"ustr": "uvalue", "uint": "44"}
    update_object["properties"] = {}
    """ update_object["properties"] = {
        "content_type": "application/json",
        "content_language": "en-CA",
        "storage_class": "cool",
    } """

    with pytest.raises(NotFoundError):
        update_result = await client.update(
            key=key,
            metadata=update_object["metadata"],
            properties=update_object["properties"],
        )

    response = await client.put(
        key=key,
        value=object["value"],
        metadata=object["metadata"],
        properties=object["properties"],
    )
    result = response.result
    assert_put_result(result, object)
    with pytest.raises(PreconditionFailedError):
        await client.update(
            key=key,
            metadata=update_object["metadata"],
            properties=update_object["properties"],
            where=f"$etag='{get_bad_etag(provider_type)}'",
        )

    update_response = await client.update(
        key=key,
        metadata=update_object["metadata"],
        # properties=update_object["properties"],
    )
    update_result = update_response.result
    assert_put_result(update_result, object)

    response = await client.get_properties(key=key)
    result = response.result
    assert_properties_metadata(result, update_object)

    etag = result.properties.etag
    update_response = await client.update(
        key=key,
        metadata=update_object["metadata"],
        # properties=update_object["properties"],
        where=f"$etag='{etag}'",
    )

    response = await client.get_properties(key=key)
    result = response.result
    assert_properties_metadata(result, update_object)

    await client.delete(key={"id": key, "version": "*"})
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_conditional(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[3]
    key = get_key(object)
    await cleanup_object(provider_type, key, client)

    response = await client.put(
        key=key,
        value=object["value"],
        metadata=object["metadata"],
        properties=object["properties"],
    )
    result = response.result
    assert_put_result(result, object)
    etag = result.properties.etag
    print(etag)

    for multipart in [False, True]:
        if multipart and provider_type in except_multipart_condition_providers:
            continue
        config = None
        if multipart:
            config = {"multipart": True}
        with pytest.raises(PreconditionFailedError):
            await client.get(
                key=key,
                where=f"$etag='{get_bad_etag(provider_type)}'",
                config=config,
            )

        with pytest.raises(PreconditionFailedError):
            await client.get_properties(
                key=key,
                where=f"$etag='{get_bad_etag(provider_type)}'",
            )

        with pytest.raises(NotModified):
            await client.get(
                key=key, where=f"$etag != '{etag}'", config=config
            )

        with pytest.raises(NotModified):
            await client.get_properties(key=key, where=f"$etag != '{etag}'")

        response = await client.get(
            key=key, where=f"$etag = '{etag}'", config=config
        )
        result = response.result
        assert_get_result(result, object)

        response = await client.get_properties(
            key=key, where=f"$etag = '{etag}'"
        )
        result = response.result
        assert_properties_metadata(result, object)

        response = await client.get(
            key=key, where="$etag = '*'", config=config
        )
        result = response.result
        assert_get_result(result, object)

        response = await client.get_properties(key=key, where="$etag = '*'")
        result = response.result
        assert_properties_metadata(result, object)

        if provider_type not in except_if_modified_providers:
            time.sleep(3)
            with pytest.raises(NotModified):
                await client.get(
                    key=key,
                    where="$modified > @p1",
                    params={"p1": datetime.now(timezone.utc).timestamp()},
                    config=config,
                )

            with pytest.raises(NotModified):
                await client.get_properties(
                    key=key,
                    where="$modified > @p1",
                    params={"p1": datetime.now(timezone.utc).timestamp()},
                )

            with pytest.raises(PreconditionFailedError):
                await client.get(
                    key=key,
                    where="$modified < @p1",
                    params={
                        "p1": (
                            datetime.now(timezone.utc) - timedelta(days=1)
                        ).timestamp()
                    },
                    config=config,
                )

            with pytest.raises(PreconditionFailedError):
                await client.get_properties(
                    key=key,
                    where="$modified < @p1",
                    params={
                        "p1": (
                            datetime.now(timezone.utc) - timedelta(days=1)
                        ).timestamp()
                    },
                )

            response = await client.get(
                key=key,
                where="$modified > @p1",
                params={
                    "p1": (
                        datetime.now(timezone.utc) - timedelta(days=1)
                    ).timestamp()
                },
                config=config,
            )
            result = response.result
            assert_get_result(result, object)

            response = await client.get_properties(
                key=key,
                where="$modified > @p1",
                params={
                    "p1": (
                        datetime.now(timezone.utc) - timedelta(days=1)
                    ).timestamp()
                },
            )
            result = response.result
            assert_properties_metadata(result, object)

            response = await client.get(
                key=key,
                where="$modified < @p1",
                params={"p1": datetime.now(timezone.utc).timestamp()},
                config=config,
            )
            result = response.result
            assert_get_result(result, object)

            response = await client.get_properties(
                key=key,
                where="$modified < @p1",
                params={"p1": datetime.now(timezone.utc).timestamp()},
            )
            result = response.result
            assert_properties_metadata(result, object)

    await client.delete(key=key)
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_versions(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    object = objects[3]
    key = str(uuid.uuid4())
    object["id"] = key
    await cleanup_object(provider_type, {"id": key, "version": "*"}, client)

    update_object = copy.deepcopy(object)
    update_object["value"] = b"Updated value"
    update_object["metadata"] = {"ustr": "uvalue", "uint": "44"}
    update_object["properties"] = {
        "content_type": "application/json",
        "content_language": "en-CA",
        "storage_class": "cool",
    }

    with pytest.raises(NotFoundError):
        await client.get_versions(key=key)

    response = await client.put(
        key=key,
        value=object["value"],
        metadata=object["metadata"],
        properties=object["properties"],
    )
    result = response.result
    assert_put_result(result, object)
    v1 = result.key.version

    time.sleep(0.1)
    response = await client.put(
        key=key,
        value=update_object["value"],
        metadata=update_object["metadata"],
        properties=update_object["properties"],
    )
    result = response.result
    assert_put_result(result, update_object)
    v2 = result.key.version

    response = await client.get(key={"id": key, "version": v1})
    result = response.result
    assert_get_result(result, object)

    response = await client.get(key={"id": key, "version": v2})
    result = response.result
    assert_get_result(result, update_object)

    response = await client.get_properties(key={"id": key, "version": v1})
    result = response.result
    assert_properties_metadata(result, object)

    response = await client.get_properties(key={"id": key, "version": v2})
    result = response.result
    assert_properties_metadata(result, update_object)

    time.sleep(0.1)
    response = await client.put(
        key=key,
        value=object["value"],
        metadata=object["metadata"],
        properties=object["properties"],
    )
    result = response.result
    assert_put_result(result, object)
    v3 = result.key.version

    check_version_ids = [v1, v2, v3]
    version_ids = []
    response = await client.get_versions(key=key)
    result = response.result
    for version in result.versions:
        version_ids.append(version.version)
    assert check_version_ids == version_ids

    await client.delete(key={"id": key, "version": "*"})
    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_batch(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)

    total = 4
    for i in range(0, total):
        await cleanup_object(provider_type, get_key(objects[i]), client)

    for i in range(0, total):
        await client.put(key=get_key(objects[i]), value=objects[i]["value"])

    for i in range(0, total):
        await client.get(key=get_key(objects[i]))

    batch = ObjectBatch()
    for i in range(0, total):
        batch.delete(key=get_key(objects[i]))
    print(batch)
    await client.batch(batch=batch)

    for i in range(0, total):
        with pytest.raises(NotFoundError):
            await client.get(key=get_key(objects[i]))

    await client.close()
    return


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ObjectStoreProvider.AMAZON_S3,
        ObjectStoreProvider.AZURE_BLOB_STORAGE,
        ObjectStoreProvider.GOOGLE_CLOUD_STORAGE,
        ObjectStoreProvider.FILE_SYSTEM,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query(provider_type: str, async_call: bool):
    client = ObjectStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)

    response = await client.query()
    result = response.result
    for item in result.items:
        await cleanup_object(provider_type, item.key, client)

    for object in query_objects:
        await client.put(key=get_key(object), value=object["value"])

    for query in queries:
        response = await client.query(
            where=query["where"], limit=query["limit"]
        )
        result = response.result
        result_index = query["result_index"]
        assert len(result_index) == len(result.items)
        for i in range(0, len(result_index)):
            assert (
                result.items[i].key.id == query_objects[result_index[i]]["id"]
            )
        if "prefixes" in query:
            assert query["prefixes"] == result.prefixes
        assert result.continuation is None
        if "should_count" in query:
            response = await client.count(where=query["where"])
            result = response.result
            total = len(result_index)
            if "prefixes" in query:
                total = total + len(query["prefixes"])
            assert total == result

    page_size = 5
    items = []
    result_index = queries[0]["result_index"]
    remaining = len(result_index)
    continuation = None
    while True:
        response = await client.query(
            continuation=continuation,
            config={"paging": True, "page_size": page_size},
        )
        result = response.result
        assert (
            len(result.items) == page_size
            if page_size < remaining
            else remaining
        )
        remaining = remaining - len(result.items)
        items.extend(result.items)
        continuation = result.continuation
        if continuation is None:
            break
    for i in range(0, len(result_index)):
        assert items[i].key.id == query_objects[result_index[i]]["id"]

    response = await client.query()
    result = response.result
    for item in result.items:
        await client.delete(key=item.key)
    await client.close()
    return


def get_key(item):
    return item["id"]


def assert_properties_metadata(item: ObjectItem, object: dict):
    assert item.key.id == object["id"]
    assert item.key.version is not None
    assert item.properties.etag is not None
    if "properties" in object:
        for k, v in object["properties"].items():
            assert v == getattr(item.properties, k)
    if "metadata" in object:
        assert item.metadata == object["metadata"]


def assert_get_result(
    item: ObjectItem,
    object: dict,
    in_file: Any = None,
    in_stream_file: Any = None,
    out_file: Any = None,
    out_stream: Any = None,
    multipart: bool = False,
    versioned: bool = True,
):
    assert item.key.id == object["id"]
    if not multipart:
        if versioned:
            assert item.key.version is not None
        assert item.properties.etag is not None
    if in_file is not None:
        assert filecmp.cmp(in_file, out_file)
    elif in_stream_file is not None:
        out_stream.seek(0)
        with open(in_stream_file, "rb") as in_stream:
            assert in_stream.read() == out_stream.read()
    else:
        assert item.value == object["value"]


def assert_put_result(
    item: ObjectItem,
    object: dict,
    multipart: bool = False,
    versioned: bool = True,
):
    assert item.key.id == object["id"]
    if not multipart:
        if versioned:
            assert item.key.version is not None
        assert item.properties.etag is not None


async def cleanup_object(
    provider_type, key, client: ObjectStoreSyncAndAsyncClient
):
    try:
        await client.delete(key=key)
    except NotFoundError:
        return


async def wait_for_drop(provider_type):
    if provider_type == ObjectStoreProvider.AZURE_BLOB_STORAGE:
        time.sleep(30)


def get_bad_etag(provider_type: str):
    return "CK/Z68ON54sDENED"


async def create_collection_if_needed(
    provider_type: str, client: ObjectStoreSyncAndAsyncClient
):
    if provider_type == ObjectStoreProvider.FILE_SYSTEM:
        await client.create_collection(config={"versioned": True})
