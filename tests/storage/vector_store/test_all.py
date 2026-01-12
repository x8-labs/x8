# type: ignore
import time

import pytest

from x8.core.exceptions import ConflictError, NotFoundError
from x8.storage.vector_store import CollectionStatus, VectorBatch, VectorItem

from ._data import vectors
from ._providers import VectorStoreProvider
from ._sync_and_async_client import VectorStoreSyncAndAsyncClient

queries = [
    {
        "args": {
            "search": "vector_search(vector=@p1)",
            "order_by": "$id",
            "params": {"p1": [0.0] * 4},
        },
        "result_index": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
        "count": 10,
    },
    {
        "args": {
            "search": "vector_search(vector=@p1)",
            "limit": 5,
            "params": {"p1": [0.1, 0.1, 0.1, 0.1]},
        },
        "result_index": [9, 8, 7, 6, 5],
        "count": 5,
    },
    {
        "args": {
            "search": "vector_search(vector=@p1)",
            "limit": 5,
            "offset": 2,
            "params": {"p1": [0.1, 0.1, 0.1, 0.1]},
        },
        "result_index": [7, 6, 5, 4, 3],
        "count": 5,
    },
    {
        "args": {
            "search": "vector_search(vector=@p1)",
            "order_by": "$score",
            "limit": 5,
            "params": {"p1": [0.1, 0.1, 0.1, 0.1]},
        },
        "result_index": [5, 6, 7, 8, 9],
        "count": 5,
    },
    {
        "args": {
            "search": "vector_search(vector=@p1)",
            "limit": 5,
            "where": "$metadata.int > 80",
            "params": {"p1": [0.1, 0.1, 0.1, 0.1]},
        },
        "result_index": [9, 8, 7],
        "count": 3,
    },
    {
        "args": {
            "search": "vector_search(vector=@p1, sparse_vector=@p2)",
            "limit": 5,
            "params": {"p1": [0.1, 0.1, 0.1, 0.1], "p2": {12: 0.11, 13: 0.22}},
        },
        "result_index": [9, 8, 7, 6, 5],
        "count": 5,
        "except_providers": [
            VectorStoreProvider.MILVUS,
            VectorStoreProvider.CHROMA,
            VectorStoreProvider.WEAVIATE,
        ],
    },
    {
        "args": {
            "where": "$metadata.bool = true",
            "order_by": "$id",
        },
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "*",
            "where": "$metadata.bool = false",
            "order_by": "$id",
        },
        "result_index": [5, 6, 7, 8, 9],
        "count": 5,
    },
    {
        "args": {
            "select": "$metadata",
            "where": "$metadata.bool = true",
            "order_by": "$id",
        },
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "$value, $metadata",
            "where": "$metadata.bool = true",
            "order_by": "$id",
        },
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int <= 30""",
            "order_by": "$id",
        },
        "result_index": [0, 1, 2],
        "count": 3,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int >= 30
                        and $metadata.int <= 50""",
            "order_by": "$id",
        },
        "result_index": [1, 2, 3, 4],
        "count": 4,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int between 30 and 50""",
            "order_by": "$id",
        },
        "result_index": [1, 2, 3, 4],
        "count": 4,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int < 50
                        and $metadata.str = 'value1'""",
            "order_by": "$id",
        },
        "result_index": [0],
        "count": 1,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int < 50
                        and ($metadata.str = 'value1'
                        or $metadata.str = 'value2')""",
            "order_by": "$id",
        },
        "result_index": [0, 1],
        "count": 2,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int < 50
                        and $metadata.str in ('value1', 'value2')""",
            "order_by": "$id",
        },
        "result_index": [0, 1],
        "count": 2,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and $metadata.int <= 50
                        and $metadata.str not in ('value1', 'value2')""",
            "order_by": "$id",
        },
        "result_index": [2, 3, 4],
        "count": 1,
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and array_contains($metadata.arr, 'tag1')""",
            "order_by": "$id",
        },
        "result_index": [0, 1],
        "count": 2,
        "except_providers": [VectorStoreProvider.CHROMA],
    },
    {
        "args": {
            "where": """$metadata.bool = true
                        and array_contains_any($metadata.arr,
                        ['tag1', 'tag2'])""",
            "order_by": "$id",
        },
        "result_index": [0, 1, 2, 3],
        "count": 4,
        "except_providers": [VectorStoreProvider.CHROMA],
    },
    {
        "args": {
            "where": """$metadata.int != 30
                        and $metadata.bool = true
                        and is_defined($metadata.str)
                        """,
            "order_by": "$id",
        },
        "result_index": [0, 3, 4],
        "count": 3,
        "except_providers": [
            VectorStoreProvider.CHROMA,
            VectorStoreProvider.WEAVIATE,
        ],
    },
    {
        "args": {
            "where": """$metadata.int != 30
                        and $metadata.bool = true
                        and is_not_defined($metadata.str2)
                        """,
            "order_by": "$id",
        },
        "result_index": [0, 3, 4],
        "count": 3,
        "except_providers": [
            VectorStoreProvider.CHROMA,
            VectorStoreProvider.WEAVIATE,
        ],
    },
]

except_update_providers = [
    VectorStoreProvider.MILVUS,
    VectorStoreProvider.QDRANT,
]
except_sparse_vector_providers = [
    VectorStoreProvider.MILVUS,
    VectorStoreProvider.CHROMA,
    VectorStoreProvider.WEAVIATE,
]
except_delete_filter_providers = [VectorStoreProvider.PINECONE]
except_delete_all_providers = [
    VectorStoreProvider.MILVUS,
    VectorStoreProvider.QDRANT,
    VectorStoreProvider.CHROMA,
    VectorStoreProvider.WEAVIATE,
]
except_count_providers = [VectorStoreProvider.MILVUS]
except_metadata_list_providers = [VectorStoreProvider.CHROMA]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        # VectorStoreProvider.PINECONE,
        # VectorStoreProvider.MILVUS,
        # VectorStoreProvider.QDRANT,
        VectorStoreProvider.CHROMA,
        # VectorStoreProvider.WEAVIATE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_collection(provider_type: str, async_call: bool):
    new_collection = "ntest"
    client = VectorStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    response = await client.list_collections()
    result = response.result
    if new_collection in result:
        await client.drop_collection(new_collection)

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

    item = vectors[0]
    await client.put(
        key=get_key(item),
        value=item["value"],
        metadata=item["metadata"],
        collection=new_collection,
    )
    await wait_for_put(provider_type)
    response = await client.get(get_key(item), collection=new_collection)
    result = response.result
    assert_vector_item(provider_type, result, item)

    response = await client.list_collections()
    result = response.result
    assert new_collection in result

    response = await client.has_collection(new_collection)
    result = response.result
    assert result is True

    response = await client.drop_collection(new_collection)
    result = response.result
    assert result.status == CollectionStatus.DROPPED
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
        VectorStoreProvider.PINECONE,
        VectorStoreProvider.MILVUS,
        VectorStoreProvider.QDRANT,
        VectorStoreProvider.CHROMA,
        VectorStoreProvider.WEAVIATE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query(provider_type: str, async_call: bool):
    client = VectorStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    for item in vectors:
        await cleanup_vector(provider_type, get_key(item), client=client)

    batch = VectorBatch()
    for item in vectors:
        batch.put(
            key=get_key(item),
            value=item["value"],
            metadata=item["metadata"],
        )
    response = await client.batch(batch=batch)
    result = response.result
    await wait_for_indexing(provider_type)

    response = await client.query()
    result = response.result
    assert_select_result(provider_type, result.items, vectors, False)

    if provider_type not in except_count_providers:
        response = await client.count()
        result = response.result
        assert_count_result(result, len(vectors))

    for query in queries:
        if "except_providers" in query:
            if provider_type in query["except_providers"]:
                continue
        args = query["args"]
        filtered_items = filter_items(vectors, query["result_index"])

        projected = None
        if "select" in query["args"]:
            projected = query["args"]["select"]

        response = await client.query(**args)
        result = response.result
        ordered = True if "ordered" not in query else query["ordered"]
        assert_select_result(
            provider_type, result.items, filtered_items, ordered, projected
        )

        # count = query["count"]
        # result = await client.count(**args)
        # assert_count_result(result, count)

    batch = VectorBatch()
    for item in vectors:
        batch.delete(key=get_key(item))
    response = await client.batch(batch=batch)
    result = response.result

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VectorStoreProvider.PINECONE,
        VectorStoreProvider.MILVUS,
        VectorStoreProvider.QDRANT,
        VectorStoreProvider.CHROMA,
        VectorStoreProvider.WEAVIATE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_get_update_delete(provider_type: str, async_call: bool):
    vector = vectors[0]
    old_vector = vectors[0]
    new_vector = vectors[1]
    key = get_key(vector)
    client = VectorStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    await cleanup_vector(provider_type, key, client=client)

    with pytest.raises(NotFoundError):
        await client.get(key=key)

    response = await client.put(
        key=key, value=vector["value"], metadata=vector["metadata"]
    )
    result = response.result
    assert_put_result(result, vector)
    await wait_for_put(provider_type)

    response = await client.get(key=key)
    result = response.result
    assert_vector_item(provider_type, result, vector)

    vector["value"] = new_vector["value"]
    vector["metadata"] = new_vector["metadata"]
    response = await client.put(
        key=key, value=vector["value"], metadata=vector["metadata"]
    )
    result = response.result
    assert_put_result(result, vector)
    await wait_for_put(provider_type)

    response = await client.get(key=key)
    result = response.result
    assert_vector_item(provider_type, result, vector)

    if provider_type not in except_update_providers:
        vector["value"] = old_vector["value"]
        response = await client.update(key=key, value=vector["value"])
        result = response.result
        assert_put_result(result, vector)

        response = await client.get(key=key)
        result = response.result
        assert_vector_item(provider_type, result, vector)

        vector["metadata"] = old_vector["metadata"]
        response = await client.update_metadata(
            key=key, metadata=vector["metadata"]
        )
        result = response.result
        assert_put_result(result, vector)

        response = await client.get(key=key)
        result = response.result
        assert_vector_item(provider_type, result, vector)

    response = await client.delete(key=key)
    result = response.result
    assert result is None
    await wait_for_delete(provider_type)

    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # delete is best effort
    await client.delete(key=key)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VectorStoreProvider.PINECONE,
        VectorStoreProvider.MILVUS,
        VectorStoreProvider.QDRANT,
        VectorStoreProvider.CHROMA,
        VectorStoreProvider.WEAVIATE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_batch(provider_type: str, async_call: bool):
    client = VectorStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    for item in vectors:
        await cleanup_vector(provider_type, get_key(item), client=client)

    batch = VectorBatch()
    for item in vectors:
        batch.put(
            key=get_key(item),
            value=item["value"],
            metadata=item["metadata"],
        )
    response = await client.batch(batch=batch)
    result = response.result
    for i in range(0, len(vectors)):
        assert_put_result(result[i], vectors[i])

    await wait_for_put(provider_type)

    batch = VectorBatch()
    for item in vectors:
        batch.get(key=get_key(item))
    response = await client.batch(batch=batch)
    result = response.result
    result = sorted(result, key=lambda x: x.key.id)
    sorted_data = sorted(vectors, key=lambda x: x["id"])
    for i in range(0, len(vectors)):
        assert_vector_item(provider_type, result[i], sorted_data[i])

    batch = VectorBatch()
    for item in vectors:
        batch.delete(key=get_key(item))
    response = await client.batch(batch=batch)
    result = response.result
    for i in range(0, len(vectors)):
        assert result[i] is None

    await wait_for_delete(provider_type)
    for item in vectors:
        with pytest.raises(NotFoundError):
            await client.get(key=get_key(item))

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VectorStoreProvider.PINECONE,
        VectorStoreProvider.MILVUS,
        VectorStoreProvider.QDRANT,
        VectorStoreProvider.CHROMA,
        VectorStoreProvider.WEAVIATE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_delete(provider_type: str, async_call: bool):
    client = VectorStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)
    for item in vectors:
        await cleanup_vector(provider_type, get_key(item), client=client)

    batch = VectorBatch()
    for item in vectors:
        batch.put(
            key=get_key(item),
            value=item["value"],
            metadata=item["metadata"],
        )
    await client.batch(batch=batch)
    await wait_for_put(provider_type)

    for i in range(0, len(vectors)):
        await client.get(get_key(item))

    if provider_type not in except_delete_filter_providers:
        await client.delete(where="$metadata.bool = true")
        await wait_for_delete_bulk(provider_type)
        for i in range(0, 5):
            with pytest.raises(NotFoundError):
                await client.get(get_key(vectors[i]))

    for i in range(5, 10):
        await client.get(get_key(item))

    if provider_type not in except_delete_all_providers:
        await client.delete()
        await wait_for_delete_bulk(provider_type)
        for i in range(0, len(vectors)):
            with pytest.raises(NotFoundError):
                await client.get(get_key(vectors[i]))

    await client.close()


def get_key(vector):
    return vector["id"]


def assert_put_result(item: VectorItem, vector: dict):
    assert item.key.id == vector["id"]


def assert_vector_list(
    provider_type, result: list[VectorItem], sorted_index: list[int]
):
    assert len(result) == len(sorted_index)
    for i in range(len(sorted_index)):
        assert_vector_item(provider_type, result[i], vectors[sorted_index[i]])


def assert_vector_item(provider_type, item: VectorItem, vector: dict):
    assert item.key.id == vector["id"]
    if "value" in vector:
        for idx, val in enumerate(item.value.vector):
            assert round(val, 1) == vector["value"]["vector"][idx]
        if provider_type not in except_sparse_vector_providers:
            assert item.value.sparse_vector == vector["value"]["sparse_vector"]
        assert item.value.content == vector["value"]["content"]
    if "metadata" in vector:
        citem = None
        if vector["metadata"] is not None:
            citem = {}
            for k, v in vector["metadata"].items():
                if isinstance(v, list):
                    if provider_type not in except_metadata_list_providers:
                        citem[k] = v
                else:
                    citem[k] = v
        assert item.metadata == citem


def assert_select_result(
    provider_type: str,
    result: list,
    items: list,
    ordered: bool = True,
    projected: str | None = None,
):
    if projected is not None and projected != "*":
        fields = [field.strip() for field in projected.split(",")]
        projected_items = []
        for item in items:
            pitem = {}
            pitem["id"] = item["id"]
            for field in fields:
                pitem[field.replace("$", "")] = item[field.replace("$", "")]
            projected_items.append(pitem)
        items = projected_items
    assert len(result) == len(items)
    if not ordered:
        result = sorted(result, key=lambda x: x.key.id)
        items = sorted(items, key=lambda x: x["id"])
    for i in range(0, len(result)):
        assert_vector_item(provider_type, result[i], items[i])


def assert_count_result(result: int, count: int):
    assert result == count


def filter_items(items: list, index: list) -> list:
    result = []
    for i in range(0, len(index)):
        result.append(items[index[i]])
    return result


async def cleanup_vector(
    provider_type, key, client: VectorStoreSyncAndAsyncClient
):
    try:
        await client.delete(key=key)
    except NotFoundError:
        return

    # await wait_for_delete(provider_type)


async def wait_for_delete_bulk(provider_type):
    if provider_type == VectorStoreProvider.PINECONE:
        time.sleep(60)
    if provider_type == VectorStoreProvider.MILVUS:
        time.sleep(20)


async def wait_for_indexing(provider_type):
    if provider_type == VectorStoreProvider.PINECONE:
        time.sleep(20)
    if provider_type == VectorStoreProvider.MILVUS:
        time.sleep(20)


async def wait_for_delete(provider_type):
    if (
        provider_type == VectorStoreProvider.MILVUS
        or provider_type == VectorStoreProvider.PINECONE
    ):
        time.sleep(5)


async def wait_for_put(provider_type):
    if (
        provider_type == VectorStoreProvider.MILVUS
        or provider_type == VectorStoreProvider.PINECONE
    ):
        time.sleep(5)


async def create_collection_if_needed(
    provider_type: str, client: VectorStoreSyncAndAsyncClient
):
    response = await client.list_collections()
    result = response.result
    if "test" in result:
        return
    await client.create_collection(
        collection="test",
        config={"vector_index": {"field": "$value"}},
    )
