# type: ignore

import copy
import time

import pytest

from x8.storage._common import Comparator
from x8.storage.search_store import (
    CollectionStatus,
    ConflictError,
    NotFoundError,
    PreconditionFailedError,
    SearchItem,
)

from ._data import documents
from ._providers import SearchStoreProvider
from ._queries import (
    bad_complex_condition_1,
    complex_condition_1,
    complex_condition_1_params,
    complex_condition_1_with_params,
    complex_condition_2,
    hybrid_text_search_queries,
    hybrid_vector_search_queries,
    queries,
    sparse_vector_search_queries,
    text_search_queries,
    vector_search_queries,
)
from ._sync_and_async_client import SearchStoreSyncAndAsyncClient


def get_key(document: dict) -> dict:
    return {"id": document["id"]}


def update_0(doc):
    doc["float"] += 0.5
    doc["obj"]["nint"] -= 10
    return doc


def update_1(doc):
    doc["int"] = 99
    doc["str"] = "new nine"
    doc["obj"]["nstr"] = "90"
    doc["float"] = 1.9
    doc["newint"] = 999
    doc["obj"]["nnewstr"] = None
    doc.pop("bool")
    doc["obj"].pop("narr")
    doc["arrint"] = [1, 2, 3]
    doc["newobj"] = {"int": 90, "str": "ninety"}
    return doc


def update_2(doc):
    doc["arrint"][0] = 999
    doc["arrstr"].insert(0, "nine")
    del doc["obj"]["narr"][0]
    doc["arrint"][1] -= 100
    doc["arrobj"][1]["oint"] = 900
    return doc


def update_3(doc):
    doc["arrstr"].append("million nine")
    doc["arrint"].append(90009)
    doc["obj"]["narr"].append(980)
    doc["arrobj"].append({"ostr": "a", "oint": 9})
    return doc


def update_4(doc):
    doc["arrstr"].insert(1, "ten nine")
    doc["arrint"].insert(2, 919)
    doc["obj"]["narr"].insert(3, 925)
    doc["arrobj"].insert(2, {"ostr": "b", "oint": 99})
    return doc


def update_5(doc):
    doc["newarrstr"] = doc["arrstr"]
    doc.pop("arrstr")
    doc["obj"]["arrobj"] = doc["arrobj"]
    doc.pop("arrobj")
    doc["nstr"] = doc["obj"]["nstr"]
    doc["obj"].pop("nstr")
    doc["obj"]["nobj"]["newnnstr"] = doc["obj"]["nobj"]["nnstr"]
    doc["obj"]["nobj"].pop("nnstr")
    return doc


def update_6(doc):
    doc["arrstr"].append("million nine")
    doc["arrstr"].append("billion nine")
    doc["arrint"].remove(99)
    doc["obj"]["narr"].append(980)
    return doc


updates = [
    {
        "set": """
            float=increment(0.5),
            obj.nint=increment(-10)
            """,
        "update_method": update_0,
    },
    {
        "set": """
            int=put(99),
            str=put('new nine'),
            obj.nstr=put('90'),
            float=insert(1.9),
            newint=insert(999),
            obj.nnewstr=insert(null),
            bool=delete(),
            obj.narr=delete(),
            arrint=put([1, 2, 3]),
            newobj=put({ "int": 90, "str": "ninety" })
            """,
        "update_method": update_1,
    },
    {
        "set": """
            int=put(@s1),
            str=put(@s2),
            obj.nstr=put(@s3),
            float=insert(@s4),
            newint=insert(@s5),
            obj.nnewstr=insert(@s6),
            bool=delete(),
            obj.narr=delete(),
            arrint=put(@s7),
            newobj=put(@s8)
            """,
        "params": {
            "s1": 99,
            "s2": "new nine",
            "s3": "90",
            "s4": 1.9,
            "s5": 999,
            "s6": None,
            "s7": [1, 2, 3],
            "s8": {"int": 90, "str": "ninety"},
        },
        "update_method": update_1,
    },
    {
        "set": """
            arrint[0]=put(999),
            arrstr[0]=insert('nine'),
            obj.narr[0]=delete(),
            arrint[1]=increment(-100),
            arrobj[1].oint=put(900)
            """,
        "update_method": update_2,
    },
    {
        "set": """
            arrstr[-]=insert('million nine'),
            arrint[-]=insert(90009),
            obj.narr[-]=insert(980),
            arrobj[-]=insert({"ostr": "a", "oint": 9})
            """,
        "update_method": update_3,
    },
    {
        "set": """
            arrstr[1]=insert('ten nine'),
            arrint[2]=insert(919),
            obj.narr[3]=insert(925),
            arrobj[2]=insert({"ostr": "b", "oint": 99})
            """,
        "update_method": update_4,
    },
    {
        "set": """
            arrstr=move(newarrstr),
            arrobj=move(obj.arrobj),
            obj.nstr=move(nstr),
            obj.nobj.nnstr=move(obj.nobj.newnnstr)
            """,
        "update_method": update_5,
    },
    {
        "set": """
            arrstr=array_union(["nineteen", "million nine", "billion nine"]),
            arrint=array_remove([99, 90009]),
            obj.narr=array_union([960, 970, 980])
            """,
        "update_method": update_6,
    },
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SearchStoreProvider.ELASTICSEARCH,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_collection(provider_type: str, async_call: bool):
    index_needed_providers: list = []

    new_collection = f"ntest{str(async_call).lower()}"
    client = SearchStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    config = None
    if provider_type in index_needed_providers:
        config = {
            "indexes": [
                {
                    "type": "composite",
                    "fields": [
                        {"type": "asc", "field": "id", "field_type": "string"},
                        {"type": "asc", "field": "pk", "field_type": "string"},
                    ],
                }
            ]
        }

    response = await client.list_collections()
    result = response.result
    if new_collection in result:
        await client.drop_collection(collection=new_collection)

    response = await client.list_collections()
    result = response.result
    assert new_collection not in result

    response = await client.has_collection(collection=new_collection)
    result = response.result
    assert result is False

    response = await client.create_collection(
        collection=new_collection, config=config
    )
    result = response.result
    assert result.status == CollectionStatus.CREATED

    response = await client.create_collection(
        collection=new_collection, config=config
    )
    result = response.result
    assert result.status == CollectionStatus.EXISTS
    with pytest.raises(ConflictError):
        await client.create_collection(
            collection=new_collection, where="not_exists()"
        )

    await client.put(value=documents[0], collection=new_collection)
    response = await client.get(
        key=get_key(documents[0]), collection=new_collection
    )
    result = response.result
    assert_get_result(result, documents[0])

    response = await client.list_collections()
    result = response.result
    assert new_collection in result

    response = await client.has_collection(collection=new_collection)
    result = response.result
    assert result is True

    response = await client.drop_collection(collection=new_collection)
    result = response.result
    assert result.status == CollectionStatus.DROPPED

    response = await client.list_collections()
    result = response.result
    assert new_collection not in result

    response = await client.drop_collection(collection=new_collection)
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
        SearchStoreProvider.ELASTICSEARCH,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_get_delete(provider_type: str, async_call: bool):
    client = SearchStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )
    await create_collection_if_needed(provider_type, client)

    document = documents[-1]
    replace_document = documents[-2].copy()
    replace_document["id"] = document["id"]
    replace_document["pk"] = document["pk"]
    key = get_key(document)
    await cleanup_document(document, client)

    # get when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # unconditional delete when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.delete(key=key)

    # unconditional put when item doesn't exist
    response = await client.put(value=document)
    result = response.result
    assert_put_result(result, document)

    # get when item exists
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, document)
    assert Comparator.contains(replace_document, result.value) is False

    # unconditional put when item exists
    response = await client.put(value=document)
    result = response.result
    assert_put_result(result, document)
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, document)
    assert Comparator.contains(document, result.value)

    # unconditional delete when item exists
    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    # get when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.get(key=key)

    # conditional put (exists=True) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.put(value=document, where="exists()")

    # conditional put (exists=False) when item doesn't exist
    # INSERT
    response = await client.put(value=document, where="not_exists()")
    result = response.result
    assert_put_result(result, document)
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, document)

    # conditional put (exists=False) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.put(value=document, where="not_exists()")

    # conditional put (exists=True) when item exists
    # REPLACE
    put_response = await client.put(value=replace_document, where="exists()")
    put_result = put_response.result
    assert_put_result(put_result, replace_document)
    get_response = await client.get(key=key)
    get_result = get_response.result
    assert_get_result(get_result, replace_document)
    assert get_result.properties.etag == put_result.properties.etag

    old_etag = get_result.properties.etag
    etag_condition = f"$etag='{old_etag}'"
    bad_etag_condition = f"$etag='{old_etag}1'"

    # conditional put (bad etag) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.put(value=document, where=bad_etag_condition)

    # conditional put (good etag) when item exists
    put_response = await client.put(value=document, where=etag_condition)
    put_result = put_response.result
    assert_put_result(put_result, document)
    get_response = await client.get(key=key)
    get_result = get_response.result
    assert_get_result(get_result, document)
    assert get_result.properties.etag == put_result.properties.etag
    assert old_etag != get_result.properties.etag

    etag = get_result.properties.etag
    etag_condition = f"$etag='{etag}'"

    # conditional delete (bad etag) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.delete(key=key, where=bad_etag_condition)

    # conditional delete (good etag) when item exists
    response = await client.delete(key=key, where=etag_condition)
    result = response.result
    assert_delete_result(result)

    # conditional put (etag) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.put(value=document, where=etag_condition)

    # conditional delete (bad etag) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.delete(key=key, where=etag_condition)

    response = await client.put(value=document)
    result = response.result
    assert_put_result(result, document)

    # conditional put (bad condition) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.put(value=replace_document, where=bad_complex_condition_1)

    # conditional put (good condition) when item exists
    put_response = await client.put(
        value=replace_document, where=complex_condition_1
    )
    put_result = put_response.result
    assert_put_result(put_result, replace_document)

    # conditional delete (bad condition) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.delete(key=key, where=complex_condition_1)

    # conditional delete (good condition) when item exists
    response = await client.delete(key=key, where=complex_condition_2)
    result = response.result
    assert_delete_result(result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SearchStoreProvider.ELASTICSEARCH,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_update(provider_type: str, async_call: bool):
    client = SearchStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await create_collection_if_needed(provider_type, client)
    document = documents[-1]
    document_copy = copy.deepcopy(document)
    key = get_key(document)
    await cleanup_document(document, client)

    set = updates[0]["set"]
    update_method = updates[0]["update_method"]

    # unconditional update when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.update(key=key, set=set)

    bad_etag_condition = "$etag='1-1'"
    # unconditional update (etag) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.update(key=key, set=set, where=bad_etag_condition)

    bad_etag_condition = "$etag='1-1'"
    # unconditional update (condition) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.update(key=key, set=set, where=complex_condition_1)

    response = await client.put(value=document)
    result = response.result
    assert_put_result(result, document)

    # unconditional update when item exists with not returning
    document_copy = update_method(document_copy)
    response = await client.update(key=key, set=set)
    result = response.result
    assert_update_result(result, document_copy, "old")

    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, document_copy)

    # unconditional update when item exists with not returning
    document_copy = update_method(document_copy)
    response = await client.update(key=key, set=set, returning="new")
    result = response.result
    assert_update_result(result, document_copy, "new")

    # conditional update (bad etag) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.update(key=key, set=set, where=bad_etag_condition)

    # conditional update (good etag) when item exists
    etag = result.properties.etag
    etag_condition = f"$etag='{etag}'"
    document_copy = update_method(document_copy)
    response = await client.update(
        key=key, set=set, where=etag_condition, returning="new"
    )
    result = response.result
    assert_update_result(result, document_copy, "new")

    # replace to original document
    response = await client.put(value=document)
    result = response.result
    document_copy = copy.deepcopy(document)

    # condition update (good condition) when item exists
    document_copy = update_method(document_copy)
    response = await client.update(
        key=key,
        set=set,
        where=complex_condition_1_with_params,
        returning="new",
        params=complex_condition_1_params,
    )
    result = response.result
    assert_update_result(result, document_copy, "new")

    # condition update (bad condition) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.update(
            key=key, set=set, where=complex_condition_1, returning="new"
        )

    for i in range(1, len(updates)):
        if "except_providers" in updates[i]:
            if provider_type in updates[i]["except_providers"]:
                continue

        # replace to original document
        result = await client.put(value=document)
        document_copy = copy.deepcopy(document)

        set = updates[i]["set"]
        update_method = updates[i]["update_method"]
        params = None
        if "params" in updates[i]:
            params = updates[i]["params"]

        response = await client.update(
            key=key,
            set=set,
            where=complex_condition_1,
            returning="new",
            params=params,
        )
        result = response.result
        document_copy = update_method(document_copy)
        assert_update_result(result, document_copy, "new")

        response = await client.get(key=key)
        result = response.result
        assert_get_result(result, document_copy)

    response = await client.delete(key=key)
    result = response.result
    assert_delete_result(result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SearchStoreProvider.ELASTICSEARCH,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query_count(provider_type: str, async_call: bool):
    client = SearchStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await client.drop_collection()
    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    for document in documents:
        response = await client.put(value=document)
        result = response.result
        assert_put_result(result, document)

    time.sleep(1)  # wait for indexing

    response = await client.query()
    result = response.result
    assert_select_result(result.items, documents, False)

    response = await client.count()
    result = response.result
    assert_count_result(result, len(documents))

    for query in queries:
        if "except_providers" in query:
            if provider_type in query["except_providers"]:
                continue
        args = query["args"]
        filtered_documents = filter_documents(documents, query["result_index"])

        projected = None
        if "select" in query["args"]:
            projected = query["args"]["select"]

        response = await client.query(**args)
        result = response.result
        ordered = True if "ordered" not in query else query["ordered"]

        assert_select_result(
            result.items, filtered_documents, ordered, projected
        )

        count = query["count"]
        response = await client.count(**args)
        result = response.result
        assert_count_result(result, count)

    for document in documents:
        key = get_key(document)
        response = await client.delete(key=key)
        result = response.result
        assert_delete_result(result)

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        SearchStoreProvider.ELASTICSEARCH,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_search(provider_type: str, async_call: bool):
    client = SearchStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    search_queries = [
        text_search_queries,
        vector_search_queries,
        sparse_vector_search_queries,
        hybrid_vector_search_queries,
        hybrid_text_search_queries,
    ]

    await client.drop_collection()
    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    for document in documents:
        response = await client.put(value=document)
        result = response.result
        assert_put_result(result, document)

    time.sleep(1)  # wait for indexing

    for q in search_queries:
        for query in q:
            if "except_providers" in query:
                if provider_type in query["except_providers"]:
                    continue
            args = query["args"]
            filtered_documents = filter_documents(
                documents, query["result_index"]
            )

            projected = None
            if "select" in query["args"]:
                projected = query["args"]["select"]

            response = await client.query(**args)
            result = response.result
            ordered = True if "ordered" not in query else query["ordered"]
            assert_select_result(
                result.items, filtered_documents, ordered, projected
            )

    for document in documents:
        key = get_key(document)
        response = await client.delete(key=key)
        result = response.result
        assert_delete_result(result)

    await client.close()


def filter_documents(documents: list, index: list) -> list:
    result = []
    for i in range(0, len(index)):
        result.append(documents[index[i]])
    return result


def assert_select_result(
    result: list[SearchItem],
    documents: list,
    ordered: bool = True,
    projected: str | None = None,
):
    if projected:
        if projected != "*":
            fields = [field.strip() for field in projected.split(",")]
            projected_documents = []
            for doc in documents:
                pdoc = {}
                for field in fields:
                    if "." in field:
                        parts = field.split(".")
                        current_pdoc = pdoc
                        current_doc = doc
                        for i in range(0, len(parts) - 1):
                            if parts[i] not in pdoc:
                                current_pdoc[parts[i]] = {}
                            current_pdoc = current_pdoc[parts[i]]
                            current_doc = current_doc[parts[i]]
                            current_pdoc[parts[-1]] = current_doc[parts[-1]]
                    else:
                        pdoc[field.replace("$", "")] = doc[
                            field.replace("$", "")
                        ]
                projected_documents.append(pdoc)
            documents = projected_documents
    assert len(result) == len(documents)
    if not ordered:
        result = sorted(result, key=lambda x: x.key.id)
        documents = sorted(documents, key=lambda x: x["id"])
    for i in range(0, len(result)):
        assert_get_result(result[i], documents[i], etag=projected is None)


def assert_count_result(result: int, count: int):
    assert result == count


def assert_put_result(result: SearchItem, document: dict):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    assert result.properties.etag is not None


def assert_update_result(result: SearchItem, document: dict, returning: str):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    assert result.properties.etag is not None
    if returning == "new":
        assert Comparator.contains(document, result.value)


def assert_get_result(
    result: SearchItem, document: dict, etag: bool | None = True
):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    if etag:
        assert result.properties.etag is not None
    assert Comparator.contains(document, result.value)


def assert_delete_result(result: dict | None):
    assert result is None


async def cleanup_document(
    document: dict, client: SearchStoreSyncAndAsyncClient
):
    try:
        await client.delete(key=get_key(document))
    except NotFoundError:
        return


async def create_collection_if_needed(
    provider_type: str,
    client: SearchStoreSyncAndAsyncClient,
):
    indexes = [
        {
            "field": "id",
            "type": "range",
            "field_type": "string",
        },
        {
            "field": "pk",
            "type": "range",
            "field_type": "string",
        },
        {
            "field": "int",
            "type": "range",
            "field_type": "integer",
        },
        {
            "field": "str",
            "type": "range",
            "field_type": "string",
        },
        {
            "field": "arrstr",
            "type": "array",
            "field_type": "string",
        },
        {
            "field": "obj.nobj.nnstr",
            "type": "range",
            "field_type": "string",
        },
        {
            "field": "text1",
            "type": "text",
            "field_type": "string",
        },
        {
            "field": "text2",
            "type": "text",
            "field_type": "string",
        },
        {
            "field": "vector",
            "type": "vector",
            "dimension": 4,
            "metric": "euclidean",
        },
        {
            "field": "sparse_vector",
            "type": "sparse_vector",
        },
    ]
    await client.create_collection(config={"indexes": indexes})
