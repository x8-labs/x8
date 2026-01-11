# type: ignore
import asyncio
import copy
import json
import os

import pytest
from x8.core import DataModel
from x8.storage._common import Comparator, StoreOperation
from x8.storage.document_store import (
    CollectionStatus,
    ConflictError,
    DocumentItem,
    DocumentStoreFeature,
    IndexStatus,
    NotFoundError,
    PreconditionFailedError,
)

from ._data import bson, documents
from ._providers import DocumentStoreProvider
from ._sync_and_async_client import DocumentStoreSyncAndAsyncClient

if os.name == "nt":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def get_key(document: dict) -> dict:
    return {"id": document["id"], "pk": document["pk"]}


complex_condition_1 = """
    length(arrstr[0]) > 7
    and contains(arrstr[1], 'und')
    and not contains(const, '$')
    and starts_with(arrobj[0].ostr, 'nine')
    and 8 = array_length(obj.narr)
    and array_contains(arrint, 909)
    and array_contains_any(arrstr, ['xyz', 'hundred nine', 'abc'])
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, 'number')
    and is_type(obj.nobj, 'object')
    and is_type(empty, 'null')
    and not is_type(bool, 'array')
    and obj.nstr = "9"
    and arrobj[1].oint = 9000000000
    and 9.1 != float
    and int >= 8 and (bool = true or obj.nobj.nnfloat <= -900.1)
    and not (pk = "pk00" or length(obj.nobj.nnstr) != 2)
    and obj.nint between -10 and 10
    and str in ('one', 'two', 'eight', 'nine')
    and obj.nint not in (-1, -2, -8)
    """
bad_complex_condition_1 = f"{complex_condition_1} and obj.nint > 0"

complex_condition_1_with_params = """
    length(arrstr[0]) > @p1
    and contains(arrstr[1], @p2)
    and not contains(const, @p3)
    and starts_with(arrobj[0].ostr, @p4)
    and @p5 = array_length(obj.narr)
    and array_contains(arrint, @p6)
    and array_contains_any(arrstr, @p7)
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, @p8)
    and is_type(obj.nobj, @p9)
    and is_type(empty, @p10)
    and not is_type(bool, @p11)
    and obj.nstr = @p12
    and arrobj[1].oint = @p13
    and @p14 != float
    and int >= @p15 and (bool = @p16 or obj.nobj.nnfloat <= @p17)
    and not (pk = @p18 or length(obj.nobj.nnstr) != @p19)
    and obj.nint between @p20 and @p21
    and str in (@p22, @p23, @p24, @p25)
    and obj.nint not in (@p26, @p27, @p28)
    """

complex_condition_1_params = {
    "p1": 7,
    "p2": "und",
    "p3": "$",
    "p4": "nine",
    "p5": 8,
    "p6": 909,
    "p7": ["xyz", "hundred nine", "abc"],
    "p8": "number",
    "p9": "object",
    "p10": "null",
    "p11": "array",
    "p12": "9",
    "p13": 9000000000,
    "p14": 9.1,
    "p15": 8,
    "p16": True,
    "p17": -900.1,
    "p18": "pk00",
    "p19": 2,
    "p20": -10,
    "p21": 10,
    "p22": "one",
    "p23": "two",
    "p24": "eight",
    "p25": "nine",
    "p26": -1,
    "p27": -2,
    "p28": -8,
}

complex_condition_2 = """
    length(arrstr[0]) > 7
    and contains(arrstr[1], 'und')
    and not contains(const, '$')
    and starts_with(arrobj[0].ostr, 'eight')
    and 7 = array_length(obj.narr)
    and array_contains(arrint, 808)
    and array_contains_any(arrstr, ['xyz', 'hundred eight', 'abc'])
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, 'number')
    and is_type(obj.nobj, 'object')
    and is_type(empty, 'null')
    and not is_type(bool, 'array')
    and obj.nstr = "8"
    and arrobj[1].oint = 8000000000
    and 8.1 != float
    and int >= 7 and (bool = false or obj.nobj.nnfloat <= -800.1)
    and not (pk = "pk00" or length(obj.nobj.nnstr) != 4)
    and obj.nint between -10 and 10
    and str in ('one', 'two', 'eight', 'nine')
    and obj.nint not in (-1, -2, -9)
    """

queries = [
    {
        "args": {
            "where": "pk = 'pk00'",
            "order_by": "id",
        },
        "statement": "WHERE pk = 'pk00' ORDER BY id",
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "*",
            "where": "$pk = 'pk00'",
            "order_by": "$id",
        },
        "statement": "SELECT * WHERE $pk = 'pk00' ORDER BY $id",
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "bool, obj, $pk, $id",
            "where": "$pk = 'pk00'",
            "order_by": "$id",
        },
        "statement": """SELECT bool, obj, $pk, $id
                    WHERE $pk = 'pk00' ORDER BY $id""",
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "obj.nstr, obj.nint, $pk, $id",
            "where": "$pk = 'pk00'",
            "order_by": "$id",
        },
        "statement": "SELECT @p1 WHERE @p2 ORDER BY @p3",
        "params": {
            "p1": "obj.nstr, obj.nint, $pk, $id",
            "p2": "$pk = @p4",
            "p3": "$id",
            "p4": "pk00",
        },
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "where": "pk = 'pk01'",
            "order_by": "id DESC",
        },
        "statement": "WHERE pk = 'pk01' ORDER BY id DESC",
        "result_index": [9, 8, 7, 6, 5],
        "count": 5,
    },
    {
        "args": {
            "where": "pk = 'pk01'",
            "order_by": "id DESC",
            "limit": 3,
        },
        "statement": "WHERE pk = 'pk01' ORDER BY id DESC LIMIT 3",
        "result_index": [9, 8, 7],
        "count": 5,
    },
    {
        "args": {
            "where": "pk = 'pk01'",
            "order_by": "id DESC",
            "limit": 10,
            "offset": 3,
        },
        "statement": "WHERE pk = 'pk01' ORDER BY id DESC LIMIT 10 OFFSET 3",
        "result_index": [6, 5],
        "count": 5,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and int <= 2
                """,
            "order_by": "int",
        },
        "statement": "WHERE pk = 'pk00' and int <=2 ORDER BY int",
        "result_index": [0, 1, 2],
        "count": 3,
    },
    {
        "args": {
            "where": """
                pk = 'pk01' and str > 'eight'
                """,
            "order_by": "str",
        },
        "statement": "WHERE pk = 'pk01' and str > 'eight' ORDER BY str",
        "result_index": [5, 9, 7, 6],
        "count": 4,
        "except_providers": [DocumentStoreProvider.REDIS],
    },
    {
        "args": {
            "where": """
                 pk = 'pk01' and int != 7
                 """
        },
        "statement": "WHERE pk = 'pk01' and int !=7",
        "result_index": [9, 8, 6, 5],
        "ordered": False,
        "count": 4,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and int between 1 and 3
                """,
            "order_by": "int DESC",
        },
        "statement": """WHERE pk = 'pk00' and int between 1 and 3
                        ORDER BY int DESC""",
        "result_index": [3, 2, 1],
        "count": 3,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and opt in ('abcd', 'cdef', 'xxx')
                """
        },
        "statement": """WHERE pk = 'pk00'
                    and opt in ('abcd', 'cdef', 'xxx')
                    """,
        "result_index": [2, 4],
        "ordered": False,
        "count": 2,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and int not in (1, 3, 5)
                """
        },
        "statement": "WHERE pk = 'pk00' and int not in (1, 3, 5)",
        "result_index": [0, 2, 4],
        "ordered": False,
        "count": 3,
    },
    {
        "args": {
            "where": """
                pk = 'pk01'
                and str = 'xsix'
                and (int = 5 or bool = true)
                """,
            "order_by": "str",
        },
        "statement": """WHERE
                pk = 'pk01'
                and str = 'xsix'
                and (int = 5 or bool = true)
                ORDER BY str
                """,
        "result_index": [6],
        "count": 1,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and obj.nint >= -1
                """
        },
        "statement": """WHERE
                pk = 'pk00' and obj.nint >= -1
                """,
        "result_index": [0, 1],
        "ordered": False,
        "count": 2,
    },
    {
        "args": {
            "where": """
                pk = 'pk01' and starts_with(str, 'xs')
                """,
            "order_by": "str",
        },
        "statement": """WHERE
                pk = 'pk01' and starts_with(str, 'xs')
                ORDER BY str
                """,
        "result_index": [7, 6],
        "count": 2,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and array_contains(arrstr, 'zero')
                """,
            "order_by": "str",
        },
        "statement": """WHERE
                pk = 'pk00' and array_contains(arrstr, 'zero')
                ORDER BY str
                """,
        "result_index": [4, 3, 2, 0],
        "count": 4,
    },
    {
        "args": {
            "where": """
                pk = 'pk01'
                and array_contains(arrobj,
                {"ostr": "nine million", "oint": 9000000})
                """,
            "order_by": "str",
        },
        "statement": """WHERE
                pk = 'pk01'
                and array_contains(arrobj,
                {"ostr": "nine million", "oint": 9000000})
                ORDER BY str
                """,
        "result_index": [9],
        "count": 1,
        "except_providers": [DocumentStoreProvider.REDIS],
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and array_contains_any(obj.narr, [400, 200, 1000])
                """,
            "order_by": "int DESC",
        },
        "statement": "WHERE @p1 ORDER BY @p2",
        "params": {
            "p1": """pk = 'pk00'
                    and array_contains_any(obj.narr, [400, 200, 1000])""",
            "p2": "int DESC",
        },
        "result_index": [4, 2],
        "count": 2,
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and contains(opt, 'cde')
                """,
            "order_by": "int",
        },
        "statement": "WHERE @p1 ORDER BY @p2",
        "params": {"p1": "pk = 'pk00' and contains(opt, 'cde')", "p2": "int"},
        "result_index": [3, 4],
        "count": 2,
        "except_providers": [DocumentStoreProvider.GOOGLE_FIRESTORE],
    },
    {
        "args": {
            "where": """
                pk = 'pk00' and arrstr[3] = 'zero'
                and obj.narr[0] <= 200
                and arrobj[1].oint >= 2000000000
                """
        },
        "statement": """WHERE
                pk = 'pk00' and arrstr[3] = 'zero'
                and obj.narr[0] <= 200
                and arrobj[1].oint >= 2000000000
                """,
        "result_index": [2],
        "ordered": False,
        "count": 1,
        "except_providers": [
            DocumentStoreProvider.GOOGLE_FIRESTORE,
            DocumentStoreProvider.REDIS,
        ],
    },
    {
        "args": {"where": complex_condition_1},
        "statement": f"WHERE {complex_condition_1}",
        "result_index": [9],
        "count": 1,
        "except_providers": [
            DocumentStoreProvider.GOOGLE_FIRESTORE,
            DocumentStoreProvider.REDIS,
        ],
    },
    {
        "args": {"where": complex_condition_2},
        "statement": f"WHERE {complex_condition_2}",
        "result_index": [8],
        "count": 1,
        "except_providers": [
            DocumentStoreProvider.GOOGLE_FIRESTORE,
            DocumentStoreProvider.REDIS,
        ],
    },
]


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
        "except_providers": [DocumentStoreProvider.GOOGLE_FIRESTORE],
    },
    {
        "set": """
            arrstr[-]=insert('million nine'),
            arrint[-]=insert(90009),
            obj.narr[-]=insert(980),
            arrobj[-]=insert({"ostr": "a", "oint": 9})
            """,
        "update_method": update_3,
        "except_providers": [DocumentStoreProvider.GOOGLE_FIRESTORE],
    },
    {
        "set": """
            arrstr[1]=insert('ten nine'),
            arrint[2]=insert(919),
            obj.narr[3]=insert(925),
            arrobj[2]=insert({"ostr": "b", "oint": 99})
            """,
        "update_method": update_4,
        "except_providers": [
            DocumentStoreProvider.GOOGLE_FIRESTORE,
            DocumentStoreProvider.AMAZON_DYNAMODB,
        ],
    },
    {
        "set": """
            arrstr=move(newarrstr),
            arrobj=move(obj.arrobj),
            obj.nstr=move(nstr),
            obj.nobj.nnstr=move(obj.nobj.newnnstr)
            """,
        "update_method": update_5,
        "except_providers": [
            DocumentStoreProvider.GOOGLE_FIRESTORE,
            DocumentStoreProvider.REDIS,
        ],
    },
    {
        "set": """
            arrstr=array_union(["nineteen", "million nine", "billion nine"]),
            arrint=array_remove([99, 90009]),
            obj.narr=array_union([960, 970, 980])
            """,
        "update_method": update_6,
        "except_providers": [
            DocumentStoreProvider.AMAZON_DYNAMODB,
            DocumentStoreProvider.AZURE_COSMOS_DB,
            DocumentStoreProvider.REDIS,
        ],
    },
]

batches = [
    {
        "pre_delete": [0, 1, 2, 3, 4],
        "operations": [
            StoreOperation.put(value=documents[0]),
            StoreOperation.put(value=documents[1]),
            StoreOperation.put(value=documents[2]),
            StoreOperation.put(value=documents[3]),
            StoreOperation.put(value=documents[4]),
        ],
        "statement": f"""BATCH
                    PUT VALUE {json.dumps(documents[0])};
                    PUT VALUE {json.dumps(documents[1])};
                    PUT VALUE {json.dumps(documents[2])};
                    PUT VALUE {json.dumps(documents[3])};
                    PUT VALUE {json.dumps(documents[4])};
                    END
                    """,
        "check_exists": [0, 1, 2, 3, 4],
        "expected_result": [
            documents[0],
            documents[1],
            documents[2],
            documents[3],
            documents[4],
        ],
    },
    {
        "pre_put": [0, 1, 2, 3, 4],
        "operations": [
            StoreOperation.delete(get_key(documents[0])),
            StoreOperation.delete(get_key(documents[1])),
            StoreOperation.delete(get_key(documents[2])),
            StoreOperation.delete(get_key(documents[3])),
            StoreOperation.delete(get_key(documents[4])),
        ],
        "statement": f"""BATCH
                    DELETE KEY {json.dumps(get_key(documents[0]))};
                    DELETE KEY {json.dumps(get_key(documents[1]))};
                    DELETE KEY {json.dumps(get_key(documents[2]))};
                    DELETE KEY {json.dumps(get_key(documents[3]))};
                    DELETE KEY {json.dumps(get_key(documents[4]))};
                    END
                    """,
        "check_not_exists": [0, 1, 2, 3, 4],
        "expected_result": [None, None, None, None, None],
    },
    {
        "pre_put": [0, 1, 2],
        "pre_delete": [3, 4],
        "operations": [
            StoreOperation.delete(get_key(documents[0])),
            StoreOperation.delete(get_key(documents[1])),
            StoreOperation.delete(get_key(documents[2])),
            StoreOperation.put(value=documents[3]),
            StoreOperation.put(value=documents[4]),
        ],
        "statement": """BATCH
                    DELETE KEY @p1;
                    DELETE KEY @p2;
                    DELETE KEY @p3;
                    PUT VALUE @p4;
                    PUT VALUE @p5;
                    END
                    """,
        "params": {
            "p1": get_key(documents[0]),
            "p2": get_key(documents[1]),
            "p3": get_key(documents[2]),
            "p4": documents[3],
            "p5": documents[4],
        },
        "check_exists": [3, 4],
        "check_not_exists": [0, 1, 2],
        "expected_result": [None, None, None, documents[3], documents[4]],
    },
    {
        "pre_delete": [0, 1, 8, 9],
        "operations": [
            StoreOperation.put(value=documents[0]),
            StoreOperation.put(value=documents[1]),
            StoreOperation.put(value=documents[8]),
            StoreOperation.put(value=documents[9]),
        ],
        "check_exists": [0, 1, 8, 9],
        "expected_result": [
            documents[0],
            documents[1],
            documents[8],
            documents[9],
        ],
    },
    {
        "pre_put": [3, 4, 5, 6],
        "operations": [
            StoreOperation.delete(get_key(documents[3])),
            StoreOperation.delete(get_key(documents[4])),
            StoreOperation.delete(get_key(documents[5])),
            StoreOperation.delete(get_key(documents[6])),
        ],
        "check_not_exists": [3, 4, 5, 6],
        "expected_result": [None, None, None, None],
    },
]

transactions = [
    {
        "pre_delete": [7, 8, 9],
        "operations": [
            StoreOperation.put(value=documents[7]),
            StoreOperation.put(value=documents[8]),
            StoreOperation.put(value=documents[9]),
        ],
        "statement": f"""TRANSACT
                    PUT VALUE {json.dumps(documents[7])};
                    PUT VALUE {json.dumps(documents[8])};
                    PUT VALUE {json.dumps(documents[9])};
                    END""",
        "check_exists": [7, 8, 9],
        "expected_result": [
            documents[7],
            documents[8],
            documents[9],
        ],
    },
    {
        "pre_delete": [6],
        "pre_put": [7, 8, 9],
        "operations": [
            StoreOperation.put(value=documents[6], where="not_exists()"),
            StoreOperation.put(value=documents[7], where="exists()"),
            StoreOperation.delete(
                get_key(documents[8]), where=complex_condition_2
            ),
            StoreOperation.update(
                get_key(documents[9]),
                updates[0]["set"],
                where=complex_condition_1,
                returning="new",
            ),
        ],
        "statement": f"""TRANSACT
                    PUT VALUE {json.dumps(documents[6])} WHERE NOT_EXISTS();
                    PUT VALUE {json.dumps(documents[7])} WHERE EXISTS();
                    DELETE KEY {json.dumps(get_key(documents[8]))}
                        WHERE {complex_condition_2};
                    UPDATE KEY {json.dumps(get_key(documents[9]))}
                        SET {updates[0]["set"]}
                        WHERE {complex_condition_1}
                        RETURNING "new";
                    END""",
        "check_not_exists": [8],
        "check_exists": [6, 7, 9],
        "expected_result": [
            documents[6],
            documents[7],
            None,
            update_0(copy.deepcopy(documents[9])),
        ],
    },
    {
        "pre_delete": [6],
        "pre_put": [7, 8, 9],
        "operations": [
            StoreOperation.put(value=documents[6], where="not_exists()"),
            StoreOperation.put(value=documents[7], where="exists()"),
            StoreOperation.delete(
                get_key(documents[8]), where=complex_condition_2
            ),
            StoreOperation.update(
                get_key(documents[9]),
                updates[0]["set"],
                where=bad_complex_condition_1,
                returning="new",
            ),
        ],
        "check_not_exists": [6],
        "check_exists": [7, 8, 9],
        "expected_error": True,
    },
    {
        "pre_delete": [0, 1, 8, 9],
        "operations": [
            StoreOperation.put(value=documents[0]),
            StoreOperation.put(value=documents[1]),
            StoreOperation.put(value=documents[8]),
            StoreOperation.put(value=documents[9]),
        ],
        "check_exists": [0, 1, 8, 9],
        "expected_result": [
            documents[0],
            documents[1],
            documents[8],
            documents[9],
        ],
        "except_providers": [DocumentStoreProvider.AZURE_COSMOS_DB],
    },
    {
        "pre_create": "test2",
        "pre_delete": [3, 4],
        "pre_delete_2": [5, 6],
        "operations": [
            StoreOperation.put(value=documents[3], collection="test"),
            StoreOperation.put(value=documents[4], collection="test"),
            StoreOperation.put(value=documents[5], collection="test2"),
            StoreOperation.put(value=documents[6], collection="test2"),
        ],
        "statement": f"""TRANSACT
                    PUT VALUE {json.dumps(documents[3])} INTO test;
                    PUT VALUE {json.dumps(documents[4])} INTO test;
                    PUT VALUE {json.dumps(documents[5])} INTO test2;
                    PUT VALUE {json.dumps(documents[6])} INTO test2;
                    END""",
        "check_exists": [3, 4],
        "check_exists_2": [5, 6],
        "expected_result": [
            documents[3],
            documents[4],
            documents[5],
            documents[6],
        ],
        "except_providers": [DocumentStoreProvider.AZURE_COSMOS_DB],
    },
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_collection(provider_type: str, async_call: bool):
    index_needed_providers = [
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.REDIS,
    ]

    new_collection = f"ntest{str(async_call).lower()}"
    client = DocumentStoreSyncAndAsyncClient(
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
        # Dynamo DB takes forever to create indexes.
        # DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_index(provider_type: str, async_call: bool):
    def has_index(indexes: list, index: dict) -> bool:
        for i in indexes:
            if isinstance(i, DataModel):
                i = i.to_dict()
            if i["type"] == index["type"]:
                if i["type"] == "composite":
                    if len(i["fields"]) != len(index["fields"]):
                        continue
                    match_failed = False
                    for a, b in zip(
                        sorted(i["fields"], key=lambda x: x["field"]),
                        sorted(index["fields"], key=lambda x: x["field"]),
                    ):
                        h = has_index([a], b)
                        if not h:
                            match_failed = True
                    if not match_failed:
                        return True
                elif i["type"] == "wildcard":
                    if i["field"] == index["field"]:
                        return sorted(i["excluded"]) == sorted(
                            index["excluded"]
                        )
                else:
                    if i["field"] == index["field"]:
                        return True
        return False

    indexes = [
        {
            "type": "wildcard",
            "field": "*",
            "excluded": ["nested", "k.l.r"],
            "except_providers": [
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.REDIS,
            ],
        },
        {
            "type": "composite",
            "fields": [
                {"type": "asc", "field": "f1", "field_type": "string"},
                {"type": "asc", "field": "f2", "field_type": "number"},
            ],
            "except_providers": [],
        },
        {
            "type": "composite",
            "fields": [
                {"type": "asc", "field": "x.y", "field_type": "string"},
                {"type": "asc", "field": "a.b.c", "field_type": "number"},
                {"type": "asc", "field": "obj", "field_type": "number"},
                {"type": "desc", "field": "int", "field_type": "number"},
                {"type": "desc", "field": "str", "field_type": "string"},
                # {"type": "array", "field": "p.q.r", "field_type": "number"},
            ],
            "except_providers": [
                DocumentStoreProvider.AMAZON_DYNAMODB,
            ],
        },
        {
            "type": "field",
            "field": "a.b.c",
            "field_type": "number",
            "except_providers": [
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
            ],
        },
        {
            "type": "field",
            "field": "str",
            "field_type": "string",
            "except_providers": [
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
            ],
        },
        {
            "type": "field",
            "field": "int",
            "field_type": "number",
            "except_providers": [
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
            ],
        },
        {
            "type": "array",
            "field": "obj.arr",
            "except_providers": [
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.AZURE_COSMOS_DB,
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.POSTGRESQL,
            ],
        },
        {
            "type": "hash",
            "field": "name",
            "except_providers": [
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AZURE_COSMOS_DB,
            ],
        },
        {
            "type": "vector",
            "field": "x.vector",
            "partitions": 10,
            "dimension": 1920,
            "except_providers": [
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.MONGODB,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.AZURE_COSMOS_DB,
            ],
        },
        {
            "type": "vector",
            "field": "vec",
            "structure": "hnsw",
            "metric": "cosine",
            "m": 4,
            "ef_construction": 12,
            "except_providers": [
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.MONGODB,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.AZURE_COSMOS_DB,
            ],
        },
        {
            "type": "text",
            "field": "y.text",
            "except_providers": [
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.AZURE_COSMOS_DB,
            ],
        },
        {
            "type": "geospatial",
            "field": "x.location",
            "except_providers": [
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.GOOGLE_FIRESTORE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
            ],
        },
        {
            "type": "ttl",
            "field": "_ts",
            "except_providers": [
                DocumentStoreProvider.POSTGRESQL,
                DocumentStoreProvider.SQLITE,
                DocumentStoreProvider.AMAZON_DYNAMODB,
                DocumentStoreProvider.REDIS,
            ],
        },
    ]
    default_wildcard_providers = [
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
    ]
    except_wilcard_drop_providers = [
        DocumentStoreProvider.AZURE_COSMOS_DB,
    ]
    default_wilcard_index = {
        "type": "wildcard",
        "field": "*",
        "excluded": [],
    }

    duplicate_index = indexes[1]

    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    new_collection = f"i1test{str(async_call).lower()}"
    response = await client.has_collection(collection=new_collection)
    result = response.result
    if result:
        await client.drop_collection(collection=new_collection)

    await client.create_collection(collection=new_collection)

    findexes = [
        index
        for index in indexes
        if not (
            "except_providers" in index
            and provider_type in index["except_providers"]
        )
    ]
    response = await client.list_indexes(collection=new_collection)
    for index in findexes:
        if provider_type in default_wildcard_providers:
            if index["type"] == "wildcard":
                assert has_index(response.result, default_wilcard_index)
        else:
            assert not has_index(response.result, index)
    for index in findexes:
        response = await client.create_index(
            index=index, collection=new_collection
        )
        result = response.result
        assert result.status == IndexStatus.CREATED
    response = await client.list_indexes(collection=new_collection)
    for index in findexes:
        assert has_index(response.result, index)

    # Create a duplicate index. Best effort.
    response = await client.create_index(
        index=duplicate_index, collection=new_collection
    )
    result = response.result
    assert (
        result.status == IndexStatus.EXISTS
        or result.status == IndexStatus.COVERED
    )

    # Create a duplicate index. Raise conflict.
    with pytest.raises(ConflictError):
        await client.create_index(
            index=duplicate_index,
            collection=new_collection,
            where="not_exists()",
        )

    for index in findexes:
        if (
            index["type"] == "wildcard"
            and provider_type in except_wilcard_drop_providers
        ):
            continue
        response = await client.drop_index(
            index=index, collection=new_collection
        )
        result = response.result
        assert result.status == IndexStatus.DROPPED
        response = await client.list_indexes(collection=new_collection)
        if provider_type in default_wildcard_providers:
            if index["type"] == "wildcard":
                assert has_index(response.result, default_wilcard_index)
        else:
            assert not has_index(response.result, index)

    # Drop a non-existent index. Best effort.
    response = await client.drop_index(
        index=duplicate_index, collection=new_collection
    )
    result = response.result
    assert result.status == IndexStatus.NOT_EXISTS

    # Create a non-existent index. Raise not found.
    with pytest.raises(NotFoundError):
        await client.drop_index(
            index=duplicate_index,
            collection=new_collection,
            where="exists()",
        )

    await client.drop_collection(collection=new_collection)

    new_collection = f"i2test{str(async_call).lower()}"
    response = await client.has_collection(collection=new_collection)
    result = response.result
    if result:
        await client.drop_collection(collection=new_collection)
    response = await client.create_collection(
        collection=new_collection, config={"indexes": findexes}
    )
    result = response.result
    assert len(findexes) == len(result.indexes)
    for index_result in result.indexes:
        assert index_result.status == IndexStatus.CREATED
    response = await client.list_indexes(collection=new_collection)
    for index in findexes:
        assert has_index(response.result, index)
    await client.drop_collection(collection=new_collection)
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_put_get_delete(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
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
    bad_etag_condition = f"$etag='{old_etag}xxx'"

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
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_update(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
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

    bad_etag_condition = "$etag='xxx'"
    # unconditional update (etag) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.update(key=key, set=set, where=bad_etag_condition)

    bad_etag_condition = "$etag='xxx'"
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
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_query_count(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    for document in documents:
        response = await client.put(value=document)
        result = response.result
        assert_put_result(result, document)

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
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_batch(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    for batch in batches:
        if "pre_put" in batch:
            for i in batch["pre_put"]:
                await client.put(value=documents[i])
            for i in batch["pre_put"]:
                response = await client.get(key=get_key(documents[i]))
                result = response.result
                assert_get_result(result, documents[i])
        if "pre_delete" in batch:
            for i in batch["pre_delete"]:
                try:
                    await client.delete(key=get_key(documents[i]))
                except NotFoundError:
                    pass
            for i in batch["pre_delete"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))
        operations = batch["operations"]
        response = await client.batch(batch={"operations": operations})
        result = response.result
        assert_batch_result(result, batch["expected_result"])
        if "check_exists" in batch:
            for i in batch["check_exists"]:
                response = await client.get(key=get_key(documents[i]))
                result = response.result
                assert_get_result(result, documents[i])
        if "check_not_exists" in batch:
            for i in batch["check_not_exists"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_transact(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    for transaction in transactions:
        if "except_providers" in transaction:
            if provider_type in transaction["except_providers"]:
                continue
        if "pre_create" in transaction:
            await client.create_collection(
                collection=transaction["pre_create"]
            )
        if "pre_put" in transaction:
            for i in transaction["pre_put"]:
                await client.put(value=documents[i])
            for i in transaction["pre_put"]:
                response = await client.get(key=get_key(documents[i]))
                result = response.result
                assert_get_result(result, documents[i])
        if "pre_delete" in transaction:
            for i in transaction["pre_delete"]:
                try:
                    await client.delete(key=get_key(documents[i]))
                except NotFoundError:
                    pass
            for i in transaction["pre_delete"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))
        if "pre_put_2" in transaction:
            for i in transaction["pre_put_2"]:
                await client.put(
                    value=documents[i], collection=transaction["pre_create"]
                )
            for i in transaction["pre_put_2"]:
                response = await client.get(
                    key=get_key(documents[i]),
                    collection=transaction["pre_create"],
                )
                result = response.result
                assert_get_result(result, documents[i])
        if "pre_delete_2" in transaction:
            for i in transaction["pre_delete_2"]:
                try:
                    await client.delete(
                        key=get_key(documents[i]),
                        collection=transaction["pre_create"],
                    )
                except NotFoundError:
                    pass
            for i in transaction["pre_delete_2"]:
                with pytest.raises(NotFoundError):
                    await client.get(
                        key=get_key(documents[i]),
                        collection=transaction["pre_create"],
                    )
        operations = transaction["operations"]
        if "expected_error" in transaction:
            with pytest.raises(ConflictError):
                await client.transact(transaction={"operations": operations})
        else:
            response = await client.transact(
                transaction={"operations": operations}
            )
            result = response.result
            assert_batch_result(result, transaction["expected_result"])
        if "check_exists" in transaction:
            for i in transaction["check_exists"]:
                response = await client.get(key=get_key(documents[i]))
        if "check_not_exists" in transaction:
            for i in transaction["check_not_exists"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))
        if "check_exists_2" in transaction:
            for i in transaction["check_exists_2"]:
                response = await client.get(
                    key=get_key(documents[i]),
                    collection=transaction["pre_create"],
                )
        if "check_not_exists_2" in transaction:
            for i in transaction["check_not_exists_2"]:
                with pytest.raises(NotFoundError):
                    await client.get(
                        key=get_key(documents[i]),
                        collection=transaction["pre_create"],
                    )
        if "pre_create" in transaction:
            await client.drop_collection(collection=transaction["pre_create"])
    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.SQLITE,
        DocumentStoreProvider.MEMORY,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_execute(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    await create_collection_if_needed(provider_type, client)
    for document in documents:
        await cleanup_document(document, client)

    document = documents[-1]
    replace_document = documents[-2].copy()
    replace_document["id"] = document["id"]
    replace_document["pk"] = document["pk"]
    key = get_key(document)

    # unconditional put when item doesn't exist
    response = await client.__execute__(
        statement="PUT VALUE @p1", params={"p1": document}
    )
    result = response.result
    assert_put_result(result, document)

    # get when item exists
    response = await client.__execute__(
        statement="GET KEY @p1", params={"p1": key}
    )
    result = response.result
    assert_get_result(result, document)

    # unconditional put when item exists
    response = await client.__execute__(
        statement=f"PUT VALUE {json.dumps(document)}"
    )
    result = response.result
    assert_put_result(result, document)
    response = await client.__execute__(statement=f"GET KEY {json.dumps(key)}")
    result = response.result
    assert_get_result(result, document)
    assert Comparator.contains(document, result.value)

    # unconditional delete when item exists
    response = await client.__execute__(
        statement="DELETE KEY @p1", params={"p1": key}
    )
    result = response.result
    assert_delete_result(result)

    response = await client.__execute__(
        statement=f"PUT VALUE {json.dumps(document)} WHERE not_exists()"
    )
    result = response.result
    assert_put_result(result, document)

    # conditional put (good condition) when item exists
    put_response = await client.__execute__(
        statement=f"""PUT VALUE {json.dumps(replace_document)}
            WHERE {complex_condition_1}"""
    )
    put_result = put_response.result
    assert_put_result(put_result, replace_document)
    response = await client.__execute__(statement=f"GET KEY {json.dumps(key)}")
    result = response.result
    assert_get_result(result, replace_document)

    # conditional delete (bad condition) when item exists
    with pytest.raises(PreconditionFailedError):
        await client.__execute__(
            statement=f"""DELETE KEY {json.dumps(key)}
                WHERE {complex_condition_1}"""
        )

    # conditional delete (good condition) when item exists
    response = await client.__execute__(
        statement="DELETE KEY @p1 WHERE @p2",
        params={"p1": key, "p2": complex_condition_2},
    )
    result = response.result
    assert_delete_result(result)

    # test params passed to operation
    response = await client.__execute__(
        statement=f"PUT VALUE {json.dumps(document)}"
    )
    result = response.result
    assert_put_result(result, document)
    params = {"q1": replace_document}
    params = params | complex_condition_1_params
    put_response = await client.__execute__(
        statement=f"PUT VALUE @q1 WHERE {complex_condition_1_with_params}",
        params=params,
    )
    put_result = put_response.result
    assert_put_result(put_result, replace_document)
    response = await client.__execute__(statement=f"GET KEY {json.dumps(key)}")
    result = response.result
    assert_get_result(result, replace_document)

    # conditional delete (good condition) when item exists
    response = await client.__execute__(
        statement="DELETE KEY @p1", params={"p1": key}
    )
    result = response.result
    assert_delete_result(result)

    document = documents[-1]
    document_copy = copy.deepcopy(document)
    key = get_key(document)
    await cleanup_document(document, client)

    set = updates[0]["set"]
    # unconditional update when item doesn't exist
    with pytest.raises(NotFoundError):
        await client.__execute__(
            statement=f"UPDATE KEY {json.dumps(key)} SET {set}"
        )

    # unconditional update (condition) when item doesn't exist
    with pytest.raises(PreconditionFailedError):
        await client.__execute__(
            statement=f"""UPDATE KEY {json.dumps(key)}
                             SET {set} WHERE {complex_condition_1}"""
        )

    response = await client.__execute__(
        statement=f"PUT VALUE {json.dumps(document)}"
    )
    result = response.result
    assert_put_result(result, document)

    set = updates[2]["set"]
    params = updates[2]["params"]
    params = params | complex_condition_1_params
    params = params | {
        "r1": key,
        "r2": set,
        "r3": complex_condition_1_with_params,
        "r4": "new",
    }
    document_copy = update_1(document_copy)
    response = await client.__execute__(
        statement="UPDATE KEY @r1 SET @r2 WHERE @r3 RETURNING @r4",
        params=params,
    )
    result = response.result
    assert_update_result(result, document_copy, "new")

    response = await client.__execute__(
        statement=f"DELETE KEY {json.dumps(key)}"
    )
    result = response.result
    assert_delete_result(result)

    for document in documents:
        response = await client.__execute__(
            statement="PUT VALUE @p1", params={"p1": document}
        )
        result = response.result
        assert_put_result(result, document)

    response = await client.__execute__(statement="QUERY")
    result = response.result
    assert_select_result(result.items, documents, False)

    response = await client.__execute__(statement="COUNT")
    result = response.result
    assert_count_result(result, len(documents))

    for query in queries:
        if "except_providers" in query:
            if provider_type in query["except_providers"]:
                continue
        if "statement" not in query:
            continue
        statement = query["statement"]
        params = None
        if "params" in query:
            params = query["params"]
        filtered_documents = filter_documents(documents, query["result_index"])

        projected = None
        if "select" in query["args"]:
            projected = query["args"]["select"]

        response = await client.__execute__(
            statement=f"QUERY {statement}", params=params
        )
        result = response.result
        ordered = True if "ordered" not in query else query["ordered"]

        assert_select_result(
            result.items, filtered_documents, ordered, projected
        )

        count = query["count"]
        response = await client.__execute__(
            statement=f"COUNT {statement}", params=params
        )
        result = response.result
        assert_count_result(result, count)

    for batch in batches:
        if "statement" not in batch:
            continue
        if "pre_put" in batch:
            for i in batch["pre_put"]:
                await client.put(value=documents[i])
        if "pre_delete" in batch:
            for i in batch["pre_delete"]:
                try:
                    await client.delete(key=get_key(documents[i]))
                except NotFoundError:
                    pass
        statement = batch["statement"]
        params = None
        if "params" in batch:
            params = batch["params"]
        response = await client.__execute__(statement=statement, params=params)
        result = response.result
        assert_batch_result(result, batch["expected_result"])
        if "check_exists" in batch:
            for i in batch["check_exists"]:
                response = await client.get(key=get_key(documents[i]))
                result = response.result
                assert_get_result(result, documents[i])
        if "check_not_exists" in batch:
            for i in batch["check_not_exists"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))

    for transaction in transactions:
        if "except_providers" in transaction:
            if provider_type in transaction["except_providers"]:
                continue
        if "statement" not in transaction:
            continue
        if "pre_create" in transaction:
            await client.create_collection(
                collection=transaction["pre_create"]
            )
        if "pre_put" in transaction:
            for i in transaction["pre_put"]:
                await client.put(value=documents[i])
        if "pre_delete" in transaction:
            for i in transaction["pre_delete"]:
                try:
                    await client.delete(key=get_key(documents[i]))
                except NotFoundError:
                    pass
        if "pre_put_2" in transaction:
            for i in transaction["pre_put_2"]:
                await client.put(
                    value=documents[i], collection=transaction["pre_create"]
                )
        if "pre_delete_2" in transaction:
            for i in transaction["pre_delete_2"]:
                try:
                    await client.delete(
                        key=get_key(documents[i]),
                        collection=transaction["pre_create"],
                    )
                except NotFoundError:
                    pass
        statement = transaction["statement"]
        params = None
        if "params" in transaction:
            params = transaction["params"]
        if "expected_error" in transaction:
            with pytest.raises(ConflictError):
                result = await client.__execute__(
                    statement=statement, params=params
                )
        else:
            response = await client.__execute__(
                statement=statement, params=params
            )
            result = response.result
            assert_batch_result(result, transaction["expected_result"])
        if "check_exists" in transaction:
            for i in transaction["check_exists"]:
                result = await client.get(key=get_key(documents[i]))
        if "check_not_exists" in transaction:
            for i in transaction["check_not_exists"]:
                with pytest.raises(NotFoundError):
                    await client.get(key=get_key(documents[i]))
        if "check_exists_2" in transaction:
            for i in transaction["check_exists_2"]:
                response = await client.get(
                    key=get_key(documents[i]),
                    collection=transaction["pre_create"],
                )
                result = response.result
        if "check_not_exists_2" in transaction:
            for i in transaction["check_not_exists_2"]:
                with pytest.raises(NotFoundError):
                    await client.get(
                        key=get_key(documents[i]),
                        collection=transaction["pre_create"],
                    )
        if "pre_create" in transaction:
            await client.drop_collection(collection=transaction["pre_create"])

    await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        DocumentStoreProvider.AMAZON_DYNAMODB,
        DocumentStoreProvider.AZURE_COSMOS_DB,
        DocumentStoreProvider.GOOGLE_FIRESTORE,
        DocumentStoreProvider.MONGODB,
        DocumentStoreProvider.POSTGRESQL,
        DocumentStoreProvider.REDIS,
        DocumentStoreProvider.MEMORY,
        DocumentStoreProvider.SQLITE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_bytes(provider_type: str, async_call: bool):
    client = DocumentStoreSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    if not client.__supports__(feature=DocumentStoreFeature.TYPE_BINARY):
        return

    await create_collection_if_needed(provider_type, client)
    key = get_key(bson)
    await cleanup_document(bson, client)

    await client.put(value=bson)
    response = await client.get(key=key)
    result = response.result
    assert_get_result(result, bson)

    await client.delete(key=key)
    await client.close()


def assert_batch_result(result: list, expected_result: list):
    assert len(result) == len(expected_result)
    for i in range(0, len(expected_result)):
        if expected_result[i] is None:
            assert result[i] is None
        else:
            assert_put_result(result[i], expected_result[i])


def assert_select_result(
    result: list[DocumentItem],
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


def assert_put_result(result: DocumentItem, document: dict):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    assert result.key.pk == document["pk"]
    assert result.properties.etag is not None


def assert_update_result(result: DocumentItem, document: dict, returning: str):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    assert result.key.pk == document["pk"]
    assert result.properties.etag is not None
    if returning == "new":
        assert Comparator.contains(document, result.value)


def assert_get_result(
    result: DocumentItem, document: dict, etag: bool | None = True
):
    assert Comparator.equals(get_key(document), result.key.to_dict())
    assert result.key.id == document["id"]
    assert result.key.pk == document["pk"]
    if etag:
        assert result.properties.etag is not None
    assert Comparator.contains(document, result.value)


def assert_delete_result(result: dict | None):
    assert result is None


async def cleanup_document(
    document: dict, client: DocumentStoreSyncAndAsyncClient
):
    try:
        await client.delete(key=get_key(document))
    except NotFoundError:
        return


def serialize(val) -> str:
    return json.dumps(val)


def filter_documents(documents: list, index: list) -> list:
    result = []
    for i in range(0, len(index)):
        result.append(documents[index[i]])
    return result


async def create_collection_if_needed(
    provider_type: str,
    client: DocumentStoreSyncAndAsyncClient,
):
    composite_indexes = [
        [("pk", "hash", "string"), ("id", "asc", "string")],
        [("pk", "hash", "string"), ("id", "desc", "string")],
        [("pk", "hash", "string"), ("str", "asc", "string")],
        [("pk", "hash", "string"), ("opt", "asc", "string")],
        [("pk", "hash", "string"), ("obj.nint", "asc", "number")],
        [("pk", "hash", "string"), ("int", "asc", "number")],
        [("pk", "hash", "string"), ("int", "desc", "number")],
        [
            ("pk", "hash", "string"),
            ("int", "asc", "number"),
            ("str", "asc", "string"),
        ],
        [
            ("pk", "hash", "string"),
            ("opt", "asc", "string"),
            ("int", "asc", "number"),
        ],
        [
            ("pk", "hash", "string"),
            ("bool", "asc", "boolean"),
            ("str", "asc", "string"),
        ],
        [
            ("pk", "hash", "string"),
            ("str", "asc", "string"),
            ("int", "asc", "number"),
            ("bool", "asc", "boolean"),
        ],
        [
            ("arrstr", "array", "string"),
            ("pk", "hash", "string"),
            ("str", "asc", "string"),
        ],
        [
            ("obj.narr", "array", "number"),
            ("pk", "hash", "string"),
            ("int", "desc", "number"),
        ],
        [
            ("arrobj", "array", "object"),
            ("pk", "hash", "string"),
            ("str", "asc", "string"),
        ],
    ]
    indexes = []
    for composite_index in composite_indexes:
        index = {"type": "composite", "fields": []}
        for field_index in composite_index:
            index["fields"].append(
                {
                    "field": field_index[0],
                    "type": field_index[1],
                    "field_type": field_index[2],
                }
            )
        indexes.append(index)

    await client.create_collection(config={"indexes": indexes})
