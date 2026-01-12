from ._providers import SearchStoreProvider

complex_condition_1 = """
    length(arrstr[0]) > 7
    and not contains(const, '$')
    and 8 = array_length(obj.narr)
    and array_contains(arrint, 909)
    and array_contains_any(arrstr, ['xyz', 'hundred nine', 'abc'])
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, 'number')
    and is_type(empty, 'null')
    and not is_type(bool, 'array')
    and obj.nstr = "9"
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
    and not contains(const, @p3)
    and @p5 = array_length(obj.narr)
    and array_contains(arrint, @p6)
    and array_contains_any(arrstr, @p7)
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, @p8)
    and is_type(empty, @p10)
    and not is_type(bool, @p11)
    and obj.nstr = @p12
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
    and not contains(const, '$')
    and 7 = array_length(obj.narr)
    and array_contains(arrint, 808)
    and array_contains_any(arrstr, ['xyz', 'hundred eight', 'abc'])
    and is_defined(str)
    and is_not_defined(opt)
    and is_type(float, 'number')
    and is_type(empty, 'null')
    and not is_type(bool, 'array')
    and obj.nstr = "8"
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
            "where": "pk = 'pk00'",
            "order_by": "$id",
        },
        "statement": "SELECT * WHERE pk = 'pk00' ORDER BY $id",
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "bool, obj, pk, $id",
            "where": "pk = 'pk00'",
            "order_by": "id",
        },
        "statement": """SELECT bool, obj, pk, $id
                    WHERE pk = 'pk00' ORDER BY $id""",
        "result_index": [0, 1, 2, 3, 4],
        "count": 5,
    },
    {
        "args": {
            "select": "obj.nstr, obj.nint, pk, $id",
            "where": "pk = 'pk00'",
            "order_by": "$id",
        },
        "statement": "SELECT @p1 WHERE @p2 ORDER BY @p3",
        "params": {
            "p1": "obj.nstr, obj.nint, pk, $id",
            "p2": "pk = @p4",
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
        "except_providers": [],
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
        "except_providers": [SearchStoreProvider.ELASTICSEARCH],
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
        "except_providers": [],
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
        "except_providers": [SearchStoreProvider.ELASTICSEARCH],
    },
    {
        "args": {"where": complex_condition_1},
        "statement": f"WHERE {complex_condition_1}",
        "result_index": [9],
        "count": 1,
        "except_providers": [],
    },
    {
        "args": {"where": complex_condition_2},
        "statement": f"WHERE {complex_condition_2}",
        "result_index": [8],
        "count": 1,
        "except_providers": [],
    },
]

text_search_queries = [
    {
        "args": {
            "search": 'text_search("yellow")',
            "where": "pk = 'pk00'",
            "order_by": "id",
        },
        "statement": """SEARCH text_search("yellow")
                        WHERE pk = 'pk00' ORDER BY id""",
        "result_index": [0, 2],
        "count": 2,
    },
    {
        "args": {
            "search": 'text_search("yellow")',
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search("yellow")
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2, 0],
        "count": 2,
    },
    {
        "args": {
            "search": 'text_search("yellow")',
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search("yellow")
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2, 0],
        "count": 2,
    },
    {
        "args": {
            "search": 'text_search("yellow code")',
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search("yellow code")
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2, 0],
        "count": 2,
    },
    {
        "args": {
            "search": 'text_search(query="yellow code", match_mode="and")',
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow code", match_mode="and")
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2],
        "count": 1,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood", match_mode="and")',
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood", match_mode="and")
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [],
        "count": 0,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood", match_mode="and", query_type="cross_fields")',  # noqa
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood", match_mode="and", query_type="cross_fields") # noqa
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2],
        "count": 1,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood", fields=["text1"], match_mode="and", query_type="cross_fields")',  # noqa
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood", fields=["text1"], match_mode="and", query_type="cross_fields") # noqa
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [],
        "count": 0,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood", fields=["text1", "text2"], match_mode="and", query_type="cross_fields")',  # noqa
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood", fields=["text1", "text2"], match_mode="and", query_type="cross_fields") # noqa
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [2],
        "count": 1,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood green blue", fields=["text1", "text2"], match_mode="and", query_type="cross_fields", minimum_should_match="75%")',  # noqa
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood green blue", fields=["text1", "text2"], match_mode="and", query_type="cross_fields", minimum_should_match="75%") # noqa
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [],
        "count": 0,
    },
    {
        "args": {
            "search": 'text_search(query="yellow wood green blue", fields=["text1", "text2"], match_mode="and", query_type="cross_fields", minimum_should_match="50%")',  # noqa
            "where": "pk = 'pk00'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="yellow wood green blue", fields=["text1", "text2"], match_mode="and", query_type="cross_fields", minimum_should_match="50%") # noqa
                        WHERE pk = 'pk00' ORDER BY $score DESC""",
        "result_index": [],
        "count": 0,
    },
    {
        "args": {
            "search": 'text_search(query="cats arms")',  # noqa
            "where": "pk = 'pk01'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="cats arms") # noqa
                        WHERE pk = 'pk01' ORDER BY $score DESC""",
        "result_index": [8, 9],
        "count": 2,
    },
    {
        "args": {
            "search": 'text_search(query="cats arms", boost={"text1": 2, "text2": 1})',  # noqa
            "where": "pk = 'pk01'",
            "order_by": "$score DESC",
        },
        "statement": """SEARCH text_search(
                        query="cats arms", boost={"text1": 2, "text2": 1}) # noqa
                        WHERE pk = 'pk01' ORDER BY $score DESC""",
        "result_index": [9, 8],
        "count": 2,
    },
]

vector_search_queries = [
    {
        "args": {
            "search": "vector_search([1, 1, 0, 0], 'vector', 3)",
        },
        "statement": """SEARCH vector_search([1, 1, 0, 0], 'vector', 3)""",
        "result_index": [1, 0, 2],
        "count": 3,
    },
    {
        "args": {
            "search": "vector_search([4, 4, 4, 4], 'vector', 4)",
        },
        "statement": """SEARCH vector_search([4, 4, 4, 4], 'vector', 4)""",
        "result_index": [4, 5, 3, 6],
        "count": 4,
    },
    {
        "args": {
            "search": "vector_search([4, 4, 4, 4], 'vector', 4)",
            "where": "pk = 'pk00'",
        },
        "statement": """SEARCH vector_search([4, 4, 4, 4], 'vector', 4)""",
        "result_index": [4, 3, 2, 1],
        "count": 4,
    },
]

sparse_vector_search_queries = [
    {
        "args": {
            "search": 'sparse_vector_search({"0": 0.31, "3": 0.76}, "sparse_vector")',  # noqa
            "limit": 3,
        },
        "statement": """SEARCH sparse_vector_search({"0": 0.31, "3": 0.76}, "sparse_vector")""",  # noqa
        "result_index": [3, 6, 4],
        "count": 3,
    },
    {
        "args": {
            "search": 'sparse_vector_search({"0": 0.31, "3": 0.76}, "sparse_vector")',  # noqa
            "where": "pk = 'pk00'",
            "limit": 3,
        },
        "statement": """SEARCH sparse_vector_search({"0": 0.31, "3": 0.76}, "sparse_vector")""",  # noqa
        "result_index": [3, 4, 1],
        "count": 3,
    },
]

hybrid_vector_search_queries = [
    {
        "args": {
            "search": """hybrid_vector_search([1, 1, 0, 0], 'vector', 3, 10, {"0": 0.31, "3": 0.76}, 'sparse_vector')""",  # noqa
            "limit": 3,
        },
        "statement": """SEARCH hybrid_vector_search([1, 1, 0, 0], 'vector', 3, 10, {"0": 0.31, "3": 0.76}, 'sparse_vector')""",  # noqa
        "result_index": [3, 1, 6],
        "count": 3,
    },
    {
        "args": {
            "search": """hybrid_vector_search([1, 1, 0, 0], 'vector', 3, 10, {"0": 0.31, "3": 0.76}, 'sparse_vector')""",  # noqa
            "where": "pk = 'pk00'",
            "limit": 3,
        },
        "statement": """SEARCH hybrid_vector_search([1, 1, 0, 0], 'vector', 3, 10, {"0": 0.31, "3": 0.76}, 'sparse_vector')""",  # noqa
        "result_index": [3, 1, 4],
        "count": 3,
    },
]

hybrid_text_search_queries = [
    {
        "args": {
            "search": """hybrid_text_search(query="yellow wood", vector=[1, 1, 0, 0], vector_field='vector', k=3, num_candidates=10)""",  # noqa,
            "where": "pk = 'pk00'",
            "limit": 3,
        },
        "statement": """SEARCH hybrid_text_search(query="yellow wood", vector=[1, 1, 0, 0], vector_field='vector', k=3, num_candidates=10)""",  # noqa
        "result_index": [2, 0, 1],
        "count": 3,
    }
]
