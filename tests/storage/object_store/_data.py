objects = [
    {
        "id": "test1.txt",
        "value": b"Hello World One",
    },
    {
        "id": "test2.txt",
        "value": b"Hello World Two",
        "properties": {
            "content_type": "text/plain",
            "content_disposition": "inline",
            "content_encoding": "none",
            "content_language": "en-US",
            "cache_control": "no-cache",
        },
    },
    {
        "id": "test3.txt",
        "value": b"Hello World Three",
        "metadata": {
            "str": "value3",
            "int": "30",
            "bool": "false",
            "arr": "tag1, tag2",
        },
    },
    {
        "id": "/c/d/test4.txt",
        "value": b"Hello World Four",
        "properties": {
            "content_type": "text/plain",
            "content_disposition": "inline",
            "content_encoding": "none",
            "content_language": "en-US",
            "cache_control": "no-cache",
        },
        "metadata": {
            "str": "value4",
            "int": "40",
            "bool": "true",
            "arr": "tag1",
        },
    },
    {
        "id": "test5.jpg",
        "file": "test-image.jpg",
        "properties": {
            "content_type": "image/jpeg",
            "storage_class": "hot",
        },
        "metadata": {
            "str": "value5",
            "int": "50",
            "bool": "true",
            "arr": "tag2",
        },
    },
    {
        "id": "test6.json",
        "file": "test-json.json",
        "properties": {
            "content_type": "application/json",
            "storage_class": "cool",
        },
        "metadata": {
            "str": "value6",
            "int": "60",
            "bool": "false",
            "arr": "tag1",
        },
    },
    {
        "id": "test7.txt",
        "stream": "test-text.txt",
        "properties": {
            "content_type": "text/plain",
        },
        "metadata": {
            "str": "value7",
            "int": "70",
            "bool": "true",
            "arr": "tag2",
        },
    },
    {
        "id": "test8.mp4",
        "stream": "test-video.mp4",
        "properties": {
            "content_type": "video/mp4",
        },
        "metadata": {
            "str": "value8",
            "int": "80",
            "bool": "false",
            "arr": "tag1, tag2",
        },
    },
]

query_objects = [
    {
        "id": "test00.txt",
        "value": b"value0",
    },
    {
        "id": "test01.txt",
        "value": b"value1",
    },
    {
        "id": "data/test02.txt",
        "value": b"value2",
    },
    {
        "id": "data/test03.txt",
        "value": b"value3",
    },
    {
        "id": "data/ab/test04.txt",
        "value": b"value4",
    },
    {
        "id": "data/ab/test05.txt",
        "value": b"value5",
    },
    {
        "id": "data/cd/test06.txt",
        "value": b"value6",
    },
    {
        "id": "data/xy/test07.txt",
        "value": b"value7",
    },
    {
        "id": "data/xy/test08.txt",
        "value": b"value8",
    },
    {
        "id": "abc/test09.txt",
        "value": b"value9",
    },
    {
        "id": "abc/test10.txt",
        "value": b"value10",
    },
    {
        "id": "tzyx/test13.txt",
        "value": b"value11",
    },
    {
        "id": "tzyx/test14.txt",
        "value": b"value12",
    },
    {
        "id": "aaa.txt",
        "value": b"value13",
    },
]
