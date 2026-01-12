messages = [
    dict(
        value="Test message 1",
        metadata={"str": "hello", "int": "10", "bool": "False"},
        properties=dict(
            message_id="id1",
            group_id="group1",
            content_type="text/plain",
        ),
    ),
    dict(
        value=b"Hello world",
        metadata={"str": "hello", "int": "10", "bool": "False"},
        properties=dict(
            message_id="id2",
            group_id="group2",
            content_type="application/octet-stream",
        ),
    ),
    dict(
        value={"str": "world", "int": 20, "bool": True},
        metadata={"str": "hello", "int": "10", "bool": "False"},
        properties=dict(
            message_id="id3",
            group_id="group2",
            content_type="application/json",
        ),
    ),
]
