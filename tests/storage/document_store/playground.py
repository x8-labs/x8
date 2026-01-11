from x8.storage.document_store import DocumentStore


def run():
    c = DocumentStore()
    c.create_collection(collection="test")
    c.put(key="x", value={"a": "b"}, collection="test")


if __name__ == "__main__":
    run()
