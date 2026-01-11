from x8.storage.config_store import ConfigStore
from x8.storage.document_store import DocumentStore
from x8.storage.object_store import ObjectStore
from x8.storage.secret_store import SecretStore


def playConfigStore():
    print("\n----- ----- ----- ----- ----- ----- \nConfig Store\n----- -----")
    c = ConfigStore(__provider__="local")
    c.put(key="x", value="y")
    print(c.get(key="x"))
    print(c.query())
    print(c.count())


def playSecretStore():
    print("\n----- ----- ----- ----- ----- ----- \nSecret Store\n----- -----")
    s = SecretStore(__provider__="local")
    s.put(key="x", value="y")
    print(s.get(key="x"))
    print(s.query())
    print(s.count())


def playDocumentStore():
    print(
        "\n----- ----- ----- ----- ----- ----- \nDocument Store\n----- -----"
    )
    d = DocumentStore(__provider__="local")
    d.create_collection("test")
    d.put(key="x", value={"a": "b"}, collection="test")
    print(d.get(key="x", collection="test"))
    print(d.query(collection="test"))
    print(d.count(collection="test"))


def playObjectStore():
    print("\n----- ----- ----- ----- ----- ----- \nObject Store\n----- -----")
    o = ObjectStore(__provider__="local")
    o.create_collection("test")
    print(o.list_collections())
    o.drop_collection("test")
    print(o.list_collections())


playConfigStore()
playSecretStore()
playDocumentStore()
playObjectStore()

print("\n----- ----- ----- ----- ----- ----- \nALL DONE\n----- -----")
print("SUCCESS")
