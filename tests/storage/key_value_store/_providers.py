from typing import Any

from common.secrets import get_secrets
from storage.document_store import get_component as get_ds_component
from storage.object_store import get_component as get_os_component

from x8.storage.key_value_store import KeyValueStore

secrets = get_secrets()


class KeyValueStoreProvider:
    REDIS = "redis"
    REDIS_SIMPLE = "redis_simple"
    MEMCACHED = "memcached"
    MEMORY = "memory"
    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    DS_AMAZON_DYNAMODB = "ds_amazon_dynamodb"
    DS_AZURE_COSMOS_DB = "ds_azure_cosmos_db"
    DS_GOOGLE_FIRESTORE = "ds_google_firestore"
    DS_MONGODB = "ds_mongodb"
    DS_POSTGRESQL = "ds_postgresql"
    DS_REDIS = "ds_redis"
    DS_SQLITE = "ds_sqlite"
    DS_MEMORY = "ds_memory"
    OS_AMAZON_S3 = "os_amazon_s3"
    OS_AZURE_BLOB_STORAGE = "os_azure_blob_storage"
    OS_GOOGLE_CLOUD_STORAGE = "os_google_cloud_storage"
    OS_FILE_SYSTEM = "os_file_system"


provider_parameters: dict[str, dict[str, Any]] = {
    KeyValueStoreProvider.REDIS: {
        "host": secrets["storage-document-redis-host"],
        "port": secrets["storage-document-redis-port"],
        "password": secrets["storage-document-redis-password"],
    },
    KeyValueStoreProvider.REDIS_SIMPLE: {
        "host": secrets["storage-document-redis-host"],
        "port": secrets["storage-document-redis-port"],
        "password": secrets["storage-document-redis-password"],
    },
    KeyValueStoreProvider.MEMCACHED: {
        "host": "127.0.0.1",
        "port": 11211,
    },
    KeyValueStoreProvider.MEMORY: {},
    KeyValueStoreProvider.SQLITE: {},
    KeyValueStoreProvider.POSTGRESQL: {
        "connection_string": secrets[
            "storage-document-postgresql-connection-string"
        ]
    },
}


def get_component(provider_type: str, type="binary", collection: str = "test"):
    base_provider_type = provider_type
    if provider_type.startswith("ds"):
        splits = provider_type.split("_")
        base_provider_type = "document_store_provider"
        parameters = {
            "store": get_ds_component("_".join(splits[1:]), "kvtest")
        }
    elif provider_type.startswith("os"):
        splits = provider_type.split("_")
        base_provider_type = "object_store_provider"
        parameters = {"store": get_os_component("_".join(splits[1:]))}
    else:
        parameters = provider_parameters[provider_type]

    component = KeyValueStore(
        type=type,
        collection=collection,
        __provider__=dict(
            type=base_provider_type,
            parameters=parameters,
        ),
    )
    return component
