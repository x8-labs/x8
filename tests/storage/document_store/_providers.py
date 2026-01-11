from typing import Any

from common.secrets import get_secrets
from x8.storage.document_store import DocumentStore

secrets = get_secrets()


class DocumentStoreProvider:
    AMAZON_DYNAMODB = "amazon_dynamodb"
    AZURE_COSMOS_DB = "azure_cosmos_db"
    GOOGLE_FIRESTORE = "google_firestore"
    MONGODB = "mongodb"
    POSTGRESQL = "postgresql"
    REDIS = "redis"
    SQLITE = "sqlite"
    MEMORY = "memory"


provider_parameters: dict[str, dict[str, Any]] = {
    DocumentStoreProvider.AMAZON_DYNAMODB: {
        "region_name": secrets["storage-document-amazon-dynamodb-region-name"],
        "aws_access_key_id": secrets[
            "storage-document-amazon-dynamodb-aws-access-key-id"
        ],
        "aws_secret_access_key": secrets[
            "storage-document-amazon-dynamodb-aws-secret-access-key"
        ],
    },
    DocumentStoreProvider.AZURE_COSMOS_DB: {
        "endpoint": secrets["storage-document-azure-cosmos-db-endpoint"],
        "access_key": secrets["storage-document-azure-cosmos-db-access-key"],
        "database": "test",
        "suppress_fields": ["_rid", "_ts", "_self", "_attachments"],
    },
    DocumentStoreProvider.GOOGLE_FIRESTORE: {
        "project": secrets["storage-document-google-firestore-project"],
        "database": "test",
    },
    DocumentStoreProvider.MONGODB: {
        "uri": secrets["storage-document-mongodb-uri"],
        "database": "test",
        "suppress_fields": ["_id"],
    },
    DocumentStoreProvider.POSTGRESQL: {
        "connection_string": secrets[
            "storage-document-postgresql-connection-string"
        ],
        "id_column": "id",
        "value_column": "value",
    },
    DocumentStoreProvider.REDIS: {
        "host": secrets["storage-document-redis-host"],
        "port": secrets["storage-document-redis-port"],
        "password": secrets["storage-document-redis-password"],
        "field_types": {
            "bool": "boolean",
            "obj": "object",
            "obj.nint": "number",
        },
    },
    DocumentStoreProvider.SQLITE: {},
    DocumentStoreProvider.MEMORY: {},
}


def get_component(provider_type: str, collection: str = "test"):
    component = DocumentStore(
        collection=collection,
        __provider__=dict(
            type=provider_type,
            parameters=provider_parameters[provider_type],
        ),
    )
    return component
