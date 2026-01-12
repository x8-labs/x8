import os
import tempfile
from typing import Any

from common.secrets import get_secrets

from x8.storage.object_store import ObjectStore

secrets = get_secrets()


class ObjectStoreProvider:
    AMAZON_S3 = "amazon_s3"
    AZURE_BLOB_STORAGE = "azure_blob_storage"
    GOOGLE_CLOUD_STORAGE = "google_cloud_storage"
    FILE_SYSTEM = "file_system"
    MEMORY = "memory"


provider_parameters: dict[str, dict[str, Any]] = {
    ObjectStoreProvider.AMAZON_S3: {
        "region_name": secrets["storage-object-amazon-s3-region-name"],
        "aws_access_key_id": secrets[
            "storage-object-amazon-s3-aws-access-key-id"
        ],
        "aws_secret_access_key": secrets[
            "storage-object-amazon-s3-aws-secret-access-key"
        ],
        "bucket": "x8-test-versioned",
    },
    ObjectStoreProvider.AZURE_BLOB_STORAGE: {
        "connection_string": secrets[
            "storage-object-azure-blob-storage-connection-string"
        ],
        "container": "x8-test-versioned",
    },
    ObjectStoreProvider.GOOGLE_CLOUD_STORAGE: {
        "project": secrets["storage-object-google-cloud-storage-project"],
        "service_account_info": secrets[
            "storage-object-google-cloud-storage-service-account-info"
        ],
        "bucket": "x8-test-versioned",
    },
    ObjectStoreProvider.FILE_SYSTEM: {
        "store_path": os.path.join(tempfile.gettempdir(), "object-store"),
        "folder": "x8-test-versioned",
    },
}


def get_component(provider_type: str, collection: str | None = None):
    parameters = provider_parameters[provider_type]
    component = ObjectStore(
        collection=collection,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
