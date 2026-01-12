from typing import Any

from common.secrets import get_secrets
from storage.document_store import get_component as get_ds_component

from x8.storage.secret_store import SecretStore

secrets = get_secrets()


class SecretStoreProvider:
    AWS_SECRETS_MANAGER = "aws_secrets_manager"
    AZURE_KEY_VAULT = "azure_key_vault"
    GOOGLE_SECRET_MANAGER = "google_secret_manager"
    HASHICORP_VAULT = "hashicorp_vault"
    SQLITE = "sqlite"
    ENV = "env"
    DS_AMAZON_DYNAMODB = "ds_amazon_dynamodb"
    DS_AZURE_COSMOS_DB = "ds_azure_cosmos_db"
    DS_GOOGLE_FIRESTORE = "ds_google_firestore"
    DS_MONGODB = "ds_mongodb"
    DS_POSTGRESQL = "ds_postgresql"
    DS_REDIS = "ds_redis"
    DS_SQLITE = "ds_sqlite"
    DS_MEMORY = "ds_memory"


provider_parameters: dict[str, dict[str, Any]] = {
    SecretStoreProvider.AWS_SECRETS_MANAGER: {
        "region_name": secrets[
            "storage-secret-aws-secrets-manager-region-name"
        ],
        "aws_access_key_id": secrets[
            "storage-secret-aws-secrets-manager-aws-access-key-id"
        ],
        "aws_secret_access_key": secrets[
            "storage-secret-aws-secrets-manager-aws-secret-access-key"
        ],
    },
    SecretStoreProvider.AZURE_KEY_VAULT: {
        "vault_url": secrets["storage-secret-azure-key-vault-url"],
        "credential_type": "client_secret",
        "tenant_id": secrets["storage-secret-azure-key-vault-tenant-id"],
        "client_id": secrets["storage-secret-azure-key-vault-client-id"],
        "client_secret": secrets[
            "storage-secret-azure-key-vault-client-secret"
        ],
    },
    SecretStoreProvider.GOOGLE_SECRET_MANAGER: {
        "project": secrets["google-cloud-project"],
    },
    SecretStoreProvider.HASHICORP_VAULT: {
        "url": secrets["storage-secret-hashicorp-vault-url"],
        "token": secrets["storage-secret-hashicorp-vault-token"],
        "namespace": "admin",
    },
    SecretStoreProvider.SQLITE: {
        "database": ":memory:",
    },
    SecretStoreProvider.ENV: dict(),
}


def get_component(provider_type: str):
    base_provider_type = provider_type
    if provider_type.startswith("ds_"):
        splits = provider_type.split("_")
        base_provider_type = "document_store_provider"
        parameters: dict = {
            "store": get_ds_component("_".join(splits[1:]), "secret")
        }
    else:
        parameters = provider_parameters[provider_type]

    component = SecretStore(
        __provider__=dict(
            type=base_provider_type,
            parameters=parameters,
        )
    )
    return component
