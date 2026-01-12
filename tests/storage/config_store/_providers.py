from typing import Any

from common.secrets import get_secrets
from storage.document_store import get_component as get_ds_component

from x8.storage.config_store import ConfigStore

secrets = get_secrets()


class ConfigStoreProvider:
    AWS_PARAMETER_STORE = "aws_parameter_store"
    AZURE_APP_CONFIGURATION = "azure_app_configuration"
    GOOGLE_RUNTIME_CONFIGURATOR = "google_runtime_configurator"
    DS_AMAZON_DYNAMODB = "ds_amazon_dynamodb"
    DS_AZURE_COSMOS_DB = "ds_azure_cosmos_db"
    DS_GOOGLE_FIRESTORE = "ds_google_firestore"
    DS_MONGODB = "ds_mongodb"
    DS_POSTGRESQL = "ds_postgresql"
    DS_REDIS = "ds_redis"
    DS_SQLITE = "ds_sqlite"
    DS_MEMORY = "ds_memory"


provider_parameters: dict[str, dict[str, Any]] = {
    ConfigStoreProvider.AWS_PARAMETER_STORE: {
        "region_name": secrets[
            "storage-config-aws-parameter-store-region-name"
        ],
        "aws_access_key_id": secrets[
            "storage-config-aws-parameter-store-aws-access-key-id"
        ],
        "aws_secret_access_key": secrets[
            "storage-config-aws-parameter-store-aws-secret-access-key"
        ],
    },
    ConfigStoreProvider.AZURE_APP_CONFIGURATION: {
        "connection_string": secrets[
            "storage-config-azure-app-configuration-connection-string"
        ],
    },
    ConfigStoreProvider.GOOGLE_RUNTIME_CONFIGURATOR: {
        "project": secrets[
            "storage-config-google-runtime-configurator-project"
        ],
    },
}


def get_component(provider_type: str):
    base_provider_type = provider_type
    if provider_type.startswith("ds"):
        splits = provider_type.split("_")
        base_provider_type = "document_store_provider"
        parameters = {
            "store": get_ds_component("_".join(splits[1:]), "config")
        }
    else:
        parameters = provider_parameters[provider_type]

    component = ConfigStore(
        __provider__=dict(
            type=base_provider_type,
            parameters=parameters,
        )
    )
    return component
