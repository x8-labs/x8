from typing import Any

from common.secrets import get_secrets

from x8.messaging.queue import Queue

secrets = get_secrets()


class QueueProvider:
    AMAZON_SQS = "amazon_sqs"
    AZURE_SERVICE_BUS = "azure_service_bus"
    AZURE_QUEUE_STORAGE = "azure_queue_storage"
    GOOGLE_PUBSUB = "google_pubsub"
    REDIS = "redis"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


provider_parameters: dict[str, dict[str, Any]] = {
    QueueProvider.AMAZON_SQS: {
        "region_name": secrets["messaging-amazon-sqs-region-name"],
        "aws_access_key_id": secrets["messaging-amazon-sqs-aws-access-key-id"],
        "aws_secret_access_key": secrets[
            "messaging-amazon-sqs-aws-secret-access-key"
        ],
        "queue": "test",
    },
    QueueProvider.AZURE_SERVICE_BUS: {
        "connection_string": secrets[
            "messaging-azure-service-bus-connection-string"
        ],
        "queue": "test",
    },
    QueueProvider.AZURE_QUEUE_STORAGE: {
        "connection_string": secrets[
            "storage-object-azure-blob-storage-connection-string"
        ],
        "queue": "test",
    },
    QueueProvider.GOOGLE_PUBSUB: {
        "project": secrets["messaging-google-pubsub-project"],
        "queue": "test",
    },
    QueueProvider.REDIS: {
        "host": secrets["storage-document-redis-host"],
        "port": secrets["storage-document-redis-port"],
        "password": secrets["storage-document-redis-password"],
        "queue": "test",
    },
    QueueProvider.POSTGRESQL: {
        "connection_string": secrets[
            "storage-document-postgresql-connection-string"
        ],
        "queue": "test",
    },
    QueueProvider.SQLITE: {
        "database": ":memory:",
        "queue": "test",
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = Queue(
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        )
    )
    return component
