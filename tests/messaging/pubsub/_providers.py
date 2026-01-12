from typing import Any

from common.secrets import get_secrets

from x8.messaging.pubsub import PubSub

secrets = get_secrets()


class PubSubProvider:
    AMAZON_SNS = "amazon_sns"
    AZURE_SERVICE_BUS = "azure_service_bus"
    GOOGLE_PUBSUB = "google_pubsub"
    REDIS = "redis"
    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


provider_parameters: dict[str, dict[str, Any]] = {
    PubSubProvider.AMAZON_SNS: {
        "region_name": secrets["messaging-amazon-sqs-region-name"],
        "aws_access_key_id": secrets["messaging-amazon-sqs-aws-access-key-id"],
        "aws_secret_access_key": secrets[
            "messaging-amazon-sqs-aws-secret-access-key"
        ],
        "topic": "topictest",
        "subscription": "sub1",
    },
    PubSubProvider.AZURE_SERVICE_BUS: {
        "connection_string": secrets[
            "messaging-azure-service-bus-connection-string"
        ],
        "topic": "topictest",
        "subscription": "sub1",
    },
    PubSubProvider.GOOGLE_PUBSUB: {
        "project": secrets["messaging-google-pubsub-project"],
        "topic": "topictest",
        "subscription": "sub1",
    },
    PubSubProvider.REDIS: {
        "host": secrets["storage-document-redis-host"],
        "port": secrets["storage-document-redis-port"],
        "password": secrets["storage-document-redis-password"],
        "topic": "topictest",
        "subscription": "sub1",
    },
    PubSubProvider.POSTGRESQL: {
        "connection_string": secrets[
            "storage-document-postgresql-connection-string"
        ],
        "topic": "topictest",
        "subscription": "sub1",
    },
    PubSubProvider.SQLITE: {
        "database": ":memory:",
        "topic": "topictest",
        "subscription": "sub1",
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = PubSub(
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        )
    )
    return component
