from typing import Any

from common.secrets import get_secrets

from x8.compute.container_registry import ContainerRegistry

secrets = get_secrets()


class ContainerRegistryProvider:
    AMAZON_ELASTIC_CONTAINER_REGISTRY = "amazon_elastic_container_registry"
    AZURE_CONTAINER_REGISTRY = "azure_container_registry"
    GOOGLE_ARTIFACT_REGISTRY = "google_artifact_registry"
    DOCKER_LOCAL = "docker_local"


provider_parameters: dict[str, dict[str, Any]] = {
    ContainerRegistryProvider.AMAZON_ELASTIC_CONTAINER_REGISTRY: {
        "region": "us-west-2",
        "aws_access_key_id": secrets["aws-access-key-id"],
        "aws_secret_access_key": secrets["aws-secret-access-key"],
    },
    ContainerRegistryProvider.AZURE_CONTAINER_REGISTRY: {
        "name": secrets["azure-container-registry-name"],
    },
    ContainerRegistryProvider.GOOGLE_ARTIFACT_REGISTRY: {
        "project": secrets["google-cloud-project"],
        "location": "us-west1",
        "name": "test-registry",
    },
    ContainerRegistryProvider.DOCKER_LOCAL: {},
}


def get_component(provider_type: str):
    component = ContainerRegistry(
        __provider__=dict(
            type=provider_type,
            parameters=provider_parameters[provider_type],
        ),
    )
    return component
