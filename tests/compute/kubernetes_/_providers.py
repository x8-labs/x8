from typing import Any

from common.secrets import get_secrets

from x8.compute.kubernetes import Kubernetes

secrets = get_secrets()


class KubernetesProvider:
    AZURE_KUBERNETES_SERVICE = "azure_kubernetes_service"
    MINIKUBE = "minikube"


provider_parameters: dict[str, dict[str, Any]] = {
    KubernetesProvider.AZURE_KUBERNETES_SERVICE: {},
    KubernetesProvider.MINIKUBE: {},
}


def get_component(provider_type: str):
    component = Kubernetes(
        __provider__=dict(
            type=provider_type,
            parameters=provider_parameters[provider_type],
        ),
    )
    return component
