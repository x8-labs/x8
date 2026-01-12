from typing import Any

from common.secrets import get_secrets

from x8.compute.container_deployment import ContainerDeployment

secrets = get_secrets()


class ContainerDeploymentProvider:
    AWS_APP_RUNNER = "aws_app_runner"
    AMAZON_ECS_EC2 = "amazon_ecs_ec2"
    AMAZON_ECS_FARGATE = "amazon_ecs_fargate"
    AZURE_CONTAINER_INSTANCES = "azure_container_instances"
    AZURE_CONTAINER_APPS = "azure_container_apps"
    GOOGLE_CLOUD_RUN = "google_cloud_run"
    LOCAL = "local"


provider_parameters: dict[str, dict[str, Any]] = {
    ContainerDeploymentProvider.AWS_APP_RUNNER: {},
    ContainerDeploymentProvider.AMAZON_ECS_FARGATE: {},
    ContainerDeploymentProvider.AMAZON_ECS_EC2: {
        "ec2_desired_capacity": 3,
    },
    ContainerDeploymentProvider.AZURE_CONTAINER_INSTANCES: {},
    ContainerDeploymentProvider.AZURE_CONTAINER_APPS: {},
    ContainerDeploymentProvider.GOOGLE_CLOUD_RUN: {},
    ContainerDeploymentProvider.LOCAL: {},
}


def get_component(provider_type: str):
    component = ContainerDeployment(
        __provider__=dict(
            type=provider_type,
            parameters=provider_parameters[provider_type],
        ),
    )
    return component
