"""
AWS ECS Fargate deployment.
"""

from __future__ import annotations

__all__ = ["AmazonECSFargate"]

from typing import Any

from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer

from .._models import ServiceDefinition, ServiceOverlay
from ._amazon_ecs import AmazonECS


class AmazonECSFargate(AmazonECS):
    def __init__(
        self,
        region: str = "us-west-2",
        cluster_name: str | None = None,
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        capacity_providers: list[str] | None = None,
        default_capacity_provider_strategy: list[dict[str, Any]] | None = None,
        vpc_id: str | None = None,
        subnet_ids: list[str] | None = None,
        execution_role_arn: str | None = None,
        task_role_arn: str | None = None,
        ecs_security_group_id: str | None = None,
        alb_security_group_id: str | None = None,
        alb_arn: str | None = None,
        target_group_arn: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
        nparams: dict[str, Any] = {},
        **kwargs: Any,
    ):
        """Initialize AWS ECS Fargate deployment provider.

        Args:
            region: AWS region for the ECS service.
            cluster_name: The name of the ECS cluster.
            service_name: The name of the ECS service to deploy.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            capacity_providers: List of capacity providers for the ECS cluster.
            default_capacity_provider_strategy:
                Default capacity provider strategy.
            vpc_id: VPC ID to use for the ECS service.
            subnet_ids: List of subnet IDs to use for the ECS service.
            execution_role_arn: Execution role ARN for the ECS tasks.
            task_role_arn: Task role ARN for the ECS tasks.
            ecs_security_group_id: Security group ID for the ECS tasks.
            alb_security_group_id: Security group ID for the ALB.
            alb_arn: ARN of the Application Load Balancer.
            target_group_arn: ARN of the target group for the ALB.
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            aws_session_token: AWS session token.
            profile_name: AWS profile name to use.
            nparams: Native params to AWS clients.
        """

        super().__init__(
            region=region,
            cluster_name=cluster_name,
            service_name=service_name,
            service=service,
            service_override=service_override,
            containerizer=containerizer,
            container_registry=container_registry,
            launch_kind="FARGATE",
            network_mode="awsvpc",
            capacity_providers=capacity_providers,
            default_capacity_provider_strategy=default_capacity_provider_strategy,  # noqa
            vpc_id=vpc_id,
            subnet_ids=subnet_ids,
            execution_role_arn=execution_role_arn,
            task_role_arn=task_role_arn,
            ecs_security_group_id=ecs_security_group_id,
            alb_security_group_id=alb_security_group_id,
            alb_arn=alb_arn,
            target_group_arn=target_group_arn,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            profile_name=profile_name,
            nparams=nparams,
            **kwargs,
        )
