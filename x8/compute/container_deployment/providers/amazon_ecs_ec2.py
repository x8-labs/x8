"""
AWS ECS EC2 deployment.
"""

from __future__ import annotations

__all__ = ["AmazonECSEC2"]

from typing import Any, Literal

from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer

from .._models import ServiceDefinition, ServiceOverlay
from ._amazon_ecs import AmazonECS


class AmazonECSEC2(AmazonECS):
    def __init__(
        self,
        region: str = "us-west-2",
        cluster_name: str | None = None,
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        network_mode: Literal["awsvpc", "bridge", "host"] = "awsvpc",
        ec2_ami_id: str | None = None,
        ec2_instance_type: str | None = None,
        ec2_min_size: int | None = None,
        ec2_max_size: int | None = None,
        ec2_desired_capacity: int | None = None,
        ec2_target_capacity: int | None = None,
        ec2_launch_template_id: str | None = None,
        ec2_launch_template_version: str | None = None,
        ec2_auto_scaling_group_arn: str | None = None,
        ecs_instance_profile_arn: str | None = None,
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
        """Initialize AWS ECS EC2 deployment provider.

        Args:
            region: AWS region for the ECS service.
            cluster_name: The name of the ECS cluster.
            service_name: The name of the ECS service to deploy.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            network_mode:
                Network mode for the ECS service,
                either "awsvpc", "bridge", or "host".
            ec2_ami_id: AMI ID for EC2 instances (if launch_kind is "EC2").
            ec2_instance_type: EC2 instance type (if launch_kind is "EC2").
            ec2_min_size:
                Minimum size of the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_max_size:
                Maximum size of the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_desired_capacity:
                Desired capacity of the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_target_capacity:
                Target capacity for the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_launch_template_id:
                Launch template ID for the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_launch_template_version:
                Launch template version for the EC2 Auto Scaling group
                (if launch_kind is "EC2").
            ec2_auto_scaling_group_arn:
                ARN of the EC2 Auto Scaling group (if launch_kind is "EC2").
            ecs_instance_profile_arn:
                ECS instance role ARN (if launch_kind is "EC2").
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
            launch_kind="EC2",
            network_mode=network_mode,
            ec2_ami_id=ec2_ami_id,
            ec2_instance_type=ec2_instance_type,
            ec2_min_size=ec2_min_size,
            ec2_max_size=ec2_max_size,
            ec2_desired_capacity=ec2_desired_capacity,
            ec2_target_capacity=ec2_target_capacity,
            ec2_launch_template_id=ec2_launch_template_id,
            ec2_launch_template_version=ec2_launch_template_version,
            ec2_auto_scaling_group_arn=ec2_auto_scaling_group_arn,
            ecs_instance_profile_arn=ecs_instance_profile_arn,
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
