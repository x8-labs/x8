"""
AWS ECS deployment.
"""

from __future__ import annotations

__all__ = ["AmazonECS"]

import base64
import json
import time
from datetime import datetime
from typing import Any, Literal

import boto3
from botocore.exceptions import ClientError

from x8.compute._common._amazon_ec2_helper import (
    auto_detect_network_config,
    delete_launch_template,
    ensure_launch_template,
)
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, OperationParser, Response, RunContext
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import Expression

from .._helper import map_images, requires_container_registry
from .._models import (
    Container,
    EnvVar,
    HTTPGetAction,
    Ingress,
    Port,
    Probe,
    ProbeSet,
    ResourceLimits,
    ResourceRequests,
    ResourceRequirements,
    Revision,
    Scale,
    ScaleRule,
    ServiceDefinition,
    ServiceItem,
    ServiceOverlay,
    TCPSocketAction,
    TrafficAllocation,
    Volume,
    VolumeMount,
)
from ._base import BaseContainerDeploymentProvider


class AmazonECS(BaseContainerDeploymentProvider):
    region: str
    cluster_name: str | None
    service_name: str | None

    launch_kind: Literal["FARGATE", "EC2"]
    network_mode: Literal["awsvpc", "bridge", "host"]
    ec2_ami_id: str | None
    ec2_instance_type: str | None
    ec2_min_size: int | None
    ec2_max_size: int | None
    ec2_desired_capacity: int | None
    ec2_target_capacity: int | None
    ec2_launch_template_id: str | None
    ec2_launch_template_version: str | None
    ec2_auto_scaling_group_arn: str | None
    ecs_instance_profile_arn: str | None

    capacity_providers: list[str] | None
    default_capacity_provider_strategy: list[dict[str, Any]] | None

    vpc_id: str | None
    subnet_ids: list[str] | None
    execution_role_arn: str | None
    task_role_arn: str | None
    ecs_security_group_id: str | None
    alb_security_group_id: str | None
    alb_arn: str | None
    target_group_arn: str | None

    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    profile_name: str | None
    nparams: dict[str, Any]

    _ecs_client: Any
    _iam_client: Any
    _elbv2_client: Any
    _ec2_client: Any
    _application_autoscaling_client: Any
    _autoscaling_client: Any
    _init: bool = False
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        region: str = "us-west-2",
        cluster_name: str | None = None,
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        launch_kind: Literal["FARGATE", "EC2"] = "FARGATE",
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
        """Initialize AWS ECS deployment provider.

        Args:
            region: AWS region for the ECS service.
            cluster_name: The name of the ECS cluster.
            service_name: The name of the ECS service to deploy.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            launch_kind:
                Launch kind for the ECS service, either "FARGATE" or "EC2".
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

        self.region = region
        self.cluster_name = cluster_name
        self.service_name = service_name
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry
        self.launch_kind = launch_kind
        self.network_mode = network_mode
        self.ec2_ami_id = ec2_ami_id
        self.ec2_instance_type = ec2_instance_type
        self.ec2_min_size = ec2_min_size
        self.ec2_max_size = ec2_max_size
        self.ec2_desired_capacity = ec2_desired_capacity
        self.ec2_target_capacity = ec2_target_capacity
        self.ec2_launch_template_id = ec2_launch_template_id
        self.ec2_launch_template_version = ec2_launch_template_version
        self.ec2_auto_scaling_group_arn = ec2_auto_scaling_group_arn
        self.ecs_instance_profile_arn = ecs_instance_profile_arn
        self.capacity_providers = capacity_providers
        self.default_capacity_provider_strategy = (
            default_capacity_provider_strategy
        )
        self.vpc_id = vpc_id
        self.subnet_ids = subnet_ids
        self.execution_role_arn = execution_role_arn
        self.task_role_arn = task_role_arn
        self.ecs_security_group_id = ecs_security_group_id
        self.alb_security_group_id = alb_security_group_id
        self.alb_arn = alb_arn
        self.target_group_arn = target_group_arn
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.nparams = nparams

        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()

        super().__init__(**kwargs)

    def __supports__(self, feature):
        # ECS supports all features including revision management
        return True

    def __setup__(self, context: Context | None = None) -> None:
        if self.launch_kind == "FARGATE" and self.network_mode != "awsvpc":
            raise BadRequestError(
                "Fargate only supports 'awsvpc' network_mode."
            )

        if self._init:
            return

        session_kwargs = {}
        if self.aws_access_key_id:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
        if self.aws_secret_access_key:
            session_kwargs["aws_secret_access_key"] = (
                self.aws_secret_access_key
            )
        if self.aws_session_token:
            session_kwargs["aws_session_token"] = self.aws_session_token
        if self.profile_name:
            session_kwargs["profile_name"] = self.profile_name

        session = boto3.Session(**session_kwargs)

        # Initialize AWS clients
        self._ecs_client = session.client(
            "ecs", region_name=self.region, **self.nparams
        )
        self._elbv2_client = session.client(
            "elbv2", region_name=self.region, **self.nparams
        )
        self._ec2_client = session.client(
            "ec2", region_name=self.region, **self.nparams
        )
        self._iam_client = session.client(
            "iam", region_name=self.region, **self.nparams
        )
        self._application_autoscaling_client = session.client(
            "application-autoscaling", region_name=self.region, **self.nparams
        )
        self._autoscaling_client = session.client(
            "autoscaling", region_name=self.region, **self.nparams
        )

        self._init = True

    def _ensure_container_registry(
        self, service: ServiceDefinition
    ) -> ContainerRegistry | None:
        container_registry = self._get_container_registry()
        if container_registry:
            return container_registry
        if not requires_container_registry(service):
            return None

        from x8.compute.container_registry import ContainerRegistry
        from x8.compute.container_registry.providers.amazon_elastic_container_registry import (  # noqa
            AmazonElasticContainerRegistry,
        )

        return ContainerRegistry(
            __provider__=AmazonElasticContainerRegistry(
                region=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                profile_name=self.profile_name,
            ),
        )

    def create_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        self.__setup__()
        where_exists = OperationParser.parse_where_exists(where)
        service_def = self._normalize_service_definition(service)
        service_name = self._get_service_name(service_def)
        cluster_name = self._get_cluster_name(service_name)
        execution_role_arn = self.execution_role_arn
        ecs_security_group_id = self.ecs_security_group_id
        alb_security_group_id = self.alb_security_group_id
        target_group_arn = self.target_group_arn
        alb_arn = None
        capacity_providers = self.capacity_providers
        default_capacity_provider_strategy = (
            self.default_capacity_provider_strategy
        )

        existing_service = None
        try:
            response = self._ecs_client.describe_services(
                cluster=cluster_name, services=[service_name]
            )
            if response["services"]:
                existing_service = response["services"][0]
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundException":
                pass
            else:
                raise e

        if existing_service and existing_service["status"] == "ACTIVE":
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {service_name} already exists."
                )
            vpc_id, subnet_ids = auto_detect_network_config(self._ec2_client)
            if not vpc_id or not subnet_ids:
                raise BadRequestError(
                    "VPC ID is required for AWS ECS Fargate deployment."
                )
            if not execution_role_arn:
                execution_role_arn = self._create_ecs_execution_role()

            ingress = self._op_converter.convert_ingress(service_def)
            if ingress["external"]:
                target_group_arn = None
                lbs = existing_service.get("loadBalancers") or []
                if lbs:
                    target_group_arn = lbs[0].get("targetGroupArn")
                if target_group_arn:
                    self._apply_readiness_probe_to_target_group(
                        target_group_arn=target_group_arn,
                        readiness_probe=self._get_readiness_probe(service_def),
                        network_mode=self.network_mode,
                    )
            if not ecs_security_group_id:
                ecs_security_group_id = self._ensure_ecs_security_group(
                    cluster_name=cluster_name,
                    vpc_id=vpc_id,
                    network_mode=self.network_mode,
                )
            # (NEW) Re-authorize ALB -> ECS SG if ports/SGs changed
            caller_sg_id = self.alb_security_group_id
            if ingress["external"]:
                if not alb_security_group_id:
                    alb_security_group_id = self._ensure_alb_security_group(
                        cluster_name=cluster_name,
                        vpc_id=vpc_id,
                        port=ingress["exposed_port"],
                    )
                    caller_sg_id = alb_security_group_id

            self._ensure_authorize_security_group_ingress(
                security_group_id=ecs_security_group_id,
                port=ingress["target_port"],
                caller_sg_id=caller_sg_id,
            )
            if self.launch_kind == "EC2":
                self._ensure_ec2_capacity(
                    cluster_name=cluster_name,
                    subnet_ids=subnet_ids,
                    security_group_id=ecs_security_group_id,
                )
            images = map_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service_def
                ),
                run_context=run_context,
            )
            task_def = self._op_converter.convert_task_definition(
                service_name=service_name,
                service=service_def,
                images=images,
                launch_kind=self.launch_kind,
                network_mode=self.network_mode,
                execution_role_arn=execution_role_arn,
                task_role_arn=self.task_role_arn,
            )
            register_response = self._ecs_client.register_task_definition(
                **task_def
            )
            task_def_arn = register_response["taskDefinition"][
                "taskDefinitionArn"
            ]
            update_service_args = self._op_converter.convert_update_service(
                cluster_name=cluster_name,
                service_name=service_name,
                task_definition=task_def_arn,
                service=service_def,
                launch_kind=self.launch_kind,
            )
            self._ecs_client.update_service(**update_service_args)
            self._ensure_application_autoscaling(
                cluster_name=cluster_name,
                service_name=service_name,
                scale=service_def.scale,
            )
            self._wait_for_service_stable(
                cluster_name=cluster_name,
                service_name=service_name,
                alb_arn=None,
            )
        else:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {service_name} not found."
                )
            if self.launch_kind == "FARGATE":
                if not capacity_providers:
                    capacity_providers = ["FARGATE"]
                if not default_capacity_provider_strategy:
                    default_capacity_provider_strategy = [
                        {"capacityProvider": "FARGATE", "weight": 1}
                    ]
            self._ensure_cluster(
                cluster_name=cluster_name,
                capacity_providers=capacity_providers,
                default_capacity_provider_strategy=default_capacity_provider_strategy,  # noqa: E501
            )
            if not execution_role_arn:
                execution_role_arn = self._create_ecs_execution_role()

            vpc_id, subnet_ids = auto_detect_network_config(self._ec2_client)
            if not vpc_id or not subnet_ids:
                raise BadRequestError(
                    "VPC ID is required for AWS ECS Fargate deployment."
                )

            ingress = self._op_converter.convert_ingress(service_def)
            caller_sg_id = self.alb_security_group_id
            if ingress["external"]:
                if not alb_security_group_id:
                    alb_security_group_id = self._ensure_alb_security_group(
                        cluster_name=cluster_name,
                        vpc_id=vpc_id,
                        port=ingress["exposed_port"],
                    )
                    caller_sg_id = alb_security_group_id
                if not target_group_arn:
                    alb_arn, target_group_arn = self._ensure_load_balancer(
                        service_name=service_name,
                        vpc_id=vpc_id,
                        subnet_ids=subnet_ids,
                        security_group_id=alb_security_group_id,
                        target_port=ingress["target_port"],
                        exposed_port=ingress["exposed_port"],
                        network_mode=self.network_mode,
                    )
                else:
                    alb_arn = self._get_alb_from_target_group(target_group_arn)
                self._apply_readiness_probe_to_target_group(
                    target_group_arn,
                    readiness_probe=self._get_readiness_probe(service_def),
                    network_mode=self.network_mode,
                )

            if not ecs_security_group_id:
                ecs_security_group_id = self._ensure_ecs_security_group(
                    cluster_name=cluster_name,
                    vpc_id=vpc_id,
                    network_mode=self.network_mode,
                )
            self._ensure_authorize_security_group_ingress(
                security_group_id=ecs_security_group_id,
                port=ingress["target_port"],
                caller_sg_id=caller_sg_id,
            )
            if self.launch_kind == "EC2":
                self._ensure_ec2_capacity(
                    cluster_name=cluster_name,
                    subnet_ids=subnet_ids,
                    security_group_id=ecs_security_group_id,
                )
            images = map_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service_def
                ),
                run_context=run_context,
            )
            task_def = self._op_converter.convert_task_definition(
                service_name=service_name,
                service=service_def,
                images=images,
                launch_kind=self.launch_kind,
                network_mode=self.network_mode,
                execution_role_arn=execution_role_arn,
                task_role_arn=self.task_role_arn,
            )
            register_response = self._ecs_client.register_task_definition(
                **task_def
            )
            task_def_arn = register_response["taskDefinition"][
                "taskDefinitionArn"
            ]
            create_service_args = self._op_converter.convert_create_service(
                cluster_name=cluster_name,
                service_name=service_name,
                task_definition=task_def_arn,
                service=service_def,
                launch_kind=self.launch_kind,
                network_mode=self.network_mode,
                ingress=ingress,
                subnet_ids=subnet_ids,
                security_group_id=ecs_security_group_id,
                target_group_arn=target_group_arn,
                capacity_provider_strategy=default_capacity_provider_strategy,
            )
            self._ecs_client.create_service(**create_service_args)
            self._ensure_application_autoscaling(
                cluster_name=cluster_name,
                service_name=service_name,
                scale=service_def.scale,
            )
            self._wait_for_service_stable(
                cluster_name=cluster_name,
                service_name=service_name,
                alb_arn=alb_arn,
            )

        return self.get_service(service_name)

    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        return self.create_service(service, where, run_context, **kwargs)

    def get_service(self, name: str, **kwargs: Any) -> Response[ServiceItem]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)
        return self._get_service(cluster_name=cluster_name, service_name=name)

    def delete_service(self, name: str, **kwargs: Any) -> Response[None]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)
        try:
            response = self._ecs_client.describe_services(
                cluster=cluster_name, services=[name]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundException":
                raise NotFoundError(f"Cluster {cluster_name} not found.")
            raise e
        if not response["services"]:
            raise NotFoundError(f"Service {name} not found.")
        try:
            self._ecs_client.update_service(
                cluster=cluster_name, service=name, desiredCount=0
            )
            print(f"Waiting for service {name} to be drained")
            self.wait_for_drained(cluster_name=cluster_name, service_name=name)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in [
                "ServiceNotActiveException",
                "ClusterNotFoundException",
            ]:
                pass
            else:
                raise e
        self._ecs_client.delete_service(
            cluster=cluster_name, service=name, force=True
        )
        print(f"Deleted service {name}.")

        if self.launch_kind == "EC2":
            self._delete_ec2_capacity(cluster_name=cluster_name)

        self._deregister_tasks(service_name=name)

        if not self.execution_role_arn:
            self._delete_ecs_execution_role()

        self._delete_load_balancer(service_name=name)

        self._delete_ecs_security_group(
            cluster_name=cluster_name, network_mode=self.network_mode
        )
        self._delete_alb_security_group(cluster_name=cluster_name)
        self._delete_autoscaling(cluster_name=cluster_name, service_name=name)
        self._delete_cluster(cluster_name=cluster_name)
        return Response(result=None)

    def list_services(
        self,
        **kwargs: Any,
    ) -> Response[list[ServiceItem]]:
        self.__setup__()

        clusters: list[str] = []
        if self.cluster_name:
            clusters.append(self.cluster_name)
        else:
            cl_paginator = self._ecs_client.get_paginator("list_clusters")
            for page in cl_paginator.paginate():
                clusters.extend(page.get("clusterArns", []))

        if not clusters:
            return Response(result=[])

        result: list[ServiceItem] = []

        # 2) For each cluster, list and describe services
        ls_paginator = self._ecs_client.get_paginator("list_services")
        for cluster in clusters:
            service_arns: list[str] = []
            for page in ls_paginator.paginate(cluster=cluster):
                service_arns.extend(page.get("serviceArns", []))

            if not service_arns:
                continue

            for service_arn in service_arns:
                response = self._get_service(
                    cluster_name=cluster,
                    service_name=service_arn,
                )
                result.append(response.result)

        return Response(result=result)

    def list_revisions(
        self,
        name: str,
        limit: int | None = None,  # optional cap
        **kwargs: Any,
    ) -> Response[list[Revision]]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)

        svc = self._ecs_client.describe_services(
            cluster=cluster_name, services=[name]
        )
        if not svc["services"]:
            raise NotFoundError(f"Service {name} not found.")
        service = svc["services"][0]
        if service["status"] != "ACTIVE":
            raise NotFoundError(f"Service {name} is not active.")

        current_td_arn = service["taskDefinition"]
        family = current_td_arn.split("/")[-1].rsplit(":", 1)[0]

        paginator = self._ecs_client.get_paginator("list_task_definitions")
        revisions: list[Revision] = []
        collected = 0

        for page in paginator.paginate(
            familyPrefix=family,
            status="ACTIVE",
            sort="DESC",
            PaginationConfig={"PageSize": 100},
        ):
            for td_arn in page.get("taskDefinitionArns", []):
                td = self._ecs_client.describe_task_definition(
                    taskDefinition=td_arn
                )
                td_id = td_arn.split("/")[-1]
                current = td_arn == current_td_arn
                revisions.append(
                    self._result_converter.convert_revision(
                        td_id, td["taskDefinition"], current
                    )
                )
                collected += 1
                if limit and collected >= limit:
                    return Response(result=revisions)

        return Response(result=revisions)

    def get_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)
        service_response = self._ecs_client.describe_services(
            cluster=cluster_name, services=[name]
        )

        if not service_response["services"]:
            raise NotFoundError(f"Service {name} not found.")

        service = service_response["services"][0]
        if service["status"] != "ACTIVE":
            raise NotFoundError(f"Service {name} is not active.")
        current_task_def_arn = service["taskDefinition"]

        try:
            task_def_response = self._ecs_client.describe_task_definition(
                taskDefinition=revision
            )
            task_def_arn = task_def_response["taskDefinition"][
                "taskDefinitionArn"
            ]
            current = task_def_arn == current_task_def_arn
            td_id = task_def_arn.split("/")[-1]

            result = self._result_converter.convert_revision(
                td_id,
                task_def_response["taskDefinition"],
                current,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClientException":
                raise NotFoundError(f"Revision {revision} not found.")
            raise e
        return Response(result=result)

    def delete_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[None]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)
        service_response = self._ecs_client.describe_services(
            cluster=cluster_name, services=[name]
        )

        if not service_response["services"]:
            raise NotFoundError(f"Service {name} not found.")

        service = service_response["services"][0]
        if service["status"] != "ACTIVE":
            raise NotFoundError(f"Service {name} is not active.")
        current_task_def_arn = service["taskDefinition"]
        current_revision = current_task_def_arn.split("/")[-1]

        if revision == current_revision:
            raise PreconditionFailedError(
                "Cannot delete the current revision of the service."
            )

        try:
            self._ecs_client.deregister_task_definition(
                taskDefinition=revision
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClientException":
                raise NotFoundError(f"Revision {revision} not found.")
            raise e

        return Response(result=None)

    def update_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        self.__setup__()
        cluster_name = self._get_cluster_name(name)
        revision = None
        for allocation in traffic:
            if allocation.percent > 0 and allocation.percent < 100:
                raise BadRequestError(
                    "Partial traffic allocation is not supported."
                )
            if allocation.percent == 100:
                revision = allocation.revision
                break
        if not revision:
            raise BadRequestError(
                "No revision with 100% traffic allocation found."
            )
        try:
            print("Updating to revision:", revision)
            self._ecs_client.update_service(
                cluster=cluster_name,
                service=name,
                taskDefinition=revision,
            )
            self._wait_for_service_stable(
                cluster_name=cluster_name,
                service_name=name,
                alb_arn=None,
            )
            return self.get_service(name)
        except ClientError as e:
            code = e.response["Error"]["Code"]
            if code in (
                "ServiceNotFoundException",
                "ClusterNotFoundException",
            ):
                raise NotFoundError(f"Service {name} not found.")
            if code == "ClientException" and "taskDefinition" in str(e):
                raise NotFoundError(f"Revision {revision} not found.")
            raise e

    def close(self) -> None:
        self._init = False

    def _get_service(
        self, cluster_name: str, service_name: str, **kwargs: Any
    ) -> Response[ServiceItem]:
        try:
            response = self._ecs_client.describe_services(
                cluster=cluster_name, services=[service_name]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ClusterNotFoundException":
                raise NotFoundError(f"Cluster {cluster_name} not found.")
            raise e
        if not response["services"]:
            raise NotFoundError(f"Service {service_name} not found.")
        service_desc = response["services"][0]
        if service_desc["status"] != "ACTIVE":
            raise NotFoundError(f"Service {service_name} is not active.")
        task_def_arn = response["services"][0]["taskDefinition"]
        svc_name = service_desc["serviceName"]
        task_def = self._ecs_client.describe_task_definition(
            taskDefinition=task_def_arn
        )
        scale = self._get_autoscaling(
            cluster_name=cluster_name, service_name=svc_name
        )
        endpoint = self._get_service_endpoint(service_desc=service_desc)
        traffic, latest_created_revision = self._get_revisions_traffic(
            service_desc=service_desc
        )
        result = self._result_converter.convert_service_item(
            service_desc,
            task=task_def["taskDefinition"],
            scale=scale,
            endpoint=endpoint,
            traffic=traffic,
            latest_created_revision=latest_created_revision,
        )
        return Response(result=result)

    def _get_revisions_traffic(
        self,
        service_desc: dict[str, Any],
        **kwargs: Any,
    ) -> tuple[list[TrafficAllocation], str | None]:
        traffic = []
        current_td_arn = service_desc["taskDefinition"]
        family = current_td_arn.split("/")[-1].rsplit(":", 1)[0]

        latest_created_revision = None
        paginator = self._ecs_client.get_paginator("list_task_definitions")
        for page in paginator.paginate(
            familyPrefix=family,
            status="ACTIVE",
            sort="DESC",
            PaginationConfig={"PageSize": 100},
        ):
            for td_arn in page.get("taskDefinitionArns", []):
                td_id = td_arn.split("/")[-1]
                if not latest_created_revision:
                    latest_created_revision = td_id
                current = td_arn == current_td_arn
                if current:
                    traffic.append(
                        TrafficAllocation(
                            revision=td_id,
                            percent=100,
                            latest_revision=current,
                        )
                    )
                else:
                    traffic.append(
                        TrafficAllocation(
                            revision=td_id,
                            percent=0,
                            latest_revision=current,
                        )
                    )
        return traffic, latest_created_revision

    def _get_alb_from_target_group(self, tg_arn: str) -> str | None:
        tg = self._elbv2_client.describe_target_groups(
            TargetGroupArns=[tg_arn]
        )["TargetGroups"][0]
        lbs = tg.get("LoadBalancerArns") or []
        return lbs[0] if lbs else None

    def _deregister_tasks(self, service_name: str) -> None:
        task_definitions = self._ecs_client.list_task_definitions(
            familyPrefix=service_name
        )["taskDefinitionArns"]
        for td_arn in task_definitions:
            self._ecs_client.deregister_task_definition(taskDefinition=td_arn)

    def _ensure_cluster(
        self,
        cluster_name: str,
        capacity_providers: list[str] | None,
        default_capacity_provider_strategy: list[dict[str, Any]] | None = None,
    ) -> None:
        """Ensure the ECS cluster exists."""
        response = self._ecs_client.describe_clusters(clusters=[cluster_name])
        clusters = response.get("clusters", [])

        if not clusters or clusters[0].get("status") != "ACTIVE":
            args: dict[str, Any] = {"clusterName": cluster_name}
            if capacity_providers:
                args["capacityProviders"] = capacity_providers
            if default_capacity_provider_strategy:
                args["defaultCapacityProviderStrategy"] = (
                    default_capacity_provider_strategy
                )
            self._ecs_client.create_cluster(**args)
            print(f"Created ECS cluster: {cluster_name}")
        else:
            print(f"ECS cluster already exists: {cluster_name}")

    def _delete_cluster(self, cluster_name: str) -> None:
        ls_paginator = self._ecs_client.get_paginator("list_services")
        remaining_services = []
        for page in ls_paginator.paginate(cluster=cluster_name):
            service_arns = page.get("serviceArns", [])
            remaining_services.extend(service_arns)
        if not remaining_services:
            self._ecs_client.delete_cluster(cluster=cluster_name)
            print(f"Deleted ECS cluster: {cluster_name}")

    def _create_ecs_execution_role(self) -> str:
        """Create or get ECS task execution role for
        pulling private ECR images."""
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ecs-tasks.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        role_name = "ECSTaskExecutionRole"
        try:
            role_response = self._iam_client.create_role(
                RoleName=role_name,
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Role for ECS tasks to pull from ECR",
            )
            role_arn = role_response["Role"]["Arn"]
        except self._iam_client.exceptions.EntityAlreadyExistsException:
            # Role already exists, get its ARN
            role_response = self._iam_client.get_role(RoleName=role_name)
            role_arn = role_response["Role"]["Arn"]

        # Attach ECS task execution policy
        self._iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",  # noqa: E501
        )
        return role_arn

    def _delete_ecs_execution_role(self) -> None:
        role_name = "ECSTaskExecutionRole"
        try:
            self._iam_client.detach_role_policy(
                RoleName=role_name,
                PolicyArn="arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy",  # noqa: E501
            )
            self._iam_client.delete_role(RoleName=role_name)
            print(f"Deleted ECS execution role: {role_name}")
        except self._iam_client.exceptions.NoSuchEntityException:
            print(f"ECS execution role {role_name} does not exist.")
        except ClientError as e:
            raise BadRequestError(f"Could not delete ECS execution role: {e}")

    def _ensure_ecs_instance_profile(self) -> str:
        role_name = "ecsInstanceRole"
        assume_role_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "ec2.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }

        # 1. Create role if missing
        try:
            self._iam_client.get_role(RoleName=role_name)
            print(f"IAM role {role_name} already exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                self._iam_client.create_role(
                    RoleName=role_name,
                    AssumeRolePolicyDocument=json.dumps(assume_role_policy),
                    Description="Allows EC2 instances to call ECS and related AWS services",  # noqa
                )
                print(f"Created IAM role {role_name}.")
            else:
                raise

        # 2. Attach AmazonEC2ContainerServiceforEC2Role policy if not already
        policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonEC2ContainerServiceforEC2Role"  # noqa
        attached_policies = self._iam_client.list_attached_role_policies(
            RoleName=role_name
        )["AttachedPolicies"]
        if not any(p["PolicyArn"] == policy_arn for p in attached_policies):
            self._iam_client.attach_role_policy(
                RoleName=role_name, PolicyArn=policy_arn
            )
            print(f"Attached policy {policy_arn} to role {role_name}.")

        # 3. Create instance profile if missing
        try:
            self._iam_client.get_instance_profile(
                InstanceProfileName=role_name
            )
            print(f"Instance profile {role_name} already exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchEntity":
                self._iam_client.create_instance_profile(
                    InstanceProfileName=role_name
                )
                print(f"Created instance profile {role_name}.")
            else:
                raise

        # 4. Add role to instance profile if not already added
        ip_roles = self._iam_client.get_instance_profile(
            InstanceProfileName=role_name
        )["InstanceProfile"]["Roles"]
        if not any(r["RoleName"] == role_name for r in ip_roles):
            self._iam_client.add_role_to_instance_profile(
                InstanceProfileName=role_name, RoleName=role_name
            )
            print(f"Added role {role_name} to instance profile {role_name}.")

        self._wait_for_instance_profile_ready(role_name)
        instance_profile = self._iam_client.get_instance_profile(
            InstanceProfileName=role_name
        )["InstanceProfile"]
        return instance_profile["Arn"]

    def _wait_for_instance_profile_ready(
        self,
        role_name: str,
        timeout: int = 60,
    ):
        start = time.monotonic()
        while True:
            try:
                ip = self._iam_client.get_instance_profile(
                    InstanceProfileName=role_name
                )["InstanceProfile"]
                has_role = any(
                    r["RoleName"] == role_name for r in ip.get("Roles", [])
                )
                if has_role:
                    print(f"Instance profile {role_name} is ready.")
                    return
            except self._iam_client.exceptions.NoSuchEntityException:
                pass
            if time.monotonic() - start > timeout:
                print("Timeout waiting for instance profile to be ready.")
                break
            time.sleep(2)

    def _delete_ecs_instance_profile(self):
        role_name = "ecsInstanceRole"
        # 1. Remove the role from the instance profile
        try:
            ip = self._iam_client.get_instance_profile(
                InstanceProfileName=role_name
            )
            roles = ip["InstanceProfile"]["Roles"]
            for r in roles:
                self._iam_client.remove_role_from_instance_profile(
                    InstanceProfileName=role_name, RoleName=r["RoleName"]
                )
                print(
                    f"Removed role {r['RoleName']} from instance profile "
                    f"{role_name}."
                )
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

        # 2. Delete the instance profile
        try:
            self._iam_client.delete_instance_profile(
                InstanceProfileName=role_name
            )
            print(f"Deleted instance profile {role_name}.")
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

        # 3. Detach all policies from the role
        try:
            attached_policies = self._iam_client.list_attached_role_policies(
                RoleName=role_name
            )["AttachedPolicies"]
            for p in attached_policies:
                self._iam_client.detach_role_policy(
                    RoleName=role_name, PolicyArn=p["PolicyArn"]
                )
                print(
                    f"Detached policy {p['PolicyArn']} from role {role_name}."
                )
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

        # 4. Delete the role
        try:
            self._iam_client.delete_role(RoleName=role_name)
            print(f"Deleted IAM role {role_name}.")
        except ClientError as e:
            if e.response["Error"]["Code"] != "NoSuchEntity":
                raise

    def _apply_readiness_probe_to_target_group(
        self,
        target_group_arn: str,
        readiness_probe: Probe | None,
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> None:
        if not readiness_probe:
            return

        if not readiness_probe.http_get:
            return

        h = readiness_probe.http_get
        protocol = (h.scheme or "HTTP").upper()
        if protocol not in ("HTTP", "HTTPS"):
            protocol = "HTTP"

        self._elbv2_client.modify_target_group(
            TargetGroupArn=target_group_arn,
            HealthCheckEnabled=True,
            HealthCheckProtocol=protocol,
            HealthCheckPort=(
                str(h.port) if network_mode == "awsvpc" else "traffic-port"
            ),
            HealthCheckPath=h.path or "/",
            HealthCheckIntervalSeconds=readiness_probe.period_seconds or 30,
            HealthCheckTimeoutSeconds=readiness_probe.timeout_seconds or 5,
            HealthyThresholdCount=readiness_probe.success_threshold or 3,
            UnhealthyThresholdCount=readiness_probe.failure_threshold or 3,
            # Matcher={"HttpCode": "200-399"},
        )
        print(f"Updated health check on target group {target_group_arn}")

    def _ensure_alb_security_group(
        self,
        cluster_name: str,
        vpc_id: str,
        port: int,
    ) -> str:
        """Ensure security group for ALB exists."""
        if self.alb_security_group_id:
            return self.alb_security_group_id

        group_name = f"{cluster_name}-alb-sg"

        try:
            # Try to create the security group
            sg_response = self._ec2_client.create_security_group(
                GroupName=group_name,
                Description=f"Security group for {cluster_name} ALB",
                VpcId=vpc_id,
            )
            alb_sg_id = sg_response["GroupId"]

            # Allow public access on the specified port
            self._ec2_client.authorize_security_group_ingress(
                GroupId=alb_sg_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": port,
                        "ToPort": port,
                        "IpRanges": [{"CidrIp": "0.0.0.0/0"}],
                    }
                ],
            )

            print(f"Created ALB security group: {alb_sg_id}")
            return alb_sg_id

        except ClientError as e:
            if e.response["Error"]["Code"] == "InvalidGroup.Duplicate":
                # Lookup existing security group by name and VPC
                describe = self._ec2_client.describe_security_groups(
                    Filters=[
                        {"Name": "group-name", "Values": [group_name]},
                        {"Name": "vpc-id", "Values": [vpc_id]},
                    ]
                )
                groups = describe.get("SecurityGroups", [])
                if groups:
                    sg_id = groups[0]["GroupId"]
                    print(f"Using existing ALB security group: {sg_id}")
                    return sg_id
                else:
                    raise BadRequestError(
                        (
                            f"Security group '{group_name}' already exists "
                            "but could not be found"
                        )
                    )
            else:
                raise BadRequestError(
                    f"Could not create ALB security group: {e}"
                )

    def _delete_alb_security_group(self, cluster_name: str) -> None:
        group_name = f"{cluster_name}-alb-sg"
        sgs = self._ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [group_name]}]
        )
        for sg in sgs["SecurityGroups"]:
            self._wait_for_security_group_detach(sg["GroupId"])

    def _ensure_ecs_security_group(
        self,
        cluster_name: str,
        vpc_id: str,
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> str:
        if self.ecs_security_group_id:
            return self.ecs_security_group_id

        if network_mode == "awsvpc":
            sg_name = f"{cluster_name}-ecs-task-sg"
        else:
            sg_name = f"{cluster_name}-ecs-instance-sg"
        existing = self._ec2_client.describe_security_groups(
            Filters=[
                {"Name": "group-name", "Values": [sg_name]},
                {"Name": "vpc-id", "Values": [vpc_id]},
            ]
        )["SecurityGroups"]

        if existing:
            return existing[0]["GroupId"]

        sg_id = self._ec2_client.create_security_group(
            GroupName=sg_name,
            Description=f"ECS EC2 instances for cluster {cluster_name}",
            VpcId=vpc_id,
        )["GroupId"]
        return sg_id

    def _ensure_authorize_security_group_ingress(
        self,
        security_group_id: str,
        port: int | None = None,
        caller_sg_id: str | None = None,
    ) -> None:
        if not port or not caller_sg_id:
            return
        try:
            self._ec2_client.authorize_security_group_ingress(
                GroupId=security_group_id,
                IpPermissions=[
                    {
                        "IpProtocol": "tcp",
                        "FromPort": port,
                        "ToPort": port,
                        "UserIdGroupPairs": [{"GroupId": caller_sg_id}],
                    }
                ],
            )
        except ClientError as e:
            if (
                e.response.get("Error", {}).get("Code")
                != "InvalidPermission.Duplicate"
            ):
                raise e

    def _delete_ecs_security_group(
        self,
        cluster_name: str,
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> None:
        if network_mode == "awsvpc":
            group_name = f"{cluster_name}-ecs-task-sg"
        else:
            group_name = f"{cluster_name}-ecs-instance-sg"
        sgs = self._ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [group_name]}]
        )
        for sg in sgs["SecurityGroups"]:
            self._wait_for_security_group_detach(sg["GroupId"])

    def _delete_instance_security_group(self, cluster_name: str) -> None:
        group_name = f"{cluster_name}-ecs-instances"
        sgs = self._ec2_client.describe_security_groups(
            Filters=[{"Name": "group-name", "Values": [group_name]}]
        )
        for sg in sgs["SecurityGroups"]:
            self._wait_for_security_group_detach(sg["GroupId"])

    def _ensure_load_balancer(
        self,
        service_name: str,
        vpc_id: str,
        subnet_ids: list[str],
        security_group_id: str,
        target_port: int,
        exposed_port: int,
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> tuple[str | None, str]:
        """Ensure ALB and Target Group exist, return Target Group ARN."""
        if self.target_group_arn:
            return None, self.target_group_arn

        alb_name = f"{service_name}-alb"
        tg_name = f"{service_name}-tg"

        # 1. Create or get ALB
        try:
            response = self._elbv2_client.create_load_balancer(
                Name=alb_name,
                Subnets=subnet_ids,
                SecurityGroups=[security_group_id],
                Scheme="internet-facing",
                Type="application",
                IpAddressType="ipv4",
            )
            alb_arn = response["LoadBalancers"][0]["LoadBalancerArn"]
            print(f"Created ALB: {alb_arn}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateLoadBalancerName":
                alb = self._elbv2_client.describe_load_balancers(
                    Names=[alb_name]
                )["LoadBalancers"][0]
                alb_arn = alb["LoadBalancerArn"]
                print(f"Using existing ALB: {alb_arn}")
            else:
                raise BadRequestError(f"Could not create ALB: {e}")

        # 2. Create or get Target Group
        target_type = "ip" if network_mode == "awsvpc" else "instance"
        health_check_port = (
            str(target_port) if target_type == "ip" else "traffic-port"
        )
        try:
            tg = self._elbv2_client.create_target_group(
                Name=tg_name,
                Protocol="HTTP",
                Port=target_port,
                VpcId=vpc_id,
                TargetType=target_type,
                HealthCheckProtocol="HTTP",
                HealthCheckPort=health_check_port,
            )
            tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]
            print(f"Created Target Group: {tg_arn}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateTargetGroupName":
                tg = self._elbv2_client.describe_target_groups(
                    Names=[tg_name]
                )["TargetGroups"][0]
                tg_arn = tg["TargetGroupArn"]
                print(f"Using existing Target Group: {tg_arn}")
            else:
                raise BadRequestError(f"Could not create Target Group: {e}")

        # 3. Create listener if not exists
        try:
            self._elbv2_client.create_listener(
                LoadBalancerArn=alb_arn,
                Protocol="HTTP",
                Port=exposed_port,
                DefaultActions=[{"Type": "forward", "TargetGroupArn": tg_arn}],
            )
            print(f"Created listener on port {exposed_port}")
        except ClientError as e:
            if e.response["Error"]["Code"] == "DuplicateListener":
                print(f"Listener on port {exposed_port} already exists")
            else:
                raise BadRequestError(f"Could not create listener: {e}")
        return alb_arn, tg_arn

    def _delete_load_balancer(self, service_name: str) -> None:
        tg_name = f"{service_name}-tg"
        alb_name = f"{service_name}-alb"

        # ALB
        try:
            alb = self._elbv2_client.describe_load_balancers(Names=[alb_name])
            alb_arn = alb["LoadBalancers"][0]["LoadBalancerArn"]
        except ClientError as e:
            if e.response["Error"]["Code"] in (
                "LoadBalancerNotFound",
                "LoadBalancerNotFoundException",
            ):
                print(f"ALB {alb_name} not found.")
                alb_arn = None
            else:
                raise

        if alb_arn:
            listeners = self._elbv2_client.describe_listeners(
                LoadBalancerArn=alb_arn
            )["Listeners"]
            for listener in listeners:
                self._elbv2_client.delete_listener(
                    ListenerArn=listener["ListenerArn"]
                )
            self._elbv2_client.delete_load_balancer(LoadBalancerArn=alb_arn)
            print(f"Deleted ALB: {alb_arn}")

        # Target Group
        try:
            tg = self._elbv2_client.describe_target_groups(Names=[tg_name])
            tg_arn = tg["TargetGroups"][0]["TargetGroupArn"]
            self._elbv2_client.delete_target_group(TargetGroupArn=tg_arn)
            print(f"Deleted Target Group: {tg_arn}")
        except ClientError as e:
            if e.response["Error"]["Code"] in (
                "TargetGroupNotFound",
                "TargetGroupNotFoundException",
            ):
                print(f"Target Group {tg_name} not found.")
            else:
                raise

    def _ensure_application_autoscaling(
        self,
        cluster_name: str,
        service_name: str,
        scale: Scale | None,
    ) -> None:
        if not scale or scale.mode != "auto":
            return
        scalable_target = self._op_converter.convert_scalable_target(
            cluster_name,
            service_name,
            scale,
        )
        if scalable_target:
            self._application_autoscaling_client.register_scalable_target(
                **scalable_target
            )

        for rule in scale.rules:
            policy = self._op_converter.convert_scale_rule_to_policy(
                cluster_name,
                service_name,
                scale,
                rule,
            )
            if policy:
                self._application_autoscaling_client.put_scaling_policy(
                    **policy
                )

    def _get_autoscaling(self, cluster_name: str, service_name: str) -> Scale:
        scalable_targets = (
            self._application_autoscaling_client.describe_scalable_targets(
                ServiceNamespace="ecs",
                ResourceIds=[f"service/{cluster_name}/{service_name}"],
            )["ScalableTargets"]
        )
        if not scalable_targets:
            return Scale(mode="manual")
        target = scalable_targets[0]
        scale = self._result_converter.convert_scalable_target_to_scale(target)
        scaling_policies = (
            self._application_autoscaling_client.describe_scaling_policies(
                ServiceNamespace="ecs",
                ResourceId=f"service/{cluster_name}/{service_name}",
            )["ScalingPolicies"]
        )
        for policy in scaling_policies:
            rule = self._result_converter.convert_scaling_policy_to_scale_rule(
                policy
            )
            if rule:
                scale.rules.append(rule)
                scale.cooldown_period = (
                    self._result_converter.get_cooldown_period(policy)
                )
        return scale

    def _delete_autoscaling(
        self,
        cluster_name: str,
        service_name: str,
    ) -> None:
        scalable_targets = (
            self._application_autoscaling_client.describe_scalable_targets(
                ServiceNamespace="ecs",
                ResourceIds=[f"service/{cluster_name}/{service_name}"],
            )["ScalableTargets"]
        )

        # Deregister all scalable targets
        for target in scalable_targets:
            self._application_autoscaling_client.deregister_scalable_target(
                ServiceNamespace="ecs",
                ResourceId=target["ResourceId"],
                ScalableDimension=target["ScalableDimension"],
            )
            print(f"Deregistered scalable target for {service_name}")

        # List all scaling policies for the service
        scaling_policies = (
            self._application_autoscaling_client.describe_scaling_policies(
                ServiceNamespace="ecs",
                ResourceId=f"service/{cluster_name}/{service_name}",
            )["ScalingPolicies"]
        )

        # Delete all scaling policies
        for policy in scaling_policies:
            self._application_autoscaling_client.delete_scaling_policy(
                ServiceNamespace="ecs",
                PolicyName=policy["PolicyName"],
                ResourceId=f"service/{cluster_name}/{service_name}",
                ScalableDimension=policy["ScalableDimension"],
            )
            print(f"Deleted scaling policy: {policy['PolicyName']}")

    def _wait_for_security_group_detach(self, sg_id, timeout=60):
        for _ in range(timeout):
            try:
                self._ec2_client.delete_security_group(GroupId=sg_id)
                print(f"Deleted security group: {sg_id}")
                return
            except ClientError as e:
                if "DependencyViolation" in str(e):
                    time.sleep(5)
                else:
                    raise e
        print("Timed out waiting for security group to be deleted.")

    def _wait_for_service_stable(
        self,
        cluster_name: str,
        service_name: str,
        alb_arn: str | None,
        timeout: int = 600,
    ) -> None:
        print(f"Waiting for service {service_name} to be stable")
        start_time = time.time()
        while True:
            response = self._ecs_client.describe_services(
                cluster=cluster_name, services=[service_name]
            )
            service = response["services"][0]
            primary = next(
                (
                    d
                    for d in service["deployments"]
                    if d["status"] == "PRIMARY"
                ),
                None,
            )

            if (
                primary
                and primary.get("rolloutState") == "COMPLETED"
                and service["runningCount"] == service["desiredCount"]
            ):
                print(f"Service '{service_name}' is stable.")
                break

            if time.time() - start_time > timeout:
                print(
                    f"Service {service_name} did not become stable within "
                    f"{timeout} seconds."
                )
                break

            time.sleep(5)

        start_time = time.time()
        if alb_arn:
            while True:
                lb_desc = self._elbv2_client.describe_load_balancers(
                    LoadBalancerArns=[alb_arn]
                )
                state = lb_desc["LoadBalancers"][0]["State"]["Code"]
                if state == "active":
                    print(f"ALB {alb_arn} is now ACTIVE.")
                    break
                if time.time() - start_time > timeout:
                    print(
                        f"ALB {alb_arn} did not become ACTIVE within "
                        f"{timeout} seconds."
                    )
                    break
                time.sleep(5)

    def wait_for_drained(
        self, cluster_name: str, service_name: str, timeout=600
    ):
        t0 = time.time()
        while True:
            svc = self._ecs_client.describe_services(
                cluster=cluster_name, services=[service_name]
            )["services"][0]
            running = svc.get("runningCount", 0)
            deployments = svc.get("deployments", [])
            draining = any(d["status"] != "PRIMARY" for d in deployments)
            if running == 0 and not draining:
                return
            if time.time() - t0 > timeout:
                print("Timed out waiting for service to drain.")
            time.sleep(5)

    def _get_service_endpoint(
        self, service_desc: dict[str, Any]
    ) -> dict | None:
        lbs = service_desc.get("loadBalancers") or []
        if not lbs:
            return None
        tg_arn = lbs[0].get("targetGroupArn")
        if not tg_arn:
            return None

        tg = self._elbv2_client.describe_target_groups(
            TargetGroupArns=[tg_arn]
        )["TargetGroups"][0]
        lb_arn = tg["LoadBalancerArns"][0]
        lb = self._elbv2_client.describe_load_balancers(
            LoadBalancerArns=[lb_arn]
        )["LoadBalancers"][0]
        listeners = self._elbv2_client.describe_listeners(
            LoadBalancerArn=lb_arn
        )["Listeners"]

        # prefer the listener that forwards to our TG
        matched = None
        for lst in listeners:
            for act in lst.get("DefaultActions", []):
                if (
                    act.get("Type") == "forward"
                    and act.get("TargetGroupArn") == tg_arn
                ):
                    matched = lst
                    break
            if matched:
                break
        # fallback: first listener
        use = matched or (listeners[0] if listeners else None)
        if not use:
            return None
        scheme = "https" if use.get("Port") == 443 else "http"
        return {
            "uri": f"{scheme}://{lb['DNSName']}",
            "port": use.get("Port", 80),
            "scheme": scheme,
        }

    def _get_readiness_probe(self, service: ServiceDefinition) -> Probe | None:
        for container in service.containers:
            if container.type == "main" and container.ports:
                if container.probes and container.probes.readiness_probe:
                    return container.probes.readiness_probe
        return None

    def _get_service_name(self, service: ServiceDefinition) -> str:
        if self.service_name:
            return self.service_name
        if service.name:
            return service.name
        raise BadRequestError(
            "Service name must be provided or defined in the service."
        )

    def _get_cluster_name(self, service_name: str) -> str:
        if self.cluster_name:
            return self.cluster_name
        return f"{service_name}-cluster"

    def _get_latest_ecs_ami_id(self) -> str:
        images = self._ec2_client.describe_images(
            Filters=[
                {"Name": "name", "Values": ["amzn2-ami-ecs-hvm-*-x86_64-ebs"]},
                {"Name": "state", "Values": ["available"]},
            ],
            Owners=["amazon"],
        )["Images"]
        if not images:
            raise BadRequestError("No ECS-optimized AMI found.")
        images.sort(key=lambda x: x["CreationDate"], reverse=True)
        return images[0]["ImageId"]

    def _get_user_data(self, cluster_name: str):
        # Amazon Linux ECS agent bootstrapping
        script = f"""#!/bin/bash
echo ECS_CLUSTER={cluster_name} >> /etc/ecs/ecs.config
"""
        return base64.b64encode(script.encode("utf-8")).decode("utf-8")

    def _ensure_auto_scaling_group(
        self,
        cluster_name: str,
        lt_id: str,
        lt_version: str,
        subnets: list[str],
        min_size: int,
        max_size: int,
        desired: int,
    ) -> str:
        asg_name = f"{cluster_name}-asg"
        subnet_str = ",".join(subnets)
        response = self._autoscaling_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        groups = response.get("AutoScalingGroups", [])
        exists = any(
            g.get("AutoScalingGroupName") == asg_name
            and g.get("AutoScalingGroupARN")
            and g.get("Status", "Active") == "Active"
            for g in groups
        )
        if exists:
            self._autoscaling_client.update_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                MinSize=min_size,
                MaxSize=max_size,
                DesiredCapacity=desired,
                LaunchTemplate={
                    "LaunchTemplateId": lt_id,
                    "Version": lt_version,
                },
                VPCZoneIdentifier=subnet_str,
            )
            print(f"Updated auto scaling group: {asg_name}")
            return response["AutoScalingGroups"][0]["AutoScalingGroupARN"]
        else:
            attempts, delay = 5, 0.7
            for i in range(1, attempts + 1):
                try:
                    self._autoscaling_client.create_auto_scaling_group(
                        AutoScalingGroupName=asg_name,
                        MinSize=min_size,
                        MaxSize=max_size,
                        DesiredCapacity=desired,
                        LaunchTemplate={
                            "LaunchTemplateId": lt_id,
                            "Version": lt_version,
                        },
                        VPCZoneIdentifier=subnet_str,
                        HealthCheckType="EC2",
                        NewInstancesProtectedFromScaleIn=False,
                    )
                    response = (
                        self._autoscaling_client.describe_auto_scaling_groups(
                            AutoScalingGroupNames=[asg_name]
                        )["AutoScalingGroups"][0]
                    )
                    print(f"Created auto scaling group: {asg_name}")
                    break
                except ClientError as e:
                    err = e.response.get("Error", {})
                    code = err.get("Code", "")
                    msg = (err.get("Message") or "").lower()

                    # Retry only for the profile-propagation case
                    should_retry = (
                        code == "ValidationError"
                        and "iaminstanceprofile" in msg
                        and ("invalid" in msg or "not found" in msg)
                    )
                    if not should_retry or i == attempts:
                        raise e

                    # small backoff with jitter
                    time.sleep(delay)
                    delay = min(delay * 1.7 + (0.1 * i), 3.0)
            return response["AutoScalingGroupARN"]

    def _delete_autoscaling_group(self, cluster_name: str) -> None:
        asg_name = f"{cluster_name}-asg"
        try:
            resp = self._autoscaling_client.describe_auto_scaling_groups(
                AutoScalingGroupNames=[asg_name]
            )
            groups = resp.get("AutoScalingGroups", [])
            if not groups:
                return
        except ClientError:
            return

        # Scale to zero (min/max/desired = 0)
        try:
            self._autoscaling_client.update_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                MinSize=0,
                MaxSize=0,
                DesiredCapacity=0,
            )
        except ClientError:
            pass

        # Option A: force delete (terminates instances automatically)
        try:
            self._autoscaling_client.delete_auto_scaling_group(
                AutoScalingGroupName=asg_name,
                ForceDelete=True,
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code == "ResourceInUse":
                # Try again after a short wait
                time.sleep(5)
                self._autoscaling_client.delete_auto_scaling_group(
                    AutoScalingGroupName=asg_name,
                    ForceDelete=True,
                )
            elif code == "ValidationError":
                # Might be already gone
                return
            else:
                raise

        # Poll until it's gone (idempotent)
        for _ in range(24):  # ~2 minutes
            time.sleep(5)
            try:
                out = self._autoscaling_client.describe_auto_scaling_groups(
                    AutoScalingGroupNames=[asg_name]
                )
                if not out.get("AutoScalingGroups"):
                    break
            except ClientError:
                break
        print("Deleted auto scaling group:", asg_name)

    def _ensure_capacity_provider(
        self,
        cluster_name: str,
        auto_scaling_group_arn: str,
        target_capacity: int,
    ) -> str:
        cp_name = f"{cluster_name}-cp"
        response = self._ecs_client.describe_capacity_providers(
            capacityProviders=[cp_name]
        )
        exists = False
        if (
            response["capacityProviders"]
            and response["capacityProviders"][0]["status"] == "ACTIVE"
        ):
            cp = response["capacityProviders"][0]
            current = cp.get("autoScalingGroupProvider", {}) or {}
            cur_ms = current.get("managedScaling", {}) or {}
            exists = True
            existing_asg_arn = current.get("autoScalingGroupArn")
            if existing_asg_arn != auto_scaling_group_arn:
                print(
                    (
                        f"Capacity provider {cp_name} already exists but "
                        f"with different auto scaling group. Deleting it."
                    )
                )
                self._delete_capacity_provider(cluster_name=cluster_name)
                exists = False

        if exists:
            need_update = (
                current.get("autoScalingGroupArn") != auto_scaling_group_arn
                or cur_ms.get("status") != "ENABLED"
                or int(cur_ms.get("targetCapacity", 0)) != int(target_capacity)
            )
            if need_update:
                self._ecs_client.update_capacity_provider(
                    name=cp_name,
                    autoScalingGroupProvider={
                        "managedScaling": {
                            "status": "ENABLED",
                            "targetCapacity": int(target_capacity),
                        },
                        "managedTerminationProtection": "DISABLED",
                    },
                )
            print(f"Updated capacity provider {cp_name}.")
            return cp["name"]
        else:
            cp = self._ecs_client.create_capacity_provider(
                name=cp_name,
                autoScalingGroupProvider={
                    "autoScalingGroupArn": auto_scaling_group_arn,
                    "managedScaling": {
                        "status": "ENABLED",
                        "targetCapacity": int(target_capacity),
                        "minimumScalingStepSize": 1,
                        "maximumScalingStepSize": 1000,
                    },
                    "managedTerminationProtection": "DISABLED",
                },
            )
            print(f"Created capacity provider {cp_name}.")
            return cp["capacityProvider"]["name"]

    def _delete_capacity_provider(self, cluster_name: str) -> None:
        # Best-effort deletion (idempotent)
        cp_name = f"{cluster_name}-cp"
        resp = self._ecs_client.describe_capacity_providers(
            capacityProviders=[cp_name]
        )
        if not resp.get("capacityProviders"):
            return
        try:
            self._ecs_client.delete_capacity_provider(capacityProvider=cp_name)
            # No waiter available;
            # a short sleep reduces immediate read-after-write flakiness
            time.sleep(2)
        except ClientError as e:
            msg = e.response.get("Error", {}).get("Code", "")
            if msg in ("CapacityProviderNotFoundException",):
                return
            if msg == "ResourceInUseException":
                raise RuntimeError(
                    (
                        f"Capacity provider '{cp_name}' is still in use by a "
                        "cluster or service."
                    )
                )
            raise
        print(f"Deleted capacity provider {cp_name}.")

    def _attach_capacity_providers_to_cluster(
        self,
        cluster_name: str,
        capacity_providers: list[str],
        default_capacity_provider_strategy: list[dict[str, Any]],
    ):
        self._ecs_client.put_cluster_capacity_providers(
            cluster=cluster_name,
            capacityProviders=capacity_providers,
            defaultCapacityProviderStrategy=default_capacity_provider_strategy,
        )

    def _detach_capacity_provider_from_cluster(
        self,
        cluster_name: str,
    ) -> None:
        try:
            self._ecs_client.put_cluster_capacity_providers(
                cluster=cluster_name,
                capacityProviders=[],
                defaultCapacityProviderStrategy=[],
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("ClusterNotFoundException", "ClientException"):
                return
            raise

    def _ensure_ec2_capacity(
        self,
        cluster_name: str,
        subnet_ids: list[str],
        security_group_id: str,
    ) -> None:
        ami_id = self.ec2_ami_id
        ecs_instance_profile_arn = self.ecs_instance_profile_arn
        auto_scaling_group_arn = self.ec2_auto_scaling_group_arn
        capacity_providers = self.capacity_providers
        default_capacity_provider_strategy = (
            self.default_capacity_provider_strategy
        )
        if not ami_id:
            ami_id = self._get_latest_ecs_ami_id()
        if not ecs_instance_profile_arn:
            ecs_instance_profile_arn = self._ensure_ecs_instance_profile()
        if self.ec2_launch_template_id:
            lt_id = self.ec2_launch_template_id
            lt_ver = self.ec2_launch_template_version or "$Latest"
        else:
            lt_id, lt_ver = ensure_launch_template(
                ec2_client=self._ec2_client,
                launch_template_name=f"{cluster_name}-lt",
                ami_id=ami_id,
                instance_type=self.ec2_instance_type or "t3.micro",
                security_group_id=security_group_id,
                instance_profile_arn=ecs_instance_profile_arn,
                user_data=self._get_user_data(cluster_name=cluster_name),
            )

        if not auto_scaling_group_arn:
            auto_scaling_group_arn = self._ensure_auto_scaling_group(
                cluster_name=cluster_name,
                lt_id=lt_id,
                lt_version=lt_ver,
                subnets=subnet_ids,
                min_size=self.ec2_min_size or 1,
                max_size=self.ec2_max_size or 10,
                desired=self.ec2_desired_capacity or 2,
            )
        if not capacity_providers:
            cp_name = self._ensure_capacity_provider(
                cluster_name=cluster_name,
                auto_scaling_group_arn=auto_scaling_group_arn,
                target_capacity=self.ec2_target_capacity or 100,
            )
            capacity_providers = [cp_name]
            default_capacity_provider_strategy = [
                {"capacityProvider": cp_name, "weight": 1}
            ]
        if not default_capacity_provider_strategy:
            default_capacity_provider_strategy = [
                {"capacityProvider": capacity_providers[0], "weight": 1}
            ]
        self._attach_capacity_providers_to_cluster(
            cluster_name,
            capacity_providers=capacity_providers,
            default_capacity_provider_strategy=default_capacity_provider_strategy,  # noqa
        )
        self._wait_for_container_instances(cluster_name=cluster_name)
        print("Ensured EC2 capacity for ECS cluster.")

    def _delete_ec2_capacity(
        self,
        cluster_name: str,
    ) -> None:
        # 1) Detach & delete capacity provider
        if not self.capacity_providers:
            self._detach_capacity_provider_from_cluster(
                cluster_name=cluster_name
            )
            self._delete_capacity_provider(cluster_name=cluster_name)

        # 2) Scale ASG to zero and delete
        if not self.ec2_auto_scaling_group_arn:
            self._delete_autoscaling_group(cluster_name=cluster_name)

        # 3) Delete launch template (by id if you stored it; otherwise by name)
        if not self.ec2_launch_template_id:
            delete_launch_template(
                ec2_client=self._ec2_client,
                launch_template_name=f"{cluster_name}-lt",
            )

        # 4) (Optional) Delete ecsInstanceRole instance profile + role
        if not self.ecs_instance_profile_arn:
            self._delete_ecs_instance_profile()

    def _wait_for_container_instances(
        self, cluster_name: str, timeout: int = 300
    ):
        import time

        start = time.time()
        while True:
            arns = self._ecs_client.list_container_instances(
                cluster=cluster_name
            ).get("containerInstanceArns", [])
            if arns:
                desc = self._ecs_client.describe_container_instances(
                    cluster=cluster_name, containerInstances=arns
                )
                active = [
                    ci
                    for ci in desc["containerInstances"]
                    if ci["status"] == "ACTIVE" and ci["agentConnected"]
                ]
                if active:
                    return
            if time.time() - start > timeout:
                raise TimeoutError(
                    "No ECS container instances registered in time"
                )
            time.sleep(5)


class OperationConverter:
    def convert_update_service(
        self,
        cluster_name: str,
        service_name: str,
        task_definition: str,
        service: ServiceDefinition,
        launch_kind: Literal["FARGATE", "EC2"],
    ) -> dict[str, Any]:
        args = {
            "cluster": cluster_name,
            "service": service_name,
            "taskDefinition": task_definition,
            "desiredCount": self._convert_scale_to_desired_count(
                service.scale
            ),
            "forceNewDeployment": True,
        }
        if launch_kind == "FARGATE":
            args["platformVersion"] = "LATEST"
        return args

    def convert_create_service(
        self,
        cluster_name: str,
        service_name: str,
        task_definition: str,
        service: ServiceDefinition,
        launch_kind: Literal["FARGATE", "EC2"],
        network_mode: Literal["awsvpc", "bridge", "host"],
        ingress: dict[str, Any],
        subnet_ids: list[str],
        security_group_id: str,
        target_group_arn: str | None,
        capacity_provider_strategy: list[dict] | None,
    ) -> dict[str, Any]:
        service_def: dict[str, Any] = {
            "cluster": cluster_name,
            "serviceName": service_name,
            "taskDefinition": task_definition,
            "desiredCount": self._convert_scale_to_desired_count(
                service.scale
            ),
        }
        if capacity_provider_strategy:
            service_def["capacityProviderStrategy"] = (
                capacity_provider_strategy
            )
        else:
            service_def["launchType"] = launch_kind
        if launch_kind == "FARGATE":
            service_def["platformVersion"] = "LATEST"
        if network_mode == "awsvpc":
            service_def["networkConfiguration"] = {
                "awsvpcConfiguration": {
                    "subnets": subnet_ids,
                    "securityGroups": [security_group_id],
                }
            }
            if launch_kind == "FARGATE":
                service_def["networkConfiguration"]["awsvpcConfiguration"][
                    "assignPublicIp"
                ] = ("ENABLED" if ingress["external"] else "DISABLED")
        if ingress["external"]:
            service_def["loadBalancers"] = [
                {
                    "targetGroupArn": target_group_arn,
                    "containerName": ingress["container_name"],
                    "containerPort": ingress["target_port"],
                }
            ]
        return service_def

    def convert_ingress(self, service: ServiceDefinition) -> dict[str, Any]:
        external: bool = True
        target_port: int | None = None
        exposed_port: int | None = None
        container_name: str | None = None

        if service.ingress:
            external = service.ingress.external
            target_port = service.ingress.target_port
            exposed_port = service.ingress.port

        for container in service.containers:
            if container.type == "main" and container.ports:
                container_name = container.name
                if not target_port:
                    target_port = container.ports[0].container_port
                break

        if not container_name:
            for container in service.containers:
                if container.type == "main":
                    container_name = container.name
                    break

        if not exposed_port:
            exposed_port = target_port

        return dict(
            external=external,
            target_port=target_port,
            exposed_port=exposed_port,
            container_name=container_name,
        )

    def convert_task_definition(
        self,
        service_name: str,
        service: ServiceDefinition,
        images: list[str],
        launch_kind: Literal["FARGATE", "EC2"],
        network_mode: Literal["awsvpc", "bridge", "host"],
        execution_role_arn: str | None = None,
        task_role_arn: str | None = None,
    ) -> dict[str, Any]:
        task_def: dict[str, Any] = {
            "family": service_name,
            "networkMode": network_mode,
            "requiresCompatibilities": [launch_kind],
        }
        resources = self._convert_aggregate_resources(service)
        if launch_kind == "FARGATE":
            task_def["cpu"] = resources.get("cpu", "256")
            task_def["memory"] = resources.get("memory", "512")
        else:
            if resources.get("cpu"):
                task_def["cpu"] = str(resources["cpu"])
            if resources.get("memory"):
                task_def["memory"] = str(resources["memory"])
        if execution_role_arn:
            task_def["executionRoleArn"] = execution_role_arn
        if task_role_arn:
            task_def["taskRoleArn"] = task_role_arn
        task_def["containerDefinitions"] = self._convert_containers(
            service,
            images,
            network_mode,
        )
        task_def["volumes"] = self._convert_volumes(service.volumes)
        return task_def

    def _convert_containers(
        self,
        service: ServiceDefinition,
        images: list[str],
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> list[dict[str, Any]]:
        containers: list[dict[str, Any]] = []
        for container, image in zip(service.containers, images):
            container_def = self._convert_container(
                container, image, network_mode
            )
            depends_on = self._convert_depends_on(
                container.name or "main", service
            )
            if depends_on:
                container_def["dependsOn"] = depends_on
            if container_def:
                containers.append(container_def)
        return containers

    def _convert_container(
        self,
        container: Container,
        image: str,
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> dict:
        container_def: dict[str, Any] = {
            "name": container.name or "main",
            "image": image,
            "essential": True,
            "environment": self._convert_env(container.env),
            "portMappings": self._convert_ports(container.ports, network_mode),
        }

        if container.command:
            container_def["entryPoint"] = container.command
        if container.args:
            container_def["command"] = container.args
        if container.working_dir:
            container_def["workingDirectory"] = container.working_dir

        if container.resources:
            resources = self._convert_resources(container.resources)
            if resources:
                container_def["cpu"] = resources.get("cpu")
                container_def["memory"] = resources.get("memory")

        if container.probes:
            healthcheck = self._convert_probes_to_healthcheck(container.probes)
            if healthcheck:
                container_def["healthCheck"] = healthcheck

        if container.volume_mounts:
            container_def["mountPoints"] = self._convert_volume_mounts(
                container.volume_mounts
            )

        return container_def

    def _convert_aggregate_resources(
        self,
        service: ServiceDefinition,
    ) -> dict[str, Any]:
        agg_cpu: int = 0
        agg_memory: int = 0
        for container in service.containers:
            if container.resources:
                container_resources = self._convert_resources(
                    container.resources
                )
                if container_resources:
                    agg_cpu += int(container_resources.get("cpu", 0))
                    agg_memory += int(container_resources.get("memory", 0))
        cpu, memory = self._convert_fargate_cpu_memory(agg_cpu, agg_memory)
        return {
            "cpu": cpu,
            "memory": memory,
        }

    def _convert_resources(
        self,
        resources: ResourceRequirements | None,
    ) -> dict[str, Any] | None:
        if resources and resources.limits:
            if resources.limits.cpu and resources.limits.memory:
                cpu, memory = self._convert_fargate_cpu_memory(
                    int(float(resources.limits.cpu) * 1024),
                    int(resources.limits.memory),
                )
                return {
                    "cpu": cpu,
                    "memory": memory,
                }
        elif resources and resources.requests:
            if resources.requests.cpu and resources.requests.memory:
                cpu, memory = self._convert_fargate_cpu_memory(
                    int(float(resources.requests.cpu) * 1024),
                    int(resources.requests.memory),
                )
                return {
                    "cpu": cpu,
                    "memory": memory,
                }
        return None

    def _convert_fargate_cpu_memory(
        self, cpu: int, memory: int
    ) -> tuple[str, str]:
        CONFIGS = {
            256: [512, 1024, 2048],
            512: [1024, 2048, 3072, 4096],
            1024: [2048, 3072, 4096, 5120, 6144, 7168, 8192],
            2048: list(range(4096, 16385, 1024)),
            4096: list(range(8192, 30721, 1024)),
            8192: list(range(16384, 61441, 4096)),  # requires platform 1.4.0+
            16384: list(
                range(32768, 122881, 8192)
            ),  # requires platform 1.4.0+
        }

        for cpu_val in sorted(CONFIGS):
            if cpu <= cpu_val:
                for mem in CONFIGS[cpu_val]:
                    if memory <= mem:
                        return str(cpu_val), str(mem)
                raise ValueError(
                    f"Memory {memory}MiB is too large for {cpu_val} CPU"
                )

        raise ValueError(f"Unsupported CPU value: {cpu}")

    def _convert_depends_on(
        self,
        container_name: str,
        service: ServiceDefinition,
    ) -> list[dict[str, str]]:
        init_containers: list[str] = []
        for container in service.containers:
            if container.type == "init":
                init_containers.append(container.name)
        if container_name in init_containers:
            return []
        return [
            {"containerName": name, "condition": "COMPLETE"}
            for name in init_containers
        ]

    def _convert_ports(
        self,
        ports: list[Port],
        network_mode: Literal["awsvpc", "bridge", "host"],
    ) -> list[dict]:
        mappings: list[dict] = []
        for port in ports:
            mapping: dict[str, Any] = {
                "containerPort": port.container_port,
            }
            if port.protocol:
                mapping["protocol"] = port.protocol.upper()
            if network_mode in ("bridge", "host"):
                mapping["hostPort"] = port.host_port or 0
            elif port.host_port:
                mapping["hostPort"] = port.host_port
            mappings.append(mapping)
        return mappings

    def _convert_env(
        self,
        env: list[EnvVar],
    ) -> list[dict]:
        return [{"name": var.name, "value": str(var.value)} for var in env]

    def _convert_volumes(self, volumes: list[Volume]) -> list[dict[str, Any]]:
        task_volumes = []
        for volume in volumes:
            if volume.type == "emptyDir":
                name = volume.name
                task_volumes.append({"name": name})
        return task_volumes

    def _convert_volume_mounts(
        self,
        volume_mounts: list[VolumeMount],
    ) -> list[dict]:
        mounts = []
        for mount in volume_mounts:
            if mount.sub_path:
                print(
                    (
                        f"Warning: ECS Fargate does not support sub_path "
                        f"(ignored): {mount.sub_path}"
                    )
                )
            mounts.append(
                {
                    "sourceVolume": mount.name,
                    "containerPath": mount.mount_path,
                    "readOnly": mount.read_only,
                }
            )
        return mounts

    def _convert_probes_to_healthcheck(
        self, probes: ProbeSet | None
    ) -> dict | None:
        if not probes:
            return None
        probe = (
            probes.liveness_probe
            or probes.readiness_probe
            or probes.startup_probe
        )
        if not probe:
            return None
        interval = max(5, (probe.period_seconds or 30))
        timeout = max(2, (probe.timeout_seconds or 5))
        retries = max(1, (probe.failure_threshold or 3))
        start_period = max(0, (probe.initial_delay_seconds or 0))
        if probe.http_get:
            h = probe.http_get
            port = h.port or 80
            path = h.path or "/"
            host = h.host or "127.0.0.1"
            scheme = (h.scheme or "HTTP").lower()
            cmd = [
                "CMD-SHELL",
                f"curl -fsS {scheme}://{host}:{port}{path} || exit 1",
            ]
        elif probe.tcp_socket:
            s = probe.tcp_socket
            host = s.host or "127.0.0.1"
            port = s.port or 80
            cmd = ["CMD-SHELL", f"bash -c '</dev/tcp/{host}/{port}' || exit 1"]
        else:
            return None
        return {
            "command": cmd,
            "interval": int(interval),
            "timeout": int(timeout),
            "retries": int(retries),
            "startPeriod": int(start_period),
        }

    def _convert_scale_to_desired_count(self, scale: Scale | None) -> int:
        if not scale:
            return 1
        if scale.mode == "auto":
            return scale.min_replicas or 1
        return scale.replicas or 1

    def convert_scalable_target(
        self,
        cluster_name: str,
        service_name: str,
        scale: Scale,
    ) -> dict[str, Any]:
        if scale.mode != "auto":
            return {}

        return {
            "ServiceNamespace": "ecs",
            "ResourceId": f"service/{cluster_name}/{service_name}",
            "ScalableDimension": "ecs:service:DesiredCount",
            "MinCapacity": scale.min_replicas or 1,
            "MaxCapacity": scale.max_replicas,
        }

    def convert_scale_rule_to_policy(
        self,
        cluster_name: str,
        service_name: str,
        scale: Scale,
        rule: ScaleRule,
    ) -> dict[str, Any] | None:
        if rule.metadata is None:
            raise BadRequestError(
                "Scale rule metadata must be provided for ECS Fargate."
            )
        config: dict[str, Any] = {
            "PolicyName": rule.name,
            "ServiceNamespace": "ecs",
            "ResourceId": f"service/{cluster_name}/{service_name}",
            "ScalableDimension": "ecs:service:DesiredCount",
            "PolicyType": "TargetTrackingScaling",
            "TargetTrackingScalingPolicyConfiguration": {
                "TargetValue": float(rule.metadata["targetValue"]),
                "ScaleInCooldown": scale.cooldown_period or 60,
                "ScaleOutCooldown": scale.cooldown_period or 60,
            },
        }

        if rule.type == "cpu":
            config["TargetTrackingScalingPolicyConfiguration"][
                "PredefinedMetricSpecification"
            ] = {"PredefinedMetricType": "ECSServiceAverageCPUUtilization"}
        elif rule.type == "memory":
            config["TargetTrackingScalingPolicyConfiguration"][
                "PredefinedMetricSpecification"
            ] = {"PredefinedMetricType": "ECSServiceAverageMemoryUtilization"}
        elif rule.type == "custom":
            # Requires `custom_metric_name` and optional `namespace`,
            # `dimensions`, etc.
            config["TargetTrackingScalingPolicyConfiguration"][
                "CustomizedMetricSpecification"
            ] = {
                "MetricName": rule.metadata["metricName"],
                "Namespace": rule.metadata.get("namespace", "Custom/ECS"),
                "Statistic": rule.metadata.get("statistic", "Average"),
                "Unit": rule.metadata.get("unit", "None"),
                "Dimensions": [
                    {
                        "Name": k,
                        "Value": v,
                    }
                    for k, v in rule.metadata.get("dimensions", {}).items()
                ],
            }
        else:
            raise ValueError(f"Unsupported rule type: {rule.type}")

        return config


class ResultConverter:
    def convert_service_item(
        self,
        service: dict[str, Any],
        task: dict[str, Any],
        scale: Scale,
        endpoint: dict | None,
        traffic: list[TrafficAllocation],
        latest_created_revision: str | None,
    ) -> ServiceItem:
        return ServiceItem(
            name=service["serviceName"],
            uri=endpoint.get("uri") if endpoint else None,
            service=self.convert_service(
                service,
                task,
                scale,
                endpoint,
                traffic,
                latest_created_revision,
            ),
        )

    def convert_service(
        self,
        service: dict[str, Any],
        task: dict[str, Any],
        scale: Scale,
        endpoint: dict | None,
        traffic: list[TrafficAllocation],
        latest_created_revision: str | None,
    ) -> ServiceDefinition:
        containers = self.convert_containers(
            task.get("containerDefinitions", [])
        )
        volumes = self._convert_volumes(task.get("volumes", []))
        desired_count = service.get("desiredCount", 1)
        if scale.mode == "manual":
            scale.replicas = desired_count
        latest_ready_revision = None
        for traffic_item in traffic:
            if traffic_item.percent == 100:
                latest_ready_revision = traffic_item.revision
        return ServiceDefinition(
            name=service["serviceName"],
            containers=containers,
            volumes=volumes,
            ingress=self._convert_ingress(service, endpoint),
            scale=scale,
            traffic=traffic,
            latest_created_revision=latest_created_revision,
            latest_ready_revision=latest_ready_revision,
        )

    def convert_revision(
        self,
        task_id: str,
        task: dict[str, Any],
        current: bool,
    ) -> Revision:
        containers = self.convert_containers(
            task.get("containerDefinitions", [])
        )
        volumes = self._convert_volumes(task.get("volumes", []))
        registered_at = task.get("registeredAt")
        created_time = (
            registered_at.timestamp()
            if isinstance(registered_at, datetime)
            else None
        )
        return Revision(
            name=task_id,
            traffic=100 if current else 0,
            created_time=created_time,
            status=task.get("status"),
            active=current,
            containers=containers,
            volumes=volumes,
        )

    def _convert_ingress(
        self, service: dict[str, Any], endpoint: dict | None
    ) -> Ingress:
        if endpoint is None:
            return Ingress(external=False)
        lb = service.get("loadBalancers", [{}])[0]
        containerPort = lb.get("containerPort", 80)
        port = endpoint.get("port", 80)
        scheme = endpoint.get("scheme", "http").lower()
        return Ingress(
            external=True,
            target_port=containerPort,
            port=port,
            transport=scheme,
        )

    def convert_containers(
        self,
        containers: list[dict[str, Any]],
    ) -> list[Container]:
        result = [self.convert_container(c) for c in containers]
        init_containers = []
        for container in containers:
            depends_on = container.get("dependsOn", [])
            for dep in depends_on:
                name = dep.get("containerName")
                if name and name not in init_containers:
                    init_containers.append(name)
        for result_container in result:
            if result_container.name in init_containers:
                result_container.type = "init"
            else:
                result_container.type = "main"
        return result

    def convert_container(self, container: dict[str, Any]) -> Container:
        return Container(
            name=container.get("name", "main"),
            image=container.get("image", ""),
            command=container.get("entryPoint", None),
            args=container.get("command", None),
            working_dir=container.get("workingDirectory", None),
            ports=self._convert_ports(container.get("portMappings", [])),
            env=self._convert_env(container.get("environment", [])),
            resources=self._convert_resources(
                container.get("cpu"), container.get("memory")
            ),
            probes=self._convert_healthcheck_to_probes(
                container.get("healthCheck")
            ),
            volume_mounts=self._convert_volume_mounts(
                container.get("mountPoints", [])
            ),
        )

    def _convert_ports(
        self,
        port_mappings: list[dict],
    ) -> list[Port]:
        result: list[Port] = []
        for pm in port_mappings or []:
            container_port = pm.get("containerPort")
            if container_port:
                port_obj = Port(
                    container_port=container_port,
                    name=pm.get("name", None),
                    protocol=pm.get("protocol", "TCP"),
                    host_port=pm.get("hostPort", None),
                )
                result.append(port_obj)
        return result

    def _convert_env(
        self,
        env_list: list[dict],
    ) -> list[EnvVar]:
        result: list[EnvVar] = []
        for env_item in env_list or []:
            name = env_item.get("name")
            value = env_item.get("value")
            if name:
                result.append(EnvVar(name=name, value=value))
        return result

    def _convert_volumes(
        self,
        volumes: list[dict[str, Any]],
    ) -> list[Volume]:
        result: list[Volume] = []
        for vol in volumes or []:
            name = vol.get("name")
            if name:
                result.append(Volume(name=name, type="emptyDir"))
        return result

    def _convert_volume_mounts(
        self,
        mounts: list[dict],
    ) -> list[VolumeMount]:
        result: list[VolumeMount] = []
        for mp in mounts or []:
            name = mp.get("sourceVolume")
            container_path = mp.get("containerPath")
            read_only = bool(mp.get("readOnly", False))
            if name and container_path:
                result.append(
                    VolumeMount(
                        name=name,
                        mount_path=container_path,
                        read_only=read_only,
                        sub_path=None,
                    )
                )
        return result

    def _convert_resources(
        self, cpu: str | None, memory: str | None
    ) -> ResourceRequirements | None:
        if not cpu or not memory:
            return None
        cpu_cores, mem_mib = self._convert_cpu_memory(cpu, memory)
        return ResourceRequirements(
            requests=ResourceRequests(
                cpu=cpu_cores,
                memory=mem_mib,
            ),
            limits=ResourceLimits(
                cpu=cpu_cores,
                memory=mem_mib,
            ),
        )

    def _convert_cpu_memory(self, cpu: str, memory: str) -> tuple[float, int]:
        cpu_units = int(cpu)
        mem_mib = int(memory)
        cores_map = {
            256: 0.25,
            512: 0.5,
            1024: 1.0,
            2048: 2.0,
            4096: 4.0,
            8192: 8.0,
            16384: 16.0,
        }
        cpu_cores = cores_map.get(cpu_units, cpu_units / 1024.0)
        return cpu_cores, mem_mib

    def _convert_healthcheck_to_probes(
        self,
        healthcheck: dict | None,
    ) -> ProbeSet | None:
        if not healthcheck:
            return None

        # Map timing fields back
        period_seconds = int(healthcheck.get("interval", 30))
        timeout_seconds = int(healthcheck.get("timeout", 5))
        failure_threshold = int(healthcheck.get("retries", 3))
        initial_delay_seconds = int(healthcheck.get("startPeriod", 0))

        # Parse the command
        cmd = healthcheck.get("command")
        if not cmd:
            return None

        # Normalize to a shell string we can parse
        if isinstance(cmd, list):
            # e.g. ["CMD-SHELL", "curl -fsS http://127.0.0.1:80/ || exit 1"]
            if cmd and cmd[0] == "CMD-SHELL":
                shell = " ".join(cmd[1:]).strip()
            else:
                shell = " ".join(cmd).strip()
        else:
            # string form
            shell = str(cmd).strip()

        probe = None

        import re

        m = re.search(
            r"""curl\s+.*?\s+
                (?P<scheme>https?)://
                (?P<host>[^:/\s]+)
                (?::(?P<port>\d+))?
                (?P<path>/\S*)?
            """,
            shell,
            flags=re.IGNORECASE | re.VERBOSE,
        )
        if m:
            scheme = m.group("scheme").upper() if m.group("scheme") else "HTTP"
            host = m.group("host") or "127.0.0.1"
            port = int(m.group("port") or 80)
            path = m.group("path") or "/"
            probe = Probe(
                http_get=HTTPGetAction(
                    host=host,
                    port=port,
                    path=path,
                    scheme=scheme,
                ),
                tcp_socket=None,
                period_seconds=period_seconds,
                timeout_seconds=timeout_seconds,
                failure_threshold=failure_threshold,
                initial_delay_seconds=initial_delay_seconds,
            )

        # Fallback: TCP form  bash -c '</dev/tcp/host/port' || exit 1
        if probe is None:
            m = re.search(
                r"</dev/tcp/(?P<host>[^/]+)/(?P<port>\d+)>",
                shell,
                flags=re.IGNORECASE,
            )
            if m:
                host = m.group("host") or "127.0.0.1"
                port = int(m.group("port") or 80)
                probe = Probe(
                    http_get=None,
                    tcp_socket=TCPSocketAction(
                        host=host,
                        port=port,
                    ),
                    period_seconds=period_seconds,
                    timeout_seconds=timeout_seconds,
                    failure_threshold=failure_threshold,
                    initial_delay_seconds=initial_delay_seconds,
                )

        if probe is None:
            # Unknown command pattern — nothing to reconstruct
            return None

        return ProbeSet(
            liveness_probe=probe,
            readiness_probe=None,
            startup_probe=None,
        )

    def convert_scalable_target_to_scale(
        self,
        target: dict[str, Any],
    ) -> Scale:
        ns = target.get("ServiceNamespace")
        dim = target.get("ScalableDimension")
        is_ecs_desired = ns == "ecs" and dim == "ecs:service:DesiredCount"

        mode: Literal["manual", "auto"] = (
            "auto" if is_ecs_desired else "manual"
        )
        min_replicas = target.get("MinCapacity")
        max_replicas = target.get("MaxCapacity")

        return Scale(
            mode=mode,
            min_replicas=min_replicas,
            max_replicas=max_replicas,
        )

    def get_cooldown_period(
        self,
        policy: dict[str, Any],
    ) -> int:
        cooldown = policy.get(
            "TargetTrackingScalingPolicyConfiguration", {}
        ).get("ScaleInCooldown", None) or policy.get(
            "TargetTrackingScalingPolicyConfiguration", {}
        ).get(
            "ScaleOutCooldown", None
        )
        return cooldown if cooldown is not None else 60

    def convert_scaling_policy_to_scale_rule(
        self,
        policy: dict[str, Any],
    ) -> ScaleRule | None:
        if (
            policy.get("PolicyType") != "TargetTrackingScaling"
            or "TargetTrackingScalingPolicyConfiguration" not in policy
        ):
            return None

        config = policy["TargetTrackingScalingPolicyConfiguration"]
        metadata: dict[str, Any] = {
            "targetValue": config["TargetValue"],
        }

        # Determine rule type
        rule_type = None
        if "PredefinedMetricSpecification" in config:
            metric_type = config["PredefinedMetricSpecification"][
                "PredefinedMetricType"
            ]
            if metric_type == "ECSServiceAverageCPUUtilization":
                rule_type = "cpu"
            elif metric_type == "ECSServiceAverageMemoryUtilization":
                rule_type = "memory"
            else:
                raise ValueError(
                    f"Unsupported predefined metric type: {metric_type}"
                )
        elif "CustomizedMetricSpecification" in config:
            cms = config["CustomizedMetricSpecification"]
            rule_type = "custom"
            metadata.update(
                {
                    "metricName": cms["MetricName"],
                    "namespace": cms.get("Namespace", "Custom/ECS"),
                    "statistic": cms.get("Statistic", "Average"),
                    "unit": cms.get("Unit", "None"),
                    "dimensions": {
                        d["Name"]: d["Value"]
                        for d in cms.get("Dimensions", [])
                    },
                }
            )
        else:
            raise ValueError("No metric specification found in policy.")

        rule = ScaleRule(
            name=policy.get("PolicyName", ""),
            type=rule_type,
            metadata=metadata,
        )
        return rule
