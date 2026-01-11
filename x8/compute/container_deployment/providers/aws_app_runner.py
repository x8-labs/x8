"""
AWS App Runner deployment.
"""

from __future__ import annotations

__all__ = ["AWSAppRunner"]

import json
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, OperationParser, Response, RunContext
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import Expression

from .._feature import ContainerDeploymentFeature
from .._helper import map_images, requires_container_registry
from .._models import (
    Container,
    EnvVar,
    HTTPGetAction,
    Ingress,
    Port,
    Probe,
    ProbeSet,
    ResourceRequests,
    ResourceRequirements,
    Revision,
    Scale,
    ServiceDefinition,
    ServiceItem,
    ServiceOverlay,
    TCPSocketAction,
    TrafficAllocation,
)
from ._base import BaseContainerDeploymentProvider


class AWSAppRunner(BaseContainerDeploymentProvider):
    region: str
    service_name: str | None

    ecr_connection_arn: str | None = None
    auto_scaling_configuration_arn: str | None = None
    vpc_connector_arn: str | None = None
    authentication_configuration_access_role_arn: str | None = None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    profile_name: str | None
    nparams: dict[str, Any]

    _client: Any
    _iam_client: Any
    _init: bool = False
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        region: str = "us-west-2",
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        ecr_connection_arn: str | None = None,
        auto_scaling_configuration_arn: str | None = None,
        vpc_connector_arn: str | None = None,
        authentication_configuration_access_role_arn: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        profile_name: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize AWS App Runner deployment provider.

        Args:
            region: AWS region for the App Runner service.
            service_name: The name of the App Runner service.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            ecr_connection_arn: ARN of the ECR connection (if any).
            auto_scaling_configuration_arn:
                ARN of the auto scaling configuration (if any).
            vpc_connector_arn: ARN of the VPC connector to use (if any).
            authentication_configuration_access_role_arn:
                ARN of the IAM role for App Runner authentication.
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            aws_session_token: AWS session token.
            profile_name: AWS profile name to use.
            nparams: Additional parameters for the AWS client.
        """
        self.region = region
        self.service_name = service_name
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry
        self.ecr_connection_arn = ecr_connection_arn
        self.auto_scaling_configuration_arn = auto_scaling_configuration_arn
        self.vpc_connector_arn = vpc_connector_arn
        self.authentication_configuration_access_role_arn = (
            authentication_configuration_access_role_arn
        )
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.profile_name = profile_name
        self.nparams = nparams

        self._init = False
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()

        super().__init__(**kwargs)

    def __supports__(self, feature):
        return feature not in [
            ContainerDeploymentFeature.MULTIPLE_REVISIONS,
            ContainerDeploymentFeature.REVISION_DELETE,
            ContainerDeploymentFeature.MULTIPLE_CONTAINERS,
        ]

    def __setup__(self, context: Context | None = None) -> None:
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
        self._client = session.client(
            "apprunner", region_name=self.region, **self.nparams
        )
        self._iam_client = session.client(
            "iam", region_name=self.region, **self.nparams
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
        app_runner_service_name = self._get_service_name(service_def)
        ecr_connection_arn = self.ecr_connection_arn
        auto_scaling_configuration_arn = self.auto_scaling_configuration_arn

        app_runner_service = self._get_service_by_name(app_runner_service_name)

        if app_runner_service:
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {app_runner_service_name} already exists."
                )

            # Update existing service
            images = map_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service=service_def
                ),
                run_context=run_context,
            )

            if not auto_scaling_configuration_arn:
                auto_scaling_configuration_arn = (
                    self._get_or_create_auto_scaling_configuration(
                        service_name=app_runner_service_name,
                        scale=service_def.scale,
                    )
                )

            update_params = self._op_converter.convert_service_update(
                service_arn=app_runner_service["ServiceArn"],
                service=service_def,
                images=images,
                auto_scaling_config_arn=auto_scaling_configuration_arn,
                ecr_connection_arn=ecr_connection_arn,
                vpc_connector_arn=self.vpc_connector_arn,
            )

            response = self._client.update_service(**update_params)
            operation_id = response["OperationId"]

            # Wait for operation to complete
            self._wait_for_creation(
                operation_id, response["Service"]["ServiceArn"]
            )

            # Get updated service
            updated_service = self._client.describe_service(
                ServiceArn=response["Service"]["ServiceArn"]
            )
            result = self._result_converter.convert_service_item(
                updated_service["Service"]
            )
            print(f"Service updated successfully: {result.uri}")
        else:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {app_runner_service_name} not found."
                )

            # Create new service
            images = map_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service=service_def
                ),
                run_context=run_context,
            )

            if not auto_scaling_configuration_arn:
                auto_scaling_configuration_arn = (
                    self._get_or_create_auto_scaling_configuration(
                        service_name=app_runner_service_name,
                        scale=service_def.scale,
                    )
                )

            authentication_configuration_access_role_arn = (
                self.authentication_configuration_access_role_arn
            )
            if not authentication_configuration_access_role_arn:
                authentication_configuration_access_role_arn = (
                    self._create_ecr_access_role()
                )
            create_params = self._op_converter.convert_service_create(
                service_name=app_runner_service_name,
                service=service_def,
                images=images,
                auto_scaling_config_arn=auto_scaling_configuration_arn,
                ecr_connection_arn=ecr_connection_arn,
                vpc_connector_arn=self.vpc_connector_arn,
                authentication_configuration_access_role_arn=authentication_configuration_access_role_arn,  # noqa
            )

            response = self._client.create_service(**create_params)
            operation_id = response["OperationId"]

            # Wait for operation to complete
            self._wait_for_creation(
                operation_id, response["Service"]["ServiceArn"]
            )

            # Get created service
            created_service = self._client.describe_service(
                ServiceArn=response["Service"]["ServiceArn"]
            )
            result = self._result_converter.convert_service_item(
                created_service["Service"]
            )
            print(f"Service created successfully: {result.uri}")

        return Response(result=result)

    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        return self.create_service(
            service=service,
            where=where,
            run_context=run_context,
            **kwargs,
        )

    def get_service(self, name: str, **kwargs: Any) -> Response[ServiceItem]:
        self.__setup__()
        app_runner_service = self._get_service_by_name(name)
        if not app_runner_service:
            raise NotFoundError(f"Service {name} not found.")
        try:
            response = self._client.describe_service(
                ServiceArn=app_runner_service["ServiceArn"]
            )
            result = self._result_converter.convert_service_item(
                response["Service"]
            )
            return Response(result=result)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise NotFoundError(f"Service {name} not found.")
            raise

    def delete_service(self, name: str, **kwargs: Any) -> Response[None]:
        self.__setup__()
        app_runner_service = self._get_service_by_name(name)
        if not app_runner_service:
            raise NotFoundError(f"Service {name} not found.")
        try:
            _ = self._client.delete_service(
                ServiceArn=app_runner_service["ServiceArn"]
            )
            # Wait for deletion to complete
            self._wait_for_deletion(app_runner_service["ServiceArn"])
            if not self.auto_scaling_configuration_arn:
                self._delete_auto_scaling_configuration(name)
            print(f"Service {name} deleted successfully.")
            return Response(result=None)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise NotFoundError(f"Service {name} not found.")
            raise

    def list_services(self, **kwargs: Any) -> Response[list[ServiceItem]]:
        self.__setup__()
        response = self._client.list_services()
        services = []

        for service_summary in response.get("ServiceSummaryList", []):
            # Get detailed service information
            detailed_response = self._client.describe_service(
                ServiceArn=service_summary["ServiceArn"]
            )
            services.append(detailed_response["Service"])

        result = self._result_converter.convert_services(services)
        return Response(result=result)

    def list_revisions(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[list[Revision]]:
        self.__setup__()
        app_runner_service = self._get_service_by_name(name)
        if not app_runner_service:
            raise NotFoundError(f"Service {name} not found.")
        try:
            response = self._client.describe_service(
                ServiceArn=app_runner_service["ServiceArn"]
            )
            result = self._result_converter.convert_service_item(
                response["Service"]
            )
            revision = Revision(
                name="latest",
                traffic=100,
                containers=result.service.containers if result.service else [],
                volumes=result.service.volumes if result.service else [],
                created_time=None,
                active=True,
                status="running",
            )
            return Response(result=[revision])
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise NotFoundError(f"Service {name} not found.")
            raise

    def get_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        self.__setup__()
        app_runner_service = self._get_service_by_name(name)
        if not app_runner_service:
            raise NotFoundError(f"Service {name} not found.")
        try:
            response = self._client.describe_service(
                ServiceArn=app_runner_service["ServiceArn"]
            )
            result = self._result_converter.convert_service_item(
                response["Service"]
            )
            return Response(
                result=Revision(
                    name="latest",
                    traffic=100,
                    containers=(
                        result.service.containers if result.service else []
                    ),
                    volumes=result.service.volumes if result.service else [],
                    created_time=None,
                    active=True,
                    status="running",
                )
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise NotFoundError(f"Service {name} not found.")
            raise

    def delete_revision(self, name: str, revision: str) -> Response[None]:
        self.__setup__()
        app_runner_service = self._get_service_by_name(name)
        if not app_runner_service:
            raise NotFoundError(f"Service {name} not found.")
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        try:
            self._client.describe_service(
                ServiceArn=app_runner_service["ServiceArn"]
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                raise NotFoundError(f"Service {name} not found.")
            raise
        raise PreconditionFailedError(
            (
                f"Cannot delete active revision {revision} "
                f"of service {name}."
            )
        )

    def update_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
    ) -> Response[ServiceItem]:
        return self.get_service(name)

    def close(self) -> None:
        self._init = False

    def _get_service_name(self, service: ServiceDefinition) -> str:
        if self.service_name:
            return self.service_name
        if service.name:
            return service.name
        raise BadRequestError(
            "Service name must be provided or defined in the service."
        )

    def _get_service_by_name(self, name: str) -> dict[str, Any] | None:
        response = self._client.list_services()
        services = response.get("ServiceSummaryList", [])
        for service in services:
            if service["ServiceName"] == name:
                return service
        return None

    def _wait_for_creation(self, operation_id: str, service_arn: str) -> None:
        max_attempts = 60
        delay = 5  # seconds
        for attempt in range(max_attempts):
            response = self._client.describe_service(ServiceArn=service_arn)
            status = response["Service"]["Status"]
            if status in ("RUNNING", "PAUSED"):
                return
            elif status in ("FAILED", "DELETED"):
                raise RuntimeError(
                    f"Service operation failed with status: {status}"
                )
            time.sleep(delay)

    def _wait_for_deletion(self, service_arn: str) -> None:
        max_attempts = 60
        delay = 5  # seconds

        for attempt in range(max_attempts):
            try:
                response = self._client.describe_service(
                    ServiceArn=service_arn
                )
                status = response["Service"]["Status"]
                if status == "DELETED":
                    return
            except ClientError as e:
                if e.response["Error"]["Code"] == "ResourceNotFoundException":
                    return
                raise

            time.sleep(delay)

    def _get_or_create_ecr_connection(self) -> str:
        connection_name = "default-ecr-connection"
        arn = self._get_ecr_connection_arn()
        if arn:
            return arn

        try:
            response = self._client.create_connection(
                ConnectionName=connection_name,
                ProviderType="ECR",
            )
            connection_arn = response["Connection"]["ConnectionArn"]
            return connection_arn
        except ClientError as e:
            raise RuntimeError(f"Failed to create connection: {e}")

    def _get_ecr_connection_arn(self) -> str | None:
        connections = self._client.list_connections()
        for conn in connections["ConnectionSummaryList"]:
            if conn["ProviderType"] == "ECR":
                return conn["ConnectionArn"]
        return None

    def _get_or_create_auto_scaling_configuration(
        self,
        service_name: str,
        scale: Scale | None = None,
    ) -> str:
        max_concurrency = (
            scale.max_concurrency if scale and scale.max_concurrency else 100
        )
        min_size = scale.min_replicas if scale and scale.min_replicas else 1
        max_size = scale.max_replicas if scale and scale.max_replicas else 1
        existing_arn = self._get_active_auto_scaling_config_arn(
            service_name, max_concurrency, min_size, max_size
        )
        if existing_arn:
            return existing_arn

        response = self._client.create_auto_scaling_configuration(
            AutoScalingConfigurationName=service_name,
            MaxConcurrency=max_concurrency,
            MinSize=min_size,
            MaxSize=max_size,
        )
        arn = response["AutoScalingConfiguration"][
            "AutoScalingConfigurationArn"
        ]
        return arn

    def _get_active_auto_scaling_config_arn(
        self,
        name: str,
        max_concurrency: int,
        min_size: int,
        max_size: int,
    ) -> str | None:
        configs = self._client.list_auto_scaling_configurations()
        for config in configs["AutoScalingConfigurationSummaryList"]:
            if config["AutoScalingConfigurationName"] == name:
                full_config = self._client.describe_auto_scaling_configuration(
                    AutoScalingConfigurationArn=config[
                        "AutoScalingConfigurationArn"
                    ]
                )["AutoScalingConfiguration"]
                if (
                    full_config["MaxConcurrency"] == max_concurrency
                    and full_config["MinSize"] == min_size
                    and full_config["MaxSize"] == max_size
                ):
                    print("Found existing auto scaling configuration")
                    return config["AutoScalingConfigurationArn"]
        return None

    def _delete_auto_scaling_configuration(self, name: str) -> None:
        configs = self._client.list_auto_scaling_configurations()
        for config in configs["AutoScalingConfigurationSummaryList"]:
            if config["AutoScalingConfigurationName"] == name:
                self._client.delete_auto_scaling_configuration(
                    AutoScalingConfigurationArn=config[
                        "AutoScalingConfigurationArn"
                    ]
                )

    def _create_ecr_access_role(self) -> str:
        trust_policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"Service": "build.apprunner.amazonaws.com"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
        role_name = "AppRunnerECRAccessRole"
        try:
            role_response = self._iam_client.create_role(
                RoleName="AppRunnerECRAccessRole",
                AssumeRolePolicyDocument=json.dumps(trust_policy),
                Description="Role for App Runner to access ECR",
            )
            role_arn = role_response["Role"]["Arn"]
        except self._iam_client.exceptions.EntityAlreadyExistsException:
            # Role already exists, get its ARN
            role_response = self._iam_client.get_role(RoleName=role_name)
            role_arn = role_response["Role"]["Arn"]
        self._iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn="arn:aws:iam::aws:policy/service-role/AWSAppRunnerServicePolicyForECRAccess",  # noqa
        )
        return role_arn


class OperationConverter:
    def convert_service_create(
        self,
        service_name: str,
        service: ServiceDefinition,
        images: list[str],
        ecr_connection_arn: str | None = None,
        auto_scaling_config_arn: str | None = None,
        vpc_connector_arn: str | None = None,
        authentication_configuration_access_role_arn: str | None = None,
    ) -> dict[str, Any]:
        if len(service.containers) != 1:
            raise BadRequestError(
                "App Runner supports only one container per service"
            )

        container = service.containers[0]
        image = images[0]
        config = {
            "ServiceName": service_name,
            "SourceConfiguration": self._convert_source_configuration(
                container,
                image,
                ecr_connection_arn,
                authentication_configuration_access_role_arn,
            ),
            "InstanceConfiguration": self._convert_instance_configuration(
                container.resources,
            ),
        }

        config["NetworkConfiguration"] = self._convert_network_configuration(
            ingress=service.ingress,
            vpc_connector_arn=vpc_connector_arn,
        )

        if container.probes and container.probes.readiness_probe:
            config["HealthCheckConfiguration"] = (
                self._convert_health_check_configuration(
                    container.probes.readiness_probe
                )
            )

        if auto_scaling_config_arn:
            config["AutoScalingConfigurationArn"] = auto_scaling_config_arn

        return config

    def convert_service_update(
        self,
        service_arn: str,
        service: ServiceDefinition,
        images: list[str],
        ecr_connection_arn: str | None = None,
        auto_scaling_config_arn: str | None = None,
        vpc_connector_arn: str | None = None,
    ) -> dict[str, Any]:
        if len(service.containers) != 1:
            raise BadRequestError(
                "App Runner supports only one container per service"
            )

        container = service.containers[0]
        image = images[0]

        config = {
            "ServiceArn": service_arn,
            "SourceConfiguration": self._convert_source_configuration(
                container, image, ecr_connection_arn
            ),
            "NetworkConfiguration": self._convert_network_configuration(
                service.ingress, vpc_connector_arn
            ),
        }
        if auto_scaling_config_arn:
            config["AutoScalingConfigurationArn"] = auto_scaling_config_arn
        return config

    def _convert_source_configuration(
        self,
        container: Container,
        image: str,
        ecr_connection_arn: str | None = None,
        authentication_configuration_access_role_arn: str | None = None,
    ) -> dict[str, Any]:
        image_identifier = image
        if ":" not in image:
            image_identifier = f"{image}:latest"
        config: dict = {
            "ImageRepository": {
                "ImageRepositoryType": "ECR",
                "ImageIdentifier": image_identifier,
                "ImageConfiguration": self._convert_image_configuration(
                    container
                ),
            },
            "AutoDeploymentsEnabled": False,
        }
        if ecr_connection_arn:
            config["ImageRepository"][
                "ImageRepositoryConnectionArn"
            ] = ecr_connection_arn
        if authentication_configuration_access_role_arn:
            config["AuthenticationConfiguration"] = {
                "AccessRoleArn": authentication_configuration_access_role_arn
            }
        return config

    def _convert_image_configuration(
        self,
        container: Container,
    ) -> dict[str, Any]:
        config: dict[str, Any] = {}
        if container.ports:
            config["Port"] = str(container.ports[0].container_port)
        if container.command:
            if container.args:
                full_command = container.command + container.args
            else:
                full_command = container.command
            config["StartCommand"] = " ".join(full_command)
        if container.env:
            config["RuntimeEnvironmentVariables"] = {
                var.name: str(var.value) for var in container.env
            }
        return config

    def _convert_instance_configuration(
        self,
        resources: ResourceRequirements | None,
    ) -> dict[str, Any]:
        config: dict[str, Any] = {}
        if resources and resources.requests and resources.requests.cpu:
            cpu = f"{resources.requests.cpu} vCPU"
        else:
            cpu = "0.25 vCPU"

        if resources and resources.requests and resources.requests.memory:
            # convert memory mb to gb
            memory_gb = resources.requests.memory / 1024
        else:
            memory_gb = 0.5

        if memory_gb <= 0.5:
            memory = "0.5 GB"
        elif memory_gb <= 1:
            memory = "1 GB"
        elif memory_gb <= 2:
            memory = "2 GB"
        elif memory_gb <= 3:
            memory = "3 GB"
        elif memory_gb <= 4:
            memory = "4 GB"
        else:
            memory = "8 GB"

        config["Cpu"] = cpu
        config["Memory"] = memory
        return config

    def _convert_health_check_configuration(
        self,
        probe: Probe,
    ) -> dict[str, Any]:
        config: dict[str, Any] = {}

        if probe.http_get:
            config["Protocol"] = "HTTP"
            config["Path"] = probe.http_get.path or "/"
        elif probe.tcp_socket:
            config["Protocol"] = "TCP"
        else:
            config["Protocol"] = "HTTP"
            config["Path"] = "/"

        if probe.period_seconds is not None:
            config["Interval"] = probe.period_seconds
        if probe.timeout_seconds is not None:
            config["Timeout"] = probe.timeout_seconds
        if probe.success_threshold is not None:
            config["HealthyThreshold"] = probe.success_threshold
        if probe.failure_threshold is not None:
            config["UnhealthyThreshold"] = probe.failure_threshold

        return config

    def _convert_network_configuration(
        self,
        ingress: Ingress | None = None,
        vpc_connector_arn: str | None = None,
    ) -> dict[str, Any]:
        config: dict[str, Any] = {}
        if not ingress or ingress.external:
            config["EgressConfiguration"] = {"EgressType": "DEFAULT"}
        else:
            config["EgressConfiguration"] = {
                "EgressType": "VPC",
                "VpcConnectorArn": vpc_connector_arn,
            }
        return config


class ResultConverter:
    def convert_service_item(self, service: dict[str, Any]) -> ServiceItem:
        return ServiceItem(
            name=service["ServiceName"],
            uri=f"https://{service.get("ServiceUrl", "")}",
            service=self.convert_service(service),
        )

    def convert_service(self, service: dict[str, Any]) -> ServiceDefinition:
        # Extract container info from source configuration
        source_config = service.get("SourceConfiguration", {})
        image_repo = source_config.get("ImageRepository", {})
        image_config = image_repo.get("ImageConfiguration", {})
        heath_check_config = service.get("HealthCheckConfiguration", {})
        network_config = service.get("NetworkConfiguration", {})

        # Create container from service configuration
        container = Container(
            name=image_repo.get("ImageIdentifier", "")
            .split("/")[-1]
            .split(":")[0],
            type="main",
            image=image_repo.get("ImageIdentifier", ""),
            env=self._convert_env(
                image_config.get("RuntimeEnvironmentVariables", {})
            ),
            ports=self._convert_ports(image_config.get("Port", None)),
            resources=self._convert_resources(
                service.get("InstanceConfiguration", {})
            ),
            probes=self._convert_probes(
                heath_check_config, image_config.get("Port", None)
            ),
        )

        return ServiceDefinition(
            name=service["ServiceName"],
            containers=[container],
            ingress=self._convert_ingress(network_config),
            latest_ready_revision="latest",
            latest_created_revision="latest",
            traffic=[TrafficAllocation(revision="latest", percent=100)],
        )

    def _convert_ingress(
        self, network_config: dict[str, Any]
    ) -> Ingress | None:
        egress_config = network_config.get("EgressConfiguration", {})
        if egress_config.get("EgressType") == "VPC":
            return Ingress(
                external=False,
            )
        return Ingress(external=True)

    def _convert_probes(
        self,
        health_check_config: dict[str, Any],
        port: str | None = None,
    ) -> ProbeSet | None:
        if not health_check_config:
            return None

        readiness_probe = Probe(
            period_seconds=health_check_config.get("Interval", None),
            timeout_seconds=health_check_config.get("Timeout", None),
            success_threshold=health_check_config.get(
                "HealthyThreshold", None
            ),
            failure_threshold=health_check_config.get(
                "UnhealthyThreshold", None
            ),
        )
        if health_check_config.get("Protocol") == "HTTP":
            readiness_probe.http_get = HTTPGetAction(
                path=health_check_config.get("Path", "/"),
            )
        elif health_check_config.get("Protocol") == "TCP":
            readiness_probe.tcp_socket = TCPSocketAction(
                port=health_check_config.get("Port", port or "80")
            )

        probes = ProbeSet(readiness_probe=readiness_probe)
        return probes

    def _convert_env(self, env_vars: dict[str, str]) -> list[EnvVar]:
        return [
            EnvVar(name=name, value=value) for name, value in env_vars.items()
        ]

    def _convert_ports(self, port: str | None) -> list:
        if not port:
            return []
        return [Port(container_port=int(port), protocol="TCP")]

    def _convert_resources(
        self, instance_config: dict[str, Any]
    ) -> ResourceRequirements | None:
        if not instance_config:
            return None

        cpu_str = instance_config.get("Cpu", "0.25 vCPU")
        memory_str = instance_config.get("Memory", "0.5 GB")

        cpu = None
        memory = None
        if cpu_str.endswith(" vCPU"):
            cpu_value = cpu_str[:-5]
            cpu = float(cpu_value)

        if memory_str.endswith(" GB"):
            memory_value = float(memory_str[:-3])
            memory = int(memory_value * 1024)

        return ResourceRequirements(
            requests=ResourceRequests(cpu=cpu, memory=memory)
        )

    def convert_services(
        self, services: list[dict[str, Any]]
    ) -> list[ServiceItem]:
        return [self.convert_service_item(service) for service in services]
