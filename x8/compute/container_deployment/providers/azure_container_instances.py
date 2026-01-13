"""
Azure Container Instances deployment.
"""

from __future__ import annotations

__all__ = ["AzureContainerInstances"]

from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.aio import (
    ContainerInstanceManagementClient as AContainerInstanceManagementClient,
)
from azure.mgmt.containerinstance.models import Container as ACIContainer
from azure.mgmt.containerinstance.models import (
    ContainerExec,
    ContainerGroup,
    ContainerGroupDiagnostics,
    ContainerGroupIpAddressType,
    ContainerGroupRestartPolicy,
    ContainerGroupSku,
    ContainerHttpGet,
    ContainerNetworkProtocol,
    ContainerPort,
    ContainerProbe,
    EnvironmentVariable,
)
from azure.mgmt.containerinstance.models import GpuResource as ACIGpuResource
from azure.mgmt.containerinstance.models import GpuSku
from azure.mgmt.containerinstance.models import HttpHeader as ACIHttpHeader
from azure.mgmt.containerinstance.models import (
    ImageRegistryCredential,
    InitContainerDefinition,
    IpAddress,
    LogAnalytics,
    OperatingSystemTypes,
)
from azure.mgmt.containerinstance.models import Port as ACIPort
from azure.mgmt.containerinstance.models import (
    ResourceLimits as ACIResourceLimits,
)
from azure.mgmt.containerinstance.models import (
    ResourceRequests as ACIResourceRequests,
)
from azure.mgmt.containerinstance.models import (
    ResourceRequirements as ACIResourceRequirements,
)
from azure.mgmt.containerinstance.models import (
    SecurityContextCapabilitiesDefinition,
    SecurityContextDefinition,
)
from azure.mgmt.containerinstance.models import Volume as ACIVolume
from azure.mgmt.containerinstance.models import VolumeMount as ACIVolumeMount

from x8._common.azure_provider import AzureProvider
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
from .._helper import amap_images, map_images, requires_container_registry
from .._models import (
    Container,
    ContainerRegistryCredentials,
    EnvVar,
    ExecAction,
    GPUResource,
    HTTPGetAction,
    HTTPHeader,
    Ingress,
    Port,
    Probe,
    ProbeSet,
    ResourceLimits,
    ResourceRequests,
    ResourceRequirements,
    Revision,
    SecurityContext,
    ServiceDefinition,
    ServiceItem,
    ServiceOverlay,
    TrafficAllocation,
    Volume,
    VolumeMount,
)
from ._base import BaseContainerDeploymentProvider


class AzureContainerInstances(AzureProvider, BaseContainerDeploymentProvider):
    subscription_id: str | None
    resource_group: str | None
    location: str
    container_group_name: str | None
    container_registry_credentials: list[ContainerRegistryCredentials] | None
    os_type: str
    sku: str | None
    log_analytics_workspace_id: str | None
    log_analytics_workspace_key: str | None
    nparams: dict[str, Any]

    _client: ContainerInstanceManagementClient
    _aclient: AContainerInstanceManagementClient
    _credential: Any
    _acredential: Any
    _init: bool = False
    _ainit: bool = False
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _cached_subscription_id: str | None = None

    def __init__(
        self,
        subscription_id: str | None = None,
        resource_group: str | None = None,
        location: str = "westus2",
        container_group_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        container_registry_credentials: (
            list[ContainerRegistryCredentials] | None
        ) = None,
        os_type: str = "Linux",
        sku: str | None = None,
        log_analytics_workspace_id: str | None = None,
        log_analytics_workspace_key: str | None = None,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            subscription_id: Azure subscription ID.
            resource_group: The resource group to deploy to.
            location: Azure region for the container instance.
            container_group_name: The name of the container group to deploy.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            container_registry_credentials:
                Credentials for the container registry.
            os_type: Operating system type for the container.
            sku: SKU for the container group.
            credential_type:
                Type of Azure credential to use. Options are:
                - default: DefaultAzureCredential
                - client_secret: ClientSecretCredential
                - certificate: CertificateCredential
                - azure_cli: AzureCliCredential
                - shared_token_cache: SharedTokenCacheCredential
                - managed_identity: ManagedIdentityCredential
            tenant_id: Tenant ID for Azure authentication.
            client_id: Client ID for Azure authentication.
            client_secret: Client secret for Azure authentication.
            certificate_path: Path to certificate file for authentication.
            nparams: Native params to Azure client.
        """
        self.subscription_id = subscription_id
        self.resource_group = resource_group
        self.location = location
        self.container_group_name = container_group_name
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry
        self.container_registry_credentials = container_registry_credentials
        self.os_type = os_type
        self.sku = sku
        self.log_analytics_workspace_id = log_analytics_workspace_id
        self.log_analytics_workspace_key = log_analytics_workspace_key
        self.nparams = nparams

        self._init = False
        self._ainit = False
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        self._credential = None
        self._acredential = None
        self._cached_subscription_id = None

        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __supports__(self, feature):
        return feature not in [
            ContainerDeploymentFeature.MULTIPLE_REVISIONS,
            ContainerDeploymentFeature.REVISION_DELETE,
        ]

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self._credential = self._get_credential()
        subscription_id = self._get_subscription_id()
        self._client = ContainerInstanceManagementClient(
            credential=self._credential,
            subscription_id=subscription_id,
            **self.nparams,
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        self._acredential = self._aget_credential()
        subscription_id = self._get_subscription_id()
        self._aclient = AContainerInstanceManagementClient(
            credential=self._acredential,
            subscription_id=subscription_id,
            **self.nparams,
        )
        self._ainit = True

    def _init_credentials(self):
        if not self._credential:
            self._credential = self._get_credential()

    def _ensure_container_registry(
        self, service: ServiceDefinition
    ) -> ContainerRegistry | None:
        container_registry = self._get_container_registry()
        if container_registry:
            return container_registry
        if not requires_container_registry(service):
            return None

        from x8.compute.container_registry.providers.azure_container_registry import (  # noqa
            AzureContainerRegistry,
        )

        acr_name = f"{self._get_container_group_name(service)}-acr".replace(
            "_", ""
        ).replace("-", "")
        acr = AzureContainerRegistry(
            name=acr_name,
            credential_type=self.credential_type,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            certificate_path=self.certificate_path,
        )
        acr.create_resource(
            resource_group=self._get_resource_group_name(
                self._get_container_group_name(service)
            ),
            location=self.location,
            subscription_id=self.subscription_id,
        )
        print(f"Created container registry: {acr_name}")
        return ContainerRegistry(__provider__=acr)

    def _check_delete_container_registry(self, name: str) -> None:
        container_registry = self._get_container_registry()
        if container_registry:
            return None

        from x8.compute.container_registry.providers.azure_container_registry import (  # noqa
            AzureContainerRegistry,
        )

        acr_name = f"{name}-acr".replace("_", "").replace("-", "")
        acr = AzureContainerRegistry(
            name=acr_name,
            credential_type=self.credential_type,
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret,
            certificate_path=self.certificate_path,
        )
        try:
            acr.get_resource(
                resource_group=self._get_resource_group_name(name),
                name=acr_name,
                subscription_id=self.subscription_id,
            )
            acr.delete_resource(
                resource_group=self._get_resource_group_name(name),
                subscription_id=self.subscription_id,
            )
            print(f"Deleted container registry: {acr_name}")
        except NotFoundError:
            pass

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
        container_group_name = self._get_container_group_name(service_def)
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_group_name=container_group_name
        )
        try:
            existing_service = self._client.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=container_group_name,
            )
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {container_group_name} already exists."
                )
            self._ensure_resource_group(
                resource_group=resource_group_name,
                location=self.location,
                subscription_id=subscription_id,
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
            image_registry_credentials = self._get_image_registry_credentials(
                subscription_id=subscription_id,
                images=images,
            )
            updated_service = self._op_converter.update_service(
                existing_service=existing_service,
                service=service_def,
                images=images,
                container_group_name=container_group_name,
                location=self.location,
                image_registry_credentials=image_registry_credentials,
                diagnostics=self._get_diagnostics(),
                os_type=self.os_type,
                sku=self.sku,
            )

            operation = self._client.container_groups.begin_create_or_update(
                resource_group_name=resource_group_name,
                container_group_name=container_group_name,
                container_group=updated_service,
            )
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {result.uri}")
        except ResourceNotFoundError:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {container_group_name} not found."
                )
            self._ensure_resource_group(
                resource_group=resource_group_name,
                location=self.location,
                subscription_id=subscription_id,
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
            image_registry_credentials = self._get_image_registry_credentials(
                subscription_id=subscription_id,
                images=images,
            )
            new_service = self._op_converter.convert_service(
                service=service_def,
                images=images,
                container_group_name=container_group_name,
                location=self.location,
                image_registry_credentials=image_registry_credentials,
                diagnostics=self._get_diagnostics(),
                os_type=self.os_type,
                sku=self.sku,
            )
            operation = self._client.container_groups.begin_create_or_update(
                resource_group_name=resource_group_name,
                container_group_name=container_group_name,
                container_group=new_service,
            )
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service created successfully: {result.uri}")
        return Response(result=result)

    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        where_exists = OperationParser.parse_where_exists(where)
        service_def = self._normalize_service_definition(service)
        container_group_name = self._get_container_group_name(service_def)
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_group_name=container_group_name
        )
        try:
            existing_service = await self._aclient.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=container_group_name,
            )
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {container_group_name} already exists."
                )
            images = await amap_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service_def
                ),
                run_context=run_context,
            )
            image_registry_credentials = self._get_image_registry_credentials(
                subscription_id=subscription_id, images=images
            )
            self._ensure_resource_group(
                resource_group=resource_group_name,
                location=self.location,
                subscription_id=subscription_id,
            )
            updated_service = self._op_converter.update_service(
                existing_service=existing_service,
                service=service_def,
                images=images,
                container_group_name=container_group_name,
                location=self.location,
                image_registry_credentials=image_registry_credentials,
                diagnostics=self._get_diagnostics(),
                os_type=self.os_type,
                sku=self.sku,
            )
            operation = (
                await self._aclient.container_groups.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    container_group_name=container_group_name,
                    container_group=updated_service,
                )
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {result.uri}")
        except ResourceNotFoundError:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {container_group_name} not found."
                )
            images = await amap_images(
                containers=service_def.containers,
                images=service_def.images,
                containerizer=self._get_containerizer(),
                container_registry=self._ensure_container_registry(
                    service_def
                ),
                run_context=run_context,
            )
            image_registry_credentials = self._get_image_registry_credentials(
                subscription_id=subscription_id, images=images
            )
            new_service = self._op_converter.convert_service(
                service=service_def,
                images=images,
                container_group_name=container_group_name,
                location=self.location,
                image_registry_credentials=image_registry_credentials,
                diagnostics=self._get_diagnostics(),
                os_type=self.os_type,
                sku=self.sku,
            )
            self._ensure_resource_group(
                resource_group=resource_group_name,
                location=self.location,
                subscription_id=subscription_id,
            )
            operation = (
                await self._aclient.container_groups.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    container_group_name=container_group_name,
                    container_group=new_service,
                )
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service created successfully: {result.uri}")
        return Response(result=result)

    def get_service(self, name: str, **kwargs: Any) -> Response[ServiceItem]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = self._client.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    async def aget_service(
        self, name: str, **kwargs: Any
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = await self._aclient.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    def delete_service(self, name: str, **kwargs: Any) -> Response[None]:
        self.__setup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            self._client.container_groups.begin_delete(
                resource_group_name=resource_group_name,
                container_group_name=name,
            ).result()
            print(f"Service {name} deleted successfully.")
            self._check_delete_container_registry(name)
            if not self.resource_group:
                self._delete_resource_group_if_empty(
                    subscription_id=subscription_id,
                    resource_group=resource_group_name,
                )
            return Response(result=None)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    async def adelete_service(
        self, name: str, **kwargs: Any
    ) -> Response[None]:
        await self.__asetup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            operation = await self._aclient.container_groups.begin_delete(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            await operation.result()
            print(f"Service {name} deleted successfully.")
            self._check_delete_container_registry(name)
            if not self.resource_group:
                self._delete_resource_group_if_empty(
                    subscription_id=subscription_id,
                    resource_group=resource_group_name,
                )
            return Response(result=None)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    def list_services(self, **kwargs: Any) -> Response[list[ServiceItem]]:
        self.__setup__()
        response = self._client.container_groups.list()
        result = self._result_converter.convert_services(
            services=list(response)
        )
        return Response(result=result)

    async def alist_services(
        self, **kwargs: Any
    ) -> Response[list[ServiceItem]]:
        await self.__asetup__()
        response = self._aclient.container_groups.list()
        result = self._result_converter.convert_services(
            services=[service async for service in response]
        )
        return Response(result=result)

    def list_revisions(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[list[Revision]]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = self._client.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
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
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    async def alist_revisions(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[list[Revision]]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = await self._aclient.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
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
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    def get_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = self._client.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
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
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    async def aget_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        try:
            response = await self._aclient.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
            result = self._result_converter.convert_service_item(response)
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
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    def delete_revision(self, name: str, revision: str) -> Response[None]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        try:
            self._client.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )
        raise PreconditionFailedError(
            (
                f"Cannot delete active revision {revision} "
                f"of service {name}."
            )
        )

    async def adelete_revision(
        self, name: str, revision: str
    ) -> Response[None]:
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_group_name=name
        )
        if revision != "latest":
            raise NotFoundError("Only 'latest' revision is supported.")
        try:
            await self._aclient.container_groups.get(
                resource_group_name=resource_group_name,
                container_group_name=name,
            )
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )
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

    async def aupdate_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
    ) -> Response[ServiceItem]:
        return await self.aget_service(name)

    def close(self) -> None:
        self._init = False

    async def aclose(self) -> None:
        if hasattr(self, "_aclient") and self._aclient:
            await self._aclient.close()
        if hasattr(self, "_acredential") and self._acredential:
            await self._acredential.close()
        self._ainit = False

    def _get_subscription_id(self) -> str:
        if self.subscription_id:
            return self.subscription_id
        if self._cached_subscription_id:
            return self._cached_subscription_id
        self._cached_subscription_id = self._get_default_subscription_id()
        return self._cached_subscription_id

    def _get_default_subscription_id(self) -> str:
        from azure.mgmt.resource import SubscriptionClient

        self._init_credentials()
        sub_client = SubscriptionClient(credential=self._credential)
        subs = sub_client.subscriptions.list()
        if not subs:
            raise BadRequestError(
                "No subscriptions found for the given credentials."
            )
        sub = next(sub_client.subscriptions.list())
        return sub.subscription_id

    def _get_diagnostics(self) -> ContainerGroupDiagnostics | None:
        if (
            self.log_analytics_workspace_id
            and self.log_analytics_workspace_key
        ):
            return ContainerGroupDiagnostics(
                log_analytics=LogAnalytics(
                    workspace_id=self.log_analytics_workspace_id,
                    workspace_key=self.log_analytics_workspace_key,
                )
            )
        return None

    def _get_container_group_name(self, service: ServiceDefinition) -> str:
        if self.container_group_name:
            return self.container_group_name
        if service.name:
            return service.name
        raise BadRequestError(
            "Container group name must be provided or defined in the service."
        )

    def _get_resource_group_name(self, container_group_name: str) -> str:
        """Get the resource group name for the Container App."""
        if self.resource_group:
            return self.resource_group
        return f"{container_group_name}-rg"

    def _get_image_registry_credentials(
        self, subscription_id: str, images: list[str]
    ) -> list[ImageRegistryCredential]:
        """Get registry credentials for ACR authentication."""
        credentials: list[ImageRegistryCredential] = []
        if self.container_registry_credentials is None:
            container_registry_credentials: list[
                ContainerRegistryCredentials
            ] = []
            hash_set = set()
            for image_uri in images:
                if ".azurecr.io" in image_uri:
                    acr_name = image_uri.split(".")[0]
                    server = f"{acr_name}.azurecr.io"
                    if server not in hash_set:
                        container_registry_credentials.append(
                            ContainerRegistryCredentials(
                                server=server, auth_type="basic"
                            )
                        )
                        hash_set.add(server)
        else:
            container_registry_credentials = (
                self.container_registry_credentials
            )

        for cred in container_registry_credentials:
            if cred.auth_type == "managed":
                continue
            elif cred.auth_type == "basic":
                if cred.username and cred.password:
                    credentials.append(
                        ImageRegistryCredential(
                            server=cred.server,
                            username=cred.username,
                            password=cred.password,
                        )
                    )
                elif ".azurecr.io" in cred.server:
                    try:
                        from azure.mgmt.containerregistry import (
                            ContainerRegistryManagementClient,
                        )

                        self._init_credentials()

                        acr_client = ContainerRegistryManagementClient(
                            credential=self._credential,
                            subscription_id=subscription_id,
                        )

                        target_server = cred.server.lower()
                        found = None
                        for reg in acr_client.registries.list():
                            # reg.login_server looks like 'myacr.azurecr.io'
                            if reg.login_server == target_server:
                                found = reg
                                break

                        if not found:
                            print(
                                f"Warning: ACR with login server '{cred.server}' not found in subscription {subscription_id}."  # noqa
                            )
                            continue

                        # Parse resource group from resource ID:
                        # /subscriptions/{sub}/resourceGroups/{rg}/providers/Microsoft.ContainerRegistry/registries/{name}
                        reg_id = found.id
                        parts = reg_id.split("/")
                        try:
                            rg_name = parts[parts.index("resourceGroups") + 1]
                        except Exception:
                            print(
                                f"Warning: Could not parse resource group from ACR id: {reg_id}"  # noqa
                            )
                            continue

                        reg_name = found.name
                        if not reg_name:
                            print(
                                "Warning: ACR resource missing name; cannot list credentials."  # noqa
                            )
                            continue

                        # Requires ACR Admin user to be enabled.
                        cred_result = acr_client.registries.list_credentials(
                            resource_group_name=rg_name,
                            registry_name=reg_name,
                        )
                        user = cred_result.username
                        pwds = cred_result.passwords or []
                        pwd_value = (
                            pwds[0].value if pwds and pwds[0].value else None
                        )

                        if user and pwd_value:
                            credentials.append(
                                ImageRegistryCredential(
                                    server=cred.server,
                                    username=user,
                                    password=pwd_value,
                                )
                            )
                        else:
                            print(
                                (
                                    "Warning: ACR admin user may be disabled or returned no passwords; "  # noqa
                                    "consider using auth_type='managed' or provide explicit basic credentials."  # noqa
                                )
                            )
                    except Exception as e:
                        print(
                            f"Warning: Could not retrieve ACR credentials: {e}"
                        )
        return credentials


class OperationConverter:
    def update_service(
        self,
        existing_service: ContainerGroup,
        service: ServiceDefinition,
        images: list[str],
        container_group_name: str,
        location: str,
        image_registry_credentials: (
            list[ImageRegistryCredential] | None
        ) = None,
        diagnostics: ContainerGroupDiagnostics | None = None,
        os_type: str | OperatingSystemTypes = OperatingSystemTypes.LINUX,
        sku: str | ContainerGroupSku | None = None,
    ) -> ContainerGroup:
        new_service = self.convert_service(
            service=service,
            images=images,
            container_group_name=container_group_name,
            location=location,
            image_registry_credentials=image_registry_credentials,
            diagnostics=diagnostics,
            os_type=os_type,
            sku=sku,
        )
        existing_service.init_containers = new_service.init_containers
        existing_service.containers = new_service.containers
        existing_service.os_type = new_service.os_type
        existing_service.volumes = new_service.volumes
        existing_service.ip_address = new_service.ip_address
        existing_service.image_registry_credentials = (
            new_service.image_registry_credentials
        )
        existing_service.diagnostics = new_service.diagnostics
        existing_service.restart_policy = new_service.restart_policy
        existing_service.sku = new_service.sku
        return existing_service

    def convert_service(
        self,
        service: ServiceDefinition,
        images: list[str],
        container_group_name: str,
        location: str,
        image_registry_credentials: (
            list[ImageRegistryCredential] | None
        ) = None,
        diagnostics: ContainerGroupDiagnostics | None = None,
        os_type: str | OperatingSystemTypes = OperatingSystemTypes.LINUX,
        sku: str | ContainerGroupSku | None = None,
    ) -> ContainerGroup:
        containers: list[ACIContainer] = []
        init_containers: list[InitContainerDefinition] = []
        for container, image in zip(service.containers, images):
            if container.type == "init":
                init_containers.append(
                    self._convert_init_container(
                        container=container,
                        image=image,
                    )
                )
            else:
                containers.append(
                    self._convert_container(
                        container=container,
                        image=image,
                    )
                )
        return ContainerGroup(
            containers=containers,
            init_containers=init_containers,
            location=location,
            os_type=os_type,
            volumes=self._convert_volumes(service.volumes),
            ip_address=self._convert_ingress(
                ingress=service.ingress,
                dns_name_label=container_group_name,
            ),
            image_registry_credentials=image_registry_credentials,
            diagnostics=diagnostics,
            restart_policy=service.restart_policy,
            sku=sku,
        )

    def _convert_ingress(
        self, ingress: Ingress | None, dns_name_label: str
    ) -> IpAddress:
        if not ingress:
            return IpAddress(
                type=ContainerGroupIpAddressType.PUBLIC,
                ports=[ACIPort(port=80, protocol="TCP")],
                dns_name_label=dns_name_label,
            )
        return IpAddress(
            type=(
                ContainerGroupIpAddressType.PUBLIC
                if ingress.external
                else ContainerGroupIpAddressType.PRIVATE
            ),
            ports=[
                ACIPort(
                    port=int(ingress.port) if ingress.port else 80,
                    protocol=ingress.transport or "TCP",
                )
            ],
            dns_name_label=dns_name_label,
        )

    def _convert_volumes(
        self,
        volumes: list[Volume],
    ) -> list[ACIVolume]:
        result: list[ACIVolume] = []
        for volume in volumes:
            if volume.type == "emptyDir":
                result.append(
                    ACIVolume(
                        name=volume.name,
                        empty_dir={},
                    )
                )
        return result

    def _convert_init_container(
        self,
        container: Container,
        image: str,
    ) -> InitContainerDefinition:
        return InitContainerDefinition(
            name=container.name,
            image=image,
            command=(container.command or []) + (container.args or []),
            environment_variables=self._convert_env(container.env),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
        )

    def _convert_container(
        self,
        container: Container,
        image: str,
    ) -> ACIContainer:
        return ACIContainer(
            name=container.name,
            image=image,
            resources=self._convert_resource_requirements(
                container.resources or ResourceRequirements()
            ),
            command=(container.command or []) + (container.args or []),
            environment_variables=self._convert_env(container.env),
            ports=self._convert_ports(container.ports),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
            liveness_probe=self._convert_probe(
                container.probes.liveness_probe if container.probes else None
            ),
            readiness_probe=self._convert_probe(
                container.probes.readiness_probe if container.probes else None
            ),
            security_context=self._convert_security_context(
                container.security_context
            ),
        )

    def _convert_security_context(
        self,
        security_context: SecurityContext | None,
    ) -> SecurityContextDefinition | None:
        if not security_context:
            return None
        return SecurityContextDefinition(
            capabilities=SecurityContextCapabilitiesDefinition(
                add=(
                    security_context.capabilities.get("add", [])
                    if security_context.capabilities
                    else []
                ),
                drop=(
                    security_context.capabilities.get("drop", [])
                    if security_context.capabilities
                    else []
                ),
            ),
            run_as_user=security_context.run_as_user,
            run_as_group=security_context.run_as_group,
            privileged=security_context.privileged,
            allow_privilege_escalation=(
                security_context.allow_privilege_escalation
            ),
        )

    def _convert_env(self, env: list[EnvVar]) -> list[EnvironmentVariable]:
        return [
            EnvironmentVariable(name=var.name, value=str(var.value))
            for var in env
        ]

    def _convert_ports(
        self,
        ports: list[Port],
    ) -> list[ContainerPort]:
        return [
            ContainerPort(port=port.container_port, protocol=port.protocol)
            for port in ports
        ]

    def _convert_resource_requirements(
        self,
        resources: ResourceRequirements,
    ) -> ACIResourceRequirements:
        if resources.requests:
            requests = ACIResourceRequests(
                cpu=resources.requests.cpu or 1.0,
                memory_in_gb=(
                    float(resources.requests.memory or 512) / 1024.0
                ),
            )
            if resources.requests.gpu:
                requests.gpu = ACIGpuResource(
                    count=resources.requests.gpu.count,
                    sku=resources.requests.gpu.type or GpuSku.K80,
                )
        else:
            requests = ACIResourceRequests(
                cpu=1.0,
                memory_in_gb=0.5,
                gpu=None,
            )
        if resources.limits:
            limits = ACIResourceLimits(
                cpu=float(resources.limits.cpu or "1"),
                memory_in_gb=(float(resources.limits.memory or 512) / 1024.0),
            )
            if resources.limits.gpu:
                limits.gpu = ACIGpuResource(
                    count=resources.limits.gpu.count,
                    sku=resources.limits.gpu.type or GpuSku.K80,
                )
        else:
            limits = None
        return ACIResourceRequirements(requests=requests, limits=limits)

    def _convert_volume_mounts(
        self,
        volume_mounts: list[VolumeMount],
    ) -> list[ACIVolumeMount] | None:
        return [
            ACIVolumeMount(
                name=mount.name,
                mount_path=mount.mount_path,
                read_only=mount.read_only,
            )
            for mount in volume_mounts
        ]

    def _convert_probe(self, probe: Probe | None) -> ContainerProbe | None:
        if not probe:
            return None
        args: dict = {}
        if probe.http_get:
            args["http_get"] = self._convert_http_get_action(probe.http_get)
        elif probe.exec:
            args["exec_property"] = self._convert_exec_action(probe.exec)
        return ContainerProbe(
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            failure_threshold=probe.failure_threshold,
            success_threshold=probe.success_threshold,
            **args,
        )

    def _convert_http_get_action(
        self, action: HTTPGetAction
    ) -> ContainerHttpGet:
        headers = [
            ACIHttpHeader(name=header.name, value=header.value)
            for header in action.http_headers
        ]
        return ContainerHttpGet(
            port=int(action.port),
            path=action.path,
            scheme=action.scheme,
            http_headers=headers,
        )

    def _convert_exec_action(self, action: ExecAction) -> ContainerExec:
        return ContainerExec(
            command=action.command,
        )


class ResultConverter:
    def convert_service_item(
        self,
        service: ContainerGroup,
    ) -> ServiceItem:
        return ServiceItem(
            name=service.name or "unknown",
            uri=self._get_service_uri(service),
            service=self.convert_service(service),
        )

    def convert_service(self, service: ContainerGroup) -> ServiceDefinition:
        result = ServiceDefinition(
            name=service.name,
            ingress=self._convert_ingress(service.ip_address),
            volumes=self._convert_volumes(service.volumes),
            restart_policy=self._convert_restart_policy(
                service.restart_policy
            ),
            latest_ready_revision="latest",
            latest_created_revision="latest",
            traffic=[TrafficAllocation(revision="latest", percent=100)],
        )
        if service.init_containers:
            result.containers = [
                self._convert_init_container(container)
                for container in service.init_containers
            ]
        if service.containers:
            result.containers += [
                self._convert_container(container)
                for container in service.containers
            ]
        return result

    def _convert_restart_policy(
        self,
        restart_policy: str | ContainerGroupRestartPolicy | None,
    ) -> str:
        if isinstance(restart_policy, str):
            return restart_policy
        if isinstance(restart_policy, ContainerGroupRestartPolicy):
            return restart_policy.value
        return "Always"

    def _convert_init_container(
        self,
        container: InitContainerDefinition,
    ) -> Container:
        return Container(
            name=container.name,
            type="init",
            image=container.image,
            command=container.command,
            env=self._convert_env(container.environment_variables),
            volume_mounts=self._convert_volume_mounts(
                container.volume_mounts or []
            ),
        )

    def _convert_container(
        self,
        container: ACIContainer,
    ) -> Container:
        result = Container(
            name=container.name,
            type="main",
            image=container.image,
            command=container.command,
            env=self._convert_env(container.environment_variables),
            ports=self._convert_ports(container.ports),
            resources=self._convert_resource_requirements(container.resources),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
            probes=ProbeSet(
                liveness_probe=self._convert_probe(container.liveness_probe),
                readiness_probe=self._convert_probe(container.readiness_probe),
            ),
            security_context=self._convert_security_context(
                container.security_context
            ),
        )
        return result

    def _convert_volume_mounts(
        self,
        volume_mounts: list[ACIVolumeMount] | None,
    ) -> list[VolumeMount]:
        if not volume_mounts:
            return []
        return [
            VolumeMount(
                name=mount.name,
                mount_path=mount.mount_path,
                read_only=mount.read_only,
            )
            for mount in volume_mounts
        ]

    def _convert_ports(
        self,
        ports: list[ContainerPort] | None,
    ) -> list[Port]:
        if not ports:
            return []
        return [
            Port(
                container_port=port.port,
                protocol=self._convert_container_network_protocol(
                    port.protocol
                ),
            )
            for port in ports
        ]

    def _convert_env(
        self,
        env: list[EnvironmentVariable] | None,
    ) -> list[EnvVar]:
        if not env:
            return []
        return [EnvVar(name=var.name, value=var.value) for var in env]

    def _convert_security_context(
        self,
        security_context: SecurityContextDefinition | None,
    ) -> SecurityContext | None:
        if not security_context:
            return None
        result = SecurityContext(
            run_as_user=security_context.run_as_user,
            run_as_group=security_context.run_as_group,
            privileged=security_context.privileged,
            allow_privilege_escalation=(
                security_context.allow_privilege_escalation
            ),
        )
        if security_context.capabilities:
            result.capabilities = {}
            if security_context.capabilities.add:
                result.capabilities["add"] = (
                    security_context.capabilities.add or []
                )
            if security_context.capabilities.drop:
                result.capabilities["drop"] = (
                    security_context.capabilities.drop or []
                )
        return result

    def _convert_container_network_protocol(
        self,
        protocol: str | ContainerNetworkProtocol | None,
    ) -> str:
        if not protocol:
            return "TCP"
        if isinstance(protocol, str):
            return protocol
        if protocol == ContainerNetworkProtocol.TCP:
            return "TCP"
        elif protocol == ContainerNetworkProtocol.UDP:
            return "UDP"
        else:
            raise BadRequestError(f"Unsupported network protocol: {protocol}")

    def _convert_resource_requirements(
        self,
        resources: ACIResourceRequirements | None,
    ) -> ResourceRequirements | None:
        if not resources:
            return None
        requests = ResourceRequests(
            cpu=resources.requests.cpu,
            memory=int(resources.requests.memory_in_gb * 1024),
        )
        if resources.requests.gpu:
            requests.gpu = GPUResource(
                count=resources.requests.gpu.count,
                type=(
                    resources.requests.gpu.sku.value
                    if isinstance(resources.requests.gpu.sku, GpuSku)
                    else resources.requests.gpu.sku
                ),
            )
        if resources.limits:
            limits = ResourceLimits()
            if resources.limits.cpu:
                limits.cpu = resources.limits.cpu
            if resources.limits.memory_in_gb:
                limits.memory = int(resources.limits.memory_in_gb * 1024)
            if resources.limits.gpu:
                limits.gpu = GPUResource(
                    count=resources.limits.gpu.count,
                    type=(
                        resources.limits.gpu.sku.value
                        if isinstance(resources.limits.gpu.sku, GpuSku)
                        else resources.limits.gpu.sku
                    ),
                )
        else:
            limits = None
        return ResourceRequirements(
            requests=requests,
            limits=limits,
        )

    def _convert_ingress(
        self,
        ingress: IpAddress | None,
    ) -> Ingress | None:
        if not ingress:
            return None
        return Ingress(
            external=ingress.type == ContainerGroupIpAddressType.PUBLIC,
            target_port=ingress.ports[0].port if ingress.ports else 80,
            transport=ingress.ports[0].protocol if ingress.ports else "TCP",
        )

    def _convert_volumes(
        self,
        volumes: list[ACIVolume] | None,
    ) -> list[Volume]:
        if not volumes:
            return []
        result: list[Volume] = []
        for volume in volumes:
            if volume.empty_dir:
                result.append(
                    Volume(
                        name=volume.name,
                        type="emptyDir",
                    )
                )
        return result

    def _convert_probe(self, probe: ContainerProbe | None) -> Probe | None:
        if not probe:
            return None
        http_get = None
        exec_action = None
        if probe.http_get:
            http_get = self._convert_http_get_action(probe.http_get)
        elif probe.exec_property:
            exec_action = self._convert_exec_action(probe.exec_property)
        return Probe(
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            failure_threshold=probe.failure_threshold,
            success_threshold=probe.success_threshold,
            http_get=http_get,
            exec=exec_action,
        )

    def _convert_http_get_action(
        self, action: ContainerHttpGet
    ) -> HTTPGetAction:
        headers = []
        if action.http_headers:
            for header in action.http_headers:
                if header.name and header.value:
                    headers.append(
                        HTTPHeader(name=header.name, value=header.value)
                    )
        return HTTPGetAction(
            port=str(action.port),
            path=action.path or "/",
            scheme=action.scheme or "HTTP",
            http_headers=headers,
        )

    def _convert_exec_action(self, action: ContainerExec) -> ExecAction:
        return ExecAction(
            command=action.command or [],
        )

    def convert_services(
        self,
        services: list[ContainerGroup],
    ) -> list[ServiceItem]:
        result: list[ServiceItem] = []
        for service in services:
            result.append(self.convert_service_item(service))
        return result

    def _get_service_uri(self, container_group: ContainerGroup) -> str | None:
        """Extract the public URL from the container group response."""
        if container_group.ip_address and container_group.ip_address.fqdn:
            port = ""
            if (
                container_group.ip_address.ports
                and container_group.ip_address.ports[0].port != 80
            ):
                port = f":{container_group.ip_address.ports[0].port}"
            return f"http://{container_group.ip_address.fqdn}{port}"
        elif container_group.ip_address and container_group.ip_address.ip:
            port = ""
            if (
                container_group.ip_address.ports
                and container_group.ip_address.ports[0].port != 80
            ):
                port = f":{container_group.ip_address.ports[0].port}"
            return f"http://{container_group.ip_address.ip}{port}"
        return None
