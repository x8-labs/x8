"""
Azure Container Apps deployment.
"""

from __future__ import annotations

__all__ = ["AzureContainerApps"]

from typing import Any

from azure.core.exceptions import ResourceNotFoundError
from azure.mgmt.appcontainers import ContainerAppsAPIClient
from azure.mgmt.appcontainers.aio import (
    ContainerAppsAPIClient as AsyncContainerAppsAPIClient,
)
from azure.mgmt.appcontainers.models import AppLogsConfiguration
from azure.mgmt.appcontainers.models import Configuration as ACAConfiguration
from azure.mgmt.appcontainers.models import Container as ACAContainer
from azure.mgmt.appcontainers.models import ContainerApp as ACAContainerApp
from azure.mgmt.appcontainers.models import (
    ContainerAppProbe as ACAContainerAppProbe,
)
from azure.mgmt.appcontainers.models import (
    ContainerAppProbeHttpGet as ACAContainerAppProbeHttpGet,
)
from azure.mgmt.appcontainers.models import (
    ContainerAppProbeHttpGetHttpHeadersItem,
)
from azure.mgmt.appcontainers.models import (
    ContainerAppProbeTcpSocket as ACAContainerAppProbeTcpSocket,
)
from azure.mgmt.appcontainers.models import (
    ContainerResources as ACAContainerResources,
)
from azure.mgmt.appcontainers.models import (
    CustomScaleRule as ACACustomScaleRule,
)
from azure.mgmt.appcontainers.models import EnvironmentVar as ACAEnvironmentVar
from azure.mgmt.appcontainers.models import HttpScaleRule as ACAHttpScaleRule
from azure.mgmt.appcontainers.models import Ingress as ACAIngress
from azure.mgmt.appcontainers.models import IngressTransportMethod
from azure.mgmt.appcontainers.models import InitContainer as ACAInitContainer
from azure.mgmt.appcontainers.models import (
    LogAnalyticsConfiguration,
    ManagedEnvironment,
    RegistryCredentials,
)
from azure.mgmt.appcontainers.models import Revision as ACARevision
from azure.mgmt.appcontainers.models import Scale as ACAScale
from azure.mgmt.appcontainers.models import ScaleRule as ACAScaleRule
from azure.mgmt.appcontainers.models import ScaleRuleAuth as ACAScaleRuleAuth
from azure.mgmt.appcontainers.models import Secret as ACASecret
from azure.mgmt.appcontainers.models import TcpScaleRule as ACATcpScaleRule
from azure.mgmt.appcontainers.models import Template as ACATemplate
from azure.mgmt.appcontainers.models import TrafficWeight as ACATrafficWeight
from azure.mgmt.appcontainers.models import Volume as ACAVolume
from azure.mgmt.appcontainers.models import VolumeMount as ACAVolumeMount
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
    HTTPGetAction,
    HTTPHeader,
    Ingress,
    Probe,
    ProbeSet,
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


class AzureContainerApps(AzureProvider, BaseContainerDeploymentProvider):
    subscription_id: str | None
    resource_group: str | None
    location: str
    environment_name: str | None
    container_app_name: str | None
    base_url: str
    container_registry_credentials: list[ContainerRegistryCredentials] | None
    log_analytics_workspace_id: str | None
    log_analytics_workspace_key: str | None
    revision_mode: str | None = None
    nparams: dict[str, Any]

    _client: ContainerAppsAPIClient
    _aclient: AsyncContainerAppsAPIClient
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
        environment_name: str | None = None,
        container_app_name: str | None = None,
        base_url: str = "https://management.azure.com",
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        container_registry_credentials: (
            list[ContainerRegistryCredentials] | None
        ) = None,
        log_analytics_workspace_id: str | None = None,
        log_analytics_workspace_key: str | None = None,
        revision_mode: str | None = None,
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
            location: Azure region for the container app.
            environment_name: The name of the Container Apps environment.
            container_app_name: The name of the container app to deploy.
            base_url: Base URL for the container app management.
            service: Service definition to deploy.
            service_override: Service override definition to apply.
            containerizer: Containerizer instance for building images.
            container_registry: Container registry instance for pushing images.
            container_registry_credentials:
                List of container registry credentials for ACR authentication.
            log_analytics_workspace_id: Log Analytics workspace ID.
            log_analytics_workspace_key: Log Analytics workspace key.
            revision_mode: Revision mode for the container app.
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
        self.environment_name = environment_name
        self.container_app_name = container_app_name
        self.base_url = base_url
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry
        self.container_registry_credentials = container_registry_credentials
        self.log_analytics_workspace_id = log_analytics_workspace_id
        self.log_analytics_workspace_key = log_analytics_workspace_key
        self.revision_mode = revision_mode
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
        return feature not in [ContainerDeploymentFeature.REVISION_DELETE]

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self._credential = self._get_credential()
        subscription_id = self._get_subscription_id()
        self._client = ContainerAppsAPIClient(
            credential=self._credential,
            subscription_id=subscription_id,
            base_url=self.base_url,
            **self.nparams,
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return
        self._acredential = self._aget_credential()
        subscription_id = self._get_subscription_id()
        self._aclient = AsyncContainerAppsAPIClient(
            credential=self._acredential,
            subscription_id=subscription_id,
            base_url=self.base_url,
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

        acr_name = f"{self._get_container_app_name(service)}-acr".replace(
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
                self._get_container_app_name(service)
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
        container_app_name = self._get_container_app_name(service_def)
        environment_name = self._get_environment_name(
            container_app_name=container_app_name
        )
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=container_app_name
        )

        try:
            existing_app = self._client.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=container_app_name,
            )
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {container_app_name} already exists."
                )
            self._ensure_resource_group(
                subscription_id=subscription_id,
                resource_group=resource_group_name,
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
            self._ensure_environment(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            registry_credentials, secrets = (
                self._get_registry_credentials_with_secrets(
                    subscription_id=subscription_id, images=images
                )
            )
            updated_app = self._op_converter.update_service(
                existing_service=existing_app,
                service=service_def,
                images=images,
                location=self.location,
                environment_id=self._get_environment_id(
                    subscription_id=subscription_id,
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                ),
                registry_credentials=registry_credentials,
                secrets=secrets,
            )

            operation = self._client.container_apps.begin_create_or_update(
                resource_group_name=resource_group_name,
                container_app_name=container_app_name,
                container_app_envelope=updated_app,
            )
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {result.uri}")
        except ResourceNotFoundError:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {container_app_name} not found."
                )
            self._ensure_resource_group(
                subscription_id=subscription_id,
                resource_group=resource_group_name,
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
            self._ensure_environment(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            registry_credentials, secrets = (
                self._get_registry_credentials_with_secrets(
                    subscription_id=subscription_id, images=images
                )
            )
            container_app = self._op_converter.convert_service(
                service=service_def,
                images=images,
                location=self.location,
                environment_id=self._get_environment_id(
                    subscription_id=subscription_id,
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                ),
                registry_credentials=registry_credentials,
                secrets=secrets,
            )
            operation = self._client.container_apps.begin_create_or_update(
                resource_group_name=resource_group_name,
                container_app_name=container_app_name,
                container_app_envelope=container_app,
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
        container_app_name = self._get_container_app_name(service_def)
        environment_name = self._get_environment_name(
            container_app_name=container_app_name
        )
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=container_app_name
        )

        try:
            existing_app = await self._aclient.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=container_app_name,
            )
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {container_app_name} already exists."
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
            self._ensure_resource_group(
                subscription_id=subscription_id,
                resource_group=resource_group_name,
            )
            await self._aensure_environment(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            registry_credentials, secrets = (
                await self._aget_registry_credentials_with_secrets(
                    subscription_id=subscription_id, images=images
                )
            )

            updated_app = self._op_converter.update_service(
                existing_service=existing_app,
                service=service_def,
                images=images,
                location=self.location,
                environment_id=self._get_environment_id(
                    subscription_id=subscription_id,
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                ),
                registry_credentials=registry_credentials,
                secrets=secrets,
                revision_mode=self.revision_mode,
            )
            operation = (
                await self._aclient.container_apps.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    container_app_name=container_app_name,
                    container_app_envelope=updated_app,
                )
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {result.uri}")
        except ResourceNotFoundError:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {container_app_name} not found."
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
            self._ensure_resource_group(
                subscription_id=subscription_id,
                resource_group=resource_group_name,
            )
            await self._aensure_environment(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            registry_credentials, secrets = (
                await self._aget_registry_credentials_with_secrets(
                    subscription_id=subscription_id, images=images
                )
            )
            container_app = self._op_converter.convert_service(
                service=service_def,
                images=images,
                location=self.location,
                environment_id=self._get_environment_id(
                    subscription_id=subscription_id,
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                ),
                registry_credentials=registry_credentials,
                secrets=secrets,
                revision_mode=self.revision_mode,
            )
            operation = (
                await self._aclient.container_apps.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    container_app_name=container_app_name,
                    container_app_envelope=container_app,
                )
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service created successfully: {result.uri}")
        return Response(result=result)

    def get_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            response = self._client.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=name,
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
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            response = await self._aclient.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=name,
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

    def delete_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        self.__setup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            operation = self._client.container_apps.begin_delete(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            operation.result()
            print(f"Service {name} deleted successfully.")
            if not self.environment_name:
                environment_name = self._get_environment_name(
                    container_app_name=name
                )
                self._delete_environment(
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                )
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
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        await self.__asetup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            operation = await self._aclient.container_apps.begin_delete(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            await operation.result()
            print(f"Service {name} deleted successfully.")
            if not self.environment_name:
                environment_name = self._get_environment_name(
                    container_app_name=name
                )
                await self._adelete_environment(
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                )
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

    def list_services(
        self,
        **kwargs: Any,
    ) -> Response[list[ServiceItem]]:
        self.__setup__()
        if self.resource_group:
            response = self._client.container_apps.list_by_resource_group(
                resource_group_name=self.resource_group,
            )
        else:
            response = self._client.container_apps.list_by_subscription()

        result = self._result_converter.convert_services(
            services=list(response)
        )
        return Response(result=result)

    async def alist_services(
        self,
        **kwargs: Any,
    ) -> Response[list[ServiceItem]]:
        await self.__asetup__()
        if self.resource_group:
            response = self._aclient.container_apps.list_by_resource_group(
                resource_group_name=self.resource_group,
            )
        else:
            response = self._aclient.container_apps.list_by_subscription()
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
            container_app_name=name
        )
        try:
            response = self._client.container_apps_revisions.list_revisions(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            result = self._result_converter.convert_revisions(
                revisions=list(response)
            )
            return Response(result=result)
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
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            response = self._aclient.container_apps_revisions.list_revisions(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            result = self._result_converter.convert_revisions(
                revisions=[revision async for revision in response]
            )
            return Response(result=result)
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
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            response = self._client.container_apps_revisions.get_revision(
                resource_group_name=resource_group_name,
                container_app_name=name,
                revision_name=revision,
            )
            result = self._result_converter.convert_revision(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Revision {revision} of service {name} not found in "
                    f"resource group {resource_group_name}."
                )
            )

    async def aget_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            response = (
                await self._aclient.container_apps_revisions.get_revision(
                    resource_group_name=resource_group_name,
                    container_app_name=name,
                    revision_name=revision,
                )
            )
            result = self._result_converter.convert_revision(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Revision {revision} of service {name} not found in "
                    f"resource group {resource_group_name}."
                )
            )

    def delete_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[None]:
        self.__setup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            revisions = self._client.container_apps_revisions.list_revisions(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            for rev in revisions:
                if rev.name == revision:
                    if rev.active:
                        raise PreconditionFailedError(
                            (
                                f"Cannot delete active revision {revision} of "
                                f"service {name}. Deactivate it first."
                            )
                        )
                else:
                    break
            self._client.container_apps_revisions.deactivate_revision(
                resource_group_name=resource_group_name,
                container_app_name=name,
                revision_name=revision,
            )
            return Response(result=None)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Revision {revision} of service {name} not found in "
                    f"resource group {resource_group_name}."
                )
            )

    async def adelete_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[None]:
        await self.__asetup__()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            revisions = self._aclient.container_apps_revisions.list_revisions(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            async for rev in revisions:
                if rev.name == revision:
                    if rev.active:
                        raise PreconditionFailedError(
                            (
                                f"Cannot delete active revision {revision} of "
                                f"service {name}. Deactivate it first."
                            )
                        )
                else:
                    break
            await self._aclient.container_apps_revisions.deactivate_revision(
                resource_group_name=resource_group_name,
                container_app_name=name,
                revision_name=revision,
            )
            return Response(result=None)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Revision {revision} of service {name} not found in "
                    f"resource group {resource_group_name}."
                )
            )

    def update_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        self.__setup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            app = self._client.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            traffic_weights = self._op_converter.convert_traffic(traffic)
            if app.configuration and app.configuration.ingress:
                app.configuration.ingress.traffic = traffic_weights
            else:
                raise BadRequestError(
                    (
                        "Service configuration or ingress is missing; "
                        "cannot update traffic."
                    )
                )

            # ACA loses secrets during get. Need to repopulate.
            images = self._result_converter.get_images(app)
            _, secrets = self._get_registry_credentials_with_secrets(
                subscription_id=subscription_id, images=images
            )
            if app.configuration:
                app.configuration.secrets = secrets

            operation = self._client.container_apps.begin_create_or_update(
                resource_group_name=resource_group_name,
                container_app_name=name,
                container_app_envelope=app,
            )
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

    async def aupdate_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        subscription_id = self._get_subscription_id()
        resource_group_name = self._get_resource_group_name(
            container_app_name=name
        )
        try:
            app = await self._aclient.container_apps.get(
                resource_group_name=resource_group_name,
                container_app_name=name,
            )
            traffic_weights = self._op_converter.convert_traffic(traffic)
            if app.configuration and app.configuration.ingress:
                app.configuration.ingress.traffic = traffic_weights
            else:
                raise BadRequestError(
                    (
                        "Service configuration or ingress is missing; "
                        "cannot update traffic."
                    )
                )

            # ACA loses secrets during get. Need to repopulate.
            images = self._result_converter.get_images(app)
            _, secrets = await self._aget_registry_credentials_with_secrets(
                subscription_id=subscription_id,
                images=images,
            )
            if app.configuration:
                app.configuration.secrets = secrets

            operation = (
                await self._aclient.container_apps.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    container_app_name=name,
                    container_app_envelope=app,
                )
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except ResourceNotFoundError:
            raise NotFoundError(
                (
                    f"Service {name} not found in resource group "
                    f"{resource_group_name}."
                )
            )

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

    def _ensure_resource_group(
        self, subscription_id: str, resource_group: str
    ) -> None:
        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.resource import ResourceManagementClient

        self._init_credentials()
        rg_client = ResourceManagementClient(
            credential=self._credential,
            subscription_id=subscription_id,
        )
        try:
            rg_client.resource_groups.get(resource_group)
        except ResourceNotFoundError:
            rg_client.resource_groups.create_or_update(
                resource_group, {"location": self.location}
            )
            print(f"Created resource group: {resource_group}")

    def _delete_resource_group_if_empty(
        self, subscription_id: str, resource_group: str
    ) -> None:
        from azure.core.exceptions import ResourceNotFoundError
        from azure.mgmt.resource import ResourceManagementClient

        self._init_credentials()
        rg_client = ResourceManagementClient(
            credential=self._credential,
            subscription_id=subscription_id,
        )

        # Check if the resource group is empty before deleting
        resources = list(
            rg_client.resources.list_by_resource_group(resource_group)
        )
        for resource in resources:
            print(f"Found resource in group: {resource.name}")
        if resources:
            print(
                f"Resource group {resource_group} is not empty; "
                "skipping deletion."
            )
            return
        try:
            operation = rg_client.resource_groups.begin_delete(resource_group)
            operation.result()
            print(f"Deleted resource group: {resource_group}")
        except ResourceNotFoundError:
            print(f"Resource group {resource_group} not found to delete.")

    def _ensure_environment(
        self, resource_group_name: str, environment_name: str
    ) -> None:
        """Ensure the Container Apps environment exists."""
        try:
            self._client.managed_environments.get(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
        except ResourceNotFoundError:
            # Create the environment
            environment = ManagedEnvironment(
                location=self.location,
                app_logs_configuration=self._get_logs_configuration(),
            )
            operation = (
                self._client.managed_environments.begin_create_or_update(
                    resource_group_name=resource_group_name,
                    environment_name=environment_name,
                    environment_envelope=environment,
                )
            )
            operation.result()
            print(f"Created Container Apps environment: {environment_name}")

    async def _aensure_environment(
        self, resource_group_name: str, environment_name: str
    ) -> None:
        """Ensure the Container Apps environment exists (async)."""
        try:
            await self._aclient.managed_environments.get(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
        except ResourceNotFoundError:
            # Create the environment
            environment = ManagedEnvironment(
                location=self.location,
                app_logs_configuration=self._get_logs_configuration(),
            )
            operation = await self._aclient.managed_environments.begin_create_or_update(  # noqa
                resource_group_name=resource_group_name,
                environment_name=environment_name,
                environment_envelope=environment,
            )
            await operation.result()
            print(f"Created Container Apps environment: {environment_name}")

    def _delete_environment(
        self, resource_group_name: str, environment_name: str
    ) -> None:
        try:
            operation = self._client.managed_environments.begin_delete(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            operation.result()
            print(f"Deleted Container Apps environment: {environment_name}")
        except ResourceNotFoundError:
            print(
                f"Container Apps environment {environment_name} not found; "
                "nothing to delete."
            )

    async def _adelete_environment(
        self, resource_group_name: str, environment_name: str
    ) -> None:
        try:
            operation = await self._aclient.managed_environments.begin_delete(
                resource_group_name=resource_group_name,
                environment_name=environment_name,
            )
            await operation.result()
            print(f"Deleted Container Apps environment: {environment_name}")
        except ResourceNotFoundError:
            print(
                f"Container Apps environment {environment_name} not found; "
                "nothing to delete."
            )

    def _get_container_app_name(self, service: ServiceDefinition) -> str:
        if self.container_app_name:
            return self.container_app_name
        if service.name:
            return service.name
        raise BadRequestError(
            "Container app name must be provided or defined in the service."
        )

    def _get_resource_group_name(self, container_app_name: str) -> str:
        """Get the resource group name for the Container App."""
        if self.resource_group:
            return self.resource_group
        return f"{container_app_name}-rg"

    def _get_environment_name(self, container_app_name: str) -> str:
        """Get the name of the Container Apps environment."""
        if self.environment_name:
            return self.environment_name
        return f"{container_app_name}-env"

    def _get_environment_id(
        self,
        subscription_id: str,
        resource_group_name: str,
        environment_name: str,
    ) -> str:
        """Get the full resource ID of the Container Apps environment."""
        return (
            f"/subscriptions/{subscription_id}/"
            f"resourceGroups/{resource_group_name}/"
            f"providers/Microsoft.App/"
            f"managedEnvironments/{environment_name}"
        )

    def _get_logs_configuration(self) -> AppLogsConfiguration | None:
        """Get the log configuration for the container app."""
        if (
            self.log_analytics_workspace_id
            and self.log_analytics_workspace_key
        ):
            log_analytics_config = LogAnalyticsConfiguration(
                customer_id=self.log_analytics_workspace_id,
                shared_key=self.log_analytics_workspace_key,
            )
            return AppLogsConfiguration(
                destination="log-analytics",
                log_analytics_configuration=log_analytics_config,
            )
        return AppLogsConfiguration()

    def _get_registry_credentials_with_secrets(
        self,
        subscription_id: str,
        images: list[str],
    ) -> tuple[list[RegistryCredentials], list[ACASecret]]:
        credentials: list[RegistryCredentials] = []
        secrets: list[ACASecret] = []

        # 1) If none were explicitly provided, infer ACRs from image URIs.
        if self.container_registry_credentials is None:
            container_registry_credentials: list[
                ContainerRegistryCredentials
            ] = []
            seen = set()
            for image_uri in images:
                if ".azurecr.io" in image_uri:
                    acr_server = image_uri.split("/")[
                        0
                    ]  # e.g., myacr.azurecr.io
                    if acr_server not in seen:
                        container_registry_credentials.append(
                            ContainerRegistryCredentials(
                                server=acr_server, auth_type="basic"
                            )
                        )
                        seen.add(acr_server)
        else:
            container_registry_credentials = (
                self.container_registry_credentials
            )

        # 2) Convert provided/inferred credentials into
        # ACA registries + secrets.
        for cred in container_registry_credentials:
            if cred.auth_type == "managed":
                # Managed identity is handled by ACA; nothing to add here.
                continue

            if cred.auth_type == "basic":
                secret_name = (
                    f"registry-password-{cred.server.replace('.', '-')}"
                )
                if cred.username and cred.password:
                    # Use explicitly provided basic credentials.
                    credentials.append(
                        RegistryCredentials(
                            server=cred.server,
                            username=cred.username,
                            password_secret_ref=secret_name,
                        )
                    )
                    secrets.append(
                        ACASecret(name=secret_name, value=cred.password)
                    )
                    continue

                # Try SDK-based ACR Admin credentials if it's an ACR server.
                if ".azurecr.io" in (cred.server or ""):
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
                                RegistryCredentials(
                                    server=cred.server,
                                    username=user,
                                    password_secret_ref=secret_name,
                                )
                            )
                            secrets.append(
                                ACASecret(name=secret_name, value=pwd_value)
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

        return credentials, secrets

    async def _aget_registry_credentials_with_secrets(
        self,
        subscription_id: str,
        images: list[str],
    ) -> tuple[list[RegistryCredentials], list[ACASecret]]:
        credentials: list[RegistryCredentials] = []
        secrets: list[ACASecret] = []

        # 1) If none were explicitly provided, infer ACRs from image URIs.
        if self.container_registry_credentials is None:
            container_registry_credentials: list[
                ContainerRegistryCredentials
            ] = []
            seen = set()
            for image_uri in images:
                if ".azurecr.io" in image_uri:
                    acr_server = image_uri.split("/")[
                        0
                    ]  # e.g., myacr.azurecr.io
                    if acr_server not in seen:
                        container_registry_credentials.append(
                            ContainerRegistryCredentials(
                                server=acr_server, auth_type="basic"
                            )
                        )
                        seen.add(acr_server)
        else:
            container_registry_credentials = (
                self.container_registry_credentials
            )

        # 2) Convert provided/inferred credentials into
        # ACA registries + secrets.
        for cred in container_registry_credentials:
            if cred.auth_type == "managed":
                # Managed identity is handled by ACA; nothing to add here.
                continue

            if cred.auth_type == "basic":
                secret_name = (
                    f"registry-password-{cred.server.replace('.', '-')}"
                )
                if cred.username and cred.password:
                    # Use explicitly provided basic credentials.
                    credentials.append(
                        RegistryCredentials(
                            server=cred.server,
                            username=cred.username,
                            password_secret_ref=secret_name,
                        )
                    )
                    secrets.append(
                        ACASecret(name=secret_name, value=cred.password)
                    )
                    continue

                # Try SDK-based ACR Admin credentials if it's an ACR server.
                if ".azurecr.io" in (cred.server or ""):
                    try:
                        from azure.mgmt.containerregistry.aio import (
                            ContainerRegistryManagementClient,
                        )

                        acr_client = ContainerRegistryManagementClient(
                            credential=self._acredential,
                            subscription_id=subscription_id,
                        )

                        target_server = cred.server.lower()
                        found = None
                        async for reg in acr_client.registries.list():
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
                        cred_result = (
                            await acr_client.registries.list_credentials(
                                resource_group_name=rg_name,
                                registry_name=reg_name,
                            )
                        )
                        user = cred_result.username
                        pwds = cred_result.passwords or []
                        pwd_value = (
                            pwds[0].value if pwds and pwds[0].value else None
                        )

                        if user and pwd_value:
                            credentials.append(
                                RegistryCredentials(
                                    server=cred.server,
                                    username=user,
                                    password_secret_ref=secret_name,
                                )
                            )
                            secrets.append(
                                ACASecret(name=secret_name, value=pwd_value)
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

        return credentials, secrets


class OperationConverter:
    def update_service(
        self,
        existing_service: ACAContainerApp,
        service: ServiceDefinition,
        images: list[str],
        location: str,
        environment_id: str,
        registry_credentials: list[RegistryCredentials] | None = None,
        secrets: list[ACASecret] | None = None,
        revision_mode: str | None = None,
    ) -> ACAContainerApp:
        new_service = self.convert_service(
            service=service,
            images=images,
            location=location,
            environment_id=environment_id,
            registry_credentials=registry_credentials,
            secrets=secrets,
            revision_mode=revision_mode,
        )
        existing_service.template = new_service.template
        existing_service.configuration = new_service.configuration
        return existing_service

    def convert_service(
        self,
        service: ServiceDefinition,
        images: list[str],
        location: str,
        environment_id: str,
        registry_credentials: list[RegistryCredentials] | None = None,
        secrets: list[ACASecret] | None = None,
        revision_mode: str | None = None,
    ) -> ACAContainerApp:
        containers: list[ACAContainer] = []
        init_containers: list[ACAInitContainer] = []

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

        template = ACATemplate(
            containers=containers,
            init_containers=init_containers,
            volumes=self._convert_volumes(service.volumes),
            scale=self._convert_scale(service.scale),
        )

        configuration = ACAConfiguration(
            ingress=self._convert_ingress(
                service.ingress,
                service.containers,
                service.traffic,
            ),
            registries=registry_credentials,
            secrets=secrets,
            active_revisions_mode=revision_mode or "Multiple",
        )

        return ACAContainerApp(
            location=location,
            environment_id=environment_id,
            configuration=configuration,
            template=template,
        )

    def _convert_scale(self, scale: Scale | None) -> ACAScale | None:
        if not scale:
            return None
        return ACAScale(
            min_replicas=scale.min_replicas,
            max_replicas=scale.max_replicas or 10,
            cooldown_period=scale.cooldown_period,
            polling_interval=scale.polling_interval,
            rules=self._convert_scale_rules(scale.rules),
        )

    def _convert_scale_rules(
        self, scale_rules: list[ScaleRule]
    ) -> list[ACAScaleRule]:
        result: list[ACAScaleRule] = []
        for rule in scale_rules:
            scale_rule_auth = None
            if rule.auth:
                scale_rule_auth = []
                for auth in rule.auth:
                    scale_rule_auth.append(ACAScaleRuleAuth(**auth))
            if rule.type == "http":
                result.append(
                    ACAScaleRule(
                        name=rule.name,
                        http_scale_rule=ACAHttpScaleRule(
                            metadata=rule.metadata,
                            auth=scale_rule_auth,
                        ),
                    )
                )
            elif rule.type == "tcp":
                result.append(
                    ACAScaleRule(
                        name=rule.name,
                        tcp_scale_rule=ACATcpScaleRule(
                            metadata=rule.metadata,
                            auth=scale_rule_auth,
                        ),
                    )
                )
            elif rule.type == "custom":
                result.append(
                    ACAScaleRule(
                        name=rule.name,
                        custom_scale_rule=ACACustomScaleRule(
                            metadata=rule.metadata,
                            auth=scale_rule_auth,
                        ),
                    )
                )
        return result

    def _convert_ingress(
        self,
        ingress: Ingress | None,
        containers: list[Container],
        traffic: list[TrafficAllocation] | None,
    ) -> ACAIngress | None:
        target_port = None
        exposed_port = None
        external = True
        transport = IngressTransportMethod.AUTO
        if ingress:
            if ingress.target_port:
                target_port = ingress.target_port
            if ingress.external is not None:
                external = ingress.external
            if ingress.transport:
                transport = self._convert_transport(ingress.transport)
            if ingress.port:
                exposed_port = ingress.port

        if not target_port:
            for container in containers:
                if container.type == "main":
                    if container.ports:
                        for port in container.ports:
                            if port.container_port:
                                target_port = port.container_port
                                break

        return ACAIngress(
            external=external,
            target_port=int(target_port) if target_port else None,
            exposed_port=int(exposed_port) if exposed_port else None,
            transport=transport,
            traffic=self.convert_traffic(traffic),
        )

    def convert_traffic(
        self, traffic: list[TrafficAllocation] | None
    ) -> list[ACATrafficWeight]:
        if not traffic:
            return [
                ACATrafficWeight(
                    weight=100,
                    latest_revision=True,
                )
            ]
        result: list[ACATrafficWeight] = []
        for item in traffic:
            result.append(
                ACATrafficWeight(
                    revision_name=item.revision,
                    weight=int(item.percent),
                    latest_revision=item.latest_revision,
                    label=item.tag,
                )
            )
        return result

    def _convert_transport(
        self, transport: str | None
    ) -> IngressTransportMethod:
        if not transport or transport.upper() == "TCP":
            return IngressTransportMethod.TCP
        elif transport.upper() == "HTTP":
            return IngressTransportMethod.HTTP
        elif transport.upper() == "HTTP2":
            return IngressTransportMethod.HTTP2
        else:
            return IngressTransportMethod.AUTO

    def _convert_volumes(self, volumes: list[Volume]) -> list[ACAVolume]:
        result: list[ACAVolume] = []
        for volume in volumes:
            if volume.type == "emptyDir":
                result.append(
                    ACAVolume(
                        name=volume.name,
                        storage_type="EmptyDir",
                    )
                )
        return result

    def _convert_init_container(
        self,
        container: Container,
        image: str,
    ) -> ACAInitContainer:
        return ACAInitContainer(
            name=container.name,
            image=image,
            command=container.command,
            args=container.args,
            env=self._convert_env(container.env),
            resources=self._convert_resources(
                container.resources or ResourceRequirements()
            ),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
        )

    def _convert_container(
        self,
        container: Container,
        image: str,
    ) -> ACAContainer:
        return ACAContainer(
            name=container.name,
            image=image,
            command=container.command,
            args=container.args,
            env=self._convert_env(container.env),
            resources=self._convert_resources(
                container.resources or ResourceRequirements()
            ),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
            probes=self._convert_probes(container.probes),
        )

    def _convert_probes(
        self, probes: ProbeSet | None
    ) -> list[ACAContainerAppProbe]:
        result = []
        if probes:
            if probes.liveness_probe:
                probe = self._convert_probe(probes.liveness_probe, "Liveness")
                result.append(probe)
            if probes.readiness_probe:
                probe = self._convert_probe(
                    probes.readiness_probe, "Readiness"
                )
                result.append(probe)
            if probes.startup_probe:
                probe = self._convert_probe(probes.startup_probe, "Startup")
                result.append(probe)
        return result

    def _convert_probe(self, probe: Probe, type: str) -> ACAContainerAppProbe:
        result = ACAContainerAppProbe(
            failure_threshold=probe.failure_threshold,
            success_threshold=probe.success_threshold,
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            termination_grace_period_seconds=probe.termination_grace_period_seconds,  # noqa
            type=type,
        )
        if probe.http_get:
            result.http_get = self._convert_http_get_action(probe.http_get)
        if probe.tcp_socket:
            result.tcp_socket = self._convert_tcp_socket_action(
                probe.tcp_socket
            )
        return result

    def _convert_http_get_action(
        self, action: HTTPGetAction
    ) -> ACAContainerAppProbeHttpGet:
        headers = []
        for header in action.http_headers:
            headers.append(
                ContainerAppProbeHttpGetHttpHeadersItem(
                    name=header.name,
                    value=header.value,
                )
            )

        return ACAContainerAppProbeHttpGet(
            port=int(action.port),
            host=action.host,
            path=action.path,
            scheme=action.scheme,
            http_headers=headers,
        )

    def _convert_tcp_socket_action(
        self, action: TCPSocketAction
    ) -> ACAContainerAppProbeTcpSocket:
        return ACAContainerAppProbeTcpSocket(
            port=int(action.port),
            host=action.host,
        )

    def _convert_env(self, env: list[EnvVar]) -> list[ACAEnvironmentVar]:
        return [
            ACAEnvironmentVar(name=var.name, value=str(var.value))
            for var in env
        ]

    def _convert_resources(
        self,
        resources: ResourceRequirements,
    ) -> ACAContainerResources:
        cpu = 0.25
        memory = 512
        if resources.requests and resources.requests.cpu:
            cpu = resources.requests.cpu
        if resources.requests and resources.requests.memory:
            memory = resources.requests.memory
        if resources.limits and resources.limits.cpu:
            cpu = resources.limits.cpu
        if resources.limits and resources.limits.memory:
            memory = resources.limits.memory

        gi = float(memory) / 1024.0
        memory_str = f"{gi:.2f}".rstrip("0").rstrip(".")
        if memory_str == "":
            memory_str = "0"
        return ACAContainerResources(
            cpu=float(cpu),
            memory=f"{memory_str}Gi",
        )

    def _convert_volume_mounts(
        self,
        volume_mounts: list[VolumeMount],
    ) -> list[ACAVolumeMount]:
        return [
            ACAVolumeMount(
                volume_name=mount.name,
                mount_path=mount.mount_path,
                sub_path=mount.sub_path,
            )
            for mount in volume_mounts
        ]


class ResultConverter:
    def convert_service_item(
        self,
        container_app: ACAContainerApp,
    ) -> ServiceItem:
        return ServiceItem(
            name=container_app.name or "unknown",
            uri=self._get_service_uri(container_app),
            service=self.convert_service(container_app),
        )

    def convert_service(
        self, container_app: ACAContainerApp
    ) -> ServiceDefinition:
        template = container_app.template
        configuration = container_app.configuration

        result = ServiceDefinition(
            name=container_app.name,
            latest_ready_revision=container_app.latest_ready_revision_name,
            latest_created_revision=container_app.latest_revision_name,
        )

        if configuration:
            result.ingress = self._convert_ingress(configuration.ingress)
            if configuration.ingress:
                result.traffic = self._convert_traffic(
                    configuration.ingress.traffic
                )
        if template:
            result.volumes = self._convert_volumes(template.volumes)
            result.scale = self._convert_scale(template.scale)
            containers = []
            if template and template.init_containers:
                containers.extend(
                    [
                        self._convert_init_container(container)
                        for container in template.init_containers
                    ]
                )
            if template and template.containers:
                containers.extend(
                    [
                        self._convert_container(container)
                        for container in template.containers
                    ]
                )

            result.containers = containers
        return result

    def _convert_init_container(
        self,
        container: ACAInitContainer,
    ) -> Container:
        return Container(
            name=container.name or "default",
            type="init",
            image=container.image,
            command=container.command,
            args=container.args,
            env=self._convert_env(container.env),
            resources=self._convert_resources(container.resources),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
        )

    def _convert_container(
        self,
        container: ACAContainer,
    ) -> Container:
        return Container(
            name=container.name or "default",
            type="main",
            image=container.image,
            command=container.command,
            args=container.args,
            env=self._convert_env(container.env),
            resources=self._convert_resources(container.resources),
            volume_mounts=self._convert_volume_mounts(container.volume_mounts),
            probes=self._convert_probes(container.probes),
        )

    def _convert_probes(
        self, probes: list[ACAContainerAppProbe] | None
    ) -> ProbeSet | None:
        if not probes:
            return None

        liveness_probe = None
        readiness_probe = None
        startup_probe = None

        for probe in probes:
            converted_probe = self._convert_probe(probe)
            if probe.type == "Liveness":
                liveness_probe = converted_probe
            elif probe.type == "Readiness":
                readiness_probe = converted_probe
            elif probe.type == "Startup":
                startup_probe = converted_probe

        return ProbeSet(
            liveness_probe=liveness_probe,
            readiness_probe=readiness_probe,
            startup_probe=startup_probe,
        )

    def _convert_probe(self, probe: ACAContainerAppProbe) -> Probe:
        http_get = None
        tcp_socket = None

        if probe.http_get:
            http_get = self._convert_http_get_action(probe.http_get)
        if probe.tcp_socket:
            tcp_socket = self._convert_tcp_socket_action(probe.tcp_socket)

        return Probe(
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            failure_threshold=probe.failure_threshold,
            success_threshold=probe.success_threshold,
            http_get=http_get,
            tcp_socket=tcp_socket,
        )

    def _convert_http_get_action(
        self, action: ACAContainerAppProbeHttpGet
    ) -> HTTPGetAction:
        headers = []
        if action.http_headers:
            for header in action.http_headers:
                headers.append(
                    HTTPHeader(name=header.name, value=header.value)
                )

        return HTTPGetAction(
            port=action.port,
            path=action.path or "/",
            scheme=action.scheme or "HTTP",
            host=action.host,
            http_headers=headers,
        )

    def _convert_tcp_socket_action(
        self, action: ACAContainerAppProbeTcpSocket
    ) -> TCPSocketAction:
        return TCPSocketAction(
            port=action.port,
            host=action.host,
        )

    def _convert_volume_mounts(
        self,
        volume_mounts: list[ACAVolumeMount] | None,
    ) -> list[VolumeMount]:
        if not volume_mounts:
            return []
        result: list[VolumeMount] = []
        for mount in volume_mounts:
            if mount.volume_name and mount.mount_path:
                result.append(
                    VolumeMount(
                        name=mount.volume_name,
                        mount_path=mount.mount_path,
                        sub_path=mount.sub_path,
                    )
                )
        return result

    def _convert_env(
        self,
        env: list[ACAEnvironmentVar] | None,
    ) -> list[EnvVar]:
        if not env:
            return []
        result: list[EnvVar] = []
        for var in env:
            if var.name:
                result.append(
                    EnvVar(
                        name=var.name,
                        value=var.value,
                    )
                )
        return result

    def _convert_resources(
        self,
        resources: ACAContainerResources | None,
    ) -> ResourceRequirements | None:
        if not resources:
            return None

        memory = None
        if resources.memory:
            if resources.memory.endswith("Mi"):
                memory = int(resources.memory[:-2])
            elif resources.memory.endswith("Gi"):
                memory = int(float(resources.memory[:-2]) * 1024)
            elif resources.memory.endswith("Ti"):
                memory = int(float(resources.memory[:-2]) * 1024 * 1024)
        requests = ResourceRequests(
            cpu=resources.cpu if resources.cpu else None,
            memory=memory,
        )

        return ResourceRequirements(
            requests=requests,
        )

    def _convert_ingress(
        self,
        ingress: ACAIngress | None,
    ) -> Ingress:
        if not ingress:
            return Ingress()

        transport = "TCP"
        if ingress.transport == IngressTransportMethod.HTTP:
            transport = "HTTP"
        elif ingress.transport == IngressTransportMethod.HTTP2:
            transport = "HTTP2"
        elif ingress.transport == IngressTransportMethod.AUTO:
            transport = "AUTO"

        return Ingress(
            external=ingress.external,
            target_port=ingress.target_port,
            transport=transport,
        )

    def _convert_traffic(
        self,
        traffic: list[ACATrafficWeight] | None,
    ) -> list[TrafficAllocation]:
        if not traffic:
            return []

        result: list[TrafficAllocation] = []
        for item in traffic:
            allocation = TrafficAllocation(
                revision=(
                    item.revision_name if item.revision_name else "default"
                ),
                percent=item.weight if item.weight else 100,
                latest_revision=item.latest_revision or False,
                tag=item.label,
            )
            result.append(allocation)
        return result

    def _convert_volumes(
        self,
        volumes: list[ACAVolume] | None,
    ) -> list[Volume]:
        if not volumes:
            return []
        result: list[Volume] = []
        for volume in volumes:
            if volume.storage_type == "EmptyDir":
                result.append(
                    Volume(
                        name=volume.name or "empty-dir",
                        type="emptyDir",
                    )
                )
        return result

    def _convert_scale_rule_auth(
        self, auth: list[ACAScaleRuleAuth] | None
    ) -> list[dict[str, str]] | None:
        result: list = []
        for auth_item in auth or []:
            item: dict = {}
            if auth_item.secret_ref:
                item["secret_ref"] = auth_item.secret_ref
            if auth_item.trigger_parameter:
                item["trigger_parameter"] = auth_item.trigger_parameter
            result.append(item)
        return result

    def _convert_scale_rule(self, scale_rule: ACAScaleRule) -> ScaleRule:
        if scale_rule.http:
            return ScaleRule(
                name=scale_rule.name,
                type="http",
                metadata=scale_rule.http.metadata,
                auth=self._convert_scale_rule_auth(scale_rule.http.auth),
            )
        elif scale_rule.tcp:
            return ScaleRule(
                name=scale_rule.name,
                type="tcp",
                metadata=scale_rule.tcp.metadata,
                auth=self._convert_scale_rule_auth(scale_rule.tcp.auth),
            )
        elif scale_rule.custom:
            return ScaleRule(
                name=scale_rule.name,
                type="custom",
                metadata=scale_rule.custom.metadata,
                auth=self._convert_scale_rule_auth(scale_rule.custom.auth),
            )
        else:
            raise ValueError(f"Unsupported scale rule type: {scale_rule}")

    def _convert_scale(self, scale: ACAScale | None) -> Scale | None:
        if not scale:
            return None

        rules = []
        for rule in scale.rules or []:
            rules.append(self._convert_scale_rule(rule))

        return Scale(
            min_replicas=scale.min_replicas,
            max_replicas=scale.max_replicas,
            cooldown_period=scale.cooldown_period,
            polling_interval=scale.polling_interval,
            rules=rules,
        )

    def convert_revisions(
        self, revisions: list[ACARevision]
    ) -> list[Revision]:
        return [self.convert_revision(revision) for revision in revisions]

    def convert_revision(self, revision: ACARevision) -> Revision:
        return Revision(
            name=revision.name or "default",
            traffic=revision.traffic_weight,
            created_time=(
                revision.created_time.timestamp()
                if revision.created_time
                else None
            ),
            active=revision.active,
            containers=(
                [
                    self._convert_container(container)
                    for container in revision.template.containers
                ]
                if revision.template
                else []
            ),
            volumes=self._convert_volumes(
                revision.template.volumes if revision.template else []
            ),
        )

    def convert_services(
        self,
        services: list[ACAContainerApp],
    ) -> list[ServiceItem]:
        result: list[ServiceItem] = []
        for service in services:
            result.append(self.convert_service_item(service))
        return result

    def _get_service_uri(self, container_app: ACAContainerApp) -> str | None:
        """Extract the public URL from the container app response."""
        ingress = getattr(container_app.configuration, "ingress", None)
        if ingress and ingress.fqdn:
            scheme = "https" if ingress.external else "http"
            port = (
                f":{ingress.target_port}"
                if ingress.target_port and ingress.target_port not in [80, 443]
                else ""
            )
            return f"{scheme}://{ingress.fqdn}{port}"
        return None

    def get_images(self, container_app: ACAContainerApp) -> list[str]:
        """Extract the image URIs from the container app response."""
        if container_app.template and container_app.template.containers:
            return [
                container.image
                for container in container_app.template.containers
                if container.image is not None
            ]
        return []
