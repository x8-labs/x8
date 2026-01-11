"""
Google cloud run container deployment.
"""

from __future__ import annotations

__all__ = ["GoogleCloudRun"]

from typing import Any

from google.api_core.exceptions import FailedPrecondition, NotFound
from google.cloud import run_v2

from x8._common.google_provider import GoogleProvider
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, OperationParser, Response, RunContext
from x8.core.exceptions import (
    BadRequestError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import Expression

from .._helper import amap_images, map_images, requires_container_registry
from .._models import (
    Container,
    EnvVar,
    GPUResource,
    GRPCAction,
    HTTPGetAction,
    HTTPHeader,
    Ingress,
    Port,
    Probe,
    ProbeSet,
    ResourceLimits,
    ResourceRequirements,
    Revision,
    Scale,
    ServiceDefinition,
    ServiceItem,
    ServiceOverlay,
    TCPSocketAction,
    TrafficAllocation,
    Volume,
    VolumeMount,
)
from ._base import BaseContainerDeploymentProvider


class GoogleCloudRun(GoogleProvider, BaseContainerDeploymentProvider):
    project: str | None
    location: str
    service_name: str | None
    nparams: dict[str, Any]

    _credentials: Any
    _services_client: run_v2.ServicesClient
    _aservices_client: run_v2.ServicesAsyncClient
    _revisions_client: run_v2.RevisionsClient
    _arevisions_client: run_v2.RevisionsAsyncClient
    _init: bool
    _ainit: bool
    _op_converter: OperationConverter
    _result_converter: ResultConverter
    _cached_project: str | None

    def __init__(
        self,
        project: str | None = None,
        location: str = "us-central1",
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        allow_unauthenticated_access: bool | None = True,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            project:
                Google project id.
            location:
                Google cloud location.
            service_name:
                Cloud run service name.
            service:
                Service definition to deploy.
            service_override:
                Service override definition to apply.
            containerizer:
                Containerizer to use for building the container.
            container_registry:
                Container registry to use for pushing the container.
            service_account_info:
                Google service account info with serialized credentials.
            service_account_file:
                Google service account file with credentials.
            access_token:
                Google access token.
            nparams:
                Native params to google client.
        """
        self.project = project
        self.location = location
        self.service_name = service_name
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry
        self.allow_unauthenticated_access = allow_unauthenticated_access
        self.nparams = nparams

        self._credentials = None
        self._init = False
        self._ainit = False
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()
        self._cached_project = None

        GoogleProvider.__init__(
            self,
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self._credentials = self._get_credentials()
        self._services_client = run_v2.ServicesClient(
            credentials=self._credentials
        )
        self._revisions_client = run_v2.RevisionsClient(
            credentials=self._credentials
        )
        self._init = True

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._ainit:
            return

        self._credentials = self._get_credentials()
        self._aservices_client = run_v2.ServicesAsyncClient(
            credentials=self._credentials
        )
        self._arevisions_client = run_v2.RevisionsAsyncClient(
            credentials=self._credentials
        )
        self._ainit = True

    def _ensure_container_registry(
        self, service: ServiceDefinition
    ) -> ContainerRegistry | None:
        container_registry = self._get_container_registry()
        if container_registry:
            return container_registry
        if not requires_container_registry(service):
            return None

        from x8.compute.container_registry.providers.google_artifact_registry import (  # noqa
            GoogleArtifactRegistry,
        )

        service_name = self.service_name or service.name
        gcr_name = f"{service_name}-gcr"
        gcr = GoogleArtifactRegistry(
            project=self.project,
            location=self.location,
            name=gcr_name,
            service_account_info=self.service_account_info,
            service_account_file=self.service_account_file,
            access_token=self.access_token,
        )
        gcr.create_resource(format="DOCKER", mode="STANDARD")
        print(f"Created container registry: {gcr_name}")
        return ContainerRegistry(__provider__=gcr)

    def _check_delete_container_registry(self, name: str) -> None:
        container_registry = self._get_container_registry()
        if container_registry:
            return None

        from x8.compute.container_registry.providers.google_artifact_registry import (  # noqa
            GoogleArtifactRegistry,
        )

        gcr_name = f"{name}-gcr"
        gcr = GoogleArtifactRegistry(
            project=self.project,
            location=self.location,
            name=gcr_name,
            service_account_info=self.service_account_info,
            service_account_file=self.service_account_file,
            access_token=self.access_token,
        )
        try:
            gcr.get_resource()
            gcr.delete_resource()
            print(f"Deleted container registry: {gcr_name}")
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
        client = self._services_client
        where_exists = OperationParser.parse_where_exists(where)
        service_def = self._normalize_service_definition(service)
        project = self._get_project()
        service_name, parent_path, service_path = self._get_service_paths(
            project, service_def.name
        )
        try:
            existing_service = client.get_service(name=service_path)
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {service_name} already exists."
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
            updated_service = self._op_converter.update_service(
                existing_service,
                service_def,
                images,
            )
            operation = client.update_service(service=updated_service)
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {response.uri}")
        except NotFound:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {service_name} not found."
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
            gcr_service = self._op_converter.convert_service(
                service_def, images
            )
            operation = client.create_service(
                parent=parent_path,
                service=gcr_service,
                service_id=service_name,
            )
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service created successfully: {response.uri}")
        if not service_def.ingress or service_def.ingress.external:
            self._allow_unauthenticated_access(project, service_name)
        return Response(result=result)

    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        client = self._aservices_client
        where_exists = OperationParser.parse_where_exists(where)
        service_def = self._normalize_service_definition(service)
        project = self._get_project()
        service_name, parent_path, service_path = self._get_service_paths(
            project, service_def.name
        )
        try:
            existing_service = await client.get_service(name=service_path)
            if where_exists is False:
                raise PreconditionFailedError(
                    f"Service {service_name} already exists."
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
            updated_service = self._op_converter.update_service(
                existing_service,
                service_def,
                images,
            )
            operation = await client.update_service(service=updated_service)
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service updated successfully: {response.uri}")
        except NotFound:
            if where_exists is True:
                raise PreconditionFailedError(
                    f"Service {service_name} not found."
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
            gcr_service = self._op_converter.convert_service(
                service_def, images
            )
            operation = await client.create_service(
                parent=parent_path,
                service=gcr_service,
                service_id=service_name,
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            print(f"Service created successfully: {response.uri}")
        if not service_def.ingress or service_def.ingress.external:
            self._allow_unauthenticated_access(project, service_name)
        return Response(result=result)

    def get_service(self, name: str) -> Response[ServiceItem]:
        self.__setup__()
        client = self._services_client
        project = self._get_project()
        try:
            response = client.get_service(
                name=self._get_service_path(project, name)
            )
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    async def aget_service(self, name: str) -> Response[ServiceItem]:
        await self.__asetup__()
        client = self._aservices_client
        project = self._get_project()
        try:
            response = await client.get_service(
                name=self._get_service_path(project, name)
            )
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    def delete_service(self, name: str) -> Response[None]:
        self.__setup__()
        client = self._services_client
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        try:
            client.delete_service(name=service_path)
            print(f"Service {name} deleted successfully.")
            self._check_delete_container_registry(name)
            return Response(result=None)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    async def adelete_service(self, name: str) -> Response[None]:
        await self.__asetup__()
        client = self._aservices_client
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        try:
            await client.delete_service(name=service_path)
            print(f"Service {name} deleted successfully.")
            self._check_delete_container_registry(name)
            return Response(result=None)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    def list_services(self) -> Response[list[ServiceItem]]:
        self.__setup__()
        project = self._get_project()
        parent_path = f"projects/{project}/locations/{self.location}"
        request = run_v2.ListServicesRequest(
            parent=parent_path,
            page_size=1000,
            show_deleted=False,
        )
        services = self._services_client.list_services(request=request)
        result = self._result_converter.convert_services(
            services=list(services)
        )
        return Response(result=result)

    async def alist_services(self) -> Response[list[ServiceItem]]:
        await self.__asetup__()
        project = self._get_project()
        parent_path = f"projects/{project}/locations/{self.location}"
        request = run_v2.ListServicesRequest(
            parent=parent_path,
            page_size=1000,
            show_deleted=False,
        )
        services = await self._aservices_client.list_services(request=request)
        result = self._result_converter.convert_services(
            services=[service async for service in services]
        )
        return Response(result=result)

    def list_revisions(self, name: str) -> Response[list[Revision]]:
        self.__setup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        request = run_v2.ListRevisionsRequest(
            parent=service_path,
            page_size=1000,
            show_deleted=False,
        )
        try:
            service = self._services_client.get_service(name=service_path)
            revisions = self._revisions_client.list_revisions(request=request)
            result = self._result_converter.convert_revisions(
                revisions=list(revisions),
                service=service,
            )
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    async def alist_revisions(self, name: str) -> Response[list[Revision]]:
        await self.__asetup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        request = run_v2.ListRevisionsRequest(
            parent=service_path,
            page_size=1000,
            show_deleted=False,
        )
        try:
            service = await self._aservices_client.get_service(
                name=service_path
            )
            revisions = await self._arevisions_client.list_revisions(
                request=request
            )
            result = self._result_converter.convert_revisions(
                revisions=[rev async for rev in revisions],
                service=service,
            )
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    def get_revision(self, name: str, revision: str) -> Response[Revision]:
        self.__setup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        revision_path = f"{service_path}/revisions/{revision}"
        try:
            service = self._services_client.get_service(name=service_path)
            revision = self._revisions_client.get_revision(name=revision_path)
            result = self._result_converter.convert_revision(
                revision=revision,
                service=service,
            )
            return Response(result=result)
        except NotFound:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
            )

    async def aget_revision(
        self, name: str, revision: str
    ) -> Response[Revision]:
        await self.__asetup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        revision_path = f"{service_path}/revisions/{revision}"
        try:
            service = await self._aservices_client.get_service(
                name=service_path
            )
            revision = await self._arevisions_client.get_revision(
                name=revision_path
            )
            result = self._result_converter.convert_revision(
                revision=revision,
                service=service,
            )
            return Response(result=result)
        except NotFound:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
            )

    def delete_revision(self, name: str, revision: str) -> Response[None]:
        self.__setup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        revision_path = f"{service_path}/revisions/{revision}"
        try:
            self._revisions_client.delete_revision(name=revision_path)
            return Response(result=None)
        except NotFound:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
            )
        except FailedPrecondition:
            raise PreconditionFailedError(
                f"Revision {revision} of service {name} cannot be deleted."
            )

    async def adelete_revision(
        self, name: str, revision: str
    ) -> Response[None]:
        await self.__asetup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        revision_path = f"{service_path}/revisions/{revision}"
        try:
            await self._arevisions_client.delete_revision(name=revision_path)
            return Response(result=None)
        except NotFound:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
            )
        except FailedPrecondition:
            raise PreconditionFailedError(
                f"Revision {revision} of service {name} cannot be deleted."
            )

    def update_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
    ) -> Response[ServiceItem]:
        self.__setup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        try:
            service = self._services_client.get_service(name=service_path)
            traffic_targets = self._op_converter.convert_traffic(traffic)
            service.traffic = traffic_targets
            operation = self._services_client.update_service(service=service)
            response = operation.result()
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    async def aupdate_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
    ) -> Response[ServiceItem]:
        await self.__asetup__()
        project = self._get_project()
        service_path = self._get_service_path(project, name)
        try:
            service = await self._aservices_client.get_service(
                name=service_path
            )
            traffic_targets = self._op_converter.convert_traffic(traffic)
            service.traffic = traffic_targets
            operation = await self._aservices_client.update_service(
                service=service
            )
            response = await operation.result()
            result = self._result_converter.convert_service_item(response)
            return Response(result=result)
        except NotFound:
            raise NotFoundError(f"Service {name} not found.")

    def close(self) -> None:
        self._init = False

    async def aclose(self) -> None:
        self._ainit = False

    def _get_project(self) -> str:
        if self.project:
            return self.project
        if self._cached_project:
            return self._cached_project
        self._cached_project = self._get_default_project()
        if not self._cached_project:
            raise BadRequestError("Project is required.")
        return self._cached_project

    def _get_default_project(self) -> str:
        import google.auth

        _, project = google.auth.default()
        return project

    def _get_service_path(self, project: str, service_name: str) -> str:
        parent_path = f"projects/{project}/locations/{self.location}"
        service_path = f"{parent_path}/services/{service_name}"
        return service_path

    def _get_service_paths(
        self,
        project: str,
        service_name: str | None,
    ) -> tuple[str, str, str]:
        service_name = self.service_name or service_name
        if not service_name:
            raise BadRequestError("Service name is required.")
        parent_path = f"projects/{project}/locations/{self.location}"
        service_path = f"{parent_path}/services/{service_name}"
        return service_name, parent_path, service_path

    def _allow_unauthenticated_access(self, project: str, service_name: str):
        from googleapiclient.discovery import build

        client = build("run", "v1")
        parent = f"projects/{project}/locations/{self.location}"
        service_path = f"{parent}/services/{service_name}"
        request = (
            client.projects()
            .locations()
            .services()
            .getIamPolicy(resource=service_path)
        )
        policy = request.execute()
        binding = {
            "role": "roles/run.invoker",
            "members": ["allUsers"],
        }
        policy.setdefault("bindings", []).append(binding)
        set_policy_request = (
            client.projects()
            .locations()
            .services()
            .setIamPolicy(
                resource=service_path,
                body={"policy": policy},
            )
        )
        set_policy_request.execute()
        print("Allowed unauthenticated access")


class OperationConverter:
    def update_service(
        self,
        existing_service: run_v2.Service,
        service: ServiceDefinition,
        images: list[str],
    ) -> run_v2.Service:
        gcr_service = self.convert_service(
            service=service,
            images=images,
        )
        existing_service.template.containers = gcr_service.template.containers
        existing_service.template.scaling = gcr_service.template.scaling
        existing_service.template.volumes = gcr_service.template.volumes
        existing_service.scaling = gcr_service.scaling
        existing_service.ingress = gcr_service.ingress
        return existing_service

    def convert_service(
        self,
        service: ServiceDefinition,
        images: list[str],
    ) -> run_v2.Service:
        containers: list[run_v2.Container] = []
        for container, image in zip(service.containers, images):
            containers.append(
                self._convert_container(
                    container=container,
                    image=image,
                    service=service,
                )
            )
        return run_v2.Service(
            template=run_v2.RevisionTemplate(
                containers=containers,
                scaling=self._convert_revision_scaling(
                    scale=service.scale,
                ),
                volumes=self._convert_volumes(service.volumes),
            ),
            scaling=self._convert_service_scaling(
                scale=service.scale,
            ),
            ingress=self._convert_ingress(
                ingress=service.ingress,
            ),
            traffic=self.convert_traffic(
                traffic=service.traffic,
            ),
        )

    def _convert_ingress(
        self, ingress: Ingress | None
    ) -> run_v2.IngressTraffic:
        if ingress is None:
            return run_v2.IngressTraffic(
                run_v2.IngressTraffic.INGRESS_TRAFFIC_ALL
            )
        if ingress.external:
            return run_v2.IngressTraffic(
                run_v2.IngressTraffic.INGRESS_TRAFFIC_ALL
            )
        return run_v2.IngressTraffic(
            run_v2.IngressTraffic.INGRESS_TRAFFIC_INTERNAL_ONLY
        )

    def convert_traffic(
        self, traffic: list[TrafficAllocation] | None
    ) -> list[run_v2.TrafficTarget]:
        if traffic is None:
            return [
                run_v2.TrafficTarget(
                    percent=100,
                    type_=run_v2.TrafficTargetAllocationType(
                        run_v2.TrafficTargetAllocationType.TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST  # noqa
                    ),
                )
            ]
        result: list[run_v2.TrafficTarget] = []
        for item in traffic:
            tt = run_v2.TrafficTarget(
                revision=item.revision,
                percent=int(item.percent),
            )
            if item.latest_revision:
                tt.type_ = run_v2.TrafficTargetAllocationType(
                    run_v2.TrafficTargetAllocationType.TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST  # noqa
                )
            else:
                tt.type_ = run_v2.TrafficTargetAllocationType(
                    run_v2.TrafficTargetAllocationType.TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION  # noqa
                )
            if item.tag:
                tt.tag = item.tag
            result.append(tt)
        return result

    def _convert_volumes(
        self,
        volumes: list[Volume],
    ) -> list[run_v2.Volume]:
        result: list[run_v2.Volume] = []
        for volume in volumes:
            if volume.type == "emptyDir":
                result.append(
                    run_v2.Volume(
                        name=volume.name,
                        empty_dir=run_v2.EmptyDirVolumeSource(
                            size_limit=volume.size_limit,
                        ),
                    )
                )
        return result

    def _convert_service_scaling(
        self, scale: Scale | None
    ) -> run_v2.ServiceScaling:
        if scale is None:
            return run_v2.ServiceScaling(
                scaling_mode=run_v2.ServiceScaling.ScalingMode.AUTOMATIC,
                min_instance_count=0,
            )
        if scale.mode == "manual":
            return run_v2.ServiceScaling(
                scaling_mode=run_v2.ServiceScaling.ScalingMode.MANUAL,
                manual_instance_count=scale.replicas or 1,
            )
        return run_v2.ServiceScaling(
            scaling_mode=run_v2.ServiceScaling.ScalingMode.AUTOMATIC,
            min_instance_count=scale.min_replicas or 1,
        )

    def _convert_revision_scaling(
        self,
        scale: Scale | None,
    ) -> run_v2.RevisionScaling:
        if scale is None:
            return run_v2.RevisionScaling(
                min_instance_count=0,
                max_instance_count=10,
            )
        return run_v2.RevisionScaling(
            min_instance_count=scale.min_replicas or 0,
            max_instance_count=scale.max_replicas or 10,
        )

    def _convert_container(
        self,
        container: Container,
        image: str,
        service: ServiceDefinition,
    ) -> run_v2.Container:
        args: dict[str, Any] = {}
        if container.ports:
            args["ports"] = self._convert_ports(container.ports)
        if container.resources:
            args["resources"] = self._convert_resource_requirements(
                container.resources
            )
        if container.volume_mounts:
            args["volume_mounts"] = self._convert_volume_mounts(
                container.volume_mounts
            )
        if container.probes:
            if container.probes.liveness_probe:
                args["liveness_probe"] = self._convert_probe(
                    container.probes.liveness_probe
                )
            if container.probes.startup_probe:
                args["startup_probe"] = self._convert_probe(
                    container.probes.startup_probe
                )
        depends_on = self._convert_depends_on(
            container_name=container.name,
            service=service,
        )
        if depends_on:
            args["depends_on"] = depends_on
        return run_v2.Container(
            name=container.name,
            image=image,
            command=container.command or [],
            args=container.args or [],
            working_dir=container.working_dir,
            env=self._convert_env(container.env),
            **args,
        )

    def _convert_env(self, env: list[EnvVar]) -> list[run_v2.EnvVar]:
        return [
            run_v2.EnvVar(name=var.name, value=str(var.value)) for var in env
        ]

    def _convert_ports(
        self,
        ports: list[Port],
    ) -> list[run_v2.ContainerPort]:
        return [
            run_v2.ContainerPort(
                container_port=port.container_port,
                name=port.name,
            )
            for port in ports
        ]

    def _convert_resource_requirements(
        self,
        resources: ResourceRequirements,
    ) -> run_v2.ResourceRequirements | None:
        cpu = "1"
        memory = "512Mi"
        limits = {
            "cpu": cpu,
            "memory": memory,
        }
        if resources.limits:
            if resources.limits.cpu:
                cpu = str(resources.limits.cpu)
                limits["cpu"] = cpu
            if resources.limits.memory:
                memory = f"{str(resources.limits.memory)}Mi"
                limits["memory"] = memory
            if resources.limits.gpu and resources.limits.gpu.count:
                limits["nvidia.com/gpu"] = str(resources.limits.gpu.count)

        return run_v2.ResourceRequirements(
            limits=limits,
            cpu_idle=resources.cpu_idle,
            startup_cpu_boost=resources.cpu_boost,
        )

    def _convert_volume_mounts(
        self,
        volume_mounts: list[VolumeMount],
    ) -> list[run_v2.VolumeMount] | None:
        return [
            run_v2.VolumeMount(
                name=mount.name,
                mount_path=mount.mount_path,
            )
            for mount in volume_mounts
        ]

    def _convert_probe(self, probe: Probe | None) -> run_v2.Probe | None:
        if not probe:
            return None
        args: dict = {}
        if probe.http_get:
            args["http_get"] = self._convert_http_get_action(probe.http_get)
        elif probe.grpc:
            args["grpc"] = self._convert_grpc_action(probe.grpc)
        elif probe.tcp_socket:
            args["tcp_socket"] = self._convert_tcp_socket_action(
                probe.tcp_socket
            )
        return run_v2.Probe(
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            failure_threshold=probe.failure_threshold,
            **args,
        )

    def _convert_http_get_action(
        self, action: HTTPGetAction
    ) -> run_v2.HTTPGetAction:
        headers = [
            run_v2.HTTPHeader(name=header.name, value=header.value)
            for header in action.http_headers
        ]
        return run_v2.HTTPGetAction(
            path=action.path,
            port=action.port,
            http_headers=headers,
        )

    def _convert_grpc_action(self, action: GRPCAction) -> run_v2.GRPCAction:
        return run_v2.GRPCAction(
            port=action.port,
            service=action.service,
        )

    def _convert_tcp_socket_action(
        self, action: TCPSocketAction
    ) -> run_v2.TCPSocketAction:
        return run_v2.TCPSocketAction(
            port=action.port,
        )

    def _convert_depends_on(
        self,
        container_name: str,
        service: ServiceDefinition,
    ) -> list[str]:
        init_containers: list[str] = []
        for container in service.containers:
            if container.type == "init":
                init_containers.append(container.name)
        if container_name in init_containers:
            return []
        return init_containers


class ResultConverter:
    def convert_service_item(
        self,
        service: run_v2.Service,
    ) -> ServiceItem:
        return ServiceItem(
            name=service.name.split("/")[-1],
            uri=service.uri,
            service=self.convert_service(service),
        )

    def convert_service(self, service: run_v2.Service) -> ServiceDefinition:
        return ServiceDefinition(
            name=service.name.split("/")[-1],
            containers=[
                self._convert_container(container)
                for container in service.template.containers
            ],
            ingress=self._convert_ingress(service.ingress),
            traffic=self._convert_traffic(
                list(service.traffic), service.latest_ready_revision
            ),
            volumes=self._convert_volumes(list(service.template.volumes)),
            scale=self._convert_scale(
                service.scaling, service.template.scaling
            ),
            latest_ready_revision=service.latest_ready_revision.split("/")[-1],
            latest_created_revision=service.latest_created_revision.split("/")[
                -1
            ],
        )

    def _convert_container(
        self,
        container: run_v2.Container,
    ) -> Container:
        return Container(
            name=container.name,
            image=container.image,
            command=(list(container.command) if container.command else []),
            args=list(container.args) if container.args else [],
            working_dir=container.working_dir,
            env=[
                EnvVar(name=env.name, value=env.value) for env in container.env
            ],
            ports=[
                Port(container_port=port.container_port, name=port.name)
                for port in container.ports
            ],
            resources=self._convert_resource_requirements(container.resources),
            volume_mounts=[
                VolumeMount(name=mount.name, mount_path=mount.mount_path)
                for mount in container.volume_mounts
            ],
            probes=ProbeSet(
                liveness_probe=self._convert_probe(container.liveness_probe),
                startup_probe=self._convert_probe(container.startup_probe),
            ),
        )

    def _convert_resource_requirements(
        self,
        resources: run_v2.ResourceRequirements | None,
    ) -> ResourceRequirements | None:
        if not resources:
            return None

        # In some protobuf maps, .limits behaves like a dict already;
        # normalize it.
        limits = dict(resources.limits)

        # --- CPU ---
        cpu_raw = limits.get("cpu")
        cpu_cores: float | None = None
        if cpu_raw is not None:
            if isinstance(cpu_raw, (int, float)):
                cpu_cores = float(cpu_raw)
            else:
                s = str(cpu_raw).strip()
                if s.endswith("m"):
                    # millicpu -> cores
                    num = s[:-1] or "0"
                    cpu_cores = float(num) / 1000.0
                else:
                    cpu_cores = float(s)  # e.g. "1", "0.5"

        # --- Memory (normalize to MiB integer) ---
        mem_raw = limits.get("memory")
        memory_mb: int | None = None
        if mem_raw is not None:
            ms = str(mem_raw).strip()
            try:
                if ms.endswith("Mi"):
                    memory_mb = int(float(ms[:-2]))
                elif ms.endswith("Gi"):
                    memory_mb = int(float(ms[:-2]) * 1024)
                elif ms.endswith("Ti"):
                    memory_mb = int(float(ms[:-2]) * 1024 * 1024)
                elif ms.isdigit():
                    # Assume MiB if bare number sneaks through
                    memory_mb = int(ms)
            except ValueError:
                # Leave as None if unparsable; better than crashing
                memory_mb = None

        # --- GPU (if present) ---
        gpu_count = None
        if "nvidia.com/gpu" in limits:
            try:
                gpu_count = int(limits.get("nvidia.com/gpu", 0))
            except (TypeError, ValueError):
                gpu_count = None

        return ResourceRequirements(
            limits=ResourceLimits(
                cpu=cpu_cores,
                memory=memory_mb,
                gpu=(
                    GPUResource(count=gpu_count)
                    if (gpu_count and gpu_count > 0)
                    else None
                ),
            ),
            cpu_idle=resources.cpu_idle,
            cpu_boost=resources.startup_cpu_boost,
        )

    def _convert_ingress(
        self,
        ingress: run_v2.IngressTraffic,
    ) -> Ingress:
        if ingress == run_v2.IngressTraffic.INGRESS_TRAFFIC_ALL:
            return Ingress(external=True)
        return Ingress(external=False)

    def _convert_traffic(
        self,
        traffic: list[run_v2.TrafficTarget],
        latest_revision_name: str,
    ) -> list[TrafficAllocation]:
        result: list[TrafficAllocation] = []
        for item in traffic:
            latest_revision = item.type_ == run_v2.TrafficTargetAllocationType(
                run_v2.TrafficTargetAllocationType.TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST  # noqa
            )
            if not item.revision and latest_revision:
                revision = latest_revision_name
            else:
                revision = item.revision
            result.append(
                TrafficAllocation(
                    revision=revision,
                    percent=item.percent,
                    latest_revision=latest_revision,
                    tag=item.tag,
                )
            )
        return result

    def _convert_volumes(
        self,
        volumes: list[run_v2.Volume],
    ) -> list[Volume]:
        result: list[Volume] = []
        for volume in volumes:
            if volume.empty_dir:
                result.append(
                    Volume(
                        name=volume.name,
                        type="emptyDir",
                        size_limit=volume.empty_dir.size_limit,
                    )
                )
        return result

    def _convert_scale(
        self,
        service_scaling: run_v2.ServiceScaling,
        revision_scaling: run_v2.RevisionScaling,
    ) -> Scale:
        if (
            service_scaling.scaling_mode
            == run_v2.ServiceScaling.ScalingMode.MANUAL
        ):
            return Scale(
                mode="manual",
                replicas=service_scaling.manual_instance_count or 1,
            )
        return Scale(
            mode="auto",
            min_replicas=revision_scaling.min_instance_count or 0,
            max_replicas=revision_scaling.max_instance_count or 10,
        )

    def _convert_probe(
        self,
        probe: run_v2.Probe | None,
    ) -> Probe | None:
        if not probe:
            return None
        args: dict = {}
        if probe.http_get:
            args["http_get"] = self._convert_http_get_action(probe.http_get)
        elif probe.grpc:
            args["grpc"] = self._convert_grpc_action(probe.grpc)
        elif probe.tcp_socket:
            args["tcp_socket"] = self._convert_tcp_socket_action(
                probe.tcp_socket
            )
        return Probe(
            initial_delay_seconds=probe.initial_delay_seconds,
            timeout_seconds=probe.timeout_seconds,
            period_seconds=probe.period_seconds,
            failure_threshold=probe.failure_threshold,
            **args,
        )

    def _convert_http_get_action(
        self, action: run_v2.HTTPGetAction
    ) -> HTTPGetAction:
        headers = [
            HTTPHeader(name=header.name, value=header.value)
            for header in action.http_headers
        ]
        return HTTPGetAction(
            path=action.path,
            port=action.port,
            http_headers=headers,
        )

    def _convert_grpc_action(self, action: run_v2.GRPCAction) -> GRPCAction:
        return GRPCAction(
            port=action.port,
            service=action.service,
        )

    def _convert_tcp_socket_action(
        self, action: run_v2.TCPSocketAction
    ) -> TCPSocketAction:
        return TCPSocketAction(
            port=action.port,
        )

    def convert_revisions(
        self,
        revisions: list[run_v2.Revision],
        service: run_v2.Service,
    ) -> list[Revision]:
        result: list[Revision] = []
        for rev in revisions:
            result.append(self.convert_revision(revision=rev, service=service))
        return result

    def convert_revision(
        self,
        revision: run_v2.Revision,
        service: run_v2.Service,
    ) -> Revision:
        traffic_map = self._get_traffic_map(service)
        return Revision(
            name=revision.name.split("/")[-1],
            traffic=traffic_map.get(revision.name.split("/")[-1], 0),
            created_time=(
                revision.create_time.timestamp()
                if revision.create_time
                else None
            ),  # noqa
            active=self._is_revision_active(revision),
            containers=[
                self._convert_container(container)
                for container in revision.containers
            ],
            volumes=self._convert_volumes(list(revision.volumes)),
        )

    def convert_services(
        self, services: list[run_v2.Service]
    ) -> list[ServiceItem]:
        result: list[ServiceItem] = []
        for service in services:
            result.append(self.convert_service_item(service))
        return result

    def _get_traffic_map(self, service: run_v2.Service) -> dict[str, int]:
        result: dict = {}
        for t in service.traffic:
            if t.type_ == run_v2.TrafficTargetAllocationType(
                run_v2.TrafficTargetAllocationType.TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST  # noqa
            ):
                result[service.latest_ready_revision.split("/")[-1]] = 100
            else:
                result[t.revision] = t.percent
        return result

    def _is_revision_active(self, revision: run_v2.Revision) -> bool:
        if not revision.conditions:
            return False

        for condition in revision.conditions:
            if condition.type_ == "Ready":
                return (
                    condition.state
                    == run_v2.Condition.State.CONDITION_SUCCEEDED
                )
        return False
