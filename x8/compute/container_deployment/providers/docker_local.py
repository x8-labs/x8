"""
Local Docker container deployment provider.
"""

from __future__ import annotations

from typing import Literal

__all__ = ["DockerLocal"]

import time
from typing import Any

import docker
from docker.errors import NotFound
from docker.models.containers import Container as DockerContainer
from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, OperationParser, Response, RunContext
from x8.core.constants import ROOT_PACKAGE_NAME
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
    Ingress,
    Port,
    ResourceLimits,
    ResourceRequirements,
    Revision,
    Scale,
    ServiceDefinition,
    ServiceItem,
    ServiceOverlay,
    TrafficAllocation,
    VolumeMount,
)
from ._base import BaseContainerDeploymentProvider

LABEL_PREFIX = ROOT_PACKAGE_NAME


class DockerLocal(BaseContainerDeploymentProvider):
    service_name: str | None

    _client: docker.DockerClient
    _init: bool
    _op_converter: OperationConverter
    _result_converter: ResultConverter

    def __init__(
        self,
        service_name: str | None = None,
        service: ServiceDefinition | None = None,
        service_override: ServiceOverlay | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize Local Docker provider.

        Args:
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
        """
        self.service_name = service_name
        self.service = service
        self.overlay = service_override
        self.containerizer = containerizer
        self.container_registry = container_registry

        self._init = False
        self._op_converter = OperationConverter()
        self._result_converter = ResultConverter()

        super().__init__(**kwargs)

    def __supports__(self, feature):
        return feature not in [
            ContainerDeploymentFeature.MULTIPLE_REVISIONS,
            ContainerDeploymentFeature.REVISION_DELETE,
        ]

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        self._client = docker.from_env()
        self._init = True

    def _ensure_container_registry(
        self, service: ServiceDefinition
    ) -> ContainerRegistry | None:
        container_registry = self._get_container_registry()
        if container_registry:
            return container_registry
        if not requires_container_registry(service):
            return None

        from x8.compute.container_registry.providers.docker_local import (  # noqa
            DockerLocal,
        )

        return ContainerRegistry(__provider__=DockerLocal())

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
        existing_containers = self._get_service_containers(service_name)

        if existing_containers and where_exists is False:
            raise PreconditionFailedError(
                f"Service {service_def.name} already exists."
            )

        if not existing_containers and where_exists is True:
            raise PreconditionFailedError(
                f"Service {service_def.name} not found."
            )

        images = map_images(
            containers=service_def.containers,
            images=service_def.images,
            containerizer=self._get_containerizer(),
            container_registry=self._ensure_container_registry(service_def),
            run_context=run_context,
        )

        self._ensure_network(service_name)

        if existing_containers:
            self._stop_and_remove_containers(existing_containers)

        revision = "revision:" + str(int(time.time()))
        container_specs = (
            self._op_converter.convert_service_to_container_specs(
                service_name,
                service_def,
                images,
                revision,
            )
        )
        self._create_containers_from_specs(container_specs)
        result = self._result_converter.convert_service_item(
            service_name, service_def
        )

        action = "updated" if existing_containers else "created"
        print(f"Service {action} successfully: {service_def.name}")

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

    def get_service(self, name: str) -> Response[ServiceItem]:
        self.__setup__()

        containers = self._get_service_containers(name)
        if not containers:
            raise NotFoundError(f"Service {name} not found.")

        service_def = self._result_converter.convert_service(containers)
        result = self._result_converter.convert_service_item(
            name,
            service_def,
        )

        return Response(result=result)

    def delete_service(self, name: str) -> Response[None]:
        self.__setup__()

        containers = self._get_service_containers(name)
        if not containers:
            raise NotFoundError(f"Service {name} not found.")

        self._stop_and_remove_containers(containers)
        self._delete_network(name)
        return Response(result=None)

    def list_services(self) -> Response[list[ServiceItem]]:
        self.__setup__()

        containers = self._client.containers.list(
            all=True, filters={"label": f"{LABEL_PREFIX}.managed=true"}
        )

        services: dict = {}
        for container in containers:
            service_name = container.labels.get(f"{LABEL_PREFIX}.service")
            if service_name:
                if service_name not in services:
                    services[service_name] = []
                services[service_name].append(container)

        result = []
        for service_name, service_containers in services.items():
            service_def = self._result_converter.convert_service(
                service_containers
            )
            service_item = self._result_converter.convert_service_item(
                service_name, service_def
            )
            result.append(service_item)

        return Response(result=result)

    def list_revisions(self, name: str) -> Response[list[Revision]]:
        self.__setup__()

        containers = self._get_service_containers(name)
        if not containers:
            raise NotFoundError(f"Service {name} not found.")

        revisions = []
        revision = self._result_converter.convert_revision(containers)
        revisions.append(revision)
        return Response(result=revisions)

    def get_revision(self, name: str, revision: str) -> Response[Revision]:
        self.__setup__()

        try:
            containers = self._get_revision_containers(name, revision)
            result = self._result_converter.convert_revision(containers)
            return Response(result=result)
        except NotFound:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
            )

    def delete_revision(self, name: str, revision: str) -> Response[None]:
        self.__setup__()
        containers = self._get_revision_containers(name, revision)
        if not containers:
            raise NotFoundError(
                f"Revision {revision} of service {name} not found."
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

    def close(self) -> None:
        if hasattr(self, "_client"):
            self._client.close()
        self._init = False

    def _create_containers_from_specs(
        self, container_specs: list[dict[str, Any]]
    ) -> list[DockerContainer]:
        containers = []

        # Separate init and regular containers
        init_specs = [spec for spec in container_specs if spec.get("is_init")]
        main_specs = [
            spec for spec in container_specs if not spec.get("is_init")
        ]

        # Run init containers first and wait for completion
        for spec in init_specs:
            container = self._create_single_container(spec)
            container.wait()  # Wait for init container to complete
            containers.append(container)

        # find the main spec, run it first
        main_idx = next(
            (
                i
                for i, s in enumerate(main_specs)
                if s.get("labels", {}).get(f"{LABEL_PREFIX}.type") == "main"
            ),
            0,
        )
        main_spec = main_specs.pop(main_idx)
        main_container = self._create_single_container(main_spec)
        containers.append(main_container)
        time.sleep(5)  # Give time for the main container to start

        # Run regular containers
        for spec in main_specs:
            if spec.get("labels", {}).get(f"{LABEL_PREFIX}.type") == "side":
                spec = spec.copy()
                spec.pop("network", None)  # can't combine with network_mode
                spec.pop("ports", None)  # can't publish when sharing netns
                spec["network_mode"] = f"container:{main_container.id}"
            container = self._create_single_container(spec)
            containers.append(container)
            time.sleep(5)

        return containers

    def _create_single_container(
        self, spec: dict[str, Any]
    ) -> DockerContainer:
        """Create a single Docker container from specification."""
        # Remove custom fields that aren't Docker API parameters
        docker_spec = spec.copy()
        docker_spec.pop("is_init", None)

        return self._client.containers.run(**docker_spec)

    def _get_service_containers(
        self, service_name: str
    ) -> list[DockerContainer]:
        try:
            containers = self._client.containers.list(
                all=True,
                filters={
                    "label": [
                        f"{LABEL_PREFIX}.managed=true",
                        f"{LABEL_PREFIX}.service={service_name}",
                    ]
                },
            )
            return containers
        except Exception:
            return []

    def _get_revision_containers(
        self, service_name: str, revision: str
    ) -> list[DockerContainer]:
        try:
            containers = self._client.containers.list(
                all=True,
                filters={
                    "label": [
                        f"{LABEL_PREFIX}.managed=true",
                        f"{LABEL_PREFIX}.service={service_name}",
                        f"{LABEL_PREFIX}.revision={revision}",
                    ]
                },
            )
            return containers
        except Exception:
            return []

    def _stop_and_remove_containers(
        self, containers: list[DockerContainer]
    ) -> None:
        """Stop and remove containers."""
        for container in containers:
            try:
                container.stop(timeout=10)
                container.remove()
            except Exception as e:
                print(f"Error removing container {container.name}: {e}")

    def _ensure_network(self, service_name) -> None:
        try:
            self._client.networks.get(service_name)
        except NotFound:
            self._client.networks.create(
                service_name,
                driver="bridge",
                labels={f"{LABEL_PREFIX}.managed": "true"},
            )

    def _delete_network(self, service_name: str) -> None:
        try:
            network = self._client.networks.get(service_name)
            network.remove()
        except NotFound:
            pass

    def _get_service_name(self, service: ServiceDefinition) -> str:
        if self.service_name:
            return self.service_name
        if service.name:
            return service.name
        raise BadRequestError(
            "Service name must be provided or defined in the service."
        )


class OperationConverter:
    def convert_service_to_container_specs(
        self,
        service_name: str,
        service: ServiceDefinition,
        images: list[str],
        revision: str,
    ) -> list[dict[str, Any]]:
        container_specs = []

        for container_def, image in zip(service.containers, images):
            if container_def.type == "main" and container_def.ports:
                inferred_type = "main"
            elif container_def.type == "init":
                inferred_type = "init"
            else:
                inferred_type = "side"
            spec = self._convert_container_to_spec(
                service_name,
                service,
                container_def,
                image,
                inferred_type,
                revision,
            )
            container_specs.append(spec)

        return container_specs

    def _convert_container_to_spec(
        self,
        service_name: str,
        service: ServiceDefinition,
        container_def: Container,
        image: str,
        inferred_type: str,
        revision: str,
    ) -> dict[str, Any]:
        """Convert a single container definition to Docker spec."""
        is_init = container_def.type == "init"

        # Prepare environment variables
        environment = {}
        for env_var in container_def.env:
            environment[env_var.name] = str(env_var.value)
        explicit_env_names = ",".join(sorted(environment.keys()))

        # Prepare port bindings
        ports: dict = {}
        if container_def.ports and not is_init:
            for port in container_def.ports:
                container_port = f"{port.container_port}/tcp"
                ports[container_port] = port.host_port or port.container_port

        # Prepare volumes
        volumes = {}
        if container_def.volume_mounts:
            for mount in container_def.volume_mounts:
                volumes[mount.name] = {"bind": mount.mount_path, "mode": "rw"}

        # Prepare labels
        labels = {
            f"{LABEL_PREFIX}.managed": "true",
            f"{LABEL_PREFIX}.service": service.name,
            f"{LABEL_PREFIX}.container": container_def.name,
            f"{LABEL_PREFIX}.type": inferred_type,
            f"{LABEL_PREFIX}.created": str(int(time.time())),
            f"{LABEL_PREFIX}.env_names": explicit_env_names,
            f"{LABEL_PREFIX}.revision": revision,
        }

        # Prepare resource constraints
        mem_limit = None
        cpu_period = None
        cpu_quota = None

        if container_def.resources and container_def.resources.limits:
            if container_def.resources.limits.memory:
                mem_limit = container_def.resources.limits.memory
            if container_def.resources.limits.cpu:
                cpu_period, cpu_quota = self._convert_cpu_limit(
                    container_def.resources.limits.cpu
                )

        args = {
            "image": image,
            "entrypoint": container_def.command,
            "command": container_def.args,
            "environment": environment,
            "volumes": volumes,
            "labels": labels,
            "network": service_name,
            "name": f"{service.name}-{container_def.name}-{int(time.time())}",
            "detach": True,
            "mem_limit": mem_limit,
            "cpu_period": cpu_period,
            "cpu_quota": cpu_quota,
            "working_dir": container_def.working_dir,
            "restart_policy": (
                {"Name": "unless-stopped"} if not is_init else None
            ),
            "is_init": is_init,
        }
        if ports:
            args["ports"] = ports
        return args

    def _convert_cpu_limit(
        self, cpu_limit: float
    ) -> tuple[int | None, int | None]:
        """Convert CPU limit to Docker CPU period and quota."""
        cores = float(cpu_limit)
        cpu_period = 100000
        cpu_quota = int(cores * cpu_period)
        return cpu_period, cpu_quota


class ResultConverter:
    def convert_service_item(
        self,
        service_name: str,
        service_def: ServiceDefinition,
    ) -> ServiceItem:
        # Generate a simple URI for the service
        uri = "http://localhost"
        return ServiceItem(
            name=service_name,
            uri=uri,
            service=service_def,
        )

    def convert_service(
        self,
        containers: list[DockerContainer],
    ) -> ServiceDefinition:
        # Get service name from first container
        main = next(
            (
                c
                for c in containers
                if c.labels.get(f"{LABEL_PREFIX}.type") == "main"
            ),
            containers[0],
        )
        service_name = main.labels.get(f"{LABEL_PREFIX}.service")
        revision = main.labels.get(f"{LABEL_PREFIX}.revision", "unknown")
        if not service_name:
            raise ValueError("Service name not found in container labels")

        # Convert containers
        container_defs = []
        for docker_container in containers:
            container_def = self._convert_container(docker_container)
            container_defs.append(container_def)

        ingress = Ingress(external=True)
        traffic = [
            TrafficAllocation(
                revision=revision,
                percent=100,
                latest_revision=True,
            )
        ]

        # Basic scaling info
        scale = Scale(
            mode="manual",
            replicas=1,
        )

        return ServiceDefinition(
            name=service_name,
            containers=container_defs,
            ingress=ingress,
            traffic=traffic,
            scale=scale,
            latest_ready_revision=revision,
            latest_created_revision=revision,
        )

    def _convert_container(
        self, docker_container: DockerContainer
    ) -> Container:
        """Convert Docker container to Container definition."""

        # Reload container to get fresh state
        docker_container.reload()

        container_name = docker_container.labels.get(
            f"{LABEL_PREFIX}.container", "unknown"
        )
        container_type = docker_container.labels.get(
            f"{LABEL_PREFIX}.type", "regular"
        )

        # Extract environment variables
        env_vars = []
        labels = docker_container.labels or {}
        explicit = labels.get(f"{LABEL_PREFIX}.env_names", "")
        explicit_set = {n for n in explicit.split(",") if n}
        cfg_env = (docker_container.attrs.get("Config", {}) or {}).get(
            "Env"
        ) or []
        for env_str in cfg_env:
            if "=" not in env_str:
                continue
            name, value = env_str.split("=", 1)
            if name in explicit_set:
                env_vars.append(EnvVar(name=name, value=value))

        # Extract ports
        ports = []
        if docker_container.attrs.get("Config", {}).get("ExposedPorts"):
            for port_str in docker_container.attrs["Config"][
                "ExposedPorts"
            ].keys():
                if "/" in port_str:
                    port_num = int(port_str.split("/")[0])
                    ports.append(
                        Port(container_port=port_num, name=f"port-{port_num}")
                    )

        # Extract resource requirements
        resources = None
        config = docker_container.attrs.get("HostConfig", {})
        if config.get("Memory") or config.get("CpuQuota"):
            limits = ResourceLimits()
            if config.get("Memory"):
                limits.memory = config["Memory"]
            if config.get("CpuQuota") and config.get("CpuPeriod"):
                cpu_cores = config["CpuQuota"] / config["CpuPeriod"]
                limits.cpu = cpu_cores
            resources = ResourceRequirements(limits=limits)

        # Extract volumes
        volume_mounts = []
        if docker_container.attrs.get("Mounts"):
            for mount in docker_container.attrs["Mounts"]:
                if mount.get("Type") == "volume":
                    volume_mounts.append(
                        VolumeMount(
                            name=mount.get(
                                "Name", mount.get("Source", "unknown")
                            ),
                            mount_path=mount["Destination"],
                        )
                    )
        type: Literal["main", "init"] = (
            "init" if container_type == "init" else "main"
        )
        return Container(
            name=container_name,
            image=(
                docker_container.image.tags[0]
                if docker_container.image.tags
                else "unknown"
            ),
            command=docker_container.attrs.get("Config", {}).get(
                "Entrypoint", []
            ),
            args=docker_container.attrs.get("Config", {}).get("Cmd", []),
            working_dir=docker_container.attrs.get("Config", {}).get(
                "WorkingDir"
            ),
            env=env_vars,
            ports=ports,
            resources=resources,
            volume_mounts=volume_mounts,
            type=type,
        )

    def convert_revision(self, containers: list[DockerContainer]) -> Revision:
        """Convert Docker container to Revision."""
        container_defs = []
        for docker_container in containers:
            container_def = self._convert_container(docker_container)
            container_defs.append(container_def)

        main = next(
            (
                c
                for c in containers
                if c.labels.get(f"{LABEL_PREFIX}.type") == "main"
            ),
            containers[0],
        )

        # Get creation time
        created_time = None
        if main.attrs.get("Created"):
            from datetime import datetime

            created_str = main.attrs["Created"]
            # Parse ISO format timestamp
            created_dt = datetime.fromisoformat(
                created_str.replace("Z", "+00:00")
            )
            created_time = created_dt.timestamp()

        # Check if container is active (running)
        active = main.status == "running"
        revision = main.labels.get(f"{LABEL_PREFIX}.revision", "unknown")

        return Revision(
            name=revision,
            traffic=100 if active else 0,
            created_time=created_time,
            status="active" if active else "inactive",
            active=active,
            containers=container_defs,
            volumes=[],
        )

    def convert_uri(self, service: ServiceDefinition) -> str:
        host_port = None
        for container in service.containers:
            if container.type == "main":
                if container.ports:
                    host_port = (
                        container.ports[0].host_port
                        or container.ports[0].container_port
                    )
                    break
        uri = (
            f"http://localhost:{host_port}"
            if host_port
            else "http://localhost"
        )
        return uri
