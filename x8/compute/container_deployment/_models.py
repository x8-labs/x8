from typing import Any, Literal

from x8.compute._common._models import ImageMap
from x8.core import DataModel


class HTTPHeader(DataModel):
    """HTTP header for HTTP probe requests."""

    name: str
    value: str


class HTTPGetAction(DataModel):
    """HTTP GET action for probes."""

    path: str = "/"
    port: int | str = 80
    host: str | None = None
    scheme: str = "HTTP"
    http_headers: list[HTTPHeader] = []


class TCPSocketAction(DataModel):
    """TCP socket action for probes."""

    port: int | str
    host: str | None = None


class ExecAction(DataModel):
    """Exec action for probes."""

    command: list[str]


class GRPCAction(DataModel):
    """gRPC action for probes."""

    port: int
    service: str | None = None


class Probe(DataModel):
    """Container probe configuration."""

    # Probe actions (exactly one must be specified)
    http_get: HTTPGetAction | None = None
    tcp_socket: TCPSocketAction | None = None
    exec: ExecAction | None = None
    grpc: GRPCAction | None = None

    # Timing configuration in seconds
    initial_delay_seconds: int | None = None
    period_seconds: int | None = None
    timeout_seconds: int | None = None

    success_threshold: int | None = None
    failure_threshold: int | None = None

    # Termination grace period (for liveness probes)
    termination_grace_period_seconds: int | None = None


class ProbeSet(DataModel):
    """Container probes configuration."""

    liveness_probe: Probe | None = None
    readiness_probe: Probe | None = None
    startup_probe: Probe | None = None


class Port(DataModel):
    """Port configuration for containers."""

    name: str | None = None
    container_port: int
    host_port: int | None = None
    protocol: str = "TCP"


class VolumeMount(DataModel):
    """Volume mount configuration for containers."""

    name: str
    mount_path: str
    sub_path: str | None = None
    read_only: bool | None = None
    config: dict[str, str] | None = None


class GPUResource(DataModel):
    """GPU resource configuration."""

    count: int = 1
    type: str | None = None
    memory: int | None = None


class ResourceRequests(DataModel):
    cpu: float | None = None
    memory: int | None = None
    gpu: GPUResource | None = None


class ResourceLimits(DataModel):
    cpu: float | None = None
    memory: int | None = None
    gpu: GPUResource | None = None


class ResourceRequirements(DataModel):
    """Container resource requirements and limits."""

    requests: ResourceRequests | None = None

    limits: ResourceLimits | None = None

    cpu_idle: bool = True
    cpu_boost: bool = False

    config: dict[str, str] | None = None


class EnvVar(DataModel):
    """Environment variable configuration."""

    name: str
    value: str | None = None
    value_from: dict[str, Any] | None = None


class Lifecycle(DataModel):
    pre_stop: ExecAction | None = None
    post_start: ExecAction | None = None


class SecurityContext(DataModel):
    run_as_user: int | None = None
    run_as_group: int | None = None
    allow_privilege_escalation: bool | None = None
    read_only_root_filesystem: bool | None = None
    privileged: bool | None = None
    capabilities: dict[str, list[str]] | None = None
    config: dict[str, Any] | None = None


class Container(DataModel):
    """Container configuration."""

    name: str
    type: Literal["main", "init"] = "main"

    # Image configuration
    image: str | None = None

    # Image pull policy
    image_pull_policy: str | None = None

    # Runtime configuration
    command: list[str] | None = None
    args: list[str] | None = None
    working_dir: str | None = None
    env: list[EnvVar] = []

    # Networking
    ports: list[Port] = []

    # Storage
    volume_mounts: list[VolumeMount] = []

    # Resources and limits
    resources: ResourceRequirements | None = None

    # Health checks
    probes: ProbeSet | None = None

    # Lifecycle hooks
    lifecycle: Lifecycle | None = None

    # Security context
    security_context: SecurityContext | None = None

    # Additional configuration
    config: dict[str, str] | None = None


class Volume(DataModel):
    name: str
    type: Literal[
        "emptyDir",
        "hostPath",
        "persistent",
        "configMap",
        "secret",
        "objectStorage",
        "fileStorage",
        "ephemeral",
        "nfs",
    ]

    size_limit: str | None = None
    read_only: bool = False
    config: dict[str, Any] | None = None


class ScaleRule(DataModel):
    type: str
    name: str | None = None
    metadata: dict[str, Any] | None = None
    auth: list[dict[str, str]] | None = None


class Scale(DataModel):
    mode: Literal["manual", "auto"] = "auto"
    replicas: int | None = 1
    min_replicas: int | None = 0
    max_replicas: int | None = 10
    max_concurrency: int | None = None
    cooldown_period: int | None = 300
    polling_interval: int | None = 30
    rules: list[ScaleRule] = []
    config: dict[str, str] | None = None


class Ingress(DataModel):
    external: bool = True
    target_port: int | None = None
    port: int | None = None
    transport: str | None = None
    config: dict[str, Any] | None = None


class TrafficAllocation(DataModel):
    revision: str
    percent: float
    latest_revision: bool = False
    tag: str | None = None


class ServiceDefinition(DataModel):
    name: str | None = None
    images: list[ImageMap] = []
    containers: list[Container] = []
    volumes: list[Volume] = []
    ingress: Ingress | None = None
    scale: Scale | None = None
    traffic: list[TrafficAllocation] | None = None
    restart_policy: str = "Always"  # One of "Always", "OnFailure", "Never"
    latest_ready_revision: str | None = None
    latest_created_revision: str | None = None
    config: dict[str, Any] | None = None


class ContainerRegistryCredentials(DataModel):
    """Container registry credentials."""

    server: str
    auth_type: Literal["basic", "token", "managed"] = "managed"
    username: str | None = None
    password: str | None = None
    token: str | None = None
    config: dict[str, Any] | None = None


class ContainerOverride(DataModel):
    """Container override configuration."""

    name: str
    env: list[EnvVar] = []


class ServiceOverlay(DataModel):
    """Service override configuration."""

    containers: list[ContainerOverride] = []


class Revision(DataModel):
    name: str
    traffic: float | None = None
    created_time: float | None = None
    status: str | None = None
    active: bool | None = None
    containers: list[Container] = []
    volumes: list[Volume] = []


class ServiceItem(DataModel):
    name: str
    uri: str | None = None
    service: ServiceDefinition | None = None
