from typing import Any, Literal

from x8.compute._common._models import ImageMap
from x8.compute.container_registry.component import ContainerRegistry
from x8.compute.containerizer.component import Containerizer
from x8.core import Component, Response, operation

from ._models import ManifestsType


class Kubernetes(Component):
    kubeconfig: str | dict[str, Any] | None
    context: str | None
    manifests: ManifestsType
    overlays: ManifestsType
    namespace: str | None
    images: list[ImageMap] | None
    containerizer: Containerizer | None
    container_registry: ContainerRegistry | None

    def __init__(
        self,
        kubeconfig: str | dict[str, Any] | None = None,
        context: str | None = None,
        manifests: ManifestsType = None,
        overlays: ManifestsType = None,
        namespace: str | None = None,
        images: list[ImageMap] | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        **kwargs: Any,
    ):
        self.kubeconfig = kubeconfig
        self.context = context
        self.manifests = manifests
        self.overlays = overlays
        self.namespace = namespace
        self.images = images
        self.containerizer = containerizer
        self.container_registry = container_registry
        super().__init__(**kwargs)

    @operation()
    def apply(
        self,
        manifests: ManifestsType = None,
        overlays: ManifestsType = None,
        namespace: str | None = None,
        server_side: bool = False,
        force_conflicts: bool = False,
        field_manager: str | None = None,
        dry_run: Literal["client", "server"] | None = None,
        validate: bool | Literal["strict"] | None = True,
        prune: bool = False,
        selector: str | None = None,
        prune_all: bool = False,
        prune_allowlist: list[str] | None = None,
        wait: bool = True,
        timeout: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        raise NotImplementedError
