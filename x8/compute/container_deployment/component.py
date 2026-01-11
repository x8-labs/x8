from typing import Any, List

from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Component, Response, RunContext, operation
from x8.ql import Expression

from ._models import (
    Revision,
    ServiceDefinition,
    ServiceItem,
    TrafficAllocation,
)


class ContainerDeployment(Component):
    service: ServiceDefinition | None = None
    containerizer: Containerizer | None = None
    container_registry: ContainerRegistry | None = None

    def __init__(
        self,
        service: ServiceDefinition | None = None,
        containerizer: Containerizer | None = None,
        container_registry: ContainerRegistry | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            service:
                Service definition to deploy.
            containerizer:
                Containerizer to use for building the container.
            container_registry:
                Container registry to use for pushing the container.
        """
        self.service = service
        self.containerizer = containerizer
        self.container_registry = container_registry
        super().__init__(**kwargs)

    @operation()
    def create_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Create the service."""
        ...

    @operation()
    def delete_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete the service."""
        ...

    @operation()
    def get_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Get the service."""
        ...

    @operation()
    def list_services(
        self,
        **kwargs: Any,
    ) -> Response[List[ServiceItem]]:
        """List deployed services."""
        ...

    @operation()
    def get_logs(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Get the logs of the service."""
        ...

    @operation()
    def list_revisions(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[List[Revision]]:
        """List revisions of the service."""
        ...

    @operation()
    def get_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        """Get a specific revision of the service."""
        ...

    @operation()
    def delete_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete a specific revision of the service."""
        ...

    @operation()
    def update_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Update traffic distribution for the service."""
        ...

    @operation()
    def close(self) -> None:
        """Close the component."""
        ...

    @operation()
    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Create the service."""
        ...

    @operation()
    async def adelete_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete the service."""
        ...

    @operation()
    async def aget_service(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Get the service."""
        ...

    @operation()
    async def alist_services(
        self,
        **kwargs: Any,
    ) -> Response[List[ServiceItem]]:
        """List deployed services."""
        ...

    @operation()
    async def aget_logs(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Get the logs of the service."""
        ...

    @operation()
    async def alist_revisions(
        self,
        name: str,
        **kwargs: Any,
    ) -> Response[List[Revision]]:
        """List revisions of the service."""
        ...

    @operation()
    async def aget_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[Revision]:
        """Get a specific revision of the service."""
        ...

    @operation()
    async def adelete_revision(
        self,
        name: str,
        revision: str,
        **kwargs: Any,
    ) -> Response[None]:
        """Delete a specific revision of the service."""
        ...

    @operation()
    async def aupdate_traffic(
        self,
        name: str,
        traffic: list[TrafficAllocation],
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        """Update traffic distribution for the service."""
        ...

    @operation()
    async def aclose(self) -> None:
        """Close the component."""
        ...
