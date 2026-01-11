from typing import Any

from x8.compute.container_registry import ContainerRegistry
from x8.compute.containerizer import Containerizer
from x8.core import Context, Operation, Provider, Response, RunContext
from x8.core.exceptions import BadRequestError
from x8.ql import Expression

from .._helper import merge_service_overlay
from .._models import ServiceDefinition, ServiceItem, ServiceOverlay


class BaseContainerDeploymentProvider(Provider):
    service: ServiceDefinition | None
    overlay: ServiceOverlay | None
    containerizer: Containerizer | None
    container_registry: ContainerRegistry | None

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if not operation:
            self.__setup__(context)
            result = self.create_service(
                run_context=self._get_run_context(context)
            )
            return Response(result=result)
        return super().__run__(operation=operation, context=context, **kwargs)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if not operation:
            await self.__asetup__(context)
            result = await self.acreate_service(
                run_context=self._get_run_context(context)
            )
            return Response(result=result)
        return await super().__arun__(
            operation=operation, context=context, **kwargs
        )

    def create_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        raise NotImplementedError(
            "Create service method must be implemented by provider."
        )

    async def acreate_service(
        self,
        service: ServiceDefinition | None = None,
        where: str | Expression | None = None,
        run_context: RunContext = RunContext(),
        **kwargs: Any,
    ) -> Response[ServiceItem]:
        raise NotImplementedError(
            "Create service method must be implemented by provider."
        )

    def _get_run_context(self, context: Context | None = None) -> RunContext:
        if context and context.data:
            return context.data.pop("__run__", RunContext())
        return RunContext()

    def _get_containerizer(self) -> Containerizer:
        return (
            self.containerizer
            or self.__component__.containerizer
            or Containerizer(__provider__="default")
        )

    def _get_container_registry(self) -> ContainerRegistry | None:
        return (
            self.container_registry
            or self.__component__.container_registry
            or None
        )

    def _deploy(
        self,
        service: ServiceDefinition,
        images: list[str],
        where_exists: bool | None,
    ) -> ServiceItem:
        raise NotImplementedError(
            "Deploy method must be implemented by provider."
        )

    def _normalize_service_definition(
        self, service: ServiceDefinition | None = None
    ) -> ServiceDefinition:
        service_def = service or self.service or self.__component__.service
        if not service_def:
            raise BadRequestError("Service definition is required.")
        service_def = merge_service_overlay(
            service=service_def, overlay=self.overlay
        )
        return service_def
