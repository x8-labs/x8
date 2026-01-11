from typing import Any

from x8.core import Context, Operation, Provider, warn
from x8.core.exceptions import BadRequestError

from .._models import ChoiceProviderInfo


class Default(Provider):
    providers: list[ChoiceProviderInfo]

    _provider_map: dict[str, Provider]

    def __init__(self, providers: list[ChoiceProviderInfo], **kwargs):
        self.providers = providers
        self._init_provider_map()

    def _init_provider_map(self):
        self._provider_map = {
            provider.key: provider.provider for provider in self.providers
        }

    def _get_choice_provider(self, context: Context | None = None) -> Provider:
        if (
            context
            and context.data
            and "provider" in context.data
            and context.data["provider"] in self._provider_map
        ):
            return self._provider_map[context.data["provider"]]
        warn("Choice provider not found. Falling back to the first provider")
        if not self.providers:
            raise BadRequestError("No choice providers available")

        return self.providers[0].provider

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        provider = self._get_choice_provider(context=context)
        return provider.__run__(operation=operation, context=context, **kwargs)

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        provider = self._get_choice_provider(context=context)
        return await provider.__arun__(
            operation=operation, context=context, **kwargs
        )
