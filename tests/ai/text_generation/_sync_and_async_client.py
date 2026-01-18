from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class TextGenerationSyncAndAsyncClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def generate(self, **kwargs):
        return await self._execute_method(**kwargs)
