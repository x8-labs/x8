from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class ContainerRegistryClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def push(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def pull(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def tag(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def delete(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_images(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_digests(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def close(self, **kwargs):
        return await self._execute_method(**kwargs)
