from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class KeyValueStoreSyncAndAsyncClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool, type: str):
        self.client = get_component(provider_type, type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def exists(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def put(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def update(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def delete(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def query(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def count(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def batch(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def transact(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def close(self, **kwargs):
        return await self._execute_method(**kwargs)
