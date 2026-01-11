from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class DocumentStoreSyncAndAsyncClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    def __supports__(self, **kwargs):
        return self.client.__supports__(**kwargs)

    async def __execute__(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def create_collection(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def drop_collection(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_collections(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def has_collection(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def create_index(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def drop_index(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_indexes(self, **kwargs):
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
