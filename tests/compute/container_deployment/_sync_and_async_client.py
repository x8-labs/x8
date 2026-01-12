from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class ContainerDeploymentClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def create_service(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def delete_service(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_service(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_logs(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_revisions(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_revision(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def delete_revision(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def update_traffic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_services(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def close(self, **kwargs):
        return await self._execute_method(**kwargs)
