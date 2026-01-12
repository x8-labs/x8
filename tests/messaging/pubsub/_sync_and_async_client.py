from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class PubSubSyncAndAsyncClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def create_topic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def update_topic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def drop_topic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_topics(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def has_topic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_topic(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def create_subscription(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def update_subscription(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def drop_subscription(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def list_subscriptions(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def has_subscription(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_subscription(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def put(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def batch(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def pull(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def ack(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def nack(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def extend(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def purge(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def close(self, **kwargs):
        return await self._execute_method(**kwargs)
