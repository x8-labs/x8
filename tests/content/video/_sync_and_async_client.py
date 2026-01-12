from common.sync_and_async_client import SyncAndAsyncClient

from ._providers import get_component


class VideoSyncAndAsyncClient(SyncAndAsyncClient):
    def __init__(self, provider_type: str, async_call: bool, video_param):
        self.client = get_component(provider_type, video_param)
        self.async_call = async_call
        self.provider_type = provider_type

    async def get_info(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def seek_frame(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_frame(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def get_audio(self, **kwargs):
        return await self._execute_method(**kwargs)

    async def close(self, **kwargs):
        return await self._execute_method(**kwargs)
