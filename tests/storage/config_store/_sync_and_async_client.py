from x8.core import Response
from x8.storage.config_store import ConfigItem, ConfigList

from ._providers import get_component


class ConfigStoreSyncAndAsyncClient:
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def get(
        self,
        key,
        **kwargs,
    ) -> Response[ConfigItem]:
        if self.async_call:
            return await self.client.aget(
                key=key,
                **kwargs,
            )
        return self.client.get(
            key,
            **kwargs,
        )

    async def put(
        self,
        key,
        value,
        **kwargs,
    ) -> Response[ConfigItem]:
        if self.async_call:
            return await self.client.aput(
                key,
                value,
                **kwargs,
            )
        return self.client.put(
            key,
            value,
            **kwargs,
        )

    async def delete(
        self,
        key,
        **kwargs,
    ) -> Response[None]:
        if self.async_call:
            return await self.client.adelete(
                key,
                **kwargs,
            )
        return self.client.delete(
            key,
            **kwargs,
        )

    async def query(
        self,
        where=None,
        **kwargs,
    ) -> Response[ConfigList]:
        if self.async_call:
            return await self.client.aquery(
                where,
                **kwargs,
            )
        return self.client.query(
            where,
            **kwargs,
        )

    async def count(
        self,
        where=None,
        **kwargs,
    ) -> Response[int]:
        if self.async_call:
            return await self.client.acount(
                where,
                **kwargs,
            )
        return self.client.count(
            where,
            **kwargs,
        )

    async def close(self) -> Response[None]:
        if self.async_call:
            return await self.client.aclose()
        else:
            return Response(result=None)
