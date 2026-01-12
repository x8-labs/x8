from x8.core import Response
from x8.storage.secret_store import SecretItem, SecretList

from ._providers import get_component


class SecretStoreSyncAndAsyncClient:
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def get(self, key, **kwargs) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aget(key=key, **kwargs)
        return self.client.get(key=key, **kwargs)

    async def get_metadata(self, key, **kwargs) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aget_metadata(key=key, **kwargs)
        return self.client.get_metadata(key=key, **kwargs)

    async def get_versions(self, key, **kwargs) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aget_versions(key=key, **kwargs)
        return self.client.get_versions(key=key, **kwargs)

    async def put(
        self, key, value, metadata=None, where=None, **kwargs
    ) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aput(
                key=key, value=value, metadata=metadata, where=where, **kwargs
            )
        return self.client.put(
            key=key, value=value, metadata=metadata, where=where, **kwargs
        )

    async def update(self, key, value, **kwargs) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aupdate(key=key, value=value, **kwargs)
        return self.client.update(key=key, value=value, **kwargs)

    async def update_metadata(
        self, key, metadata, **kwargs
    ) -> Response[SecretItem]:
        if self.async_call:
            return await self.client.aupdate_metadata(
                key=key, metadata=metadata, **kwargs
            )
        return self.client.update_metadata(
            key=key, metadata=metadata, **kwargs
        )

    async def delete(self, key, **kwargs) -> Response[None]:
        if self.async_call:
            return await self.client.adelete(key=key, **kwargs)
        return self.client.delete(key=key, **kwargs)

    async def query(self, where=None, **kwargs) -> Response[SecretList]:
        if self.async_call:
            return await self.client.aquery(where=where, **kwargs)
        return self.client.query(where=where, **kwargs)

    async def count(self, where=None, **kwargs) -> Response[int]:
        if self.async_call:
            return await self.client.acount(where=where, **kwargs)
        return self.client.count(where=where, **kwargs)

    async def close(self) -> Response[None]:
        if self.async_call:
            return await self.client.aclose()
        else:
            return Response(result=None)
