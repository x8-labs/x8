from typing import Any

from x8.core import Response
from x8.storage.object_store import ObjectItem, ObjectList

from ._providers import get_component


class ObjectStoreSyncAndAsyncClient:
    def __init__(self, provider_type: str, async_call: bool):
        self.client = get_component(provider_type)
        self.async_call = async_call
        self.provider_type = provider_type

    async def create_collection(
        self, collection=None, config=None, **kwargs
    ) -> Response[None]:
        if self.async_call:
            return await self.client.acreate_collection(
                collection=collection, config=config, **kwargs
            )
        return self.client.create_collection(
            collection=collection, config=config, **kwargs
        )

    async def drop_collection(
        self, collection=None, **kwargs
    ) -> Response[None]:
        if self.async_call:
            return await self.client.adrop_collection(
                collection=collection, **kwargs
            )
        return self.client.drop_collection(collection=collection, **kwargs)

    async def list_collections(self, **kwargs) -> Response[list[str]]:
        if self.async_call:
            return await self.client.alist_collections(**kwargs)
        return self.client.list_collections(**kwargs)

    async def has_collection(
        self, collection=None, **kwargs
    ) -> Response[bool]:
        if self.async_call:
            return await self.client.ahas_collection(
                collection=collection, **kwargs
            )
        return self.client.has_collection(collection=collection, **kwargs)

    async def put(
        self,
        key,
        value=None,
        file=None,
        stream=None,
        metadata=None,
        properties=None,
        where=None,
        collection=None,
        config=None,
        **kwargs,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aput(
                key=key,
                value=value,
                file=file,
                stream=stream,
                metadata=metadata,
                properties=properties,
                where=where,
                collection=collection,
                config=config,
                **kwargs,
            )
        return self.client.put(
            key=key,
            value=value,
            file=file,
            stream=stream,
            metadata=metadata,
            properties=properties,
            where=where,
            collection=collection,
            config=config,
            **kwargs,
        )

    async def get(
        self,
        key,
        file=None,
        stream=None,
        where=None,
        start=None,
        end=None,
        config=None,
        collection=None,
        **kwargs,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aget(
                key=key,
                file=file,
                stream=stream,
                where=where,
                start=start,
                end=end,
                config=config,
                collection=collection,
                **kwargs,
            )
        return self.client.get(
            key=key,
            file=file,
            stream=stream,
            where=where,
            start=start,
            end=end,
            config=config,
            collection=collection,
            **kwargs,
        )

    async def get_metadata(
        self,
        key,
        where=None,
        collection=None,
        **kwargs,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aget_metadata(
                key=key, where=where, collection=collection, **kwargs
            )
        return self.client.get_metadata(
            key=key, where=where, collection=collection, **kwargs
        )

    async def get_properties(
        self,
        key,
        where=None,
        collection=None,
        **kwargs,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aget_properties(
                key=key, where=where, collection=collection, **kwargs
            )
        return self.client.get_properties(
            key=key, where=where, collection=collection, **kwargs
        )

    async def get_versions(
        self, key, collection=None, **kwargs
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aget_versions(
                key=key, collection=collection, **kwargs
            )
        return self.client.get_versions(
            key=key, collection=collection, **kwargs
        )

    async def update(
        self,
        key,
        metadata=None,
        properties=None,
        where=None,
        collection=None,
        **kwargs,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.aupdate(
                key=key,
                metadata=metadata,
                properties=properties,
                where=where,
                collection=collection,
                **kwargs,
            )
        return self.client.update(
            key=key,
            metadata=metadata,
            properties=properties,
            where=where,
            collection=collection,
            **kwargs,
        )

    async def delete(
        self, key=None, where=None, collection=None, **kwargs
    ) -> Response[None]:
        if self.async_call:
            return await self.client.adelete(
                key=key, where=where, collection=collection, **kwargs
            )
        return self.client.delete(
            key=key, where=where, collection=collection, **kwargs
        )

    async def copy(
        self, key, source, where=None, collection=None, **kwargs
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.acopy(
                key=key,
                source=source,
                where=where,
                collection=collection,
                **kwargs,
            )
        else:
            return self.client.copy(
                key=key,
                source=source,
                where=where,
                collection=collection,
                **kwargs,
            )

    async def generate(
        self,
        key,
        method,
        expiry,
        collection=None,
    ) -> Response[ObjectItem]:
        if self.async_call:
            return await self.client.agenerate(
                key=key,
                method=method,
                expiry=expiry,
                collection=collection,
            )
        else:
            return self.client.generate(
                key=key,
                method=method,
                expiry=expiry,
                collection=collection,
            )

    async def query(
        self,
        where=None,
        limit=None,
        continuation=None,
        config=None,
        collection=None,
        **kwargs,
    ) -> Response[ObjectList]:
        if self.async_call:
            return await self.client.aquery(
                where=where,
                limit=limit,
                continuation=continuation,
                config=config,
                collection=collection,
            )
        else:
            return self.client.query(
                where=where,
                limit=limit,
                continuation=continuation,
                config=config,
                collection=collection,
            )

    async def count(
        self,
        where=None,
        collection=None,
        **kwargs,
    ) -> Response[int]:
        if self.async_call:
            return await self.client.acount(
                where=where,
                collection=collection,
            )
        else:
            return self.client.count(
                where=where,
                collection=collection,
            )

    async def batch(self, batch, **kwargs) -> Response[list[Any]]:
        if self.async_call:
            return await self.client.abatch(batch=batch, **kwargs)
        return self.client.batch(batch=batch, **kwargs)

    async def close(self) -> Response[None]:
        if self.async_call:
            return await self.client.aclose()
        else:
            return self.client.close()
