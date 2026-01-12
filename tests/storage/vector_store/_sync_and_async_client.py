from typing import Any

from x8.core import Response
from x8.storage.vector_store import VectorItem, VectorList

from ._providers import get_component


class VectorStoreSyncAndAsyncClient:
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

    async def get(self, key, **kwargs) -> Response[VectorItem]:
        if self.async_call:
            return await self.client.aget(key=key, **kwargs)
        return self.client.get(key=key, **kwargs)

    async def put(
        self, key, value, metadata=None, collection=None, **kwargs
    ) -> Response[VectorItem]:
        if self.async_call:
            return await self.client.aput(
                key=key,
                value=value,
                metadata=metadata,
                collection=collection,
                **kwargs,
            )
        return self.client.put(
            key=key,
            value=value,
            metadata=metadata,
            collection=collection,
            **kwargs,
        )

    async def update(
        self, key, value, collection=None, **kwargs
    ) -> Response[VectorItem]:
        if self.async_call:
            return await self.client.aupdate(
                key=key, value=value, collection=collection, **kwargs
            )
        return self.client.update(
            key=key, value=value, collection=collection, **kwargs
        )

    async def update_metadata(
        self, key, metadata, collection=None, **kwargs
    ) -> Response[VectorItem]:
        if self.async_call:
            return await self.client.aupdate_metadata(
                key=key, metadata=metadata, collection=collection, **kwargs
            )
        return self.client.update_metadata(
            key=key, metadata=metadata, collection=collection, **kwargs
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

    async def query(
        self,
        search=None,
        select=None,
        where=None,
        order_by=None,
        limit=None,
        offset=None,
        collection=None,
        **kwargs,
    ) -> Response[VectorList]:
        if self.async_call:
            return await self.client.aquery(
                search=search,
                select=select,
                where=where,
                order_by=order_by,
                limit=limit,
                offset=offset,
                collection=collection,
                **kwargs,
            )
        return self.client.query(
            search=search,
            select=select,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
            collection=collection,
            **kwargs,
        )

    async def count(
        self, search=None, where=None, collection=None, **kwargs
    ) -> Response[int]:
        if self.async_call:
            return await self.client.acount(
                search=search, where=where, collection=collection, **kwargs
            )
        return self.client.count(
            search=search, where=where, collection=collection, **kwargs
        )

    async def batch(self, batch, **kwargs) -> Response[list[Any]]:
        if self.async_call:
            return await self.client.abatch(batch=batch, **kwargs)
        return self.client.batch(batch=batch, **kwargs)

    async def transact(self, transaction, **kwargs) -> Response[list[Any]]:
        if self.async_call:
            return await self.client.atransact(
                transaction=transaction, **kwargs
            )
        return self.client.transact(transaction=transaction, **kwargs)

    async def close(self) -> Response[None]:
        if self.async_call:
            return await self.client.aclose()
        else:
            return self.client.close()
