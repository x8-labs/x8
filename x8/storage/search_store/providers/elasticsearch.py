"""
Elastic Search.
"""

from __future__ import annotations

import re

__all__ = ["Elasticsearch"]


from typing import Any

from elasticsearch import ApiError, AsyncElasticsearch
from elasticsearch import Elasticsearch as SyncElasticsearch
from elasticsearch.exceptions import ConflictError as ESConflictError
from elasticsearch.exceptions import NotFoundError as ESNotFoundError

from x8.core import Context, DataModel, Response
from x8.core.exceptions import (
    BadRequestError,
    ConflictError,
    NotFoundError,
    PreconditionFailedError,
)
from x8.ql import (
    And,
    Comparison,
    ComparisonOp,
    Expression,
    Field,
    Function,
    FunctionNamespace,
    Not,
    Or,
    OrderBy,
    OrderByDirection,
    QueryFunctionName,
    QueryProcessor,
    Select,
    Update,
    UpdateOp,
)
from x8.storage._common import (
    ArrayIndex,
    AscIndex,
    CollectionResult,
    CollectionStatus,
    CompositeIndex,
    DescIndex,
    ExcludeIndex,
    FieldIndex,
    GeospatialFieldType,
    GeospatialIndex,
    HashIndex,
    Index,
    ItemProcessor,
    ParameterParser,
    RangeIndex,
    RankIndex,
    SparseVectorIndex,
    StoreOperationParser,
    StoreProvider,
    TextIndex,
    TextSimilarityAlgorithm,
    VectorIndex,
    VectorIndexMetric,
    VectorIndexStructure,
)

from .._helper import Helper
from .._models import (
    SearchCollectionConfig,
    SearchFieldType,
    SearchItem,
    SearchKey,
    SearchList,
    SearchProperties,
)


class Elasticsearch(StoreProvider):
    hosts: str | dict[str, str | int]
    cloud_id: str | None
    api_key: str | list[str] | None
    basic_auth: str | list[str] | None
    bearer_auth: str | None
    opaque_id: str | None
    http_auth: str | Any | None

    headers: dict[str, str] | None
    verify_certs: bool | None
    ca_certs: str | None
    client_cert: str | None
    client_key: str | None
    ssl_assert_hostname: str | None
    ssl_assert_fingerprint: str | None
    ssl_version: int | None

    index: str | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None
    nparams: dict[str, Any]

    _client: SyncElasticsearch
    _aclient: AsyncElasticsearch

    _init: bool
    _ainit: bool

    _op_parser: StoreOperationParser
    _collection_cache: dict[str, ElasticsearchCollection]

    def __init__(
        self,
        hosts: str | dict[str, str | int],
        cloud_id: str | None = None,
        api_key: str | list[str] | None = None,
        basic_auth: str | list[str] | None = None,
        bearer_auth: str | None = None,
        opaque_id: str | None = None,
        http_auth: str | list[str] | Any | None = None,
        headers: dict[str, str] | None = None,
        verify_certs: bool | None = None,
        ca_certs: str | None = None,
        client_cert: str | None = None,
        client_key: str | None = None,
        ssl_assert_hostname: str | None = None,
        ssl_assert_fingerprint: str | None = None,
        ssl_version: int | None = None,
        index: str | None = None,
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            hosts:
                Elasticsearch hosts.
            cloud_id:
                Elasticsearch cloud id.
            api_key:
                Elasticsearch api key.
            basic_auth:
                Elasticsearch basic auth.
            bearer_auth:
                Elasticsearch bearer auth.
            opaque_id:
                Elasticsearch opaque id.
            http_auth:
                Elasticsearch http auth.
            headers:
                Elasticsearch http headers.
            verify_certs:
                Elasticsearch verify certs.
            ca_certs:
                Elasticsearch ca certs.
            client_cert:
                Elasticsearch client cert.
            client_key:
                Elasticsearch client key.
            ssl_assert_hostname:
                Elasticsearch ssl assert hostname.
            ssl_assert_fingerprint:
                Elasticsearch ssl assert fingerprint.
            ssl_version:
                Elasticsearch ssl version.
            index:
                Elasticsearch index mapped to
                search store collection.
            id_map_field:
                Field in the content to map into id.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            pk_map_field:
                Field in the content to map into pk.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            nparams:
                Native parameters to Elasticsearch client.
        """
        self.hosts = hosts
        self.cloud_id = cloud_id
        self.api_key = api_key
        self.basic_auth = basic_auth
        self.bearer_auth = bearer_auth
        self.opaque_id = opaque_id
        self.http_auth = http_auth
        self.headers = headers
        self.verify_certs = verify_certs
        self.ca_certs = ca_certs
        self.client_cert = client_cert
        self.client_key = client_key
        self.ssl_assert_hostname = ssl_assert_hostname
        self.ssl_assert_fingerprint = ssl_assert_fingerprint
        self.ssl_version = ssl_version

        self.index = index
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.nparams = nparams

        self._init = False
        self._ainit = False
        self._collection_cache = dict()

    @property
    def client(self) -> SyncElasticsearch:
        if not self._init:
            self._client = SyncElasticsearch(**self._get_client_params())
            self._init = True
        return self._client

    @property
    def aclient(self) -> AsyncElasticsearch:
        if not self._ainit:
            self._aclient = AsyncElasticsearch(**self._get_client_params())
            self._ainit = True
        return self._aclient

    def __setup__(self, context: Context | None = None) -> None:
        _ = self.client

    async def __asetup__(self, context: Context | None = None) -> None:
        _ = self.aclient

    def _get_client_params(self) -> dict:
        def _add_if_not_none(key, value):
            return {key: value} if value is not None else {}

        def _convert_if_list(value):
            return tuple(value) if isinstance(value, list) else value

        args = {
            "hosts": self.hosts,
            **_add_if_not_none("cloud_id", self.cloud_id),
            **_add_if_not_none("api_key", _convert_if_list(self.api_key)),
            **_add_if_not_none(
                "basic_auth", _convert_if_list(self.basic_auth)
            ),
            **_add_if_not_none("bearer_auth", self.bearer_auth),
            **_add_if_not_none("opaque_id", self.opaque_id),
            **_add_if_not_none("http_auth", self.http_auth),
            **_add_if_not_none("headers", self.headers),
            **_add_if_not_none("verify_certs", self.verify_certs),
            **_add_if_not_none("ca_certs", self.ca_certs),
            **_add_if_not_none("client_cert", self.client_cert),
            **_add_if_not_none("client_key", self.client_key),
            **_add_if_not_none(
                "ssl_assert_hostname", self.ssl_assert_hostname
            ),
            **_add_if_not_none(
                "ssl_assert_fingerprint", self.ssl_assert_fingerprint
            ),
            **_add_if_not_none("ssl_version", self.ssl_version),
        }

        if self.nparams is not None:
            args.update(self.nparams)

        return args

    def _get_collection_name(
        self,
        collection_name: str | None,
    ) -> str:
        collection_name = (
            collection_name or self.index or self.__component__.collection
        )
        if not collection_name:
            raise BadRequestError("Collection name must be specified")
        return collection_name

    def _get_collection(
        self,
        collection_name: str | None,
    ) -> ElasticsearchCollection:
        col_name = self._get_collection_name(collection_name)
        if col_name in self._collection_cache:
            return self._collection_cache[col_name]
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field,
            col_name,
        )
        collection = ElasticsearchCollection(
            id_map_field,
            col_name,
        )
        self._collection_cache[col_name] = collection
        return collection

    def create_collection(
        self,
        collection: str | None = None,
        config: dict | SearchCollectionConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        args = OperationConverter.convert_create_collection(
            collection=self._get_collection_name(collection),
            config=Helper.get_collection_config(config),
        )
        exists = StoreOperationParser.parse_where_exists(
            where=where, params=kwargs.pop("params", None)
        )
        status = CollectionStatus.CREATED
        try:
            self.client.indices.create(**args)
        except ApiError as e:
            if e.error == "resource_already_exists_exception":
                if exists is False:
                    raise ConflictError("Collection already exists.") from e
                status = CollectionStatus.EXISTS
            else:
                raise e
        result = CollectionResult(
            status=status,
            indexes=[],
        )
        return Response(result=result)

    async def acreate_collection(
        self,
        collection: str | None = None,
        config: dict | SearchCollectionConfig | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        args = OperationConverter.convert_create_collection(
            collection=self._get_collection_name(collection),
            config=Helper.get_collection_config(config),
        )
        exists = StoreOperationParser.parse_where_exists(
            where=where, params=kwargs.pop("params", None)
        )
        status = CollectionStatus.CREATED
        try:
            await self.aclient.indices.create(**args)
        except ApiError as e:
            if e.error == "resource_already_exists_exception":
                if exists is False:
                    raise ConflictError("Collection already exists.") from e
                status = CollectionStatus.EXISTS
            else:
                raise e
        result = CollectionResult(
            status=status,
            indexes=[],
        )
        return Response(result=result)

    def drop_collection(
        self,
        collection: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        index = self._get_collection_name(collection)
        exists = StoreOperationParser.parse_where_exists(
            where=where, params=kwargs.pop("params", None)
        )
        try:
            self.client.indices.delete(index=index)
            # Clear cached collection metadata if present
            if index in self._collection_cache:
                del self._collection_cache[index]
            status = CollectionStatus.DROPPED
        except ApiError as e:
            if getattr(e, "error", None) == "index_not_found_exception":
                if exists is True:
                    raise NotFoundError("Collection does not exist.") from e
                status = CollectionStatus.NOT_EXISTS
            else:
                raise e
        result = CollectionResult(status=status, indexes=[])
        return Response(result=result)

    async def adrop_collection(
        self,
        collection: str | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> Response[CollectionResult]:
        index = self._get_collection_name(collection)
        exists = StoreOperationParser.parse_where_exists(
            where=where, params=kwargs.pop("params", None)
        )
        try:
            await self.aclient.indices.delete(index=index)
            if index in self._collection_cache:
                del self._collection_cache[index]
            status = CollectionStatus.DROPPED
        except ApiError as e:
            if getattr(e, "error", None) == "index_not_found_exception":
                if exists is True:
                    raise NotFoundError("Collection does not exist.")
                status = CollectionStatus.NOT_EXISTS
            else:
                raise e
        result = CollectionResult(status=status, indexes=[])
        return Response(result=result)

    def list_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        nmap = self.client.indices.get_alias(index="*")
        names = list(nmap.keys())
        names = sorted(
            [name for name in names if not str(name).startswith(".")]
        )
        return Response(result=names)

    async def alist_collections(
        self,
        **kwargs: Any,
    ) -> Response[list[str]]:
        nmap = await self.aclient.indices.get_alias(index="*")
        names = list(nmap.keys())
        names = sorted(
            [name for name in names if not str(name).startswith(".")]
        )
        return Response(result=names)

    def has_collection(
        self,
        collection: str | None = None,
    ) -> Response[bool]:
        index = self._get_collection_name(collection)
        exists = bool(self.client.indices.exists(index=index))
        return Response(result=exists)

    async def ahas_collection(
        self,
        collection: str | None = None,
    ) -> Response[bool]:
        index = self._get_collection_name(collection)
        exists = bool(await self.aclient.indices.exists(index=index))
        return Response(result=exists)

    def get(
        self,
        key: str | dict | SearchKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_get(
            key=key,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            resp = self.client.get(**args)
        except ESNotFoundError:
            raise NotFoundError("Item not found.")
        item = col.result_converter.convert_get(response=resp)
        return Response(result=item, native=dict(result=resp))

    async def aget(
        self,
        key: str | dict | SearchKey,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_get(
            key=key,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            resp = await self.aclient.get(**args)
        except ESNotFoundError:
            raise NotFoundError("Item not found.")
        item = col.result_converter.convert_get(response=resp)
        return Response(result=item, native=dict(result=resp))

    def put(
        self,
        value: dict[str, Any] | DataModel,
        key: str | dict | SearchKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_put(
            value=value,
            key=key,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "update":
                resp = self.client.update(**args)
            elif action == "custom":
                get_response = self.get(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return self.put(
                    value=value,
                    key=key,
                    where=f"$etag='{etag}'",
                    collection=collection,
                    **kwargs,
                )
            else:
                resp = self.client.index(**args)
        except NotFoundError:
            raise PreconditionFailedError
        except ESNotFoundError:
            raise PreconditionFailedError
        except ESConflictError:
            raise PreconditionFailedError
        item = col.result_converter.convert_put(response=resp)
        return Response(result=item, native=dict(result=resp))

    async def aput(
        self,
        value: dict[str, Any] | DataModel,
        key: str | dict | SearchKey | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_put(
            value=value,
            key=key,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "update":
                resp = await self.aclient.update(**args)
            elif action == "custom":
                get_response = await self.aget(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return await self.aput(
                    value=value,
                    key=key,
                    where=f"$etag='{etag}'",
                    collection=collection,
                    **kwargs,
                )
            else:
                resp = await self.aclient.index(**args)
        except NotFoundError:
            raise PreconditionFailedError
        except ESNotFoundError:
            raise PreconditionFailedError
        except ESConflictError:
            raise PreconditionFailedError
        item = col.result_converter.convert_put(response=resp)
        return Response(result=item, native=dict(result=resp))

    def delete(
        self,
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_delete(
            key=key,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "custom":
                get_response = self.get(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return self.delete(
                    key=key,
                    where=f"$etag='{etag}'",
                    collection=collection,
                    **kwargs,
                )
            else:
                self.client.delete(**args)
        except ESNotFoundError:
            raise NotFoundError("Item not found.")
        except ESConflictError:
            raise PreconditionFailedError
        return Response(result=None)

    async def adelete(
        self,
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[None]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_delete(
            key=key,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "custom":
                get_response = await self.aget(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return await self.adelete(
                    key=key,
                    where=f"$etag='{etag}'",
                    collection=collection,
                    **kwargs,
                )
            else:
                await self.aclient.delete(**args)
        except ESNotFoundError:
            raise NotFoundError("Item not found.")
        except ESConflictError:
            raise PreconditionFailedError
        return Response(result=None)

    def update(
        self,
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_update(
            key=key,
            set=set,
            where=where,
            returning=returning,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "custom":
                get_response = self.get(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return self.update(
                    key=key,
                    set=set,
                    where=f"$etag='{etag}'",
                    returning=returning,
                    collection=collection,
                    **kwargs,
                )
            else:
                resp = self.client.update(**args)
        except NotFoundError:
            if where:
                raise PreconditionFailedError
            raise NotFoundError
        except ESNotFoundError:
            if where:
                raise PreconditionFailedError
            raise NotFoundError
        except ESConflictError:
            raise PreconditionFailedError
        item = col.result_converter.convert_update(response=resp)
        return Response(result=item, native=dict(result=resp))

    async def aupdate(
        self,
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        collection: str | None = None,
        **kwargs: Any,
    ) -> Response[SearchItem]:
        col = self._get_collection(collection_name=collection)
        action, args = col.op_converter.convert_update(
            key=key,
            set=set,
            where=where,
            returning=returning,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        try:
            if action == "custom":
                get_response = await self.aget(
                    key=SearchKey(id=args["id"]), collection=collection
                )
                if not QueryProcessor.filter_item(
                    get_response.result.value,
                    where=StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params", None)
                    ),
                    field_resolver=col.processor.resolve_field,
                ):
                    raise PreconditionFailedError
                etag = (
                    get_response.result.properties.etag
                    if get_response.result.properties
                    else None
                )
                return await self.aupdate(
                    key=key,
                    set=set,
                    where=f"$etag='{etag}'",
                    returning=returning,
                    collection=collection,
                    **kwargs,
                )
            else:
                resp = await self.aclient.update(**args)
        except NotFoundError:
            if where:
                raise PreconditionFailedError
            raise NotFoundError
        except ESNotFoundError:
            if where:
                raise PreconditionFailedError
            raise NotFoundError
        except ESConflictError:
            raise PreconditionFailedError
        item = col.result_converter.convert_update(response=resp)
        return Response(result=item, native=dict(result=resp))

    def query(
        self,
        search: str | Expression | None = None,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[SearchList]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_query(
            search=search,
            select=select,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        resp = self.client.search(**args)
        result = col.result_converter.convert_query(response=resp)
        return Response(result=result, native=dict(result=resp))

    async def aquery(
        self,
        search: str | Expression | None = None,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[SearchList]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_query(
            search=search,
            select=select,
            where=where,
            order_by=order_by,
            limit=limit,
            offset=offset,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        resp = await self.aclient.search(**args)
        result = col.result_converter.convert_query(response=resp)
        return Response(result=result, native=dict(result=resp))

    def count(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_count(
            search=search,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        resp = self.client.count(**args)
        result = col.result_converter.convert_count(response=resp)
        return Response(result=result, native=dict(result=resp))

    async def acount(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        collection: str | None = None,
        **kwargs,
    ) -> Response[int]:
        col = self._get_collection(collection_name=collection)
        args = col.op_converter.convert_count(
            search=search,
            where=where,
            **kwargs,
        )
        args.update(kwargs.get("nargs", {}))
        resp = await self.aclient.count(**args)
        result = col.result_converter.convert_count(response=resp)
        return Response(result=result, native=dict(result=resp))

    def close(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        if self._init:
            self.client.close()
            self._init = False
        return Response(result=None)

    async def aclose(
        self,
        **kwargs: Any,
    ) -> Response[None]:
        if self._ainit:
            await self.aclient.close()
            self._ainit = False
        return Response(result=None)


class OperationConverter:
    processor: ItemProcessor
    collection: str

    def __init__(self, processor: ItemProcessor, collection: str) -> None:
        self.processor = processor
        self.collection = collection

    @staticmethod
    def convert_create_collection(
        collection: str,
        config: SearchCollectionConfig | None,
    ) -> dict:
        args: dict = {"index": collection}
        body: dict = {}
        if config and config.nconfig:
            body.update(config.nconfig)
        if config and config.indexes:
            body["mappings"] = OperationConverter._convert_indexes(
                config.indexes
            )
        args["body"] = body
        return args

    @staticmethod
    def _convert_indexes(
        indexes: list[Index],
    ) -> dict:
        mappings: dict = {}
        for index in indexes:
            IndexHelper.add_index(mappings, index)
        return mappings

    def convert_get(
        self,
        key: str | dict | SearchKey,
        **kwargs: Any,
    ) -> dict:
        id = Helper.get_id(
            processor=self.processor,
            key=key,
            value=None,
        )
        args = {
            "index": self.collection,
            "id": id,
        }
        return args

    def convert_put(
        self,
        value: dict[str, Any] | DataModel,
        key: str | dict | SearchKey | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> tuple[str, dict]:
        id = Helper.get_id(processor=self.processor, key=key, value=value)
        val = Helper.get_value(value)
        where_etag = StoreOperationParser.parse_where_etag(
            where=where, params=kwargs.get("params", None)
        )
        where_exists = StoreOperationParser.parse_where_exists(
            where=where, params=kwargs.get("params", None)
        )
        where_generic = StoreOperationParser.parse_where(
            where=where, params=kwargs.get("params", None)
        )
        action = "index"
        if_primary_term = None
        if_seq_no = None
        op_type = "index"
        if where_etag is not None:
            if_seq_no, if_primary_term = self._convert_etag(where_etag)
        elif where_exists is False:
            op_type = "create"
        elif where_exists is True:
            action = "update"
        elif where_generic is not None:
            action = "custom"

        if action == "index":
            args: dict = {
                "index": self.collection,
                "id": id,
                "document": val,
                "if_seq_no": if_seq_no,
                "if_primary_term": if_primary_term,
                "op_type": op_type,
            }
        elif action == "update" or action == "custom":
            args = {
                "index": self.collection,
                "id": id,
                "doc": val,
                "if_seq_no": if_seq_no,
                "if_primary_term": if_primary_term,
                "doc_as_upsert": False,
            }
        return action, args

    def convert_delete(
        self,
        key: str | dict | SearchKey,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> tuple[str, dict]:
        id = Helper.get_id(
            processor=self.processor,
            key=key,
            value=None,
        )
        where_etag = StoreOperationParser.parse_where_etag(
            where=where, params=kwargs.get("params", None)
        )
        where_generic = StoreOperationParser.parse_where(
            where=where, params=kwargs.get("params", None)
        )
        if_seq_no = None
        if_primary_term = None
        action = "delete"
        if where_etag is not None:
            if_seq_no, if_primary_term = self._convert_etag(where_etag)
        elif where_generic is not None:
            action = "custom"
        args = {
            "index": self.collection,
            "id": id,
            "if_seq_no": if_seq_no,
            "if_primary_term": if_primary_term,
        }
        return action, args

    def convert_update(
        self,
        key: str | dict | SearchKey,
        set: str | Update,
        where: str | Expression | None = None,
        returning: str | None = None,
        **kwargs: Any,
    ) -> tuple[str, dict]:
        id = Helper.get_id(processor=self.processor, key=key)
        where_etag = StoreOperationParser.parse_where_etag(
            where=where, params=kwargs.get("params", None)
        )
        where_generic = StoreOperationParser.parse_where(
            where=where, params=kwargs.get("params", None)
        )
        if_seq_no = None
        if_primary_term = None
        action = "delete"
        if where_etag is not None:
            if_seq_no, if_primary_term = self._convert_etag(where_etag)
        elif where_generic is not None:
            action = "custom"
        update = StoreOperationParser.parse_set(
            set=set, params=kwargs.get("params", None)
        )
        script_lines: list[str] = []
        params = {}
        param_idx = 0
        var_idx = 0

        def add_param(value: Any) -> str:
            nonlocal param_idx
            name = f"p{param_idx}"
            param_idx += 1
            params[name] = value
            return name

        def make_expr(path: str) -> str:
            """Turn 'a.b.c' into ctx._source['a']['b']['c']."""
            parts = [p for p in path.split(".") if p]
            expr = "ctx._source"
            for p in parts:
                if p.isnumeric():
                    expr += f"[{int(p)}]"
                else:
                    expr += f"['{p}']"
            return expr

        def new_var() -> str:
            nonlocal var_idx
            name = f"arr{var_idx}"
            var_idx += 1
            return name

        for operation in update.operations:
            field = self.processor.resolve_field(operation.field)
            norm_field = field.replace("[", ".").replace("]", "").rstrip(".")
            splits = norm_field.split(".")
            op = operation.op
            if splits[-1].isnumeric():
                array_field = str.join(".", splits[:-1])
                array_index = int(splits[-1])
                array_expr = make_expr(array_field)

                if op == UpdateOp.PUT:
                    # Added if array doesn't exist; replace element at index if present; # noqa
                    # append if index == size.
                    value = operation.args[0]
                    v_name = add_param(value)
                    idx_name = add_param(array_index)
                    var = new_var()
                    script_lines.append(
                        f"if ({array_expr} == null) {{ {array_expr} = new ArrayList(); }} "  # noqa
                        f"List {var} = (List){array_expr}; "
                        f"if (params.{idx_name} >= 0 && params.{idx_name} < {var}.size()) {{ "  # noqa
                        f"  {var}.set(params.{idx_name}, params.{v_name}); "
                        f"}} else if (params.{idx_name} == {var}.size()) {{ "
                        f"  {var}.add(params.{v_name}); "
                        f"}}"
                    )
                elif op == UpdateOp.INSERT:
                    # Added if array doesn't exist. Array element *added* at index. # noqa
                    value = operation.args[0]
                    v_name = add_param(value)
                    idx_name = add_param(array_index)
                    var = new_var()
                    script_lines.append(
                        f"if ({array_expr} == null) {{ {array_expr} = new ArrayList(); }} "  # noqa
                        f"List {var} = (List){array_expr}; "
                        f"if (params.{idx_name} >= 0 && params.{idx_name} <= {var}.size()) {{ "  # noqa
                        f"  {var}.add(params.{idx_name}, params.{v_name}); "
                        f"}} else if (params.{idx_name} > {var}.size()) {{ "
                        f"  {var}.add(params.{v_name}); "
                        f"}}"
                    )
                elif op == UpdateOp.INCREMENT:
                    # Increment/decrement numeric element; create element if needed. # noqa
                    value = operation.args[0]
                    d_name = add_param(value)
                    idx_name = add_param(array_index)
                    var = new_var()
                    script_lines.append(
                        f"if ({array_expr} == null) {{ {array_expr} = new ArrayList(); }} "  # noqa
                        f"List {var} = (List){array_expr}; "
                        f"while ({var}.size() <= params.{idx_name}) {{ {var}.add(null); }} "  # noqa
                        f"Object cur = {var}.get(params.{idx_name}); "
                        f"if (cur == null) {{ "
                        f"  {var}.set(params.{idx_name}, params.{d_name}); "
                        f"}} else {{ "
                        f"  {var}.set(params.{idx_name}, "
                        f"      ((Number)cur).doubleValue() + ((Number)params.{d_name}).doubleValue()); "  # noqa
                        f"}}"
                    )
                elif op == UpdateOp.DELETE:
                    # Remove array element at index.
                    idx_name = add_param(array_index)
                    var = new_var()
                    script_lines.append(
                        f"if ({array_expr} instanceof List) {{ "
                        f"  List {var} = (List){array_expr}; "
                        f"  if (params.{idx_name} >= 0 && params.{idx_name} < {var}.size()) {{ "  # noqa
                        f"    {var}.remove(params.{idx_name}); "
                        f"  }} "
                        f"}}"
                    )
            elif splits[-1] == "-":
                array_field = str.join(".", splits[:-1])
                array_expr = make_expr(array_field)
                if op == UpdateOp.INSERT:
                    # INSERT with '-' → insert at end.
                    value = operation.args[0]
                    v_name = add_param(value)
                    script_lines.append(
                        f"if ({array_expr} == null) {{ {array_expr} = new ArrayList(); }} "  # noqa
                        f"((List){array_expr}).add(params.{v_name});"
                    )
                elif op == UpdateOp.DELETE:
                    # DELETE with '-' → remove last element.
                    var = new_var()
                    script_lines.append(
                        f"if ({array_expr} instanceof List) {{ "
                        f"  List {var} = (List){array_expr}; "
                        f"  if ({var}.size() > 0) {{ {var}.remove({var}.size() - 1); }} "  # noqa
                        f"}}"
                    )
            else:
                field_expr = make_expr(norm_field)
                if op == UpdateOp.PUT:
                    # Added if it doesn't exist; replaced if it exists.
                    value = operation.args[0]
                    v_name = add_param(value)
                    script_lines.append(f"{field_expr} = params.{v_name};")
                elif op == UpdateOp.INSERT:
                    # For scalars, same semantics as PUT (add/replace).
                    value = operation.args[0]
                    v_name = add_param(value)
                    script_lines.append(f"{field_expr} = params.{v_name};")
                elif op == UpdateOp.INCREMENT:
                    # Increment/decrement numeric field; create if missing.
                    value = operation.args[0]
                    d_name = add_param(value)
                    script_lines.append(
                        f"if ({field_expr} == null) {{ "
                        f"  {field_expr} = params.{d_name}; "
                        f"}} else {{ "
                        f"  {field_expr} = "
                        f"    ((Number){field_expr}).doubleValue() + ((Number)params.{d_name}).doubleValue(); "  # noqa
                        f"}}"
                    )
                elif op == UpdateOp.DELETE:
                    # Remove field. Handle top-level vs nested.
                    if len(splits) == 1:
                        script_lines.append(
                            f"ctx._source.remove('{splits[0]}');"
                        )
                    else:
                        parent_path = ".".join(splits[:-1])
                        parent_expr = make_expr(parent_path)
                        leaf = splits[-1]
                        script_lines.append(
                            f"if ({parent_expr} instanceof Map) {{ "
                            f"  ((Map){parent_expr}).remove('{leaf}'); "
                            f"}}"
                        )
                elif op == UpdateOp.MOVE:
                    # MOVE: operation.field is destination; args[0] is source field. # noqa

                    src_field = self.processor.resolve_field(operation.field)
                    src_norm = (
                        src_field.replace("[", ".")
                        .replace("]", "")
                        .rstrip(".")
                    )
                    src_splits = src_norm.split(".")
                    dest_field = self.processor.resolve_field(
                        operation.args[0].path
                    )
                    dest_norm = (
                        dest_field.replace("[", ".")
                        .replace("]", "")
                        .rstrip(".")
                    )
                    dest_expr = make_expr(dest_norm)

                    # Assign dest = src
                    script_lines.append(
                        f"if ({field_expr} != null) {{ "
                        f"  {dest_expr} = {field_expr}; "
                        f"}}"
                    )
                    # Remove source key
                    if len(src_splits) == 1:
                        script_lines.append(
                            f"ctx._source.remove('{src_splits[0]}');"
                        )
                    else:
                        src_parent_path = ".".join(src_splits[:-1])
                        src_parent_expr = make_expr(src_parent_path)
                        src_leaf = src_splits[-1]
                        script_lines.append(
                            f"if ({src_parent_expr} instanceof Map) {{ "
                            f"  ((Map){src_parent_expr}).remove('{src_leaf}'); "  # noqa
                            f"}}"
                        )
                elif op == UpdateOp.ARRAY_UNION:
                    # Union values into array without duplicates.
                    # value can be a scalar or list.
                    value = operation.args[0]
                    v_name = add_param(value)
                    var = new_var()
                    item = new_var()
                    script_lines.append(
                        f"if ({field_expr} == null) {{ {field_expr} = new ArrayList(); }} "  # noqa
                        f"List {var} = (List){field_expr}; "
                        f"if (params.{v_name} instanceof List) {{ "
                        f"  for (def {item} : params.{v_name}) {{ "
                        f"    if (!{var}.contains({item})) {{ {var}.add({item}); }} "  # noqa
                        f"  }} "
                        f"}} else {{ "
                        f"  if (!{var}.contains(params.{v_name})) {{ {var}.add(params.{v_name}); }} "  # noqa
                        f"}}"
                    )
                elif op == UpdateOp.ARRAY_REMOVE:
                    # Remove all occurrences of given values from array.
                    value = operation.args[0]
                    v_name = add_param(value)
                    var = new_var()
                    item = new_var()
                    vals = new_var()
                    script_lines.append(
                        f"if ({field_expr} instanceof List) {{ "
                        f"  def {var} = {field_expr}; "
                        f"  def {vals} = params.{v_name}; "
                        f"  if (!({vals} instanceof List)) {{ {vals} = [{vals}]; }} "  # noqa
                        f"  for (def {item} : {vals}) {{ "
                        f"    for (int i = {var}.size() - 1; i >= 0; i--) {{ "
                        f"      def cur = {var}.get(i); "
                        f"      if (cur == {item} || (cur != null && cur.equals({item}))) {{ "  # noqa
                        f"        {var}.remove(i); "
                        f"      }} "
                        f"    }} "
                        f"  }} "
                        f"}}"
                    )
                elif op == UpdateOp.APPEND:
                    # Append to existing string value.
                    value = operation.args[0]
                    v_name = add_param(value)
                    script_lines.append(
                        f"if ({field_expr} == null) {{ "
                        f"  {field_expr} = params.{v_name}.toString(); "
                        f"}} else {{ "
                        f"  {field_expr} = {field_expr}.toString() + params.{v_name}.toString(); "  # noqa
                        f"}}"
                    )
                elif op == UpdateOp.PREPEND:
                    # Prepend to existing string value.
                    value = operation.args[0]
                    v_name = add_param(value)
                    script_lines.append(
                        f"if ({field_expr} == null) {{ "
                        f"  {field_expr} = params.{v_name}.toString(); "
                        f"}} else {{ "
                        f"  {field_expr} = params.{v_name}.toString() + {field_expr}.toString(); "  # noqa
                        f"}}"
                    )

        script = "\n".join(script_lines)
        args = {
            "index": self.collection,
            "id": id,
            "script": {
                "source": script,
                "lang": "painless",
                "params": params,
            },
            "if_seq_no": if_seq_no,
            "if_primary_term": if_primary_term,
        }
        if returning == "new":
            args["source"] = True
        return action, args

    def convert_query(
        self,
        search: str | Expression | None = None,
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        **kwargs: Any,
    ) -> dict:
        args: dict = {
            "index": self.collection,
            "seq_no_primary_term": True,
            "track_scores": True,
            "source": {"exclude_vectors": False},
        }
        if limit:
            args["size"] = self.convert_limit(limit=limit)
        if offset:
            args["from_"] = self.convert_offset(offset=offset)
        if select:
            args["source"] = self.convert_select(
                select=StoreOperationParser.parse_select(
                    select=select, params=kwargs.get("params", None)
                )
            )
        if order_by:
            args["sort"] = self.convert_order_by(
                order_by=StoreOperationParser.parse_order_by(
                    order_by=order_by, params=kwargs.get("params", None)
                )
            )
        if where or search:
            search_where_query = self.convert_search_where(
                search=search,
                where=where,
                **kwargs,
            )
            args.update(search_where_query or {})
        return args

    def convert_count(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> dict:
        args: dict = {
            "index": self.collection,
        }
        if where or search:
            search_where_query = self.convert_search_where(
                search=search,
                where=where,
                **kwargs,
            )
            args.update(search_where_query or {})
        return args

    def convert_search_where(
        self,
        search: str | Expression | None = None,
        where: str | Expression | None = None,
        **kwargs: Any,
    ) -> dict | None:
        search_query = None
        where_query = None
        if search is not None:
            search_expr = StoreOperationParser.parse_search(
                search=search, params=kwargs.get("params")
            )
            search_query = self.convert_expr(search_expr)
            if "knn" in search_query:
                if where is not None:
                    where_expr = StoreOperationParser.parse_where(
                        where=where, params=kwargs.get("params")
                    )
                    where_query = self.convert_expr(where_expr)
                    search_query["knn"]["filter"] = where_query
                    if "query" in search_query:
                        search_query["query"] = {
                            "bool": {
                                "must": [
                                    search_query["query"],
                                    where_query,
                                ]
                            }
                        }
                return search_query

        if where is not None:
            where_expr = StoreOperationParser.parse_where(
                where=where, params=kwargs.get("params")
            )
            where_query = self.convert_expr(where_expr)
        if search_query and where_query:
            return {
                "query": {
                    "bool": {
                        "must": [
                            search_query,
                            where_query,
                        ]
                    }
                }
            }
        elif search_query:
            return {"query": search_query}
        elif where_query:
            return {"query": where_query}
        return None

    def convert_select(self, select: Select | None = None) -> list | None:
        if select is None:
            return None
        fields: list[str] = []
        for term in select.terms:
            resolved = self.processor.resolve_field(term.field)
            fields.append(resolved)
        return fields

    def convert_limit(
        self,
        limit: int | None = None,
    ) -> int | None:
        return limit

    def convert_offset(
        self,
        offset: int | None = None,
    ) -> int | None:
        return offset

    def convert_order_by(
        self,
        order_by: OrderBy | None = None,
    ) -> list:
        args: list = []
        if order_by is None:
            return args
        for term in order_by.terms:
            field = self.processor.resolve_field(term.field)
            direction = (
                "desc" if term.direction == OrderByDirection.DESC else "asc"
            )
            sort_spec: dict = {"order": direction}
            nested_spec = self._build_nested_chain(field)
            if nested_spec is not None:
                sort_spec["nested"] = nested_spec
            args.append({field: sort_spec})
        return args

    def convert_expr(self, expr: Expression | None) -> Any:
        if expr is None or isinstance(
            expr, (str, int, float, bool, dict, list)
        ):
            return expr
        if isinstance(expr, Field):
            return self.convert_field(expr)
        if isinstance(expr, Function):
            return self.convert_func(expr)
        if isinstance(expr, Comparison):
            return self.convert_comparison(expr)
        if isinstance(expr, And):
            return {
                "bool": {
                    "must": [
                        self.convert_expr(expr.lexpr),
                        self.convert_expr(expr.rexpr),
                    ]
                }
            }
        if isinstance(expr, Or):
            return {
                "bool": {
                    "should": [
                        self.convert_expr(expr.lexpr),
                        self.convert_expr(expr.rexpr),
                    ],
                    "minimum_should_match": 1,
                }
            }
        if isinstance(expr, Not):
            return {
                "bool": {
                    "must_not": [
                        self.convert_expr(expr.expr),
                    ]
                }
            }

        raise BadRequestError(f"Expression {expr!r} not supported")

    def convert_comparison(self, expr: Comparison) -> dict[str, Any]:
        """
        Convert a Comparison expression into an Elasticsearch query fragment.

        Supported ops:
        - <, <=, >, >=  -> range
        - =             -> term
        - !=            -> bool.must_not + term
        - IN            -> terms
        - NIN           -> bool.must_not + terms
        - BETWEEN       -> range gte/lte
        - LIKE          -> regexp
        """

        func = None
        reverse_op = {
            ComparisonOp.LT: ComparisonOp.GT,
            ComparisonOp.LTE: ComparisonOp.GTE,
            ComparisonOp.GT: ComparisonOp.LT,
            ComparisonOp.GTE: ComparisonOp.LTE,
        }

        op = expr.op

        if isinstance(expr.lexpr, Field):
            lhs = self.convert_field(expr.lexpr)
            rhs: Any = expr.rexpr
        elif isinstance(expr.rexpr, Field):
            lhs = self.convert_field(expr.rexpr)
            rhs = expr.lexpr
            op = reverse_op.get(op, op)
        elif isinstance(expr.lexpr, Function):
            func = expr.lexpr
            rhs = expr.rexpr
        elif isinstance(expr.rexpr, Function):
            func = expr.rexpr
            rhs = expr.lexpr
            op = reverse_op.get(op, op)
        else:
            raise BadRequestError(
                "Comparison not supported: no field or function"
            )

        if func:
            if (
                func.namespace == FunctionNamespace.BUILTIN
                and func.name == QueryFunctionName.LENGTH
            ):
                field = self.convert_field(func.args[0])
                return self._convert_length_condition(
                    func=QueryFunctionName.LENGTH,
                    field=field,
                    op=op,
                    value=int(rhs),
                )
            elif (
                func.namespace == FunctionNamespace.BUILTIN
                and func.name == QueryFunctionName.ARRAY_LENGTH
            ):
                field = self.convert_field(func.args[0])
                return self._convert_length_condition(
                    func=QueryFunctionName.ARRAY_LENGTH,
                    field=field,
                    op=op,
                    value=int(rhs),
                )
            else:
                raise BadRequestError(
                    f"Function {func} not supported in comparison for Elasticsearch"  # noqa
                )

        field = lhs
        value = rhs

        # --------------------------------
        # Simple operators
        # --------------------------------
        if op is ComparisonOp.EQ:
            # exact match
            return {"term": {field: value}}

        if op is ComparisonOp.NEQ:
            # NOT equal: wrap in bool.must_not
            return {"bool": {"must_not": [{"term": {field: value}}]}}

        if op in (
            ComparisonOp.LT,
            ComparisonOp.LTE,
            ComparisonOp.GT,
            ComparisonOp.GTE,
        ):
            range_map = {
                ComparisonOp.LT: "lt",
                ComparisonOp.LTE: "lte",
                ComparisonOp.GT: "gt",
                ComparisonOp.GTE: "gte",
            }
            es_op = range_map[op]
            return {"range": {field: {es_op: value}}}

        if op is ComparisonOp.IN:
            # value should be a list/iterable
            return {"terms": {field: value}}

        if op is ComparisonOp.NIN:
            return {"bool": {"must_not": [{"terms": {field: value}}]}}

        # --------------------------------
        # BETWEEN and LIKE
        # --------------------------------
        if op is ComparisonOp.BETWEEN:
            # value is expected to be [lower, upper]
            if isinstance(value, list) and len(value) >= 2:
                return {
                    "range": {
                        field: {
                            "gte": value[0],
                            "lte": value[1],
                        }
                    }
                }
            raise BadRequestError(
                f"BETWEEN expects [lower, upper], got {value!r}"
            )

        if op is ComparisonOp.LIKE:
            # Here we don't introduce wildcards; this behaves like "match this literal string" # noqa
            # but with regex engine (you can extend later if you want %/_-style LIKE).  # noqa
            pattern = re.escape(str(value))
            return {"regexp": {field: pattern}}

        raise BadRequestError(f"Comparison {expr} not supported")

    def convert_func(self, expr: Function) -> Any:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        named_args = expr.named_args

        if namespace != FunctionNamespace.BUILTIN:
            raise BadRequestError(
                f"Function namespace {namespace} not supported for Elasticsearch"  # noqa
            )

        # -------------------------
        # IS_TYPE(field, type)
        # -------------------------
        if name == QueryFunctionName.IS_TYPE:
            return self._convert_is_type(
                field=self.convert_field(args[0]),
                type=str(args[1]),
            )

        # -------------------------
        # IS_DEFINED(field)
        # -------------------------
        if name == QueryFunctionName.IS_DEFINED:
            field = self.convert_field(args[0])
            return {
                "exists": {
                    "field": field,
                }
            }

        # -------------------------
        # IS_NOT_DEFINED(field)
        # -------------------------
        if name == QueryFunctionName.IS_NOT_DEFINED:
            field = self.convert_field(args[0])
            return {
                "bool": {
                    "must_not": [
                        {
                            "exists": {
                                "field": field,
                            }
                        }
                    ]
                }
            }

        # -------------------------
        # CONTAINS(field, substring)
        # -------------------------
        if name == QueryFunctionName.CONTAINS:
            field = self.convert_field(args[0])
            substring = str(args[1])

            # NOTE: this assumes you're querying a keyword / non-analyzed field. # noqa
            # We wrap with *...* to represent "contains".
            #
            # You may want to escape '*' and '?' which have special meaning in wildcard queries. # noqa
            pattern = f"*{substring}*"
            return {
                "wildcard": {
                    field: {
                        "value": pattern,
                    }
                }
            }

        # -------------------------
        # STARTS_WITH(field, prefix)
        # -------------------------
        if name == QueryFunctionName.STARTS_WITH:
            field = self.convert_field(args[0])
            prefix = str(args[1])
            return {"prefix": {field: prefix}}

        # -------------------------
        # ARRAY_CONTAINS(field, value)
        # -------------------------
        if name == QueryFunctionName.ARRAY_CONTAINS:
            field = self.convert_field(args[0])
            value = args[1]

            # ES represents arrays as multi-valued fields; term query works for "contains". # noqa
            return {"term": {field: value}}

        # -------------------------
        # ARRAY_CONTAINS_ANY(field, values)
        # -------------------------
        if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
            field = self.convert_field(args[0])
            values = args[1]

            # Any of the values present -> terms query
            return {"terms": {field: values}}

        if name == QueryFunctionName.TEXT_SEARCH:
            return self._convert_text_search_func(
                args=args, named_args=named_args
            )

        if name == QueryFunctionName.VECTOR_SEARCH:
            return self._convert_vector_search_func(
                args=args, named_args=named_args
            )

        if name == QueryFunctionName.SPARSE_VECTOR_SEARCH:
            return self._convert_sparse_vector_search_func(
                args=args, named_args=named_args
            )

        if name == QueryFunctionName.HYBRID_VECTOR_SEARCH:
            return self._convert_hybrid_vector_search(
                args=args, named_args=named_args
            )

        if name == QueryFunctionName.HYBRID_TEXT_SEARCH:
            return self._convert_hybrid_text_search_func(
                args=args, named_args=named_args
            )

        raise BadRequestError(
            f"Function {name} not recognized or not supported for Elasticsearch"  # noqa
        )

    def _convert_hybrid_text_search_func(
        self,
        args: list[Any],
        named_args: dict[str, Any],
    ) -> dict[str, Any]:
        text_named_args = named_args.copy()
        vector_named_args = named_args.copy()
        sparse_vector_named_args = named_args.copy()
        if args:
            if len(args) >= 1:
                text_named_args["query"] = args[0]
            if len(args) >= 2:
                text_named_args["fields"] = args[1]
            if len(args) >= 3:
                text_named_args["match_mode"] = args[2]
            if len(args) >= 4:
                text_named_args["query_type"] = args[3]
            if len(args) >= 5:
                text_named_args["fuzziness"] = args[4]
            if len(args) >= 6:
                text_named_args["minimum_should_match"] = args[5]
            if len(args) >= 7:
                text_named_args["analyzer"] = args[6]
            if len(args) >= 8:
                text_named_args["boost"] = args[7]
            if len(args) >= 9:
                vector_named_args["vector"] = args[8]
            if len(args) >= 10:
                vector_named_args["field"] = args[9]
            if len(args) >= 11:
                vector_named_args["k"] = args[10]
            if len(args) >= 12:
                vector_named_args["num_candidates"] = args[11]
            if len(args) >= 13:
                sparse_vector_named_args["sparse_vector"] = args[12]
            if len(args) >= 14:
                sparse_vector_named_args["field"] = args[13]
            if len(args) >= 15:
                named_args["hybrid_mode"] = args[14]
            if len(args) >= 16:
                named_args["text_weight"] = args[15]
            if len(args) >= 17:
                named_args["vector_weight"] = args[16]
        result: dict = {}
        if "vector" in vector_named_args:
            vector_named_args["field"] = vector_named_args.get(
                "field"
            ) or named_args.get("vector_field")
            result = self._convert_vector_search_func(
                args=args[8:12],
                named_args=vector_named_args,
            )
        if "query" in text_named_args:
            text_search_clause = self._convert_text_search_func(
                args=args[:8],
                named_args=text_named_args,
            )
            if "knn" in result:
                result["query"] = text_search_clause
            else:
                result = text_search_clause
        if "sparse_vector" in sparse_vector_named_args:
            sparse_vector_named_args["field"] = sparse_vector_named_args.get(
                "field"
            ) or named_args.get("sparse_vector_field")
            sparse_vector_search_clause = (
                self._convert_sparse_vector_search_func(
                    args=args[12:13],
                    named_args=sparse_vector_named_args,
                )
            )
            if "knn" in result:
                if "query" in result:
                    result["query"] = {
                        "bool": {
                            "must": [
                                result["query"],
                                sparse_vector_search_clause,
                            ]
                        }
                    }
                else:
                    result["query"] = sparse_vector_search_clause
            else:
                if not result:
                    result = sparse_vector_search_clause
                else:
                    result = {
                        "bool": {
                            "must": [
                                result,
                                sparse_vector_search_clause,
                            ]
                        }
                    }
        hybrid_mode = named_args.get("hybrid_mode") or None
        if hybrid_mode is not None and "knn" in result:
            if hybrid_mode != "rrf":
                raise BadRequestError(
                    f"Elasticsearch hybrid text+vector+sparse currently supports only "  # noqa
                    f"hybrid_mode='rrf' (got {hybrid_mode!r})."
                )

            k = vector_named_args.get("k")
            window_size = int(k or 60)
            result["rank"] = {
                "rrf": {
                    "window_size": window_size,
                    "rank_constant": 60,
                }
            }
        return result

    def _convert_hybrid_vector_search(
        self,
        args: list[Any],
        named_args: dict[str, Any],
    ) -> dict[str, Any]:
        """
        HYBRID_VECTOR_SEARCH(vector, field, k, num_candidates, sparse_vector, sparse_vector_field, hybrid_mode, vector_weight, sparse_vector_weight) -> ES 'knn' clause with sparse vector query. # noqa

        Expected named_args keys:
        - vector: list[float] (required)
        - field: str (required)
        - k: int (optional)
        - num_candidates: int (optional)
        - sparse_vector: dict[int, float] (optional)
        - sparse_vector_field: str (optional)
        - hybrid_mode: str (optional, default "rrf")
        - vector_weight: float (optional)
        - sparse_vector_weight: float (optional)
        """
        vector_named_args = named_args.copy()
        sparse_vector_named_args = named_args.copy()
        if args:
            if len(args) >= 1:
                vector_named_args["vector"] = args[0]
            if len(args) >= 2:
                vector_named_args["field"] = args[1]
            if len(args) >= 3:
                vector_named_args["k"] = args[2]
            if len(args) >= 4:
                vector_named_args["num_candidates"] = args[3]
            if len(args) >= 5:
                sparse_vector_named_args["sparse_vector"] = args[4]
            if len(args) >= 6:
                sparse_vector_named_args["field"] = args[5]
            if len(args) >= 7:
                named_args["hybrid_mode"] = args[6]
        vector_search_clause = self._convert_vector_search_func(
            args=args[:4],
            named_args=vector_named_args,
        )
        if "sparse_vector" in sparse_vector_named_args:
            sparse_vector_named_args["field"] = sparse_vector_named_args.get(
                "field"
            ) or named_args.get("sparse_vector_field")
            sparse_vector_search_clause = (
                self._convert_sparse_vector_search_func(
                    args=args[4:5],
                    named_args=sparse_vector_named_args,
                )
            )
            vector_search_clause["query"] = sparse_vector_search_clause
            hybrid_mode = named_args.get("hybrid_mode") or None
            if hybrid_mode is not None:
                if hybrid_mode != "rrf":
                    raise BadRequestError(
                        f"Elasticsearch hybrid vector+sparse currently supports only "  # noqa
                        f"hybrid_mode='rrf' (got {hybrid_mode!r})."
                    )

                k = vector_named_args.get("k")
                window_size = int(k or 60)
                vector_search_clause["rank"] = {
                    "rrf": {
                        "window_size": window_size,
                        "rank_constant": 60,
                    }
                }
        return vector_search_clause

    def _convert_vector_search_func(
        self,
        args: list[Any],
        named_args: dict[str, Any],
    ) -> dict[str, Any]:
        """
        VECTOR_SEARCH(vector, field, k, num_candidates) -> ES 'knn' clause.

        Expected named_args keys:
        - vector: list[float] (required)
        - field: str (required)
        - k: int (optional)
        - num_candidates: int (optional)
        """
        DEFAULT_K = 10
        DEFAULT_NUM_CANDIDATES_MULTIPLIER = 10
        if args:
            if len(args) >= 1:
                named_args["vector"] = args[0]
            if len(args) >= 2:
                named_args["field"] = args[1]
            if len(args) >= 3:
                named_args["k"] = args[2]
            if len(args) >= 4:
                named_args["num_candidates"] = args[3]
        vector = named_args.get("vector")
        if not vector:
            raise BadRequestError(
                "VECTOR_SEARCH requires a non-empty 'vector' argument"
            )

        field = named_args.get("field")
        if not field:
            raise BadRequestError("VECTOR_SEARCH requires 'field' argument")

        # k and num_candidates defaults
        k = named_args.get("k") or DEFAULT_K

        num_candidates = (
            named_args.get("num_candidates")
            or k * DEFAULT_NUM_CANDIDATES_MULTIPLIER
        )
        if num_candidates < k:
            num_candidates = k
        return {
            "knn": {
                "field": field,
                "query_vector": vector,
                "k": k,
                "num_candidates": num_candidates,
            }
        }

    def _convert_sparse_vector_search_func(
        self,
        args: list[Any],
        named_args: dict[str, Any],
    ) -> dict[str, Any]:
        if args:
            if len(args) >= 1:
                named_args["sparse_vector"] = args[0]
            if len(args) >= 2:
                named_args["field"] = args[1]
        sparse_vector = named_args.get("sparse_vector")
        if not sparse_vector:
            raise BadRequestError(
                "SPARSE_VECTOR_SEARCH requires a non-empty 'sparse_vector' argument"  # noqa
            )

        field = named_args.get("field")
        if not field:
            raise BadRequestError(
                "SPARSE_VECTOR_SEARCH requires 'field' argument"
            )
        return {
            field: {
                "field": field,
                "query_vector": sparse_vector,
            }
        }

    def _convert_text_search_func(
        self,
        args: list[Any],
        named_args: dict[str, Any],
    ) -> dict[str, Any]:
        if args:
            if len(args) >= 1:
                named_args["query"] = args[0]
            if len(args) >= 2:
                named_args["fields"] = args[1]
            if len(args) >= 3:
                named_args["match_mode"] = args[2]
            if len(args) >= 4:
                named_args["query_type"] = args[3]
            if len(args) >= 5:
                named_args["fuzziness"] = args[4]
            if len(args) >= 6:
                named_args["minimum_should_match"] = args[5]
            if len(args) >= 7:
                named_args["analyzer"] = args[6]
            if len(args) >= 8:
                named_args["boost"] = args[7]

        def apply_boosts_to_fields(
            fields: list[str] | None,
            boost: dict[str, float] | None,
        ) -> list[str] | None:
            if fields is None and not boost:
                return None

            fields = list(fields or [])
            boost = boost or {}

            es_fields: list[str] = []

            # Add boosts for listed fields
            for f in fields:
                b = boost.get(f)
                if b is not None:
                    es_fields.append(f"{f}^{b}")
                else:
                    es_fields.append(f)

            # Add boosted fields not explicitly listed
            for f, b in boost.items():
                if f not in fields:
                    es_fields.append(f"{f}^{b}")

            return es_fields or None

        def set_if_not_none(d: dict[str, Any], key: str, value: Any) -> None:
            if value is not None:
                d[key] = value

        def resolve_multi_match_type(query_type: str | None) -> str:
            if query_type in (
                "best_fields",
                "most_fields",
                "cross_fields",
                "phrase",
                "phrase_prefix",
                "prefix",
            ):
                return query_type
            return "best_fields"

        query: str = named_args.get("query") or ""
        fields: list[str] | None = named_args.get("fields") or None
        match_mode = named_args.get("match_mode")
        query_type: str | None = named_args.get("query_type")
        fuzziness = named_args.get("fuzziness")
        minimum_should_match = named_args.get("minimum_should_match")
        analyzer: str | None = named_args.get("analyzer")
        boost: dict[str, float] | None = named_args.get("boost")

        operator = "and" if match_mode == "and" else "or"
        num_fields = len(fields) if fields else 0

        # 1) SIMPLE → simple_query_string
        if query_type == "simple":
            es_fields = apply_boosts_to_fields(fields, boost)
            body: dict[str, Any] = {
                "query": query,
                "default_operator": operator,
            }
            set_if_not_none(body, "fields", es_fields)
            set_if_not_none(body, "analyzer", analyzer)
            # fuzziness / minimum_should_match don't map cleanly here
            return {"simple_query_string": body}

        # 2) FULL → query_string
        if query_type == "full":
            es_fields = apply_boosts_to_fields(fields, boost)
            body = {
                "query": query,
                "default_operator": operator,
            }
            set_if_not_none(body, "fields", es_fields)
            set_if_not_none(body, "analyzer", analyzer)
            return {"query_string": body}

        # From here on: match / multi_match family
        es_type = resolve_multi_match_type(query_type)

        # 3) PHRASE / PHRASE_PREFIX
        if es_type in ("phrase", "phrase_prefix"):
            if fields and num_fields == 1:
                field = fields[0]
                inner: dict[str, Any] = {"query": query}
                set_if_not_none(inner, "analyzer", analyzer)
                if es_type == "phrase":
                    return {"match_phrase": {field: inner}}
                return {"match_phrase_prefix": {field: inner}}
            else:
                es_fields = apply_boosts_to_fields(fields, boost) or ["*"]
                body = {
                    "query": query,
                    "fields": es_fields,
                    "type": es_type,
                }
                set_if_not_none(body, "analyzer", analyzer)
                return {"multi_match": body}

        # 4) PREFIX → match_bool_prefix / multi_match(bool_prefix)
        if es_type == "prefix":
            if fields and num_fields == 1:
                field = fields[0]
                inner = {
                    "query": query,
                    "operator": operator,
                }
                set_if_not_none(inner, "analyzer", analyzer)
                return {"match_bool_prefix": {field: inner}}
            else:
                es_fields = apply_boosts_to_fields(fields, boost) or ["*"]
                body = {
                    "query": query,
                    "fields": es_fields,
                    "type": "bool_prefix",
                    "operator": operator,
                }
                set_if_not_none(body, "analyzer", analyzer)
                return {"multi_match": body}

        # 5) GENERIC: single field → match, multi-field → multi_match
        if fields and num_fields == 1:
            field = fields[0]
            inner = {
                "query": query,
                "operator": operator,
            }
            set_if_not_none(inner, "fuzziness", fuzziness)
            set_if_not_none(
                inner, "minimum_should_match", minimum_should_match
            )
            set_if_not_none(inner, "analyzer", analyzer)
            # boost is redundant for a single field
            return {"match": {field: inner}}

        es_fields = apply_boosts_to_fields(fields, boost) or ["*"]
        body = {
            "query": query,
            "fields": es_fields,
            "type": (
                es_type
                if es_type in ("best_fields", "most_fields", "cross_fields")
                else "best_fields"
            ),
            "operator": operator,
        }
        set_if_not_none(body, "fuzziness", fuzziness)
        set_if_not_none(body, "minimum_should_match", minimum_should_match)
        set_if_not_none(body, "analyzer", analyzer)

        return {"multi_match": body}

    def _convert_length_condition(
        self,
        func: str,
        field: str,
        op: ComparisonOp,
        value: int,
    ) -> dict[str, Any]:
        """
        field: "name" or "tags.0" etc
        op: one of "==", "!=", ">", ">=", "<", "<="
        value: integer length to compare against
        """

        op_map = {
            ComparisonOp.EQ: "==",
            ComparisonOp.NEQ: "!=",
            ComparisonOp.LT: "<",
            ComparisonOp.LTE: "<=",
            ComparisonOp.GT: ">",
            ComparisonOp.GTE: ">=",
        }

        # Map to actual comparison operator symbol
        if op not in op_map:
            raise BadRequestError(
                f"Unsupported length comparison operator: {op}"
            )

        if func == QueryFunctionName.LENGTH:
            splits = field.split(".")
            last = splits[-1]
            # ---------- Case 1: array element, e.g. "tags.0" ----------
            if last.isnumeric():
                arr_field = ".".join(splits[:-1])
                arr_index = int(last)
                source = f"""
                    if (!doc.containsKey(params.arr_field)) {{
                        return false;
                    }}
                    if (doc[params.arr_field].size() <= params.idx) {{
                        return false;
                    }}

                    def val = doc[params.arr_field][params.idx];
                    if (!(val instanceof String)) {{
                        return false;
                    }}

                    int len = val.length();
                    return len {op_map[op]} params.value;
                """

                return {
                    "script": {
                        "script": {
                            "source": source,
                            "params": {
                                "arr_field": arr_field,
                                "idx": arr_index,
                                "value": value,
                            },
                        }
                    }
                }
            else:
                source = f"""
                    if (!doc.containsKey(params.field)) {{
                        return false;
                    }}
                    if (doc[params.field].size() == 0) {{
                        return false;
                    }}

                    def val = doc[params.field].value;
                    if (!(val instanceof String)) {{
                        return false;
                    }}

                    int len = val.length();
                    return len {op_map[op]} params.value;
                """

                return {
                    "script": {
                        "script": {
                            "source": source,
                            "params": {
                                "field": field,
                                "value": value,
                            },
                        }
                    }
                }
        elif func == QueryFunctionName.ARRAY_LENGTH:
            source = f"""
                if (!doc.containsKey(params.field)) {{
                    return false;
                }}

                int len = doc[params.field].size();
                return len {op_map[op]} params.value;
            """
            return {
                "script": {
                    "script": {
                        "source": source,
                        "params": {
                            "field": field,
                            "value": value,
                        },
                    }
                }
            }
        raise BadRequestError(f"Unsupported length function: {func}")

    def _convert_is_type(self, field: str, type: str) -> dict[str, Any]:
        """
        NOTE:
        - ES has no native "type" operator, so we emulate it using scripts.
        - OBJECT detection is only approximate (see comments).
        """

        # STRING
        if type == SearchFieldType.STRING:
            return {
                "script": {
                    "script": {
                        "source": """
                            doc.containsKey(params.field) &&
                            doc[params.field].size() > 0 &&
                            (doc[params.field].value instanceof String)
                        """,
                        "params": {"field": field},
                    }
                }
            }

        # NUMBER (double / long / int etc.)
        if type == SearchFieldType.NUMBER:
            return {
                "script": {
                    "script": {
                        "source": """
                            doc.containsKey(params.field) &&
                            doc[params.field].size() > 0 &&
                            (
                                doc[params.field].value instanceof Double ||
                                doc[params.field].value instanceof Long   ||
                                doc[params.field].value instanceof Integer
                            )
                        """,
                        "params": {"field": field},
                    }
                }
            }

        # BOOLEAN
        if type == SearchFieldType.BOOLEAN:
            return {
                "script": {
                    "script": {
                        "source": """
                            doc.containsKey(params.field) &&
                            doc[params.field].size() > 0 &&
                            (doc[params.field].value instanceof Boolean)
                        """,
                        "params": {"field": field},
                    }
                }
            }

        # ARRAY  (multi-valued field: more than one value in doc-values)
        if type == SearchFieldType.ARRAY:
            return {
                "script": {
                    "script": {
                        "source": """
                            doc.containsKey(params.field) &&
                            doc[params.field].size() > 1
                        """,
                        "params": {"field": field},
                    }
                }
            }

        # NULL  (ES doesn't store null; "null" ≈ "field does not exist")
        if type == SearchFieldType.NULL:
            return {"bool": {"must_not": [{"exists": {"field": field}}]}}

        # OBJECT (approximate – ES doesn't expose objects as doc-values)
        if type == SearchFieldType.OBJECT:
            # This is heuristic: "field exists and is not a common primitive"
            # It will *not* be perfect, but it's the closest we can get
            # without consulting index mappings.
            return {
                "script": {
                    "script": {
                        "source": """
                            doc.containsKey(params.field) &&
                            doc[params.field].size() > 0 &&
                            !(
                                doc[params.field].value instanceof String  ||
                                doc[params.field].value instanceof Long    ||
                                doc[params.field].value instanceof Double  ||
                                doc[params.field].value instanceof Integer ||
                                doc[params.field].value instanceof Boolean
                            )
                        """,
                        "params": {"field": field},
                    }
                }
            }

        raise BadRequestError(
            f"Unsupported DocumentFieldType for IS_TYPE: {type}"
        )

    def _build_nested_chain(self, field: str) -> dict[str, Any] | None:
        parts = field.split(".")
        if len(parts) <= 1:
            paths = []
        paths = [".".join(parts[:i]) for i in range(1, len(parts))]
        if not paths:
            return None

        nested: dict[str, Any] | None = None
        for path in reversed(paths):
            if nested is None:
                nested = {"path": path}
            else:
                nested = {"path": path, "nested": nested}
        return nested

    def convert_field(self, field: Field) -> str:
        resolved = self.processor.resolve_field(field.path)
        path = resolved.replace("[", ".").replace("]", "").rstrip(".")
        return path

    def _convert_etag(self, etag: str) -> tuple[int, int]:
        splits = etag.split("-")
        if len(splits) != 2:
            raise BadRequestError("ETag format error")
        return int(splits[0]), int(splits[1])


class ResultConverter:
    processor: ItemProcessor
    collection: str

    def __init__(self, processor: ItemProcessor, collection: str) -> None:
        self.processor = processor
        self.collection = collection

    def convert_get(
        self,
        response: Any,
    ) -> SearchItem:
        id = response["_id"]
        source = response["_source"]
        seq_no = response["_seq_no"]
        primary_term = response["_primary_term"]
        etag = f"{seq_no}-{primary_term}"
        item = SearchItem(
            key=SearchKey(id=id),
            properties=SearchProperties(etag=etag),
            value=source,
        )
        return item

    def convert_put(
        self,
        response: Any,
    ) -> SearchItem:
        id = response["_id"]
        seq_no = response["_seq_no"]
        primary_term = response["_primary_term"]
        etag = self._convert_etag(seq_no, primary_term)
        item = SearchItem(
            key=SearchKey(id=id),
            properties=SearchProperties(etag=etag),
        )
        return item

    def convert_update(
        self,
        response: Any,
    ) -> SearchItem:
        id = response["_id"]
        seq_no = response["_seq_no"]
        primary_term = response["_primary_term"]
        etag = self._convert_etag(seq_no, primary_term)
        value = response.get("get", {}).get("_source", None)
        item = SearchItem(
            key=SearchKey(id=id),
            properties=SearchProperties(etag=etag),
            value=value,
        )
        return item

    def convert_query(
        self,
        response: Any,
    ) -> SearchList:
        hits_block = response.get("hits", {})
        hits = hits_block.get("hits", [])
        items: list[SearchItem] = []
        for hit in hits:
            id = hit.get("_id")
            source = hit.get("_source", {})
            score = hit.get("_score", None)

            seq_no = hit.get("_seq_no")
            primary_term = hit.get("_primary_term")

            etag: str | None = None
            if seq_no is not None and primary_term is not None:
                etag = self._convert_etag(seq_no, primary_term)

            item = SearchItem(
                key=SearchKey(id=id),
                properties=SearchProperties(etag=etag, score=score),
                value=source,
            )
            items.append(item)
        return SearchList(
            items=items,
        )

    def convert_count(
        self,
        response: Any,
    ) -> int:
        count = response.get("count", 0)
        return count

    def _convert_etag(self, seq_no: int, primary_term: int) -> str:
        return f"{seq_no}-{primary_term}"


class IndexHelper:
    @staticmethod
    def add_index(mappings: dict, index: Index) -> None:
        field, config = IndexHelper.convert_index_to_config(index)
        if not field:
            return
        path = re.sub(r"\[\d+\]", "", field)
        path_parts = path.split(".")
        current = mappings
        for part in path_parts[:-1]:
            is_array = "[]" in part
            part = part.replace("[]", "")
            if "properties" not in current:
                current["properties"] = {}
            if part not in current["properties"]:
                current["properties"][part] = (
                    {"type": "nested"} if is_array else {}
                )
            current = current["properties"][part]
        if "properties" not in current:
            current["properties"] = {}
        current["properties"][path_parts[-1]] = config

    @staticmethod
    def remove_index(mappings: dict, index: Index) -> None:
        field, config = IndexHelper.convert_index_to_config(index)
        if not field:
            return
        path = re.sub(r"\[\d+\]", "", field)
        path_parts = path.split(".")
        current = mappings
        stack = []
        for part in path_parts[:-1]:
            if (
                "properties" not in current
                or part not in current["properties"]
            ):
                return
            stack.append((current, part))
            current = current["properties"][part]

        field_name = path_parts[-1]
        if "properties" in current and field_name in current["properties"]:
            del current["properties"][field_name]

        while stack:
            parent, part = stack.pop()
            if "properties" in parent[part] and not parent[part]["properties"]:
                del parent[part]
            else:
                break

    @staticmethod
    def convert_index_to_config(index: Index) -> tuple[str | None, dict]:
        def convert_field_type(field_type: str | None) -> str:
            if field_type is None:
                return "text"
            field_type_map = {
                SearchFieldType.STRING.value: "keyword",
                SearchFieldType.TEXT.value: "text",
                SearchFieldType.NUMBER.value: "double",
                SearchFieldType.INTEGER.value: "integer",
                SearchFieldType.LONG.value: "long",
                SearchFieldType.FLOAT.value: "float",
                SearchFieldType.DOUBLE.value: "float",
                SearchFieldType.BOOLEAN.value: "boolean",
                SearchFieldType.DATE.value: "date",
                SearchFieldType.GEO_POINT.value: "geo_point",
                SearchFieldType.GEO_SHAPE.value: "geo_shape",
                SearchFieldType.ARRAY.value: "object",
                SearchFieldType.OBJECT.value: "object",
                SearchFieldType.VECTOR.value: "dense_vector",
                SearchFieldType.SPARSE_VECTOR.value: "sparse_vector",
            }
            return field_type_map.get(field_type, "text")

        field = None
        config: dict = {}
        if isinstance(
            index,
            (
                RangeIndex,
                HashIndex,
                FieldIndex,
                ArrayIndex,
                AscIndex,
                DescIndex,
            ),
        ):
            field = index.field
            config["type"] = convert_field_type(index.field_type)
        elif isinstance(index, TextIndex):
            field = index.field
            if index.variant == "match_only_text":
                config["type"] = "match_only_text"
            else:
                config["type"] = "text"
            if index.similarity is not None:
                similarity_map = {
                    TextSimilarityAlgorithm.BM25: "BM25",
                    TextSimilarityAlgorithm.BOOLEAN: "boolean",
                }
                config["similarity"] = similarity_map[index.similarity]
        elif isinstance(index, VectorIndex):
            field = index.field
            metric_map = {
                VectorIndexMetric.DOT_PRODUCT: "dot_product",
                VectorIndexMetric.EUCLIDEAN: "l2_norm",
                VectorIndexMetric.COSINE: "cosine",
                VectorIndexMetric.MAX_INNER_PRODUCT: "dot_product",
            }
            element_type_map = {
                SearchFieldType.FLOAT.value: "float",
                SearchFieldType.DOUBLE.value: "float",
                SearchFieldType.BYTE.value: "byte",
                # or map to "byte" if you’re quantizing
            }
            config["type"] = "dense_vector"
            config["dims"] = index.dimension
            config["similarity"] = metric_map[index.metric]
            if index.field_type is not None:
                config["element_type"] = element_type_map.get(
                    index.field_type, "float"
                )
            if index.structure is not None:
                structure_map = {
                    VectorIndexStructure.FLAT: "flat",
                    VectorIndexStructure.INT8_FLAT: "int8_flat",
                    VectorIndexStructure.INT4_FLAT: "int4_flat",
                    VectorIndexStructure.BQ_FLAT: "bbq_flat",
                    VectorIndexStructure.HNSW: "hnsw",
                    VectorIndexStructure.INT8_HNSW: "int8_hnsw",
                    VectorIndexStructure.INT4_HNSW: "int4_hnsw",
                    VectorIndexStructure.BQ_HNSW: "bq_hnsw",
                }
                index_options: dict = {"type": structure_map[index.structure]}
                if index.m is not None:
                    index_options["m"] = index.m
                if index.confidence_interval is not None:
                    index_options["confidence_interval"] = (
                        index.confidence_interval
                    )
                if index.ef_construction is not None:
                    index_options["ef_construction"] = index.ef_construction
                config["index_options"] = index_options
                config["index"] = True
        elif isinstance(index, SparseVectorIndex):
            field = index.field
            config["type"] = "sparse_vector"
        elif isinstance(index, GeospatialIndex):
            field = index.field
            field_type_map = {
                GeospatialFieldType.POINT.value: "geo_point",
                GeospatialFieldType.SHAPE.value: "geo_shape",
            }
            if index.field_type:
                config["type"] = field_type_map[index.field_type]
            else:
                config["type"] = "geo_point"
        elif isinstance(index, RankIndex):
            field = index.field
            config["type"] = "rank_feature"
        elif isinstance(index, CompositeIndex):
            pass
        elif isinstance(index, ExcludeIndex):
            field = index.field
            config = {"index": False}
        if isinstance(index.nconfig, dict):
            config.update(index.nconfig)
        return field, config


class ElasticsearchCollection:
    index: str
    processor: ItemProcessor
    op_converter: OperationConverter
    result_converter: ResultConverter

    def __init__(
        self,
        id_map_field: str | None,
        collection: str,
    ):
        self.index = collection
        self.processor = ItemProcessor(
            id_map_field=id_map_field,
            score_resolve_field="_score",
        )
        self.op_converter = OperationConverter(
            processor=self.processor, collection=collection
        )
        self.result_converter = ResultConverter(
            processor=self.processor, collection=collection
        )
