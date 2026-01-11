"""
Vector Store on Qdrant.
"""

from __future__ import annotations

__all__ = ["Qdrant"]

import copy
from typing import Any

from qdrant_client import models
from qdrant_client.http.exceptions import UnexpectedResponse

from x8.core import Context, NCall, Operation, Response
from x8.core.exceptions import BadRequestError, ConflictError, NotFoundError
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
    QueryFunctionName,
    QueryProcessor,
    Select,
)
from x8.storage._common import (
    CollectionResult,
    CollectionStatus,
    ItemProcessor,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    Validator,
    VectorIndexMetric,
)

from .._helper import get_collection_config, is_metadata_field, is_value_field
from .._models import (
    VectorCollectionConfig,
    VectorItem,
    VectorKey,
    VectorList,
    VectorProperties,
    VectorSearchArgs,
    VectorValue,
)

DEFAULT_LIMIT = 100


class Qdrant(StoreProvider):
    location: str | None
    url: str | None
    api_key: str | None
    host: str | None
    port: int | None
    path: str | None
    collection: str | None
    vector_field: str
    sparse_vector_field: str | None
    content_payload_field: str | None
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _processor: ItemProcessor

    _collection_cache: dict[str, QdrantCollection]
    _acollection_cache: dict[str, QdrantCollection]

    def __init__(
        self,
        location: str | None = None,
        url: str | None = None,
        api_key: str | None = None,
        host: str | None = None,
        port: int | None = 6333,
        path: str | None = None,
        collection: str | None = None,
        vector_field: str = "vector",
        sparse_vector_field: str | None = "sparse_vector",
        content_payload_field: str | None = "_content",
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            location:
                Qdrant location.
            url:
                Qdrant url.
            api_key:
                Qdrant api key.
            host:
                Qdrant host.
            port:
                Qdrant port.
            path:
                Qdrant path.
            collection:
                Qdrant collection mapped to vector store collection.
            vector_field:
                Vector field name. Defaults to "vector".
            sparse_vector_field:
                Sparse vector field name. Defaults to "sparse_vector".
            content_payload_field:
                Payload field where content is stored.
                Qdrant doesn't support a separate content field.
                Defaults to "_content".
            nparams:
                Native parameters to Qdrant client.
        """
        self.location = location
        self.url = url
        self.api_key = api_key
        self.host = host
        self.port = port
        self.path = path
        self.collection = collection
        self.vector_field = vector_field
        self.sparse_vector_field = sparse_vector_field
        self.content_payload_field = content_payload_field
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._processor = ItemProcessor()
        self._collection_cache = dict()
        self._acollection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        from qdrant_client import QdrantClient

        self._client = QdrantClient(
            location=self.location,
            url=self.url,
            api_key=self.api_key,
            host=self.host,
            port=self.port,
            path=self.path,
            **self.nparams,
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return

        from qdrant_client import AsyncQdrantClient

        self._aclient = AsyncQdrantClient(
            location=self.location,
            url=self.url,
            api_key=self.api_key,
            host=self.host,
            port=self.port,
            path=self.path,
            **self.nparams,
        )

    def _get_collection_name(
        self, op_parser: StoreOperationParser
    ) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            or op_parser.op_equals(StoreOperation.TRANSACT)
            else op_parser.get_collection_name()
        )
        collection = (
            collection_name or self.collection or self.__component__.collection
        )
        return collection

    def _get_collection(
        self, op_parser: StoreOperationParser
    ) -> QdrantCollection | None:
        if op_parser.is_resource_op():
            return None
        collection_name = self._get_collection_name(op_parser)
        if collection_name is None:
            raise BadRequestError("Collection name must be specified")
        if collection_name in self._collection_cache:
            return self._collection_cache[collection_name]
        description = self._client.get_collection(collection_name)
        dimension = description.config.params.vectors[self.vector_field].size
        col = QdrantCollection(
            self._client,
            collection_name,
            dimension,
            self.vector_field,
            self.sparse_vector_field,
            self.content_payload_field,
        )
        self._collection_cache[collection_name] = col
        return col

    async def _aget_collection(
        self, op_parser: StoreOperationParser
    ) -> QdrantCollection | None:
        if op_parser.is_resource_op():
            return None
        collection_name = self._get_collection_name(op_parser)
        if collection_name is None:
            raise BadRequestError("Collection name must be specified")
        if collection_name in self._acollection_cache:
            return self._acollection_cache[collection_name]
        description = await self._aclient.get_collection(collection_name)
        dimension = description.config.params.vectors[self.vector_field].size
        col = QdrantCollection(
            self._aclient,
            collection_name,
            dimension,
            self.vector_field,
            self.sparse_vector_field,
            self.content_payload_field,
        )
        self._acollection_cache[collection_name] = col
        return col

    def _validate(self, op_parser: StoreOperationParser):
        if op_parser.op_equals(StoreOperation.BATCH):
            Validator.validate_batch(
                op_parser.get_operation_parsers(),
                allowed_ops=[
                    StoreOperation.PUT,
                    StoreOperation.DELETE,
                    StoreOperation.GET,
                ],
                single_collection=True,
            )
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            Validator.validate_transact(
                op_parser.get_operation_parsers(),
                allowed_ops=[
                    StoreOperation.PUT,
                    StoreOperation.DELETE,
                    StoreOperation.GET,
                ],
                single_collection=True,
            )

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        collection = self._get_collection(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collection,
            ResourceHelper(self._client),
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(nresult, state, op_parser, collection)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        await self.__asetup__(context=context)
        op_parser = self.get_op_parser(operation)
        self._validate(op_parser)
        collection = await self._aget_collection(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collection,
            AsyncResourceHelper(self._aclient),
        )
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(nresult, state, op_parser, collection)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: QdrantCollection | None,
        resource_helper: Any,
    ) -> tuple[NCall | None, dict | None]:
        if collection is not None:
            converter = collection.converter
            client = collection.client
        call = None
        state = None
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            args: dict[Any, Any] | None = {
                "collection": self._get_collection_name(op_parser),
                "config": get_collection_config(op_parser),
                "vector_field": self.vector_field,
                "sparse_vector_field": self.sparse_vector_field,
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.create_collection, args)
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            args = {
                "collection": self._get_collection_name(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.drop_collection, args)
        # LIST COLLECTION
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_collections, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = {
                "collection": self._get_collection_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.has_collection, args)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = converter.convert_get(op_parser.get_id_as_str())
            call = NCall(client.retrieve, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args = converter.convert_put(
                op_parser.get_id_as_str(),
                op_parser.get_vector_value(),
                op_parser.get_metadata(),
            )
            call = NCall(client.upsert, args, nargs)
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args = converter.convert_update_value(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value(),
            )
            call = NCall(client.update_vectors, args, nargs)
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            args = converter.convert_update_metadata(
                id=op_parser.get_id_as_str(),
                metadata=op_parser.get_metadata(),
            )
            call = NCall(client.set_payload, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = converter.convert_delete(
                op_parser.get_id_as_str_or_none(), op_parser.get_where()
            )
            call = NCall(
                client.delete,
                args,
                nargs,
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args, func = converter.convert_query(
                search=op_parser.get_search_as_function(),
                where=op_parser.get_where(),
                select=op_parser.get_select(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
            )
            if func == "search":
                call = NCall(client.search, args, nargs)
            elif func == "search_batch":
                call = NCall(client.search_batch, args, nargs)
            state = {"func": func}
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = converter.convert_count(
                where=op_parser.get_where(),
            )
            call = NCall(client.count, args, nargs)
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            args, func = converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            if func == "get":
                call = NCall(client.retrieve, args, nargs)
            elif func == "put":
                call = NCall(client.upsert, args, nargs)
            elif func == "delete":
                call = NCall(client.delete, args, nargs)
            state = {"func": func}
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(resource_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict | None,
        op_parser: StoreOperationParser,
        collection: QdrantCollection | None,
    ) -> Any:
        if collection is not None:
            processor = collection.processor
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            result = nresult
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            result = nresult
        # LIST COLLECTION
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = [c.name for c in nresult.collections]
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            if len(nresult) == 0:
                raise NotFoundError
            result = self._convert_to_item(nresult[0], processor)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = VectorItem(
                key=VectorKey(id=op_parser.get_id_as_str()),
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = VectorItem(key=VectorKey(id=op_parser.get_id_as_str()))
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            result = VectorItem(
                key=VectorKey(id=op_parser.get_id_as_str()),
                metadata=op_parser.get_metadata(),
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            func = state["func"] if state is not None else None
            if func == "search":
                for item in nresult:
                    items.append(self._convert_to_item(item, processor))
                items = QueryProcessor.order_items(
                    items,
                    op_parser.get_order_by(),
                    processor.resolve_root_field,
                )
            elif func == "search_batch":
                limit = op_parser.get_limit()
                items = self._merge_batch(
                    nresult,
                    limit if limit is not None else DEFAULT_LIMIT,
                    processor,
                )
                items = QueryProcessor.order_items(
                    items,
                    op_parser.get_order_by(),
                    processor.resolve_root_field,
                )
            result = VectorList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = nresult.count
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            result = []
            func = state["func"] if state is not None else None
            if func == "get":
                for item in nresult:
                    result.append(self._convert_to_item(item, processor))
            elif func == "put":
                for op_parser in op_parser.get_operation_parsers():
                    result.append(
                        VectorItem(key=VectorKey(id=op_parser.get_id_as_str()))
                    )
            elif func == "delete":
                for op_parser in op_parser.get_operation_parsers():
                    result.append(None)
        return result

    def _merge_batch(
        self, batch_items: list, limit: int, processor: ItemProcessor
    ) -> list:
        unique_items: dict = dict()
        for nitems in batch_items:
            for nitem in nitems:
                item = self._convert_to_item(nitem, processor)
                if item.key.id in unique_items:
                    s1 = (
                        item.properties.score
                        if item.properties is not None
                        and item.properties.score is not None
                        else 0
                    )
                    dict_item = unique_items[item.key.id]
                    s2 = (
                        dict_item.properties.score
                        if dict_item.properties is not None
                        and dict_item.properties.score is not None
                        else 0
                    )
                    if s1 > s2:
                        unique_items[item.key.id] = item
                else:
                    unique_items[item.key.id] = item
        return sorted(
            list(unique_items.values())[0:limit],
            key=lambda x: x.properties.score,
            reverse=True,
        )

    def _convert_to_item(
        self, nitem: Any, processor: ItemProcessor
    ) -> VectorItem:
        content = None
        metadata: dict | None = None
        key = VectorKey(id=nitem.id)
        metadata = nitem.payload
        if metadata is not None:
            if (
                self.content_payload_field is not None
                and self.content_payload_field in metadata
            ):
                content = metadata[self.content_payload_field]
                metadata.pop(self.content_payload_field)
                if len(metadata) == 0:
                    metadata = None
        vector = None
        sparse_vector = None
        if nitem.vector is not None:
            if self.vector_field in nitem.vector:
                vector = nitem.vector[self.vector_field]
            if (
                self.sparse_vector_field is not None
                and self.sparse_vector_field in nitem.vector
            ):
                sparse_vector = dict()
                nsparse_vector = nitem.vector[self.sparse_vector_field]
                for i in range(0, len(nsparse_vector.indices)):
                    sparse_vector[nsparse_vector.indices[i]] = (
                        nsparse_vector.values[i]
                    )
        value = VectorValue(
            vector=vector,
            sparse_vector=sparse_vector,
            content=content,
        )
        properties = None
        if isinstance(nitem, models.ScoredPoint):
            properties = VectorProperties(score=nitem.score)
        return VectorItem(
            key=key, value=value, metadata=metadata, properties=properties
        )


class ResourceHelper:
    client: Any

    def __init__(self, client: Any):
        self.client = client

    def create_collection(
        self,
        collection: str,
        config: VectorCollectionConfig | None,
        vector_field: str,
        sparse_vector_field: str | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = OperationConverter.convert_create_collection(
            collection, config, vector_field, sparse_vector_field
        )
        try:
            NCall(self.client.create_collection, args, nargs).invoke()
        except UnexpectedResponse as e:
            if exists is False and e.status_code == 409:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        collection: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {
            "collection_name": collection,
        }
        if not self.client.collection_exists(collection):
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        NCall(self.client.delete_collection, args, nargs).invoke()
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        return NCall(self.client.get_collections, None, nargs).invoke()

    def has_collection(
        self,
        collection: str,
        nargs: Any,
    ):
        args = {
            "collection_name": collection,
        }
        return NCall(self.client.collection_exists, args, nargs).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class AsyncResourceHelper:
    client: Any

    def __init__(self, client: Any):
        self.client = client

    async def create_collection(
        self,
        collection: str,
        config: VectorCollectionConfig | None,
        vector_field: str,
        sparse_vector_field: str | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = OperationConverter.convert_create_collection(
            collection, config, vector_field, sparse_vector_field
        )
        try:
            await NCall(self.client.create_collection, args, nargs).ainvoke()
        except UnexpectedResponse as e:
            if exists is False and e.status_code == 409:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    async def drop_collection(
        self,
        collection: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {
            "collection_name": collection,
        }
        if not await self.client.collection_exists(collection):
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        await NCall(self.client.delete_collection, args, nargs).ainvoke()
        return CollectionResult(status=CollectionStatus.DROPPED)

    async def list_collections(self, nargs) -> Any:
        return await NCall(self.client.get_collections, None, nargs).ainvoke()

    async def has_collection(
        self,
        collection: str,
        nargs: Any,
    ):
        args = {
            "collection_name": collection,
        }
        return await NCall(
            self.client.collection_exists, args, nargs
        ).ainvoke()

    async def close(self, nargs: Any) -> Any:
        pass


class OperationConverter:
    processor: ItemProcessor
    collection: str | None
    dimension: int
    vector_field: str
    sparse_vector_field: str | None
    content_payload_field: str | None

    def __init__(
        self,
        processor: ItemProcessor,
        collection: str | None,
        dimension: int,
        vector_field: str,
        sparse_vector_field: str | None,
        content_payload_field: str | None,
    ) -> None:
        self.processor = processor
        self.collection = collection
        self.dimension = dimension
        self.vector_field = vector_field
        self.sparse_vector_field = sparse_vector_field
        self.content_payload_field = content_payload_field

    @staticmethod
    def convert_create_collection(
        collection: str,
        config: VectorCollectionConfig | None,
        vector_field: str,
        sparse_vector_field: str | None,
    ) -> dict:
        args: dict = {"collection_name": collection}
        metric_mapping = {
            VectorIndexMetric.DOT_PRODUCT: models.Distance.DOT,
            VectorIndexMetric.COSINE: models.Distance.COSINE,
            VectorIndexMetric.EUCLIDEAN: models.Distance.EUCLID,
            VectorIndexMetric.MANHATTAN: models.Distance.MANHATTAN,
        }
        vector_index = config.vector_index if config is not None else None
        if vector_index is not None:
            dimension = vector_index.dimension
            metric = metric_mapping[vector_index.metric]
            nconfig = (
                vector_index.nconfig
                if vector_index.nconfig is not None
                else {}
            )
        else:
            dimension = 4
            metric = models.Distance.DOT
            nconfig = {}

        args["vectors_config"] = {
            vector_field: models.VectorParams(
                size=dimension, distance=metric, **nconfig
            )
        }

        if sparse_vector_field is not None:
            args["sparse_vectors_config"] = {
                sparse_vector_field: models.SparseVectorParams()
            }

        return args

    def convert_batch(
        self, op_parsers: list[StoreOperationParser]
    ) -> tuple[dict, str]:
        batch_get = False
        batch_put = False
        batch_delete = False
        for op_parser in op_parsers:
            if op_parser.op_equals(StoreOperation.GET):
                batch_get = True
            elif op_parser.op_equals(StoreOperation.PUT):
                batch_put = True
            elif op_parser.op_equals(StoreOperation.DELETE):
                batch_delete = True
        if (
            (batch_get and batch_put)
            or (batch_put and batch_delete)
            or (batch_delete and batch_get)
        ):
            raise BadRequestError(
                "Batch operation supports only one operation type"
            )
        if batch_get:
            return self.convert_batch_get(op_parsers), "get"
        elif batch_put:
            return self.convert_batch_put(op_parsers), "put"
        elif batch_delete:
            return self.convert_batch_delete(op_parsers), "delete"
        raise BadRequestError("Batch operation not supported")

    def convert_batch_get(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {
            "collection_name": self.collection,
            "ids": ids,
            "with_vectors": True,
        }

    def convert_batch_put(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        vectors = []
        for op_parser in op_parsers:
            vectors.append(
                self.convert_put_vector(
                    op_parser.get_id_as_str(),
                    op_parser.get_vector_value(),
                    op_parser.get_metadata(),
                )
            )
        return {
            "collection_name": self.collection,
            "points": vectors,
        }

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids: list = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {
            "collection_name": self.collection,
            "points_selector": models.PointIdsList(
                points=ids,
            ),
        }

    def convert_get(self, id: str) -> dict:
        return {
            "collection_name": self.collection,
            "ids": [id],
            "with_vectors": True,
        }

    def convert_put(self, id: str, value: dict, metadata: dict | None) -> dict:
        return {
            "collection_name": self.collection,
            "points": [self.convert_put_vector(id, value, metadata)],
        }

    def convert_put_vector(
        self, id: str, value: dict, metadata: dict | None
    ) -> Any:
        vector_value = VectorValue(**value)
        args: dict = {}
        args[self.vector_field] = vector_value.vector
        if (
            self.sparse_vector_field is not None
            and vector_value.sparse_vector is not None
        ):
            args[self.sparse_vector_field] = self.convert_sparse_vector(
                vector_value.sparse_vector
            )
        payload: dict | None = None
        if metadata is None:
            if (
                vector_value.content is not None
                and self.content_payload_field is not None
            ):
                payload = {self.content_payload_field: vector_value.content}
        else:
            payload = copy.deepcopy(metadata)
            if (
                vector_value.content is not None
                and self.content_payload_field is not None
            ):
                payload[self.content_payload_field] = vector_value.content
        return models.PointStruct(id=id, vector=args, payload=payload)

    def convert_update_value(self, id: str, value: dict) -> dict:
        if self.sparse_vector_field is not None:
            raise BadRequestError("Sparse vector update is not supported")
        if self.content_payload_field is not None:
            raise BadRequestError("Content update is not supported")
        args: dict = {"collection_name": self.collection}
        vector_value = VectorValue(**value)
        vector: dict = {self.vector_field: vector_value.vector}
        args["points"] = [models.PointVectors(id=id, vector=vector)]
        return args

    def convert_update_metadata(self, id: str, metadata: dict | None) -> dict:
        return {
            "collection_name": self.collection,
            "payload": metadata,
            "points": [id],
        }

    def convert_delete(self, id: str | None, where: Expression) -> dict:
        if id is None and where is None:
            raise BadRequestError("Id or where should be provided for delete")
        if id is not None and where is None:
            return {
                "collection_name": self.collection,
                "points_selector": models.PointIdsList(
                    points=[id],
                ),
            }
        if where is not None and id is None:
            return {
                "collection_name": self.collection,
                "points_selector": models.FilterSelector(
                    filter=models.Filter(must=[self.convert_expr(where)])
                ),
            }
        raise BadRequestError(
            "Both id and where cannot be provided for delete"
        )

    def convert_query(
        self,
        search: Function | None,
        where: Expression,
        select: Select | None,
        order_by: OrderBy | None,
        limit: int | None,
        offset: int | None,
    ) -> tuple[dict, str]:
        search_args = self.convert_search(search)
        filter = None
        if where is not None:
            filter = models.Filter(must=[self.convert_expr(where)])
        limit_arg = {"limit": DEFAULT_LIMIT if limit is None else limit}
        if isinstance(search_args, list):
            return {
                "collection_name": self.collection,
                "requests": [
                    models.SearchRequest(
                        **(
                            self.convert_select(select, True)
                            | search_args[0]
                            | limit_arg
                            | {
                                "filter": filter,
                                "offset": offset,
                            }
                        )
                    ),
                    models.SearchRequest(
                        **(
                            self.convert_select(select, True)
                            | search_args[1]
                            | limit_arg
                            | {
                                "filter": filter,
                                "offset": offset,
                            }
                        )
                    ),
                ],
            }, "search_batch"
        else:
            return (
                search_args
                | self.convert_select(select, False)
                | limit_arg
                | {
                    "collection_name": self.collection,
                    "query_filter": filter,
                    "offset": offset,
                }
            ), "search"

    def convert_select(self, select: Select | None, is_batch: bool) -> dict:
        include_value = False
        include_metadata = False
        if select is None or len(select.terms) == 0:
            include_value = True
            include_metadata = True
        else:
            for term in select.terms:
                if is_value_field(term.field):
                    # include metadata since content is stored there
                    # filter it out when returning
                    # optimize later
                    include_value = True
                    include_metadata = True
                elif is_metadata_field(term.field):
                    include_metadata = True
        if is_batch:
            return {
                "with_vector": include_value,
                "with_payload": include_metadata,
            }
        else:
            return {
                "with_vectors": include_value,
                "with_payload": include_metadata,
            }

    def convert_search(self, search: Function | None) -> Any:
        if search is None:
            return {
                "query_vector": models.NamedVector(
                    name=self.vector_field, vector=[0.0] * self.dimension
                )
            }
        namespace = search.namespace
        name = search.name
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.VECTOR_SEARCH:
                args = VectorSearchArgs(**search.named_args)
                if args.vector is not None and args.sparse_vector is None:
                    return self.convert_vector_search(args.vector, False)
                elif args.vector is None and args.sparse_vector is not None:
                    return self.convert_sparse_vector_search(
                        args.sparse_vector, False
                    )
                elif (
                    args.vector is not None and args.sparse_vector is not None
                ):
                    return [
                        self.convert_vector_search(args.vector, True),
                        self.convert_sparse_vector_search(
                            args.sparse_vector, True
                        ),
                    ]

        raise BadRequestError("Search function not supported")

    def convert_vector_search(
        self, vector: list[float], is_batch: bool
    ) -> Any:
        key = "vector" if is_batch else "query_vector"
        return {key: models.NamedVector(name=self.vector_field, vector=vector)}

    def convert_sparse_vector_search(
        self, sparse_vector: dict, is_batch: bool
    ) -> Any:
        key = "vector" if is_batch else "query_vector"
        if self.sparse_vector_field is not None:
            return {
                key: models.NamedSparseVector(
                    name=self.sparse_vector_field,
                    vector=self.convert_sparse_vector(sparse_vector),
                )
            }
        raise BadRequestError("Sparse vector search not supported")

    def convert_sparse_vector(
        self, sparse_vector: dict[int, float] | None
    ) -> Any:
        if sparse_vector is None:
            return None
        indices = []
        values = []
        for k, v in sparse_vector.items():
            indices.append(k)
            values.append(v)
        return models.SparseVector(indices=indices, values=values)

    def convert_expr(self, expr: Expression) -> Any:
        if expr is None or isinstance(
            expr, (str, int, float, bool, dict, list)
        ):
            return expr
        if isinstance(expr, Field):
            field = self.processor.resolve_metadata_field(expr.path)
            return field
        if isinstance(expr, Function):
            return self.convert_func(expr)
        if isinstance(expr, Comparison):
            op = expr.op
            if isinstance(expr.lexpr, Field):
                field = self.convert_expr(expr.lexpr)
                value = self.convert_expr(expr.rexpr)
            elif isinstance(expr.rexpr, Field):
                field = self.convert_expr(expr.rexpr)
                value = self.convert_expr(expr.lexpr)
                op = Comparison.reverse_op(op)
            if op == ComparisonOp.EQ:
                return models.FieldCondition(
                    key=field, match=models.MatchValue(value=value)
                )
            elif op == ComparisonOp.NEQ:
                return models.Filter(
                    must_not=[
                        models.FieldCondition(
                            key=field, match=models.MatchValue(value=value)
                        )
                    ]
                )
            elif op == ComparisonOp.GT:
                return models.FieldCondition(
                    key=field, range=models.Range(gt=value)
                )
            elif op == ComparisonOp.GTE:
                return models.FieldCondition(
                    key=field, range=models.Range(gte=value)
                )
            elif op == ComparisonOp.LT:
                return models.FieldCondition(
                    key=field, range=models.Range(lt=value)
                )
            elif op == ComparisonOp.LTE:
                return models.FieldCondition(
                    key=field, range=models.Range(lte=value)
                )
            elif op == ComparisonOp.BETWEEN:
                return models.Filter(
                    must=[
                        models.FieldCondition(
                            key=field, range=models.Range(gte=value[0])
                        ),
                        models.FieldCondition(
                            key=field, range=models.Range(lte=value[1])
                        ),
                    ]
                )
            elif op == ComparisonOp.IN:
                return models.FieldCondition(
                    key=field, match=models.MatchAny(any=value)
                )
            elif op == ComparisonOp.NIN:
                return models.FieldCondition(
                    key=field, match=models.MatchExcept(**{"except": value})
                )
            else:
                raise BadRequestError(f"Comparison op {expr.op} not supported")
        if isinstance(expr, And):
            return models.Filter(
                must=[
                    self.convert_expr(expr.lexpr),
                    self.convert_expr(expr.rexpr),
                ]
            )
        if isinstance(expr, Or):
            return models.Filter(
                should=[
                    self.convert_expr(expr.lexpr),
                    self.convert_expr(expr.rexpr),
                ]
            )
        if isinstance(expr, Not):
            return models.Filter(must_not=[self.convert_expr(expr.expr)])
        return str(expr)

    def convert_func(self, expr: Function) -> Any:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(args[0])
                return models.Filter(
                    must_not=[
                        models.IsEmptyCondition(
                            is_empty=models.PayloadField(key=field)
                        )
                    ]
                )
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(args[0])
                return models.IsEmptyCondition(
                    is_empty=models.PayloadField(key=field)
                )
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(args[0])
                return models.FieldCondition(
                    key=field, match=models.MatchValue(value=args[1])
                )
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(args[0])
                return models.FieldCondition(
                    key=field, match=models.MatchAny(any=args[1])
                )
        raise BadRequestError(f"Function {name} not supported")

    def convert_count(self, where: Expression) -> dict | None:
        args: dict = {"collection_name": self.collection}
        if where is not None:
            args["count_filter"] = models.Filter(
                must=[self.convert_expr(where)]
            )
        return args


class QdrantCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor
    vector_field: str
    sparse_vector_field: str | None

    def __init__(
        self,
        client: Any,
        collection: str | None,
        dimension: int,
        vector_field: str,
        sparse_vector_field: str | None,
        content_payload_field: str | None,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.converter = OperationConverter(
            processor=self.processor,
            collection=collection,
            dimension=dimension,
            vector_field=vector_field,
            sparse_vector_field=sparse_vector_field,
            content_payload_field=content_payload_field,
        )
