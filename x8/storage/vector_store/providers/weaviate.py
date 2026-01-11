"""
Vector Store on Weavite.
"""

from __future__ import annotations

__all__ = ["Weaviate"]

import copy
from typing import Any

import weaviate
import weaviate.classes as wvc
from weaviate.classes.query import Filter

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
    ParameterParser,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    Validator,
    VectorIndexMetric,
    VectorIndexStructure,
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


class Weaviate(StoreProvider):
    type: str
    collection: str | None
    dimension: int | dict[str, int]
    content_properties_field: str | None
    url: str | None
    api_key: str | None
    http_host: str
    http_port: int
    http_secure: bool
    grpc_host: str
    grpc_port: int
    grpc_secure: bool
    nparams: dict[str, Any]

    _database_client: weaviate.WeaviateClient
    _init: bool

    _processor: ItemProcessor

    _collection_cache: dict[str, WeaviateCollection]

    def __init__(
        self,
        type: str = "wcs",
        collection: str | None = None,
        dimension: int | dict[str, int] = 4,
        content_properties_field: str | None = "_content",
        url: str | None = None,
        api_key: str | None = None,
        http_host: str = "localhost",
        http_port: int = 8080,
        http_secure: bool = False,
        grpc_host: str = "localhost",
        grpc_port: int = 50051,
        grpc_secure: bool = False,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            type:
                Weaviate connection type.
                Values are "wcs", "local", "embedded", "custom".
                Defaults to "wcs".
            collection:
                Collection name mapped to vector store collection.
            dimension:
                Vector dimension.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the dimension.
            content_properties_field:
                Properties field where content is stored.
                Weaviate doesn't support a separate content field.
                Defaults to "_content".
            url:
                Weaviate url.
            api_key:
                Weaviate api key.
            http_host:
                Http host.
            http_port:
                Http port.
            http_secure:
                Secure http option.
            grpc_host:
                Grpc host.
            grpc_port:
                Grpc port.
            grpc_secure:
                Secure grpc option.
            nparams:
                Native parameters for weaviate client.
        """
        self.type = type
        self.collection = collection
        self.dimension = dimension
        self.content_properties_field = content_properties_field
        self.url = url
        self.api_key = api_key
        self.http_host = http_host
        self.http_port = http_port
        self.http_secure = http_secure
        self.grpc_host = grpc_host
        self.grpc_port = grpc_port
        self.grpc_secure = grpc_secure
        self.nparams = nparams

        self._init = False
        self._processor = ItemProcessor()
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        if (
            self.type == "wcs"
            and self.url is not None
            and self.api_key is not None
        ):
            self._database_client = weaviate.connect_to_wcs(
                cluster_url=self.url,
                auth_credentials=weaviate.auth.AuthApiKey(self.api_key),
                **self.nparams,
            )
        elif self.type == "local":
            self._database_client = weaviate.connect_to_local(
                **self.nparams,
            )
        elif self.type == "embedded":
            self._database_client = weaviate.connect_to_embedded(
                **self.nparams,
            )
        elif self.type == "custom":
            self._database_client = weaviate.connect_to_custom(
                http_host=self.http_host,
                http_port=self.http_port,
                http_secure=self.http_secure,
                grpc_host=self.grpc_host,
                grpc_port=self.grpc_port,
                grpc_secure=self.grpc_secure,
                **self.nparams,
            )
        else:
            raise BadRequestError("Cannot initalize client")
        self._init = True

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
    ) -> WeaviateCollection | None:
        if op_parser.is_resource_op():
            return None
        collection_name = self._get_collection_name(op_parser)
        if collection_name is None:
            raise BadRequestError("Collection name must be specified")
        if collection_name in self._collection_cache:
            return self._collection_cache[collection_name]
        client = self._database_client.collections.get(collection_name)
        dimension = ParameterParser.get_collection_parameter(
            self.dimension, collection_name
        )
        col = WeaviateCollection(
            client,
            collection_name,
            dimension,
            self.content_properties_field,
        )
        self._collection_cache[collection_name] = col
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
            ResourceHelper(self._database_client),
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

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: WeaviateCollection | None,
        resource_helper: Any,
    ) -> tuple[NCall | None, dict | None]:
        if collection is not None:
            converter = collection.converter
            client = collection.client
            helper = collection.helper
        call = None
        state = None
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            args: dict[Any, Any] | None = {
                "collection": self._get_collection_name(op_parser),
                "config": get_collection_config(op_parser),
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
            call = NCall(client.query.fetch_object_by_id, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args = {
                "id": op_parser.get_id_as_str(),
                "value": op_parser.get_vector_value(),
                "metadata": op_parser.get_metadata(),
                "nargs": nargs,
            }
            call = NCall(
                helper.put,
                args,
                None,
            )
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args = converter.convert_update_value(
                id=op_parser.get_id_as_str(),
                value=op_parser.get_value(),
            )
            call = NCall(client.data.update, args, nargs)
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            args = converter.convert_update_metadata(
                id=op_parser.get_id_as_str(),
                metadata=op_parser.get_metadata(),
            )
            call = NCall(client.data.update, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args, func = converter.convert_delete(
                id=op_parser.get_id_as_str_or_none(),
                where=op_parser.get_where(),
            )
            if func == "delete_by_id":
                call = NCall(client.data.delete_by_id, args, nargs)
            elif func == "delete_many":
                call = NCall(client.data.delete_many, args, nargs)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = converter.convert_query(
                search=op_parser.get_search_as_function(),
                where=op_parser.get_where(),
                select=op_parser.get_select(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
            )
            call = NCall(client.query.near_vector, args, nargs)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = converter.convert_count(
                where=op_parser.get_where(),
            )
            call = NCall(client.aggregate.over_all, args, nargs)
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            args, func = converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            if func == "get":
                call = NCall(client.query.fetch_objects, args, nargs)
            elif func == "put":
                call = NCall(client.data.insert_many, args, nargs)
            elif func == "delete":
                call = NCall(client.data.delete_many, args, nargs)
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
        collection: WeaviateCollection | None,
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
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = [col.lower() for col in list(nresult.keys())]
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            if nresult is None:
                raise NotFoundError
            result = self._convert_to_item(nresult, processor)
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
            for item in nresult.objects:
                items.append(self._convert_to_item(item, processor))
            items = QueryProcessor.order_items(
                items, op_parser.get_order_by(), processor.resolve_root_field
            )
            result = VectorList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = nresult.total_count
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            result = []
            func = state["func"] if state is not None else None
            if func == "get":
                for item in nresult.objects:
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

    def _convert_to_item(
        self, nitem: Any, processor: ItemProcessor
    ) -> VectorItem:
        content = None
        metadata: dict | None = None
        vector = None
        key = VectorKey(id=str(nitem.uuid))
        if nitem.properties is not None:
            metadata = nitem.properties
            if (
                metadata is not None
                and self.content_properties_field is not None
                and self.content_properties_field in metadata
            ):
                content = metadata[self.content_properties_field]
                metadata.pop(self.content_properties_field)
                if len(metadata) == 0:
                    metadata = None
        if nitem.vector is not None and len(nitem.vector) > 0:
            vector = nitem.vector["default"]
        value = VectorValue(
            vector=vector,
            sparse_vector=None,
            content=content,
        )
        properties = None
        if (
            nitem.metadata is not None
            and hasattr(nitem.metadata, "distance")
            and nitem.metadata.distance is not None
        ):
            properties = VectorProperties(score=nitem.metadata.distance * -1)
        return VectorItem(
            key=key, value=value, metadata=metadata, properties=properties
        )


class ResourceHelper:
    database_client: weaviate.WeaviateClient

    def __init__(self, database_client: weaviate.WeaviateClient):
        self.database_client = database_client

    def create_collection(
        self,
        collection: str,
        config: VectorCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        if self.database_client.collections.exists(collection):
            if exists is False:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)

        metric_mapping = {
            VectorIndexMetric.DOT_PRODUCT: wvc.config.VectorDistances.DOT,
            VectorIndexMetric.COSINE: wvc.config.VectorDistances.COSINE,
            VectorIndexMetric.EUCLIDEAN: wvc.config.VectorDistances.L2_SQUARED,
            VectorIndexMetric.MANHATTAN: wvc.config.VectorDistances.MANHATTAN,
            VectorIndexMetric.HAMMING: wvc.config.VectorDistances.HAMMING,
        }
        vector_index = config.vector_index if config is not None else None
        if vector_index is not None:
            metric = metric_mapping[vector_index.metric]
            structure = vector_index.structure
            nconfig = (
                vector_index.nconfig
                if vector_index.nconfig is not None
                else {}
            )
        else:
            metric = wvc.config.VectorDistances.DOT
            structure = None
            nconfig = {}
        if structure is None or structure == VectorIndexStructure.HNSW:
            vector_index_config: Any = wvc.config.Configure.VectorIndex.hnsw(
                distance_metric=metric, **nconfig
            )
        elif structure == VectorIndexStructure.FLAT:
            vector_index_config = wvc.config.Configure.VectorIndex.flat(
                distance_metric=metric, **nconfig
            )
        args = {
            "name": collection,
            "vectorizer_config": wvc.config.Configure.Vectorizer.none(),
            "vector_index_config": vector_index_config,
        }
        try:
            NCall(
                self.database_client.collections.create,
                args,
                nargs,
                {weaviate.exceptions.UnexpectedStatusCodeError: None},
            ).invoke()
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if e.status_code == 422:
                return CollectionResult(status=CollectionStatus.EXISTS)
            raise e
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        collection: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        if not self.database_client.collections.exists(collection):
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)

        args = {"name": collection}
        NCall(self.database_client.collections.delete, args, nargs).invoke()
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        return NCall(
            self.database_client.collections.list_all, None, nargs
        ).invoke()

    def has_collection(
        self,
        collection: str,
        nargs: Any,
    ):
        args = {"name": collection}
        return NCall(
            self.database_client.collections.exists, args, nargs
        ).invoke()

    def close(self, nargs: Any) -> Any:
        return NCall(self.database_client.close, None, nargs).invoke()


class ClientHelper:
    client: Any
    converter: OperationConverter

    def __init__(self, client: Any, converter: OperationConverter) -> None:
        self.client = client
        self.converter = converter

    def put(self, id: str, value: dict, metadata: dict, nargs: Any) -> Any:
        args = self.converter.convert_put(id, value, metadata)
        try:
            return NCall(self.client.data.insert, args, nargs).invoke()
        except weaviate.exceptions.UnexpectedStatusCodeError as e:
            if e.status_code == 422:
                return NCall(self.client.data.replace, args, nargs).invoke()
            raise e

    def delete(self, id: str, where: Expression, nargs: Any) -> Any:
        if id is None and where is None:
            raise BadRequestError("Id or where should be provided for delete")
        if id is not None and where is None:
            args = {"uuid": id}
            return NCall(self.client.data.delete_by_id, args, nargs).invoke()
        if where is not None and id is None:
            args = {"where": self.converter.convert_expr(where)}
            return NCall(self.client.data.delete_many, args, nargs).invoke()
        raise BadRequestError(
            "Both id and where cannot be provided for delete"
        )


class OperationConverter:
    processor: ItemProcessor
    collection: str | None
    dimension: int
    content_properties_field: str | None

    def __init__(
        self,
        processor: ItemProcessor,
        collection: str | None,
        dimension: int,
        content_properties_field: str | None,
    ) -> None:
        self.processor = processor
        self.collection = collection
        self.dimension = dimension
        self.content_properties_field = content_properties_field

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
        ids: list = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {
            "filters": Filter.by_id().contains_any(ids),
            "include_vector": True,
        }

    def convert_batch_put(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        objs = list()
        for op_parser in op_parsers:
            objs.append(
                wvc.data.DataObject(
                    **self.convert_put(
                        op_parser.get_id_as_str(),
                        op_parser.get_vector_value(),
                        op_parser.get_metadata(),
                    )
                )
            )
        return {"objects": objs}

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids: list = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {"where": Filter.by_id().contains_any(ids)}

    def convert_get(self, id: str) -> dict:
        return {"uuid": id, "include_vector": True}

    def convert_put(self, id: str, value: dict, metadata: dict | None) -> dict:
        args: dict = {"uuid": id}
        content = None
        md = None
        vector_value = VectorValue(**value)
        if vector_value.vector is not None:
            args["vector"] = vector_value.vector

        content = vector_value.content
        if metadata is None:
            if (
                content is not None
                and self.content_properties_field is not None
            ):
                md = {self.content_properties_field: content}
        else:
            md = copy.deepcopy(metadata)
            if (
                content is not None
                and self.content_properties_field is not None
            ):
                md[self.content_properties_field] = content
        if md is not None:
            args["properties"] = md
        return args

    def convert_update_value(self, id: str, value: dict) -> dict:
        args: dict = {"uuid": id}
        vector_value = VectorValue(**value)
        if vector_value.vector is not None:
            args["vector"] = vector_value.vector
        if (
            vector_value.content is not None
            and self.content_properties_field is not None
        ):
            args["properties"] = {
                self.content_properties_field: vector_value.content
            }
        return args

    def convert_update_metadata(self, id: str, metadata: dict | None) -> dict:
        return {
            "uuid": id,
            "properties": metadata,
        }

    def convert_delete(self, id: str | None, where: Expression) -> Any:
        if id is None and where is None:
            raise BadRequestError("Id or where should be provided for delete")
        if id is not None and where is None:
            return {"uuid": id}, "delete_by_id"
        if where is not None and id is None:
            return {"where": self.convert_expr(where)}, "delete_many"
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
    ) -> dict:
        args: dict = dict()
        search_args = self.convert_search(search)
        args = args | search_args
        select_args = self.convert_select(select)
        if select_args is not None:
            args = args | select_args
        if where is not None:
            args["filters"] = self.convert_expr(where)
        args["limit"] = limit
        args["offset"] = offset
        return args

    def convert_select(self, select: Select | None) -> dict:
        include_value = False
        if select is None or len(select.terms) == 0:
            include_value = True
        else:
            for term in select.terms:
                if is_value_field(term.field):
                    include_value = True
                elif is_metadata_field(term.field):
                    pass
        return {
            "include_vector": include_value,
            "return_metadata": wvc.query.MetadataQuery(
                distance=True, score=True
            ),
        }

    def convert_search(self, search: Function | None) -> dict:
        if search is None:
            return {"near_vector": [0.0] * self.dimension}
        namespace = search.namespace
        name = search.name
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.VECTOR_SEARCH:
                args = VectorSearchArgs(**search.named_args)
                return {
                    "near_vector": args.vector,
                }

        raise BadRequestError("Search function not supported")

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
                return Filter.by_property(field).equal(value)
            elif op == ComparisonOp.NEQ:
                return Filter.by_property(field).not_equal(value)
            elif op == ComparisonOp.GT:
                return Filter.by_property(field).greater_than(value)
            elif op == ComparisonOp.GTE:
                return Filter.by_property(field).greater_or_equal(value)
            elif op == ComparisonOp.LT:
                return Filter.by_property(field).less_than(value)
            elif op == ComparisonOp.LTE:
                return Filter.by_property(field).less_or_equal(value)
            elif op == ComparisonOp.BETWEEN:
                return Filter.by_property(field).greater_or_equal(
                    value[0]
                ) & Filter.by_property(field).less_or_equal(value[1])
            elif op == ComparisonOp.IN:
                x = Filter.by_property(field).equal(value[0])
                for i in range(0, len(value)):
                    x = x | Filter.by_property(field).equal(value[i])
                return x
            elif op == ComparisonOp.NIN:
                x = Filter.by_property(field).not_equal(value[0])
                for i in range(0, len(value)):
                    x = x & Filter.by_property(field).not_equal(value[i])
                return x
            elif op == ComparisonOp.LIKE:
                return Filter.by_property(field).like(value)
            else:
                raise BadRequestError(f"Comparison op {expr.op} not supported")
        if isinstance(expr, And):
            return self.convert_expr(expr.lexpr) & self.convert_expr(
                expr.rexpr
            )
        if isinstance(expr, Or):
            return self.convert_expr(expr.lexpr) | self.convert_expr(
                expr.rexpr
            )
        if isinstance(expr, Not):
            raise BadRequestError("Not expression is not supported")
        return str(expr)

    def convert_func(self, expr: Function) -> Any:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(args[0])
                return Filter.by_property(field).is_none(False)
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(args[0])
                return Filter.by_property(field).is_none(True)
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(args[0])
                return Filter.by_property(field).contains_all([args[1]])
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(args[0])
                return Filter.by_property(field).contains_any(args[1])
        raise BadRequestError(f"Function {name} not supported")

    def convert_count(self, where: Expression) -> dict | None:
        if where is None:
            return None
        args = {"filters": self.convert_expr(where)}
        return args


class WeaviateCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor
    helper: ClientHelper

    def __init__(
        self,
        client: Any,
        collection: str | None,
        dimension: int,
        content_properties_field: str | None,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.converter = OperationConverter(
            processor=self.processor,
            collection=collection,
            dimension=dimension,
            content_properties_field=content_properties_field,
        )
        self.helper = ClientHelper(self.client, self.converter)
