"""
Vector Store on Pinecone.
"""

from __future__ import annotations

__all__ = ["Pinecone"]

import copy
from typing import Any

from pinecone import NotFoundException
from pinecone import Pinecone as PineconeClient
from pinecone import PineconeApiException, ServerlessSpec

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


class Pinecone(StoreProvider):
    api_key: str
    host: str | None
    index: str | None
    namespace: str | None
    content_metadata_field: str | None
    nparams: dict[str, Any]

    _database_client: PineconeClient
    _processor: ItemProcessor

    _collection_cache: dict[str, PineconeCollection]

    def __init__(
        self,
        api_key: str,
        host: str | None = None,
        index: str | None = None,
        namespace: str | None = None,
        content_metadata_field: str | None = "_content",
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                Pinecone API key.
            host:
                Pinecone host.
            index:
                Pinecone index mapped to vector store collection.
            namespace:
                Index namespace.
            content_metadata_field:
                Metadata field where content is stored.
                Pinecone doesn't support a separate content field.
                Defaults to "_content".
            nparams:
                Native parameters to pinecone client.
        """
        self.api_key = api_key
        self.host = host
        self.index = index
        self.namespace = namespace
        self.content_metadata_field = content_metadata_field
        self.nparams = nparams

        self._database_client = None
        self._processor = ItemProcessor()
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._database_client is not None:
            return

        self._database_client = PineconeClient(
            api_key=self.api_key,
            host=self.host,
            **self.nparams,
        )

    def _get_index_name(self, op_parser: StoreOperationParser) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            or op_parser.op_equals(StoreOperation.TRANSACT)
            else op_parser.get_collection_name()
        )
        index = collection_name or self.index or self.__component__.collection
        return index

    def _get_collection(
        self, op_parser: StoreOperationParser
    ) -> PineconeCollection | None:
        if op_parser.is_resource_op():
            return None
        index_name = self._get_index_name(op_parser)
        if index_name is None:
            raise BadRequestError("Collection name must be specified")
        if index_name in self._collection_cache:
            return self._collection_cache[index_name]
        client = self._database_client.Index(index_name)
        description = self._database_client.describe_index(index_name)
        col = PineconeCollection(
            client,
            self.namespace,
            description.dimension,
            self.content_metadata_field,
        )
        self._collection_cache[index_name] = col
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
        collection: PineconeCollection | None,
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
                "index": self._get_index_name(op_parser),
                "config": get_collection_config(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.create_collection, args)
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            args = {
                "index": self._get_index_name(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.drop_collection, args)
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_collections, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = {
                "index": self._get_index_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.has_collection, args)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = converter.convert_get(op_parser.get_id_as_str())
            call = NCall(client.fetch, args, nargs)
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
            call = NCall(client.update, args, nargs)
        # UPDATE metadata
        elif op_parser.op_equals(StoreOperation.UPDATE_METADATA):
            args = converter.convert_update_metadata(
                id=op_parser.get_id_as_str(),
                metadata=op_parser.get_metadata(),
            )
            call = NCall(client.update, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = converter.convert_delete(
                op_parser.get_id_as_str_or_none(), op_parser.get_where()
            )
            call = NCall(
                client.delete, args, nargs, {NotFoundException: NotFoundError}
            )
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
            call = NCall(client.query, args, nargs)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = converter.convert_count(
                where=op_parser.get_where(),
            )
            call = NCall(client.describe_index_stats, args, nargs)
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            args, func = converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            if func == "get":
                call = NCall(client.fetch, args, nargs)
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
        collection: PineconeCollection | None,
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
            result = nresult.names()
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            if len(nresult["vectors"]) == 0:
                raise NotFoundError
            for item in nresult["vectors"].values():
                result = self._convert_to_item(item, processor)
                break
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
            for item in nresult["matches"]:
                items.append(self._convert_to_item(item, processor))
            items = QueryProcessor.order_items(
                items, op_parser.get_order_by(), processor.resolve_root_field
            )
            limit = op_parser.get_limit()
            items = QueryProcessor.limit_items(
                items,
                limit if limit is not None else DEFAULT_LIMIT,
                op_parser.get_offset(),
            )
            result = VectorList(items=items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            ns = self.namespace if self.namespace is not None else ""
            result = nresult["namespaces"][ns]["vector_count"]
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            result = []
            func = state["func"] if state is not None else None
            if func == "get":
                for item in nresult["vectors"].values():
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
        key = VectorKey(id=nitem["id"])
        if "metadata" in nitem:
            metadata = nitem["metadata"]
            if (
                metadata is not None
                and self.content_metadata_field is not None
                and self.content_metadata_field in metadata
            ):
                content = metadata[self.content_metadata_field]
                metadata.pop(self.content_metadata_field)
                if len(metadata) == 0:
                    metadata = None
        if "sparse_values" in nitem:
            sparse_values = nitem["sparse_values"]
            sparse_vector = dict()
            for i in range(0, len(sparse_values["indices"])):
                sparse_vector[sparse_values["indices"][i]] = sparse_values[
                    "values"
                ][i]
        else:
            sparse_vector = None
        value = VectorValue(
            vector=nitem["values"],
            sparse_vector=sparse_vector,
            content=content,
        )
        properties = None
        if "score" in nitem:
            properties = VectorProperties(score=nitem["score"])
        return VectorItem(
            key=key,
            value=value,
            metadata=metadata,
            properties=properties,
        )


class ResourceHelper:
    database_client: PineconeClient

    def __init__(self, database_client: PineconeClient):
        self.database_client = database_client

    def create_collection(
        self,
        index: str,
        config: VectorCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        metrics_map = {
            VectorIndexMetric.DOT_PRODUCT: "dotproduct",
            VectorIndexMetric.COSINE: "cosine",
            VectorIndexMetric.EUCLIDEAN: "euclidean",
        }
        vector_index = config.vector_index if config is not None else None
        if vector_index is not None:
            dimension = vector_index.dimension
            metric = metrics_map[vector_index.metric]
        else:
            dimension = 4
            metric = "dotproduct"
        if (
            config is not None
            and config.nconfig is not None
            and "spec" in config.nconfig
        ):
            spec = config.nconfig["spec"]
        else:
            spec = ServerlessSpec(cloud="gcp", region="us-central1")
        args = {
            "name": index,
            "dimension": dimension,
            "metric": metric,
            "spec": spec,
        }
        try:
            NCall(self.database_client.create_index, args, nargs).invoke()
        except PineconeApiException as e:
            if e.status == 409:
                if exists is False:
                    raise ConflictError
                return CollectionResult(status=CollectionStatus.EXISTS)
            else:
                raise
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        index: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {"name": index}
        try:
            NCall(self.database_client.delete_index, args, nargs).invoke()
        except NotFoundException:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        return NCall(self.database_client.list_indexes, None, nargs).invoke()

    def has_collection(
        self,
        index: str,
        nargs: Any,
    ):
        args = {"name": index}
        try:
            NCall(self.database_client.describe_index, args, nargs).invoke()
            return True
        except NotFoundException:
            return False

    def close(self, nargs: Any) -> Any:
        pass


class OperationConverter:
    processor: ItemProcessor
    namespace: str | None
    dimension: int
    content_metadata_field: str | None

    def __init__(
        self,
        processor: ItemProcessor,
        namespace: str | None,
        dimension: int,
        content_metadata_field: str | None,
    ) -> None:
        self.processor = processor
        self.namespace = namespace
        self.dimension = dimension
        self.content_metadata_field = content_metadata_field

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
        return {"ids": ids, "namespace": self.namespace}

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
            "vectors": vectors,
            "namespace": self.namespace,
        }

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {"ids": ids, "namespace": self.namespace}

    def convert_get(self, id: str) -> dict:
        return {"ids": [id], "namespace": self.namespace}

    def convert_put(self, id: str, value: dict, metadata: dict | None) -> dict:
        return {
            "vectors": [self.convert_put_vector(id, value, metadata)],
            "namespace": self.namespace,
        }

    def convert_put_vector(
        self, id: str, value: dict, metadata: dict | None
    ) -> dict:
        args: dict = {"id": id}
        content = None
        md = None
        vector_value = VectorValue(**value)
        if vector_value.vector is not None:
            args["values"] = vector_value.vector
        if vector_value.sparse_vector is not None:
            args["sparse_values"] = self.convert_sparse_vector(
                vector_value.sparse_vector
            )
        content = vector_value.content
        if metadata is None:
            if content is not None and self.content_metadata_field is not None:
                md = {self.content_metadata_field: content}
        else:
            md = copy.deepcopy(metadata)
            if content is not None and self.content_metadata_field is not None:
                md[self.content_metadata_field] = content
        if md is not None:
            args["metadata"] = md
        return args

    def convert_update_value(self, id: str, value: dict) -> dict:
        args: dict = {"id": id, "namespace": self.namespace}
        vector_value = VectorValue(**value)
        if vector_value.vector is not None:
            args["values"] = vector_value.vector
        if vector_value.sparse_vector is not None:
            args["sparse_values"] = self.convert_sparse_vector(
                vector_value.sparse_vector
            )
        if (
            vector_value.content is not None
            and self.content_metadata_field is not None
        ):
            args["set_metadata"] = {
                self.content_metadata_field: vector_value.content
            }
        return args

    def convert_update_metadata(self, id: str, metadata: dict | None) -> dict:
        return {
            "id": id,
            "set_metadata": metadata,
            "namespace": self.namespace,
        }

    def convert_delete(self, id: str | None, where: Expression) -> dict:
        if id is None and where is None:
            return {"delete_all": True, "namespace": self.namespace}
        if id is not None and where is None:
            return {"ids": [id], "namespace": self.namespace}
        if where is not None and id is None:
            return {
                "filter": self.convert_expr(where),
                "namespace": self.namespace,
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
    ) -> dict:
        args: dict = dict()
        search_args = self.convert_search(search)
        args = args | search_args
        select_args = self.convert_select(select)
        if select_args is not None:
            args = args | select_args
        if where is not None:
            args["filter"] = self.convert_expr(where)
        _offset = offset if offset is not None else 0
        _limit = limit if limit is not None else DEFAULT_LIMIT
        args["top_k"] = _limit + _offset
        return args

    def convert_select(self, select: Select | None) -> dict:
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
        return {
            "include_values": include_value,
            "include_metadata": include_metadata,
        }

    def convert_search(self, search: Function | None) -> dict:
        if search is None:
            return {"vector": [0.0] * self.dimension}
        namespace = search.namespace
        name = search.name
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.VECTOR_SEARCH:
                args = VectorSearchArgs(**search.named_args)
                return {
                    "vector": args.vector,
                    "sparse_vector": self.convert_sparse_vector(
                        args.sparse_vector
                    ),
                }

        raise BadRequestError("Search function not supported")

    def convert_sparse_vector(
        self, sparse_vector: dict[int, float] | None
    ) -> dict | None:
        if sparse_vector is None:
            return None
        indices = []
        values = []
        for k, v in sparse_vector.items():
            indices.append(k)
            values.append(v)
        return {"indices": indices, "values": values}

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
            comparion_map = {
                ComparisonOp.EQ: "$eq",
                ComparisonOp.NEQ: "$ne",
                ComparisonOp.GT: "$gt",
                ComparisonOp.GTE: "$gte",
                ComparisonOp.LT: "$lt",
                ComparisonOp.LTE: "$lte",
                ComparisonOp.IN: "$in",
                ComparisonOp.NIN: "$nin",
            }
            op = expr.op
            if isinstance(expr.lexpr, Field):
                field = self.convert_expr(expr.lexpr)
                value = self.convert_expr(expr.rexpr)
            elif isinstance(expr.rexpr, Field):
                field = self.convert_expr(expr.rexpr)
                value = self.convert_expr(expr.lexpr)
                op = Comparison.reverse_op(op)

            if op in comparion_map:
                pc_op = comparion_map[op]
                return {field: {pc_op: value}}
            elif op == ComparisonOp.BETWEEN:
                return {
                    "$and": [
                        {field: {"$gte": value[0]}},
                        {field: {"$lte": value[1]}},
                    ]
                }
            else:
                raise BadRequestError(f"Comparison op {expr.op} not supported")
        if isinstance(expr, And):
            return {
                "$and": [
                    self.convert_expr(expr.lexpr),
                    self.convert_expr(expr.rexpr),
                ]
            }
        if isinstance(expr, Or):
            return {
                "$or": [
                    self.convert_expr(expr.lexpr),
                    self.convert_expr(expr.rexpr),
                ]
            }
        if isinstance(expr, Not):
            raise BadRequestError("Not expression is not supported")
        return str(expr)

    def convert_func(self, expr: Function) -> dict:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(args[0])
                return {field: {"$exists": True}}
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(args[0])
                return {field: {"$exists": False}}
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(args[0])
                return {field: {"$eq": args[1]}}
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(args[0])
                return {field: {"$in": args[1]}}
        raise BadRequestError(f"Function {name} not supported")

    def convert_count(self, where: Expression) -> dict | None:
        if where is None:
            return None
        args = {"filter": self.convert_expr(where)}
        return args


class PineconeCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor

    def __init__(
        self,
        client: Any,
        namespace: str | None,
        dimension: int,
        content_metadata_field: str | None,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.converter = OperationConverter(
            processor=self.processor,
            namespace=namespace,
            dimension=dimension,
            content_metadata_field=content_metadata_field,
        )
