"""
Vector Store on Milvus.
"""

from __future__ import annotations

__all__ = ["Milvus"]

import json
from typing import Any

from pymilvus import DataType, MilvusClient

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


class Milvus(StoreProvider):
    uri: str
    token: str | None
    user: str | None
    password: str | None
    collection: str | None
    id_field: str
    vector_field: str
    metadata_field: str
    content_field: str | None
    sparse_vector_field: str | None
    nparams: dict[str, Any]

    _client: MilvusClient
    _processor: ItemProcessor

    _collection_cache: dict[str, MilvusCollection]

    def __init__(
        self,
        uri: str,
        token: str | None = None,
        user: str | None = None,
        password: str | None = None,
        collection: str | None = None,
        id_field: str = "id",
        vector_field: str = "vector",
        metadata_field: str = "metadata",
        content_field: str | None = "content",
        sparse_vector_field: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            uri:
                Milvus endpoint.
            token:
                Milvus API key.
            user:
                Milvus user name.
            password:
                Milvus password.
            collection:
                Milvus collection name mapped to vector store collection.
            id_field:
                Milvus field mapped as id attribute. Defaults to "id".
            vector_field:
                Milvus field mapped as vector value attribute.
                Defaults to "vector".
            metadata_field:
                Milvus field mapped as metadata attribute.
                Defaults to "metadata".
            content_field:
                Milvus field mapped as content value attribute.
                Defaults to "content".
            sparse_vector_field:
                Milvus field mapped as sparse vector value attribute.
                Defaults to None.
            nparams:
                Native parameters to milvus client.
        """
        self.uri = uri
        self.token = token
        self.user = user
        self.password = password
        self.collection = collection
        self.id_field = id_field
        self.vector_field = vector_field
        self.metadata_field = metadata_field
        self.content_field = content_field
        self.sparse_vector_field = sparse_vector_field
        self.nparams = nparams

        self._client = None
        self._processor = ItemProcessor()
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        self._client = MilvusClient(
            uri=self.uri,
            token=self.token,
            user=self.user,
            password=self.password,
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
    ) -> MilvusCollection | None:
        if op_parser.is_resource_op():
            return None
        collection_name = self._get_collection_name(op_parser)
        if collection_name is None:
            raise BadRequestError("Collection name must be specified")
        if collection_name in self._collection_cache:
            return self._collection_cache[collection_name]
        description = self._client.describe_collection(collection_name)
        dimension = self._parse_dimension(description)
        col = MilvusCollection(
            self._client,
            collection_name,
            dimension,
            self.id_field,
            self.vector_field,
            self.metadata_field,
            self.content_field,
            self.sparse_vector_field,
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

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collection: MilvusCollection | None,
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
                "id_field": self.id_field,
                "vector_field": self.vector_field,
                "metadata_field": self.metadata_field,
                "content_field": self.content_field,
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
            call = NCall(client.get, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args = converter.convert_put(
                op_parser.get_id_as_str(),
                op_parser.get_vector_value(),
                op_parser.get_metadata(),
            )
            call = NCall(client.upsert, args, nargs)
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
            args = converter.convert_query(
                search=op_parser.get_search_as_function(),
                where=op_parser.get_where(),
                select=op_parser.get_select(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
            )
            call = NCall(client.search, args, nargs)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = converter.convert_count(
                where=op_parser.get_where(),
            )
            call = NCall(client.get_collection_stats, args, nargs)
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            args, func = converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            if func == "get":
                call = NCall(client.get, args, nargs)
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
        collection: MilvusCollection | None,
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
            result = nresult
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
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items = []
            for item in nresult[0]:
                items.append(self._convert_to_item(item, processor, True))
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
            result = nresult["row_count"]
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

    def _convert_to_item(
        self, nitem: Any, processor: ItemProcessor, entity: bool = False
    ) -> VectorItem:
        vector = None
        content = None
        sparse_vector = None
        metadata: dict | None = None
        distance: float | None = None
        if entity:
            distance = nitem["distance"]
            nitem = nitem["entity"]
        key = VectorKey(id=nitem[self.id_field])
        if self.vector_field in nitem:
            if entity:
                vector = nitem[self.vector_field]
            else:
                vector = [v.item() for v in nitem[self.vector_field]]
        if self.sparse_vector_field in nitem:
            sparse_vector = nitem[self.sparse_vector_field]
        if self.content_field in nitem:
            content = nitem[self.content_field]
        value = VectorValue(
            vector=vector,
            sparse_vector=sparse_vector,
            content=content,
        )

        if self.metadata_field in nitem:
            metadata = nitem[self.metadata_field]
            if isinstance(metadata, dict) and len(metadata) == 0:
                metadata = None
        properties = None
        if distance is not None:
            properties = VectorProperties(score=distance)
        return VectorItem(
            key=key, value=value, metadata=metadata, properties=properties
        )

    def _parse_dimension(self, description: Any) -> int:
        fields = description["fields"]
        for f in fields:
            if f["name"] == self.vector_field:
                return f["params"]["dim"]
        raise BadRequestError("Vector field not found")


class ResourceHelper:
    client: MilvusClient

    def __init__(self, client: MilvusClient):
        self.client = client

    def create_collection(
        self,
        collection: str,
        config: VectorCollectionConfig | None,
        id_field: str,
        vector_field: str,
        metadata_field: str,
        content_field: str | None,
        sparse_vector_field: str | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        metric_mapping = {
            VectorIndexMetric.DOT_PRODUCT: "IP",
            VectorIndexMetric.COSINE: "COSINE",
            VectorIndexMetric.EUCLIDEAN: "EUCLIDEAN",
        }
        vector_index = config.vector_index if config is not None else None
        if vector_index is not None:
            dimension = vector_index.dimension
            metric = metric_mapping[vector_index.metric]
        else:
            dimension = 4
            metric = "IP"

        schema = MilvusClient.create_schema(
            auto_id=False,
            enable_dynamic_field=False,
        )
        schema.add_field(
            field_name=id_field,
            datatype=DataType.VARCHAR,
            max_length=256,
            is_primary=True,
        )
        schema.add_field(
            field_name=vector_field,
            datatype=DataType.FLOAT_VECTOR,
            dim=dimension,
        )
        schema.add_field(field_name=metadata_field, datatype=DataType.JSON)
        if content_field is not None:
            schema.add_field(
                field_name=content_field,
                datatype=DataType.VARCHAR,
                max_length=65535,
            )
        if sparse_vector_field is not None:
            schema.add_field(
                field_name=sparse_vector_field,
                datatype=DataType.SPARSE_FLOAT_VECTOR,
            )
        index_params = self.client.prepare_index_params()
        index_params.add_index(field_name=id_field)
        index_params.add_index(
            field_name=vector_field,
            index_type="AUTOINDEX",
            metric_type=metric,
        )
        if sparse_vector_field is not None:
            index_params.add_index(field_name=sparse_vector_field)
        args = {
            "collection_name": collection,
            "schema": schema,
            "index_params": index_params,
        }
        if self.client.has_collection(collection_name=collection):
            if exists is False:
                raise ConflictError
            return CollectionResult(status=CollectionStatus.EXISTS)

        NCall(self.client.create_collection, args, nargs).invoke()
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
        if not self.client.has_collection(collection):
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        NCall(self.client.drop_collection, args, nargs).invoke()
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        return NCall(self.client.list_collections, None, nargs).invoke()

    def has_collection(
        self,
        collection: str,
        nargs: Any,
    ):
        args = {
            "collection_name": collection,
        }
        return NCall(self.client.has_collection, args, nargs).invoke()

    def close(self, nargs: Any) -> Any:
        pass


class OperationConverter:
    processor: ItemProcessor
    collection: str
    dimension: int
    id_field: str
    vector_field: str
    metadata_field: str
    content_field: str | None
    sparse_vector_field: str | None

    def __init__(
        self,
        processor: ItemProcessor,
        collection: str,
        dimension: int,
        id_field: str,
        vector_field: str,
        metadata_field: str,
        content_field: str | None,
        sparse_vector_field: str | None,
    ) -> None:
        self.processor = processor
        self.collection = collection
        self.dimension = dimension
        self.id_field = id_field
        self.vector_field = vector_field
        self.metadata_field = metadata_field
        self.content_field = content_field
        self.sparse_vector_field = sparse_vector_field

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
        return {"collection_name": self.collection, "ids": ids}

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
            "data": vectors,
        }

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {"collection_name": self.collection, "ids": ids}

    def convert_get(self, id: str) -> dict:
        return {"collection_name": self.collection, "ids": [id]}

    def convert_put(self, id: str, value: dict, metadata: dict | None) -> dict:
        return {
            "collection_name": self.collection,
            "data": [self.convert_put_vector(id, value, metadata)],
        }

    def convert_put_vector(
        self, id: str, value: dict, metadata: dict | None
    ) -> dict:
        vector_value = VectorValue(**value)
        args: dict = {
            self.id_field: id,
            self.vector_field: vector_value.vector,
        }
        if metadata is not None:
            args[self.metadata_field] = metadata
        else:
            args[self.metadata_field] = {}
        if self.content_field is not None and vector_value.content is not None:
            args[self.content_field] = vector_value.content
        if (
            self.sparse_vector_field is not None
            and vector_value.sparse_vector is not None
        ):
            args[self.sparse_vector_field] = vector_value.sparse_vector
        return args

    def convert_delete(self, id: str | None, where: Expression) -> dict:
        if id is None and where is None:
            return {"collection_name": self.collection}
        if id is not None and where is None:
            return {"collection_name": self.collection, "ids": [id]}
        if where is not None and id is None:
            return {
                "collection_name": self.collection,
                "filter": self.convert_expr(where),
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
        args: dict = {"collection_name": self.collection}
        search_args = self.convert_search(search)
        args = args | search_args
        select_args = self.convert_select(select)
        if select_args is not None:
            args = args | select_args
        if where is not None:
            args["filter"] = self.convert_expr(where)
        _offset = offset if offset is not None else 0
        _limit = limit if limit is not None else DEFAULT_LIMIT
        args["limit"] = _limit + _offset
        return args

    def convert_select(self, select: Select | None) -> dict:
        fields = [self.id_field]
        select_value = False
        if select is None or len(select.terms) == 0:
            select_value = True
            fields.append(self.metadata_field)
        else:
            for term in select.terms:
                if is_value_field(term.field):
                    select_value = True
                elif is_metadata_field(term.field):
                    fields.append(self.metadata_field)
        if select_value:
            fields.append(self.vector_field)
            if self.content_field is not None:
                fields.append(self.content_field)
            if self.sparse_vector_field is not None:
                fields.append(self.sparse_vector_field)
        return {"output_fields": fields}

    def convert_search(self, search: Function | None) -> dict:
        if search is None:
            return {"data": [[0.0] * self.dimension]}
        namespace = search.namespace
        name = search.name
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.VECTOR_SEARCH:
                args = VectorSearchArgs(**search.named_args)
                return {
                    "data": [args.vector],
                }

        raise BadRequestError("Search function not supported")

    def convert_expr(self, expr: Expression) -> str:
        if expr is None or isinstance(
            expr, (str, int, float, bool, dict, list)
        ):
            return json.dumps(expr)
        if isinstance(expr, Field):
            field = self.processor.resolve_metadata_field(expr.path)
            return f"{self.metadata_field}['{field}']"
        if isinstance(expr, Function):
            return self.convert_func(expr)
        if isinstance(expr, Comparison):
            comparion_map = {
                ComparisonOp.EQ: "==",
                ComparisonOp.NEQ: "!=",
                ComparisonOp.GT: ">",
                ComparisonOp.GTE: ">=",
                ComparisonOp.LT: "<",
                ComparisonOp.LTE: "<=",
                ComparisonOp.IN: "in",
                ComparisonOp.NIN: "not in",
                ComparisonOp.LIKE: "like",
            }
            op = expr.op
            if op in comparion_map:
                if isinstance(expr.lexpr, Field):
                    field = self.convert_expr(expr.lexpr)
                    value = self.convert_expr(expr.rexpr)
                elif isinstance(expr.rexpr, Field):
                    field = self.convert_expr(expr.rexpr)
                    value = self.convert_expr(expr.lexpr)
                    op = Comparison.reverse_op(op)
                m_op = comparion_map[op]
                return f"{field} {m_op} {value}"
            elif op == ComparisonOp.BETWEEN:
                field = self.convert_expr(expr.lexpr)
                if isinstance(expr.rexpr, list):
                    low = self.convert_expr(expr.rexpr[0])
                    high = self.convert_expr(expr.rexpr[1])
                    return f"{field} >= {low} and {field} <= {high}"
                raise BadRequestError("BETWEEN has bad value type")
            else:
                raise BadRequestError(f"Comparison op {expr.op} not supported")
        if isinstance(expr, And):
            lhs = self.convert_expr(expr.lexpr)
            rhs = self.convert_expr(expr.rexpr)
            return f"({lhs} and {rhs})"
        if isinstance(expr, Or):
            lhs = self.convert_expr(expr.lexpr)
            rhs = self.convert_expr(expr.rexpr)
            return f"({lhs} or {rhs})"
        if isinstance(expr, Not):
            hs = self.convert_expr(expr.expr)
            return f"not {hs}"
        return str(expr)

    def convert_func(self, expr: Function) -> str:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(args[0])
                return f"exists {field}"
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(args[0])
                return f"not exists {field}"
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(args[0])
                value = self.convert_expr(args[1])
                return f"json_contains({field}, {value})"
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(args[0])
                value = self.convert_expr(args[1])
                return f"json_contains_any({field}, {value})"
        raise BadRequestError(f"Function {name} not supported")

    def convert_count(self, where: Expression) -> dict | None:
        return {"collection_name": self.collection}


class MilvusCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor

    def __init__(
        self,
        client: Any,
        collection: str,
        dimension: int,
        id_field: str,
        vector_field: str,
        metadata_field: str,
        content_field: str | None,
        sparse_vector_field: str | None,
    ):
        self.client = client
        self.processor = ItemProcessor()
        self.converter = OperationConverter(
            processor=self.processor,
            collection=collection,
            dimension=dimension,
            id_field=id_field,
            vector_field=vector_field,
            metadata_field=metadata_field,
            content_field=content_field,
            sparse_vector_field=sparse_vector_field,
        )
