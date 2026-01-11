"""
Vector Store on Chroma.
"""

from __future__ import annotations

import chromadb.errors

__all__ = ["Chroma"]

from typing import Any

import chromadb
import chromadb.config

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


class Chroma(StoreProvider):
    type: str | None
    collection: str | None
    dimension: int | dict[str, int]
    tenant: str
    database: str
    path: str
    host: str
    port: int
    ssl: bool
    auth_provider: str | None
    auth_credentials: str | None
    nparams: dict[str, Any]

    _init: bool = False
    _database_client: chromadb.ClientAPI
    _processor: ItemProcessor
    _collection_cache: dict[str, ChromaCollection]

    def __init__(
        self,
        type: str | None = "memory",
        collection: str | None = None,
        dimension: int | dict[str, int] = 4,
        tenant: str = "default_tenant",
        database: str = "default_database",
        path: str = "./chroma",
        host: str = "localhost",
        port: int = 8000,
        ssl: bool = False,
        auth_provider: str | None = None,
        auth_credentials: str | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            type:
                Chroma client type. "memory", "persistant", or "http".
                Defaults to "memory".
            collection:
                Chroma collection name mapped to vector store collection.
            dimension:
                Vector dimension.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the dimension.
            tenant:
                Chroma tenant name. Defaults to "default_tenant".
            database:
                Chroma database name. Defaults to "default_database".
            path:
                Path to persistent database. Defaults to "./chroma".
            host:
                Host for http client. Defaults to "localhost".
            port:
                Port for http client. Defaults to 8000.
            ssl:
                SSL option for http client. Defaults to False.
            auth_provider:
                Auth provider for http client. Defaults to None.
            auth_credentials:
                Auth credentials for http client. Defaults to None.
            nparams:
                Native parameters to chroma client.
        """
        self.type = type
        self.collection = collection
        self.dimension = dimension
        self.tenant = tenant
        self.database = database
        self.path = path
        self.host = host
        self.port = port
        self.ssl = ssl
        self.auth_provider = auth_provider
        self.auth_credentials = auth_credentials
        self.nparams = nparams

        self._init = False
        self._processor = ItemProcessor()
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        if self.type == "memory":
            self._database_client = chromadb.Client(
                tenant=self.tenant,
                database=self.database,
                **self.nparams,
            )
        elif self.type == "persistent":
            self._database_client = chromadb.PersistentClient(
                path=self.path,
                tenant=self.tenant,
                database=self.database,
                **self.nparams,
            )
        elif self.type == "http":
            settings = None
            if self.auth_provider is not None:
                cred = self.auth_credentials
                settings = chromadb.config.Settings(
                    chroma_client_auth_provider=self.auth_provider,
                    chroma_client_auth_credentials=cred,
                )
            if settings is None:
                self._database_client = chromadb.HttpClient(
                    host=self.host,
                    port=self.port,
                    ssl=self.ssl,
                    **self.nparams,
                )
            else:
                self._database_client = chromadb.HttpClient(
                    host=self.host,
                    port=self.port,
                    ssl=self.ssl,
                    settings=settings,
                    **self.nparams,
                )
        else:
            raise BadRequestError(f"Type {self.type} is not supported")
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
    ) -> ChromaCollection | None:
        if op_parser.is_resource_op():
            return None
        collection_name = self._get_collection_name(op_parser)
        if collection_name is None:
            raise BadRequestError("Collection name must be specified")
        if collection_name in self._collection_cache:
            return self._collection_cache[collection_name]
        client = self._database_client.get_collection(collection_name)
        dimension = ParameterParser.get_collection_parameter(
            self.dimension, collection_name
        )
        col = ChromaCollection(
            client,
            dimension,
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
        collection: ChromaCollection | None,
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
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.create_collection,
                args,
            )
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
            call = NCall(client.query, args, nargs)
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
        collection: ChromaCollection | None,
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
            if len(nresult["ids"]) == 0:
                raise NotFoundError
            result = self._convert_to_items(nresult, processor)[0]
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
            items: list[VectorItem] = self._convert_to_items(
                nresult, processor, is_query=True
            )
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
            result = nresult
        # BATCH or TRANSACT
        elif op_parser.op_equals(StoreOperation.BATCH) or op_parser.op_equals(
            StoreOperation.TRANSACT
        ):
            result = []
            func = state["func"] if state is not None else None
            if func == "get":
                result = self._convert_to_items(nresult, processor)
            elif func == "put":
                for op_parser in op_parser.get_operation_parsers():
                    result.append(
                        VectorItem(key=VectorKey(id=op_parser.get_id_as_str()))
                    )
            elif func == "delete":
                for op_parser in op_parser.get_operation_parsers():
                    result.append(None)
        return result

    def _convert_to_items(
        self, nitems: Any, processor: ItemProcessor, is_query: bool = False
    ) -> list[VectorItem]:
        items = []
        count = len(nitems["ids"][0]) if is_query else len(nitems["ids"])
        for i in range(0, count):
            vector = None
            content = None
            metadata = None
            if is_query:
                key = VectorKey(id=nitems["ids"][0][i])
            else:
                key = VectorKey(id=nitems["ids"][i])
            if "metadatas" in nitems and nitems["metadatas"]:
                if is_query:
                    metadata = nitems["metadatas"][0][i]
                else:
                    metadata = nitems["metadatas"][i]
            if "embeddings" in nitems and nitems["embeddings"] is not None:
                if is_query:
                    vector = nitems["embeddings"][0][i]
                else:
                    vector = nitems["embeddings"][i]
            if "documents" in nitems and nitems["documents"]:
                if is_query:
                    content = nitems["documents"][0][i]
                else:
                    content = nitems["documents"][i]
            value = VectorValue(
                vector=vector,
                content=content,
            )
            properties = None
            if "distances" in nitems and nitems["distances"]:
                if is_query:
                    score = nitems["distances"][0][i]
                else:
                    score = nitems["distances"][i]
                properties = VectorProperties(score=1 - score)
            items.append(
                VectorItem(
                    key=key,
                    value=value,
                    metadata=metadata,
                    properties=properties,
                )
            )
        return items


class ResourceHelper:
    database_client: chromadb.ClientAPI

    def __init__(self, database_client: Any):
        self.database_client = database_client

    def create_collection(
        self,
        collection: str,
        config: VectorCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        metric_mapping = {
            VectorIndexMetric.DOT_PRODUCT: "ip",
            VectorIndexMetric.COSINE: "cosine",
            VectorIndexMetric.EUCLIDEAN: "l2",
        }
        vector_index = config.vector_index if config is not None else None
        if vector_index is not None:
            metric = metric_mapping[vector_index.metric]
        else:
            metric = "ip"

        args = {
            "name": collection,
            "metadata": {"hnsw:space": metric},
        }
        try:
            NCall(self.database_client.create_collection, args, nargs).invoke()
        except chromadb.errors.InternalError as e:
            if "exists" in str(e):
                if exists is False:
                    raise ConflictError
            else:
                raise e
            return CollectionResult(status=CollectionStatus.EXISTS)
        return CollectionResult(status=CollectionStatus.CREATED)

    def drop_collection(
        self,
        collection: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        args = {
            "name": collection,
        }
        try:
            NCall(self.database_client.delete_collection, args, nargs).invoke()
        except chromadb.errors.NotFoundError:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        response = NCall(
            self.database_client.list_collections, None, nargs
        ).invoke()
        return [col.name for col in response]

    def has_collection(
        self,
        collection: str,
        nargs: Any,
    ):
        args = {
            "name": collection,
        }
        try:
            NCall(self.database_client.get_collection, args, nargs).invoke()
            return True
        except chromadb.errors.NotFoundError:
            return False

    def close(self, nargs: Any) -> Any:
        pass


class OperationConverter:
    processor: ItemProcessor
    dimension: int

    def __init__(
        self,
        processor: ItemProcessor,
        dimension: int,
    ) -> None:
        self.processor = processor
        self.dimension = dimension

    def filter_metadata(self, metadata: dict | None) -> dict | None:
        if metadata is None:
            return None
        md = dict()
        for k, v in metadata.items():
            if isinstance(v, (str, int, bool)):
                md[k] = v
        return md

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
            "ids": ids,
            "include": ["embeddings", "documents", "metadatas"],
        }

    def convert_batch_put(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids = []
        embeddings = []
        documents = []
        metadatas = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
            metadatas.append(self.filter_metadata(op_parser.get_metadata()))
            vector_value = VectorValue(**op_parser.get_vector_value())
            embeddings.append(vector_value.vector)
            documents.append(vector_value.content)
        return {
            "ids": ids,
            "embeddings": embeddings,
            "metadatas": metadatas,
            "documents": documents,
        }

    def convert_batch_delete(
        self, op_parsers: list[StoreOperationParser]
    ) -> dict:
        ids = []
        for op_parser in op_parsers:
            ids.append(op_parser.get_id_as_str())
        return {
            "ids": ids,
        }

    def convert_get(self, id: str) -> dict:
        return {
            "ids": [id],
            "include": ["embeddings", "documents", "metadatas"],
        }

    def convert_put(self, id: str, value: dict, metadata: dict | None) -> dict:
        vector_value = VectorValue(**value)
        args = {
            "ids": [id],
            "embeddings": [vector_value.vector],
        }
        if vector_value.content is not None:
            args["documents"] = [vector_value.content]
        if metadata is not None:
            args["metadatas"] = [self.filter_metadata(metadata)]
        return args

    def convert_update_value(self, id: str, value: dict) -> dict:
        args: dict = {"ids": [id]}
        vector_value = VectorValue(**value)
        args["embeddings"] = [vector_value.vector]
        if vector_value.content is not None:
            args["documents"] = [vector_value.content]
        return args

    def convert_update_metadata(self, id: str, metadata: dict | None) -> dict:
        args = {"ids": [id], "metadatas": [self.filter_metadata(metadata)]}
        return args

    def convert_delete(self, id: str | None, where: Expression) -> dict:
        if id is None and where is None:
            raise BadRequestError("Id or where should be provided for delete")
        if id is not None and where is None:
            return {"ids": [id]}
        if where is not None and id is None:
            return {"where": self.convert_expr(where)}
        return {"ids": [id], "where": self.convert_expr(where)}

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
            args["where"] = self.convert_expr(where)
        _offset = offset if offset is not None else 0
        _limit = limit if limit is not None else DEFAULT_LIMIT
        args["n_results"] = _limit + _offset
        return args

    def convert_select(self, select: Select | None) -> dict:
        include = ["distances"]
        if select is None or len(select.terms) == 0:
            include.extend(["embeddings", "documents", "metadatas"])
        else:
            for term in select.terms:
                if is_value_field(term.field):
                    include.extend(["embeddings", "documents"])
                elif is_metadata_field(term.field):
                    include.extend(["metadatas"])
        return {"include": include}

    def convert_search(self, search: Function | None) -> Any:
        if search is None:
            return {"query_embeddings": [0.0] * self.dimension}
        namespace = search.namespace
        name = search.name
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.VECTOR_SEARCH:
                args = VectorSearchArgs(**search.named_args)
                return {
                    "query_embeddings": [args.vector],
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
        name = expr.name
        raise BadRequestError(f"Function {name} not supported")

    def convert_count(self, where: Expression) -> dict | None:
        if where is not None:
            raise BadRequestError("Where in count is not supported")
        args: dict = {}
        return args


class ChromaCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor

    def __init__(self, client: Any, dimension: int):
        self.client = client
        self.processor = ItemProcessor()
        self.converter = OperationConverter(
            processor=self.processor,
            dimension=dimension,
        )
