"""
Document Store on Azure Cosmos DB.
"""

from __future__ import annotations

__all__ = ["AzureCosmosDB"]

import json
from typing import Any

from azure.core import MatchConditions
from azure.cosmos import PartitionKey, ThroughputProperties
from azure.cosmos.exceptions import (
    CosmosAccessConditionFailedError,
    CosmosBatchOperationError,
    CosmosHttpResponseError,
    CosmosResourceExistsError,
    CosmosResourceNotFoundError,
)

from x8._common.azure_provider import AzureProvider
from x8.core import Context, DataAccessor, NCall, Operation, Response
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
    QueryFunctionName,
    QueryProcessor,
    Select,
    Update,
    UpdateOp,
    Value,
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
)
from x8.storage._common import IndexHelper as BaseIndexHelper
from x8.storage._common import (
    IndexResult,
    IndexStatus,
    ItemProcessor,
    ParameterParser,
    RangeIndex,
    StoreOperation,
    StoreOperationParser,
    StoreProvider,
    TTLIndex,
    Validator,
    VectorIndex,
    VectorIndexStructure,
    WildcardIndex,
)

from .._feature import DocumentStoreFeature
from .._helper import (
    build_item_from_value,
    build_query_result,
    get_collection_config,
)
from .._models import (
    DocumentCollectionConfig,
    DocumentFieldType,
    DocumentItem,
    DocumentKeyType,
)


class AzureCosmosDB(AzureProvider, StoreProvider):
    endpoint: str
    database: str
    container: str | None
    access_key: str | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None
    suppress_fields: list[str] | None
    nparams: dict[str, Any]

    _credential: Any
    _acredential: Any
    _document_client: Any
    _adocument_client: Any
    _database_client: Any
    _adatabase_client: Any

    _collection_cache: dict[str, CosmosDBCollection]
    _acollection_cache: dict[str, CosmosDBCollection]

    def __init__(
        self,
        endpoint: str,
        database: str,
        container: str | None = None,
        access_key: str | None = None,
        credential_type: str | None = "default",
        tenant_id: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        certificate_path: str | None = None,
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = "pk",
        suppress_fields: list[str] | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            endpoint:
                Cosmos DB endpoint.
            database:
                Cosmos DB database name where collection is hosted.
            container:
                Cosmos DB container name mapped to
                document store collection.
            access_key:
                Cosmos DB access key for access key-based auth.
            credential_type:
                Azure credential type.
            tenant_id:
                Azure tenant id for client_secret credential type.
            client_id:
                Azure client id for client_secret credential type.
            client_secret:
                Azure client secret for client_secret credential type.
            certificate_path:
                Certificate path for certificate credential type.
            id_map_field:
                Field in the document to map into id.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            pk_map_field:
                Field in the document to map into pk.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            suppress_fields:
                List of fields to supress when results are returned.
            nparams:
                Native parameters to Cosmos DB client.
        """
        self.endpoint = endpoint
        self.database = database
        self.container = container
        self.access_key = access_key
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.suppress_fields = suppress_fields
        self.nparams = nparams

        self._database_client = None
        self._adatabase_client = None
        self._collection_cache = dict()
        self._acollection_cache = dict()
        AzureProvider.__init__(
            self,
            credential_type=credential_type,
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            certificate_path=certificate_path,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return feature not in [DocumentStoreFeature.TYPE_BINARY]

    def __setup__(self, context: Context | None = None) -> None:
        if self._database_client is not None:
            return

        from azure.cosmos import CosmosClient

        credential = None
        if self.access_key is not None:
            document_client = CosmosClient(
                url=self.endpoint,
                credential=self.access_key,
            )
        else:
            credential = self._get_credential()
            document_client = CosmosClient(
                url=self.endpoint,
                credential=credential,
                **self.nparams,
            )
        database_client = document_client.get_database_client(
            database=self.database
        )
        self._credential, self._document_client, self._database_client = (
            credential,
            document_client,
            database_client,
        )

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._adatabase_client is not None:
            return

        from azure.cosmos.aio import CosmosClient

        credential = None
        if self.access_key is not None:
            document_client = CosmosClient(
                url=self.endpoint,
                credential=self.access_key,
            )
        else:
            credential = self._aget_credential()
            document_client = CosmosClient(
                url=self.endpoint,
                credential=credential,
                **self.nparams,
            )
        database_client = document_client.get_database_client(
            database=self.database
        )
        self._acredential, self._adocument_client, self._adatabase_client = (
            credential,
            document_client,
            database_client,
        )

    def _get_container_name(
        self, op_parser: StoreOperationParser
    ) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        container = (
            collection_name or self.container or self.__component__.collection
        )
        return container

    def _get_collections(
        self, op_parser: StoreOperationParser
    ) -> list[CosmosDBCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[CosmosDBCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(self._get_collections(single_op_parser))
            return collections
        container = self._get_container_name(op_parser)
        if container is None:
            raise BadRequestError("Collection name must be specified")
        if container in self._collection_cache:
            return [self._collection_cache[container]]
        client = self._database_client.get_container_client(
            container=container
        )
        metadata = client.read()
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field, container
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field, container
        )
        col = CosmosDBCollection(
            client,
            ClientHelper,
            metadata,
            id_map_field,
            pk_map_field,
            self.suppress_fields,
        )
        self._collection_cache[container] = col
        return [col]

    async def _aget_collections(
        self, op_parser: StoreOperationParser
    ) -> list[CosmosDBCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[CosmosDBCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(
                    await self._aget_collections(single_op_parser)
                )
            return collections
        container = self._get_container_name(op_parser)
        if container is None:
            raise BadRequestError("Collection name must be specified")
        if container in self._acollection_cache:
            return [self._acollection_cache[container]]
        client = self._adatabase_client.get_container_client(
            container=container
        )
        metadata = await client.read()
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field, container
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field, container
        )
        col = CosmosDBCollection(
            client,
            AsyncClientHelper,
            metadata,
            id_map_field,
            pk_map_field,
            self.suppress_fields,
        )
        self._acollection_cache[container] = col
        return [col]

    def _validate(self, op_parser: StoreOperationParser):
        if op_parser.op_equals(StoreOperation.BATCH):
            Validator.validate_batch(
                op_parser.get_operation_parsers(),
                allowed_ops=[StoreOperation.PUT, StoreOperation.DELETE],
                single_collection=True,
            )
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            Validator.validate_transact(
                op_parser.get_operation_parsers(),
                allowed_ops=[
                    StoreOperation.PUT,
                    StoreOperation.UPDATE,
                    StoreOperation.DELETE,
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
        collections = self._get_collections(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collections,
            ResourceHelper(
                self._document_client, self._database_client, self._credential
            ),
            self._collection_cache,
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(nresult, state, op_parser, collections)
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
        collections = await self._aget_collections(op_parser)
        ncall, state = self._get_ncall(
            op_parser,
            collections,
            AsyncResourceHelper(
                self._adocument_client,
                self._adatabase_client,
                self._acredential,
            ),
            self._acollection_cache,
        )
        if ncall is None:
            return await super().__arun__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(nresult, state, op_parser, collections)
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collections: list[CosmosDBCollection],
        resource_helper: Any,
        collection_cache: dict,
    ) -> tuple[NCall | None, dict | None]:
        if len(collections) == 1:
            converter = collections[0].converter
            client = collections[0].client
            helper = collections[0].helper
        call = None
        state = None
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            container = self._get_container_name(op_parser)
            args = {
                "database": self.database,
                "container": container,
                "config": get_collection_config(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.create_collection,
                args,
            )
            if container in collection_cache:
                del collection_cache[container]
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            container = self._get_container_name(op_parser)
            args = {
                "container": container,
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.drop_collection,
                args,
            )
            if container in collection_cache:
                del collection_cache[container]
        # LIST COLLECTION
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_collections, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = {
                "container": self._get_container_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(
                resource_helper.has_collection,
                args,
                None,
                {CosmosResourceNotFoundError: NotFoundError},
            )
        # CREATE INDEX
        elif op_parser.op_equals(StoreOperation.CREATE_INDEX):
            args = {
                "client": client,
                "index": op_parser.get_index(),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.create_index, args)
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            args = {
                "client": client,
                "index": op_parser.get_index(),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.drop_index, args)
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            args = {
                "client": client,
                "nargs": nargs,
            }
            call = NCall(resource_helper.list_indexes, args)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = converter.convert_get(op_parser.get_key())
            call = NCall(
                client.read_item,
                args,
                nargs,
                {CosmosResourceNotFoundError: NotFoundError},
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, state, func = converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where(),
                op_parser.get_where_exists(),
                op_parser.get_where_etag(),
            )
            if func == "upsert_item":
                func_ptr = client.upsert_item
            elif func == "create_item":
                func_ptr = client.create_item
            elif func == "replace_item":
                func_ptr = client.replace_item
            elif func == "helper":
                args["nargs"] = nargs
                func_ptr = helper.put
            call = NCall(
                func_ptr,
                args,
                nargs,
                {
                    CosmosResourceExistsError: PreconditionFailedError,
                    CosmosResourceNotFoundError: PreconditionFailedError,
                    CosmosAccessConditionFailedError: PreconditionFailedError,
                },
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, state = converter.convert_update(
                op_parser.get_key(),
                op_parser.get_set(),
                op_parser.get_where(),
                op_parser.get_returning_as_bool(),
            )
            call = NCall(
                client.patch_item,
                args,
                nargs,
                {
                    CosmosResourceNotFoundError: (
                        NotFoundError
                        if op_parser.get_where() is None
                        else PreconditionFailedError
                    ),
                    CosmosAccessConditionFailedError: PreconditionFailedError,
                    CosmosHttpResponseError: BadRequestError,
                },
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args, func = converter.convert_delete(
                op_parser.get_key(),
                op_parser.get_where(),
                op_parser.get_where_etag(),
            )
            if func == "delete_item":
                func_ptr = client.delete_item
            elif func == "helper":
                args["nargs"] = nargs
                func_ptr = helper.delete
            call = NCall(
                func_ptr,
                args,
                nargs,
                {
                    CosmosResourceNotFoundError: (
                        NotFoundError
                        if op_parser.get_where() is None
                        else PreconditionFailedError
                    ),
                    CosmosAccessConditionFailedError: PreconditionFailedError,
                },
            )
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = converter.convert_query(
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
            )
            args["nargs"] = nargs
            call = NCall(helper.query, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = converter.convert_count(
                where=op_parser.get_where(),
            )
            args["nargs"] = nargs
            call = NCall(helper.query, args)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, state = converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            args["nargs"] = nargs
            call = NCall(helper.batch, args, None)
        # TRANSACT
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            args, state = OperationConverter.convert_transact(
                op_parser.get_operation_parsers(),
                [col.converter for col in collections],
            )
            call = NCall(collections[0].helper.transact, args, None)
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
        collections: list[CosmosDBCollection],
    ) -> Any:
        if len(collections) == 1:
            processor = collections[0].processor
            converter = collections[0].converter
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            result = nresult
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            result = nresult
        # LIST COLLECTION
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = []
            for item in nresult:
                result.append(item["id"])
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = nresult
        # CREATE INDEX
        elif op_parser.op_equals(StoreOperation.CREATE_INDEX):
            result = nresult
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            result = nresult
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            result = build_item_from_value(
                processor=processor,
                value=nresult,
                include_value=True,
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            returning = op_parser.get_returning_as_bool()
            result = build_item_from_value(
                processor=processor,
                value=nresult,
                include_value=(returning or False),
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            returning = op_parser.get_returning_as_bool()
            if returning is None:
                result = build_item_from_value(
                    processor=processor, value=nresult, include_value=False
                )
            else:
                result = build_item_from_value(
                    processor=processor, value=nresult, include_value=True
                )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            items: list = []
            for item in nresult:
                result_item = build_item_from_value(
                    processor=processor, value=item, include_value=True
                )
                converter.normalize_select_fields(
                    op_parser.get_select(), result_item
                )
                items.append(result_item)
            result = build_query_result(items)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            for item in nresult:
                result = item
                break
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = []
            for item in nresult:
                if "resourceBody" in item:
                    result.append(
                        build_item_from_value(
                            processor=processor,
                            value=item["resourceBody"],
                        )
                    )
                else:
                    result.append(None)
        # TRANSACT
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            result = []
            op_parsers = op_parser.get_operation_parsers()
            for i in range(0, len(op_parsers)):
                if state is not None:
                    c = state["values"][i]
                result.append(
                    self._convert_nresult(
                        nresult[i], c, op_parsers[i], [collections[i]]
                    )
                )
        return result


class OperationConverter:
    processor: ItemProcessor
    field_prefix: str

    def __init__(self, processor: ItemProcessor, field_prefix: str):
        self.processor = processor
        self.field_prefix = field_prefix

    @staticmethod
    def convert_transact(
        op_parsers: list[StoreOperationParser],
        converters: list[OperationConverter],
    ) -> tuple[dict, dict]:
        ops: list = []
        states: list = []
        pks: list = []
        reads: list = []
        for i in range(0, len(op_parsers)):
            op_parser = op_parsers[i]
            converter = converters[i]
            processor = converters[i].processor
            if op_parser.op_equals(StoreOperation.PUT):
                pk = processor.get_pk_from_value(op_parser.get_value())
                args, state, func = converter.convert_put(
                    op_parser.get_key(),
                    op_parser.get_value(),
                    op_parser.get_where(),
                    op_parser.get_where_exists(),
                    op_parser.get_where_etag(),
                )
                if func == "create_item":
                    ops.append(("create", (args["body"],)))
                elif func == "upsert_item":
                    ops.append(("upsert", (args["body"],)))
                elif func == "replace_item":
                    if "etag" in args:
                        ops.append(
                            (
                                "replace",
                                (args["item"], args["body"]),
                                {"if_match_etag": args["etag"]},
                            )
                        )
                    else:
                        ops.append(("replace", (args["item"], args["body"])))
                elif func == "helper":
                    ops.append(
                        (
                            "replace",
                            (args["id"], args["document"]),
                            {"if_match_etag": None},
                            # etag will be replaced
                        )
                    )
                if func == "helper":
                    reads.append(
                        {
                            "id": args["id"],
                            "pk": args["pk"],
                            "where": args["where"],
                        }
                    )
                else:
                    reads.append(None)
                states.append(state)
            elif op_parser.op_equals(StoreOperation.UPDATE):
                pk = processor.get_pk_from_key(op_parser.get_key())
                args, state = converter.convert_update(
                    op_parser.get_key(),
                    op_parser.get_set(),
                    op_parser.get_where(),
                    op_parser.get_returning_as_bool(),
                )
                if "filter_predicate" in args:
                    ops.append(
                        (
                            "patch",
                            (args["item"], args["patch_operations"]),
                            {"filter_predicate": args["filter_predicate"]},
                        )
                    )
                else:
                    ops.append(
                        ("patch", (args["item"], args["patch_operations"]))
                    )
                states.append(state)
                reads.append(None)
            elif op_parser.op_equals(StoreOperation.DELETE):
                pk = processor.get_pk_from_key(op_parser.get_key())
                args, func = converter.convert_delete(
                    op_parser.get_key(),
                    op_parser.get_where(),
                    op_parser.get_where_etag(),
                )
                if func == "delete_item":
                    if "etag" in args:
                        ops.append(
                            (
                                "delete",
                                (args["item"],),
                                {"if_match_etag": args["etag"]},
                            )
                        )
                    else:
                        ops.append(("delete", (args["item"],)))
                elif func == "helper":
                    ops.append(
                        (
                            "delete",
                            (args["id"],),
                            {"if_match_etag": None},
                            # etag will be replaced
                        )
                    )
                if func == "helper":
                    reads.append(
                        {
                            "id": args["id"],
                            "pk": args["pk"],
                            "where": args["where"],
                        }
                    )
                else:
                    reads.append(None)
                states.append(None)
            if pk not in pks:
                pks.append(pk)
        return {"ops": ops, "reads": reads, "pks": pks}, {"values": states}

    def convert_batch(
        self, op_parsers: list[StoreOperationParser]
    ) -> tuple[dict, dict]:
        ops = []
        states: list[Any] = []
        pks = []
        for op_parser in op_parsers:
            if op_parser.op_equals(StoreOperation.PUT):
                pk = self.processor.get_pk_from_value(op_parser.get_value())
                args, state, func = self.convert_put(
                    op_parser.get_key(),
                    op_parser.get_value(),
                    op_parser.get_where(),
                    op_parser.get_where_exists(),
                    op_parser.get_where_etag(),
                )
                ops.append({"op": StoreOperation.PUT, "args": args})
                states.append(state)
            elif op_parser.op_equals(StoreOperation.DELETE):
                pk = self.processor.get_pk_from_key(op_parser.get_key())
                args, func = self.convert_delete(
                    op_parser.get_key(),
                    op_parser.get_where(),
                    op_parser.get_where_etag(),
                )
                ops.append({"op": StoreOperation.DELETE, "args": args})
                states.append(None)
            if pk not in pks:
                pks.append(pk)
        return {"ops": ops, "pks": pks}, {"values": states}

    def convert_get(self, key: Value) -> dict:
        id, pk = self.processor.get_id_pk_from_key(key=key)
        return {"item": id, "partition_key": pk}

    def convert_put(
        self,
        key: Value,
        value: dict,
        where: Expression | None,
        exists: bool | None,
        etag: Value,
    ) -> tuple[dict, dict | None, str]:
        document = self.processor.add_embed_fields(value, key)
        id, pk = self.processor.get_id_pk_from_value(document)
        args: dict[str, Any] = {"body": document}
        if exists is None and etag is None and where is None:
            func = "upsert_item"
        elif exists is False:
            func = "create_item"
        elif exists is True:
            args = args | {"item": id}
            func = "replace_item"
        elif etag is not None:
            args = args | {
                "item": id,
                "etag": etag,
                "match_condition": MatchConditions.IfNotModified,
            }
            func = "replace_item"
        elif where is not None:
            args = {"id": id, "pk": pk, "document": document, "where": where}
            func = "helper"
        return args, {"value": document}, func

    def convert_update(
        self,
        key: Value,
        set: Update,
        where: Expression | None,
        returning: bool | None,
    ) -> tuple[dict, dict | None]:
        id, pk = self.processor.get_id_pk_from_key(key=key)
        operations = self.convert_update_ops(set)
        args = {
            "item": id,
            "partition_key": pk,
            "patch_operations": operations,
        }
        if where is not None:
            filter = self.convert_update_filter(where=where)
            args = args | {"filter_predicate": filter}
        return args, None

    def convert_delete(
        self,
        key: Value,
        where: Expression | None,
        etag: Value,
    ) -> tuple[dict, str]:
        id, pk = self.processor.get_id_pk_from_key(key=key)
        if etag is not None:
            args = {
                "item": id,
                "partition_key": pk,
                "etag": etag,
                "match_condition": MatchConditions.IfNotModified,
            }
            func = "delete_item"
        elif where is not None:
            args = {"id": id, "pk": pk, "where": where}
            func = "helper"
        else:
            args = {"item": id, "partition_key": pk}
            func = "delete_item"
        return args, func

    def convert_query(
        self,
        select: Select | None = None,
        where: Expression | None = None,
        order_by: OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict:
        str = f"""SELECT {self.convert_select(
            select)} FROM {self.field_prefix}"""
        if where is not None:
            str = f"{str} WHERE {self.convert_expr(where)}"
        if order_by is not None:
            str = f"""{str} ORDER BY {self.convert_order_by(
                order_by)}"""
        if limit is not None:
            if offset is not None:
                str = f"""{str} OFFSET {offset} LIMIT {limit}"""
            else:
                str = f"""{str} OFFSET 0 LIMIT {limit}"""
        return {"query": str}

    def convert_count(self, where: Expression | None = None) -> dict:
        str = f"SELECT VALUE COUNT(1) FROM {self.field_prefix}"
        if where is not None:
            str = f"{str} WHERE {self.convert_expr(where)}"
        return {"query": str}

    def normalize_select_fields(
        self, select: Select | None, item: DocumentItem
    ) -> None:
        if select is None or len(select.terms) == 0:
            return
        for term in select.terms:
            if "." in term.field and term.alias is None:
                end_path = term.field.split(".")[-1]
                DataAccessor.update_field(
                    item.value if item.value is not None else dict(),
                    end_path,
                    UpdateOp.MOVE,
                    term.field,
                )

    def convert_expr(self, expr: Expression | None) -> str:
        if expr is None or isinstance(
            expr, (str, int, float, bool, dict, list)
        ):
            return json.dumps(expr)
        if isinstance(expr, Field):
            field = self.processor.resolve_field(expr.path)
            return f"{self.field_prefix}.{field}"
        if isinstance(expr, Function):
            return self.convert_func(expr)
        if isinstance(expr, Comparison):
            lhs = f"""{self.convert_expr(
                    expr.lexpr)}"""
            op = expr.op.value
            rhs = None
            if expr.op == ComparisonOp.BETWEEN and isinstance(
                expr.rexpr, list
            ):
                return f"""{lhs} >= {self.convert_expr(
                    expr.rexpr[0])} AND {lhs} <= {self.convert_expr(
                        expr.rexpr[1])}"""
            elif (
                expr.op == ComparisonOp.IN or expr.op == ComparisonOp.NIN
            ) and isinstance(expr.rexpr, list):
                lst = ", ".join(self.convert_expr(i) for i in expr.rexpr)
                rhs = f"({lst})"
            else:
                rhs = self.convert_expr(expr.rexpr)
            return f"""{lhs} {op} {rhs}"""
        if isinstance(expr, And):
            return f"""({self.convert_expr(
                expr.lexpr)} AND {self.convert_expr(
                    expr.rexpr)})"""
        if isinstance(expr, Or):
            return f"""({self.convert_expr(
                expr.lexpr)} OR {self.convert_expr(
                    expr.rexpr)})"""
        if isinstance(expr, Not):
            return f"""NOT {self.convert_expr(
                expr.expr)}"""
        return str(expr)

    def convert_func(self, expr: Function) -> str:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_TYPE:
                field = self.convert_expr(args[0])
                field_type = args[1]
                if isinstance(field_type, str):
                    field_type = DocumentFieldType(field_type.lower())
                if field_type == DocumentFieldType.STRING:
                    return f"IS_STRING({field})"
                elif field_type == DocumentFieldType.NUMBER:
                    return f"IS_NUMBER({field})"
                elif field_type == DocumentFieldType.BOOLEAN:
                    return f"IS_BOOL({field})"
                elif field_type == DocumentFieldType.OBJECT:
                    return f"IS_OBJECT({field})"
                elif field_type == DocumentFieldType.ARRAY:
                    return f"IS_ARRAY({field})"
                elif field_type == DocumentFieldType.NULL:
                    return f"IS_NULL({field})"
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(args[0])
                return f"IS_DEFINED({field})"
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(args[0])
                return f"(NOT IS_DEFINED({field}))"
            if name == QueryFunctionName.LENGTH:
                field = self.convert_expr(args[0])
                return f"LENGTH({field})"
            if name == QueryFunctionName.CONTAINS:
                field = self.convert_expr(args[0])
                value = self.convert_expr(args[1])
                return f"CONTAINS({field}, {value})"
            if name == QueryFunctionName.STARTS_WITH:
                field = self.convert_expr(args[0])
                value = self.convert_expr(args[1])
                return f"STARTSWITH({field}, {value})"
            if name == QueryFunctionName.ARRAY_LENGTH:
                field = self.convert_expr(args[0])
                return f"ARRAY_LENGTH({field})"
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(args[0])
                value = self.convert_expr(args[1])
                return f"ARRAY_CONTAINS({field}, {value})"
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(args[0])
                clauses = []
                for item in args[1]:
                    value = self.convert_expr(item)
                    clauses.append(f"ARRAY_CONTAINS({field}, {value})")
                return f"({str.join(' OR ', clauses)})"
        raise BadRequestError(f"Function {name} not supported")

    def convert_order_by(self, order_by: OrderBy) -> str:
        str_terms = []
        for term in order_by.terms:
            _str_term = self.convert_expr(Field(path=term.field))
            if term.direction is not None:
                _str_term = f"{_str_term} {term.direction.value}"
            str_terms.append(_str_term)
        return ", ".join([t for t in str_terms])

    def convert_select(self, select: Select | None) -> str:
        if select is None or len(select.terms) == 0:
            return "*"
        str_terms = []
        for term in select.terms:
            _str_term = self.convert_expr(Field(path=term.field))
            if term.alias is not None:
                _str_term = f"{_str_term} AS {term.alias}"
            str_terms.append(_str_term)
        return ", ".join([t for t in str_terms])

    def convert_update_filter(self, where: Expression | None) -> str:
        return f"""FROM {self.field_prefix} WHERE {self.convert_expr(
            where)}"""

    def convert_update_ops(self, update: Update) -> list[dict]:
        def _convert_update_field(field: str) -> str:
            path = "/" + (
                field.replace("[", "/")
                .replace("]", "")
                .replace(".", "/")
                .rstrip("/")
            )
            return path

        operations = []
        for operation in update.operations:
            field = self.processor.resolve_field(operation.field)
            field = _convert_update_field(field)
            if operation.op == UpdateOp.PUT:
                operations.append(
                    {"op": "set", "path": field, "value": operation.args[0]}
                )
            elif operation.op == UpdateOp.INSERT:
                operations.append(
                    {"op": "add", "path": field, "value": operation.args[0]}
                )
            elif operation.op == UpdateOp.DELETE:
                operations.append(
                    {
                        "op": "remove",
                        "path": field,
                    }
                )
            elif operation.op == UpdateOp.INCREMENT:
                operations.append(
                    {
                        "op": "incr",
                        "path": field,
                        "value": operation.args[0],
                    }
                )
            elif operation.op == UpdateOp.MOVE:
                move_field = self.processor.resolve_field(
                    operation.args[0].path
                )
                path = _convert_update_field(move_field)
                operations.append(
                    {
                        "op": "move",
                        "from": field,
                        "path": path,
                    }
                )
            else:
                raise BadRequestError("Update operation not supported")
        return operations


class ArgsParser:
    @staticmethod
    def parse_create_database_config(nconfig: Any) -> Any:
        if nconfig is None:
            return None
        if "database" in nconfig:
            if "offer_throughput" in nconfig["database"]:
                if isinstance(nconfig["database"]["offer_throughput"], dict):
                    nconfig["database"]["offer_throughput"] = (
                        ThroughputProperties(
                            **nconfig["database"]["offer_throughput"]
                        )
                    )
            return nconfig["database"]
        return None

    @staticmethod
    def parse_create_container_config(nconfig: Any) -> Any:
        if nconfig is None:
            return None
        if "container" in nconfig:
            if "offer_throughput" in nconfig["container"]:
                if isinstance(nconfig["container"]["offer_throughput"], dict):
                    nconfig["container"]["offer_throughput"] = (
                        ThroughputProperties(
                            **nconfig["container"]["offer_throughput"]
                        )
                    )
            return nconfig["container"]
        return None


class IndexHelper:
    @staticmethod
    def create_container_metadata(
        indexes: list[Index],
    ) -> tuple[dict, list[IndexResult]]:
        metadata: dict = {"indexingPolicy": {}}
        index_results = []
        for index in indexes:
            metadata, index_result = IndexHelper.add_index(metadata, index)
            index_results.append(index_result)
        return metadata, index_results

    @staticmethod
    def add_index(
        container_metadata: dict, index: Index
    ) -> tuple[dict, IndexResult]:
        indexing_policy = container_metadata["indexingPolicy"]
        added = False
        result = IndexResult(status=IndexStatus.CREATED)
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
            path = IndexHelper._convert_to_index_path(index.field)
            added = IndexHelper._add_path(
                indexing_policy, "includedPaths", path
            )
        elif isinstance(index, ExcludeIndex):
            path = IndexHelper._convert_to_index_path(index.field)
            added = IndexHelper._add_path(
                indexing_policy, "excludedPaths", path
            )
        elif isinstance(index, WildcardIndex):
            path = IndexHelper._convert_to_index_path(index.field)
            added = IndexHelper._add_path(
                indexing_policy, "includedPaths", path
            )
            for excluded_path in index.excluded:
                path = IndexHelper._convert_to_index_path(excluded_path)
                added = IndexHelper._add_path(
                    indexing_policy, "excludedPaths", path
                )
        elif isinstance(index, CompositeIndex):
            composite_index = IndexHelper._convert_composite_index(
                index.fields
            )
            added = IndexHelper._add_composite_index(
                indexing_policy, composite_index
            )
        elif isinstance(index, GeospatialIndex):
            path, type = IndexHelper._convert_spatial_index(
                index.field, index.field_type
            )
            added = IndexHelper._add_spatial_index(indexing_policy, path, type)
        elif isinstance(index, VectorIndex) and index.field is not None:
            path, type = IndexHelper._convert_vector_index(
                index.field, index.structure
            )
            added = IndexHelper._add_vector_index(indexing_policy, path, type)
        elif isinstance(index, TTLIndex):
            container_metadata["defaultTtl"] = -1
            added = True
        else:
            result = IndexResult(status=IndexStatus.NOT_SUPPORTED)
        # use it later
        if not added:
            result = IndexResult(status=IndexStatus.EXISTS, index=index)
        return container_metadata, result

    @staticmethod
    def remove_index(
        container_metadata: dict, index: Index
    ) -> tuple[dict, IndexResult]:
        indexing_policy = container_metadata["indexingPolicy"]
        removed = False
        result = IndexResult(status=IndexStatus.DROPPED)
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
            path = IndexHelper._convert_to_index_path(index.field)
            removed = IndexHelper._remove_path(
                indexing_policy, "includedPaths", path
            )
        elif isinstance(index, ExcludeIndex):
            path = IndexHelper._convert_to_index_path(index.field)
            IndexHelper._remove_path(indexing_policy, "excludedPaths", path)
        elif isinstance(index, WildcardIndex):
            path = IndexHelper._convert_to_index_path(index.field)
            IndexHelper._remove_path(indexing_policy, "includedPaths", path)
            for excluded_path in index.excluded:
                path = IndexHelper._convert_to_index_path(excluded_path)
                IndexHelper._remove_path(
                    indexing_policy, "excludedPaths", path
                )
            removed = True
        elif isinstance(index, CompositeIndex):
            composite_index = IndexHelper._convert_composite_index(
                index.fields
            )
            removed = IndexHelper._remove_composite_index(
                indexing_policy, composite_index
            )
        elif isinstance(index, GeospatialIndex):
            path, type = IndexHelper._convert_spatial_index(
                index.field, index.field_type
            )
            removed = IndexHelper._remove_spatial_index(
                indexing_policy, path, type
            )
        elif isinstance(index, VectorIndex) and index.field is not None:
            path, type = IndexHelper._convert_vector_index(
                index.field, index.structure
            )
            removed = IndexHelper._remove_vector_index(
                indexing_policy, path, type
            )
        elif isinstance(index, TTLIndex):
            container_metadata["defaultTtl"] = None
            removed = True
        else:
            result = IndexResult(status=IndexStatus.NOT_EXISTS)
        # use it later
        if not removed:
            result = IndexResult(status=IndexStatus.NOT_EXISTS)
        return container_metadata, result

    @staticmethod
    def list_indexes(container_metadata: dict) -> list[Index]:
        indexing_policy = container_metadata["indexingPolicy"]
        indexes: list[Index] = []
        indexes.extend(IndexHelper._list_field_indexes(indexing_policy))
        wildcard_indexes = IndexHelper._list_wildcard_indexes(indexing_policy)
        indexes.extend(wildcard_indexes)
        excluded = []
        for idx in wildcard_indexes:
            excluded.extend(idx.excluded)
        indexes.extend(
            IndexHelper._list_exclude_indexes(indexing_policy, excluded)
        )
        indexes.extend(IndexHelper._list_composite_indexes(indexing_policy))
        indexes.extend(IndexHelper._list_spatial_indexes(indexing_policy))
        indexes.extend(IndexHelper._list_vector_indexes(indexing_policy))
        if (
            "defaultTtl" in container_metadata
            and container_metadata["defaultTtl"] is not None
        ):
            indexes.append(TTLIndex(field="_ts"))
        return indexes

    @staticmethod
    def _convert_vector_index(
        field: str, structure: VectorIndexStructure | None
    ) -> tuple[str, str]:
        path = IndexHelper._convert_to_index_path(field, scalar=False)
        type = "flat"
        if structure == VectorIndexStructure.FLAT:
            type = "flat"
        elif structure == VectorIndexStructure.QUANTIZED_FLAT:
            type = "quantizedFlat"
        elif structure == VectorIndexStructure.DISKANN:
            type = "diskann"
        elif structure == VectorIndexStructure.HNSW:
            type = "diskann"
        return path, type

    @staticmethod
    def _add_vector_index(indexing_policy: dict, path: str, type: str) -> bool:
        set = "vectorIndexes"
        if set not in indexing_policy:
            indexing_policy[set] = []
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                return False
        indexing_policy[set].append({"path": path, "type": type})
        return True

    @staticmethod
    def _remove_vector_index(
        indexing_policy: dict, path: str, type: str
    ) -> bool:
        set = "vectorIndexes"
        if set not in indexing_policy:
            return False
        found = False
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                found = True
        if found:
            indexing_policy[set].remove({"path": path, "type": type})
        return found

    @staticmethod
    def _list_vector_indexes(indexing_policy: dict) -> list[VectorIndex]:
        set = "vectorIndexes"
        if set not in indexing_policy:
            return []
        indexes = []
        type_mapping = {
            "flat": VectorIndexStructure.FLAT,
            "quantizedFlat": VectorIndexStructure.QUANTIZED_FLAT,
            "diskann": VectorIndexStructure.DISKANN,
        }
        for idx in indexing_policy[set]:
            indexes.append(
                VectorIndex(
                    field=IndexHelper._convert_to_field(idx["path"]),
                    structure=type_mapping[idx["type"]],
                    dimension=0,
                )
            )
        return indexes

    @staticmethod
    def _convert_spatial_index(
        field: str, field_type: str | None
    ) -> tuple[str, str]:
        path = IndexHelper._convert_to_index_path(field, scalar=False)
        path = path + "/*"
        if field_type == GeospatialFieldType.POINT or field_type is None:
            type = "Point"
        elif field == GeospatialFieldType.SHAPE:
            type = "Polygon"
        elif field == "line_string":
            type = "LineString"
        else:
            type = "Point"
        return path, type

    @staticmethod
    def _add_spatial_index(
        indexing_policy: dict, path: str, type: str
    ) -> bool:
        set = "spatialIndexes"
        if set not in indexing_policy:
            indexing_policy[set] = []
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                if type in idx["types"]:
                    return False
                idx["types"].append(type)
                return True
        indexing_policy[set].append({"path": path, "types": [type]})
        return True

    @staticmethod
    def _remove_spatial_index(
        indexing_policy: dict, path: str, type: str
    ) -> bool:
        set = "spatialIndexes"
        if set not in indexing_policy:
            return False
        found = False
        delete = False
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                if type in idx["types"]:
                    idx["types"].remove(type)
                    found = True
                    if len(idx["types"]) == 0:
                        delete = True
        if delete:
            indexing_policy[set].remove({"path": path, "types": []})
        return found

    @staticmethod
    def _list_spatial_indexes(indexing_policy: dict) -> list[GeospatialIndex]:
        set = "spatialIndexes"
        if set not in indexing_policy:
            return []
        indexes = []
        """
        Cosmos DB seems to always create indexes for all types
        (Point, Polygon, LineString, MuliPolygon).
        So, we don't parse the field type.
        field_type_mapping = {
            "Point": GeospatialFieldType.POINT,
            "Polygon": GeospatialFieldType.SHAPE,
            "LineString": "line_string",
        }"""
        for idx in indexing_policy[set]:
            indexes.append(
                GeospatialIndex(
                    field=IndexHelper._convert_to_field(idx["path"]),
                )
            )
        return indexes

    @staticmethod
    def _convert_to_index_path(field: str, scalar: bool = True) -> str:
        path = "/" + field.replace(".", "/")
        if scalar:
            if not path.endswith("*") and not path.endswith("[]"):
                path = path + "/?"
        return path

    @staticmethod
    def _convert_to_field(path: str) -> str:
        return (
            path.replace("/", ".")
            .replace("?", "")
            .strip(".")
            .replace(".*", "")
        )

    @staticmethod
    def _add_path(indexing_policy: dict, set: str, path: str) -> bool:
        if set not in indexing_policy:
            indexing_policy[set] = []
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                return False
        indexing_policy[set].append({"path": path})
        return True

    @staticmethod
    def _remove_path(indexing_policy: dict, set: str, path: str) -> bool:
        if set not in indexing_policy:
            return False
        found = False
        for idx in indexing_policy[set]:
            if idx["path"] == path:
                found = True
        if found:
            indexing_policy[set].remove({"path": path})
        return found

    @staticmethod
    def _list_field_indexes(
        indexing_policy: dict,
    ) -> list[FieldIndex]:
        set = "includedPaths"
        if set not in indexing_policy:
            return []
        indexes = []
        for idx in indexing_policy[set]:
            if not idx["path"].endswith("*"):
                indexes.append(
                    FieldIndex(
                        field=IndexHelper._convert_to_field(idx["path"])
                    )
                )
        return indexes

    @staticmethod
    def _list_exclude_indexes(
        indexing_policy: dict,
        already_excluded: list[str],
    ) -> list[ExcludeIndex]:
        set = "excludedPaths"
        if set not in indexing_policy:
            return []
        indexes = []
        for idx in indexing_policy[set]:
            field = IndexHelper._convert_to_field(idx["path"])
            if field not in already_excluded:
                indexes.append(ExcludeIndex(field=field))
        return indexes

    @staticmethod
    def _list_wildcard_indexes(indexing_policy: dict) -> list[WildcardIndex]:
        set = "includedPaths"
        if set not in indexing_policy:
            return []
        indexes = []
        for idx in indexing_policy[set]:
            if idx["path"].endswith("*"):
                path = IndexHelper._convert_to_field(idx["path"])
                excluded = []
                if "excludedPaths" in indexing_policy:
                    for eidx in indexing_policy["excludedPaths"]:
                        epath = IndexHelper._convert_to_field(eidx["path"])
                        # NOTE: Verify whether this exclusion is needed.
                        if epath in ['"_etag"']:
                            continue
                        if epath.startswith(path.replace("*", "")):
                            excluded.append(epath)
                indexes.append(WildcardIndex(field=path, excluded=excluded))
        return indexes

    @staticmethod
    def _convert_composite_index(fields: list) -> list:
        index = []
        for field_index in fields:
            path = IndexHelper._convert_to_index_path(
                field_index.field, scalar=False
            )
            if isinstance(field_index, DescIndex):
                index.append({"path": path, "order": "descending"})
            else:
                index.append({"path": path, "order": "ascending"})
        return index

    @staticmethod
    def _add_composite_index(indexing_policy: dict, index: Any) -> bool:
        set = "compositeIndexes"
        if set not in indexing_policy:
            indexing_policy[set] = []
        for idx in indexing_policy[set]:
            if idx == index:
                return False
        indexing_policy[set].append(index)
        return True

    @staticmethod
    def _remove_composite_index(indexing_policy: dict, index: Any) -> bool:
        set = "compositeIndexes"
        if set not in indexing_policy:
            return False
        found = False
        for idx in indexing_policy[set]:
            if idx == index:
                found = True
                break
        if found:
            indexing_policy[set].remove(index)
            if len(indexing_policy[set]) == 0:
                indexing_policy.pop(set)
        return found

    @staticmethod
    def _list_composite_indexes(
        indexing_policy: dict,
    ) -> list[Index]:
        set = "compositeIndexes"
        if set not in indexing_policy:
            return []
        indexes: list[Index] = []
        for idx in indexing_policy[set]:
            field_indexes: list = []
            for field_index in idx:
                if field_index["order"] == "descending":
                    field_indexes.append(
                        DescIndex(
                            field=IndexHelper._convert_to_field(
                                field_index["path"]
                            )
                        )
                    )
                else:
                    field_indexes.append(
                        AscIndex(
                            field=IndexHelper._convert_to_field(
                                field_index["path"]
                            )
                        )
                    )
            indexes.append(CompositeIndex(fields=field_indexes))
        return indexes


class ResourceHelper:
    document_client: Any
    database_client: Any
    credential: Any

    def __init__(self, document_client, database_client, credential: Any):
        self.document_client = document_client
        self.database_client = database_client
        self.credential = credential

    def create_collection(
        self,
        database: str,
        container: str,
        config: DocumentCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        pk_field = (
            config.pk_field
            if config is not None and config.pk_field is not None
            else "pk"
        )
        nconfig = config.nconfig if config is not None else None
        database_nargs = ArgsParser.parse_create_database_config(nconfig)
        database_client = NCall(
            self.document_client.create_database_if_not_exists,
            {"id": database},
            database_nargs,
        ).invoke()
        container_nargs = ArgsParser.parse_create_container_config(nconfig)
        pk = PartitionKey(path=f"/{pk_field}")
        args: dict = {"id": container, "partition_key": pk}
        index_results: list = []
        if (
            config is not None
            and config.indexes is not None
            and len(config.indexes) > 0
        ):
            metadata, index_results = IndexHelper.create_container_metadata(
                config.indexes
            )
            args["indexing_policy"] = metadata["indexingPolicy"]
            args["default_ttl"] = (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            )
        status = CollectionStatus.CREATED
        try:
            NCall(
                database_client.create_container,
                args,
                container_nargs,
            ).invoke()
        except CosmosResourceExistsError:
            if exists is False:
                raise ConflictError
            status = CollectionStatus.EXISTS
        return CollectionResult(status=status, indexes=index_results)

    def drop_collection(
        self, container: str, exists: bool | None, nargs: Any
    ) -> CollectionResult:
        try:
            NCall(
                self.database_client.delete_container,
                {"container": container},
                nargs,
            ).invoke()
        except CosmosResourceNotFoundError:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        nresult = []
        response = NCall(
            self.database_client.list_containers, None, nargs
        ).invoke()
        for item in response:
            nresult.append(item)
        return nresult

    def has_collection(self, container: str, nargs: Any):
        container_client = self.database_client.get_container_client(container)
        try:
            NCall(
                container_client.read,
                None,
                nargs,
            ).invoke()
            return True
        except CosmosResourceNotFoundError:
            return False

    def create_index(
        self, client: Any, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        metadata = NCall(client.read, None).invoke()
        indexes = IndexHelper.list_indexes(metadata)
        status, match_index = BaseIndexHelper.check_index_status(
            indexes, index
        )
        if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
            if exists is False:
                raise ConflictError
            return IndexResult(status=status, index=match_index)

        metadata, index_result = IndexHelper.add_index(metadata, index)
        args = {
            "container": client,
            "partition_key": metadata["partitionKey"],
            "indexing_policy": metadata["indexingPolicy"],
            "default_ttl": (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            ),
        }
        NCall(
            self.database_client.replace_container,
            args,
            nargs,
        ).invoke()
        return index_result

    def drop_index(
        self, client: Any, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        metadata = NCall(client.read, None).invoke()
        indexes = IndexHelper.list_indexes(metadata)
        if not BaseIndexHelper.match_index(indexes, index):
            if exists is True:
                raise NotFoundError
            return IndexResult(status=IndexStatus.NOT_EXISTS)

        metadata, index_result = IndexHelper.remove_index(metadata, index)
        args = {
            "container": client,
            "partition_key": metadata["partitionKey"],
            "indexing_policy": metadata["indexingPolicy"],
            "default_ttl": (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            ),
        }
        NCall(
            self.database_client.replace_container,
            args,
            nargs,
        ).invoke()
        return index_result

    def list_indexes(self, client: Any, nargs: Any) -> Any:
        metadata = NCall(client.read, None).invoke()
        return IndexHelper.list_indexes(metadata)

    def close(self, nargs: Any) -> Any:
        pass


class AsyncResourceHelper:
    document_client: Any
    database_client: Any
    credential: Any

    def __init__(self, document_client, database_client, credential: Any):
        self.document_client = document_client
        self.database_client = database_client
        self.credential = credential

    async def create_collection(
        self,
        database: str,
        container: str,
        config: DocumentCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        pk_field = (
            config.pk_field
            if config is not None and config.pk_field is not None
            else "pk"
        )
        nconfig = config.nconfig if config is not None else None
        database_nargs = ArgsParser.parse_create_database_config(nconfig)
        database_client = await NCall(
            self.document_client.create_database_if_not_exists,
            {"id": database},
            database_nargs,
        ).ainvoke()
        container_nargs = ArgsParser.parse_create_container_config(nconfig)
        pk = PartitionKey(path=f"/{pk_field}")
        args: dict = {"id": container, "partition_key": pk}
        index_results: list = []
        if (
            config is not None
            and config.indexes is not None
            and len(config.indexes) > 0
        ):
            metadata, index_results = IndexHelper.create_container_metadata(
                config.indexes
            )
            args["indexing_policy"] = metadata["indexingPolicy"]
            args["default_ttl"] = (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            )
        status = CollectionStatus.CREATED
        try:
            await NCall(
                database_client.create_container,
                args,
                container_nargs,
            ).ainvoke()
        except CosmosResourceExistsError:
            if exists is False:
                raise ConflictError
            status = CollectionStatus.EXISTS
        return CollectionResult(status=status, indexes=index_results)

    async def drop_collection(
        self,
        container: str,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        try:
            await NCall(
                self.database_client.delete_container,
                {"container": container},
                nargs,
            ).ainvoke()
        except CosmosResourceNotFoundError:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    async def list_collections(self, nargs) -> Any:
        nresult = []
        response = NCall(
            self.database_client.list_containers, None, nargs
        ).invoke()
        async for item in response:
            nresult.append(item)
        return nresult

    async def has_collection(self, container: str, nargs: Any):
        container_client = self.database_client.get_container_client(container)
        try:
            await NCall(
                container_client.read,
                None,
                nargs,
            ).ainvoke()
            return True
        except CosmosResourceNotFoundError:
            return False

    async def create_index(
        self, client: Any, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        metadata = await NCall(client.read, None).ainvoke()
        indexes = IndexHelper.list_indexes(metadata)
        status, match_index = BaseIndexHelper.check_index_status(
            indexes, index
        )
        if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
            if exists is False:
                raise ConflictError
            return IndexResult(status=status, index=match_index)

        metadata, index_result = IndexHelper.add_index(metadata, index)
        args = {
            "container": client,
            "partition_key": metadata["partitionKey"],
            "indexing_policy": metadata["indexingPolicy"],
            "default_ttl": (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            ),
        }
        await NCall(
            self.database_client.replace_container,
            args,
            nargs,
        ).ainvoke()
        return index_result

    async def drop_index(
        self, client: Any, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        metadata = await NCall(client.read, None).ainvoke()
        indexes = IndexHelper.list_indexes(metadata)
        if not BaseIndexHelper.match_index(indexes, index):
            if exists is True:
                raise NotFoundError
            return IndexResult(status=IndexStatus.NOT_EXISTS)

        metadata, index_result = IndexHelper.remove_index(metadata, index)
        args = {
            "container": client,
            "partition_key": metadata["partitionKey"],
            "indexing_policy": metadata["indexingPolicy"],
            "default_ttl": (
                metadata["defaultTtl"] if "defaultTtl" in metadata else None
            ),
        }
        await NCall(
            self.database_client.replace_container,
            args,
            nargs,
        ).ainvoke()
        return index_result

    async def list_indexes(self, client: Any, nargs: Any) -> Any:
        metadata = await NCall(client.read, None).ainvoke()
        return IndexHelper.list_indexes(metadata)

    async def close(self, nargs: Any):
        await NCall(
            self.document_client.close,
            None,
            nargs,
        ).ainvoke()
        if self.credential is not None:
            await NCall(
                self.credential.close,
                None,
                nargs,
            ).ainvoke()


class ClientHelper:
    client: Any
    processor: ItemProcessor

    def __init__(self, client: Any, processor: ItemProcessor):
        self.client = client
        self.processor = processor

    def put(
        self,
        id: DocumentKeyType,
        pk: DocumentKeyType,
        document: dict,
        where: Any,
        nargs: Any,
    ) -> Any:
        response = NCall(
            self.client.read_item,
            {"item": id, "partition_key": pk},
            nargs,
        ).invoke()
        eval_result = QueryProcessor.eval_expr(
            response, where, self.processor.resolve_field
        )

        if not bool(eval_result):
            raise PreconditionFailedError
        return NCall(
            self.client.replace_item,
            {
                "body": document,
                "item": id,
                "etag": self.processor.get_etag_from_value(response),
                "match_condition": MatchConditions.IfNotModified,
            },
            nargs,
        ).invoke()

    def delete(
        self, id: DocumentKeyType, pk: DocumentKeyType, where: Any, nargs: Any
    ) -> Any:
        response = NCall(
            self.client.read_item,
            {"item": id, "partition_key": pk},
            nargs,
        ).invoke()
        eval_result = QueryProcessor.eval_expr(
            response, where, self.processor.resolve_field
        )
        if not bool(eval_result):
            raise PreconditionFailedError
        return NCall(
            self.client.delete_item,
            {
                "item": id,
                "partition_key": pk,
                "etag": self.processor.get_etag_from_value(response),
                "match_condition": MatchConditions.IfNotModified,
            },
            nargs,
        ).invoke()

    def query(self, query, nargs) -> Any:
        nresult = NCall(
            self.client.query_items,
            {"query": query, "enable_cross_partition_query": True},
            nargs,
        ).invoke()
        return nresult

    def batch(self, ops: list[dict], pks: list[str], nargs: Any) -> Any:
        nresult = []
        batch_ops = []
        if len(pks) == 1:
            for op in ops:
                if op["op"] == StoreOperation.PUT:
                    batch_ops.append(("upsert", (op["args"]["body"],)))
                elif op["op"] == StoreOperation.DELETE:
                    batch_ops.append(("delete", (op["args"]["item"],)))
            return NCall(
                self.client.execute_item_batch,
                {"batch_operations": batch_ops, "partition_key": pks[0]},
                nargs,
            ).invoke()
        else:
            for op in ops:
                if op["op"] == StoreOperation.PUT:
                    result = NCall(
                        self.client.upsert_item, None, op["args"]
                    ).invoke()
                    nresult.append({"resourceBody": result})
                elif op["op"] == StoreOperation.DELETE:
                    NCall(self.client.delete_item, None, op["args"]).invoke()
                    nresult.append({})
        return nresult

    def transact(self, ops: list, reads: list, pks: list) -> Any:
        if len(pks) > 1:
            raise BadRequestError("All operations should have the same pk")
        for i in range(0, len(reads)):
            read = reads[i]
            op = ops[i]
            if read is not None:
                response = NCall(
                    self.client.read_item,
                    {"item": read["id"], "partition_key": read["pk"]},
                ).invoke()
                if not QueryProcessor.eval_expr(
                    response, read["where"], self.processor.resolve_field
                ):
                    raise ConflictError
                # replace the etag
                op[2]["if_match_etag"] = self.processor.get_etag_from_value(
                    response
                )
        response = NCall(
            self.client.execute_item_batch,
            {"batch_operations": ops, "partition_key": pks[0]},
            None,
            {CosmosBatchOperationError: ConflictError},
        ).invoke()
        nresult = [
            r["resourceBody"] if "resourceBody" in r else None
            for r in response
        ]
        return nresult


class AsyncClientHelper:
    client: Any
    processor: ItemProcessor
    document_client: Any

    def __init__(
        self,
        client: Any,
        processor: ItemProcessor,
    ):
        self.client = client
        self.processor = processor

    async def put(
        self,
        id: DocumentKeyType,
        pk: DocumentKeyType,
        document: dict,
        where: Any,
        nargs: Any,
    ) -> Any:
        response = await NCall(
            self.client.read_item,
            {"item": id, "partition_key": pk},
            nargs,
        ).ainvoke()
        eval_result = QueryProcessor.eval_expr(
            response, where, self.processor.resolve_field
        )
        if not bool(eval_result):
            raise PreconditionFailedError
        return await NCall(
            self.client.replace_item,
            {
                "body": document,
                "item": id,
                "etag": self.processor.get_etag_from_value(response),
                "match_condition": MatchConditions.IfNotModified,
            },
            nargs,
        ).ainvoke()

    async def delete(
        self,
        id: DocumentKeyType,
        pk: DocumentKeyType,
        where: Any,
        nargs: Any,
    ) -> Any:
        response = await NCall(
            self.client.read_item,
            {"item": id, "partition_key": pk},
            nargs,
        ).ainvoke()
        eval_result = QueryProcessor.eval_expr(
            response, where, self.processor.resolve_field
        )
        if not bool(eval_result):
            raise PreconditionFailedError
        return await NCall(
            self.client.delete_item,
            {
                "item": id,
                "partition_key": pk,
                "etag": self.processor.get_etag_from_value(response),
                "match_condition": MatchConditions.IfNotModified,
            },
            nargs,
        ).ainvoke()

    async def query(self, query, nargs) -> Any:
        response = NCall(
            self.client.query_items, {"query": query}, nargs
        ).invoke()
        nresult = []
        async for item in response:
            nresult.append(item)
        return nresult

    async def batch(self, ops: list[dict], pks: list[str], nargs: Any) -> Any:
        nresult = []
        batch_ops = []
        if len(pks) == 1:
            for op in ops:
                if op["op"] == StoreOperation.PUT:
                    batch_ops.append(("upsert", (op["args"]["body"],)))
                elif op["op"] == StoreOperation.DELETE:
                    batch_ops.append(("delete", (op["args"]["item"],)))
            return await NCall(
                self.client.execute_item_batch,
                {"batch_operations": batch_ops, "partition_key": pks[0]},
                nargs,
            ).ainvoke()
        else:
            for op in ops:
                if op["op"] == StoreOperation.PUT:
                    result = await NCall(
                        self.client.upsert_item, None, op["args"]
                    ).ainvoke()
                    nresult.append({"resourceBody": result})
                elif op["op"] == StoreOperation.DELETE:
                    await NCall(
                        self.client.delete_item, None, op["args"]
                    ).ainvoke()
                    nresult.append({})
        return nresult

    async def transact(
        self, ops: list, reads: list[dict | None], pks: list[str]
    ) -> Any:
        if len(pks) > 1:
            raise BadRequestError("All operations should have the same pk")
        for i in range(0, len(reads)):
            read = reads[i]
            op = ops[i]
            if read is not None:
                response = await NCall(
                    self.client.read_item,
                    {"item": read["id"], "partition_key": read["pk"]},
                ).ainvoke()
                if not QueryProcessor.eval_expr(
                    response, read["where"], self.processor.resolve_field
                ):
                    raise ConflictError
                # replace the etag
                op[2]["if_match_etag"] = self.processor.get_etag_from_value(
                    response
                )
        response = await NCall(
            self.client.execute_item_batch,
            {"batch_operations": ops, "partition_key": pks[0]},
            None,
            {CosmosBatchOperationError: ConflictError},
        ).ainvoke()
        nresult = [
            r["resourceBody"] if "resourceBody" in r else None
            for r in response
        ]
        return nresult


class CosmosDBCollection:
    client: Any
    converter: OperationConverter
    processor: ItemProcessor
    helper: Any

    def __init__(
        self,
        client: Any,
        helper_type: Any,
        metadata: dict,
        id_map_field: str | None,
        pk_map_field: str | None,
        suppress_fields: list[str] | None,
    ):
        self.client = client
        pk_embed_field = metadata["partitionKey"]["paths"][0][1:]
        self.processor = ItemProcessor(
            id_embed_field="id",
            pk_embed_field=pk_embed_field,
            etag_embed_field="_etag",
            id_map_field=id_map_field,
            pk_map_field=pk_map_field,
            local_etag=False,
            suppress_fields=suppress_fields,
        )
        self.converter = OperationConverter(
            processor=self.processor, field_prefix="c"
        )
        self.helper = helper_type(client, self.processor)
