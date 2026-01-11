"""
Document Store on Amazon DynamoDB.
"""

from __future__ import annotations

__all__ = ["AmazonDynamoDB"]

import copy
import time
from decimal import Decimal
from typing import Any

import boto3
from boto3.dynamodb.types import Binary
from botocore.exceptions import ClientError

from x8.core import Context, NCall, Operation, Response
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
    FieldIndex,
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
)

from .._helper import (
    build_item_from_parts,
    build_item_from_value,
    build_query_result,
    get_collection_config,
)
from .._models import DocumentCollectionConfig, DocumentFieldType, DocumentItem


class AmazonDynamoDB(StoreProvider):
    collection: str | None
    table: str | None
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None
    etag_embed_field: str | dict | None
    suppress_fields: list[str] | None
    nparams: dict[str, Any]

    _resource: Any
    _collection_cache: dict[str, DynamoDBCollection]

    def __init__(
        self,
        table: str | None = None,
        region: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = "pk",
        etag_embed_field: str | dict | None = "_etag",
        suppress_fields: list[str] | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            table:
                DynamoDB table name mapped to document store collection.
            region:
                AWS region name.
            profile_name:
                AWS profile name.
            aws_access_key_id:
                AWS access key id.
            aws_secret_access_key:
                AWS secret access key.
            aws_session_token:
                AWS session token.
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
            etag_embed_field:
                Field to store the generated ETAG value.
                To specify for multiple collections, use a dictionary
                where the key is the collection name and the value
                is the field.
            suppress_fields:
                List of fields to supress when results are returned.
            nparams:
                Native parameters to boto3 client.
        """
        self.collection = table
        self.table = table
        self.region = region
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.etag_embed_field = etag_embed_field
        self.suppress_fields = suppress_fields
        self.nparams = nparams

        self._resource = None
        self._collection_cache = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._resource is not None:
            return

        resource = None
        if self.profile_name is not None:
            resource = boto3.resource(
                "dynamodb",
                region_name=self.region,
                profile_name=self.profile_name,
                **self.nparams,
            )
        elif (
            self.aws_access_key_id is not None
            and self.aws_secret_access_key is not None
        ):
            resource = boto3.resource(
                "dynamodb",
                region_name=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
                **self.nparams,
            )
        else:
            resource = boto3.resource(
                "dynamodb",
                region_name=self.region,
                **self.nparams,
            )

        self._resource = resource
        self._conditional_check_failed_exception = (
            resource.meta.client.exceptions.ConditionalCheckFailedException
        )

    def _get_table_name(self, op_parser: StoreOperationParser) -> str | None:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        table = (
            collection_name.split(".")[0]
            if collection_name is not None
            else self.table
        )
        table = table or self.__component__.collection
        return table

    def _get_collections(
        self, op_parser: StoreOperationParser
    ) -> list[DynamoDBCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[DynamoDBCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(self._get_collections(single_op_parser))
            return collections
        table = self._get_table_name(op_parser)
        if table is None:
            raise BadRequestError("Collection name must be specified")
        if table in self._collection_cache:
            return [self._collection_cache[table]]
        client = self._resource.Table(table)
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field, table
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field, table
        )
        etag_embed_field = ParameterParser.get_collection_parameter(
            self.etag_embed_field, table
        )
        col = DynamoDBCollection(
            table,
            client,
            ClientHelper,
            id_map_field,
            pk_map_field,
            etag_embed_field,
            self.suppress_fields,
        )
        self._collection_cache[table] = col
        return [col]

    def _validate(self, op_parser: StoreOperationParser):
        if op_parser.op_equals(StoreOperation.BATCH):
            Validator.validate_batch(
                op_parser.get_operation_parsers(),
                allowed_ops=[StoreOperation.PUT, StoreOperation.DELETE],
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
            ResourceHelper(self._resource),
            self._collection_cache,
        )
        if ncall is None:
            return super().__run__(
                operation=operation,
                context=context,
                **kwargs,
            )
        nresult, nerror = ncall.invoke(return_error=True)
        result = self._convert_nresult(
            nresult, nerror, state, op_parser, collections
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collections: list[DynamoDBCollection],
        resource_helper: Any,
        collection_cache: dict,
    ) -> tuple[NCall | None, dict | None]:
        if len(collections) == 1:
            op_converter = collections[0].op_converter
            client = collections[0].client
            helper = collections[0].helper
        call = None
        state = None
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            table_name = self._get_table_name(op_parser)
            args = {
                "table": table_name,
                "config": get_collection_config(op_parser),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.create_collection, args)
            if table_name and table_name in collection_cache:
                del collection_cache[table_name]
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            table_name = self._get_table_name(op_parser)
            args = {
                "table": table_name,
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(resource_helper.drop_collection, args)
            if table_name and table_name in collection_cache:
                del collection_cache[table_name]
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = {"nargs": nargs}
            call = NCall(resource_helper.list_collections, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = {
                "table": self._get_table_name(op_parser),
                "nargs": nargs,
            }
            call = NCall(resource_helper.has_collection, args)
        # CREATE INDEX
        elif op_parser.op_equals(StoreOperation.CREATE_INDEX):
            args = {
                "index": op_parser.get_index(),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(helper.create_index, args)
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            args = {
                "index": op_parser.get_index(),
                "exists": op_parser.get_where_exists(),
                "nargs": nargs,
            }
            call = NCall(helper.drop_index, args)
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            args = {"nargs": nargs}
            call = NCall(helper.list_indexes, args)
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            args = op_converter.convert_get(op_parser.get_key())
            call = NCall(client.get_item, args, nargs)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, state = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where(),
                op_parser.get_where_exists(),
            )
            call = NCall(client.put_item, args, nargs)
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, state = op_converter.convert_update(
                op_parser.get_key(),
                op_parser.get_set(),
                op_parser.get_where(),
                op_parser.get_returning(),
            )
            call = NCall(client.update_item, args, nargs)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = op_converter.convert_delete(
                op_parser.get_key(), op_parser.get_where()
            )
            call = NCall(client.delete_item, args, nargs)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = op_converter.convert_query(
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
                collection=op_parser.get_collection_name(),
            )
            args["nargs"] = nargs
            call = NCall(helper.query, args, None)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = op_converter.convert_count(
                where=op_parser.get_where(),
                collection=op_parser.get_collection_name(),
            )
            args["nargs"] = nargs
            call = NCall(helper.count, args, None)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            args, state = op_converter.convert_batch(
                op_parser.get_operation_parsers()
            )
            call = NCall(helper.batch, args, None)
        # TRANSACT
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            args, state = OperationConverter.convert_transact(
                op_parser.get_operation_parsers(),
                [col.op_converter for col in collections],
            )
            call = NCall(resource_helper.transact, args, None)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(resource_helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        nerror: Any,
        state: dict | None,
        op_parser: StoreOperationParser,
        collections: list[DynamoDBCollection],
    ) -> Any:
        if len(collections) == 1:
            processor = collections[0].processor
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            if nerror is not None:
                raise nerror
            result = nresult
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            if nerror is not None:
                raise nerror
            result = nresult
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            if nerror is not None:
                raise nerror
            result = [t.name for t in nresult]
        # CREATE INDEX
        if op_parser.op_equals(StoreOperation.CREATE_INDEX):
            if nerror is not None:
                raise nerror
            result = nresult
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            if nerror is not None:
                raise nerror
            result = nresult
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            if nerror is not None:
                raise nerror
            result = nresult
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ResourceNotFoundException"
                ):
                    result = False
                else:
                    raise nerror
            else:
                result = True
        # GET
        if op_parser.op_equals(StoreOperation.GET):
            if nerror is not None:
                raise nerror
            if "Item" not in nresult:
                raise NotFoundError
            document = self._convert_to_json_dict(nresult["Item"])
            result = build_item_from_value(
                processor=processor,
                value=document,
                include_value=True,
            )
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            returning = op_parser.get_returning_as_bool()
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ConditionalCheckFailedException"
                ):
                    if "Item" in nerror.response:
                        raise PreconditionFailedError
                    raise PreconditionFailedError
                raise nerror
            value: Any = state["value"] if state is not None else None
            result = build_item_from_value(
                processor=processor,
                value=value,
                include_value=(returning or False),
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            where = op_parser.get_where()
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ConditionalCheckFailedException"
                ):
                    if "Item" in nerror.response:
                        raise PreconditionFailedError
                    if where is None:
                        raise NotFoundError
                    else:
                        raise PreconditionFailedError
                elif (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ValidationException"
                    and "document path" in nerror.response["Error"]["Message"]
                ):
                    if where is None:
                        raise NotFoundError
                    else:
                        raise PreconditionFailedError
                raise nerror
            if "Attributes" in nresult:
                document = self._convert_to_json_dict(nresult["Attributes"])
                result = build_item_from_value(
                    processor=processor, value=document, include_value=True
                )
            else:
                key = op_parser.get_key()
                result = build_item_from_parts(
                    processor=processor,
                    key=key,
                    etag=(
                        state["etag"]
                        if state is not None and "etag" in state
                        else None
                    ),
                )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            where = op_parser.get_where()
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "ConditionalCheckFailedException"
                ):
                    if "Item" in nerror.response:
                        raise PreconditionFailedError
                    if where is None:
                        raise NotFoundError
                    else:
                        raise PreconditionFailedError
                raise nerror
            result = None
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            if nerror is not None:
                raise nerror
            result = build_query_result(
                self._parse_query_response(
                    nresult,
                    processor,
                    op_parser.get_limit(),
                    op_parser.get_offset(),
                )
            )
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            if nerror is not None:
                raise nerror
            result = self._parse_count_response(nresult)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            if nerror is not None:
                raise nerror
            result = []
            if state is not None:
                for st in state["values"]:
                    if st is not None:
                        result.append(
                            build_item_from_value(
                                processor=processor,
                                value=st["value"],
                            )
                        )
                    else:
                        result.append(None)
        # TRANSACT
        elif op_parser.op_equals(StoreOperation.TRANSACT):
            if nerror is not None:
                if (
                    isinstance(nerror, ClientError)
                    and nerror.response["Error"]["Code"]
                    == "TransactionCanceledException"
                ):
                    raise ConflictError
                raise nerror
            result = []
            op_parsers = op_parser.get_operation_parsers()
            for i in range(0, len(op_parsers)):
                if state is not None:
                    st = state["values"][i]
                result.append(
                    self._convert_nresult(
                        {}, None, st, op_parsers[i], [collections[i]]
                    )
                )
        return result

    def _parse_query_response(
        self,
        responses: list,
        processor: ItemProcessor,
        limit: int | None = None,
        offset: int | None = None,
    ) -> list[DocumentItem]:
        documents = []
        for response in responses:
            if "Items" in response:
                for item in response["Items"]:
                    document = self._convert_to_json_dict(item)
                    documents.append(
                        build_item_from_value(
                            processor=processor,
                            value=document,
                            include_value=True,
                        )
                    )

        end = len(documents)
        start = 0
        if limit is not None:
            if offset is not None:
                start = offset
                end = start + limit
            else:
                end = limit
        return documents[start:end]

    def _parse_count_response(self, responses: list) -> int:
        count = 0
        for response in responses:
            if "Count" in response:
                count = response["Count"]
        return count

    def _convert_to_json_dict(self, document: dict) -> dict:
        def convert_types_back(obj):
            if isinstance(obj, dict):
                return {k: convert_types_back(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_types_back(i) for i in obj]
            elif isinstance(obj, Decimal):
                return int(obj) if obj % 1 == 0 else float(obj)
            elif isinstance(obj, Binary):
                return bytes(obj)
            return obj

        return convert_types_back(document)


class IndexHelper:
    @staticmethod
    def wait_for_create_index(client, table_name, poll_interval=10):
        while True:
            table_description = client.meta.client.describe_table(
                TableName=table_name
            )
            gsis = table_description.get("Table", {}).get(
                "GlobalSecondaryIndexes", []
            )
            if any(gsi["IndexStatus"] == "CREATING" for gsi in gsis):
                time.sleep(poll_interval)
            else:
                break

    @staticmethod
    def wait_for_drop_index(client, table_name, poll_interval=10):
        while True:
            table_description = client.meta.client.describe_table(
                TableName=table_name
            )
            gsis = table_description.get("Table", {}).get(
                "GlobalSecondaryIndexes", []
            )
            if any(gsi["IndexStatus"] == "DELETING" for gsi in gsis):
                time.sleep(poll_interval)
            else:
                break

    @staticmethod
    def create_index(
        client,
        table_name: str,
        index: Index,
        exists: bool | None,
        nargs: Any,
    ) -> IndexResult:
        indexes, db_indexes = IndexHelper.list_indexes(
            client,
            table_name,
            nargs,
            use_name_type=False,
            include_main_index=True,
        )
        status, match_index = BaseIndexHelper.check_index_status(
            indexes, index
        )
        if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
            if exists is False:
                raise ConflictError
            return IndexResult(status=status, index=match_index)
        else:
            indexes = IndexHelper._convert_indexes(
                db_indexes, use_name_type=True
            )
            status, match_index = BaseIndexHelper.check_index_status(
                indexes, index
            )
            if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
                if exists is False:
                    raise ConflictError
                return IndexResult(status=status, index=match_index)

        if isinstance(index, TTLIndex):
            client.meta.client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": True,
                    "AttributeName": index.field,
                },
            )
        else:
            args = OperationConverter.convert_create_index(index)
            if args is None:
                return IndexResult(status=IndexStatus.NOT_SUPPORTED)
            else:
                try:
                    NCall(client.update, args, nargs).invoke()
                except ClientError as e:
                    if exists is False:
                        if (
                            e.response["Error"]["Code"]
                            == "ValidationException"
                            and "exists" in e.response["Error"]["Message"]
                        ):
                            raise ConflictError
                        return IndexResult(status=IndexStatus.EXISTS)
                IndexHelper.wait_for_create_index(client, table_name)
        return IndexResult(status=IndexStatus.CREATED)

    @staticmethod
    def drop_index(
        client, table_name: str, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        if isinstance(index, TTLIndex):
            client.meta.client.update_time_to_live(
                TableName=table_name,
                TimeToLiveSpecification={
                    "Enabled": False,
                    "AttributeName": index.field,
                },
            )
        else:
            args = OperationConverter.convert_drop_index(index)
            if args is None:
                return IndexResult(status=IndexStatus.NOT_SUPPORTED)
            else:
                try:
                    NCall(client.update, args, nargs).invoke()
                except ClientError as e:
                    if exists is True:
                        if (
                            e.response["Error"]["Code"]
                            == "ResourceNotFoundException"
                        ):
                            raise NotFoundError
                    return IndexResult(status=IndexStatus.NOT_EXISTS)
            IndexHelper.wait_for_drop_index(client, table_name)
        return IndexResult(status=IndexStatus.DROPPED)

    @staticmethod
    def list_indexes(
        client,
        table_name: str,
        nargs: Any,
        use_name_type=True,
        include_main_index=False,
    ) -> tuple[list[Index], list[DBIndex]]:
        db_indexes = IndexManager.get_indexes(
            client, table_name, include_main_index
        )
        indexes: list = IndexHelper._convert_indexes(db_indexes, use_name_type)
        response = client.meta.client.describe_time_to_live(
            TableName=table_name
        )
        ttl_status = response.get("TimeToLiveDescription", {})
        attribute_name = ttl_status.get("AttributeName", None)
        if attribute_name:
            indexes.append(TTLIndex(field=attribute_name))
        return indexes, db_indexes

    @staticmethod
    def _convert_indexes(db_indexes, use_name_type: bool) -> list[Index]:
        def convert_single_field_index(
            field: str,
            field_type: str | None,
            db_type: str,
            name_type: str | None,
            use_name_type: bool,
        ):
            if not use_name_type:
                if db_type == "hash":
                    return HashIndex(
                        field=field,
                        field_type=field_type,
                    )
                else:
                    return RangeIndex(
                        field=field,
                        field_type=field_type,
                    )
            else:
                if name_type == "asc":
                    return AscIndex(
                        field=field,
                        field_type=field_type,
                    )
                elif name_type == "desc":
                    return DescIndex(
                        field=field,
                        field_type=field_type,
                    )
                elif name_type == "range":
                    return RangeIndex(
                        field=field,
                        field_type=field_type,
                    )
                elif name_type == "hash":
                    return HashIndex(
                        field=field,
                        field_type=field_type,
                    )
                elif name_type == "array":
                    return ArrayIndex(
                        field=field,
                        field_type=field_type,
                    )
                else:
                    return FieldIndex(
                        field=field,
                        field_type=field_type,
                    )

        indexes: list = []
        for db_index in db_indexes:
            if db_index.range_key is None:
                name_type = BaseIndexHelper.get_type_from_name(
                    db_index.name or "idx"
                )
                index = convert_single_field_index(
                    db_index.hash_key,
                    db_index.hash_key_type,
                    "hash",
                    name_type,
                    use_name_type,
                )
                index.name = db_index.name
                indexes.append(index)
            else:
                name_types: list[Any] = (
                    BaseIndexHelper.get_composite_types_from_name(
                        db_index.name or "idx"
                    )
                )
                if len(name_types) < 2:
                    name_types = [None] * 2
                indexes.append(
                    CompositeIndex(
                        name=db_index.name,
                        fields=[
                            convert_single_field_index(
                                db_index.hash_key,
                                db_index.hash_key_type,
                                "hash",
                                name_types[0],
                                use_name_type,
                            ),
                            convert_single_field_index(
                                db_index.range_key,
                                db_index.range_key_type,
                                "range",
                                name_types[1],
                                use_name_type,
                            ),
                        ],
                    )
                )
        return indexes


class ResourceHelper:
    resource: Any

    def __init__(self, resource: Any):
        self.resource = resource

    def create_collection(
        self,
        table: str,
        config: DocumentCollectionConfig | None,
        exists: bool | None,
        nargs: Any,
    ) -> CollectionResult:
        def get_attribute_type(type: str) -> str:
            if type == DocumentFieldType.STRING.value:
                return "S"
            elif type == DocumentFieldType.NUMBER.value:
                return "N"
            raise BadRequestError("Field type not supported")

        attribute_definitions = []
        key_schema = []
        pk_field = (
            config.pk_field
            if config is not None and config.pk_field is not None
            else "pk"
        )
        pk_type = (
            config.pk_type
            if config is not None and config.pk_type is not None
            else DocumentFieldType.STRING
        )
        attribute_definitions.append(
            {
                "AttributeName": pk_field,
                "AttributeType": get_attribute_type(pk_type),
            }
        )
        key_schema.append({"AttributeName": pk_field, "KeyType": "HASH"})
        id_field = (
            config.id_field
            if config is not None and config.id_field is not None
            else "id"
        )
        id_type = (
            config.id_type
            if config is not None and config.id_type is not None
            else DocumentFieldType.STRING
        )
        attribute_definitions.append(
            {
                "AttributeName": id_field,
                "AttributeType": get_attribute_type(id_type),
            }
        )
        key_schema.append({"AttributeName": id_field, "KeyType": "RANGE"})
        args = {
            "TableName": table,
            "AttributeDefinitions": attribute_definitions,
            "KeySchema": key_schema,
            "BillingMode": "PAY_PER_REQUEST",
        }
        if config and config.nconfig:
            args = args | config.nconfig
        status = CollectionStatus.CREATED
        try:
            db_table = NCall(
                self.resource.create_table,
                args,
                nargs,
            ).invoke()
            NCall(db_table.wait_until_exists, None, nargs).invoke()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceInUseException":
                if exists is False:
                    raise ConflictError
                status = CollectionStatus.EXISTS
            else:
                raise
        index_results: list = []
        if config and config.indexes:
            client = self.resource.Table(table)
            for index in config.indexes:
                index_result = IndexHelper.create_index(
                    client, table, index, None, nargs
                )
                index_results.append(index_result)
        return CollectionResult(status=status, indexes=index_results)

    def drop_collection(
        self, table: str, exists: bool | None, nargs: Any
    ) -> CollectionResult:
        try:
            NCall(
                self.resource.meta.client.delete_table,
                {"TableName": table},
                nargs,
            ).invoke()
            waiter = NCall(
                self.resource.meta.client.get_waiter,
                ["table_not_exists"],
                nargs,
            ).invoke()
            NCall(waiter.wait, {"TableName": table}, nargs).invoke()
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                if exists is True:
                    raise NotFoundError
                return CollectionResult(status=CollectionStatus.NOT_EXISTS)
            else:
                raise
        return CollectionResult(status=CollectionStatus.DROPPED)

    def list_collections(self, nargs) -> Any:
        response = NCall(self.resource.tables.all, None, nargs).invoke()
        return response

    def has_collection(self, table: str, nargs: Any):
        NCall(
            self.resource.meta.client.describe_table,
            {"TableName": table},
            nargs,
        ).invoke()

    def transact(self, ops: list[dict]) -> Any:
        nresult = self.resource.meta.client.transact_write_items(
            TransactItems=ops
        )
        return nresult

    def close(self, nargs: Any) -> Any:
        pass


class ClientHelper:
    client: Any
    op_converter: OperationConverter
    index_manager: IndexManager
    table_name: str

    def __init__(
        self,
        client: Any,
        op_converter: OperationConverter,
        index_manager: IndexManager,
        table_name: str,
    ) -> None:
        self.client = client
        self.op_converter = op_converter
        self.index_manager = index_manager
        self.table_name = table_name

    def create_index(
        self, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        index_result = IndexHelper.create_index(
            self.client, self.table_name, index, exists, nargs
        )
        self.index_manager.refresh_indexes()
        return index_result

    def drop_index(
        self, index: Index, exists: bool | None, nargs: Any
    ) -> IndexResult:
        index_result = IndexHelper.drop_index(
            self.client, self.table_name, index, exists, nargs
        )
        self.index_manager.refresh_indexes()
        return index_result

    def list_indexes(self, nargs: Any) -> list[Index]:
        indexes, _ = IndexHelper.list_indexes(
            self.client, self.table_name, nargs
        )
        return indexes

    def query(self, query: DBQuery, nargs: Any) -> Any:
        call = self._get_query_plan(query, nargs)
        return self._execute_query(call)

    def count(self, query: DBQuery, nargs: Any) -> Any:
        call = self._get_query_plan(query, nargs)
        return self._execute_query(call)

    def batch(self, ops: list[dict]) -> Any:
        with self.client.batch_writer() as batch:
            for op in ops:
                if op["id"] == StoreOperation.PUT:
                    batch.put_item(Item=op["args"]["Item"])
                elif op["id"] == StoreOperation.DELETE:
                    batch.delete_item(Key=op["args"]["Key"])

    def _get_query_plan(self, query: DBQuery, nargs: Any) -> NCall:
        args: dict[str, Any] = {}
        if query.index_name is not None and "." in query.index_name:
            splits = query.index_name.split(".")
            args["IndexName"] = splits[1]
            args["TableName"] = splits[0]
        if query.select is not None:
            args["Select"] = query.select
        if query.projection_expression is not None:
            args["ProjectionExpression"] = query.projection_expression
        if query.key_condition_expression is not None:
            args["KeyConditionExpression"] = query.key_condition_expression
        if query.filter_expression is not None:
            args["FilterExpression"] = query.filter_expression
        if (
            query.expression_attribute_names is not None
            and len(query.expression_attribute_names) > 0
        ):
            args["ExpressionAttributeNames"] = query.expression_attribute_names
        if (
            query.expression_attribute_values is not None
            and len(query.expression_attribute_values) > 0
        ):
            args["ExpressionAttributeValues"] = (
                query.expression_attribute_values
            )
        if query.scan_index_forward is not None:
            args["ScanIndexForward"] = query.scan_index_forward

        limit: int | None = None
        if query.limit is not None:
            if query.offset is not None:
                limit = query.limit + query.offset
            else:
                limit = query.limit
        if limit is not None:
            args["Limit"] = limit

        if query.action == IndexManager.ACTION_QUERY:
            return NCall(self.client.query, args, nargs)
        return NCall(self.client.scan, args, nargs)

    def _execute_query(self, call: NCall) -> Any:
        responses = []
        last_evaluated_key = None
        count = 0
        limit = call.get_arg("Limit")
        while True:
            if last_evaluated_key is not None:
                call.set_arg("ExclusiveStartKey", last_evaluated_key)
            response: dict | None = None
            response = call.invoke()
            responses.append(response)
            if response is not None and "LastEvaluatedKey" in response:
                last_evaluated_key = response["LastEvaluatedKey"]
            else:
                last_evaluated_key = None
            if response is not None and "Count" in response:
                count = count + response["Count"]
            if last_evaluated_key is None:
                break
            if limit is not None and count >= limit:
                break
        return responses


class OperationConverter:
    processor: ItemProcessor
    index_manager: IndexManager

    def __init__(self, processor: ItemProcessor, index_manager: IndexManager):
        self.processor = processor
        self.index_manager = index_manager

    @staticmethod
    def convert_create_index(index: Index) -> dict | None:
        def get_attribute_type(type: str | None) -> str:
            if type == DocumentFieldType.STRING.value:
                return "S"
            elif type == DocumentFieldType.NUMBER.value:
                return "N"
            return "S"

        attribute_definitions = []
        key_schema = []
        if isinstance(index, HashIndex):
            if "." in index.field or "[" in index.field:
                return None
            name = BaseIndexHelper.convert_index_name(index)
            attribute_definitions.append(
                {
                    "AttributeName": index.field,
                    "AttributeType": get_attribute_type(index.field_type),
                }
            )
            key_schema.append(
                {"AttributeName": index.field, "KeyType": "HASH"}
            )
        elif isinstance(index, CompositeIndex):
            if len(index.fields) != 2:
                return None
            for field_index in index.fields:
                if "." in field_index.field or "[" in field_index.field:
                    return None
            name = BaseIndexHelper.convert_index_name(index)
            i = 0
            for field_index in index.fields:
                attribute_definitions.append(
                    {
                        "AttributeName": field_index.field,
                        "AttributeType": get_attribute_type(
                            field_index.field_type
                        ),
                    }
                )
                if i == 0:
                    if isinstance(
                        field_index,
                        (
                            HashIndex,
                            FieldIndex,
                            AscIndex,
                            DescIndex,
                            RangeIndex,
                        ),
                    ):
                        key_type = "HASH"
                    else:
                        return None
                if i == 1:
                    if isinstance(
                        field_index,
                        (
                            HashIndex,
                            FieldIndex,
                            AscIndex,
                            DescIndex,
                            RangeIndex,
                        ),
                    ):
                        key_type = "RANGE"
                    else:
                        return None
                key_schema.append(
                    {"AttributeName": field_index.field, "KeyType": key_type}
                )
                i = i + 1
        else:
            return None
        args = {
            "AttributeDefinitions": attribute_definitions,
            "GlobalSecondaryIndexUpdates": [
                {
                    "Create": {
                        "IndexName": name,
                        "KeySchema": key_schema,
                        "Projection": {"ProjectionType": "ALL"},
                    }
                }
            ],
            "BillingMode": "PAY_PER_REQUEST",
        }
        if index.nconfig:
            args = args | index.nconfig
        return args

    @staticmethod
    def convert_drop_index(index: Index) -> dict:
        name = BaseIndexHelper.convert_index_name(index)
        args = {
            "GlobalSecondaryIndexUpdates": [
                {
                    "Delete": {
                        "IndexName": name,
                    }
                }
            ]
        }
        if index.nconfig:
            args = args | index.nconfig
        return args

    @staticmethod
    def convert_transact(
        op_parsers: list[StoreOperationParser],
        converters: list[OperationConverter],
    ) -> tuple[dict, dict]:
        ops: list = []
        states: list = []
        for i in range(0, len(op_parsers)):
            op_parser = op_parsers[i]
            converter = converters[i]
            if op_parser.op_equals(StoreOperation.PUT):
                args, state = converter.convert_put(
                    op_parser.get_key(),
                    op_parser.get_value(),
                    op_parser.get_where(),
                    op_parser.get_where_exists(),
                )
                ops.append({"Put": args})
                states.append(state)
            elif op_parser.op_equals(StoreOperation.UPDATE):
                args, state = converter.convert_update(
                    op_parser.get_key(),
                    op_parser.get_set(),
                    op_parser.get_where(),
                    op_parser.get_returning(),
                )
                if "ReturnValues" in args:
                    # ReturnValues not supported in update
                    args.pop("ReturnValues")
                ops.append({"Update": args})
                states.append(state)
            elif op_parser.op_equals(StoreOperation.DELETE):
                args = converter.convert_delete(
                    op_parser.get_key(), op_parser.get_where()
                )
                ops.append({"Delete": args})
                states.append(None)
        return {"ops": ops}, {"values": states}

    def convert_batch(
        self, op_parsers: list[StoreOperationParser]
    ) -> tuple[dict, dict]:
        ops: list = []
        states: list = []
        for op_parser in op_parsers:
            if op_parser.op_equals(StoreOperation.PUT):
                args, state = self.convert_put(
                    op_parser.get_key(),
                    op_parser.get_value(),
                    op_parser.get_where(),
                    op_parser.get_where_exists(),
                )
                ops.append({"id": StoreOperation.PUT, "args": args})
                states.append(state)
            elif op_parser.op_equals(StoreOperation.DELETE):
                args = self.convert_delete(
                    op_parser.get_key(), op_parser.get_where()
                )
                ops.append({"id": StoreOperation.DELETE, "args": args})
                states.append(None)
        return {"ops": ops}, {"values": states}

    def convert_get(self, key: Value) -> dict:
        dbkey = self.processor.get_key_from_key(key=key)
        return {"Key": dbkey}

    def convert_put(
        self,
        key: Value,
        value: dict,
        where: Expression | None,
        exists: bool | None,
    ) -> tuple[dict, dict | None]:
        def convert_floats(obj):
            if isinstance(obj, dict):
                return {k: convert_floats(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_floats(i) for i in obj]
            elif isinstance(obj, float):
                return Decimal(str(obj))
            return obj

        document = self.processor.add_embed_fields(value, key)
        document = convert_floats(document)
        args: dict = {
            "TableName": self.index_manager.table_name,
            "Item": document,
        }
        if where is None:
            pass
        elif exists is False:
            condition_expression = (
                f"attribute_not_exists({self.processor.id_embed_field})"
            )
            args = args | {"ConditionExpression": condition_expression}
        elif exists is True:
            condition_expression = (
                f"attribute_exists({self.processor.id_embed_field})"
            )
            args = args | {"ConditionExpression": condition_expression}
        elif where is not None:
            attribute_names: dict[str, str] = {}
            attribute_values: dict[str, Any] = {}
            condition_expression = f"""
                attribute_exists({self.processor.id_embed_field}) and {
                self.convert_expr(
                    where, attribute_names, attribute_values)}"""
            args = args | {
                "ConditionExpression": condition_expression,
                "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
            }
            if len(attribute_names) > 0:
                args["ExpressionAttributeNames"] = attribute_names
            if len(attribute_values) > 0:
                args["ExpressionAttributeValues"] = attribute_values
        return args, {"value": document}

    def convert_update(
        self,
        key: Value,
        set: Update,
        where: Expression | None,
        returning: str | None,
    ) -> tuple[dict, dict | None]:
        uset = copy.deepcopy(set)
        state = None
        dbkey = self.processor.get_key_from_key(key=key)
        if self.processor.needs_local_etag():
            etag = self.processor.generate_etag()
            uset = self.processor.add_etag_update(uset, etag)
            state = {"etag": etag}
        attribute_names: dict = {}
        attribute_values: dict = {}
        update_expression = self.convert_update_ops(
            uset, attribute_names, attribute_values
        )
        args = {
            "TableName": self.index_manager.table_name,
            "Key": dbkey,
            "UpdateExpression": update_expression,
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
        }
        if returning == "new":
            args = args | {"ReturnValues": "ALL_NEW"}
        elif returning == "old":
            args = args | {"ReturnValues": "ALL_OLD"}
        if where is not None:
            condition_expression = self.convert_expr(
                where, attribute_names, attribute_values
            )
        else:
            condition_expression = (
                f"attribute_exists({self.processor.id_embed_field})"
            )
        args["ConditionExpression"] = condition_expression
        if len(attribute_names) > 0:
            args["ExpressionAttributeNames"] = attribute_names
        if len(attribute_values) > 0:
            args["ExpressionAttributeValues"] = attribute_values
        return args, state

    def convert_delete(self, key: Value, where: Expression | None) -> dict:
        dbkey = self.processor.get_key_from_key(key=key)
        args = {
            "TableName": self.index_manager.table_name,
            "Key": dbkey,
            "ReturnValuesOnConditionCheckFailure": "ALL_OLD",
        }
        if where is not None:
            attribute_names: dict = {}
            attribute_values: dict = {}
            condition_expression = self.convert_expr(
                where, attribute_names, attribute_values
            )
            args["ConditionExpression"] = condition_expression
            if len(attribute_names) > 0:
                args["ExpressionAttributeNames"] = attribute_names
            if len(attribute_values) > 0:
                args["ExpressionAttributeValues"] = attribute_values
        else:
            args["ConditionExpression"] = (
                f"""attribute_exists({self.processor.id_embed_field})"""
            )
        return args

    def convert_query(
        self,
        select: Select | None = None,
        where: Expression | None = None,
        order_by: OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        collection: str | None = None,
    ) -> dict:
        explicit_index = self._get_explicit_index(collection)
        order_by_field = self._get_order_by_field(order_by)
        select_fields = self._get_select_fields(select)
        where_key_fields, where_non_key_fields = self._get_where_fields(where)
        query_plan = self.index_manager.get_query_plan(
            order_by_field,
            select_fields,
            where_key_fields,
            where_non_key_fields,
            explicit_index,
        )
        query = DBQuery()
        query.action = query_plan.action
        query.index_name = explicit_index
        query.expression_attribute_names = {}
        query.expression_attribute_values = {}
        query.limit, query.offset = self.convert_limit_offset(limit, offset)
        query.select, query.projection_expression = self.convert_select(
            select, query.expression_attribute_names
        )
        if query_plan.action == IndexManager.ACTION_QUERY:
            query.index_name = query_plan.index.name
            if query.index_name == "$main":
                query.index_name = None
            ke, fe = self._split_key_filter_expression(where, query_plan.index)
            query.key_condition_expression = self.convert_expr(
                ke,
                query.expression_attribute_names,
                query.expression_attribute_values,
            )
            query.filter_expression = self.convert_expr(
                fe,
                query.expression_attribute_names,
                query.expression_attribute_values,
            )
            query.scan_index_forward = self.convert_order_by(order_by)
        elif query_plan.action == IndexManager.ACTION_SCAN:
            if where is not None:
                query.filter_expression = self.convert_expr(
                    where,
                    query.expression_attribute_names,
                    query.expression_attribute_values,
                )
        return {"query": query}

    def convert_count(
        self, where: Expression | None, collection: str | None
    ) -> dict:
        args = self.convert_query(where=where, collection=collection)
        args["query"].select = "COUNT"
        return args

    def convert_update_ops(
        self, update: Update, attribute_names, attribute_values
    ) -> str:
        sets: list[str] = []
        removes: list[str] = []
        adds: list[str] = []
        deletes: list[str] = []
        for operation in update.operations:
            field = self.convert_expr(
                Field(path=operation.field), attribute_names, attribute_values
            )
            if operation.op == UpdateOp.PUT:
                value = self.convert_expr(
                    operation.args[0], attribute_names, attribute_values
                )
                sets.append(f"{field} = {value}")
            elif operation.op == UpdateOp.INSERT:
                if field[-1] == "]":
                    value = self.convert_expr(
                        [operation.args[0]], attribute_names, attribute_values
                    )
                    if field.endswith("[-]"):
                        sets.append(
                            f"""{field[:-3]} = list_append({
                                field[:-3]}, {value})"""
                        )
                    elif field.endswith("[0]"):
                        sets.append(
                            f"""{field[:-3]} = list_append({
                                value}, {field[:-3]})"""
                        )
                    else:
                        raise BadRequestError(
                            "List update at intermediate index not supported"
                        )
                else:
                    value = self.convert_expr(
                        operation.args[0], attribute_names, attribute_values
                    )
                    sets.append(f"{field} = {value}")
            elif operation.op == UpdateOp.DELETE:
                removes.append(field)
            elif operation.op == UpdateOp.INCREMENT:
                val = operation.args[0]
                if operation.args[0] < 0:
                    val = -val
                    op = "-"
                else:
                    op = "+"
                value = self.convert_expr(
                    val, attribute_names, attribute_values
                )
                sets.append(f"{field} = {field} {op} {value}")
            elif operation.op == UpdateOp.MOVE:
                dest = self.convert_expr(
                    Field(path=operation.args[0].path),
                    attribute_names,
                    attribute_values,
                )
                sets.append(f"{dest} = {field}")
                removes.append(f"{field}")
            else:
                raise BadRequestError("Update operation not supported")

        expr = ""
        if len(sets) > 0:
            expr = f"{expr} SET {', '.join(e for e in sets)}"
        if len(removes) > 0:
            expr = f"{expr} REMOVE {', '.join(e for e in removes)}"
        if len(adds) > 0:
            expr = f"{expr} ADD {', '.join(e for e in adds)}"
        if len(deletes) > 0:
            expr = f"{expr} DELETE {', '.join(e for e in deletes)}"
        expr = expr.strip()
        return expr

    def convert_expr(
        self,
        expr: Expression | None,
        attribute_names: dict[str, str],
        attribute_values: dict[str, Any],
    ) -> str:
        if expr is None or isinstance(
            expr, (str, int, float, bool, dict, list)
        ):
            var = f":v{len(attribute_values)}"
            if isinstance(expr, float):
                attribute_values[var] = Decimal(str(expr))
            else:
                attribute_values[var] = expr
            return var
        if isinstance(expr, Field):
            field = self.processor.resolve_field(expr.path)
            field = (
                field.replace("[", "/")
                .replace("]", "")
                .replace(".", "/")
                .rstrip("/")
            )
            splits = field.split("/")
            alias = ""
            for split in splits:
                if split == "-" or split.isdigit():
                    alias = f"{alias}[{split}]"
                else:
                    var = f"#f{len(attribute_names)}"
                    attribute_names[var] = split
                    alias = f"{alias}.{var}" if alias != "" else var
            return alias
        if isinstance(expr, Function):
            return self.convert_func(expr, attribute_names, attribute_values)
        if isinstance(expr, Comparison):
            lhs = f"""{self.convert_expr(
                    expr.lexpr, attribute_names, attribute_values)}"""
            op = expr.op.value
            rhs = None
            negate = False
            if expr.op == ComparisonOp.NEQ:
                op = "<>"
            if expr.op == ComparisonOp.BETWEEN and isinstance(
                expr.rexpr, list
            ):
                rhs = f"""{self.convert_expr(
                    expr.rexpr[0], attribute_names, attribute_values)} AND {
                        self.convert_expr(
                            expr.rexpr[1],
                            attribute_names,
                            attribute_values)}"""
            elif (
                expr.op == ComparisonOp.IN or expr.op == ComparisonOp.NIN
            ) and isinstance(expr.rexpr, list):
                lst = ", ".join(
                    self.convert_expr(i, attribute_names, attribute_values)
                    for i in expr.rexpr
                )
                rhs = f"({lst})"
                if expr.op == ComparisonOp.NIN:
                    op = "IN"
                    negate = True
            else:
                rhs = self.convert_expr(
                    expr.rexpr, attribute_names, attribute_values
                )
            if negate:
                return f"""NOT {lhs} {op} {rhs}"""
            return f"""{lhs} {op} {rhs}"""
        if isinstance(expr, And):
            return f"""({self.convert_expr(
                expr.lexpr, attribute_names, attribute_values)} AND {
                    self.convert_expr(
                        expr.rexpr, attribute_names, attribute_values)})"""
        if isinstance(expr, Or):
            return f"""({self.convert_expr(
                expr.lexpr, attribute_names, attribute_values)} OR {
                    self.convert_expr(
                        expr.rexpr, attribute_names, attribute_values)})"""
        if isinstance(expr, Not):
            return f"""NOT {self.convert_expr(
                expr.expr, attribute_names, attribute_values)}"""
        return str(expr)

    def convert_func(
        self,
        expr: Function,
        attribute_names: dict[str, str],
        attribute_values: dict[str, Any],
    ) -> str:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_TYPE:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                type = args[1]
                if isinstance(type, str):
                    type = DocumentFieldType(type.lower())
                type_param = None
                if type == DocumentFieldType.STRING:
                    type_param = "S"
                elif type == DocumentFieldType.NUMBER:
                    type_param = "N"
                elif type == DocumentFieldType.BOOLEAN:
                    type_param = "BOOL"
                elif type == DocumentFieldType.OBJECT:
                    type_param = "M"
                elif type == DocumentFieldType.ARRAY:
                    type_param = "L"
                elif type == DocumentFieldType.NULL:
                    type_param = "NULL"
                value = self.convert_expr(
                    type_param, attribute_names, attribute_values
                )
                return f"attribute_type({field}, {value})"
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                return f"attribute_exists({field})"
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                return f"attribute_not_exists({field})"
            if name == QueryFunctionName.LENGTH:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                return f"size({field})"
            if name == QueryFunctionName.CONTAINS:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                value = self.convert_expr(
                    args[1], attribute_names, attribute_values
                )
                return f"contains({field}, {value})"
            if name == QueryFunctionName.STARTS_WITH:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                value = self.convert_expr(
                    args[1], attribute_names, attribute_values
                )
                return f"begins_with({field}, {value})"
            if name == QueryFunctionName.ARRAY_LENGTH:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                return f"size({field})"
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                value = self.convert_expr(
                    args[1], attribute_names, attribute_values
                )
                return f"contains({field}, {value})"
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_expr(
                    args[0], attribute_names, attribute_values
                )
                clauses = []
                for item in args[1]:
                    value = self.convert_expr(
                        item, attribute_names, attribute_values
                    )
                    clauses.append(f"contains({field}, {value})")
                return f"({str.join(' OR ', clauses)})"
        raise BadRequestError(f"Function {name} not recognized")

    def convert_select(
        self,
        select: Select | None,
        attribute_names: dict[str, str],
    ) -> tuple[str | None, str | None]:
        if select is None or len(select.terms) == 0:
            return "ALL_ATTRIBUTES", None
        return "SPECIFIC_ATTRIBUTES", ", ".join(
            self.convert_expr(Field(path=t.field), attribute_names, {})
            for t in select.terms
        )

    def convert_order_by(self, order_by: OrderBy | None) -> bool | None:
        if order_by is not None and len(order_by.terms) > 0:
            if order_by.terms[0].direction is not None:
                return order_by.terms[0].direction == OrderByDirection.ASC
            else:
                return True
        return None

    def convert_limit_offset(
        self, limit: int | None = None, offset: int | None = None
    ) -> tuple[int | None, int | None]:
        return limit, offset

    def _get_explicit_index(self, collection: str | None) -> str | None:
        if collection is not None and "." in collection:
            return collection.split(".")[-1]
        return None

    def _get_order_by_field(self, order_by: OrderBy | None) -> str | None:
        order_by_field = None
        if order_by is not None and len(order_by.terms) > 0:
            order_by_field = self.processor.resolve_field(
                order_by.terms[0].field
            )
        return order_by_field

    def _get_select_fields(self, select: Select | None) -> list[str]:
        select_fields = []
        if select is not None:
            for term in select.terms:
                field = self.processor.resolve_field(term.field)
                select_fields.append(field)
        return select_fields

    def _get_where_fields(self, expr: Expression | None) -> tuple:
        key_fields: dict = {}
        non_key_fields: list = []
        self._find_where_fields(expr, key_fields, non_key_fields, False)
        for f in non_key_fields:
            if f in key_fields:
                key_fields.pop(f)
        return key_fields, non_key_fields

    def _find_where_fields(
        self,
        expr: Expression | None,
        key_fields: dict[str, list],
        non_key_fields: list[str],
        non_key: bool,
    ):
        def try_add_key_field(field, index_types):
            field = self.processor.resolve_field(field)
            if field in key_fields:
                # If field already exists, add to non-key
                # DynamoDB supports only one comparison
                add_non_key_field(field)
            else:
                key_fields[field] = index_types

        def add_non_key_field(field):
            field = self.processor.resolve_field(field)
            if field not in non_key_fields:
                non_key_fields.append(field)

        if isinstance(expr, Comparison):
            field = None
            if isinstance(expr.lexpr, Field):
                field = expr.lexpr.path
            elif isinstance(expr.rexpr, Field):
                field = expr.rexpr.path
            if field is None:
                return
            if non_key:
                add_non_key_field(field)
                return
            if expr.op == ComparisonOp.EQ:
                try_add_key_field(
                    field,
                    [
                        IndexManager.INDEX_TYPE_HASH,
                        IndexManager.INDEX_TYPE_RANGE,
                    ],
                )
            elif expr.op in [
                ComparisonOp.LT,
                ComparisonOp.LTE,
                ComparisonOp.GT,
                ComparisonOp.GTE,
                ComparisonOp.BETWEEN,
            ]:
                try_add_key_field(field, [IndexManager.INDEX_TYPE_RANGE])
            else:
                if field is not None:
                    add_non_key_field(field)
        elif isinstance(expr, Function):
            if (
                expr.namespace == FunctionNamespace.BUILTIN
                and expr.name == QueryFunctionName.STARTS_WITH
                and isinstance(expr.args[0], Field)
                and not non_key
            ):
                try_add_key_field(
                    expr.args[0].path, [IndexManager.INDEX_TYPE_RANGE]
                )
            else:
                for arg in expr.args:
                    if isinstance(arg, Field):
                        add_non_key_field(arg.path)
        elif isinstance(expr, And):
            self._find_where_fields(
                expr.lexpr, key_fields, non_key_fields, non_key
            )
            self._find_where_fields(
                expr.rexpr, key_fields, non_key_fields, non_key
            )
        elif isinstance(expr, Or):
            self._find_where_fields(
                expr.lexpr, key_fields, non_key_fields, True
            )
            self._find_where_fields(
                expr.rexpr, key_fields, non_key_fields, True
            )
        elif isinstance(expr, Not):
            self._find_where_fields(
                expr.expr, key_fields, non_key_fields, True
            )

    def _split_key_filter_expression(
        self,
        expr: Expression,
        index: DBIndex,
    ) -> tuple[Expression, Expression]:
        def is_key_field(field):
            field = self.processor.resolve_field(field)
            if field == index.hash_key or field == index.range_key:
                return True
            return False

        if isinstance(expr, Comparison):
            field = None
            if isinstance(expr.lexpr, Field):
                field = expr.lexpr.path
            elif isinstance(expr.rexpr, Field):
                field = expr.rexpr.path
            if is_key_field(field):
                return expr, Comparison(
                    lexpr=True, op=ComparisonOp.EQ, rexpr=True
                )
            return None, expr
        elif isinstance(expr, Function):
            for arg in expr.args:
                if isinstance(arg, Field):
                    if is_key_field(arg.path):
                        return expr, Comparison(
                            lexpr=True, op=ComparisonOp.EQ, rexpr=True
                        )
            return None, expr
        elif isinstance(expr, (And, Or)):
            (
                ke_lhs,
                fe_lhs,
            ) = self._split_key_filter_expression(expr.lexpr, index)

            (
                ke_rhs,
                fe_rhs,
            ) = self._split_key_filter_expression(expr.rexpr, index)

            ke: Expression = None
            if ke_lhs is not None and ke_rhs is not None:
                ke = And(lexpr=ke_lhs, rexpr=ke_rhs)
            elif ke_lhs is not None:
                ke = ke_lhs
            elif ke_rhs is not None:
                ke = ke_rhs

            fe: Expression = (
                And(lexpr=fe_lhs, rexpr=fe_rhs)
                if isinstance(expr, And)
                else Or(lexpr=fe_lhs, rexpr=fe_rhs)
            )
            return ke, fe
        elif isinstance(expr, Not):
            ke, fe = self._split_key_filter_expression(expr.expr, index)
            return ke, Not(expr=expr.expr)
        return None, expr


class IndexManager:
    INDEX_CATEGORY_MAIN = "main"
    INDEX_CATEGORY_LOCAL = "local"
    INDEX_CATEGORY_GLOBAL = "global"

    INDEX_TYPE_HASH = "hash"
    INDEX_TYPE_RANGE = "range"

    ACTION_QUERY = "query"
    ACTION_SCAN = "scan"

    MAIN_INDEX_NAME = "$main"

    client: Any
    table_name: str
    hash_key: str
    range_key: str | None
    indexes: list[DBIndex]

    def __init__(self, client: Any, table_name: str):
        self.indexes = []
        self.table_name = table_name
        self.hash_key, self.range_key = IndexManager._parse_key_schema(
            key_schema=client.key_schema
        )
        table_info = IndexManager.fetch_table_info(client, table_name)
        self.hash_key = table_info["hash_key"]
        self.range_key = table_info["range_key"]
        self.indexes = table_info["indexes"]
        self.client = client

    def __str__(self):
        return f"[{', '.join(str(i) for i in self.indexes)}]"

    @staticmethod
    def fetch_table_info(client: Any, table_name: str) -> dict:
        table_description = client.meta.client.describe_table(
            TableName=table_name
        )
        table_def = table_description.get("Table", {})
        key_schema = table_def.get("KeySchema", {})
        gsis = table_def.get("GlobalSecondaryIndexes", [])
        lsis = table_def.get("LocalSecondaryIndexes", [])
        attribute_definitions = table_def.get("AttributeDefinitions", [])
        hash_key, range_key = IndexManager._parse_key_schema(
            key_schema=key_schema
        )
        indexes: list = []
        indexes.append(IndexManager._parse_main_index(hash_key, range_key))
        indexes.extend(
            IndexManager._parse_indexes(
                gsis,
                lsis,
                attribute_definitions,
                hash_key,
                range_key,
                table_name,
            )
        )
        return {
            "hash_key": hash_key,
            "range_key": range_key,
            "indexes": indexes,
        }

    def refresh_indexes(self) -> None:
        table_info = IndexManager.fetch_table_info(
            self.client, self.table_name
        )
        self.indexes = table_info["indexes"]

    @staticmethod
    def get_indexes(
        client: Any, table_name: str, include_main_index: bool
    ) -> list[DBIndex]:
        table_info = IndexManager.fetch_table_info(client, table_name)
        start = 0
        if not include_main_index:
            start = 1
        return table_info["indexes"][start:]

    def get_id_pk_field(self) -> tuple:
        if self.range_key is None:
            return self.hash_key, self.hash_key
        return self.range_key, self.hash_key

    def get_query_plan(
        self,
        order_by_field: str | None,
        select_fields: list[str],
        where_key_fields: dict[str, list[str]],
        where_non_key_fields: list[str],
        explicit_index: str | None,
    ) -> QueryPlan:
        candidate_indexes = self._get_order_by_candidate_indexes(
            order_by_field, self.indexes
        )
        if order_by_field is not None and len(candidate_indexes) == 0:
            raise BadRequestError(
                f"No index found to order by {order_by_field}"
            )

        candidate_indexes = self._get_select_candidate_indexes(
            select_fields, candidate_indexes
        )
        if order_by_field is not None and len(candidate_indexes) == 0:
            raise BadRequestError(
                f"""No index found to order by {
                    order_by_field} and get the select fields"""
            )

        candidate_indexes = self._get_where_candidate_indexes(
            where_key_fields, where_non_key_fields, candidate_indexes
        )
        if order_by_field is not None and len(candidate_indexes) == 0:
            raise BadRequestError(
                f"""No index found to order by {
                    order_by_field} and filter by where condition"""
            )

        action = IndexManager.ACTION_SCAN
        index = self.indexes[0]
        if explicit_index is not None:
            match = False
            for candidate_index in candidate_indexes:
                if candidate_index.name == explicit_index:
                    action = IndexManager.ACTION_QUERY
                    index = candidate_index
                    match = True
                    break
            if order_by_field is not None and not match:
                raise BadRequestError(
                    f"""Provided index does not support the order by {
                        order_by_field} query"""
                )
            if not match:
                for db_index in self.indexes:
                    if db_index.name == explicit_index:
                        action = IndexManager.ACTION_SCAN
                        index = db_index
        else:
            if len(candidate_indexes) > 0:
                action = IndexManager.ACTION_QUERY
                index = candidate_indexes[0]

        query_plan = QueryPlan()
        query_plan.action = action
        query_plan.index = index
        return query_plan

    def _get_order_by_candidate_indexes(
        self,
        order_by_field: str | None,
        current_candidate_indexes: list[DBIndex],
    ) -> list[DBIndex]:
        candidate_indices = []
        for index in current_candidate_indexes:
            if order_by_field is None or index.range_key == order_by_field:
                candidate_indices.append(index)
        return candidate_indices

    def _get_select_candidate_indexes(
        self,
        select_fields: list[str],
        current_candidate_indexes: list[DBIndex],
    ) -> list[DBIndex]:
        candidate_indices: list[DBIndex] = []
        for index in current_candidate_indexes:
            if index.projection_type == "ALL":
                # rank higher
                candidate_indices.insert(0, index)
            else:
                if len(select_fields) > 0:
                    match = True
                    for select_field in select_fields:
                        if select_field not in index.projection_fields:
                            match = False
                    if match:
                        # rank higher
                        candidate_indices.insert(0, index)
                    elif index.category == IndexManager.INDEX_CATEGORY_LOCAL:
                        # rank lower since will require extra reads
                        candidate_indices.append(index)

        return candidate_indices

    def _get_where_candidate_indexes(
        self,
        where_key_fields: dict[str, list[str]],
        where_non_key_fields: list[str],
        current_candidate_indexes: list[DBIndex],
    ) -> list[DBIndex]:
        candidate_indexes: list[DBIndex] = []
        for index in reversed(current_candidate_indexes):
            if (
                index.hash_key in where_key_fields
                and IndexManager.INDEX_TYPE_HASH
                in where_key_fields[index.hash_key]
            ):
                if index.range_key is not None:
                    if index.range_key in where_key_fields:
                        # rank higher
                        candidate_indexes.insert(0, index)
                    if index.range_key not in where_non_key_fields:
                        # rank lower
                        candidate_indexes.append(index)
                else:
                    # rank lower
                    candidate_indexes.append(index)

        return candidate_indexes

    @staticmethod
    def _add_keys_to_projection(
        keys: list[str | None],
        projection_fields: list[str],
    ):
        for key in keys:
            if key is not None and key not in projection_fields:
                projection_fields.append(key)

    @staticmethod
    def _parse_indexes(
        gsis,
        lsis,
        attribute_definitions,
        hash_key,
        range_key,
        table_name,
    ) -> list[DBIndex]:
        indexes: list = []
        for index_category in [
            IndexManager.INDEX_CATEGORY_LOCAL,
            IndexManager.INDEX_CATEGORY_GLOBAL,
        ]:
            db_indexes = None
            if index_category == IndexManager.INDEX_CATEGORY_GLOBAL:
                db_indexes = gsis
            elif index_category == IndexManager.INDEX_CATEGORY_LOCAL:
                db_indexes = lsis
            if db_indexes is not None:
                for db_index in db_indexes:
                    index = IndexManager._parse_index(
                        db_index, attribute_definitions, table_name
                    )
                    index.category = index_category
                    if index.projection_type != "ALL":
                        IndexManager._add_keys_to_projection(
                            [
                                hash_key,
                                range_key,
                                index.hash_key,
                                index.range_key,
                            ],
                            index.projection_fields,
                        )
                    indexes.append(index)
        return indexes

    @staticmethod
    def _parse_main_index(hash_key, range_key) -> DBIndex:
        index = DBIndex()
        index.name = IndexManager.MAIN_INDEX_NAME
        index.category = IndexManager.INDEX_CATEGORY_MAIN
        index.hash_key = hash_key
        index.hash_key_type = None
        index.range_key = range_key
        index.range_key_type = None
        index.projection_type = "ALL"
        index.projection_fields = []
        return index

    @staticmethod
    def _parse_index(
        db_index: dict,
        attribute_definitions: list,
        table_name: str,
    ) -> DBIndex:
        def get_key_type(type: str) -> str:
            if type == "S":
                return DocumentFieldType.STRING.value
            elif type == "N":
                return DocumentFieldType.NUMBER.value
            return DocumentFieldType.STRING.value

        index = DBIndex()
        index.name = f"{table_name}.{db_index['IndexName']}"
        index.hash_key, index.range_key = IndexManager._parse_key_schema(
            db_index["KeySchema"]
        )
        for attr in attribute_definitions:
            if attr["AttributeName"] == index.hash_key:
                index.hash_key_type = get_key_type(attr["AttributeType"])
            if attr["AttributeName"] == index.range_key:
                index.range_key_type = get_key_type(attr["AttributeType"])
        (
            index.projection_type,
            index.projection_fields,
        ) = IndexManager._parse_projection(db_index["Projection"])
        return index

    @staticmethod
    def _parse_key_schema(key_schema: list[dict]) -> tuple:
        hash_key = None
        range_key = None
        if len(key_schema) == 2:
            if key_schema[0]["KeyType"] == "HASH":
                hash_key = key_schema[0]["AttributeName"]
                range_key = key_schema[1]["AttributeName"]
            elif key_schema[0]["KeyType"] == "RANGE":
                hash_key = key_schema[1]["AttributeName"]
                range_key = key_schema[0]["AttributeName"]
        else:
            hash_key = key_schema[0]["AttributeName"]
        return hash_key, range_key

    @staticmethod
    def _parse_projection(projection: dict) -> tuple:
        projection_type = projection["ProjectionType"]
        projection_fields = []
        if projection_type == "INCLUDE":
            projection_fields = projection["NonKeyAttributes"]
        return projection_type, projection_fields


class DBIndex:
    name: str | None
    hash_key: str
    range_key: str | None
    hash_key_type: str | None
    range_key_type: str | None
    projection_type: str
    projection_fields: list[str]
    category: str

    def __repr__(self):
        return str(self.__dict__)


class QueryPlan:
    action: str
    index: DBIndex


class DBQuery:
    action: str = IndexManager.ACTION_SCAN
    index_name: str | None = None
    select: str | None = None
    projection_expression: str | None = None
    key_condition_expression: str | None = None
    filter_expression: str | None = None
    scan_index_forward: bool | None = None
    limit: int | None = None
    offset: int | None = None
    expression_attribute_values: dict[str, Any] = {}
    expression_attribute_names: dict[str, str] = {}

    def __repr__(self) -> str:
        return str(self.__dict__)


class DynamoDBCollection:
    client: Any
    op_converter: OperationConverter
    processor: ItemProcessor
    helper: Any

    def __init__(
        self,
        table_name: str,
        client: Any,
        helper_type: Any,
        id_map_field: str | None,
        pk_map_field: str | None,
        etag_embed_field: str | None,
        suppress_fields: list[str] | None,
    ):
        self.client = client
        index_manager = IndexManager(client=client, table_name=table_name)
        id_embed_field, pk_embed_field = index_manager.get_id_pk_field()
        self.processor = ItemProcessor(
            id_embed_field=id_embed_field,
            pk_embed_field=pk_embed_field,
            etag_embed_field=etag_embed_field,
            id_map_field=id_map_field,
            pk_map_field=pk_map_field,
            local_etag=True,
            suppress_fields=suppress_fields,
        )
        self.op_converter = OperationConverter(self.processor, index_manager)
        self.helper = helper_type(
            client,
            self.op_converter,
            index_manager,
            table_name,
        )
