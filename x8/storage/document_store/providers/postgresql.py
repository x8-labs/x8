"""
Document Store on PostgreSQL.
"""

from __future__ import annotations

__all__ = ["PostgreSQL"]

import copy
import json
import re
from typing import Any

import psycopg

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
    TextIndex,
    Validator,
    VectorIndex,
    VectorIndexMetric,
    VectorIndexStructure,
    WildcardIndex,
)

from .._feature import DocumentStoreFeature
from .._helper import (
    build_item_from_parts,
    build_item_from_value,
    build_query_result,
    get_collection_config,
)
from .._models import DocumentCollectionConfig, DocumentFieldType


class PostgreSQL(StoreProvider):
    connection_string: str
    table: str | None
    id_column: str | dict
    value_column: str | dict
    pk_column: str | dict | None
    id_map_field: str | dict | None
    pk_map_field: str | dict | None
    etag_embed_field: str | dict | None
    suppress_fields: list[str] | None
    nparams: dict[str, Any]

    _client: Any
    _aclient: Any
    _collection_cache: dict[str, PostgreSQLCollection]
    _acollection_cache: dict[str, PostgreSQLCollection]

    def __init__(
        self,
        connection_string: str,
        table: str | None = None,
        id_column: str | dict = "id",
        value_column: str | dict = "value",
        pk_column: str | dict | None = "pk",
        id_map_field: str | dict | None = "id",
        pk_map_field: str | dict | None = "pk",
        etag_embed_field: str | dict | None = "_etag",
        suppress_fields: list[str] | None = None,
        nparams: dict[str, Any] = dict(),
        **kwargs,
    ):
        """Initialize.

        Args:
            connection_string:
                PostgreSQL connection string.
            table:
                PostgreSQL table name mapped to document store collection.
            id_column:
                Column name to store the document id.
            value_column:
                Column name to store the document value.
                This column should have the JSONB type.
            pk_column:
                Column name to store the document pk.
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
                Native parameters to psycopg client.
        """
        self.connection_string = connection_string
        self.table = table
        self.id_column = id_column
        self.value_column = value_column
        self.pk_column = pk_column
        self.id_map_field = id_map_field
        self.pk_map_field = pk_map_field
        self.etag_embed_field = etag_embed_field
        self.suppress_fields = suppress_fields
        self.nparams = nparams

        self._client = None
        self._aclient = None
        self._collection_cache = dict()
        self._acollection_cache = dict()

    def __supports__(self, feature: str) -> bool:
        return feature not in [DocumentStoreFeature.TYPE_BINARY]

    def __setup__(self, context: Context | None = None) -> None:
        if self._client is not None:
            return

        self._client = psycopg.connect(self.connection_string, **self.nparams)

    async def __asetup__(self, context: Context | None = None) -> None:
        if self._aclient is not None:
            return
        self._aclient = await psycopg.AsyncConnection.connect(
            self.connection_string, **self.nparams
        )

    def _get_table_name(self, op_parser: StoreOperationParser) -> str:
        collection_name = (
            op_parser.get_operation_parsers()[0].get_collection_name()
            if op_parser.op_equals(StoreOperation.BATCH)
            else op_parser.get_collection_name()
        )
        table = collection_name or self.table or self.__component__.collection
        if table is None:
            raise BadRequestError("Collection name must be specified.")
        return table

    def _get_collections(
        self, op_parser: StoreOperationParser
    ) -> list[PostgreSQLCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[PostgreSQLCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(self._get_collections(single_op_parser))
            return collections
        table = self._get_table_name(op_parser)
        if table is None:
            raise BadRequestError("Collection name must be specified")
        if table in self._collection_cache:
            return [self._collection_cache[table]]
        id_column = ParameterParser.get_collection_parameter(
            self.id_column, table
        )
        value_column = ParameterParser.get_collection_parameter(
            self.value_column, table
        )
        pk_column = ParameterParser.get_collection_parameter(
            self.pk_column, table
        )
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field, table
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field, table
        )
        etag_embed_field = ParameterParser.get_collection_parameter(
            self.etag_embed_field, table
        )
        col = PostgreSQLCollection(
            table,
            id_column,
            value_column,
            pk_column,
            id_map_field,
            pk_map_field,
            etag_embed_field,
            self.suppress_fields,
        )
        self._collection_cache[table] = col
        return [col]

    async def _aget_collections(
        self, op_parser: StoreOperationParser
    ) -> list[PostgreSQLCollection]:
        if op_parser.is_resource_op():
            return []
        if op_parser.op_equals(StoreOperation.TRANSACT):
            collections: list[PostgreSQLCollection] = []
            for single_op_parser in op_parser.get_operation_parsers():
                collections.extend(
                    await self._aget_collections(single_op_parser)
                )
            return collections
        table = self._get_table_name(op_parser)
        if table is None:
            raise BadRequestError("Collection name must be specified")
        if table in self._acollection_cache:
            return [self._acollection_cache[table]]
        id_column = ParameterParser.get_collection_parameter(
            self.id_column, table
        )
        value_column = ParameterParser.get_collection_parameter(
            self.value_column, table
        )
        pk_column = ParameterParser.get_collection_parameter(
            self.pk_column, table
        )
        id_map_field = ParameterParser.get_collection_parameter(
            self.id_map_field or self.__component__.id_map_field, table
        )
        pk_map_field = ParameterParser.get_collection_parameter(
            self.pk_map_field or self.__component__.pk_map_field, table
        )
        etag_embed_field = ParameterParser.get_collection_parameter(
            self.etag_embed_field, table
        )
        col = PostgreSQLCollection(
            table,
            id_column,
            value_column,
            pk_column,
            id_map_field,
            pk_map_field,
            etag_embed_field,
            self.suppress_fields,
        )
        self._acollection_cache[table] = col
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
            op_parser, collections, ClientHelper(self._client)
        )
        if ncall is None:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        nresult = ncall.invoke()
        result = self._convert_nresult(
            nresult,
            state,
            op_parser,
            collections,
        )
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
            op_parser, collections, AsyncClientHelper(self._aclient)
        )
        if ncall is None:
            return await super().__arun__(
                operation,
                context,
                **kwargs,
            )
        nresult = await ncall.ainvoke()
        result = self._convert_nresult(
            nresult,
            state,
            op_parser,
            collections,
        )
        return Response(result=result, native=dict(result=nresult, call=ncall))

    def _get_ncall(
        self,
        op_parser: StoreOperationParser,
        collections: list[PostgreSQLCollection],
        helper: Any,
    ) -> tuple[NCall | None, dict | None]:
        if len(collections) == 1:
            op_converter = collections[0].op_converter
        call = None
        state = None
        nargs = op_parser.get_nargs()
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            table = self._get_table_name(op_parser)
            id_column = ParameterParser.get_collection_parameter(
                self.id_column, table
            )
            value_column = ParameterParser.get_collection_parameter(
                self.value_column, table
            )
            pk_column = ParameterParser.get_collection_parameter(
                self.pk_column, table
            )
            args = {
                "table": table,
                "config": get_collection_config(op_parser),
                "columns": {
                    "id_column": id_column,
                    "value_column": value_column,
                    "pk_column": pk_column,
                },
                "exists": op_parser.get_where_exists(),
            }
            call = NCall(
                helper.create_collection,
                args,
            )
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            args = {
                "table": self._get_table_name(op_parser),
                "exists": op_parser.get_where_exists(),
            }
            call = NCall(
                helper.drop_collection,
                args,
            )
        # LIST COLLECTION
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            args = OperationConverter.convert_list_collections()
            call = NCall(helper.execute, args)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            args = OperationConverter.convert_has_collection(
                self._get_table_name(op_parser),
            )
            call = NCall(
                helper.execute,
                args,
                None,
            )
        # CREATE INDEX
        elif op_parser.op_equals(StoreOperation.CREATE_INDEX):
            table = self._get_table_name(op_parser)
            value_column = ParameterParser.get_collection_parameter(
                self.value_column, table
            )
            args = {
                "index": op_parser.get_index(),
                "table": table,
                "value_column": value_column,
                "exists": op_parser.get_where_exists(),
            }
            call = NCall(
                helper.create_index,
                args,
            )
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            args = {
                "index": op_parser.get_index(),
                "table": self._get_table_name(op_parser),
                "exists": op_parser.get_where_exists(),
            }
            call = NCall(
                helper.drop_index,
                args,
            )
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            args = {
                "table": self._get_table_name(op_parser),
            }
            call = NCall(helper.list_indexes, args)
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            args = op_converter.convert_get(op_parser.get_key())
            call = NCall(helper.execute, args)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            args, state = op_converter.convert_put(
                op_parser.get_key(),
                op_parser.get_value(),
                op_parser.get_where(),
                op_parser.get_where_exists(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
                {psycopg.errors.UniqueViolation: PreconditionFailedError},
            )
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            args, state = op_converter.convert_update(
                op_parser.get_key(),
                op_parser.get_set(),
                op_parser.get_where(),
                op_parser.get_returning_as_bool(),
            )
            call = NCall(
                helper.execute,
                args,
                None,
                {psycopg.errors.UniqueViolation: PreconditionFailedError},
            )
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            args = op_converter.convert_delete(
                op_parser.get_key(), op_parser.get_where()
            )
            call = NCall(helper.execute, args)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            args = op_converter.convert_query(
                select=op_parser.get_select(),
                where=op_parser.get_where(),
                order_by=op_parser.get_order_by(),
                limit=op_parser.get_limit(),
                offset=op_parser.get_offset(),
            )
            call = NCall(helper.execute, args)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            args = op_converter.convert_count(
                where=op_parser.get_where(),
            )
            call = NCall(helper.execute, args)
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
            call = NCall(helper.transact, args, None)
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            args = {"nargs": nargs}
            call = NCall(helper.close, args)
        return call, state

    def _convert_nresult(
        self,
        nresult: Any,
        state: dict | None,
        op_parser: StoreOperationParser,
        collections: list[PostgreSQLCollection],
    ) -> Any:
        if len(collections) == 1:
            result_converter = collections[0].result_converter
        result: Any = None
        # CREATE COLLECTION
        if op_parser.op_equals(StoreOperation.CREATE_COLLECTION):
            result = nresult
        # DROP COLLECTION
        elif op_parser.op_equals(StoreOperation.DROP_COLLECTION):
            result = nresult
        # LIST COLLECTIONS
        elif op_parser.op_equals(StoreOperation.LIST_COLLECTIONS):
            result = ResultConverter.convert_list_collections(nresult)
        # HAS COLLECTION
        elif op_parser.op_equals(StoreOperation.HAS_COLLECTION):
            result = ResultConverter.convert_has_collection(nresult)
        # CREATE INDEX
        if op_parser.op_equals(StoreOperation.CREATE_INDEX):
            result = nresult
        # DROP INDEX
        elif op_parser.op_equals(StoreOperation.DROP_INDEX):
            result = nresult
        # LIST INDEXES
        elif op_parser.op_equals(StoreOperation.LIST_INDEXES):
            result = nresult
        # GET
        elif op_parser.op_equals(StoreOperation.GET):
            result = result_converter.convert_get(nresult)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            result = result_converter.convert_put(nresult, op_parser, state)
        # UPDATE
        elif op_parser.op_equals(StoreOperation.UPDATE):
            result = result_converter.convert_update(nresult, op_parser, state)
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            result = result_converter.convert_delete(nresult, op_parser)
        # QUERY
        elif op_parser.op_equals(StoreOperation.QUERY):
            result = result_converter.convert_query(nresult, op_parser)
        # COUNT
        elif op_parser.op_equals(StoreOperation.COUNT):
            result = result_converter.convert_count(nresult)
        # BATCH
        elif op_parser.op_equals(StoreOperation.BATCH):
            result = result_converter.convert_batch(nresult, state)
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


class QueryHelper:
    @staticmethod
    def normalize_select(
        processor: ItemProcessor, select: Select, item: tuple
    ) -> dict:
        result: dict = {}
        for i in range(0, len(select.terms)):
            DataAccessor.update_field(
                result,
                processor.resolve_field(select.terms[i].field),
                UpdateOp.PUT,
                item[i],
            )
        return result


class ResultConverter:
    processor: ItemProcessor

    def __init__(
        self,
        processor: ItemProcessor,
    ):
        self.processor = processor

    @staticmethod
    def convert_list_collections(nresult: Any):
        result = []
        for item in nresult:
            result.append(item[0])
        return result

    @staticmethod
    def convert_has_collection(nresult: Any):
        return nresult[0]

    @staticmethod
    def convert_list_indexes(nresult: Any, use_name_type: bool = True):
        def match_brackets(text):
            stack = []
            results = []
            start_idx = -1

            for i, char in enumerate(text):
                if char == "(":
                    if not stack:
                        start_idx = i
                    stack.append(i)
                elif char == ")":
                    stack.pop()
                    if not stack:
                        start = start_idx + 1
                        results.append(text[start:i])
            return results

        def extract_field(definition: str) -> str:
            column_pattern = r"-> '(\w+)'::text"
            matches = re.findall(column_pattern, index_def)
            return ".".join(matches) or "*"

        def extract_fields(definition: str) -> list[dict]:
            matches = match_brackets(definition)
            match = matches[0]
            path_type_pattern = (
                r"->\s*'([^']+)'::text|\->>\s*'([^']+)'::text|\)\s*::\s*(\w+)"
            )
            order_pattern = r"(DESC|ASC)$"
            splits = str(match).split(",")
            results: list[dict] = []
            for field_expr in splits:
                field_parts = []
                type_ = "text"

                # Extract ->, ->>, and ::type
                for path_match in re.finditer(path_type_pattern, field_expr):
                    if path_match.group(1):  # Matches -> 'field'
                        field_parts.append(path_match.group(1))
                    elif path_match.group(2):  # Matches ->> 'field'
                        field_parts.append(path_match.group(2))
                    elif path_match.group(3):  # Matches ::type
                        type_ = path_match.group(3)

                # Extract order (DESC/ASC)
                order_match = re.search(order_pattern, field_expr)
                order_modifier = order_match.group(1) if order_match else "ASC"

                # Combine field parts into dot-separated path
                field = ".".join(field_parts)

                # Append the result
                results.append(
                    {"field": field, "type": type_, "order": order_modifier}
                )
            return results

        def extract_with_values(sql):
            match = re.search(r"WITH\s*\((.*?)\)", sql, re.IGNORECASE)
            if not match:
                return {}
            with_content = match.group(1)
            pairs = re.findall(
                r'(\w+)\s*=\s*[\'"]?([^\'",]+)[\'"]?', with_content
            )
            return {key: value for key, value in pairs}

        type_map = {
            "text": "string",
            "numeric": "number",
            "boolean": "boolean",
            "jsonb": "object",
        }

        result: list = []
        for row in nresult:
            index_name, index_type, index_def = row
            if "UNIQUE INDEX" in index_def:
                continue
            if index_type == "btree":
                if "," not in index_def:
                    field_def = extract_fields(index_def)[0]
                    field = field_def["field"]
                    field_type = type_map[field_def["type"]]
                    name_type = BaseIndexHelper.get_type_from_name(index_name)
                    if "DESC" in index_def:
                        result.append(
                            DescIndex(
                                name=index_name,
                                field=field_def["field"],
                                field_type=field_type,
                            )
                        )
                    elif use_name_type and name_type == "asc":
                        result.append(
                            AscIndex(
                                name=index_name,
                                field=field,
                                field_type=field_type,
                            )
                        )
                    elif use_name_type and name_type == "array":
                        result.append(
                            ArrayIndex(
                                name=index_name,
                                field=field,
                                field_type=field_type,
                            )
                        )
                    elif use_name_type and name_type == "hash":
                        result.append(
                            HashIndex(
                                name=index_name,
                                field=field,
                                field_type=field_type,
                            )
                        )
                    elif use_name_type and name_type == "field":
                        result.append(
                            FieldIndex(
                                name=index_name,
                                field=field,
                                field_type=field_type,
                            )
                        )
                    else:
                        result.append(
                            RangeIndex(
                                name=index_name,
                                field=field,
                                field_type=field_type,
                            )
                        )
                elif "," in index_def:
                    index_parts: list = []
                    field_defs = extract_fields(index_def)
                    name_types: list = (
                        BaseIndexHelper.get_composite_types_from_name(
                            index_name
                        )
                    )
                    if len(field_defs) != len(name_types):
                        name_types = [None] * len(field_defs)
                    for field_def, name_type in zip(field_defs, name_types):
                        field = field_def["field"]
                        field_type = type_map[field_def["type"]]
                        if field_def["order"] == "DESC":
                            index_parts.append(
                                DescIndex(field=field, field_type=field_type)
                            )
                        else:
                            if use_name_type and name_type == "array":
                                index_parts.append(
                                    ArrayIndex(
                                        field=field, field_type=field_type
                                    )
                                )
                            elif use_name_type and name_type == "range":
                                index_parts.append(
                                    RangeIndex(
                                        field=field, field_type=field_type
                                    )
                                )
                            elif use_name_type and name_type == "hash":
                                index_parts.append(
                                    HashIndex(
                                        field=field, field_type=field_type
                                    )
                                )
                            elif use_name_type and name_type == "field":
                                index_parts.append(
                                    FieldIndex(
                                        field=field, field_type=field_type
                                    )
                                )
                            else:
                                index_parts.append(
                                    AscIndex(
                                        field=field, field_type=field_type
                                    )
                                )
                    result.append(
                        CompositeIndex(name=index_name, fields=index_parts)
                    )
                else:
                    result.append(FieldIndex(name=index_name, field=index_def))
            elif index_type == "hash":
                field_def = extract_fields(index_def)[0]
                result.append(
                    HashIndex(
                        name=index_name,
                        field=field_def["field"],
                        field_type=type_map[field_def["type"]],
                    )
                )
            elif index_type == "gist":
                result.append(
                    GeospatialIndex(
                        name=index_name,
                        field=extract_field(index_def),
                    )
                )
            elif index_type == "gin":
                if "to_tsvector" in index_def:
                    config_pattern = r"to_tsvector\('(\w+)'::regconfig"
                    matches = re.findall(config_pattern, index_def)
                    result.append(
                        TextIndex(
                            name=index_name,
                            field=extract_field(index_def),
                            nconfig=dict(config=matches[0].replace("'", "")),
                        )
                    )
                else:
                    excluded_pattern = r"\{[^']+\}"
                    matches = re.findall(excluded_pattern, index_def)
                    excluded = [
                        match.strip("{}").replace(",", ".")
                        for match in matches
                    ]
                    result.append(
                        WildcardIndex(
                            name=index_name,
                            field=extract_field(index_def),
                            excluded=excluded,
                        )
                    )
            elif index_type == "ivfflat" or index_type == "hnsw":
                metric_map = {
                    "vector_cosine_ops": VectorIndexMetric.COSINE,
                    "vector_ip_ops": VectorIndexMetric.DOT_PRODUCT,
                    "vector_l2_ops": VectorIndexMetric.EUCLIDEAN,
                }
                structure_map = {
                    "ivfflat": VectorIndexStructure.FLAT,
                    "hnsw": VectorIndexStructure.HNSW,
                }
                dimension_str = index_name.split("_")[-1]
                if dimension_str.isdigit():
                    dimension = int(dimension_str)
                else:
                    dimension = 0
                structure = structure_map[index_type]
                matches = match_brackets(index_def)
                splits = matches[0].split(" ")
                field = splits[0].replace('"', "").replace("-", ".")
                if len(splits) > 1:
                    metric = metric_map[splits[1]]

                params = extract_with_values(index_def)
                m = int(params["m"]) if "m" in params else None
                partitions = (
                    int(params["lists"]) if "lists" in params else None
                )
                ef_construction = (
                    int(params["ef_construction"])
                    if "ef_construction" in params
                    else None
                )
                result.append(
                    VectorIndex(
                        field=field,
                        dimension=dimension,
                        structure=structure,
                        metric=metric,
                        m=m,
                        partitions=partitions,
                        ef_construction=ef_construction,
                    )
                )
        return result

    def convert_get(self, nresult: Any):
        if nresult is None:
            raise NotFoundError
        return build_item_from_value(
            processor=self.processor,
            value=nresult[0],
            include_value=True,
        )

    def convert_put(
        self, nresult: Any, op_parser: StoreOperationParser, state: dict | None
    ):
        returning = op_parser.get_returning_as_bool()
        where = op_parser.get_where()
        exists = op_parser.get_where_exists()
        if exists is False:
            pass
        elif where is not None:
            if nresult == 0:
                raise PreconditionFailedError
        value: Any = state["value"] if state is not None else None
        return build_item_from_value(
            processor=self.processor,
            value=value,
            include_value=(returning or False),
        )

    def convert_update(
        self, nresult: Any, op_parser: StoreOperationParser, state: dict | None
    ):
        where = op_parser.get_where()
        returning = op_parser.get_returning_as_bool()
        if where is None:
            if returning is None:
                if nresult == 0:
                    raise NotFoundError
            else:
                if nresult is None or len(nresult) == 0:
                    raise NotFoundError
        elif where is not None:
            if returning is None:
                if nresult == 0:
                    raise PreconditionFailedError
            else:
                if nresult is None or len(nresult) == 0:
                    raise PreconditionFailedError
        if isinstance(nresult, tuple):
            result = build_item_from_value(
                processor=self.processor, value=nresult[0], include_value=True
            )
        else:
            key = op_parser.get_key()
            result = build_item_from_parts(
                processor=self.processor,
                key=key,
                etag=(
                    state["etag"]
                    if state is not None and "etag" in state
                    else None
                ),
            )
        return result

    def convert_delete(self, nresult: Any, op_parser: StoreOperationParser):
        where = op_parser.get_where()
        if where is None:
            if nresult == 0:
                raise NotFoundError
        elif where is not None:
            if nresult == 0:
                raise PreconditionFailedError
        return None

    def convert_query(self, nresult: Any, op_parser: StoreOperationParser):
        items: list = []
        select = op_parser.get_select()
        for item in nresult:
            if select is None or len(select.terms) == 0:
                nvalue = item[0]
            else:
                nvalue = QueryHelper.normalize_select(
                    self.processor, select, item
                )
            items.append(
                build_item_from_value(
                    processor=self.processor, value=nvalue, include_value=True
                )
            )
        return build_query_result(items)

    def convert_count(self, nresult: Any):
        return nresult[0]

    def convert_batch(self, nresult: Any, state: dict | None):
        result: list = []
        if state is not None:
            for c in state["values"]:
                if c is not None:
                    result.append(
                        build_item_from_value(
                            processor=self.processor,
                            value=c["value"],
                        )
                    )
                else:
                    result.append(None)
        return result


class OperationConverter:
    processor: ItemProcessor
    table: str
    id_column: str
    value_column: str
    pk_column: str | None

    FIELD_TYPE_TEXT: str = "TEXT"
    FIELD_TYPE_NUMERIC: str = "NUMERIC"
    FIELD_TYPE_BOOLEAN: str = "BOOLEAN"
    FIELD_TYPE_JSONB: str = "JSONB"

    def __init__(
        self,
        processor: ItemProcessor,
        table: str,
        id_column: str,
        value_column: str,
        pk_column: str | None,
    ):
        self.processor = processor
        self.table = table
        self.id_column = id_column
        self.value_column = value_column
        self.pk_column = pk_column

    @staticmethod
    def convert_create_collection(
        table: str,
        config: DocumentCollectionConfig | None,
        columns: dict,
        exists: bool | None,
    ) -> dict:
        def get_column_type(type: str) -> str:
            if type == DocumentFieldType.STRING:
                return "TEXT"
            elif type == DocumentFieldType.NUMBER:
                return "NUMERIC"
            raise BadRequestError("Field type not supported")

        pk_column = (
            config.pk_field
            if config is not None and config.pk_field is not None
            else columns["pk_column"]
        )
        pk_type = get_column_type(
            config.pk_type
            if config is not None and config.pk_type is not None
            else DocumentFieldType.STRING
        )
        id_column = (
            config.id_field
            if config is not None and config.id_field is not None
            else columns["id_column"]
        )
        id_type = get_column_type(
            config.id_type
            if config is not None and config.id_type is not None
            else DocumentFieldType.STRING
        )
        value_column = (
            config.value_field
            if config is not None and config.value_field is not None
            else columns["value_column"]
        )
        if exists is False:
            if_not_exists_clause = ""
        else:
            # Handle exceptions instead of the clause below
            # if_not_exists_clause = " IF NOT EXISTS"
            if_not_exists_clause = ""
        if pk_column is not None:
            query = f"""CREATE TABLE{if_not_exists_clause} {table}
                ({id_column} {id_type} PRIMARY KEY,
                {pk_column} {pk_type},
                {value_column} JSONB,
                UNIQUE({id_column}))
                """
        else:
            query = f"""CREATE TABLE{if_not_exists_clause} {table}
                ({id_column} {id_type} PRIMARY KEY,
                {value_column} JSONB,
                UNIQUE({id_column}))
                """
        return {"query": query}

    @staticmethod
    def convert_drop_collection(table: str, exists: bool | None) -> dict:
        if exists is True:
            if_exists = ""
        else:
            # Handle exceptions instead of the clause below
            # if_exists = " IF EXISTS"
            if_exists = ""
        query = f"DROP TABLE{if_exists} {table}"
        return {"query": query}

    @staticmethod
    def convert_list_collections() -> dict:
        query = "SELECT table_name FROM information_schema.tables"
        return {"query": query, "fetchall": True}

    @staticmethod
    def convert_has_collection(table: str) -> dict:
        query = f"""
            SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = '{table}'
            )
            """
        return {"query": query}

    @staticmethod
    def convert_create_index(
        index: Index,
        table: str,
        value_column: str,
        exists: bool | None,
    ) -> dict | None:
        def get_field_type(field_type) -> str | None:
            field_type_map = {
                DocumentFieldType.STRING.value: "TEXT",
                DocumentFieldType.NUMBER.value: "NUMERIC",
                DocumentFieldType.BOOLEAN.value: "BOOLEAN",
            }
            if field_type in field_type_map:
                return field_type_map[field_type]
            return None

        queries: list = []
        name = BaseIndexHelper.convert_index_name(index, table)
        create_str = f"CREATE INDEX {name} ON {table} "
        if isinstance(
            index,
            (
                RangeIndex,
                FieldIndex,
            ),
        ):
            field_type = get_field_type(index.field_type)
            path = OperationConverter._convert_field(
                value_column,
                index.field,
                field_type,
                field_type is None,
            )
            query = f"{create_str} (({path}))"
            queries.append({"query": query})
        elif isinstance(index, AscIndex):
            path = OperationConverter._convert_field(
                value_column,
                index.field,
                get_field_type(index.field_type),
            )
            query = f"{create_str} (({path}) ASC)"
            queries.append({"query": query})
        elif isinstance(index, DescIndex):
            path = OperationConverter._convert_field(
                value_column,
                index.field,
                get_field_type(index.field_type),
            )
            query = f"{create_str} (({path}) DESC)"
            queries.append({"query": query})
        elif isinstance(index, HashIndex):
            path = OperationConverter._convert_field(
                value_column,
                index.field,
                get_field_type(index.field_type),
                True,
            )
            query = f"{create_str} USING HASH (({path}))"
            queries.append({"query": query})
        elif isinstance(index, CompositeIndex):
            index_str_parts: list = []
            for part in index.fields:
                path = OperationConverter._convert_field(
                    value_column,
                    part.field,
                    get_field_type(part.field_type),
                )
                if isinstance(part, DescIndex):
                    index_str_parts.append(f"({path}) DESC")
                elif isinstance(
                    part, (AscIndex, FieldIndex, RangeIndex, HashIndex)
                ):
                    index_str_parts.append(f"({path}) ASC")
                else:
                    # not supported
                    return None
            index_str = ", ".join(index_str_parts)
            query = f"{create_str} ({index_str})"
            queries.append({"query": query})
        elif isinstance(index, WildcardIndex):
            if index.field == "*":
                column = value_column
            else:
                column = OperationConverter._convert_field(
                    value_column, index.field
                )
            if len(index.excluded) > 0:
                column = f"{column}"
                for exclude_field in index.excluded:
                    split_path = (
                        "{"
                        + ",".join(
                            exclude_field.replace(".", "/")
                            .replace("[", "/")
                            .replace("]", "/")
                            .rstrip("/")
                            .split("/")
                        )
                        + "}"
                    )
                    param = f"{column}, '{split_path}', 'null', false"
                    column = f"jsonb_set({param})"
            else:
                if index.field != "*":
                    column = f"({column})"
            query = f"{create_str} USING GIN ({column})"
            queries.append({"query": query})
        elif isinstance(index, GeospatialIndex):
            queries = []
            query = "CREATE EXTENSION IF NOT EXISTS postgis"
            queries.append({"query": query})
            path = OperationConverter._convert_field(
                value_column,
                index.field,
                get_field_type(index.field_type),
            )
            path = f"({path})::text"
            column = f"ST_GeomFromGeoJSON({path})"
            query = f"{create_str} USING GIST ({column})"
            queries.append({"query": query})
        elif isinstance(index, VectorIndex) and index.field is not None:
            name = f"{name}_{index.dimension}"
            create_str = f"CREATE INDEX {name} ON {table} "
            queries = []
            query = "CREATE EXTENSION IF NOT EXISTS vector"
            queries.append({"query": query})
            new_column = OperationConverter._convert_vector_column_name(
                index.field
            )
            path = OperationConverter._convert_field(
                value_column, index.field, None
            )
            query = f"""
                ALTER TABLE {table} ADD COLUMN {new_column}
                vector({index.dimension}) GENERATED ALWAYS AS
                (({path})::jsonb::text::vector) STORED
                """
            queries.append({"query": query})
            metric_map = {
                VectorIndexMetric.COSINE: "vector_cosine_ops",
                VectorIndexMetric.DOT_PRODUCT: "vector_ip_ops",
                VectorIndexMetric.EUCLIDEAN: "vector_l2_ops",
            }
            path = OperationConverter._convert_field(
                value_column, index.field, "string"
            )
            metric = metric_map[index.metric]
            with_params: list = []
            if index.structure == VectorIndexStructure.HNSW:
                query = f"{create_str} USING hnsw ({new_column} {metric})"
                if index.m is not None:
                    with_params.append(f"m = {index.m}")
                if index.ef_construction is not None:
                    with_params.append(
                        f"ef_construction = {index.ef_construction}"
                    )
            elif (
                index.structure == VectorIndexStructure.FLAT
                or index.structure is None
            ):
                query = f"{create_str} USING ivfflat ({new_column} {metric})"
                if index.partitions is not None:
                    with_params.append(f"lists = {index.partitions}")
            if len(with_params) > 0:
                param_str = ", ".join(with_params)
                query = f"{query} WITH ({param_str})"
            queries.append({"query": query})
        elif isinstance(index, TextIndex):
            path = OperationConverter._convert_field(
                value_column, index.field, "string"
            )
            if index.nconfig and "config" in index.nconfig:
                config = index.nconfig["config"]
            else:
                config = "english"
            query = f"""
                    {create_str} USING GIN
                    (to_tsvector('{config}', {path}))
                    """
            queries.append({"query": query})
        else:
            return None
        if exists is False:
            return {"ops": queries}
        else:
            # statement = "; ".join([q["query"] for q in queries])
            # query = f"""
            #     DO $$
            #     BEGIN
            #     IF NOT EXISTS (
            #         SELECT 1
            #         FROM pg_indexes
            #         WHERE schemaname = 'public'
            #         AND indexname = '{name}'
            #     ) THEN
            #         {statement};
            #     END IF;
            #     END $$;
            #     """
            # Handle exceptions later instead of the clause below.
            # return {"ops": [{"query": query}]}
            return {"ops": queries}

    @staticmethod
    def _convert_vector_column_name(field: str) -> str:
        field = field.replace(".", "-").replace("[", "-").replace("]", "-")
        field = f'"{field}"'
        return field

    @staticmethod
    def convert_drop_index(
        index: Index, table: str, exists: bool | None
    ) -> dict:
        if exists is True:
            if_exists = ""
        else:
            # Handle exceptions later.
            # if_exists = " IF EXISTS"
            if_exists = ""
        queries: list = []
        name = BaseIndexHelper.convert_index_name(index, table)
        if isinstance(index, VectorIndex):
            name = f"{name}_{index.dimension}"
        query = f"DROP INDEX{if_exists} {name}"
        queries.append({"query": query})
        if isinstance(index, VectorIndex):
            vector_column_name = (
                OperationConverter._convert_vector_column_name(index.field)
            )
            query = f"""ALTER TABLE {table} DROP COLUMN
                    IF EXISTS
                    {vector_column_name}"""
            queries.append({"query": query})
        return {"ops": queries}

    @staticmethod
    def convert_list_indexes(table: str) -> dict:
        query = f"""
            SELECT
                i.relname AS index_name,
                am.amname AS index_type,
                pg_get_indexdef(ix.indexrelid) AS index_definition
            FROM
                pg_class t
                JOIN pg_index ix ON t.oid = ix.indrelid
                JOIN pg_class i ON i.oid = ix.indexrelid
                JOIN pg_am am ON i.relam = am.oid
            WHERE
                t.relname = '{table}';
            """
        return {"query": query, "fetchall": True}

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
                ops.append(args)
                states.append(state)
            elif op_parser.op_equals(StoreOperation.UPDATE):
                args, state = converter.convert_update(
                    op_parser.get_key(),
                    op_parser.get_set(),
                    op_parser.get_where(),
                    op_parser.get_returning_as_bool(),
                )
                ops.append(args)
                states.append(state)
            elif op_parser.op_equals(StoreOperation.DELETE):
                args = converter.convert_delete(
                    op_parser.get_key(), op_parser.get_where()
                )
                ops.append(args)
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
                ops.append(args)
                states.append(state)
            elif op_parser.op_equals(StoreOperation.DELETE):
                args = self.convert_delete(
                    op_parser.get_key(), op_parser.get_where()
                )
                ops.append(args)
                states.append(None)
        return {"ops": ops}, {"values": states}

    def convert_get(self, key: Value) -> dict:
        query = f"""SELECT {self.value_column} FROM {self.table}
                    WHERE {self.convert_key_comparison(key)}"""
        return {"query": query}

    def convert_put(
        self,
        key: Value,
        value: dict,
        where: Expression | None,
        exists: bool | None,
    ) -> tuple[dict, dict | None]:
        document = self.processor.add_embed_fields(value, key)
        if key is not None:
            key = self.processor.get_key_from_key(key)
        else:
            key = self.processor.get_key_from_value(document)
        if (where is None and exists is None) or exists is False:
            id, pk = self.processor.get_id_pk_from_key(key)
            if self.pk_column is not None:
                columns = f"""({self.id_column},
                    {self.pk_column}, {self.value_column})"""
                json_string = self._safe_json_dumps(document)
                values = f"""({self.convert_expr(id)},
                    {self.convert_expr(pk)}, '{json_string}')"""
            else:
                columns = f"({self.id_column}, {self.value_column})"
                json_string = self._safe_json_dumps(document)
                values = f"({self.convert_expr(id)}, '{json_string}')"
            query = f"INSERT INTO {self.table} {columns} VALUES {values}"
            if exists is None:
                query = f"""{query} ON CONFLICT ({self.id_column})
                    DO UPDATE SET {self.value_column} =
                    EXCLUDED.{self.value_column}"""
        elif exists is True or where is not None:
            where_expr = self.convert_key_comparison(key)
            if exists is not True and where is not None:
                where_expr = f"{where_expr} AND {self.convert_expr(where)}"
            query = f"""UPDATE {self.table}
                SET {self.value_column} = '{self._safe_json_dumps(document)}'
                WHERE {where_expr}"""
        return {"query": query, "rowcount": True}, {"value": document}

    def convert_update(
        self,
        key: Value,
        set: Update,
        where: Expression | None,
        returning: bool | None,
    ) -> tuple[dict, dict | None]:
        uset = copy.deepcopy(set)
        state = None
        if self.processor.needs_local_etag():
            etag = self.processor.generate_etag()
            uset = self.processor.add_etag_update(uset, etag)
            state = {"etag": etag}
        query = f"UPDATE {self.table} SET {self.convert_update_ops(uset)}"
        where_expr = self.convert_key_comparison(key)
        if where is not None:
            where_expr = f"{where_expr} AND {self.convert_expr(where)}"
        query = f"{query} WHERE {where_expr}"
        if returning:
            query = f"{query} RETURNING {self.value_column}"
        if not returning:
            return {"query": query, "rowcount": True}, state
        return {"query": query}, state

    def convert_delete(self, key: Value, where: Expression | None) -> dict:
        where_expr = self.convert_key_comparison(key)
        if where is not None:
            where_expr = f"{where_expr} AND {self.convert_expr(where)}"
        query = f"DELETE FROM {self.table} WHERE {where_expr}"
        return {"query": query, "rowcount": True}

    def convert_query(
        self,
        select: Select | None = None,
        where: Expression | None = None,
        order_by: OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> dict:
        query = f"""SELECT {self.convert_select(
            select)} FROM {self.table}"""
        if where is not None:
            query = f"{query} WHERE {self.convert_expr(where)}"
        if order_by is not None:
            query = f"""{query} ORDER BY {self.convert_order_by(
                order_by)}"""
        if limit is not None:
            if offset is not None:
                query = f"""{query} OFFSET {offset} LIMIT {limit}"""
            else:
                query = f"""{query} OFFSET 0 LIMIT {limit}"""
        return {"query": query, "fetchall": True}

    def convert_count(self, where: Expression | None = None) -> dict:
        query = f"SELECT COUNT(*) FROM {self.table}"
        if where is not None:
            query = f"{query} WHERE {self.convert_expr(where)}"
        return {"query": query}

    def convert_key_comparison(self, key: Value) -> str:
        id, pk = self.processor.get_id_pk_from_key(key)
        expr = f"{self.id_column} = {self.convert_expr(id)}"
        if self.pk_column is not None:
            expr = f"{expr} AND {self.pk_column} = {self.convert_expr(pk)}"
        return expr

    @staticmethod
    def _convert_field(
        value_column: str,
        field: str,
        field_type: str | None = None,
        cast_value: bool = False,
    ):
        path = (
            field.replace(".", "/")
            .replace("[", "/")
            .replace("]", "")
            .rstrip("/")
        )
        splits = path.split("/")
        nsplits = []
        for split in splits:
            if split.isnumeric():
                nsplits.append(split)
            else:
                nsplits.append(f"'{split}'")
        splits = nsplits
        if cast_value:
            splits.insert(0, f"{value_column}::jsonb")
        else:
            splits.insert(0, value_column)
        if field_type == OperationConverter.FIELD_TYPE_TEXT:
            path = "->".join(splits[0:-1])
            path = f"{path}->>{splits[-1]}"
        elif field_type == OperationConverter.FIELD_TYPE_NUMERIC:
            path = "->".join(splits[0:-1])
            path = f"{path}->>{splits[-1]}"
            path = f"({path})::NUMERIC"
        elif field_type == OperationConverter.FIELD_TYPE_BOOLEAN:
            path = "->".join(splits[0:-1])
            path = f"{path}->>{splits[-1]}"
            path = f"({path})::BOOLEAN"
        else:
            path = "->".join(splits)
        return path

    def convert_field(
        self,
        field: Field | str,
        field_type: str | None = None,
        cast_value: bool = False,
    ) -> str:
        path: str
        if isinstance(field, str):
            path = self.processor.resolve_field(field)
        elif isinstance(field, Field):
            path = self.processor.resolve_field(field.path)
        return OperationConverter._convert_field(
            self.value_column, path, field_type, cast_value
        )

    def convert_expr(self, expr: Expression | None) -> str:
        if expr is None:
            return "NULL"
        if isinstance(expr, str):
            return f"'{expr}'"
        if isinstance(expr, bool):
            return str(expr).upper()
        if isinstance(expr, (int, float)):
            return json.dumps(expr)
        if isinstance(expr, (dict, list)):
            return f"'{self._safe_json_dumps(expr)}'"
        if isinstance(expr, Field):
            return self.convert_field(expr)
        if isinstance(expr, Function):
            return self.convert_func(expr)
        if isinstance(expr, Comparison):
            return self.convert_comparison(expr)
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

    def convert_comparison(self, expr: Comparison) -> str:
        def get_field_type(value: Any) -> str | None:
            ftype = None
            if isinstance(value, bool):
                ftype = OperationConverter.FIELD_TYPE_BOOLEAN
            elif isinstance(value, (int, float)):
                ftype = OperationConverter.FIELD_TYPE_NUMERIC
            elif isinstance(value, str):
                ftype = OperationConverter.FIELD_TYPE_TEXT
            elif isinstance(value, list) and len(value) > 0:
                return get_field_type(value[0])
            return ftype

        value = None
        if isinstance(expr.lexpr, Field):
            value = expr.rexpr
        elif isinstance(expr.rexpr, Field):
            value = expr.lexpr
        field_type = get_field_type(value)

        if isinstance(expr.lexpr, Field):
            lhs = self.convert_field(expr.lexpr, field_type)
        else:
            lhs = self.convert_expr(expr.lexpr)
        if isinstance(expr.rexpr, Field):
            rhs = self.convert_field(expr.rexpr, field_type)
        else:
            rhs = self.convert_expr(expr.rexpr)

        if expr.op == ComparisonOp.BETWEEN and isinstance(expr.rexpr, list):
            return f"""{lhs} >= {self.convert_expr(
                expr.rexpr[0])} AND {lhs} <= {self.convert_expr(
                    expr.rexpr[1])}"""
        elif (
            expr.op == ComparisonOp.IN or expr.op == ComparisonOp.NIN
        ) and isinstance(expr.rexpr, list):
            lst = ", ".join(self.convert_expr(i) for i in expr.rexpr)
            rhs = f"({lst})"
        return f"""{lhs} {expr.op.value} {rhs}"""

    def convert_func(self, expr: Function) -> str:
        namespace = expr.namespace
        name = expr.name
        args = expr.args
        if namespace == FunctionNamespace.BUILTIN:
            if name == QueryFunctionName.IS_TYPE:
                field = self.convert_field(args[0])
                field_type = args[1]
                if isinstance(field_type, str):
                    field_type = DocumentFieldType(field_type.lower()).value
                return f"JSONB_TYPEOF({field}) = '{field_type}'"
            if name == QueryFunctionName.IS_DEFINED:
                field = self.convert_field(args[0])
                return f"{field} IS NOT NULL"
            if name == QueryFunctionName.IS_NOT_DEFINED:
                field = self.convert_field(args[0])
                return f"{field} IS NULL"
            if name == QueryFunctionName.LENGTH:
                field = self.convert_field(
                    args[0], OperationConverter.FIELD_TYPE_TEXT
                )
                return f"LENGTH({field})"
            if name == QueryFunctionName.CONTAINS:
                field = self.convert_field(
                    args[0], OperationConverter.FIELD_TYPE_TEXT
                )
                value = args[1]
                return f"{field} LIKE '%{value}%'"
            if name == QueryFunctionName.STARTS_WITH:
                field = self.convert_field(
                    args[0], OperationConverter.FIELD_TYPE_TEXT
                )
                value = args[1]
                return f"{field} LIKE '{value}%'"
            if name == QueryFunctionName.ARRAY_LENGTH:
                field = self.convert_field(args[0])
                return f"JSONB_ARRAY_LENGTH({field})"
            if name == QueryFunctionName.ARRAY_CONTAINS:
                field = self.convert_field(args[0])
                if isinstance(args[1], dict):
                    value = f"[{self._safe_json_dumps(args[1])}]"
                else:
                    value = self._safe_json_dumps(args[1])
                return f"{field} @> '{value}'"
            if name == QueryFunctionName.ARRAY_CONTAINS_ANY:
                field = self.convert_field(args[0])
                clauses = []
                for item in args[1]:
                    if isinstance(args[1], dict):
                        value = f"[{self._safe_json_dumps(item)}]"
                    else:
                        value = self._safe_json_dumps(item)
                    clauses.append(f"{field} @> '{value}'")
                return f"({str.join(' OR ', clauses)})"
        raise BadRequestError(f"Function {name} not supported")

    def convert_order_by(self, order_by: OrderBy) -> str:
        str_terms = []
        for term in order_by.terms:
            _str_term = self.convert_field(term.field)
            if term.direction is not None:
                _str_term = f"{_str_term} {term.direction.value}"
            str_terms.append(_str_term)
        return ", ".join([t for t in str_terms])

    def convert_select(self, select: Select | None) -> str:
        if select is None or len(select.terms) == 0:
            return self.value_column
        str_terms = []
        for term in select.terms:
            _str_term = self.convert_expr(Field(path=term.field))
            if term.alias is not None:
                _str_term = f"{_str_term} AS {term.alias}"
            str_terms.append(_str_term)
        return ", ".join([t for t in str_terms])

    def convert_update_ops(self, update: Update) -> str:
        def get_jsonb_path(field: str) -> tuple:
            field = self.processor.resolve_field(field)
            path = (
                field.replace(".", "/")
                .replace("[", "/")
                .replace("]", "")
                .rstrip("/")
            )
            splits = path.split("/")
            if splits[-1] == "-":
                splits[-1] = "-1"
            jsonb_path = f"'{{{', '.join(splits)}}}'"
            return jsonb_path, splits

        str = self.value_column
        for operation in update.operations:
            op = operation.op
            jsonb_path, splits = get_jsonb_path(operation.field)
            if op == UpdateOp.PUT:
                value = f"'{self._safe_json_dumps(operation.args[0])}'"
                str = f"JSONB_SET({str}, {jsonb_path}, {value})"
            elif op == UpdateOp.INSERT:
                value = f"'{self._safe_json_dumps(operation.args[0])}'"
                if splits[-1] == "-1":
                    str = f"JSONB_INSERT({str}, {jsonb_path}, {value}, true)"
                elif splits[-1].isnumeric():
                    str = f"JSONB_INSERT({str}, {jsonb_path}, {value})"
                else:
                    str = f"JSONB_SET({str}, {jsonb_path}, {value})"
            elif op == UpdateOp.DELETE:
                str = f"{str} #- {jsonb_path}"
            elif op == UpdateOp.INCREMENT:
                value = operation.args[0]
                field_text = self.convert_field(
                    operation.field, OperationConverter.FIELD_TYPE_TEXT
                )
                coal = f"COALESCE({field_text}, '0')::NUMERIC + {value}"
                coal = f"({coal})::TEXT::JSONB"
                str = f"JSONB_SET({str}, {jsonb_path}, {coal})"
            elif op == UpdateOp.MOVE:
                field_jsonb = self.convert_field(operation.field)
                dest_jsonb_path, s = get_jsonb_path(operation.args[0].path)
                str = f"JSONB_SET({str}, {dest_jsonb_path}, {field_jsonb})"
                str = f"{str} #- {jsonb_path}"
            elif op == UpdateOp.ARRAY_UNION:
                field_path = self.convert_field(operation.field)
                value = self.convert_expr(operation.args[0])
                agg = "JSONB_AGG(t4.value)"
                value_query = f"""(SELECT {agg}
                    FROM (SELECT t1.value
                    FROM JSONB_ARRAY_ELEMENTS({field_path})
                    AS t1
                    UNION ALL
                    SELECT t2.value
                    FROM JSONB_ARRAY_ELEMENTS({value})
                    AS t2
                    WHERE NOT EXISTS (
                    SELECT 1
                    FROM JSONB_ARRAY_ELEMENTS({self.table}.{field_path})
                    AS t3
                    WHERE t3.value = t2.value))
                    AS t4)
                    """
                str = f"JSONB_SET({str}, {jsonb_path}, {value_query})"
            elif op == UpdateOp.ARRAY_REMOVE:
                field_path = self.convert_field(operation.field)
                value = self.convert_expr(operation.args[0])
                agg = "JSONB_AGG(t2.value)"
                value_query = f"""(SELECT {agg}
                    FROM (SELECT t1.value
                    FROM JSONB_ARRAY_ELEMENTS({field_path})
                    AS t1
                    WHERE NOT EXISTS (
                    SELECT 1
                    FROM JSONB_ARRAY_ELEMENTS({value})
                    WHERE value = t1.value))
                    AS t2)
                    """
                str = f"JSONB_SET({str}, {jsonb_path}, {value_query})"
            else:
                raise BadRequestError(
                    f"Operation {operation.op.value} is not supported"
                )
        return f"{self.value_column} = {str}"

    def _safe_json_dumps(self, value) -> str:
        return json.dumps(value).replace("'", "''")


class ClientHelper:
    client: Any

    def __init__(
        self,
        client: Any,
    ):
        self.client = client

    def create_collection(
        self,
        table: str,
        config: DocumentCollectionConfig | None,
        columns: dict,
        exists: bool | None,
    ) -> CollectionResult:
        args: Any = OperationConverter.convert_create_collection(
            table, config, columns, exists
        )
        status = CollectionStatus.CREATED
        try:
            self.execute(**args)
        except psycopg.errors.DuplicateTable:
            status = CollectionStatus.EXISTS
            if exists is False:
                raise ConflictError

        index_results = []
        if config and config.indexes:
            for index in config.indexes:
                index_results.append(
                    self.create_index(
                        index, table, columns["value_column"], None
                    )
                )
        return CollectionResult(status=status, indexes=index_results)

    def drop_collection(
        self, table: str, exists: bool | None
    ) -> CollectionResult:
        args = OperationConverter.convert_drop_collection(table, exists)
        try:
            self.execute(**args)
        except psycopg.errors.UndefinedTable:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    def create_index(
        self,
        index: Index,
        table: str,
        value_column: str,
        exists: bool | None,
    ) -> IndexResult:
        indexes = self.list_indexes(table, False)
        status, match_index = BaseIndexHelper.check_index_status(
            indexes, index
        )
        if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
            if exists is False:
                raise ConflictError
            return IndexResult(status=status, index=match_index)
        args = OperationConverter.convert_create_index(
            index, table, value_column, exists
        )
        if args:
            self.batch(**args)
            return IndexResult(status=IndexStatus.CREATED)
        else:
            return IndexResult(status=IndexStatus.NOT_SUPPORTED)

    def drop_index(
        self, index: Index, table: str, exists: bool | None
    ) -> IndexResult:
        args = OperationConverter.convert_drop_index(index, table, exists)
        try:
            self.batch(**args)
        except psycopg.errors.UndefinedObject:
            if exists is True:
                raise NotFoundError
            return IndexResult(status=IndexStatus.NOT_EXISTS)
        return IndexResult(status=IndexStatus.DROPPED)

    def list_indexes(
        self,
        table: str,
        use_name_type: bool = True,
    ) -> list[Index]:
        args = OperationConverter.convert_list_indexes(table)
        nresult = self.execute(**args)
        return ResultConverter.convert_list_indexes(nresult, use_name_type)

    def execute(
        self, query: str, rowcount: bool = False, fetchall: bool = False
    ) -> Any:
        cursor = self.client.cursor()
        try:
            cursor.execute(query)
            if rowcount:
                return cursor.rowcount
            if fetchall:
                return cursor.fetchall()
            if cursor.rowcount > 0:
                return cursor.fetchone()
        finally:
            self.client.commit()
            cursor.close()

    def batch(self, ops: list) -> Any:
        cursor = self.client.cursor()
        try:
            for op in ops:
                cursor.execute(op["query"])
        finally:
            self.client.commit()
            cursor.close()

    def transact(self, ops: list) -> Any:
        result = []
        cursor = self.client.cursor()
        try:
            for op in ops:
                cursor.execute(op["query"])
                if cursor.rowcount == 0:
                    raise ConflictError
                if "rowcount" in op and op["rowcount"]:
                    result.append(cursor.rowcount)
                elif cursor.rowcount > 0:
                    result.append(cursor.fetchone())
            self.client.commit()
        except Exception:
            self.client.rollback()
            raise
        finally:
            cursor.close()
        return result

    def close(self, nargs: Any) -> Any:
        pass


class AsyncClientHelper:
    client: Any

    def __init__(
        self,
        client: Any,
    ):
        self.client = client

    async def create_collection(
        self,
        table: str,
        config: DocumentCollectionConfig | None,
        columns: dict,
        exists: bool | None,
    ) -> CollectionResult:
        args: Any = OperationConverter.convert_create_collection(
            table, config, columns, exists
        )
        status = CollectionStatus.CREATED
        try:
            await self.execute(**args)
        except psycopg.errors.DuplicateTable:
            status = CollectionStatus.EXISTS
            if exists is False:
                raise ConflictError

        index_results = []
        if config and config.indexes:
            for index in config.indexes:
                index_results.append(
                    await self.create_index(
                        index, table, columns["value_column"], None
                    )
                )
        return CollectionResult(status=status, indexes=index_results)

    async def drop_collection(
        self, table: str, exists: bool | None
    ) -> CollectionResult:
        args = OperationConverter.convert_drop_collection(table, exists)
        try:
            await self.execute(**args)
        except psycopg.errors.UndefinedTable:
            if exists is True:
                raise NotFoundError
            return CollectionResult(status=CollectionStatus.NOT_EXISTS)
        return CollectionResult(status=CollectionStatus.DROPPED)

    async def create_index(
        self,
        index: Index,
        table: str,
        value_column: str,
        exists: bool | None,
    ) -> IndexResult:
        indexes = await self.list_indexes(table, False)
        status, match_index = BaseIndexHelper.check_index_status(
            indexes, index
        )
        if status == IndexStatus.EXISTS or status == IndexStatus.COVERED:
            if exists is False:
                raise ConflictError
            return IndexResult(status=status, index=match_index)
        args = OperationConverter.convert_create_index(
            index, table, value_column, exists
        )
        if args:
            await self.batch(**args)
            return IndexResult(status=IndexStatus.CREATED)
        else:
            return IndexResult(status=IndexStatus.NOT_SUPPORTED)

    async def drop_index(
        self, index: Index, table: str, exists: bool | None
    ) -> IndexResult:
        args = OperationConverter.convert_drop_index(index, table, exists)
        try:
            await self.batch(**args)
        except psycopg.errors.UndefinedObject:
            if exists is True:
                raise NotFoundError
            return IndexResult(status=IndexStatus.NOT_EXISTS)
        return IndexResult(status=IndexStatus.DROPPED)

    async def list_indexes(
        self,
        table: str,
        use_name_type: bool = True,
    ) -> list[Index]:
        args = OperationConverter.convert_list_indexes(table)
        nresult = await self.execute(**args)
        return ResultConverter.convert_list_indexes(nresult, use_name_type)

    async def execute(
        self, query: str, rowcount: bool = False, fetchall: bool = False
    ) -> Any:
        cursor = self.client.cursor()
        try:
            await cursor.execute(query)
            if rowcount:
                return cursor.rowcount
            if fetchall:
                return await cursor.fetchall()
            if cursor.rowcount > 0:
                return await cursor.fetchone()
        finally:
            await self.client.commit()
            await cursor.close()

    async def batch(self, ops: list) -> Any:
        cursor = self.client.cursor()
        try:
            for op in ops:
                await cursor.execute(op["query"])
        finally:
            await self.client.commit()
            await cursor.close()

    async def transact(self, ops: list) -> Any:
        result = []
        cursor = self.client.cursor()
        try:
            for op in ops:
                await cursor.execute(op["query"])
                if cursor.rowcount == 0:
                    raise ConflictError
                if "rowcount" in op and op["rowcount"]:
                    result.append(cursor.rowcount)
                elif cursor.rowcount > 0:
                    result.append(await cursor.fetchone())
            await self.client.commit()
        except Exception:
            await self.client.rollback()
            raise
        finally:
            await cursor.close()
        return result

    async def close(self, nargs: Any) -> Any:
        await self.client.close()


class PostgreSQLCollection:
    op_converter: OperationConverter
    result_converter: ResultConverter
    processor: ItemProcessor
    value_column: str

    def __init__(
        self,
        table: str,
        id_column: str,
        value_column: str,
        pk_column: str | None,
        id_map_field: str | None,
        pk_map_field: str | None,
        etag_embed_field: str | None,
        suppress_fields: list[str] | None,
    ) -> None:
        self.processor = ItemProcessor(
            etag_embed_field=etag_embed_field,
            id_map_field=id_map_field,
            pk_map_field=pk_map_field,
            local_etag=True,
            suppress_fields=suppress_fields,
        )
        self.op_converter = OperationConverter(
            self.processor,
            table,
            id_column,
            value_column,
            pk_column,
        )
        self.result_converter = ResultConverter(self.processor)
        self.value_column = value_column
