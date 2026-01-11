from __future__ import annotations

import json
from typing import IO, Any

from x8.core import ArgParser, Operation, OperationParser
from x8.core.exceptions import BadRequestError
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
    QueryFunctionName,
    Value,
)

from ._attributes import Attribute, KeyAttribute, SpecialAttribute
from ._models import (
    ArrayIndex,
    AscIndex,
    BaseIndex,
    CompositeIndex,
    DescIndex,
    FieldIndex,
    GeospatialIndex,
    HashIndex,
    Index,
    MatchCondition,
    RangeIndex,
    SparseVectorIndex,
    TextIndex,
    TTLIndex,
    VectorIndex,
    WildcardIndex,
)
from ._operation import StoreOperation


class StoreOperationParser(OperationParser):
    operation_parsers: list[StoreOperationParser] | None

    def __init__(self, operation: Operation | None):
        super().__init__(operation)
        self.operation_parsers = None

    def is_single_op(self) -> bool:
        operations = self.get_arg("operations")
        return operations is None

    def is_resource_op(self) -> bool:
        if self.get_op_name() in [
            StoreOperation.CREATE_COLLECTION,
            StoreOperation.DROP_COLLECTION,
            StoreOperation.LIST_COLLECTIONS,
            StoreOperation.HAS_COLLECTION,
            StoreOperation.CLOSE,
        ]:
            return True
        return False

    def is_collection_op(self) -> bool:
        if self.get_op_name() in [
            StoreOperation.GET,
            StoreOperation.PUT,
            StoreOperation.UPDATE,
            StoreOperation.DELETE,
            StoreOperation.QUERY,
            StoreOperation.COUNT,
            StoreOperation.BATCH,
            StoreOperation.TRANSACT,
            StoreOperation.WATCH,
            StoreOperation.CREATE_INDEX,
            StoreOperation.DROP_INDEX,
            StoreOperation.LIST_INDEXES,
        ]:
            return True
        return False

    def get_operations(self) -> list[Operation]:
        if self.has_arg("batch"):
            return self.get_arg("batch", convert_dict=True).get("operations")
        if self.has_arg("transaction"):
            return self.get_arg("transaction", convert_dict=True).get(
                "operations"
            )
        return []

    def get_operation_parsers(self) -> list[StoreOperationParser]:
        if self.operation_parsers is None:
            self.operation_parsers = []
            operations = self.get_operations()
            for operation in operations:
                if isinstance(operation, dict):
                    op_name = operation["name"]
                    op_args = operation["args"]
                else:
                    op_name = operation.name
                    op_args = operation.args
                params = self.get_params()
                if params is not None:
                    args: dict | None = {"params": params}
                    if op_args is not None and args is not None:
                        args = op_args | args
                else:
                    args = op_args
                self.operation_parsers.append(
                    StoreOperationParser(
                        ArgParser.replace_operation_params(
                            Operation(name=op_name, args=args), params
                        )
                    )
                )
        return self.operation_parsers

    def get_index(self) -> Index:
        index = self.get_arg("index")
        if index is None:
            raise BadRequestError("Index parameter missing")
        if isinstance(index, BaseIndex):
            return index
        type_map: dict = {
            "field": FieldIndex,
            "range": RangeIndex,
            "hash": HashIndex,
            "composite": CompositeIndex,
            "asc": AscIndex,
            "desc": DescIndex,
            "array": ArrayIndex,
            "ttl": TTLIndex,
            "text": TextIndex,
            "geospatial": GeospatialIndex,
            "vector": VectorIndex,
            "sparse_vector": SparseVectorIndex,
            "wildcard": WildcardIndex,
        }
        if isinstance(index, dict):
            if "type" in index and index["type"] in type_map:
                type = type_map[index["type"]]
                return type.from_dict(index)
        raise BadRequestError("Index format error")

    def get_key(self) -> Any:
        return self.get_arg("key", True)

    def get_id_from_key(self, key: Value) -> Value:
        if key is not None:
            if isinstance(key, (str, int, float, bool)):
                return key
            if isinstance(key, dict) and KeyAttribute.ID in key:
                return key[KeyAttribute.ID]
        return None

    def get_id(self) -> Value:
        return self.get_id_from_key(self.get_key())

    def get_id_as_str(self) -> str:
        id = self.get_id()
        if not isinstance(id, str):
            raise BadRequestError("id is not string")
        return id

    def get_expiry_in_seconds(self) -> int | None:
        expiry = self.get_arg("expiry")
        if expiry:
            return expiry / 1000
        return expiry

    def get_expiry(self) -> int | None:
        return self.get_arg("expiry")

    def get_method(self) -> str | None:
        return self.get_arg("method")

    def get_id_as_str_or_none(self) -> str | None:
        id = self.get_id()
        if id is None:
            return None
        if not isinstance(id, str):
            raise BadRequestError("id is not string")
        return id

    def get_version_from_key(self, key: Value) -> str | None:
        if key is not None:
            if isinstance(key, dict) and KeyAttribute.VERSION in key:
                return key[KeyAttribute.VERSION]
        return None

    def get_version(self) -> str | None:
        return self.get_version_from_key(self.get_key())

    def get_label(self) -> str | None:
        key = self.get_key()
        if key is not None:
            if isinstance(key, dict) and KeyAttribute.LABEL in key:
                return key[KeyAttribute.LABEL]
        return None

    def get_source(self) -> Value:
        return self.get_arg("source", True)

    def get_source_collection(self) -> str | None:
        source = self.get_source()
        if isinstance(source, dict):
            if "collection" in source:
                return source["collection"]
        return None

    def get_source_key(self) -> Value:
        source = self.get_source()
        if isinstance(source, dict):
            if Attribute.KEY in source:
                return source[Attribute.KEY]
        return source

    def get_source_id_as_str(self) -> str:
        source = self.get_source()
        if isinstance(source, str):
            return source
        elif isinstance(source, dict) and Attribute.KEY in source:
            id = self.get_id_from_key(source[Attribute.KEY])
            if isinstance(id, str):
                return id
        raise BadRequestError("id is not string")

    def get_source_version(self) -> str | None:
        return self.get_version_from_key(self.get_source_key())

    def get_attribute(self) -> str:
        attr = self.get_arg("attr")
        if attr is not None:
            return attr
        return Attribute.VALUE

    def get_filter(self) -> dict | None:
        return self.get_arg("filter")

    def get_value(self) -> Any:
        return self.get_arg("value", True)

    def get_value_as_bytes(self) -> bytes | None:
        value = self.get_value()
        if value is None:
            return None
        if isinstance(value, bytes):
            return value
        return json.dumps(value).encode()

    def get_file(self) -> str | None:
        return self.get_arg("file")

    def get_url(self) -> str | None:
        return self.get_arg("url")

    def get_stream(self) -> IO | None:
        return self.get_arg("stream")

    def get_start(self) -> int | None:
        return self.get_arg("start")

    def get_end(self) -> int | None:
        return self.get_arg("end")

    def get_vector_value(self) -> dict:
        value = self.get_value()
        if isinstance(value, list):
            return {"vector": value}
        if isinstance(value, dict):
            return value
        raise BadRequestError("Vector value must be list or dict")

    def get_metadata(self) -> dict | None:
        return self.get_arg("metadata")

    def get_properties(self) -> dict | None:
        return self.get_arg("properties")

    def get_search_as_function(self) -> Function | None:
        funcs = self.get_search_as_functions()
        if funcs is None:
            return None
        if funcs is not None and len(funcs) > 1:
            raise BadRequestError("Multiple search functions found.")
        return funcs[0]

    def get_search_as_functions(self) -> list[Function] | None:
        def _get_functions(expr):
            if expr is None:
                return None
            funcs = []
            if isinstance(expr, Function):
                return [expr]
            elif isinstance(expr, And):
                funcs.extend(_get_functions(expr.lexpr))
                funcs.extend(_get_functions(expr.rexpr))
            else:
                raise BadRequestError(
                    f"Expression not support in search {expr}"
                )
            return funcs

        expr = self.get_search()
        return _get_functions(expr)

    def get_match_condition(self) -> MatchCondition:
        match_condition = MatchCondition()
        expr_list = self.get_where_expr_list()
        for expr in expr_list:
            field = None
            if isinstance(expr, Function):
                if (
                    expr.namespace == FunctionNamespace.BUILTIN
                    and expr.name == QueryFunctionName.EXISTS
                ):
                    match_condition.exists = True
                if (
                    expr.namespace == FunctionNamespace.BUILTIN
                    and expr.name == QueryFunctionName.NOT_EXISTS
                ):
                    match_condition.exists = False
            if isinstance(expr, Comparison):
                if isinstance(expr.lexpr, Field):
                    field = expr.lexpr.path
                    value = expr.rexpr
                elif isinstance(expr.rexpr, Field):
                    field = expr.rexpr.path
                    value = expr.lexpr
                if field == SpecialAttribute.ETAG and isinstance(value, str):
                    if expr.op == ComparisonOp.EQ:
                        match_condition.if_match = value
                    elif expr.op == ComparisonOp.NEQ:
                        match_condition.if_none_match = value
                if field == SpecialAttribute.VERSION and isinstance(
                    value, str
                ):
                    if expr.op == ComparisonOp.EQ:
                        match_condition.if_version_match = value
                    elif expr.op == ComparisonOp.NEQ:
                        match_condition.if_version_not_match = value
                if field == SpecialAttribute.MODIFIED and isinstance(
                    value, float
                ):
                    if expr.op in [ComparisonOp.GT, ComparisonOp.GTE]:
                        match_condition.if_modified_since = value
                    elif expr.op in [ComparisonOp.LT, ComparisonOp.LTE]:
                        match_condition.if_unmodified_since = value
        return match_condition

    def get_where_expr_list(self) -> list[Expression]:
        conditions = []

        def parse_expr(expr):
            if expr is None:
                return
            if isinstance(expr, And):
                parse_expr(expr.lexpr)
                parse_expr(expr.rexpr)
            elif isinstance(expr, Or):
                raise BadRequestError("OR is not supported in WHERE")
            elif isinstance(expr, Not):
                raise BadRequestError("NOT is not supported in WHERE")
            else:
                conditions.append(expr)

        expr = self.get_where()
        parse_expr(expr)
        return conditions

    @staticmethod
    def parse_where_etag(
        where: str | Expression | None,
        params: dict[str, Any] | None = None,
    ) -> Any:
        expr = OperationParser.parse_where(where, params)
        if (
            expr is not None
            and isinstance(expr, Comparison)
            and expr.op == ComparisonOp.EQ
        ):
            if (
                isinstance(expr.lexpr, Field)
                and expr.lexpr.path == SpecialAttribute.ETAG
            ):
                return expr.rexpr
            elif (
                isinstance(expr.rexpr, Field)
                and expr.rexpr.path == SpecialAttribute.ETAG
            ):
                return expr.lexpr
        return None

    def get_where_etag(self) -> Any:
        return StoreOperationParser.parse_where_etag(
            self.get_arg("where"),
            self.get_params(),
        )
