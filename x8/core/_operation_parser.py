from __future__ import annotations

from typing import Any

from x8.ql._functions import QueryFunctionName
from x8.ql._models import (
    Collection,
    Expression,
    Function,
    FunctionNamespace,
    Not,
    OrderBy,
    Select,
    Update,
)

from ._arg_parser import ArgParser
from ._operation import Operation
from .data_model import DataModel


class OperationParser:
    operation: Operation | None

    def __init__(self, operation: Operation | None):
        self.operation = operation

    def op_equals(self, name: str | None) -> bool:
        operation_name = self.get_op_name()
        if operation_name is not None:
            operation_name = operation_name.lower()
        arg_name = name
        if arg_name is not None:
            arg_name = arg_name.lower()
        return operation_name == arg_name

    def get_args(self) -> dict:
        if self.operation is not None and self.operation.args is not None:
            return self.operation.args
        return {}

    def get_nargs(self) -> dict | None:
        return self.get_arg("nargs")

    def get_arg(self, arg: str, convert_dict: bool = False) -> Any:
        if (
            self.operation is not None
            and self.operation.args is not None
            and arg in self.operation.args
        ):
            arg_val = self.operation.args[arg]
            if convert_dict and isinstance(arg_val, DataModel):
                return arg_val.to_dict()
            return arg_val
        return None

    def set_arg(self, arg: str, value: Any) -> None:
        if self.operation is not None:
            if self.operation.args is None:
                self.operation.args = dict()
            self.operation.args[arg] = value

    def has_arg(self, arg: str) -> bool:
        return (
            self.operation is not None
            and self.operation.args is not None
            and arg in self.operation.args
        )

    def get_op_name(self) -> str | None:
        if self.operation is not None:
            return self.operation.name
        return None

    def get_statement(self) -> str | None:
        return self.get_arg("statement")

    @staticmethod
    def parse_select(
        select: str | Select | None,
        params: dict[str, Any] | None = None,
    ) -> Select | None:
        return ArgParser.get_parsed_arg(
            "select",
            select,
            params,
        )

    def get_select(self) -> Select | None:
        return OperationParser.parse_select(
            self.get_arg("select"),
            self.get_params(),
        )

    @staticmethod
    def parse_collection_name(
        collection: str | Collection | None = None,
        params: dict[str, Any] | None = None,
    ) -> str | None:
        col = ArgParser.get_parsed_arg(
            "collection",
            collection,
            params,
        )
        if col is None:
            return None
        return col.name

    def get_collection_name(self) -> str | None:
        return OperationParser.parse_collection_name(
            self.get_arg("collection"),
            self.get_params(),
        )

    @staticmethod
    def parse_set(
        set: str | Update | None,
        params: dict[str, Any] | None = None,
    ) -> Update:
        return ArgParser.get_parsed_arg(
            "set",
            set,
            params,
        )

    def get_set(self) -> Update:
        return OperationParser.parse_set(
            self.get_arg("set"),
            self.get_params(),
        )

    @staticmethod
    def parse_search(
        search: str | Expression | None,
        params: dict[str, Any] | None = None,
    ) -> Select | None:
        return ArgParser.get_parsed_arg(
            "search",
            search,
            params,
        )

    def get_search(self) -> Expression | None:
        return OperationParser.parse_search(
            self.get_arg("search"),
            self.get_params(),
        )

    @staticmethod
    def parse_where(
        where: str | Expression | None,
        params: dict[str, Any] | None = None,
    ) -> Expression | None:
        return ArgParser.get_parsed_arg(
            "where",
            where,
            params,
        )

    def get_where(self) -> Expression | None:
        return OperationParser.parse_where(
            self.get_arg("where"),
            self.get_params(),
        )

    @staticmethod
    def parse_order_by(
        order_by: str | OrderBy | None,
        params: dict[str, Any] | None = None,
    ) -> OrderBy | None:
        return ArgParser.get_parsed_arg(
            "order_by",
            order_by,
            params,
        )

    def get_order_by(self) -> OrderBy | None:
        return OperationParser.parse_order_by(
            self.get_arg("order_by"),
            self.get_params(),
        )

    @staticmethod
    def parse_rank_by(
        rank_by: str | Expression | None,
        params: dict[str, Any] | None = None,
    ) -> Expression | None:
        return ArgParser.get_parsed_arg(
            "rank_by",
            rank_by,
            params,
        )

    def get_rank_by(self) -> Expression | None:
        return OperationParser.parse_rank_by(
            self.get_arg("rank_by"),
            self.get_params(),
        )

    def get_limit(self) -> int | None:
        return self.get_arg("limit")

    def get_offset(self) -> int | None:
        return self.get_arg("offset")

    def get_returning(self) -> str | None:
        return self.get_arg("returning")

    def get_returning_as_bool(self) -> bool | None:
        returning = self.get_arg("returning")
        if returning:
            return returning == "new"
        return None

    def get_continuation(self) -> str | None:
        return self.get_arg("continuation")

    def get_config(self) -> Any:
        return self.get_arg("config")

    def get_params(self) -> dict[str, Any] | None:
        return self.get_arg("params")

    def get_nflags(self) -> dict | None:
        return self.get_arg("nflags")

    @staticmethod
    def parse_where_exists(
        where: str | Expression | None,
        params: dict[str, Any] | None = None,
    ) -> bool | None:
        expr = OperationParser.parse_where(where, params)
        if expr is not None:
            exists = True
            if isinstance(expr, Not):
                expr = expr.expr
                exists = False
            if isinstance(expr, Function):
                if (
                    expr.namespace == FunctionNamespace.BUILTIN
                    and expr.name == QueryFunctionName.EXISTS
                ):
                    return exists
                if (
                    expr.namespace == FunctionNamespace.BUILTIN
                    and expr.name == QueryFunctionName.NOT_EXISTS
                ):
                    return not exists
        return None

    def get_where_exists(self) -> bool | None:
        return OperationParser.parse_where_exists(
            self.get_arg("where"),
            self.get_params(),
        )
