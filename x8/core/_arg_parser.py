from typing import Any

from x8.ql._models import (
    And,
    Comparison,
    Function,
    Not,
    Or,
    Parameter,
    Update,
    UpdateOperation,
)

from ._operation import Operation
from .exceptions import BadRequestError


class ArgParser:
    parsable_args: dict = {
        "statement": "statement",
        "select": "select",
        "collection": "collection",
        "set": "update",
        "search": "search",
        "where": "where",
        "order_by": "order_by",
    }

    @staticmethod
    def convert_execute_operation(
        statement: str, params: dict[str, Any] | None
    ) -> Operation | None:
        operation = ArgParser.get_parsed_arg("statement", statement)
        if operation is None:
            return operation
        operation = ArgParser.replace_operation_params(operation, params)
        if params is not None:
            operation.args["params"] = params
        return operation

    @staticmethod
    def get_parsed_arg(
        arg_name: str,
        arg_value: Any,
        params: dict[str, Any] | None = None,
    ) -> Any:
        if arg_name in ArgParser.parsable_args:
            type = ArgParser.parsable_args[arg_name]
            if not isinstance(arg_value, str):
                return arg_value
            else:
                from x8.ql import QLParser

                parsed_arg = QLParser.parse(arg_value, type)
                if arg_name == "set":
                    parsed_arg = ArgParser.replace_set_params(
                        parsed_arg,
                        params,
                    )
                elif arg_name in ["search", "where"]:
                    parsed_arg = ArgParser.replace_expr_params(
                        parsed_arg, params
                    )
                return parsed_arg
        else:
            return arg_value

    @staticmethod
    def replace_operation_params(
        operation: Operation, params: dict[str, Any] | None
    ) -> Operation:
        if params is None or len(params) == 0:
            return operation
        if operation.args is not None:
            args: dict = {}
            for key, value in operation.args.items():
                args[key] = ArgParser.replace_expr_params(value, params)
            return Operation(name=operation.name, args=args)
        return Operation(name=operation.name)

    @staticmethod
    def replace_set_params(
        set: Update, params: dict[str, Any] | None
    ) -> Update:
        if params is None or len(params) == 0:
            return set
        copy = Update()
        for operation in set.operations:
            copy.operations.append(
                UpdateOperation(
                    field=operation.field,
                    op=operation.op,
                    args=[
                        ArgParser.replace_expr_params(arg, params)
                        for arg in operation.args
                    ],
                )
            )
        return copy

    @staticmethod
    def replace_expr_params(expr, params: dict[str, Any] | None) -> Any:
        if params is None or len(params) == 0:
            return expr
        if isinstance(expr, list):
            return [ArgParser.replace_expr_params(i, params) for i in expr]
        if isinstance(expr, Parameter):
            if expr.name in params:
                return params[expr.name]
            else:
                raise BadRequestError(f"Parameter {expr.name} not found")
        if isinstance(expr, Comparison):
            return Comparison(
                lexpr=ArgParser.replace_expr_params(expr.lexpr, params),
                op=expr.op,
                rexpr=ArgParser.replace_expr_params(expr.rexpr, params),
            )

        if isinstance(expr, Function):
            args = None
            named_args = None
            if expr.args is not None:
                args = [
                    ArgParser.replace_expr_params(arg, params)
                    for arg in expr.args
                ]
            if expr.named_args is not None:
                named_args = dict()
                for k, v in expr.named_args.items():
                    named_args[k] = ArgParser.replace_expr_params(v, params)

            return Function(
                namespace=expr.namespace,
                name=expr.name,
                args=args,
                named_args=named_args,
            )
        if isinstance(expr, And):
            return And(
                lexpr=ArgParser.replace_expr_params(expr.lexpr, params),
                rexpr=ArgParser.replace_expr_params(expr.rexpr, params),
            )
        if isinstance(expr, Or):
            return Or(
                lexpr=ArgParser.replace_expr_params(expr.lexpr, params),
                rexpr=ArgParser.replace_expr_params(expr.rexpr, params),
            )
        if isinstance(expr, Not):
            return Not(expr=ArgParser.replace_expr_params(expr.expr, params))
        return expr
