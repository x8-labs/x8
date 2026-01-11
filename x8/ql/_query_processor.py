import copy
import re
from typing import Any, Callable

from x8.core._data_accessor import DataAccessor
from x8.core.exceptions import BadRequestError

from ._functions import QueryFunctionName
from ._models import (
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
    Select,
    Undefined,
    Update,
    UpdateOp,
    UpdateOperation,
    Value,
)
from ._ql_parser import QLParser


class QueryProcessor:
    @staticmethod
    def count_items(
        items: list[Any],
        where: str | Expression | None = None,
        field_resolver: Callable | None = None,
    ) -> int:
        current_items = items
        current_items = QueryProcessor.filter_items(
            current_items, where, field_resolver
        )
        return len(current_items)

    @staticmethod
    def query_items(
        items: list[Any],
        select: str | Select | None = None,
        where: str | Expression | None = None,
        order_by: str | OrderBy | None = None,
        limit: int | None = None,
        offset: int | None = None,
        field_resolver: Callable | None = None,
    ) -> list[Any]:
        current_items = items
        current_items = QueryProcessor.filter_items(
            current_items, where, field_resolver
        )
        current_items = QueryProcessor.order_items(
            current_items, order_by, field_resolver
        )
        current_items = QueryProcessor.project_items(
            current_items, select, field_resolver
        )
        current_items = QueryProcessor.limit_items(
            current_items, limit, offset
        )
        return current_items

    @staticmethod
    def filter_items(
        items: list[Any],
        where: str | Expression | None = None,
        field_resolver: Callable | None = None,
    ) -> list[Any]:
        result = []
        for item in items:
            if QueryProcessor.filter_item(item, where, field_resolver):
                result.append(item)
        return result

    @staticmethod
    def filter_item(
        item: Any,
        where: str | Expression | None = None,
        field_resolver: Callable | None = None,
    ) -> Any:
        if where is None:
            return item
        if not isinstance(where, str):
            expr: Expression = where
        else:
            expr = QLParser.parse_where(where)
        if bool(QueryProcessor.eval_expr(item, expr, field_resolver)):
            return item
        return None

    @staticmethod
    def order_items(
        items: list[Any],
        order_by: str | OrderBy | None = None,
        field_resolver: Callable | None = None,
    ) -> list[Any]:
        if order_by is None:
            return items
        if isinstance(order_by, OrderBy):
            ob: OrderBy | None = order_by
        else:
            ob = QLParser.parse_order_by(order_by)
        if ob is None:
            return items
        filtered_items = items
        for term in ob.terms:
            if isinstance(term.field, str):
                filtered_items = QueryProcessor.filter_items(
                    filtered_items,
                    f"is_defined({term.field})",
                    field_resolver,
                )

        def comparer(item):
            values = []
            for term in order_by.terms:
                value = QueryProcessor.eval_expr(
                    item, Field(path=term.field), field_resolver
                )
                if term.direction == OrderByDirection.DESC:
                    values.append(DescFieldComparer(value))
                else:
                    values.append(AscFieldComparer(value))
            return tuple(values)

        sorted_items = sorted(filtered_items, key=comparer)
        return sorted_items

    @staticmethod
    def limit_items(
        items: list[Any],
        limit: int | None,
        offset: int | None,
    ) -> list[Any]:
        start = 0
        end = len(items)
        if offset is not None:
            start = offset
        if limit is not None:
            end = start + limit
        return items[start:end]

    @staticmethod
    def project_items(
        items: list[Any],
        select: str | Select | None,
        field_resolver: Callable | None = None,
    ) -> list[Any]:
        result = []
        for item in items:
            result.append(
                QueryProcessor.project_item(item, select, field_resolver)
            )
        return result

    @staticmethod
    def project_item(
        item: Any,
        select: str | Select | None,
        field_resolver: Callable | None = None,
    ) -> Any:
        result: dict = dict()
        if select is None:
            return item
        if isinstance(select, Select):
            sel: Select | None = select
        else:
            sel = QLParser.parse_select(select)
        if sel is None or sel.terms is None or len(sel.terms) == 0:
            return item
        for term in sel.terms:
            val = QueryProcessor.eval_field(
                item, Field(path=term.field), field_resolver
            )
            if isinstance(val, Undefined):
                continue
            alias = term.alias if term.alias is not None else term.field
            QueryProcessor.update_field(
                result,
                UpdateOperation(field=alias, op=UpdateOp.PUT, args=[val]),
                field_resolver,
            )
        return result

    @staticmethod
    def eval_field(
        item: Any,
        field: Field,
        field_resolver: Callable | None = None,
    ) -> Value | Undefined:
        path = field.path
        if field_resolver is not None:
            path = field_resolver(field.path)
        return DataAccessor.get_field(item, path)

    @staticmethod
    def eval_function(
        item: Any,
        expr: Function,
        field_resolver: Callable | None = None,
    ) -> Value:
        if expr.namespace != FunctionNamespace.BUILTIN:
            raise BadRequestError("Function not supported")
        if expr.name == QueryFunctionName.EXISTS:
            return item is not None
        if expr.name == QueryFunctionName.NOT_EXISTS:
            return item is None
        if expr.name == QueryFunctionName.IS_TYPE:
            val = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            type = expr.args[1]
            if not isinstance(type, str):
                raise BadRequestError(
                    "type in IS_TYPE function must be specified as a string"
                )
            if type == "string":
                return isinstance(val, str)
            if type == "number":
                return isinstance(val, (int, float))
            if type == "boolean":
                return isinstance(val, bool)
            if type == "array":
                return isinstance(val, list)
            if type == "object":
                return isinstance(val, dict)
            if type == "null":
                return val is None
            raise BadRequestError("Unknown type passed to IS_TYPE function")
        if expr.name == QueryFunctionName.IS_DEFINED:
            val = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            if isinstance(val, Undefined):
                return False
            return True
        if expr.name == QueryFunctionName.IS_NOT_DEFINED:
            val = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            if isinstance(val, Undefined):
                return True
            return False
        if expr.name == QueryFunctionName.LENGTH:
            val = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            if val is None:
                return 0
            if isinstance(val, str):
                return len(val)
            return 0
        if expr.name == QueryFunctionName.CONTAINS:
            val1 = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            val2 = QueryProcessor.eval_expr(item, expr.args[1], field_resolver)
            if val1 is None:
                return False
            if isinstance(val1, str) and isinstance(val2, str):
                return val2 in val1
            return False
        if expr.name == QueryFunctionName.STARTS_WITH:
            val1 = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            val2 = QueryProcessor.eval_expr(item, expr.args[1], field_resolver)
            if val1 is None:
                return False
            if isinstance(val1, str) and isinstance(val2, str):
                return val1.startswith(val2)
            return False
        if expr.name == QueryFunctionName.ARRAY_LENGTH:
            val = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            if val is None:
                return 0
            if isinstance(val, list):
                return len(val)
            return 0
        if expr.name == QueryFunctionName.ARRAY_CONTAINS:
            val1 = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            val2 = QueryProcessor.eval_expr(item, expr.args[1], field_resolver)
            if val1 is None:
                return False
            if isinstance(val1, list):
                return val2 in val1
            return False
        if expr.name == QueryFunctionName.ARRAY_CONTAINS_ANY:
            val1 = QueryProcessor.eval_expr(item, expr.args[0], field_resolver)
            val2 = QueryProcessor.eval_expr(item, expr.args[1], field_resolver)
            if val1 is None:
                return False
            if isinstance(val1, list) and isinstance(val2, list):
                for item in val2:
                    if item in val1:
                        return True
            return False
        if expr.name == QueryFunctionName.RANDOM:
            import random

            return random.random()
        if expr.name == QueryFunctionName.NOW:
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            formatted_now = now.strftime("%Y-%m-%d %H:%M:%S.%f%z")

            return formatted_now
        raise BadRequestError(f"Function {expr.name} not supported")

    @staticmethod
    def eval_comparison(
        item: Any | None,
        expr: Comparison,
        field_resolver: Callable | None = None,
    ) -> bool:
        lval = QueryProcessor.eval_expr(item, expr.lexpr, field_resolver)
        rval = QueryProcessor.eval_expr(item, expr.rexpr, field_resolver)
        if (
            expr.op == ComparisonOp.LT
            or expr.op == ComparisonOp.LTE
            or expr.op == ComparisonOp.GT
            or expr.op == ComparisonOp.GTE
        ):
            if isinstance(lval, str) and isinstance(rval, str):
                if expr.op == ComparisonOp.LT:
                    return lval < rval
                if expr.op == ComparisonOp.LTE:
                    return lval <= rval
                if expr.op == ComparisonOp.GT:
                    return lval > rval
                if expr.op == ComparisonOp.GTE:
                    return lval >= rval
            if isinstance(lval, (int, float)) and isinstance(
                rval, (int, float)
            ):
                if expr.op == ComparisonOp.LT:
                    return lval < rval
                if expr.op == ComparisonOp.LTE:
                    return lval <= rval
                if expr.op == ComparisonOp.GT:
                    return lval > rval
                if expr.op == ComparisonOp.GTE:
                    return lval >= rval
            return False
        elif expr.op == ComparisonOp.EQ:
            return lval == rval
        elif expr.op == ComparisonOp.NEQ:
            return lval != rval
        elif expr.op == ComparisonOp.BETWEEN:
            if isinstance(rval, list):
                rval1 = rval[0]
                rval2 = rval[1]
                if (
                    isinstance(lval, str)
                    and isinstance(rval1, str)
                    and isinstance(rval2, str)
                ):
                    return lval >= rval1 and lval <= rval2
                if (
                    isinstance(lval, (int, float))
                    and isinstance(rval1, (int, float))
                    and isinstance(rval2, (int, float))
                ):
                    return lval >= rval1 and lval <= rval2
                return False
            raise BadRequestError(
                """Right operand for BETWEEN must be a list with two values"""
            )
        elif expr.op == ComparisonOp.IN:
            if isinstance(rval, list):
                return lval in rval
            raise BadRequestError(
                "Right operand for IN comparison must be a list"
            )
        elif expr.op == ComparisonOp.NIN:
            if isinstance(rval, list):
                return lval not in rval
            raise BadRequestError(
                "Right operand for NOT IN comparison must be a list"
            )
        elif expr.op == ComparisonOp.LIKE:
            if isinstance(lval, str) and isinstance(rval, str):
                return bool(re.match(rval, lval))
            return False
        raise BadRequestError("Comparison not supported")

    @staticmethod
    def eval_expr(
        item: Any,
        expr: Expression,
        field_resolver: Callable | None = None,
    ) -> Value | Undefined:
        if expr is None:
            return True
        if isinstance(expr, (bool, int, float, str, list, dict)):
            return expr
        if isinstance(expr, Field):
            return QueryProcessor.eval_field(item, expr, field_resolver)
        if isinstance(expr, Function):
            return QueryProcessor.eval_function(item, expr, field_resolver)
        if isinstance(expr, Comparison):
            return QueryProcessor.eval_comparison(item, expr, field_resolver)
        if isinstance(expr, And):
            return bool(
                QueryProcessor.eval_expr(item, expr.lexpr, field_resolver)
            ) and bool(
                QueryProcessor.eval_expr(item, expr.rexpr, field_resolver)
            )
        if isinstance(expr, Or):
            return bool(
                QueryProcessor.eval_expr(item, expr.lexpr, field_resolver)
            ) or bool(
                QueryProcessor.eval_expr(item, expr.rexpr, field_resolver)
            )
        if isinstance(expr, Not):
            return not bool(
                QueryProcessor.eval_expr(item, expr.expr, field_resolver)
            )
        raise BadRequestError(f"Expression {str(expr)} not supported")

    @staticmethod
    def update_item(
        item: Any,
        update: str | Update,
        field_resolver: Callable | None = None,
    ) -> dict:
        if isinstance(update, Update):
            up: Update | None = update
        else:
            up = QLParser.parse_update(update)
        if up is None:
            return item
        item_copy = copy.deepcopy(item)
        for operation in up.operations:
            QueryProcessor.update_field(item_copy, operation, field_resolver)
        return item_copy

    @staticmethod
    def update_field(
        value: Any,
        operation: UpdateOperation,
        field_resolver: Callable | None = None,
    ) -> None:
        field = operation.field
        op = operation.op
        path = field
        if field_resolver is not None:
            path = field_resolver(path)
        DataAccessor.update_field(
            value,
            path,
            op,
            (
                operation.args[0]
                if operation.args is not None and len(operation.args) > 0
                else None
            ),
        )

    @staticmethod
    def extract_filter_fields(
        where: Expression, field_resolver: Callable | None = None
    ):
        fields = []

        def extract_field(expr: Expression):
            if isinstance(expr, Field):
                field = (
                    expr.path
                    if field_resolver is None
                    else field_resolver(expr.path)
                )
                field = field.split("[")[0]
                if field not in fields:
                    fields.append(field)
            if isinstance(expr, Function):
                for arg in expr.args:
                    extract_field(arg)
            if isinstance(expr, Comparison):
                extract_field(expr.lexpr)
                extract_field(expr.rexpr)
            if isinstance(expr, And):
                extract_field(expr.lexpr)
                extract_field(expr.rexpr)
            if isinstance(expr, Or):
                extract_field(expr.lexpr)
                extract_field(expr.rexpr)
            if isinstance(expr, Not):
                extract_field(expr.expr)
            return

        extract_field(where)
        return fields


class AscFieldComparer:
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        return self.obj == other.obj

    def __lt__(self, other):
        return self.obj < other.obj


class DescFieldComparer:
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        return self.obj == other.obj

    def __lt__(self, other):
        return self.obj > other.obj


def default_field_resolver(field: str) -> str:
    return field
