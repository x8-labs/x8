from x8.core.exceptions import BadRequestError
from x8.ql import (
    Comparison,
    ComparisonOp,
    Field,
    Function,
    FunctionNamespace,
    QueryFunctionName,
)
from x8.storage._common import (
    KeyAttribute,
    SpecialAttribute,
    StoreOperationParser,
)

from ._constants import DEFAULT_LABEL


def normalize_label(label: str | None):
    return label if label is not None else DEFAULT_LABEL


class QueryArgs:
    id_filter: str | None = None
    label_filter: str | None = None


def get_query_args(
    op_parser: StoreOperationParser,
) -> QueryArgs:
    args = QueryArgs()
    expr_list = op_parser.get_where_expr_list()
    not_supported = False
    for expr in expr_list:
        if isinstance(expr, Function):
            if expr.namespace == FunctionNamespace.BUILTIN:
                if expr.name == QueryFunctionName.STARTS_WITH:
                    field = expr.args[0].path
                    prefix = expr.args[1]
                    if (
                        field == KeyAttribute.ID
                        or field == SpecialAttribute.ID
                    ):
                        args.id_filter = prefix
                    else:
                        not_supported = True
                else:
                    not_supported = True
        elif isinstance(expr, Comparison):
            if isinstance(expr.lexpr, Field) and (
                expr.rexpr is None or isinstance(expr.rexpr, str)
            ):
                field = expr.lexpr.path
                value = expr.rexpr
                if (
                    field == KeyAttribute.LABEL
                    or field == SpecialAttribute.LABEL
                ):
                    if expr.op == ComparisonOp.EQ:
                        args.label_filter = value
                    else:
                        not_supported = True
                else:
                    not_supported = True
            elif isinstance(expr.rexpr, Field) and (
                expr.lexpr is None or isinstance(expr.lexpr, str)
            ):
                field = expr.rexpr.path
                value = expr.lexpr
                if (
                    field == KeyAttribute.LABEL
                    or field == SpecialAttribute.LABEL
                ):
                    if expr.op == ComparisonOp.EQ:
                        args.label_filter = value
                    else:
                        not_supported = True
                else:
                    not_supported = True
        else:
            not_supported = True
        if not_supported:
            raise BadRequestError("Condition not supported")
    return args
