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

from ._models import (
    ObjectCollectionConfig,
    ObjectQueryConfig,
    ObjectTransferConfig,
)


class QueryArgs:
    prefix: str | None = None
    delimiter: str | None = None
    start_after: str | None = None
    end_before: str | None = None
    limit: int | None = None
    continuation: str | None = None
    paging: bool | None = False
    page_size: int | None = None


def get_collection_config(op_parser: StoreOperationParser):
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return ObjectCollectionConfig.from_dict(config)
    if isinstance(config, ObjectCollectionConfig):
        return config
    raise BadRequestError("Collection config format error")


def get_query_config(op_parser: StoreOperationParser):
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return ObjectQueryConfig.from_dict(config)
    if isinstance(config, ObjectQueryConfig):
        return config
    raise BadRequestError("Query config format error")


def get_transfer_config(
    op_parser: StoreOperationParser,
) -> ObjectTransferConfig | None:
    config = op_parser.get_config()
    if config is None:
        return None
    if isinstance(config, dict):
        return ObjectTransferConfig(**config)
    if isinstance(config, ObjectTransferConfig):
        return config
    raise BadRequestError("Transfer config format error")


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
                        args.prefix = prefix
                    else:
                        not_supported = True
                elif expr.name == QueryFunctionName.STARTS_WITH_DELIMITED:
                    field = expr.args[0].path
                    prefix = expr.args[1]
                    delimiter = expr.args[2]
                    if (
                        field == KeyAttribute.ID
                        or field == SpecialAttribute.ID
                    ):
                        args.prefix = prefix
                        args.delimiter = delimiter
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
                if field == KeyAttribute.ID or field == SpecialAttribute.ID:
                    if expr.op == ComparisonOp.GT:
                        args.start_after = value
                    elif expr.op == ComparisonOp.LT:
                        args.end_before = value
                    else:
                        not_supported = True
                else:
                    not_supported = True
            elif isinstance(expr.rexpr, Field) and (
                expr.lexpr is None or isinstance(expr.lexpr, str)
            ):
                field = expr.rexpr.path
                value = expr.lexpr
                if field == KeyAttribute.ID or field == SpecialAttribute.ID:
                    if expr.op == ComparisonOp.LT:
                        args.start_after = value
                    elif expr.op == ComparisonOp.GT:
                        args.end_before = value
                    else:
                        not_supported = True
                else:
                    not_supported = True
        else:
            not_supported = True
        if not_supported:
            raise BadRequestError("Condition not supported")

    args.limit = op_parser.get_limit()
    args.continuation = op_parser.get_continuation()
    config = get_query_config(op_parser)
    if config is not None:
        args.paging = config.paging
        args.page_size = config.page_size
    return args
