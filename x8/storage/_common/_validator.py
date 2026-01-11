from ...core.exceptions import BadRequestError
from ._operation import StoreOperation
from ._operation_parser import StoreOperationParser


class Validator:
    @staticmethod
    def validate_batch(
        op_parsers: list[StoreOperationParser],
        allowed_ops: list[str],
        single_collection: bool | None = False,
        single_pk: bool | None = False,
    ):
        if len(op_parsers) == 0:
            raise BadRequestError("BATCH must have at least one operation")
        collections = []
        for op_parser in op_parsers:
            collection = op_parser.get_collection_name()
            if collection not in collections:
                collections.append(collection)
            op = op_parser.get_op_name()
            if op not in allowed_ops:
                raise BadRequestError(f"{op} not supported in BATCH")
            if (
                op == StoreOperation.PUT
                or op == StoreOperation.DELETE
                or StoreOperation.UPDATE
            ):
                where = op_parser.get_where()
                if where is not None:
                    raise BadRequestError(
                        f"BATCH does not support where clause in {op}"
                    )
            else:
                raise BadRequestError(f"{op} not supported in BATCH")

        if single_collection:
            if len(collections) > 1:
                raise BadRequestError(
                    "BATCH can operate on only a single collection"
                )

    @staticmethod
    def validate_transact(
        op_parsers: list[StoreOperationParser],
        allowed_ops: list[str],
        single_collection: bool | None = False,
        single_pk: bool | None = False,
    ):
        if len(op_parsers) == 0:
            raise BadRequestError("TRANSACT must have at least one operation")
        collections = []
        for op_parser in op_parsers:
            collection = op_parser.get_collection_name()
            if collection not in collections:
                collections.append(collection)
            op = op_parser.get_op_name()
            if op not in allowed_ops:
                raise BadRequestError(f"{op} not supported in TRANSACT")

        if single_collection:
            if len(collections) > 1:
                raise BadRequestError(
                    "TRANSACT can operate on only a single collection"
                )
