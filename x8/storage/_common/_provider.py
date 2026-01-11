from ...core import Operation, Provider
from ._operation_parser import StoreOperationParser


class StoreProvider(Provider):
    def get_op_parser(
        self, operation: Operation | None
    ) -> StoreOperationParser:
        return StoreOperationParser(operation)
