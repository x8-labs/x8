"""
Secret Store on ENV file.
"""

__all__ = ["EnvFile"]

from typing import Any

from dotenv import dotenv_values
from x8.core import Context, Operation, Response
from x8.core.exceptions import NotFoundError
from x8.storage._common import StoreOperation, StoreProvider

from .._constants import LATEST_VERSION
from .._models import SecretItem, SecretKey


class EnvFile(StoreProvider):
    path: str = ".env"

    _items: dict | None

    def __init__(self, path: str = ".env", **kwargs):
        """Initalize.

        Args:
            path:
                Path to file. Defaults to ".env".
        """
        self.path = path
        self._items = None

    def __setup__(self, context: Context | None = None) -> None:
        if self._items is not None:
            return

        self._items = dotenv_values(self.path)
        if self._items is None:
            self._items = dict()

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        result = None
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            if self._items is None:
                raise NotFoundError
            if id not in self._items:
                raise NotFoundError
            val = self._items[id]
            result = SecretItem(
                key=SecretKey(id=id, version=LATEST_VERSION), value=val
            )
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            pass
        else:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        return Response(result=result)
