"""
Config Store on ENV file.
"""

__all__ = ["EnvFile"]

from typing import Any

from dotenv import dotenv_values
from x8.core import Context, Operation, Response
from x8.core.exceptions import BadRequestError, NotFoundError
from x8.storage._common import StoreOperation, StoreProvider

from .._constants import DEFAULT_LABEL
from .._helper import normalize_label
from .._models import ConfigItem, ConfigKey


class EnvFile(StoreProvider):
    path: str

    _items: dict
    _init: bool

    def __init__(self, path: str = ".env", **kwargs):
        """Intialize.

        Args:
            path:
                ENV file path, defaults to ".env".
        """
        self.path = path

        self._init = False

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        items = dotenv_values(self.path)
        if items is None:
            self._items = dict()
        else:
            self._items = items
        self._init = True

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        self.__setup__(context=context)
        op_parser = self.get_op_parser(operation)
        # GET value
        if op_parser.op_equals(StoreOperation.GET):
            id = op_parser.get_id_as_str()
            label = normalize_label(op_parser.get_label())
            if label != DEFAULT_LABEL:
                raise BadRequestError(
                    "Custom label in not support in env file"
                )
            if id not in self._items:
                raise NotFoundError
            value = self._items[id]
            result = ConfigItem(
                key=ConfigKey(id=id, label=label),
                value=value,
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
