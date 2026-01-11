"""
Secret Store on YAML file.
"""

__all__ = ["YamlFile"]

from typing import Any

import yaml
from x8.core import Context, Operation, Response
from x8.core.exceptions import NotFoundError
from x8.storage._common import StoreOperation, StoreProvider

from .._constants import LATEST_VERSION
from .._models import SecretItem, SecretKey


class YamlFile(StoreProvider):
    path: str

    _items: dict | None

    def __init__(self, path: str = "secret.yaml", **kwargs):
        """Initialize.

        Args:
            path:
                File path. Defaults to "secret.yaml".
        """
        self.path = path
        self._items = None

    def __setup__(self, context: Context | None = None) -> None:
        if self._items is not None:
            return
        self._items = yaml.safe_load(self.path)
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
