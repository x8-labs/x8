"""
Config Store on YAML file.
"""

__all__ = ["YamlFile"]

from typing import Any

import yaml
from x8.core import Context, Operation, Response
from x8.core.exceptions import BadRequestError, NotFoundError
from x8.storage._common import StoreOperation, StoreProvider

from .._constants import DEFAULT_LABEL
from .._helper import normalize_label
from .._models import ConfigItem, ConfigKey


class YamlFile(StoreProvider):
    path: str = "config.yaml"
    flat: bool = False

    _items: dict
    _init: bool

    def __init__(
        self,
        path: str = "config.yaml",
        flat: bool = False,
        **kwargs,
    ):
        """Initialize.

        Args:
            path:
                YAML file path, defaults to "config.yaml".
            flat:
                A value indicating whether
                the YAML file is flat without any label hierarchy,
                defaults to False.
        """
        self.path = path
        self.flat = flat

        self._init = False

    def __setup__(self, context: Context | None = None) -> None:
        if self._init:
            return

        items = yaml.safe_load(self.path)

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
            if self.flat:
                if label != DEFAULT_LABEL:
                    raise BadRequestError(
                        "Custom label in not support in flat config file"
                    )
                if id not in self._items:
                    raise NotFoundError
                value = self._items[id]
            else:
                if label not in self._items:
                    raise NotFoundError
                if id not in self._items[label]:
                    raise NotFoundError
                value = self._items[label][id]
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
