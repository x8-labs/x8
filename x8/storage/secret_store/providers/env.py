"""
Secret Store on Environment variables.
"""

__all__ = ["Env"]

import os
from typing import Any

from x8.core import Context, Operation, Response
from x8.core.exceptions import NotFoundError, PreconditionFailedError
from x8.storage._common import StoreOperation, StoreProvider

from .._constants import LATEST_VERSION
from .._models import SecretItem, SecretKey, SecretVersion


class Env(StoreProvider):
    uppercase: bool

    def __init__(self, uppercase: bool = True, **kwargs):
        """Initalize."""
        self.uppercase = uppercase

    def __setup__(self, context: Context | None = None) -> None:
        pass

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
            id = self._normalize_id(id)
            if self.uppercase:
                id = id.upper()
            val = os.environ.get(id)
            if val is None:
                raise NotFoundError
            result = SecretItem(
                key=SecretKey(id=id, version=LATEST_VERSION), value=val
            )
        # GET versions
        elif op_parser.op_equals(StoreOperation.GET_VERSIONS):
            id = op_parser.get_id_as_str()
            id = self._normalize_id(id)
            val = os.environ.get(id)
            if val is None:
                raise NotFoundError
            versions: list = []
            versions.append(SecretVersion(version=LATEST_VERSION))
            result = SecretItem(key=SecretKey(id=id), versions=versions)
        # PUT
        elif op_parser.op_equals(StoreOperation.PUT):
            id = op_parser.get_id_as_str()
            id = self._normalize_id(id)
            value = str(op_parser.get_value())
            exists = op_parser.get_where_exists()
            if exists is False:
                current_value = os.environ.get(id)
                if current_value is not None:
                    raise PreconditionFailedError
            elif exists is True:
                current_value = os.environ.get(id)
                if current_value is None:
                    raise PreconditionFailedError
            os.environ[id] = value
            result = SecretItem(key=SecretKey(id=id, version=LATEST_VERSION))
        # UPDATE value
        elif op_parser.op_equals(StoreOperation.UPDATE):
            id = op_parser.get_id_as_str()
            id = self._normalize_id(id)
            value = str(op_parser.get_value())
            current_value = os.environ.get(id)
            if current_value is None:
                raise NotFoundError
            os.environ[id] = value
            result = SecretItem(key=SecretKey(id=id, version=LATEST_VERSION))
        # DELETE
        elif op_parser.op_equals(StoreOperation.DELETE):
            id = op_parser.get_id_as_str()
            id = self._normalize_id(id)
            try:
                os.environ.pop(id)
            except KeyError:
                raise NotFoundError
            result = None
        # CLOSE
        elif op_parser.op_equals(StoreOperation.CLOSE):
            result = None
        else:
            return super().__run__(
                operation,
                context,
                **kwargs,
            )
        return Response(result=result)

    def _normalize_id(self, id: str) -> str:
        id = id.replace("-", "_")
        if self.uppercase:
            id = id.upper()
        return id
