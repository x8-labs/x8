from __future__ import annotations

import json
from typing import Any

from .data_model import DataModel


class Operation(DataModel):
    """Operation.

    Attributes:
        name: Operation name.
        args: Operation arguments.
    """

    name: str | None = None
    args: dict[str, Any] | None = None

    @staticmethod
    def normalize(
        name: str | None,
        args: dict[str, Any] | None,
    ) -> Operation:
        if args is None:
            return Operation(name=name)
        if "self" in args:
            args.pop("self")
        rargs: dict = {}
        for k, v in args.items():
            if k == "kwargs":
                rargs.update(v)
            elif v is not None:
                rargs[k] = v
        return Operation(name=name, args=rargs)

    @staticmethod
    def create_with_func_name(args: dict[str, Any] | None) -> Operation:
        import inspect

        name = None
        frame = inspect.currentframe()
        if frame is not None:
            if frame.f_back is not None:
                name = frame.f_back.f_code.co_name

        return Operation.normalize(name=name, args=args)

    def __str__(self) -> str:
        if self.name:
            str = self.name
        else:
            str = ""
        if self.args is not None:
            for key, value in self.args.items():
                nkey = key.replace("_", " ").strip()
                if isinstance(value, list) and all(
                    isinstance(a, Operation) for a in value
                ):
                    nvalue = "; ".join([self._str_value(a) for a in value])
                else:
                    nvalue = self._str_value(value)
                str = f"{str} {nkey} {nvalue}"
        return str

    def _str_value(self, value: Any) -> Any:
        if value is None or isinstance(
            value, (str, int, float, bool, dict, list)
        ):
            return json.dumps(value)
        return str(value)
