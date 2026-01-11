from __future__ import annotations

from typing import Any, Callable


class NCall:
    function: Callable
    args: dict[str, Any] | list[Any] | None
    nargs: dict[str, Any] | None
    error_map: dict[Any, Any] | None

    def __init__(
        self,
        function: Callable,
        args: dict[str, Any] | list | None = None,
        nargs: dict[str, Any] | None = None,
        error_map: dict[Any, Any] | None = None,
    ):
        self.function = function
        self.args = args
        self.nargs = nargs
        self.error_map = error_map

    def __repr__(self) -> str:
        return str(self.function)

    def get_arg(self, name: str) -> Any:
        if (
            self.args is not None
            and isinstance(self.args, dict)
            and name in self.args
        ):
            return self.args[name]
        return None

    def set_arg(self, name: str, value: Any) -> None:
        if self.args is None:
            self.args = dict()
        if isinstance(self.args, dict):
            self.args[name] = value

    def invoke(self, return_error: bool = False) -> Any:
        args = self.args if self.args is not None else dict()
        nargs = self.nargs if self.nargs is not None else dict()
        if isinstance(args, dict):
            kwargs = args | nargs
        else:
            kwargs = nargs
        try:
            result = None
            if not args and not kwargs:
                result = self.function()
            if isinstance(args, dict):
                result = self.function(**kwargs)
            elif isinstance(args, list):
                result = self.function(*args, **kwargs)
            if (
                result is None
                and self.error_map is not None
                and None in self.error_map
                and self.error_map[None] is not None
            ):
                raise self.error_map[None]
            return (result, None) if return_error else result
        except Exception as e:
            if self.error_map is not None:
                key = type(e)
                if key in self.error_map:
                    if self.error_map[key] is None:
                        return (None, e) if return_error else None
                    else:
                        raise self.error_map[key]
            elif return_error:
                return (None, e)
            raise

    async def ainvoke(self, return_error: bool = False) -> Any:
        args = self.args if self.args is not None else dict()
        nargs = self.nargs if self.nargs is not None else dict()
        if isinstance(args, dict):
            kwargs = args | nargs
        else:
            kwargs = nargs
        try:
            result = None
            if not args and not kwargs:
                result = await self.function()
            if isinstance(args, dict):
                result = await self.function(**kwargs)
            elif isinstance(args, list):
                result = await self.function(*args, **kwargs)
            if (
                result is None
                and self.error_map is not None
                and None in self.error_map
                and self.error_map[None] is not None
            ):
                raise self.error_map[None]
            return (result, None) if return_error else result
        except Exception as e:
            if self.error_map is not None:
                key = type(e)
                if key in self.error_map:
                    if self.error_map[key] is None:
                        return (None, e) if return_error else None
                    else:
                        raise self.error_map[key]
            elif return_error:
                return (None, e)
            raise
