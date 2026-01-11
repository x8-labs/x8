from typing import Any

from ._arg_parser import ArgParser
from ._async_helper import run_async, run_sync
from ._context import Context
from ._operation import Operation
from ._type_converter import TypeConverter
from .exceptions import NotSupportedError


class Provider:
    __component__: Any
    __handle__: str | None
    __type__: str

    def __init__(self, **kwargs):
        self.__handle__ = kwargs.pop("__handle__", None)
        self.__type__ = kwargs.pop("__type__", self.__class__.__module__)
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __setup__(self, context: Context | None = None) -> None:
        pass

    async def __asetup__(self, context: Context | None = None) -> None:
        await run_async(func=self.__setup__, context=context)

    def __run__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if operation and operation.name:
            func_name = operation.name
            func = getattr(self, func_name, None)
            if func and callable(func):
                self.__setup__(context=context)
                args = TypeConverter.convert_args(func, operation.args or {})
                response = func(**args)
                return response

            afunc_name = f"a{operation.name}"
            afunc = getattr(self, afunc_name, None)
            if callable(afunc):
                run_sync(self.__asetup__, context=context)
                args = TypeConverter.convert_args(afunc, operation.args or {})
                response = run_sync(afunc, **args)
                return response
        raise NotSupportedError(
            operation.to_json() if operation is not None else None
        )

    async def __arun__(
        self,
        operation: Operation | None = None,
        context: Context | None = None,
        **kwargs,
    ) -> Any:
        if operation and operation.name:
            afunc_name = f"a{operation.name}"
            afunc = getattr(self, afunc_name, None)
            if afunc and callable(afunc):
                await self.__asetup__(context=context)
                args = TypeConverter.convert_args(afunc, operation.args or {})
                response = await afunc(**args)
                return response

        return await run_async(
            func=self.__run__,
            operation=operation,
            context=context,
            **kwargs,
        )

    def __supports__(self, feature: str) -> bool:
        return True

    def __execute__(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Any:
        operation = ArgParser.convert_execute_operation(statement, params)
        return self.__run__(operation, context)

    async def __aexecute__(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Any:
        operation = ArgParser.convert_execute_operation(statement, params)
        return await self.__arun__(operation, context)
