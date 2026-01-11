from __future__ import annotations

import uuid
from typing import Any

from ._async_helper import run_async, run_sync
from ._context import Context
from ._operation import Operation
from ._provider import Provider
from ._response import Response
from ._type_converter import TypeConverter
from .exceptions import NotSupportedError


class Component:
    __provider__: Provider
    __handle__: str | None
    __type__: str
    __unpack__: bool
    __native__: bool

    def __init__(
        self,
        **kwargs,
    ):
        self.__native__ = kwargs.pop("__native__", False)
        self.__unpack__ = kwargs.pop("__unpack__", False)
        self.__handle__ = kwargs.pop("__handle__", None)
        self.__type__ = kwargs.pop("__type__", self.__class__.__module__)
        if "__provider__" in kwargs:
            self.__bind__(kwargs.pop("__provider__"))

    def __bind__(
        self,
        provider: Provider | dict | str | None,
    ) -> None:
        if provider is None:
            return
        if isinstance(provider, Provider):
            provider.__component__ = self
            self.__provider__ = provider
        else:
            if isinstance(provider, dict):
                type = provider.pop("type")
                parameters = provider.pop("parameters", dict())
            elif isinstance(provider, str):
                type = provider
                parameters = dict()
            from ._loader import Loader

            module_name = self.__class__.__module__.rsplit(".", 1)[0]
            provider_path = f"{module_name}.providers.{type}"
            provider_instance = Loader.load_provider_instance(
                path=provider_path,
                parameters=parameters,
            )
            self.__bind__(provider=provider_instance)

    def __setup__(self, context: Context | None = None) -> None:
        self.__provider__.__setup__(context=context)

    async def __asetup__(self, context: Context | None = None) -> None:
        await self.__provider__.__asetup__(context=context)

    def __run__(
        self,
        operation: dict | str | Operation | None = None,
        context: dict | Context | None = None,
        **kwargs,
    ) -> Any:
        current_context = self._init_context(context)
        operation_name = self._get_operation_name(operation)
        operation_args = self._get_operation_args(operation)
        if hasattr(self, "__provider__"):
            response = self.__provider__.__run__(
                operation=self._convert_operation(operation),
                context=current_context,
                **kwargs,
            )
        elif operation and operation_name:
            func_name = operation_name
            func = getattr(self, func_name, None)
            if func and callable(func):
                args = TypeConverter.convert_args(func, operation_args or {})
                response = func(**args)
            else:
                afunc_name = f"a{operation_name}"
                afunc = getattr(self, afunc_name, None)
                if callable(afunc):
                    args = TypeConverter.convert_args(
                        afunc, operation_args or {}
                    )
                    response = run_sync(afunc, **args)
                else:
                    raise NotSupportedError(
                        operation if operation is not None else None
                    )
        else:
            raise NotSupportedError()

        if (
            hasattr(self, "__native__")
            and not self.__native__
            and isinstance(response, Response)
        ):
            response.native = None
        if (
            hasattr(self, "__unpack__")
            and self.__unpack__
            and isinstance(response, Response)
        ):
            return response.result
        return response

    async def __arun__(
        self,
        operation: dict | str | Operation | None = None,
        context: dict | Context | None = None,
        **kwargs,
    ) -> Any:
        current_context = self._init_context(context)
        operation_name = self._get_operation_name(operation)
        operation_args = self._get_operation_args(operation)
        if hasattr(self, "__provider__"):
            response = await self.__provider__.__arun__(
                operation=self._convert_operation(operation),
                context=current_context,
                **kwargs,
            )
        else:
            afunc_name = f"a{operation_name}"
            afunc = getattr(self, afunc_name, None)
            if afunc and callable(afunc):
                args = TypeConverter.convert_args(afunc, operation_args or {})
                response = await afunc(**args)
            else:
                response = await run_async(
                    func=self.__run__,
                    operation=operation,
                    context=current_context,
                    **kwargs,
                )

        if (
            hasattr(self, "__native__")
            and not self.__native__
            and isinstance(response, Response)
        ):
            response.native = None
        if (
            hasattr(self, "__unpack__")
            and self.__unpack__
            and isinstance(response, Response)
        ):
            return response.result
        return response

    def __supports__(self, feature: str) -> bool:
        return self.__provider__.__supports__(feature)

    def __execute__(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Any:
        return self.__provider__.__execute__(
            statement, params, context, **kwargs
        )

    async def __aexecute__(
        self,
        statement: str,
        params: dict[str, Any] | None = None,
        context: Context | None = None,
        **kwargs: Any,
    ) -> Any:
        return await self.__provider__.__aexecute__(
            statement, params, context, **kwargs
        )

    def __serialize__(self) -> Any:
        pass

    def _convert_operation(
        self,
        operation: dict | str | Operation | None,
    ) -> Operation | None:
        if isinstance(operation, dict):
            return Operation.from_dict(operation)
        elif isinstance(operation, str):
            return Operation(name=operation)
        return operation

    def _init_context(
        self,
        context: dict | Context | None,
    ) -> Context:
        if isinstance(context, dict):
            context = Context.from_dict(context)
        if context is not None:
            id = context.id
        else:
            id = str(uuid.uuid4())
        ctx = Context(
            id=id,
            data=context.data if context else None,
        )
        return ctx

    def _get_operation_name(
        self,
        operation: dict | str | Operation | None = None,
    ) -> str | None:
        if not operation:
            return None
        elif isinstance(operation, str):
            return operation
        elif isinstance(operation, dict):
            return operation.get("name", None)
        elif isinstance(operation, Operation):
            return operation.name
        else:
            return None

    def _get_operation_args(
        self,
        operation: dict | str | Operation | None = None,
    ) -> dict[str, Any] | None:
        if not operation:
            return None
        elif isinstance(operation, str):
            return None
        elif isinstance(operation, dict):
            return operation.get("args", None)
        elif isinstance(operation, Operation):
            return operation.args
        else:
            return None
