import inspect
from functools import wraps
from typing import Any, Callable, TypeVar, cast

from ._operation import Operation
from .exceptions import NotSupportedError

T = TypeVar("T", bound=Callable[..., Any])


def component(**config: Any) -> Callable[[T], T]:
    def decorator(cls: T) -> T:
        setattr(cls, "__config__", config)
        return cls

    return decorator


def operation(**config: Any) -> Callable[[T], T]:
    def decorator(func: T) -> T:
        setattr(func, "__operation__", True)
        setattr(func, "__config__", config)
        if not inspect.iscoroutinefunction(func):

            @wraps(func)
            def wrapper(*args, **kwargs) -> Any:
                self = args[0]
                context = kwargs.pop("__context__", None)
                if hasattr(self, "__provider__"):
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()
                    locals = dict(bound_args.arguments)
                    locals.pop("self", None)
                    operation = Operation.normalize(
                        name=func.__name__,
                        args=locals,
                    )
                    try:
                        response = self.__run__(operation, context, **kwargs)
                        return response
                    except NotSupportedError:
                        return func(*args, **kwargs)
                else:
                    return func(*args, **kwargs)

            return cast(T, wrapper)
        else:

            @wraps(func)
            async def wrapper(*args, **kwargs) -> Any:
                self = args[0]
                context = kwargs.pop("__context__", None)
                if hasattr(self, "__provider__"):
                    sig = inspect.signature(func)
                    bound_args = sig.bind(*args, **kwargs)
                    bound_args.apply_defaults()
                    locals = dict(bound_args.arguments)
                    locals.pop("self", None)
                    operation = Operation.normalize(
                        name=func.__name__[1:],
                        args=locals,
                    )
                    try:
                        response = await self.__arun__(
                            operation, context, **kwargs
                        )
                        return response
                    except NotSupportedError:
                        return await func(*args, **kwargs)
                else:
                    return await func(*args, **kwargs)

            return cast(T, wrapper)

    return decorator
