import inspect
from typing import Any


class SyncAndAsyncClient:
    client: Any
    async_call: bool

    async def _execute_method(self, **kwargs):
        caller_frame = inspect.stack()[1]
        method_name = caller_frame.function
        client_method_name = (
            f"a{method_name}" if self.async_call else method_name
        )
        if method_name.startswith("__") and method_name.endswith("__"):
            method_name = method_name[2:-2]
            client_method_name = (
                f"__a{method_name}__"
                if self.async_call
                else f"__{method_name}__"
            )
        method = getattr(
            self.client,
            client_method_name,
        )
        if self.async_call:
            return await method(**kwargs)
        return method(**kwargs)
