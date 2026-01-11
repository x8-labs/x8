from typing import Any

from ...core import Component
from ...core.exceptions import NotSupportedError
from ._operation import StoreOperation


class StoreComponent(Component):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def __getitem__(self, key: Any) -> Any:
        op = StoreOperation.get(key=key)
        try:
            response = self.__run__(op)
            if response is not None and response.result is not None:
                return response.result.value
            return None
        except NotSupportedError:
            return None

    def __setitem__(self, key: Any, value: Any) -> None:
        op = StoreOperation.put(key=key, value=value)
        self.__run__(op)
