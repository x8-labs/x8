from typing import Any, Generic, TypeVar

from ._context import Context
from .data_model import DataModel

T = TypeVar("T")


class Response(DataModel, Generic[T]):
    result: T
    """Result of the primary provider."""

    context: Context | None = None
    """Operation context."""

    native: dict[str, Any] | None = None
    """Logs from native client."""
