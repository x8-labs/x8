from __future__ import annotations

from typing import Any

from .data_model import DataModel
from .manifest import MANIFEST_FILE


class Context(DataModel):
    id: str | None = None
    data: dict[str, Any] | None = None


class RunContext(DataModel):
    """
    Run context.
    """

    handle: str | None = None
    tag: str | None = None
    path: str = "."
    manifest: str = MANIFEST_FILE
    component_type: str | None = None
