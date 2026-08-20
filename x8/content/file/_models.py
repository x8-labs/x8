from typing import Literal

from x8.core import DataModel


class FileData(DataModel):
    source: Literal["uri", "inline"]
    uri: str | None = None
    content: bytes | str | None = None
    filename: str | None = None
    media_type: str | None = None
