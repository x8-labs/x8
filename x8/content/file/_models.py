from x8.core import DataModel


class FileData(DataModel):
    source: str | None = None
    content: bytes | str | None = None
    filename: str | None = None
