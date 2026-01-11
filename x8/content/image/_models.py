from x8.core import DataModel


class ImageData(DataModel):
    source: str | None = None
    content: bytes | str | None = None
    media_type: str | None = None


class ImageInfo(DataModel):
    width: int
    height: int
