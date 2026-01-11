from __future__ import annotations

from typing import IO, Any

from x8.core import Component, operation

from ._models import ImageData, ImageInfo


class Image(Component):
    source: str | None
    content: bytes | str | None
    stream: IO | None
    _pil_image: Any | None

    def __init__(
        self,
        source: str | None = None,
        content: bytes | str | None = None,
        stream: IO | None = None,
        data: ImageData | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            source: Local path or URL.
            content: Image bytes or base64 string.
            stream: Image stream.
            data: Image data.
        """
        self.source = source
        self.stream = stream
        self.content = content
        self._pil_image = kwargs.pop("_pil_image", None)
        if data is not None:
            self.source = data.source
            self.content = data.content
        super().__init__(**kwargs)

    @operation()
    def get_info(self) -> ImageInfo:
        """Get image info.

        Returns:
            Image info.
        """

    @operation()
    def convert(
        self,
        type: str,
        format: str | None = None,
        **kwargs,
    ) -> Any:
        """Convert to type.

        Args:
            type: One of "bytes", "base64", "pil", "opencv".
            format: One of "JPEG", "PNG", "BMP".

        Returns:
            Converted image.
        """

    @operation()
    def save(
        self,
        path: str,
        format: str | None = None,
        **kwargs,
    ) -> None:
        """Save image.

        Args:
            path: Local path.
            format: One of "JPEG", "PNG", "BMP".
        """

    @operation()
    def show(
        self,
        **kwargs,
    ) -> None:
        """Show the image."""

    @operation()
    async def aget_info(self) -> ImageInfo:
        """Get image info.

        Returns:
            Image info.
        """

    @operation()
    async def aconvert(
        self,
        type: str,
        format: str | None = None,
        **kwargs,
    ) -> Any:
        """Convert to type.

        Args:
            type: One of "bytes", "base64", "pil", "opencv".
            format: One of "JPEG", "PNG", "BMP".

        Returns:
            Converted image.
        """

    @operation()
    async def asave(
        self,
        path: str,
        format: str | None = None,
        **kwargs,
    ) -> None:
        """Save image.

        Args:
            path: Local path.
            format: One of "JPEG", "PNG", "BMP".
        """

    @operation()
    async def ashow(
        self,
        **kwargs,
    ) -> None:
        """Show the image."""

    @staticmethod
    def load(image: str | bytes | IO | ImageData | dict) -> Image:
        if isinstance(image, str):
            return Image(source=image)
        elif isinstance(image, bytes):
            return Image(content=image)
        elif isinstance(image, IO):
            return Image(stream=image)
        elif isinstance(image, dict):
            return Image(data=ImageData.from_dict(image))
        elif isinstance(image, ImageData):
            return Image(data=image)
        raise ValueError("Image data not valid.")
