from typing import Literal

from x8.content.image import ImageData
from x8.core import DataModel

ImageSize = Literal[
    "auto",
    "256x256",
    "512x512",
    "1024x1024",
    "1536x1024",
    "1024x1536",
    "1792x1024",
    "1024x1792",
    "2048x2048",
    "4096x4096",
]

ImageQuality = Literal[
    "auto",
    "low",
    "medium",
    "high",
    "standard",
    "hd",
]

ImageStyle = Literal["vivid", "natural"]

ImageOutputFormat = Literal["png", "jpeg", "webp"]

ImageBackground = Literal["transparent", "opaque", "auto"]

ImageModeration = Literal["low", "auto"]

ImageInputFidelity = Literal["high", "low"]


class Usage(DataModel):
    input_tokens: int = 0
    output_tokens: int = 0
    total_tokens: int = 0


class ImageGenerationResult(DataModel):
    created: int | None = None
    revised_prompt: str | None = None
    background: str | None = None
    output_format: str | None = None
    quality: str | None = None
    size: str | None = None
    usage: Usage | None = None
    images: list[ImageData] | None = None


class ImageGenerationStreamEvent(DataModel):
    type: Literal["partial_image", "completed"] = "completed"
    partial_image_index: int | None = None
    content: str | None = None
    background: str | None = None
    output_format: str | None = None
    quality: str | None = None
    size: str | None = None
    usage: Usage | None = None
