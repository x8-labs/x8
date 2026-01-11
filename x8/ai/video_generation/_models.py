from typing import Literal

from x8.content.image import ImageData
from x8.content.video import VideoData
from x8.core import DataModel

VideoSize = Literal[
    "1280x720",
    "720x1280",
    "1920x1080",
    "1080x1920",
    "1792x1024",
    "1024x1792",
]


class Reference(DataModel):
    type: str | None = None
    image: ImageData | None = None


class KeyFrame(DataModel):
    time: float
    image: ImageData


class VideoGenerationResult(DataModel):
    id: str
    created_at: float | None = None
    completed_at: float | None = None
    expires_at: float | None = None
    duration: float | None = None
    size: str | None = None
    status: Literal["queued", "in_progress", "completed", "failed"] = "queued"
    progress: int | None = None
    error: str | None = None
    videos: list[VideoData] | None = None
