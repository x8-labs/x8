from x8.core import DataModel


class LiveStreamParam(DataModel):
    url: str | None = None
    """Live stream url."""

    min_resolution: int = 0
    """Minimum resolution of the stream to decode."""


class LiveStreamInfo(DataModel):
    width: int
    """Width of the video in pixels."""

    height: int
    """Height of the video in pixels."""

    total_frames: int
    """Total number of frames in the video stream."""

    frame_rate: float | None = None
    """Frame rate of the video."""

    format: str | None = None
    """Format of the video."""

    codec: str | None = None
    """Name of the codec used for the video stream."""

    pixel_format: str | None = None
    """Pixel format of the video stream."""

    start_time: float | None = None
    """Start time of the video in seconds."""
