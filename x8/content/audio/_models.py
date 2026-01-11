from x8.core import DataModel


class AudioData(DataModel):
    source: str | None = None
    content: bytes | str | None = None
    media_type: str | None = None


class AudioInfo(DataModel):
    duration: float
    """Duration of the audio in seconds."""

    channels: int
    """Number of audio channels."""

    sample_rate: int
    """Sample rate of the audio."""

    bit_rate: int
    """Bit rate of the audio."""

    format: str | None = None
    """Format of the audio."""

    codec: str | None = None
    """Name of the codec used for the audio stream."""
