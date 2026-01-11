from __future__ import annotations

from x8.content.video import VideoAudio, VideoFrame
from x8.core import Component, operation

from ._models import LiveStreamInfo, LiveStreamParam


class LiveStream(Component):
    source: str | None
    min_resolution: int

    def __init__(
        self,
        source: str | None = None,
        min_resolution: int = 0,
        param: LiveStreamParam | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            source:
                Live stream source, e.g., local path or URL.
            min_resolution:
                Minimum resolution of the stream to decode.
            param:
                Live stream param.
        """
        self.source = source
        self.min_resolution = min_resolution
        if param is not None:
            self.source = param.url
        super().__init__(**kwargs)

    @operation()
    def get_info(self, **kwargs) -> LiveStreamInfo:
        """Get live stream info.

        Returns:
            Live stream info.
        """

    @operation()
    def get_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        """Get the current frame in the live stream.

        Returns:
            Live stream frame.
        """

    @operation()
    def get_audio(
        self,
        duration: float,
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        """Get audio for a duration.

        Args:
            duration:
                Duration in seconds.
            format:
                Format of the audio segment.
                Possible values are "mp3", "wav", "ogg".
                Defaults to "mp3".
            codec:
                Codec of the audio segment.
                Possible values are "mp3", "pcm_s16le", etc.
                Defaults to "mp3".
            rate:
                Audio sample rate.
            channels:
                Number of channels. 1 is mono.

        Returns:
            Audio in live stream.
        """

    @operation()
    def close(self, **kwargs) -> None:
        """Close the live stream."""

    @operation()
    async def aget_info(self, **kwargs) -> LiveStreamInfo:
        """Get live stream info.

        Returns:
            Live stream info.
        """

    @operation()
    async def aget_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        """Get the current frame in the live stream.

        Returns:
            Live stream frame.
        """

    @operation()
    async def aget_audio(
        self,
        duration: float,
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        """Get audio for a duration.

        Args:
            duration:
                Duration in seconds.
            format:
                Format of the audio segment.
                Possible values are "mp3", "wav", "ogg".
                Default to "mp3".
            codec:
                Codec of the audio segment.
                Possible values are "mp3", "pcm_s16le", etc.
                Default to "mp3".
            rate:
                Audio sample rate.
            channels:
                Number of channels. 1 is mono.

        Returns:
            Audio in live stream.
        """

    @operation()
    async def aclose(self, **kwargs) -> None:
        """Close the live stream."""
