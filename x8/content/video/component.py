from __future__ import annotations

from typing import IO

from x8.core import Component, operation

from ._models import VideoAudio, VideoData, VideoFrame, VideoInfo


class Video(Component):
    source: str | None
    content: bytes | str | None
    stream: IO | None

    def __init__(
        self,
        source: str | None = None,
        content: bytes | str | None = None,
        stream: IO | None = None,
        data: VideoData | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            source: Local path or URL.
            content: Video bytes or base64-encoded string.
            stream: Video stream.
            data: Video data.
        """
        self.source = source
        self.content = content
        self.stream = stream
        if data is not None:
            self.source = data.source
            self.content = data.content
        super().__init__(
            __provider__=kwargs.pop("__provider__", "default"),
            **kwargs,
        )

    @operation()
    def get_info(self, **kwargs) -> VideoInfo:
        """Get video info.

        Returns:
            Video info.
        """
        ...

    @operation()
    def seek_frame(
        self,
        timestamp: float | None = None,
        frame_index: int | None = None,
        exact: bool = False,
        backward: bool = True,
        **kwargs,
    ) -> VideoFrame | None:
        """Seek frame.

        Args:
            timestamp:
                Timestamp in seconds to seek.
                If timestamp is provided, frame_index is ignored.
            frame_index:
                Frame index to seek.
            exact:
                If exact is false, the nearest key frame is returned.
                If exact is true, the exact frame is returned.
            backward:
                If a key frame is seeked, this value indicates
                whether to seek the key frame backwards from the
                given timestamp or frame index or forwards.

        Returns:
            Video frame.
        """
        ...

    @operation()
    def get_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        """Get the next frame in the video.

        Returns:
            Video frame.
        """
        ...

    @operation()
    def get_audio(
        self,
        start: float | None = None,
        end: float | None = None,
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        """Get audio in video between two timestamps.

        Args:
            start:
                Start timestamp in seconds.
                If start timestamp is None, the start is the
                beginning of the video.
            end:
                End timestamp in seconds.
                If end timestamp is None, the end is the
                end of the video.
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
            Audio in video.
        """
        ...

    @operation()
    def save(
        self,
        path: str,
        **kwargs,
    ) -> None:
        """Save video.

        Args:
            path: Local path.
        """
        ...

    @operation()
    def close(self, **kwargs) -> None:
        """Close the video."""
        ...

    @operation()
    async def aget_info(self, **kwargs) -> VideoInfo:
        """Get video info.

        Returns:
            Video info.
        """
        ...

    @operation()
    async def aseek_frame(
        self,
        timestamp: float | None = None,
        frame_index: int | None = None,
        exact: bool = False,
        backward: bool = True,
        **kwargs,
    ) -> VideoFrame | None:
        """Seek frame.

        Args:
            timestamp:
                Timestamp in seconds to seek.
                If timestamp is provided, frame_index is ignored.
            frame_index:
                Frame index to seek.
            exact:
                If exact is false, the nearest key frame is returned.
                If exact is true, the exact frame is returned.
            backward:
                If a key frame is seeked, this value indicates
                whether to seek the key frame backwards from the
                given timestamp or frame index or forwards.

        Returns:
            Video frame.
        """
        ...

    @operation()
    async def aget_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        """Get the next frame in the video.

        Returns:
            Video frame.
        """
        ...

    @operation()
    async def aget_audio(
        self,
        start: float | None = None,
        end: float | None = None,
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        """Get audio in video between two timestamps.

        Args:
            start:
                Start timestamp in seconds.
                If start timestamp is None, the start is the
                beginning of the video.
            end:
                End timestamp in seconds.
                If end timestamp is None, the end is the
                end of the video.
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
            Audio in video.
        """
        ...

    @operation()
    async def asave(
        self,
        path: str,
        **kwargs,
    ) -> None:
        """Save video.

        Args:
            path: Local path.
        """
        ...

    @operation()
    async def aclose(self, **kwargs) -> None:
        """Close the video."""
        ...

    @staticmethod
    def load(video: str | bytes | IO | VideoData | dict) -> Video:
        if isinstance(video, str):
            return Video(source=video)
        elif isinstance(video, bytes):
            return Video(content=video)
        elif isinstance(video, IO):
            return Video(stream=video)
        elif isinstance(video, dict):
            return Video(data=VideoData.from_dict(video))
        elif isinstance(video, VideoData):
            return Video(data=video)
        raise ValueError("Video data not valid.")
