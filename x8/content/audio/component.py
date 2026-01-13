from __future__ import annotations

from typing import IO, Any

from x8.core import Component, operation

from ._models import AudioData, AudioInfo


class Audio(Component):
    source: str | None
    content: bytes | str | None
    stream: IO | None

    def __init__(
        self,
        source: str | None = None,
        content: bytes | str | None = None,
        stream: IO | None = None,
        data: AudioData | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            source: Local path or URL.
            content: Audio bytes or base64 string.
            stream: Audio stream.
            data: Audio data.
        """
        self.source = source
        self.stream = stream
        self.content = content
        if data is not None:
            self.source = data.source
            self.content = data.content
        super().__init__(**kwargs)

    @operation()
    def get_info(self, **kwargs) -> AudioInfo:
        """Get audio info.

        Returns:
            Audio info.
        """
        raise NotImplementedError

    @operation()
    def convert(
        self,
        type: str = "stream",
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> Any:
        """Convert to type.

        Args:
            type:
                Data type. One of "bytes", "stream".
            format:
                Audio format. One of "wav", "mp3", etc.
            codec:
                Audio codec. One of "mp3", "pcm_s16le", etc.
            rate:
                Target sample rate.
            channels:
                Number of channels.

        Returns:
            Converted audio.
        """
        raise NotImplementedError

    @operation()
    def save(
        self,
        path: str,
        format: str | None = "mp3",
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> None:
        """Save audio.

        Args:
            path:
                Local path.
            format:
                Audio format. One of "wav", "mp3", etc.
            codec:
                Audio codec. One of "mp3", "pcm_s16le", etc.
            rate:
                Target sample rate.
            channels:
                Number of channels.
        """
        raise NotImplementedError

    @operation()
    def close(self, **kwargs) -> None:
        """Close the audio stream."""
        raise NotImplementedError

    @operation()
    async def aget_info(self, **kwargs) -> AudioInfo:
        """Get audio info.

        Returns:
            Audio info.
        """
        raise NotImplementedError

    @operation()
    async def aconvert(
        self,
        type: str = "stream",
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> Any:
        """Convert to type.

        Args:
            type:
                Data type. One of "bytes", "stream".
            format:
                Audio format. One of "wav", "mp3", etc.
            codec:
                Audio codec. One of "mp3", "pcm_s16le", etc.
            rate:
                Target sample rate.
            channels:
                Number of channels.

        Returns:
            Converted audio.
        """
        raise NotImplementedError

    @operation()
    async def asave(
        self,
        path: str,
        format: str | None = "mp3",
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> None:
        """Save audio.

        Args:
            path:
                Local path.
            format:
                Audio format. One of "wav", "mp3", etc.
            codec:
                Audio codec. One of "mp3", "pcm_s16le", etc.
            rate:
                Target sample rate.
            channels:
                Number of channels.
        """
        raise NotImplementedError

    @operation()
    async def aclose(self, **kwargs) -> None:
        """Close the audio stream."""
        raise NotImplementedError

    @staticmethod
    def load(audio: str | bytes | IO | AudioData | dict) -> Audio:
        if isinstance(audio, str):
            return Audio(source=audio)
        elif isinstance(audio, bytes):
            return Audio(content=audio)
        elif isinstance(audio, IO):
            return Audio(stream=audio)
        elif isinstance(audio, dict):
            return Audio(data=AudioData.from_dict(audio))
        elif isinstance(audio, AudioData):
            return Audio(data=audio)
        raise ValueError("Audio data not valid.")
