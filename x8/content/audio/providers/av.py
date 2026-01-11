"""
Audio processed with av.
"""

__all__ = ["AV"]

import base64
from io import BytesIO
from typing import Any

import av
import av.error
import av.option
from x8.core import Context, Provider
from x8.core.exceptions import BadRequestError

from .._models import AudioInfo


class AV(Provider):
    _container: Any
    _audio_stream: Any

    _audio_bytes: dict

    def __init__(self, **kwargs):
        """Intialize."""
        self._container = None
        self._audio_stream = None
        self._audio_bytes = dict()

    def __setup__(self, context: Context | None = None) -> None:
        if self._container is not None:
            return
        if self.__component__.source:
            open_param: Any = self.__component__.source
        elif self.__component__.content:
            if isinstance(self.__component__.content, bytes):
                open_param = BytesIO(self.__component__.content)
            elif isinstance(self.__component__.content, str):
                open_param = BytesIO(
                    base64.b64decode(self.__component__.content)
                )
        elif self.__component__.stream:
            open_param = self.__component__.stream
        else:
            raise BadRequestError("Audio data not provided.")
        self._container = av.open(open_param)
        for stream in self._container.streams:
            if stream.type == "audio":
                self._audio_stream = stream

    def get_info(self, **kwargs) -> AudioInfo:
        return AudioInfo(
            duration=self._container.duration / av.time_base,
            channels=self._audio_stream.codec_context.channels,
            sample_rate=self._audio_stream.codec_context.sample_rate,
            bit_rate=self._audio_stream.bit_rate,
            format=(
                self._container.format.name
                if self._container.format is not None
                else None
            ),
            codec=self._audio_stream.codec_context.codec.name,
        )

    def convert(
        self,
        type: str = "stream",
        format: str | None = None,
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ):
        if format is None and codec is None:
            if (
                type == "stream"
                and self.__component__.stream
                and isinstance(self.__component__.stream, BytesIO)
            ):
                return BytesIO(self.__component__.stream.getvalue())
            elif type == "stream" and self.__component__.content:
                if isinstance(self.__component__.content, str):
                    return BytesIO(
                        base64.b64decode(self.__component__.content)
                    )
                else:
                    return BytesIO(self.__component__.content)
            elif (
                type == "bytes"
                and self.__component__.stream
                and isinstance(self.__component__.stream, BytesIO)
            ):
                return self.__component__.stream.getvalue()
            elif type == "bytes" and self.__component__.content:
                if isinstance(self.__component__.content, str):
                    return base64.b64decode(self.__component__.content)
                else:
                    return self.__component__.content

        if format is None:
            format = "mp3"
        if format in self._audio_bytes:
            if codec in self._audio_bytes[format]:
                if type == "bytes":
                    return self._audio_bytes[format][codec]
                elif type == "stream":
                    return BytesIO(self._audio_bytes[format][codec])

        if self._audio_stream is None:
            raise BadRequestError("No audio stream found.")

        if format == "wav" and codec is None:
            codec = "pcm_s16le"
        elif codec is None:
            codec = format
        output_io = BytesIO()
        output_container = av.open(output_io, mode="w", format=format)
        args: dict = {"codec_name": codec}
        if rate is not None:
            args["rate"] = rate
        if channels == 1:
            args["layout"] = "mono"
        output_stream: Any = output_container.add_stream(**args)

        for frame in self._container.decode(self._audio_stream):
            for packet in output_stream.encode(frame):
                output_container.mux(packet)

        for packet in output_stream.encode():
            output_container.mux(packet)

        output_container.close()
        bytes = output_io.getvalue()
        if format not in self._audio_bytes:
            self._audio_bytes[format] = dict()
        self._audio_bytes[format][codec] = bytes

        if type == "bytes":
            return bytes
        elif type == "stream":
            output_io.seek(0)
            return output_io
        else:
            raise BadRequestError("Convert type not supported")

    def save(
        self,
        path: str,
        format: str | None = "mp3",
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> None:
        with open(path, "wb") as f:
            f.write(
                self.convert(
                    type="bytes",
                    format=format,
                    codec=codec,
                    channels=channels,
                    rate=rate,
                )
            )

    def close(self):
        if self._container is not None:
            self._container.close()
            self._container = None
