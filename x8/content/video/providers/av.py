"""
Video processed with av.
"""

__all__ = ["AV"]

import base64
from io import BytesIO
from typing import Any

import av
import av.error
from x8.content.audio import Audio
from x8.content.image import Image
from x8.core import Context, Provider
from x8.core.exceptions import BadRequestError

from .._models import VideoAudio, VideoFrame, VideoInfo


class AV(Provider):
    _container: Any
    _video_stream: Any
    _audio_stream: Any

    def __init__(self, **kwargs):
        """Initialize."""
        self._container = None
        self._video_stream = None
        self._audio_stream = None

    def __setup__(self, context: Context | None = None) -> None:
        if self._container is not None:
            return
        if self.__component__.source:
            open_param: Any = self.__component__.source
        elif self.__component__.content:
            if isinstance(self.__component__.content, str):
                open_param = BytesIO(
                    base64.b64decode(self.__component__.content)
                )
            else:
                open_param = BytesIO(self.__component__.content)
        elif self.__component__.stream is not None:
            open_param = self.__component__.stream
        else:
            raise BadRequestError("Video data not provided.")

        self._container = av.open(open_param)
        for stream in self._container.streams:
            if stream.type == "video":
                self._video_stream = stream
            if stream.type == "audio":
                self._audio_stream = stream

    def get_info(self, **kwargs) -> VideoInfo:
        self.__setup__()
        return VideoInfo(
            duration=self._container.duration / av.time_base,
            width=self._video_stream.width,
            height=self._video_stream.height,
            total_frames=self._video_stream.frames,
            frame_rate=self._video_stream.average_rate,
            bit_rate=self._video_stream.bit_rate,
            format=(
                self._video_stream.format.name
                if self._video_stream.format
                else None
            ),
            codec=self._video_stream.codec_context.name,
            pixel_format=self._video_stream.pix_fmt,
            start_time=(
                self._video_stream.start_time * self._video_stream.time_base
                if self._video_stream.start_time
                else None
            ),
        )

    def seek_frame(
        self,
        timestamp: float | None = None,
        frame_index: int | None = None,
        exact: bool = False,
        backward: bool = True,
        **kwargs,
    ) -> VideoFrame | None:
        self.__setup__()
        if self._video_stream is None:
            raise BadRequestError("No video stream found.")
        try:
            if exact:
                back = True
            else:
                back = backward
            if timestamp is not None:
                seek_pos = int(timestamp * av.time_base)
                self._container.seek(seek_pos, backward=back)
            elif frame_index is not None:
                target_pts = (
                    self._video_stream.time_base
                    * frame_index
                    / self._video_stream.average_rate
                )
                self._container.seek(int(target_pts), backward=back)
            else:
                raise BadRequestError(
                    "Either timestamp or frame_index must be provided."
                )

            for frame in self._container.decode(video=0):
                current_frame_timestamp = frame.pts * float(frame.time_base)
                current_frame_index = int(
                    frame.pts
                    * frame.time_base
                    * self._video_stream.average_rate
                )
                if (
                    exact
                    and timestamp is not None
                    and current_frame_timestamp < timestamp
                ):
                    continue
                elif (
                    exact
                    and frame_index is not None
                    and current_frame_index < frame_index
                ):
                    continue
                return VideoFrame(
                    timestamp=current_frame_timestamp,
                    frame_index=current_frame_index,
                    key_frame=frame.key_frame,
                    width=frame.width,
                    height=frame.height,
                    image=Image(_pil_image=frame.to_image()),
                )
        except av.error.EOFError:
            return None
        except av.error.PermissionError:
            return None
        return None

    def get_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        self.__setup__()
        if self._video_stream is None:
            raise BadRequestError("No video stream found.")
        try:
            for frame in self._container.decode(video=0):
                frame_timestamp = frame.pts * float(frame.time_base)
                frame_index = int(
                    frame.pts
                    * frame.time_base
                    * self._video_stream.average_rate
                )
                return VideoFrame(
                    timestamp=frame_timestamp,
                    frame_index=frame_index,
                    key_frame=frame.key_frame,
                    width=frame.width,
                    height=frame.height,
                    image=Image(_pil_image=frame.to_image()),
                )
            return None
        except av.error.EOFError:
            return None

    def get_audio(
        self,
        start: float | None = None,
        end: float | None = None,
        format: str | None = "mp3",
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        self.__setup__()
        if self._audio_stream is None:
            raise BadRequestError("No audio stream found.")
        if format is not None:
            format = format.lower()
        output_buffer = BytesIO()
        start_pts = None
        end_pts = None
        if start is not None:
            start_pts = int(
                start
                * self._audio_stream.time_base.denominator
                / self._audio_stream.time_base.numerator
            )
            self._container.seek(int(start * av.time_base))
        if end is not None:
            end_pts = int(
                end
                * self._audio_stream.time_base.denominator
                / self._audio_stream.time_base.numerator
            )

        output_container = av.open(output_buffer, mode="w", format=format)
        if format == "wav" and codec is None:
            codec = "pcm_s16le"
        elif codec is None:
            codec = format
        args: dict = {"codec_name": codec}
        if rate is not None:
            args["rate"] = rate
        if channels == 1:
            args["layout"] = "mono"
        output_stream: Any = output_container.add_stream(**args)

        start_timestamp = None
        end_timestamp = None
        for packet in self._container.demux(self._audio_stream):
            if (
                start_pts is not None
                and packet.pts is not None
                and packet.pts < start_pts
            ):
                continue
            if (
                end_pts is not None
                and packet.pts is not None
                and packet.pts > end_pts
            ):
                break
            for frame in packet.decode():
                current_frame_timestamp = frame.pts * float(frame.time_base)
                if start_timestamp is None:
                    start_timestamp = current_frame_timestamp
                end_timestamp = current_frame_timestamp
                for out_packet in output_stream.encode(frame):
                    output_container.mux(out_packet)

        # Flush the encoder and close the output container
        for out_packet in output_stream.encode(None):
            output_container.mux(out_packet)
        output_container.close()

        output_buffer.seek(0)
        if start_timestamp is None and end_timestamp is None:
            return None

        return VideoAudio(
            start=start_timestamp,
            end=end_timestamp,
            format=format,
            codec=codec,
            audio=Audio(stream=output_buffer),
        )

    def close(self) -> None:
        if self._container is not None:
            self._container.close()
            self._container = None

    def save(
        self,
        path: str,
    ) -> None:
        if self.__component__.content:
            if isinstance(self.__component__.content, str):
                with open(path, "wb") as f:
                    f.write(base64.b64decode(self.__component__.content))
            else:
                with open(path, "wb") as f:
                    f.write(self.__component__.content)
        elif self.__component__.source:
            source = self.__component__.source
            if source.lower().startswith(
                "http://"
            ) or source.lower().startswith("https://"):
                import httpx

                response = httpx.get(self.__component__.source)
                response.raise_for_status()
                with open(path, "wb") as f:
                    f.write(response.content)
            else:
                with open(source, "rb") as src_file:
                    with open(path, "wb") as dst_file:
                        dst_file.write(src_file.read())
        elif self.__component__.stream:
            self.__component__.stream.seek(0)
            with open(path, "wb") as f:
                f.write(self.__component__.stream.read())
        else:
            raise BadRequestError("No video data to save.")
