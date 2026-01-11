"""
Live Stream processed with av.
"""

__all__ = ["AV"]

from io import BytesIO
from typing import Any

import av
import av.error
import av.option
from x8.content.audio import Audio
from x8.content.image import Image
from x8.content.video import VideoAudio, VideoFrame
from x8.core import Context, Provider
from x8.core.exceptions import BadRequestError

from .._models import LiveStreamInfo


class AV(Provider):
    get_playlists: bool
    get_segments: bool

    _container: Any
    _video_stream: Any
    _audio_stream: Any
    _stream_id: Any
    _playlist_url: str | None

    def __init__(
        self,
        get_playlists: bool = True,
        get_segments: bool = False,
        **kwargs,
    ):
        """_summary_

        Args:
            get_playlists:
                A value indicating whether to directly parse
                the m3u8 master list to get the playlists and choose the
                best stream instead of passing the url to directly to av,
                which might load slowly.
                Defaults to True.
            get_segments:
                A value indicating whether to directly parse
                the m3u8 playlist to get the segments and
                pass the last segment to av.
                Defaults to True.
        """
        self.get_playlists = get_playlists
        self.get_segments = get_segments

        self._container = None
        self._video_stream = None
        self._audio_stream = None
        self._stream_id = None
        self._playlist_url = None

    def __setup__(self, context: Context | None = None) -> None:
        if self._container is not None:
            return
        av_url = self.__component__.source
        if self._playlist_url is not None:
            av_url = self._playlist_url
        else:
            if self.get_playlists:
                av_url = self._get_playlist_url(
                    self.__component__.source,
                    self.__component__.min_resolution,
                )
                self._playlist_url = av_url

        if self.get_segments:
            av_url = self._get_segment_url(av_url)

        self._container = av.open(av_url)
        if not self.get_playlists and not self.get_segments:
            self._init_best_stream_from_container(
                self.__component__.min_resolution
            )
        else:
            self._video_stream = self._container.streams.video[0]
            self._audio_stream = self._container.streams.audio[0]
            self._stream_id = 0

    def get_info(self, **kwargs) -> LiveStreamInfo:
        return LiveStreamInfo(
            width=self._video_stream.width,
            height=self._video_stream.height,
            total_frames=self._video_stream.frames,
            frame_rate=self._video_stream.average_rate,
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

    def get_frame(
        self,
        **kwargs,
    ) -> VideoFrame | None:
        if self._video_stream is None:
            raise BadRequestError("No video stream found.")
        try:
            for frame in self._container.decode(video=self._stream_id):
                frame_timestamp = frame.pts * float(frame.time_base)
                frame_index = None
                if self._video_stream.average_rate is not None:
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
        duration: float,
        format: str | None = "mp3",
        codec: str | None = None,
        rate: int | None = None,
        channels: int | None = None,
        **kwargs,
    ) -> VideoAudio | None:
        if self._audio_stream is None:
            raise BadRequestError("No audio stream found.")
        if format is not None:
            format = format.lower()
        output_buffer = BytesIO()
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
        done = False
        for packet in self._container.demux(self._audio_stream):
            for frame in packet.decode():
                if frame.pts is not None:
                    current_frame_timestamp = frame.pts * float(
                        frame.time_base
                    )
                    if start_timestamp is None:
                        start_timestamp = current_frame_timestamp
                    end_timestamp = current_frame_timestamp
                    if duration is not None:
                        if end_timestamp - start_timestamp >= duration:
                            done = True
                            break
                for out_packet in output_stream.encode(frame):
                    output_container.mux(out_packet)
            if done is True:
                break

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

    def close(self):
        if self._container is not None:
            self._container.close()
            self._container = None

    def _get_playlist_url(self, url: str, min_resolution: int) -> str:
        import urllib.parse

        import m3u8

        base_url = url.rsplit("/", 1)[0] + "/"
        selected_resolution = 100000
        selected_playlist: Any = None
        m3u8_obj = m3u8.load(url)
        for playlist in m3u8_obj.playlists:
            height = playlist.stream_info.resolution[1]
            if height >= min_resolution and height < selected_resolution:
                selected_playlist = playlist
                selected_resolution = height

        if selected_playlist is None:
            raise BadRequestError(
                "Video stream with specified min resolution not found."
            )
        return urllib.parse.urljoin(base_url, selected_playlist.uri)

    def _get_segment_url(self, url: str) -> str:
        import urllib.parse

        import m3u8

        base_url = url.rsplit("/", 1)[0] + "/"
        playlist_obj = m3u8.load(url)
        return urllib.parse.urljoin(base_url, playlist_obj.segments[-1].uri)

    def _init_best_stream_from_container(self, min_resolution: int):
        selected_resolution = 100000
        stream: Any
        for stream in self._container.streams.video:
            if (
                stream.height >= min_resolution
                and stream.height < selected_resolution
            ):
                self._video_stream = stream
                self._stream_id = stream.id
                selected_resolution = stream.height

        if self._stream_id is None:
            raise BadRequestError(
                "Video stream with specified min resolution not found."
            )

        for stream in self._container.streams.audio:
            if stream.id == self._stream_id:
                self._audio_stream = stream
