__all__ = ["OpenAI"]

import asyncio
import base64
import time
from typing import Any, AsyncIterator, Iterator, List, Mapping

from openai import NotFoundError as OpenAINotFoundError
from openai.types import Video
from x8.ai._common.openai_provider import OpenAIProvider
from x8.content.image import ImageData
from x8.content.video import VideoData
from x8.core import Response
from x8.core.exceptions import BadRequestError, NotFoundError

from .._models import KeyFrame, Reference, VideoGenerationResult, VideoSize


class OpenAI(OpenAIProvider):
    def __init__(
        self,
        model: str | None = "sora-2",
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
        websocket_base_url: str | None = None,
        webhook_secret: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        nparams: dict | None = None,
        **kwargs: Any,
    ):
        """Initialize.

        Args:
            model:
                OpenAI model to use for video generation.
            api_key:
                OpenAI API key.
            organization:
                OpenAI organization.
            project:
                OpenAI project.
            base_url:
                OpenAI base url.
            websocket_base_url:
                OpenAI websocket base url.
            webhook_secret:
                OpenAI webhook secret.
            timeout:
                Timeout for client.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            nparams:
                Native params for OpenAI client.
        """

        super().__init__(
            model=model,
            api_key=api_key,
            organization=organization,
            project=project,
            base_url=base_url,
            websocket_base_url=websocket_base_url,
            webhook_secret=webhook_secret,
            timeout=timeout,
            max_retries=max_retries,
            default_headers=default_headers,
            default_query=default_query,
            nparams=nparams,
            **kwargs,
        )

    def generate(
        self,
        prompt: str | None = None,
        *,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        poll: bool = False,
        poll_interval: float = 5.0,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[VideoGenerationResult]
        | Iterator[Response[VideoGenerationResult]]
    ):
        self.__setup__()
        args = self._convert_generate_args(
            prompt=prompt,
            image=image,
            references=references,
            key_frames=key_frames,
            negative_prompt=negative_prompt,
            audio=audio,
            duration=duration,
            size=size,
            samples=samples,
            seed=seed,
            quality=quality,
            nconfig=nconfig,
            **kwargs,
        )
        if not poll and not stream:
            response = self._client.videos.create(**args)
            result = self._convert_result(response)
        if stream:
            response = self._client.videos.create(**args)

            def _poll_iter() -> Iterator[Response[VideoGenerationResult]]:
                yield Response(result=self._convert_result(response))
                while True:
                    polled = self._client.videos.retrieve(response.id)
                    if polled.status == "completed":
                        content_response = (
                            self._client.videos.download_content(response.id)
                        )
                        base64_encoded_video = base64.b64encode(
                            content_response.read()
                        )
                        result = self._convert_result(polled)
                        result.videos = [
                            VideoData(
                                content=base64_encoded_video,
                                media_type="video/mp4",
                            )
                        ]
                        yield Response(result=result)
                        return
                    else:
                        yield Response(result=self._convert_result(polled))
                        if polled.status == "failed":
                            return
                    time.sleep(poll_interval)

            return _poll_iter()

        if poll:
            response = self._client.videos.create_and_poll(
                **args, poll_interval_ms=int(poll_interval * 1000)
            )
            content_response = self._client.videos.download_content(
                response.id,
                **kwargs,
            )
            base64_encoded_video = base64.b64encode(content_response.read())
            result = self._convert_result(response)
            result.videos = [
                VideoData(
                    content=base64_encoded_video,
                    media_type="video/mp4",
                )
            ]
        return Response(result=result)

    def get(self, id: str, **kwargs: Any) -> Response[VideoGenerationResult]:
        self.__setup__()
        try:
            result = self._client.videos.retrieve(id, **kwargs)
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")
        return Response(result=self._convert_result(result))

    def list(self, **kwargs: Any) -> Response[List[VideoGenerationResult]]:
        self.__setup__()
        results = self._client.videos.list(**kwargs)
        return Response(
            result=[self._convert_result(video) for video in results.data]
        )

    def delete(self, id: str, **kwargs: Any) -> Response[None]:
        self.__setup__()
        try:
            self._client.videos.delete(id, **kwargs)
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")
        return Response(result=None)

    def download(self, id: str, **kwargs: Any) -> Iterator[bytes]:
        self.__setup__()
        try:
            response = self._client.videos.download_content(id, **kwargs)
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")

        def stream() -> Iterator[bytes]:
            for chunk in response.iter_bytes():
                yield chunk

        return stream()

    async def agenerate(
        self,
        prompt: str | None = None,
        *,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        poll: bool = False,
        poll_interval: float = 5.0,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[VideoGenerationResult]
        | AsyncIterator[Response[VideoGenerationResult]]
    ):
        await self.__asetup__()
        args = self._convert_generate_args(
            prompt=prompt,
            image=image,
            references=references,
            key_frames=key_frames,
            negative_prompt=negative_prompt,
            audio=audio,
            duration=duration,
            size=size,
            samples=samples,
            seed=seed,
            quality=quality,
            nconfig=nconfig,
            **kwargs,
        )
        if not poll and not stream:
            response = await self._aclient.videos.create(**args)
            result = self._convert_result(response)
        if stream:
            response = await self._aclient.videos.create(**args)

            async def _poll_aiter() -> (
                AsyncIterator[Response[VideoGenerationResult]]
            ):
                yield Response(result=self._convert_result(response))
                while True:
                    polled = await self._aclient.videos.retrieve(response.id)
                    if polled.status == "completed":
                        content_response = (
                            await (
                                self._aclient.videos.download_content(
                                    response.id
                                )
                            )
                        )
                        base64_encoded_video = base64.b64encode(
                            content_response.read()
                        )
                        result = self._convert_result(polled)
                        result.videos = [
                            VideoData(
                                content=base64_encoded_video,
                                media_type="video/mp4",
                            )
                        ]
                        yield Response(result=result)
                        return
                    else:
                        yield Response(result=self._convert_result(polled))
                        if polled.status == "failed":
                            return
                    await asyncio.sleep(poll_interval)

            return _poll_aiter()
        if poll:
            response = await self._aclient.videos.create_and_poll(
                **args, poll_interval_ms=int(poll_interval * 1000)
            )
            result = self._convert_result(response)
            content_response = await self._aclient.videos.download_content(
                response.id,
                **kwargs,
            )
            base64_encoded_video = base64.b64encode(content_response.read())
            result = self._convert_result(response)
            result.videos = [
                VideoData(
                    content=base64_encoded_video,
                    media_type="video/mp4",
                )
            ]
        return Response(result=result)

    async def aget(
        self, id: str, **kwargs: Any
    ) -> Response[VideoGenerationResult]:
        await self.__asetup__()
        try:
            result = await self._aclient.videos.retrieve(id, **kwargs)
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")
        return Response(result=self._convert_result(result))

    async def alist(
        self, **kwargs: Any
    ) -> Response[List[VideoGenerationResult]]:
        await self.__asetup__()
        results = await self._aclient.videos.list(**kwargs)
        return Response(
            result=[self._convert_result(video) for video in results.data]
        )

    async def adelete(self, id: str, **kwargs: Any) -> Response[None]:
        await self.__asetup__()
        try:
            await self._aclient.videos.delete(id, **kwargs)
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")
        return Response(result=None)

    async def adownload(self, id: str, **kwargs: Any) -> AsyncIterator[bytes]:
        await self.__asetup__()
        try:
            response = await self._aclient.videos.download_content(
                id, **kwargs
            )
        except OpenAINotFoundError:
            raise NotFoundError(f"Video with id '{id}' not found.")

        async def stream() -> AsyncIterator[bytes]:
            for chunk in response.iter_bytes():
                yield chunk

        return stream()

    def _convert_result(self, video: Video) -> VideoGenerationResult:
        return VideoGenerationResult(
            id=video.id,
            created_at=video.created_at,
            expires_at=video.expires_at,
            completed_at=video.completed_at,
            duration=float(video.seconds),
            size=video.size,
            status=video.status,
            progress=video.progress,
            error=(
                f"{video.error.code}:{video.error.message}"
                if video.error
                else None
            ),
        )

    def _convert_generate_args(
        self,
        prompt: str | None = None,
        image: ImageData | None = None,
        references: List[Reference] | None = None,
        key_frames: List[KeyFrame] | None = None,
        negative_prompt: str | None = None,
        audio: bool | None = True,
        duration: float | None = None,
        size: VideoSize | None = None,
        samples: int | None = None,
        seed: int | None = None,
        quality: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        arg_prompt = prompt
        if negative_prompt:
            if arg_prompt is None:
                arg_prompt = f"not {negative_prompt}"
            else:
                arg_prompt = f"{arg_prompt}, not {negative_prompt}"

        arg_size = "1280x720"
        if size:
            arg_size = size
        arg_seconds = "4"
        if duration is not None:
            snapped = 4 if duration < 4 else 8 if duration < 8 else 12
            arg_seconds = str(snapped)

        arg_input_reference: str | bytes | None = None
        if image:
            arg_input_reference = self._get_input_reference(image)
        elif references and references[0].image:
            arg_input_reference = self._get_input_reference(
                references[0].image
            )
        elif key_frames and key_frames[0].image:
            arg_input_reference = self._get_input_reference(
                key_frames[0].image
            )

        args: dict[str, Any] = {
            "model": self.model,
            "prompt": arg_prompt,
            "size": arg_size,
            "seconds": arg_seconds,
        }
        if arg_input_reference is not None:
            args["input_reference"] = arg_input_reference
        args = args | kwargs
        return args

    def _get_input_reference(self, image: ImageData) -> Any:
        # 1. If user provided a file path or URL → pass it directly
        if image.source:
            return image.source

        # 2. Get raw bytes from content
        if isinstance(image.content, str):
            content_bytes = base64.b64decode(image.content)
        elif isinstance(image.content, bytes):
            content_bytes = image.content
        else:
            raise BadRequestError(
                "Image content must be bytes or base64 string."
            )

        # 3. Choose filename
        filename = getattr(image, "name", None) or "input"

        # 4. Ensure filename has extension based on media_type
        if image.media_type:
            media_type = image.media_type.lower()
            if "/" in image.media_type:
                ext = image.media_type.split("/")[-1]
                if "." not in filename:
                    filename = f"{filename}.{ext}"
        else:
            media_type = "image/png"
            filename = f"{filename}.png"

        # ✅ 5. Return OpenAI-supported format: (filename, bytes, media_type)
        return (filename, content_bytes, media_type)
