__all__ = ["OpenAI"]

import base64
from typing import Any, AsyncIterator, Iterator, List, Mapping

from openai import Stream
from openai.types import (
    ImageEditCompletedEvent,
    ImageEditPartialImageEvent,
    ImageEditStreamEvent,
    ImageGenCompletedEvent,
    ImageGenPartialImageEvent,
    ImageGenStreamEvent,
    ImagesResponse,
)

from x8.ai._common.openai_provider import OpenAIProvider
from x8.content.image import ImageData
from x8.core import Response
from x8.core.exceptions import BadRequestError

from .._models import (
    ImageBackground,
    ImageGenerationResult,
    ImageGenerationStreamEvent,
    ImageInputFidelity,
    ImageModeration,
    ImageOutputFormat,
    ImageQuality,
    ImageSize,
    ImageStyle,
    Usage,
)


class OpenAI(OpenAIProvider):
    def __init__(
        self,
        model: str | None = "gpt-image-1",
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
                OpenAI model to use for image generation.
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

    # --- Generate ---

    def generate(
        self,
        prompt: str,
        *,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        style: ImageStyle | None = None,
        background: ImageBackground | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[ImageGenerationResult]
        | Iterator[Response[ImageGenerationStreamEvent]]
    ):
        self.__setup__()
        args = self._convert_generate_args(
            prompt=prompt,
            model=model,
            n=n,
            size=size,
            quality=quality,
            style=style,
            background=background,
            output_format=output_format,
            output_compression=output_compression,
            moderation=moderation,
            stream=stream,
            partial_images=partial_images,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if stream:
                response = self._client.images.generate(**args)
                return self._stream_generate_iter(response)
            else:
                response = self._client.images.generate(**args)
                return Response(result=self._convert_result(response))
        except Exception as e:
            raise BadRequestError(str(e)) from e

    async def agenerate(
        self,
        prompt: str,
        *,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        style: ImageStyle | None = None,
        background: ImageBackground | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[ImageGenerationResult]
        | AsyncIterator[Response[ImageGenerationStreamEvent]]
    ):
        await self.__asetup__()
        args = self._convert_generate_args(
            prompt=prompt,
            model=model,
            n=n,
            size=size,
            quality=quality,
            style=style,
            background=background,
            output_format=output_format,
            output_compression=output_compression,
            moderation=moderation,
            stream=stream,
            partial_images=partial_images,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if stream:
                response = await self._aclient.images.generate(**args)
                return self._async_stream_generate_iter(response)
            else:
                response = await self._aclient.images.generate(**args)
                return Response(result=self._convert_result(response))
        except Exception as e:
            raise BadRequestError(str(e)) from e

    # --- Edit ---

    def edit(
        self,
        prompt: str,
        *,
        images: List[ImageData] | None = None,
        mask: ImageData | None = None,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        background: ImageBackground | None = None,
        input_fidelity: ImageInputFidelity | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[ImageGenerationResult]
        | Iterator[Response[ImageGenerationStreamEvent]]
    ):
        self.__setup__()
        args = self._convert_edit_args(
            prompt=prompt,
            images=images,
            mask=mask,
            model=model,
            n=n,
            size=size,
            quality=quality,
            background=background,
            input_fidelity=input_fidelity,
            output_format=output_format,
            output_compression=output_compression,
            moderation=moderation,
            stream=stream,
            partial_images=partial_images,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if stream:
                response = self._client.images.edit(**args)
                return self._stream_edit_iter(response)
            else:
                response = self._client.images.edit(**args)
                return Response(result=self._convert_result(response))
        except Exception as e:
            raise BadRequestError(str(e)) from e

    async def aedit(
        self,
        prompt: str,
        *,
        images: List[ImageData] | None = None,
        mask: ImageData | None = None,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        background: ImageBackground | None = None,
        input_fidelity: ImageInputFidelity | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[ImageGenerationResult]
        | AsyncIterator[Response[ImageGenerationStreamEvent]]
    ):
        await self.__asetup__()
        args = self._convert_edit_args(
            prompt=prompt,
            images=images,
            mask=mask,
            model=model,
            n=n,
            size=size,
            quality=quality,
            background=background,
            input_fidelity=input_fidelity,
            output_format=output_format,
            output_compression=output_compression,
            moderation=moderation,
            stream=stream,
            partial_images=partial_images,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if stream:
                response = await self._aclient.images.edit(**args)
                return self._async_stream_edit_iter(response)
            else:
                response = await self._aclient.images.edit(**args)
                return Response(result=self._convert_result(response))
        except Exception as e:
            raise BadRequestError(str(e)) from e

    # --- Conversion helpers ---

    def _convert_generate_args(
        self,
        prompt: str,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        style: ImageStyle | None = None,
        background: ImageBackground | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        args: dict[str, Any] = {
            "prompt": prompt,
            "model": model or self.model,
        }

        if n is not None:
            args["n"] = n
        if size is not None:
            args["size"] = size
        if quality is not None:
            args["quality"] = quality
        if style is not None:
            args["style"] = style
        if background is not None:
            args["background"] = background
        if output_format is not None:
            args["output_format"] = output_format
        if output_compression is not None:
            args["output_compression"] = output_compression
        if moderation is not None:
            args["moderation"] = moderation
        if stream:
            args["stream"] = True
        if partial_images is not None:
            args["partial_images"] = partial_images

        if nconfig:
            args.update(nconfig)
        args.update(kwargs)

        return args

    def _convert_edit_args(
        self,
        prompt: str,
        images: List[ImageData] | None = None,
        mask: ImageData | None = None,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        background: ImageBackground | None = None,
        input_fidelity: ImageInputFidelity | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        stream: bool | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        args: dict[str, Any] = {
            "prompt": prompt,
            "model": model or self.model,
        }

        if images:
            args["image"] = [self._convert_image_input(img) for img in images]
        if mask is not None:
            args["mask"] = self._convert_image_input(mask)
        if n is not None:
            args["n"] = n
        if size is not None:
            args["size"] = size
        if quality is not None:
            args["quality"] = quality
        if background is not None:
            args["background"] = background
        if input_fidelity is not None:
            args["input_fidelity"] = input_fidelity
        if output_format is not None:
            args["output_format"] = output_format
        if output_compression is not None:
            args["output_compression"] = output_compression
        if moderation is not None:
            args["moderation"] = moderation
        if stream:
            args["stream"] = True
        if partial_images is not None:
            args["partial_images"] = partial_images

        if nconfig:
            args.update(nconfig)
        args.update(kwargs)

        return args

    def _convert_image_input(self, image: ImageData) -> Any:
        # URL or file path source
        if image.source:
            return image.source

        # Raw content as file tuple
        if image.content is not None:
            if isinstance(image.content, str):
                content_bytes = base64.b64decode(image.content)
            elif isinstance(image.content, bytes):
                content_bytes = image.content
            else:
                raise BadRequestError(
                    "Image content must be bytes or base64 string."
                )

            filename = "image"
            media_type = image.media_type or "image/png"
            if "/" in media_type:
                ext = media_type.split("/")[-1]
                filename = f"{filename}.{ext}"

            return (filename, content_bytes, media_type)

        raise BadRequestError(
            "Image must have either a source URL or content."
        )

    def _convert_result(
        self, response: ImagesResponse
    ) -> ImageGenerationResult:
        images = None
        revised_prompt = None
        if response.data:
            media_type = (
                f"image/{response.output_format}"
                if response.output_format
                else "image/png"
            )
            images = [
                ImageData(
                    content=img.b64_json,
                    source=img.url,
                    media_type=media_type,
                )
                for img in response.data
            ]
            revised_prompt = response.data[0].revised_prompt

        usage = None
        if response.usage:
            usage = Usage(
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
                total_tokens=response.usage.total_tokens,
            )

        return ImageGenerationResult(
            created=response.created,
            revised_prompt=revised_prompt,
            background=(response.background if response.background else None),
            output_format=(
                response.output_format if response.output_format else None
            ),
            quality=response.quality if response.quality else None,
            size=response.size if response.size else None,
            usage=usage,
            images=images,
        )

    def _convert_gen_stream_event(
        self,
        event: ImageGenPartialImageEvent | ImageGenCompletedEvent,
    ) -> ImageGenerationStreamEvent:
        if isinstance(event, ImageGenPartialImageEvent):
            return ImageGenerationStreamEvent(
                type="partial_image",
                partial_image_index=event.partial_image_index,
                content=event.b64_json,
                background=(event.background if event.background else None),
                output_format=(
                    event.output_format if event.output_format else None
                ),
                quality=event.quality if event.quality else None,
                size=event.size if event.size else None,
            )
        else:
            usage = None
            if event.usage:
                usage = Usage(
                    input_tokens=event.usage.input_tokens,
                    output_tokens=event.usage.output_tokens,
                    total_tokens=event.usage.total_tokens,
                )
            return ImageGenerationStreamEvent(
                type="completed",
                content=event.b64_json,
                background=(event.background if event.background else None),
                output_format=(
                    event.output_format if event.output_format else None
                ),
                quality=event.quality if event.quality else None,
                size=event.size if event.size else None,
                usage=usage,
            )

    def _convert_edit_stream_event(
        self,
        event: ImageEditPartialImageEvent | ImageEditCompletedEvent,
    ) -> ImageGenerationStreamEvent:
        if isinstance(event, ImageEditPartialImageEvent):
            return ImageGenerationStreamEvent(
                type="partial_image",
                partial_image_index=event.partial_image_index,
                content=event.b64_json,
                background=(event.background if event.background else None),
                output_format=(
                    event.output_format if event.output_format else None
                ),
                quality=event.quality if event.quality else None,
                size=event.size if event.size else None,
            )
        else:
            usage = None
            if event.usage:
                usage = Usage(
                    input_tokens=event.usage.input_tokens,
                    output_tokens=event.usage.output_tokens,
                    total_tokens=event.usage.total_tokens,
                )
            return ImageGenerationStreamEvent(
                type="completed",
                content=event.b64_json,
                background=(event.background if event.background else None),
                output_format=(
                    event.output_format if event.output_format else None
                ),
                quality=event.quality if event.quality else None,
                size=event.size if event.size else None,
                usage=usage,
            )

    def _stream_generate_iter(
        self, response: Stream[ImageGenStreamEvent]
    ) -> Iterator[Response[ImageGenerationStreamEvent]]:
        def _iter() -> Iterator[Response[ImageGenerationStreamEvent]]:
            for event in response:
                yield Response(result=self._convert_gen_stream_event(event))

        return _iter()

    def _stream_edit_iter(
        self, response: Stream[ImageEditStreamEvent]
    ) -> Iterator[Response[ImageGenerationStreamEvent]]:
        def _iter() -> Iterator[Response[ImageGenerationStreamEvent]]:
            for event in response:
                yield Response(result=self._convert_edit_stream_event(event))

        return _iter()

    async def _async_stream_generate_iter(
        self, response: Any
    ) -> AsyncIterator[Response[ImageGenerationStreamEvent]]:
        async def _aiter() -> (
            AsyncIterator[Response[ImageGenerationStreamEvent]]
        ):
            async for event in response:
                yield Response(result=self._convert_gen_stream_event(event))

        return _aiter()

    async def _async_stream_edit_iter(
        self, response: Any
    ) -> AsyncIterator[Response[ImageGenerationStreamEvent]]:
        async def _aiter() -> (
            AsyncIterator[Response[ImageGenerationStreamEvent]]
        ):
            async for event in response:
                yield Response(result=self._convert_edit_stream_event(event))

        return _aiter()
