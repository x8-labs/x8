from typing import Any, AsyncIterator, Iterator, List, Literal, overload

from x8.content.image import ImageData
from x8.core import Component, Response, operation

from ._models import (
    ImageBackground,
    ImageGenerationResult,
    ImageGenerationStreamEvent,
    ImageInputFidelity,
    ImageModeration,
    ImageOutputFormat,
    ImageQuality,
    ImageSize,
    ImageStyle,
)


class ImageGeneration(Component):
    @overload
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
        stream: Literal[False] | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        """
        Generate images from a text prompt.

        Args:
            prompt:
                A text description of the desired image(s).
            model:
                The model to use for image generation.
            n:
                The number of images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            style:
                The style of the generated images (dall-e-3 only).
            background:
                Background transparency setting.
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
    def generate(
        self,
        prompt: str,
        *,
        stream: Literal[True],
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        style: ImageStyle | None = None,
        background: ImageBackground | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[ImageGenerationStreamEvent]]:
        """
        Generate images from a text prompt with streaming.

        Args:
            prompt:
                A text description of the desired image(s).
            stream:
                Whether to stream the response.
            model:
                The model to use for image generation.
            n:
                The number of images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            style:
                The style of the generated images (dall-e-3 only).
            background:
                Background transparency setting.
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
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
        """
        Generate images from a text prompt.

        Args:
            prompt:
                A text description of the desired image(s).
            model:
                The model to use for image generation.
            n:
                The number of images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            style:
                The style of the generated images (dall-e-3 only).
            background:
                Background transparency setting.
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
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
        stream: Literal[False] | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        """
        Edit or extend images given source images and a prompt.

        Args:
            prompt:
                A text description of the desired image edit.
            images:
                Input images to edit.
            mask:
                A mask image indicating areas to edit (inpainting).
            model:
                The model to use for image editing.
            n:
                The number of edited images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            background:
                Background transparency setting.
            input_fidelity:
                Controls fidelity to the original input image(s).
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
    def edit(
        self,
        prompt: str,
        *,
        stream: Literal[True],
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
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[ImageGenerationStreamEvent]]:
        """
        Edit or extend images given source images and a prompt, with streaming.

        Args:
            prompt:
                A text description of the desired image edit.
            stream:
                Whether to stream the response.
            images:
                Input images to edit.
            mask:
                A mask image indicating areas to edit (inpainting).
            model:
                The model to use for image editing.
            n:
                The number of edited images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            background:
                Background transparency setting.
            input_fidelity:
                Controls fidelity to the original input image(s).
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @operation(
        api={
            "path": "/edit",
            "method": "POST",
            "status": 201,
        }
    )
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
        """
        Edit or extend images given source images and a prompt.

        Args:
            prompt:
                A text description of the desired image edit.
            images:
                Input images to edit.
            mask:
                A mask image indicating areas to edit (inpainting).
            model:
                The model to use for image editing.
            n:
                The number of edited images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            background:
                Background transparency setting.
            input_fidelity:
                Controls fidelity to the original input image(s).
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    # --- Async versions ---

    @overload
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
        stream: Literal[False] | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> Response[ImageGenerationResult]:
        raise NotImplementedError

    @overload
    async def agenerate(
        self,
        prompt: str,
        *,
        stream: Literal[True],
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        style: ImageStyle | None = None,
        background: ImageBackground | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        moderation: ImageModeration | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> AsyncIterator[Response[ImageGenerationStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
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
        """
        Generate images from a text prompt.

        Args:
            prompt:
                A text description of the desired image(s).
            model:
                The model to use for image generation.
            n:
                The number of images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            style:
                The style of the generated images (dall-e-3 only).
            background:
                Background transparency setting.
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError

    @overload
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
        stream: Literal[False] | None = None,
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> Response[ImageGenerationResult]:
        raise NotImplementedError

    @overload
    async def aedit(
        self,
        prompt: str,
        *,
        stream: Literal[True],
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
        partial_images: int | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> AsyncIterator[Response[ImageGenerationStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "/edit",
            "method": "POST",
            "status": 201,
        }
    )
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
        """
        Edit or extend images given source images and a prompt.

        Args:
            prompt:
                A text description of the desired image edit.
            images:
                Input images to edit.
            mask:
                A mask image indicating areas to edit (inpainting).
            model:
                The model to use for image editing.
            n:
                The number of edited images to generate (1-10).
            size:
                The size of the generated images.
            quality:
                The quality of the generated images.
            background:
                Background transparency setting.
            input_fidelity:
                Controls fidelity to the original input image(s).
            output_format:
                The output image format (png, jpeg, webp).
            output_compression:
                Compression level (0-100) for jpeg/webp formats.
            moderation:
                Content moderation level.
            stream:
                Whether to stream the response.
            partial_images:
                Number of partial images for streaming (0-3).
            nconfig:
                Additional native configuration parameters.
        """
        raise NotImplementedError
