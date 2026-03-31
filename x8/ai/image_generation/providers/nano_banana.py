__all__ = ["NanoBanana"]

import base64
from typing import Any, List

from google.genai import Client
from google.genai import errors as google_errors
from google.genai import types

from x8._common.google_provider import GoogleProvider
from x8.content.image import ImageData
from x8.core import Response
from x8.core.exceptions import BadRequestError

from .._models import (
    ImageGenerationResult,
    ImageOutputFormat,
    ImageQuality,
    ImageSize,
    Usage,
)

# Mapping from unified pixel sizes to (aspect_ratio, resolution).
_SIZE_TO_ASPECT_RESOLUTION: dict[str, tuple[str, str | None]] = {
    "256x256": ("1:1", "512"),
    "512x512": ("1:1", "512"),
    "1024x1024": ("1:1", "1K"),
    "1536x1024": ("3:2", "1K"),
    "1024x1536": ("2:3", "1K"),
    "1792x1024": ("16:9", "2K"),
    "1024x1792": ("9:16", "2K"),
    "2048x2048": ("1:1", "2K"),
    "4096x4096": ("1:1", "4K"),
}

_OUTPUT_FORMAT_TO_MIME: dict[str, str] = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
}


class NanoBanana(GoogleProvider):
    vertexai: bool
    project: str | None
    location: str
    model: str
    api_key: str | None
    nparams: dict[str, Any] | None

    _client: Client
    _init: bool

    def __init__(
        self,
        vertexai: bool = True,
        project: str | None = None,
        location: str = "global",
        model: str = "gemini-2.5-flash-image",
        api_key: str | None = None,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        self.vertexai = vertexai
        self.project = project
        self.location = location
        self.model = model
        self.api_key = api_key
        self.nparams = nparams
        self._init = False
        super().__init__(
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context=None):
        if self._init:
            return
        if self.api_key:
            credentials = None
        else:
            credentials = self._get_credentials()
        self._client = Client(
            vertexai=self.vertexai,
            api_key=self.api_key,
            credentials=credentials,
            project=(
                self._get_project_or_default(self.project)
                if self.vertexai
                else None
            ),
            location=self.location,
        )
        self._init = True

    async def __asetup__(self, context=None):
        return self.__setup__(context)

    # --- Generate ---

    def generate(
        self,
        prompt: str,
        *,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        self.__setup__()
        config = self._build_config(
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
        )
        try:
            response = self._client.models.generate_content(
                model=model or self.model,
                contents=[prompt],
                config=config,
            )
            return Response(result=self._convert_result(response))
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    async def agenerate(
        self,
        prompt: str,
        *,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        await self.__asetup__()
        config = self._build_config(
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
        )
        try:
            response = await self._client.aio.models.generate_content(
                model=model or self.model,
                contents=[prompt],
                config=config,
            )
            return Response(result=self._convert_result(response))
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    # --- Edit ---

    def edit(
        self,
        prompt: str,
        *,
        images: List[ImageData] | None = None,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        self.__setup__()
        config = self._build_config(
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
        )
        contents = self._build_edit_contents(prompt, images)
        try:
            response = self._client.models.generate_content(
                model=model or self.model,
                contents=contents,
                config=config,
            )
            return Response(result=self._convert_result(response))
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    async def aedit(
        self,
        prompt: str,
        *,
        images: List[ImageData] | None = None,
        model: str | None = None,
        n: int | None = None,
        size: ImageSize | None = None,
        quality: ImageQuality | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[ImageGenerationResult]:
        await self.__asetup__()
        config = self._build_config(
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
        )
        contents = self._build_edit_contents(prompt, images)
        try:
            response = await self._client.aio.models.generate_content(
                model=model or self.model,
                contents=contents,
                config=config,
            )
            return Response(result=self._convert_result(response))
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    # --- Helpers ---

    def _build_config(
        self,
        size: ImageSize | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
    ) -> types.GenerateContentConfig:
        aspect_ratio: str | None = None
        resolution: str | None = None

        # Derive aspect_ratio and resolution from size.
        if size and size != "auto":
            mapped = _SIZE_TO_ASPECT_RESOLUTION.get(size)
            if mapped:
                aspect_ratio = mapped[0]
                resolution = mapped[1]

        image_config_args: dict[str, Any] = {}
        if aspect_ratio is not None:
            image_config_args["aspect_ratio"] = aspect_ratio
        if resolution is not None:
            image_config_args["image_size"] = resolution
        if output_format is not None:
            mime = _OUTPUT_FORMAT_TO_MIME.get(output_format)
            if mime:
                image_config_args["output_mime_type"] = mime
        if output_compression is not None:
            image_config_args["output_compression_quality"] = (
                output_compression
            )

        config_args: dict[str, Any] = {
            "response_modalities": ["IMAGE"],
        }
        if image_config_args:
            config_args["image_config"] = types.ImageConfig(
                **image_config_args
            )

        if nconfig:
            config_args.update(nconfig)

        return types.GenerateContentConfig(**config_args)

    def _build_edit_contents(
        self,
        prompt: str,
        images: List[ImageData] | None = None,
    ) -> list:
        contents: list = [prompt]
        if images:
            for img in images:
                contents.append(self._convert_image_to_part(img))
        return contents

    def _convert_image_to_part(self, image: ImageData) -> types.Part:
        if image.content is not None:
            if isinstance(image.content, str):
                image_bytes = base64.b64decode(image.content)
            elif isinstance(image.content, bytes):
                image_bytes = image.content
            else:
                raise BadRequestError(
                    "Image content must be bytes or base64 string."
                )
            return types.Part(
                inline_data=types.Blob(
                    data=image_bytes,
                    mime_type=image.media_type or "image/png",
                )
            )
        if image.source:
            return types.Part(file_data=types.FileData(file_uri=image.source))
        raise BadRequestError(
            "Image must have either a source URL or content."
        )

    def _convert_result(
        self, response: types.GenerateContentResponse
    ) -> ImageGenerationResult:
        images = []
        text_parts = []

        if response.candidates:
            for candidate in response.candidates:
                if candidate.content and candidate.content.parts:
                    for part in candidate.content.parts:
                        if (
                            part.inline_data
                            and part.inline_data.mime_type
                            and part.inline_data.mime_type.startswith("image/")
                        ):
                            img_bytes = part.inline_data.data
                            images.append(
                                ImageData(
                                    content=base64.b64encode(img_bytes).decode(
                                        "utf-8"
                                    ),
                                    media_type=part.inline_data.mime_type,
                                )
                            )
                        elif part.text:
                            text_parts.append(part.text)

        usage = None
        if response.usage_metadata:
            um = response.usage_metadata
            usage = Usage(
                input_tokens=um.prompt_token_count or 0,
                output_tokens=um.candidates_token_count or 0,
                total_tokens=um.total_token_count or 0,
            )

        return ImageGenerationResult(
            revised_prompt=("\n".join(text_parts) if text_parts else None),
            usage=usage,
            images=images if images else None,
        )
