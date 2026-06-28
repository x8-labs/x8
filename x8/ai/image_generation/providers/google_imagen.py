__all__ = ["GoogleImagen"]

import base64
from typing import Any

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
    "256x256": ("1:1", None),
    "512x512": ("1:1", None),
    "1024x1024": ("1:1", "1K"),
    "1536x1024": ("3:2", "1K"),
    "1024x1536": ("2:3", "1K"),
    "1792x1024": ("16:9", "2K"),
    "1024x1792": ("9:16", "2K"),
    "2048x2048": ("1:1", "2K"),
    "4096x4096": ("1:1", "4K"),
}

# Imagen supports only these aspect ratios.
_IMAGEN_ASPECT_RATIOS = {"1:1", "3:4", "4:3", "9:16", "16:9"}

# Approximate output tokens per image by resolution.
# Derived from Google's published Nano Banana pricing.
_RESOLUTION_TO_OUTPUT_TOKENS: dict[str | None, int] = {
    None: 747,
    "512": 747,
    "1K": 1120,
    "2K": 1680,
    "4K": 2520,
}

_OUTPUT_FORMAT_TO_MIME: dict[str, str] = {
    "png": "image/png",
    "jpeg": "image/jpeg",
    "webp": "image/webp",
}


class GoogleImagen(GoogleProvider):
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
        model: str = "imagen-4.0-generate-001",
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
        selected_model = model or self.model
        resolution = (
            _SIZE_TO_ASPECT_RESOLUTION[size][1]
            if size and size != "auto" and size in _SIZE_TO_ASPECT_RESOLUTION
            else None
        )
        config = self._build_config(
            n=n,
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            response = self._client.models.generate_images(
                model=selected_model,
                prompt=prompt,
                config=config,
            )
            return Response(
                result=self._convert_result(
                    response,
                    resolution,
                    model=selected_model,
                )
            )
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
        selected_model = model or self.model
        resolution = (
            _SIZE_TO_ASPECT_RESOLUTION[size][1]
            if size and size != "auto" and size in _SIZE_TO_ASPECT_RESOLUTION
            else None
        )
        config = self._build_config(
            n=n,
            size=size,
            output_format=output_format,
            output_compression=output_compression,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            response = await self._client.aio.models.generate_images(
                model=selected_model,
                prompt=prompt,
                config=config,
            )
            return Response(
                result=self._convert_result(
                    response,
                    resolution,
                    model=selected_model,
                )
            )
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    # --- Helpers ---

    def _build_config(
        self,
        n: int | None = None,
        size: ImageSize | None = None,
        output_format: ImageOutputFormat | None = None,
        output_compression: int | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> types.GenerateImagesConfig:
        config_args: dict[str, Any] = {}

        aspect_ratio: str | None = None
        resolution: str | None = None

        # Derive aspect_ratio and resolution from size.
        if size and size != "auto":
            mapped = _SIZE_TO_ASPECT_RESOLUTION.get(size)
            if mapped:
                ar = mapped[0]
                # Clamp to Imagen-supported ratios.
                if ar in _IMAGEN_ASPECT_RATIOS:
                    aspect_ratio = ar
                else:
                    aspect_ratio = "1:1"
                resolution = mapped[1]

        if aspect_ratio is not None:
            config_args["aspect_ratio"] = aspect_ratio
        if resolution is not None:
            config_args["image_size"] = resolution
        if n is not None:
            config_args["number_of_images"] = n
        if output_format is not None:
            mime = _OUTPUT_FORMAT_TO_MIME.get(output_format)
            if mime:
                config_args["output_mime_type"] = mime
        if output_compression is not None:
            config_args["output_compression_quality"] = output_compression

        if nconfig:
            config_args.update(nconfig)

        return types.GenerateImagesConfig(**config_args)

    def _convert_result(
        self,
        response: types.GenerateImagesResponse,
        resolution: str | None = None,
        model: str | None = None,
    ) -> ImageGenerationResult:
        images = None
        revised_prompt = None
        if response.generated_images:
            images = []
            for gen_img in response.generated_images:
                img_data = ImageData()
                if gen_img.image:
                    if gen_img.image.image_bytes:
                        img_data.content = base64.b64encode(
                            gen_img.image.image_bytes
                        ).decode("utf-8")
                    if gen_img.image.gcs_uri:
                        img_data.source = gen_img.image.gcs_uri
                    img_data.media_type = (
                        gen_img.image.mime_type or "image/png"
                    )
                images.append(img_data)
            if response.generated_images[0].enhanced_prompt:
                revised_prompt = response.generated_images[0].enhanced_prompt

        # Approximate output tokens from published per-resolution pricing.
        num_images = len(images) if images else 0
        tokens_per_image = _RESOLUTION_TO_OUTPUT_TOKENS.get(resolution, 747)
        output_tokens = num_images * tokens_per_image

        return ImageGenerationResult(
            model=model,
            revised_prompt=revised_prompt,
            images=images,
            usage=Usage(
                output_tokens=output_tokens,
                total_tokens=output_tokens,
            ),
        )
