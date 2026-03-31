from typing import Any

from common.secrets import get_secrets

from x8.ai.image_generation import ImageGeneration
from x8.ai.text_generation import TextGeneration

secrets = get_secrets()


class ImageGenerationProvider:
    OPENAI = "openai"
    IMAGEN = "imagen"
    NANO_BANANA = "nano_banana"


provider_types: dict[str, str] = {
    ImageGenerationProvider.OPENAI: "openai",
    ImageGenerationProvider.IMAGEN: "imagen",
    ImageGenerationProvider.NANO_BANANA: "nano_banana",
}


provider_parameters: dict[str, dict[str, Any]] = {
    ImageGenerationProvider.OPENAI: {
        "api_key": secrets["ai-openai-api-key"],
    },
    ImageGenerationProvider.IMAGEN: {},
    ImageGenerationProvider.NANO_BANANA: {},
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = ImageGeneration(
        __unpack__=True,
        __provider__=dict(
            type=provider_types[provider_type],
            parameters=parameters,
        ),
    )
    return component


def get_judge():
    component = TextGeneration(
        __unpack__=True,
        __provider__=dict(
            type="openai",
            parameters={
                "api_key": secrets["OPENAI_API_KEY"],
            },
        ),
    )
    return component
