from typing import Any

from common.secrets import get_secrets

from x8.ai.multimodal_generation import MultimodalGeneration
from x8.ai.text_generation import TextGeneration

secrets = get_secrets()


class MultimodalGenerationProvider:
    OPENAI = "openai"
    GOOGLE = "google"


provider_types: dict[str, str] = {
    MultimodalGenerationProvider.OPENAI: "openai",
    MultimodalGenerationProvider.GOOGLE: "google",
}


provider_parameters: dict[str, dict[str, Any]] = {
    MultimodalGenerationProvider.OPENAI: {
        "api_key": secrets["OPENAI_API_KEY"],
    },
    MultimodalGenerationProvider.GOOGLE: {},
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = MultimodalGeneration(
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
