from typing import Any

from common.secrets import get_secrets

from x8.ai.text_generation import TextGeneration

secrets = get_secrets()


class TextGenerationProvider:
    OPENAI = "openai"
    GOOGLE = "google"
    ANTHROPIC = "anthropic"


provider_parameters: dict[str, dict[str, Any]] = {
    TextGenerationProvider.OPENAI: {
        "api_key": secrets["OPENAI_API_KEY"],
    },
    TextGenerationProvider.GOOGLE: {},
    TextGenerationProvider.ANTHROPIC: {
        "api_key": secrets["ANTHROPIC_API_KEY"],
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = TextGeneration(
        __unpack__=True,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
