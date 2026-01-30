from typing import Any

from common.secrets import get_secrets

from x8.ai.text_generation import TextGeneration
from x8.ai.video_generation import VideoGeneration

secrets = get_secrets()


class VideoGenerationProvider:
    OPENAI = "openai"
    GOOGLE = "google"


provider_types: dict[str, str] = {
    VideoGenerationProvider.OPENAI: "openai",
    VideoGenerationProvider.GOOGLE: "google",
}


provider_parameters: dict[str, dict[str, Any]] = {
    VideoGenerationProvider.OPENAI: {
        "api_key": secrets["ai-openai-api-key"],
    },
    VideoGenerationProvider.GOOGLE: {},
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = VideoGeneration(
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
