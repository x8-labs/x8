from typing import Any

from common.secrets import get_secrets

from x8.ai.video_generation import VideoGeneration

secrets = get_secrets()


class VideoGenerationProvider:
    OPENAI = "openai"


provider_parameters: dict[str, dict[str, Any]] = {
    VideoGenerationProvider.OPENAI: {
        "api_key": secrets["ai-openai-api-key"],
    },
}


def get_component(provider_type: str):
    parameters = provider_parameters[provider_type]
    component = VideoGeneration(
        __unpack__=True,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
