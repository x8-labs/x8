from typing import Any

from x8.content.video import Video, VideoData


class VideoProvider:
    AV = "av"


provider_parameters: dict[str, dict[str, Any]] = {
    VideoProvider.AV: {},
}


def get_component(provider_type: str, video_param: VideoData):
    parameters = provider_parameters[provider_type]
    component = Video(
        data=video_param,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
