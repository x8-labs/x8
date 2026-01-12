from typing import Any

from x8.content.live_stream import LiveStream, LiveStreamParam


class LiveStreamProvider:
    AV = "av"


provider_parameters: dict[str, dict[str, Any]] = {
    LiveStreamProvider.AV: {},
}


def get_component(provider_type: str, live_stream_param: LiveStreamParam):
    parameters = provider_parameters[provider_type]
    component = LiveStream(
        param=live_stream_param,
        __provider__=dict(
            type=provider_type,
            parameters=parameters,
        ),
    )
    return component
