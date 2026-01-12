# type: ignore
import os

import pytest

from x8.content.video import VideoData

from ._providers import VideoProvider
from ._sync_and_async_client import VideoSyncAndAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VideoProvider.AV,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_single(provider_type: str, async_call: bool):
    video = VideoSyncAndAsyncClient(
        provider_type=provider_type,
        async_call=async_call,
        video_param=VideoData(source=get_file_path("test-video-1.mp4")),
    )

    info = await video.get_info()
    print(info)
    raise


def get_file_path(file):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "data",
        file,
    )
