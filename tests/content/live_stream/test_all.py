# type: ignore
import os

import pytest

from x8.content.live_stream import LiveStreamParam

from ._providers import LiveStreamProvider
from ._sync_and_async_client import LiveStreamSyncAndAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        LiveStreamProvider.AV,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_single(provider_type: str, async_call: bool):
    live_stream = LiveStreamSyncAndAsyncClient(
        provider_type=provider_type,
        async_call=async_call,
        live_stream_param=LiveStreamParam(
            url="https://newsmax.codeautoplaycdn2.workers.dev/"
        ),
    )

    info = await live_stream.get_info()
    print(info)
    raise


def get_file_path(file):
    return os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "data",
        file,
    )
