import os
import time

import pytest

from x8.content.image import Image
from x8.content.video import Video
from x8.core.exceptions import BadRequestError

from ._providers import VideoGenerationProvider, get_judge
from ._sync_and_async_client import VideoGenerationSyncAndAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VideoGenerationProvider.OPENAI,
        VideoGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_simple(provider_type: str, async_call: bool):
    client = VideoGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.generate(prompt="dog and a cat")
    id = result.id
    while True:
        result = await client.get(id=id)
        if result.status in ["completed"]:
            break
        if result.status in ["failed"]:
            raise BadRequestError("Video generation failed")
        print(result.status)
        time.sleep(1)

    content = await client.download(id=id)
    chunks = (
        [chunk async for chunk in content] if async_call else list(content)
    )
    video = Video.load(b"".join(chunks))
    judge(video, keywords=["cat", "kitten", "dog"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VideoGenerationProvider.OPENAI,
        VideoGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_poll(provider_type: str, async_call: bool):
    client = VideoGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.generate(prompt="dog and a cat playing", poll=True)
    video = Video.load(result.videos[0])
    judge(video, keywords=["cat", "kitten", "dog"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VideoGenerationProvider.OPENAI,
        VideoGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_stream(provider_type: str, async_call: bool):
    client = VideoGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result_iterator = await client.generate(
        prompt="dog and a cat playing", stream=True
    )

    final_result = None
    if async_call:
        async for response in result_iterator:
            print(f"Status: {response.result.status}")
            final_result = response.result
            if response.result.status in ["completed", "failed"]:
                break
    else:
        for response in result_iterator:
            print(f"Status: {response.result.status}")
            final_result = response.result
            if response.result.status in ["completed", "failed"]:
                break

    if final_result.status == "failed":
        raise BadRequestError(f"Video generation failed: {final_result.error}")

    video = Video.load(final_result.videos[0])
    judge(video, keywords=["cat", "kitten", "dog"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        VideoGenerationProvider.OPENAI,
        VideoGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_image(provider_type: str, async_call: bool):
    client = VideoGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Load the dog image
    image_path = os.path.join(os.path.dirname(__file__), "images", "dog.jpg")
    image = Image.load(image_path)

    result = await client.generate(
        prompt="Make the dog run and jump", image=image.get_data(), poll=True
    )
    video = Video.load(result.videos[0])
    judge(video, keywords=["dog", "run", "jump"])


def judge(video, keywords: list[str]):
    frame = video.get_frame()

    judge = get_judge()
    result = judge.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image": {
                            "content": frame.image.convert(
                                type="bytes", format="jpeg"
                            ),
                            "media_type": "image/jpeg",
                        },
                    },
                    {
                        "type": "input_text",
                        "text": "Describe this image in one sentence.",
                    },
                ],
            }
        ],
    )
    message = result.output[0]
    text_content = message.content[0].text.lower()
    print(text_content)
    for keyword in keywords:
        if keyword in text_content:
            return
    raise AssertionError(
        f"Keywords {keywords} not found in generated text: {text_content}"
    )
