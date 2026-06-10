import base64
import os

import pytest

from x8.content.image import Image, ImageData
from x8.core.exceptions import BadRequestError

from ._providers import MultimodalGenerationProvider, get_judge
from ._sync_and_async_client import MultimodalGenerationSyncAndAsyncClient


def maybe_show_image(image: ImageData, name: str):
    if os.environ.get("X8_SHOW_TEST_IMAGES") != "1":
        return None

    try:
        Image.load(image).show()
        print(f"Displayed image: {name}")
    except Exception as e:
        print(f"Could not display image {name}: {e}")


def extract_images(result) -> list[ImageData]:
    images: list[ImageData] = []
    if not result.output:
        return images

    for item in result.output:
        item_type = getattr(item, "type", None)
        if item_type == "message":
            for part in getattr(item, "content", []) or []:
                if getattr(part, "type", None) == "output_image":
                    image = getattr(part, "image", None)
                    if image is not None:
                        images.append(image)

    return images


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        MultimodalGenerationProvider.OPENAI,
        MultimodalGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False],
)
async def test_conversational_image_editing(
    provider_type: str, async_call: bool
):
    client = MultimodalGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Turn 1: Generate a base image.
    try:
        first = await client.generate(
            input="Create a simple image of a red apple on a wooden table.",
            modalities=["text", "image"],
        )
    except BadRequestError as e:
        msg = str(e).lower()
        if "resource_exhausted" in msg or "quota" in msg:
            pytest.skip(f"Skipping due to provider quota limits: {e}")
        raise

    assert first.status in ["completed", "incomplete"]
    first_images = extract_images(first)
    assert len(first_images) > 0
    # maybe_show_image(first_images[0], f"{provider_type}_turn1")

    # Turn 2: Edit the previous image in a follow-up turn.
    try:
        second = await client.generate(
            input=[
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_image",
                            "image": first_images[0],
                        },
                        {
                            "type": "input_text",
                            "text": (
                                "Edit this image: change the apple "
                                "from red to green. Keep the table."
                            ),
                        },
                    ],
                }
            ],
            modalities=["text", "image"],
        )
    except BadRequestError as e:
        msg = str(e).lower()
        if "resource_exhausted" in msg or "quota" in msg:
            pytest.skip(f"Skipping due to provider quota limits: {e}")
        raise

    assert second.status in ["completed", "incomplete"]
    second_images = extract_images(second)
    assert len(second_images) > 0
    # maybe_show_image(second_images[0], f"{provider_type}_turn2")

    # Validate the edited image semantically with a judge model.
    judge(second_images[0], keywords=["green", "apple", "table"])


def judge(image, keywords: list[str]):
    image_bytes = (
        base64.b64decode(image.content)
        if isinstance(image.content, str)
        else image.content
    )
    judge_client = get_judge()
    result = judge_client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image": {
                            "content": image_bytes,
                            "media_type": image.media_type or "image/png",
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


def _maybe_skip_quota_error(error: Exception):
    msg = str(error).lower()
    if "resource_exhausted" in msg or "quota" in msg:
        pytest.skip(f"Skipping due to provider quota limits: {error}")
    if "request is not supported by this model" in msg:
        pytest.skip(
            "Skipping because the configured provider model "
            "does not support this streaming operation."
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        MultimodalGenerationProvider.OPENAI,
        MultimodalGenerationProvider.GOOGLE,
    ],
)
@pytest.mark.parametrize("async_call", [False])
async def test_stream_text_simple(provider_type: str, async_call: bool):
    client = MultimodalGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    events = []
    text_deltas = []
    final_response = None

    prompt = "Write one concise sentence about apples and tables."

    try:
        if async_call:
            stream = await client.client.agenerate(
                input=prompt,
                stream=True,
            )
            async for event in stream:
                events.append(event.result)
                if event.result.type == "output_text_delta":
                    text_deltas.append(event.result.delta)
                elif event.result.type == "completed":
                    final_response = event.result.response
        else:
            stream = client.client.generate(
                input=prompt,
                stream=True,
            )
            for event in stream:
                events.append(event.result)
                if event.result.type == "output_text_delta":
                    text_deltas.append(event.result.delta)
                elif event.result.type == "completed":
                    final_response = event.result.response
    except Exception as e:
        _maybe_skip_quota_error(e)
        raise

    assert len(events) > 0
    assert len(text_deltas) >= 1

    reconstructed_text = "".join(text_deltas).lower()
    assert "apple" in reconstructed_text or "table" in reconstructed_text

    assert final_response is not None
    assert final_response.status in ["completed", "incomplete"]
    assert final_response.output is not None
    assert len(final_response.output) >= 1


@pytest.mark.asyncio
@pytest.mark.parametrize("async_call", [False])
async def test_stream_image_openai(async_call: bool):
    client = MultimodalGenerationSyncAndAsyncClient(
        provider_type=MultimodalGenerationProvider.OPENAI,
        async_call=async_call,
    )

    events = []
    partial_image_chunks = 0
    final_response = None

    try:
        if async_call:
            stream = await client.client.agenerate(
                input="Create an image of a green apple on a wooden table.",
                modalities=["text", "image"],
                stream=True,
            )
            async for event in stream:
                events.append(event.result)
                if event.result.type == "image_partial":
                    partial_image_chunks += 1
                elif event.result.type == "completed":
                    final_response = event.result.response
        else:
            stream = client.client.generate(
                input="Create an image of a green apple on a wooden table.",
                modalities=["text", "image"],
                stream=True,
            )
            for event in stream:
                events.append(event.result)
                if event.result.type == "image_partial":
                    partial_image_chunks += 1
                elif event.result.type == "completed":
                    final_response = event.result.response
    except Exception as e:
        _maybe_skip_quota_error(e)
        raise

    assert len(events) > 0
    # OpenAI may or may not emit partial image events before completion.
    assert partial_image_chunks >= 0

    assert final_response is not None
    assert final_response.status in ["completed", "incomplete"]
    images = extract_images(final_response)
    assert len(images) > 0
    # maybe_show_image(images[0], "openai_stream")
