import base64

import pytest

from x8.core.exceptions import BadRequestError

from ._providers import ImageGenerationProvider, get_judge
from ._sync_and_async_client import ImageGenerationSyncAndAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ImageGenerationProvider.OPENAI,
        ImageGenerationProvider.IMAGEN,
        ImageGenerationProvider.NANO_BANANA,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_simple(provider_type: str, async_call: bool):
    client = ImageGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.generate(prompt="a red apple on a wooden table")
    assert result.images is not None
    assert len(result.images) > 0
    assert result.usage is not None
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0

    judge(result.images[0], keywords=["apple", "red", "table"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ImageGenerationProvider.OPENAI,
        ImageGenerationProvider.IMAGEN,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_generate_multiple(provider_type: str, async_call: bool):
    client = ImageGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.generate(prompt="a red apple on a wooden table", n=2)
    assert result.images is not None
    assert len(result.images) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ImageGenerationProvider.OPENAI,
        ImageGenerationProvider.IMAGEN,
        ImageGenerationProvider.NANO_BANANA,
    ],
)
async def test_generate_with_options(provider_type: str):
    client = ImageGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=False
    )

    result = await client.generate(
        prompt="a blue ocean with mountains",
        size="1536x1024",
        output_format="jpeg",
    )
    assert result.images is not None
    assert len(result.images) > 0

    image = result.images[0]
    assert image.media_type == "image/jpeg"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ImageGenerationProvider.OPENAI,
        ImageGenerationProvider.NANO_BANANA,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_edit(provider_type: str, async_call: bool):
    client = ImageGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # First generate an image of a red apple
    original = await client.generate(prompt="a red apple on a wooden table")
    assert original.images is not None
    assert len(original.images) > 0

    # Edit it to make the apple green
    result = await client.edit(
        prompt="change the apple color to green",
        images=[original.images[0]],
    )
    assert result.images is not None
    assert len(result.images) > 0

    judge(result.images[0], keywords=["green", "apple", "table"])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        ImageGenerationProvider.OPENAI,
        ImageGenerationProvider.IMAGEN,
        ImageGenerationProvider.NANO_BANANA,
    ],
)
async def test_generate_invalid_prompt(provider_type: str):
    client = ImageGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=False
    )

    with pytest.raises(BadRequestError):
        await client.generate(prompt="")


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
