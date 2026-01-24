import json
from pathlib import Path

import pytest

from x8.core.exceptions import BadRequestError

from ._providers import TextGenerationProvider
from ._sync_and_async_client import TextGenerationSyncAndAsyncClient


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_simple(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    result = await client.generate(
        input="What is 2 + 3", reasoning={"effort": "none"}
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    assert result.status == "completed"
    assert result.error is None

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.status == "completed"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content contains the answer
    text_content = message.content[0]
    assert text_content.type == "output_text"
    assert "5" in text_content.text

    # Verify usage
    assert result.usage is not None
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ_LLAMA,
        TextGenerationProvider.COHERE_VISION,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_image(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Load image from the images folder
    images_dir = Path(__file__).parent / "images"
    image_path = images_dir / "cat1.jpg"
    image_bytes = image_path.read_bytes()

    # Send image with description request
    result = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image": {
                            "content": image_bytes,
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
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    assert result.status == "completed"
    assert result.error is None

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.status == "completed"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content mentions cat
    text_content = message.content[0]
    assert text_content.type == "output_text"
    assert len(text_content.text) > 0
    # The image is of a cat, so the description should mention it
    assert (
        "cat" in text_content.text.lower()
        or "kitten" in text_content.text.lower()
    )

    # Verify usage
    assert result.usage is not None
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ_LLAMA,
        TextGenerationProvider.COHERE_VISION,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_multiple_images(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Load multiple images from the images folder
    images_dir = Path(__file__).parent / "images"
    cat_bytes = (images_dir / "cat1.jpg").read_bytes()
    dog_bytes = (images_dir / "dog1.jpg").read_bytes()

    # Send multiple images with a comparison request
    result = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_image",
                        "image": {
                            "content": cat_bytes,
                            "media_type": "image/jpeg",
                        },
                    },
                    {
                        "type": "input_image",
                        "image": {
                            "content": dog_bytes,
                            "media_type": "image/jpeg",
                        },
                    },
                    {
                        "type": "input_text",
                        "text": "How many animals are shown in total across these two images? What type of animals are they?",  # noqa
                    },
                ],
            }
        ],
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    assert result.status == "completed"
    assert result.error is None

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.status == "completed"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content mentions both animals
    text_content = message.content[0]
    assert text_content.type == "output_text"
    response_text = text_content.text.lower()

    # Should mention both cat and dog
    has_cat = "cat" in response_text or "kitten" in response_text
    has_dog = "dog" in response_text or "puppy" in response_text
    assert has_cat, f"Expected 'cat' in response: {response_text}"
    assert has_dog, f"Expected 'dog' in response: {response_text}"

    # Should mention "2" or "two" animals
    has_two = "2" in response_text or "two" in response_text
    assert has_two, f"Expected '2' or 'two' in response: {response_text}"

    # Verify usage
    assert result.usage is not None
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_multi_turn(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Turn 1: Introduce ourselves
    result1 = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": "My name is Alice. Please greet me by name.",
            }
        ],
    )

    # Verify turn 1 response
    assert result1.status == "completed"
    assert result1.output is not None
    assert len(result1.output) >= 1
    message1 = result1.output[0]
    assert message1.type == "message"
    assert message1.role == "assistant"
    text1 = message1.content[0].text
    assert "alice" in text1.lower()

    # Turn 2: Continue conversation using actual output from turn 1
    result2 = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": "My name is Alice. Please greet me by name.",
            },
            message1.to_dict(),  # Attach actual assistant response from turn 1
            {
                "type": "message",
                "role": "user",
                "content": "What is my name? Answer in one word.",
            },
        ],
    )

    # Verify turn 2 response remembers the name from context
    assert result2.status == "completed"
    assert result2.output is not None
    assert len(result2.output) >= 1
    message2 = result2.output[0]
    assert message2.type == "message"
    assert message2.role == "assistant"
    text2 = message2.content[0].text
    assert "alice" in text2.lower()

    # Verify usage for both turns
    assert result1.usage is not None
    assert result1.usage.total_tokens > 0
    assert result2.usage is not None
    assert result2.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_tool(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define a simple weather tool
    tools = [
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get the current weather for a location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city name",
                    },
                },
                "required": ["location"],
            },
        }
    ]

    # Ask about weather to trigger tool use
    result = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": "What is the weather in Paris?",
            }
        ],
        tools=tools,
        tool_choice="required",
        reasoning={"effort": "none"},
    )

    # Verify response
    assert result.status == "completed"
    assert result.output is not None
    assert len(result.output) >= 1

    # Find the function call in output
    function_call = None
    for item in result.output:
        if item.type == "function_call":
            function_call = item
            break

    # Verify function call
    assert function_call is not None, "Expected a function_call in output"
    assert function_call.name == "get_weather"
    assert function_call.arguments is not None
    assert "location" in function_call.arguments
    assert "paris" in function_call.arguments["location"].lower()

    # Verify usage
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_tool_multi_turn(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define weather and temperature tools
    tools = [
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get the current weather conditions for a location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city name",
                    },
                },
                "required": ["location"],
            },
        },
        {
            "type": "function",
            "name": "get_temperature",
            "description": "Get the temperature in celsius for a location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city name",
                    },
                },
                "required": ["location"],
            },
        },
    ]

    user_message = {
        "type": "message",
        "role": "user",
        "content": "What's the weather like in Tokyo? Also tell me the temperature.",  # noqa
    }

    # Turn 1: Ask about weather - should trigger get_weather tool call
    result1 = await client.generate(
        input=[user_message],
        tools=tools,
        tool_choice="required",
        reasoning={"effort": "none"},
    )

    assert result1.status == "completed"
    assert result1.output is not None

    # Find the first function call
    function_call1 = None
    for item in result1.output:
        if item.type == "function_call":
            function_call1 = item
            break

    assert function_call1 is not None, "Expected a function_call in turn 1"

    # Turn 2: Provide tool output, should trigger another tool call
    result2 = await client.generate(
        input=[
            user_message,
            function_call1.to_dict(),  # Attach the function call from turn 1
            {
                "type": "function_call_output",
                "call_id": function_call1.call_id or function_call1.id,
                "name": function_call1.name,
                "output": "Sunny with clear skies",
            },
        ],
        tools=tools,
        tool_choice="required",
        reasoning={"effort": "none"},
    )

    assert result2.status == "completed"
    assert result2.output is not None

    # Find the second function call
    function_call2 = None
    for item in result2.output:
        if item.type == "function_call":
            function_call2 = item
            break

    assert function_call2 is not None, "Expected a function_call in turn 2"

    # Turn 3: Provide second tool output, ask for final response
    result3 = await client.generate(
        input=[
            user_message,
            function_call1.to_dict(),
            {
                "type": "function_call_output",
                "call_id": function_call1.call_id or function_call1.id,
                "name": function_call1.name,
                "output": "Sunny with clear skies",
            },
            function_call2.to_dict(),
            {
                "type": "function_call_output",
                "call_id": function_call2.call_id or function_call2.id,
                "name": function_call2.name,
                "output": "22 degrees celsius",
            },
        ],
        tools=tools,
    )

    assert result3.status == "completed"
    assert result3.output is not None

    # Find the final message
    final_message = None
    for item in result3.output:
        if item.type == "message":
            final_message = item
            break

    assert final_message is not None, "Expected a message in turn 3"
    assert final_message.role == "assistant"
    assert final_message.content is not None
    assert len(final_message.content) >= 1

    # Verify the response mentions the weather info
    response_text = final_message.content[0].text.lower()
    assert "sunny" in response_text or "clear" in response_text
    assert (
        "22" in response_text
        or "celsius" in response_text
        or "temperature" in response_text
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_json(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define JSON schema for structured output
    person_schema = {
        "type": "object",
        "properties": {
            "name": {"type": "string"},
            "age": {"type": "integer"},
            "city": {"type": "string"},
        },
        "required": ["name", "age", "city"],
        "additionalProperties": False,
    }

    result = await client.generate(
        input="Extract: John is 30 years old and lives in New York.",
        text={
            "format": {
                "type": "json_schema",
                "name": "person",
                "schema": person_schema,
                "strict": True,
            }
        },
    )

    # Verify basic response structure
    assert result.status == "completed"
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content is valid JSON
    text_content = message.content[0]
    assert text_content.type == "output_text"

    # Parse the JSON response
    parsed = json.loads(text_content.text)

    # Verify the extracted data
    assert parsed["name"].lower() == "john"
    assert parsed["age"] == 30
    assert "new york" in parsed["city"].lower()

    # Verify usage
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_stream_simple(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Collect stream events
    events = []
    text_deltas = []
    final_response = None

    # Use a prompt that generates a longer response to ensure multiple deltas
    prompt = (
        "Write a short paragraph (3-4 sentences) about why the sky is blue."
    )

    if async_call:
        stream = await client.client.agenerate(
            input=prompt,
            reasoning={"effort": "none"},
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
            reasoning={"effort": "none"},
            stream=True,
        )
        for event in stream:
            events.append(event.result)
            if event.result.type == "output_text_delta":
                text_deltas.append(event.result.delta)
            elif event.result.type == "completed":
                final_response = event.result.response

    # Verify we received events
    assert len(events) > 0, "Expected at least one stream event"

    # Verify we got multiple text deltas (longer response should have multiple)
    assert (
        len(text_deltas) > 1
    ), f"Expected multiple text deltas, got {len(text_deltas)}"

    # Reconstruct text from deltas
    reconstructed_text = "".join(text_deltas)
    assert (
        len(reconstructed_text) > 50
    ), "Expected substantial text from deltas"
    # Response should mention sky, blue, light, or related terms
    lower_text = reconstructed_text.lower()
    assert any(
        word in lower_text
        for word in ["sky", "blue", "light", "scatter", "sun"]
    ), f"Expected sky-related content: {reconstructed_text[:100]}..."

    # Verify final completed response
    assert final_response is not None, "Expected a completed response"
    assert final_response.status == "completed"
    assert final_response.output is not None
    assert len(final_response.output) >= 1

    # Verify the final message structure
    message = final_response.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # For OpenAI, the final response contains the full accumulated text
    # For Google, each streaming chunk is independent, so the final response
    # only contains the last chunk's content.
    # We verify reconstructed text instead.
    if provider_type == TextGenerationProvider.OPENAI:
        final_text = message.content[0].text
        assert reconstructed_text == final_text, (
            f"Reconstructed text should match final text.\n"
            f"Reconstructed ({len(reconstructed_text)} chars): "
            f"{reconstructed_text[:100]}...\n"
            f"Final ({len(final_text)} chars): {final_text[:100]}..."
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        # Note: FIREWORKS excluded - qwen3-vl-235b-a22b-thinking model outputs
        # tool calls as text in its thinking process rather than using the
        # proper tool_calls API field during streaming.
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_stream_tool(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define a weather tool
    tools = [
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get the current weather for a location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city name",
                    },
                },
                "required": ["location"],
            },
        }
    ]

    # Collect stream events
    events = []
    function_call_deltas = []
    final_response = None

    prompt = "What is the weather in Paris?"

    if async_call:
        stream = await client.client.agenerate(
            input=prompt,
            tools=tools,
            tool_choice="required",
            reasoning={"effort": "none"},
            stream=True,
        )
        async for event in stream:
            events.append(event.result)
            if event.result.type == "function_call_arguments_delta":
                function_call_deltas.append(event.result.delta)
            elif event.result.type == "completed":
                final_response = event.result.response
    else:
        stream = client.client.generate(
            input=prompt,
            tools=tools,
            tool_choice="required",
            reasoning={"effort": "none"},
            stream=True,
        )
        for event in stream:
            events.append(event.result)
            if event.result.type == "function_call_arguments_delta":
                function_call_deltas.append(event.result.delta)
            elif event.result.type == "completed":
                final_response = event.result.response

    # Verify we received events
    assert len(events) > 0, "Expected at least one stream event"

    # Verify we got function call argument deltas
    assert (
        len(function_call_deltas) > 0
    ), "Expected at least one function_call_arguments_delta"

    # Reconstruct arguments from deltas
    reconstructed_args = "".join(function_call_deltas)
    assert len(reconstructed_args) > 0, "Expected non-empty arguments"

    # Verify final completed response
    assert final_response is not None, "Expected a completed response"
    assert final_response.status == "completed"
    assert final_response.output is not None
    assert len(final_response.output) >= 1

    # Find the function call in the final output
    function_call = None
    for item in final_response.output:
        if item.type == "function_call":
            function_call = item
            break

    assert (
        function_call is not None
    ), "Expected a function_call in final response"
    assert function_call.name == "get_weather"
    assert function_call.arguments is not None
    assert "location" in function_call.arguments
    assert "paris" in function_call.arguments["location"].lower()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_reasoning(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Ask a question that requires reasoning
    result = await client.generate(
        input="What is 15% of 80? Show your reasoning step by step. Provide a reasoning summary.",  # noqa
        reasoning={"effort": "medium", "summary": "detailed"},
    )

    # Verify basic response structure
    assert result.status == "completed"
    assert result.output is not None
    assert len(result.output) >= 1

    # Find reasoning and message outputs
    reasoning_output = None
    message_output = None
    for item in result.output:
        if item.type == "reasoning":
            reasoning_output = item
        elif item.type == "message":
            message_output = item

    # Verify we got reasoning output (when effort is not "none")
    assert (
        reasoning_output is not None
    ), "Expected reasoning output with effort='low'"
    assert reasoning_output.type == "reasoning"

    # Reasoning should have content (the thinking) or summary
    has_content = (
        reasoning_output.content is not None
        and len(reasoning_output.content) > 0
    )
    has_summary = (
        reasoning_output.summary is not None
        and len(reasoning_output.summary) > 0
    )
    assert has_content or has_summary, "Expected reasoning content or summary"

    # If we have content, verify it has reasoning text
    if has_content:
        reasoning_text = " ".join(
            item.text for item in reasoning_output.content if item.text
        )
        assert len(reasoning_text) > 0, "Expected non-empty reasoning content"

    # Verify we also got a message with the answer
    assert message_output is not None, "Expected message output"
    assert message_output.type == "message"
    assert message_output.role == "assistant"
    assert message_output.content is not None
    assert len(message_output.content) >= 1

    # Verify the answer contains "12" (15% of 80 = 12)
    answer_text = message_output.content[0].text
    assert "12" in answer_text, f"Expected '12' in answer: {answer_text}"

    # Verify usage includes reasoning tokens
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_system_message(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Use instructions to set system-level behavior
    result = await client.generate(
        input="What is 2 + 2?",
        instructions="You must always end your response with the exact phrase 'END_OF_RESPONSE' on its own line.",  # noqa
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.status == "completed"
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify the response follows the system instruction
    response_text = message.content[0].text
    assert (
        "END_OF_RESPONSE" in response_text
    ), f"Expected 'END_OF_RESPONSE' in response: {response_text}"

    # Verify usage
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.AMAZON_BEDROCK,
        # Note: COHERE uses documents parameter, not content items for PDFs
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_pdf(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Load PDF from the documents folder
    documents_dir = Path(__file__).parent / "documents"
    pdf_path = documents_dir / "sample.pdf"
    pdf_bytes = pdf_path.read_bytes()

    # Send PDF with summarization request
    result = await client.generate(
        input=[
            {
                "type": "message",
                "role": "user",
                "content": [
                    {
                        "type": "input_file",
                        "file": {
                            "content": pdf_bytes,
                            "filename": "sample.pdf",
                            "media_type": "application/pdf",
                        },
                    },
                    {
                        "type": "input_text",
                        "text": "Summarize this PDF document in 2-3 sentences.",  # noqa
                    },
                ],
            }
        ],
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    assert result.status == "completed"
    assert result.error is None

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.status == "completed"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content contains a summary
    text_content = message.content[0]
    assert text_content.type == "output_text"
    assert len(text_content.text) > 0
    # The response should be a substantive summary
    assert (
        len(text_content.text) > 50
    ), f"Expected substantive summary, got: {text_content.text}"
    # Check for document-related terms (some models may not mention "pdf")
    response_lower = text_content.text.lower()
    assert any(
        term in response_lower
        for term in ["pdf", "document", "file", "text", "content"]
    ), f"Expected document-related term in response: {text_content.text}"

    # Verify usage
    assert result.usage is not None
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_web_search(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define web search tool
    tools = [{"type": "web_search"}]

    # Ask a question that requires current information
    result = await client.generate(
        input="What is the current weather in Tokyo right now?",
        tools=tools,
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.status == "completed"
    assert result.output is not None
    assert len(result.output) >= 1

    # Find the message in output
    message = None
    for item in result.output:
        if item.type == "message":
            message = item
            break

    # Verify we got a response (web search should be used internally)
    assert message is not None, "Expected a message in output"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify the response contains weather-related information
    response_text = message.content[0].text.lower()
    weather_indicators = [
        "tokyo",
        "weather",
        "temperature",
        "degrees",
        "celsius",
        "fahrenheit",
        "sunny",
        "cloudy",
        "rain",
        "humidity",
        "forecast",
    ]
    has_weather_info = any(
        word in response_text for word in weather_indicators
    )
    assert (
        has_weather_info
    ), f"Expected weather-related response, got: {response_text[:200]}..."

    # Verify usage
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_error(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Send an invalid request that will cause an error
    # Using an invalid image format to trigger a bad request
    with pytest.raises(BadRequestError) as exc_info:
        await client.generate(
            input=[
                {
                    "type": "message",
                    "role": "user",
                    "content": [
                        {
                            "type": "input_image",
                            "image": {
                                "content": b"not a valid image",
                                "media_type": "image/jpeg",
                            },
                        },
                        {
                            "type": "input_text",
                            "text": "Describe this image.",
                        },
                    ],
                }
            ],
            reasoning={"effort": "none"},
        )

    # Verify the error message is from the native provider
    error_message = str(exc_info.value)
    assert len(error_message) > 0, "Expected non-empty error message"

    # OpenAI, Google, and Anthropic have different error message formats,
    # but all should contain useful information
    assert any(
        word in error_message.lower()
        for word in [
            "invalid",
            "image",
            "could not",
            "error",
            "failed",
            "base64",
        ]
    ), f"Expected informative error, got: {error_message}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.GROQ_LLAMA,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_max_output_tokens(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Request a long response but limit output tokens
    # A very small limit should truncate the response
    max_tokens = 20

    result = await client.generate(
        input="Write a very long essay about the history of computers, starting from the abacus to modern supercomputers. Include many details.",  # noqa
        max_output_tokens=max_tokens,
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    # Status may be "completed" or "incomplete" depending on provider
    assert result.status in ["completed", "incomplete"]

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify the output tokens are limited
    assert result.usage is not None
    assert result.usage.output_tokens > 0
    # Output tokens should be close to or at the limit
    # Allow some margin since tokenization can vary
    assert result.usage.output_tokens <= max_tokens + 5, (
        f"Expected output_tokens <= {max_tokens + 5}, "
        f"got {result.usage.output_tokens}"
    )

    # The response text should be relatively short due to token limit
    text_content = message.content[0]
    assert text_content.type == "output_text"
    # With only ~20 tokens, the response should be incomplete/short
    # (a typical word is 1-2 tokens, so expect roughly 10-20 words max)
    assert len(text_content.text) > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.OLLAMA,
        TextGenerationProvider.GROQ_LLAMA,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_parallel_tool_calls(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Define multiple tools
    tools = [
        {
            "type": "function",
            "name": "get_weather",
            "description": "Get the current weather for a location",
            "parameters": {
                "type": "object",
                "properties": {
                    "location": {
                        "type": "string",
                        "description": "The city name",
                    },
                },
                "required": ["location"],
            },
        },
        {
            "type": "function",
            "name": "get_time",
            "description": "Get the current time in a timezone",
            "parameters": {
                "type": "object",
                "properties": {
                    "timezone": {
                        "type": "string",
                        "description": "The timezone (e.g., 'America/New_York')",  # noqa
                    },
                },
                "required": ["timezone"],
            },
        },
    ]

    # Ask a question that requires both tools
    result = await client.generate(
        input="What is the weather in Paris and what time is it in Tokyo?",
        tools=tools,
        tool_choice="required",
        parallel_tool_calls=True,
        reasoning={"effort": "none"},
    )

    # Verify response
    assert result.status == "completed"
    assert result.output is not None

    # Find all function calls in output
    function_calls = [
        item for item in result.output if item.type == "function_call"
    ]

    # Should have at least 2 function calls (weather and time)
    assert len(function_calls) >= 2, (
        f"Expected at least 2 function calls for parallel execution, "
        f"got {len(function_calls)}"
    )

    # Verify we got both types of function calls
    function_names = {fc.name for fc in function_calls}
    assert (
        "get_weather" in function_names
    ), f"Expected get_weather call, got: {function_names}"
    assert (
        "get_time" in function_names
    ), f"Expected get_time call, got: {function_names}"

    # Verify the arguments
    for fc in function_calls:
        if fc.name == "get_weather":
            assert "location" in fc.arguments
            assert "paris" in fc.arguments["location"].lower()
        elif fc.name == "get_time":
            assert "timezone" in fc.arguments
            # Should contain tokyo or asia
            tz = fc.arguments["timezone"].lower()
            assert "tokyo" in tz or "asia" in tz or "japan" in tz

    # Verify usage
    assert result.usage is not None
    assert result.usage.total_tokens > 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "provider_type",
    [
        TextGenerationProvider.MISTRAL,
        TextGenerationProvider.OPENAI,
        TextGenerationProvider.OPENAI_LEGACY,
        TextGenerationProvider.GOOGLE,
        TextGenerationProvider.ANTHROPIC,
        TextGenerationProvider.XAI,
        TextGenerationProvider.DEEPSEEK,
        TextGenerationProvider.TOGETHER,
        TextGenerationProvider.FIREWORKS,
        TextGenerationProvider.GROQ,
        TextGenerationProvider.COHERE,
        TextGenerationProvider.AZURE_OPENAI,
        TextGenerationProvider.AMAZON_BEDROCK,
    ],
)
@pytest.mark.parametrize(
    "async_call",
    [False, True],
)
async def test_code_generation(provider_type: str, async_call: bool):
    client = TextGenerationSyncAndAsyncClient(
        provider_type=provider_type, async_call=async_call
    )

    # Request a simple Python function
    result = await client.generate(
        input="Write a Python function called 'fibonacci' that returns the nth Fibonacci number. Only provide the code, no explanation.",  # noqa
        reasoning={"effort": "none"},
    )

    # Verify basic response structure
    assert result.id is not None
    assert result.model is not None
    assert result.status == "completed"
    assert result.error is None

    # Verify output
    assert result.output is not None
    assert len(result.output) >= 1

    # Verify assistant message
    message = result.output[0]
    assert message.type == "message"
    assert message.role == "assistant"
    assert message.content is not None
    assert len(message.content) >= 1

    # Verify text content contains code
    text_content = message.content[0]
    assert text_content.type == "output_text"
    response_text = text_content.text

    # Should contain the function definition
    assert (
        "def fibonacci" in response_text or "def Fibonacci" in response_text
    ), f"Expected 'def fibonacci' in response: {response_text[:200]}..."

    # Should contain Python keywords
    assert (
        "return" in response_text
    ), f"Expected 'return' in response: {response_text[:200]}..."

    # Should likely have code block markers (```python or ```)
    # Note: Some models might not use markdown, so this is optional
    has_code_block = "```" in response_text or "def fibonacci" in response_text
    assert (
        has_code_block
    ), f"Expected code in response: {response_text[:200]}..."

    # Verify usage
    assert result.usage is not None
    assert result.usage.input_tokens > 0
    assert result.usage.output_tokens > 0
    assert result.usage.total_tokens > 0
