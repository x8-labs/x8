import base64
import uuid
from typing import Any, AsyncIterator, Iterator

import replicate
from replicate import Client as ReplicateClient
from replicate.stream import ServerSentEvent

from x8.core import Response
from x8.core._provider import Provider
from x8.core.exceptions import BadRequestError

from .._models import (
    InputItem,
    OutputItem,
    OutputMessage,
    OutputText,
    Reasoning,
    ResponseCompletedEvent,
    ResponseOutputTextDeltaEvent,
    ResponseText,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
    Usage,
)


class Replicate(Provider):
    """Replicate AI provider using the native Replicate SDK.

    Replicate provides access to many AI models including Gemini, GPT,
    Llama, and more through a unified API.

    Note: Replicate uses a prompt-based API rather than chat messages.
    Tool calling is not supported by most Replicate models.
    """

    api_key: str | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: ReplicateClient
    _init: bool
    _ainit: bool

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "google/gemini-3-pro",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                Replicate API token.
            model:
                Replicate model to use for text generation.
                Popular models include:
                - google/gemini-3-pro
                - openai/gpt-4o
                - meta/llama-3.3-70b-instruct
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for Replicate client.
        """
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.nparams = nparams
        self._init = False
        self._ainit = False
        super().__init__(**kwargs)

    def __setup__(self, context=None):
        if self._init:
            return
        client_kwargs: dict[str, Any] = {}
        if self.api_key is not None:
            client_kwargs["api_token"] = self.api_key
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = ReplicateClient(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        # Replicate uses the same client for sync and async
        client_kwargs: dict[str, Any] = {}
        if self.api_key is not None:
            client_kwargs["api_token"] = self.api_key
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = ReplicateClient(**client_kwargs)
        self._ainit = True

    def generate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        instructions: str | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        max_output_tokens: int | None = None,
        text: dict | ResponseText | None = None,
        tools: list[dict | Tool] | None = None,
        tool_choice: dict | ToolChoice | None = None,
        parallel_tool_calls: bool | None = None,
        max_tool_calls: int | None = None,
        reasoning: dict | Reasoning | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[TextGenerationResult]
        | Iterator[Response[TextGenerationStreamEvent]]
    ):
        model_ref, input_args = self._convert_args(
            input=input,
            model=model,
            instructions=instructions,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            text=text,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            max_tool_calls=max_tool_calls,
            reasoning=reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            if not stream:
                output = self._client.run(model_ref, input=input_args)
                # Output is typically a list of strings or a single string
                if isinstance(output, list):
                    text_content = "".join(str(item) for item in output)
                else:
                    text_content = str(output) if output else ""

                result = self._convert_result(
                    text_content,
                    model=model_ref,
                )
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    accumulated_text = ""
                    for event in self._client.stream(
                        model_ref, input=input_args
                    ):
                        if isinstance(event, ServerSentEvent):
                            if event.event == ServerSentEvent.EventType.OUTPUT:
                                delta = event.data
                                accumulated_text += delta
                                yield Response(
                                    result=ResponseOutputTextDeltaEvent(
                                        sequence_number=None,
                                        content_index=0,
                                        output_index=0,
                                        delta=delta,
                                    )
                                )

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=model_ref,
                        accumulated_text=accumulated_text,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _stream_iter()
        except replicate.exceptions.ReplicateError as e:
            raise BadRequestError(str(e)) from e

    async def agenerate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        instructions: str | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        max_output_tokens: int | None = None,
        text: dict | ResponseText | None = None,
        tools: list[dict | Tool] | None = None,
        tool_choice: dict | ToolChoice | None = None,
        parallel_tool_calls: bool | None = None,
        max_tool_calls: int | None = None,
        reasoning: dict | Reasoning | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> (
        Response[TextGenerationResult]
        | AsyncIterator[Response[TextGenerationStreamEvent]]
    ):
        model_ref, input_args = self._convert_args(
            input=input,
            model=model,
            instructions=instructions,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            text=text,
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            max_tool_calls=max_tool_calls,
            reasoning=reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            if not stream:
                output = await self._client.async_run(
                    model_ref, input=input_args
                )
                # Output is typically a list of strings or a single string
                if isinstance(output, list):
                    text_content = "".join(str(item) for item in output)
                else:
                    text_content = str(output) if output else ""

                result = self._convert_result(
                    text_content,
                    model=model_ref,
                )
                return Response(result=result)
            else:

                async def _poll_aiter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    accumulated_text = ""
                    # async_stream returns a coroutine, must await first
                    stream = await self._client.async_stream(
                        model_ref, input=input_args
                    )
                    async for event in stream:
                        if isinstance(event, ServerSentEvent):
                            if event.event == ServerSentEvent.EventType.OUTPUT:
                                delta = event.data
                                accumulated_text += delta
                                yield Response(
                                    result=ResponseOutputTextDeltaEvent(
                                        sequence_number=None,
                                        content_index=0,
                                        output_index=0,
                                        delta=delta,
                                    )
                                )

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=model_ref,
                        accumulated_text=accumulated_text,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _poll_aiter()
        except replicate.exceptions.ReplicateError as e:
            raise BadRequestError(str(e)) from e

    def _convert_args(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        instructions: str | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        max_output_tokens: int | None = None,
        text: dict | ResponseText | None = None,
        tools: list[dict | Tool] | None = None,
        tool_choice: dict | ToolChoice | None = None,
        parallel_tool_calls: bool | None = None,
        max_tool_calls: int | None = None,
        reasoning: dict | Reasoning | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[str, dict[str, Any]]:
        """Convert unified args to Replicate format."""
        # Model reference
        model_ref = model or self.model

        # Build input args for the model
        input_args: dict[str, Any] = {}

        # Convert input to prompt and extract images
        prompt, images = self._convert_input_to_prompt(input, instructions)
        input_args["prompt"] = prompt

        if images:
            input_args["images"] = images

        # System instruction (if model supports it)
        if instructions:
            input_args["system_instruction"] = instructions

        # Temperature
        if temperature is not None:
            input_args["temperature"] = temperature

        # Top P
        if top_p is not None:
            input_args["top_p"] = top_p

        # Max tokens
        if max_output_tokens is not None:
            input_args["max_output_tokens"] = max_output_tokens
        else:
            input_args["max_output_tokens"] = self.max_tokens

        # Reasoning / thinking level
        if reasoning is not None:
            if isinstance(reasoning, dict):
                reasoning = Reasoning(**reasoning)
            effort = reasoning.effort
            if effort == "none":
                pass  # No thinking
            elif effort == "low":
                input_args["thinking_level"] = "low"
            elif effort in ("medium", "high"):
                input_args["thinking_level"] = "high"

        # JSON schema mode - append instruction to prompt
        if text is not None:
            if isinstance(text, dict):
                text = ResponseText(**text)
            if text.format is not None:
                fmt = text.format
                if isinstance(fmt, dict):
                    fmt_type = fmt.get("type")
                    fmt_schema = fmt.get("schema", {})
                else:
                    fmt_type = getattr(fmt, "type", None)
                    fmt_schema = getattr(fmt, "schema_", {})

                if fmt_type in ("json_schema", "json_object"):
                    import json

                    schema_str = json.dumps(fmt_schema, indent=2)
                    input_args["prompt"] += (
                        f"\n\nRespond with valid JSON matching this schema:\n"
                        f"```json\n{schema_str}\n```"
                    )

        # Native config overrides
        if nconfig:
            input_args.update(nconfig)

        return model_ref, input_args

    def _convert_input_to_prompt(
        self,
        input: str | list[dict[str, Any] | InputItem],
        instructions: str | None = None,
    ) -> tuple[str, list[str]]:
        """Convert unified input format to prompt string and image list."""
        prompt_parts: list[str] = []
        images: list[str] = []

        # Handle simple string input
        if isinstance(input, str):
            return input, images

        # Handle list of items - build a conversation prompt
        for item in input:
            if isinstance(item, dict):
                item_type = item.get("type")
                if item_type == "message":
                    role = item.get("role", "user")
                    content = item.get("content", "")

                    if isinstance(content, str):
                        if role == "user":
                            prompt_parts.append(f"User: {content}")
                        elif role == "assistant":
                            prompt_parts.append(f"Assistant: {content}")
                        elif role == "system":
                            prompt_parts.append(f"System: {content}")
                    elif isinstance(content, list):
                        # Handle content parts
                        text_parts = []
                        for part in content:
                            if isinstance(part, dict):
                                part_type = part.get("type")
                                if part_type == "input_text":
                                    text_parts.append(part.get("text", ""))
                                elif part_type == "input_image":
                                    img = part.get("image", {})
                                    img_url = self._convert_image(img)
                                    if img_url:
                                        images.append(img_url)

                        if text_parts:
                            combined_text = " ".join(text_parts)
                            if role == "user":
                                prompt_parts.append(f"User: {combined_text}")
                            elif role == "assistant":
                                prompt_parts.append(
                                    f"Assistant: {combined_text}"
                                )
                            elif role == "system":
                                prompt_parts.append(f"System: {combined_text}")

                elif item_type == "function_call_output":
                    # Include tool results in the prompt
                    output = item.get("output", "")
                    prompt_parts.append(f"Tool Result: {output}")
            else:
                # Handle InputItem objects
                if hasattr(item, "type"):
                    if item.type == "message":
                        role = getattr(item, "role", "user")
                        content = getattr(item, "content", "")
                        if isinstance(content, str):
                            if role == "user":
                                prompt_parts.append(f"User: {content}")
                            elif role == "assistant":
                                prompt_parts.append(f"Assistant: {content}")

        # Join all parts
        if prompt_parts:
            return "\n\n".join(prompt_parts), images

        return "", images

    def _convert_image(self, image: dict[str, Any]) -> str | None:
        """Convert image to Replicate format (URL or data URI)."""
        if "url" in image:
            return image["url"]
        elif "content" in image:
            content = image["content"]
            media_type = image.get("media_type", "image/jpeg")
            if isinstance(content, bytes):
                b64 = base64.b64encode(content).decode("utf-8")
                return f"data:{media_type};base64,{b64}"
            elif isinstance(content, str):
                # Assume already base64 encoded
                return f"data:{media_type};base64,{content}"
        return None

    def _convert_result(
        self,
        text_content: str,
        model: str,
    ) -> TextGenerationResult:
        """Convert Replicate output to unified result format."""
        output_items: list[OutputItem] = []

        # Create message with text content
        message = OutputMessage(
            type="message",
            role="assistant",
            content=[OutputText(type="output_text", text=text_content)],
        )
        output_items.append(message)

        # Build usage (Replicate doesn't provide token counts in basic output)
        # Estimate tokens roughly: ~4 chars per token
        estimated_output_tokens = max(1, len(text_content) // 4)
        usage = Usage(
            input_tokens=1,  # Minimum to pass validation
            output_tokens=estimated_output_tokens,
            total_tokens=1 + estimated_output_tokens,
        )

        return TextGenerationResult(
            id=f"replicate-{uuid.uuid4().hex[:16]}",
            model=model,
            status="completed",
            output=output_items,
            text=text_content,
            usage=usage,
        )

    def _build_final_result(
        self,
        model: str,
        accumulated_text: str,
    ) -> TextGenerationResult:
        """Build final result from accumulated stream data."""
        output_items: list[OutputItem] = []

        # Create message with accumulated text
        if accumulated_text:
            message = OutputMessage(
                type="message",
                role="assistant",
                content=[
                    OutputText(type="output_text", text=accumulated_text)
                ],
            )
            output_items.append(message)

        # Estimate tokens roughly: ~4 chars per token
        estimated_output_tokens = max(1, len(accumulated_text) // 4)
        usage = Usage(
            input_tokens=1,  # Minimum to pass validation
            output_tokens=estimated_output_tokens,
            total_tokens=1 + estimated_output_tokens,
        )

        return TextGenerationResult(
            id=f"replicate-{uuid.uuid4().hex[:16]}",
            model=model,
            status="completed",
            output=output_items,
            text=accumulated_text,
            usage=usage,
        )
