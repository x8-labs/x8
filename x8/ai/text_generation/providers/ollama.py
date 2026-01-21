import base64
import json
import uuid
from typing import Any, AsyncIterator, Iterator, Literal

import ollama as ollama_sdk

from x8.core import Response
from x8.core._provider import Provider
from x8.core.exceptions import BadRequestError

from .._models import (
    FunctionCall,
    InputItem,
    OutputItem,
    OutputMessage,
    OutputReasoning,
    OutputReasoningContentText,
    OutputText,
    Reasoning,
    ResponseCompletedEvent,
    ResponseFunctionCallArgumentsDeltaEvent,
    ResponseOutputTextDeltaEvent,
    ResponseReasoningTextDeltaEvent,
    ResponseText,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
    Usage,
)


class Ollama(Provider):
    """Ollama provider for local LLM inference.

    This provider uses the native Ollama Python SDK to interact with
    locally running Ollama models. Supports vision, tools, JSON output,
    and reasoning (think mode) for compatible models.

    Recommended models:
    - qwen3-vl:2b - Vision + tools + reasoning (1.9 GB, default)
    - qwen3-vl:8b - More capable vision model
    - qwen2.5:7b - Good tool support (no vision)
    """

    host: str | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: ollama_sdk.Client
    _async_client: ollama_sdk.AsyncClient
    _init: bool
    _ainit: bool

    def __init__(
        self,
        host: str | None = None,
        model: str = "qwen3-vl:2b",
        max_tokens: int = 4096,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            host:
                Ollama server host URL. Defaults to http://localhost:11434.
            model:
                Ollama model to use for text generation.
                Recommended: qwen3-vl:2b (vision/tools/reasoning).
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for Ollama client.
        """
        self.host = host
        self.model = model
        self.max_tokens = max_tokens
        self.nparams = nparams
        self._init = False
        self._ainit = False
        super().__init__(**kwargs)

    def __setup__(self, context=None):
        if self._init:
            return
        client_kwargs = {}
        if self.host is not None:
            client_kwargs["host"] = self.host
        self._client = ollama_sdk.Client(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        client_kwargs = {}
        if self.host is not None:
            client_kwargs["host"] = self.host
        self._async_client = ollama_sdk.AsyncClient(**client_kwargs)
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
        args, json_schema_mode, json_schema_tool_name, include_reasoning = (
            self._convert_args(
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
        )
        try:
            if not stream:
                response = self._client.chat(**args)
                result = self._convert_result(
                    response,
                    model=args.get("model", self.model),
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                    include_reasoning=include_reasoning,
                )
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    args["stream"] = True
                    response = self._client.chat(**args)
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    final_model = args.get("model", self.model)

                    for chunk in response:
                        for event in self._convert_stream_chunk(
                            chunk,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
                            include_reasoning=include_reasoning,
                        ):
                            if event.type == "output_text_delta":
                                accumulated_text += event.delta
                            elif event.type == "reasoning_text_delta":
                                accumulated_reasoning += event.delta
                            elif event.type == "function_call_arguments_delta":
                                idx = event.output_index or 0
                                if idx not in accumulated_tool_calls:
                                    accumulated_tool_calls[idx] = {
                                        "id": event.item_id,
                                        "name": "",
                                        "arguments": "",
                                    }
                                accumulated_tool_calls[idx][
                                    "arguments"
                                ] += event.delta
                            yield Response(result=event)

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=final_model,
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                        include_reasoning=include_reasoning,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _stream_iter()
        except ollama_sdk.ResponseError as e:
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
        args, json_schema_mode, json_schema_tool_name, include_reasoning = (
            self._convert_args(
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
        )
        try:
            if not stream:
                response = await self._async_client.chat(**args)
                result = self._convert_result(
                    response,
                    model=args.get("model", self.model),
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                    include_reasoning=include_reasoning,
                )
                return Response(result=result)
            else:

                async def _poll_aiter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    args["stream"] = True
                    response = await self._async_client.chat(**args)
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    final_model = args.get("model", self.model)

                    async for chunk in response:
                        for event in self._convert_stream_chunk(
                            chunk,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
                            include_reasoning=include_reasoning,
                        ):
                            if event.type == "output_text_delta":
                                accumulated_text += event.delta
                            elif event.type == "reasoning_text_delta":
                                accumulated_reasoning += event.delta
                            elif event.type == "function_call_arguments_delta":
                                idx = event.output_index or 0
                                if idx not in accumulated_tool_calls:
                                    accumulated_tool_calls[idx] = {
                                        "id": event.item_id,
                                        "name": "",
                                        "arguments": "",
                                    }
                                accumulated_tool_calls[idx][
                                    "arguments"
                                ] += event.delta
                            yield Response(result=event)

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=final_model,
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                        include_reasoning=include_reasoning,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _poll_aiter()
        except ollama_sdk.ResponseError as e:
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
    ) -> tuple[dict[str, Any], bool, str | None, bool]:
        """Convert unified args to Ollama chat format.

        Returns:
            Tuple of (args, json_schema_mode,
            json_schema_tool_name, include_reasoning)
        """
        args: dict[str, Any] = {}

        # Model
        args["model"] = model or self.model

        # Convert input to messages
        messages = self._convert_input_to_messages(input, instructions)
        args["messages"] = messages

        # Options for temperature, top_p, num_predict
        options: dict[str, Any] = {}
        if temperature is not None:
            options["temperature"] = temperature
        if top_p is not None:
            options["top_p"] = top_p
        if max_output_tokens is not None:
            # Only set num_predict if explicitly specified
            options["num_predict"] = max_output_tokens
        if options:
            args["options"] = options

        # Reasoning (think mode)
        include_reasoning = False
        if reasoning is not None:
            if isinstance(reasoning, dict):
                reasoning = Reasoning(**reasoning)
            effort = reasoning.effort
            if effort and effort != "none":
                # Ollama only accepts think=True/False, not effort strings
                args["think"] = True
                include_reasoning = True
            else:
                # Explicitly disable thinking when effort is "none"
                # to prevent model from spending tokens on thinking
                args["think"] = False

        # JSON schema mode - use format parameter
        json_schema_mode = False
        json_schema_tool_name = None
        if text is not None:
            if isinstance(text, dict):
                text = ResponseText(**text)
            if text.format is not None:
                fmt = text.format
                # Handle both dict and object formats
                if isinstance(fmt, dict):
                    fmt_type = fmt.get("type")
                    fmt_name = fmt.get("name", "json_output")
                    fmt_schema = fmt.get(
                        "schema", {"type": "object", "properties": {}}
                    )
                else:
                    # It's a ResponseFormat object
                    fmt_type = getattr(fmt, "type", None)
                    fmt_name = getattr(fmt, "name", "json_output")
                    fmt_schema = getattr(
                        fmt, "schema_", {"type": "object", "properties": {}}
                    )

                if fmt_type == "json_schema":
                    # Ollama supports JSON schema via format parameter
                    json_schema_mode = True
                    json_schema_tool_name = fmt_name
                    args["format"] = fmt_schema
                elif fmt_type == "json_object":
                    args["format"] = "json"

        # Tools
        if tools is not None:
            converted_tools = self._convert_tools(tools)
            if converted_tools:
                args["tools"] = converted_tools

        # Native config overrides
        if nconfig:
            args.update(nconfig)

        return args, json_schema_mode, json_schema_tool_name, include_reasoning

    def _convert_input_to_messages(
        self,
        input: str | list[dict[str, Any] | InputItem],
        instructions: str | None = None,
    ) -> list[dict[str, Any]]:
        """Convert unified input format to Ollama messages format."""
        messages: list[dict[str, Any]] = []

        # Add system message if instructions provided
        if instructions:
            messages.append({"role": "system", "content": instructions})

        # Handle simple string input
        if isinstance(input, str):
            messages.append({"role": "user", "content": input})
            return messages

        # Handle list of items
        for item in input:
            if isinstance(item, dict):
                item_type = item.get("type")
                if item_type == "message":
                    msg = self._convert_message_item(item)
                    if msg:
                        messages.append(msg)
                elif item_type == "function_call":
                    # Assistant message with tool call
                    tool_call = {
                        "function": {
                            "name": item.get("name"),
                            "arguments": item.get("arguments", {}),
                        },
                    }
                    messages.append(
                        {
                            "role": "assistant",
                            "content": "",
                            "tool_calls": [tool_call],
                        }
                    )
                elif item_type == "function_call_output":
                    # Tool result message
                    messages.append(
                        {
                            "role": "tool",
                            "content": str(item.get("output", "")),
                        }
                    )
            elif hasattr(item, "type"):
                # InputItem object
                if item.type == "message":
                    msg = self._convert_message_item(item.to_dict())
                    if msg:
                        messages.append(msg)

        return messages

    def _convert_message_item(
        self, item: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert a message item to Ollama format."""
        role = item.get("role")
        content = item.get("content")

        if role == "system":
            if isinstance(content, str):
                return {"role": "system", "content": content}
            elif isinstance(content, list):
                text_parts = []
                for c in content:
                    if isinstance(c, dict) and c.get("type") in [
                        "input_text",
                        "output_text",
                        "text",
                    ]:
                        text_parts.append(c.get("text", ""))
                    elif isinstance(c, str):
                        text_parts.append(c)
                return {"role": "system", "content": " ".join(text_parts)}
            return None

        if role == "user":
            if isinstance(content, str):
                return {"role": "user", "content": content}
            elif isinstance(content, list):
                # Convert content parts - handle images for vision models
                text_parts = []
                images = []
                for c in content:
                    if isinstance(c, dict):
                        c_type = c.get("type")
                        if c_type in ["input_text", "text"]:
                            text_parts.append(c.get("text", ""))
                        elif c_type == "input_image":
                            # Handle image content
                            img_data = self._convert_image_content(c)
                            if img_data:
                                images.append(img_data)
                        elif c_type == "input_file":
                            # Handle file content (PDF as image)
                            file_data = self._convert_file_content(c)
                            if file_data:
                                images.append(file_data)
                    elif isinstance(c, str):
                        text_parts.append(c)
                msg: dict[str, Any] = {
                    "role": "user",
                    "content": " ".join(text_parts),
                }
                if images:
                    msg["images"] = images
                return msg
            return None

        if role == "assistant":
            if isinstance(content, str):
                return {"role": "assistant", "content": content}
            elif isinstance(content, list):
                # Check for tool calls
                tool_calls = []
                text_parts = []
                for c in content:
                    if isinstance(c, dict):
                        c_type = c.get("type")
                        if c_type in ["output_text", "text"]:
                            text_parts.append(c.get("text", ""))
                        elif c_type == "function_call":
                            tool_calls.append(
                                {
                                    "function": {
                                        "name": c.get("name"),
                                        "arguments": c.get("arguments", {}),
                                    },
                                }
                            )
                result: dict[str, Any] = {"role": "assistant"}
                result["content"] = " ".join(text_parts) if text_parts else ""
                if tool_calls:
                    result["tool_calls"] = tool_calls
                return result
            return None

        return None

    def _convert_image_content(self, content: dict[str, Any]) -> str | None:
        """Convert input_image content to Ollama format (base64)."""
        image = content.get("image")
        if not isinstance(image, dict):
            return None

        img_content = image.get("content")

        if isinstance(img_content, (bytes, bytearray)):
            # Convert bytes to base64
            return base64.b64encode(img_content).decode("utf-8")
        elif isinstance(img_content, str):
            # Already base64 string
            return img_content

        return None

    def _convert_file_content(self, content: dict[str, Any]) -> str | None:
        """Convert input_file content to Ollama format (base64 for PDFs)."""
        file = content.get("file")
        if not isinstance(file, dict):
            return None

        file_content = file.get("content")

        if isinstance(file_content, (bytes, bytearray)):
            # Convert bytes to base64
            return base64.b64encode(file_content).decode("utf-8")
        elif isinstance(file_content, str):
            # Already base64 string
            return file_content

        return None

    def _convert_tools(
        self, tools: list[dict | Tool]
    ) -> list[dict[str, Any]] | None:
        """Convert unified tools to Ollama format."""
        converted = []
        for tool in tools:
            if isinstance(tool, dict):
                tool_type = tool.get("type")
                if tool_type == "function":
                    converted.append(
                        {
                            "type": "function",
                            "function": {
                                "name": tool.get("name"),
                                "description": tool.get("description", ""),
                                "parameters": tool.get("parameters", {}),
                            },
                        }
                    )
                elif tool_type == "web_search":
                    # Ollama doesn't have built-in web search
                    pass
            elif hasattr(tool, "type"):
                if tool.type == "function":
                    converted.append(
                        {
                            "type": "function",
                            "function": {
                                "name": tool.name,
                                "description": tool.description or "",
                                "parameters": tool.parameters or {},
                            },
                        }
                    )
        return converted if converted else None

    def _convert_result(
        self,
        response: Any,
        model: str,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
        include_reasoning: bool = False,
    ) -> TextGenerationResult:
        """Convert Ollama response to unified format."""
        output: list[OutputItem] = []
        response_id = str(uuid.uuid4())

        message = response.get("message", {})

        # Check for thinking/reasoning content - only include if requested
        thinking = message.get("thinking")
        if thinking and include_reasoning:
            output.append(
                OutputReasoning(
                    id=f"reasoning_{response_id}",
                    content=[
                        OutputReasoningContentText(
                            type="reasoning_text",
                            text=thinking,
                        )
                    ],
                    summary=None,
                    status="completed",
                )
            )

        # Check for tool calls
        tool_calls = message.get("tool_calls", [])
        if tool_calls:
            for i, tc in enumerate(tool_calls):
                func = tc.get("function", {})
                arguments = func.get("arguments", {})
                if isinstance(arguments, str):
                    try:
                        arguments = json.loads(arguments)
                    except (json.JSONDecodeError, TypeError):
                        arguments = {}

                call_id = f"call_{response_id}_{i}"
                output.append(
                    FunctionCall(
                        call_id=call_id,
                        name=func.get("name", ""),
                        id=call_id,
                        arguments=arguments,
                        status=None,
                    )
                )

        # Add text content if present
        text_content = message.get("content", "")
        if text_content and not tool_calls:
            output.append(
                OutputMessage(
                    id=f"msg_{response_id}",
                    role="assistant",
                    content=[OutputText(text=text_content, annotations=None)],
                    status="completed",
                )
            )

        # Build usage from response
        usage = None
        prompt_tokens = response.get("prompt_eval_count", 0)
        completion_tokens = response.get("eval_count", 0)
        if prompt_tokens or completion_tokens:
            usage = Usage(
                input_tokens=prompt_tokens,
                output_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            )

        # Determine status
        result_status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        if response.get("done") is False:
            result_status = "incomplete"

        return TextGenerationResult(
            id=response_id,
            model=model,
            output=output,
            usage=usage,
            status=result_status,
            error=None,
        )

    def _convert_stream_chunk(
        self,
        chunk: Any,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        include_reasoning: bool = False,
    ) -> Iterator[TextGenerationStreamEvent]:
        """Convert a streaming chunk to events."""
        message = chunk.get("message", {})

        # Check for thinking/reasoning content - only emit if requested
        thinking = message.get("thinking")
        if thinking and include_reasoning:
            yield ResponseReasoningTextDeltaEvent(
                sequence_number=None,
                delta=thinking,
                item_id=f"reasoning_{uuid.uuid4()}",
                output_index=0,
                content_index=0,
            )

        # Text content
        content = message.get("content")
        if content:
            yield ResponseOutputTextDeltaEvent(
                sequence_number=None,
                delta=content,
                item_id=f"msg_{uuid.uuid4()}",
                output_index=0,
                content_index=0,
            )

        # Tool calls
        tool_calls = message.get("tool_calls") or []
        for i, tc in enumerate(tool_calls):
            func = tc.get("function", {})
            arguments = func.get("arguments", {})
            if isinstance(arguments, dict):
                arguments = json.dumps(arguments)

            if i not in accumulated_tool_calls:
                accumulated_tool_calls[i] = {
                    "id": f"call_{uuid.uuid4()}",
                    "name": func.get("name", ""),
                    "arguments": "",
                }
            accumulated_tool_calls[i]["name"] = func.get("name", "")

            yield ResponseFunctionCallArgumentsDeltaEvent(
                sequence_number=None,
                delta=arguments,
                item_id=accumulated_tool_calls[i]["id"],
                output_index=i,
            )

    def _build_final_result(
        self,
        model: str,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
        include_reasoning: bool = False,
    ) -> TextGenerationResult:
        """Build final result from accumulated streaming data."""
        output: list[OutputItem] = []
        response_id = str(uuid.uuid4())

        # Add reasoning if present and requested
        if accumulated_reasoning and include_reasoning:
            output.append(
                OutputReasoning(
                    id=f"reasoning_{response_id}",
                    content=[
                        OutputReasoningContentText(
                            type="reasoning_text",
                            text=accumulated_reasoning,
                        )
                    ],
                    summary=None,
                    status="completed",
                )
            )

        # Add tool calls
        for idx, tc in accumulated_tool_calls.items():
            try:
                arguments = json.loads(tc["arguments"])
            except (json.JSONDecodeError, TypeError):
                arguments = {}

            output.append(
                FunctionCall(
                    call_id=tc["id"],
                    name=tc["name"],
                    id=tc["id"],
                    arguments=arguments,
                    status=None,
                )
            )

        # Add text if present
        if accumulated_text and not accumulated_tool_calls:
            output.append(
                OutputMessage(
                    id=f"msg_{response_id}",
                    role="assistant",
                    content=[
                        OutputText(text=accumulated_text, annotations=None)
                    ],
                    status="completed",
                )
            )

        return TextGenerationResult(
            id=response_id,
            model=model,
            output=output,
            usage=None,  # Usage not available in streaming
            status="completed",
            error=None,
        )
