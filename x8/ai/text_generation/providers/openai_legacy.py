import base64
import json
import uuid
from typing import Any, AsyncIterator, Iterator, Literal, Mapping

import openai

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


class OpenAILegacy(Provider):
    """OpenAI provider using Chat Completions API.

    This provider uses the traditional Chat Completions API
    (chat.completions.create) instead of the newer Responses API.
    It's compatible with OpenAI and any OpenAI-compatible API
    endpoints (DeepSeek, Together AI, etc.).
    """

    api_key: str | None
    base_url: str | None
    timeout: float | None
    max_retries: int | None
    default_headers: Mapping[str, str] | None
    default_query: Mapping[str, object] | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: openai.OpenAI
    _async_client: openai.AsyncOpenAI
    _init: bool
    _ainit: bool

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        model: str = "gpt-4o",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                OpenAI API key.
            base_url:
                API base url. None uses OpenAI's default.
            timeout:
                Timeout for client requests.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            model:
                Model to use for text generation.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for OpenAI client.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.max_retries = max_retries
        self.default_headers = default_headers
        self.default_query = default_query
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
        if self.api_key is not None:
            client_kwargs["api_key"] = self.api_key
        if self.base_url is not None:
            client_kwargs["base_url"] = self.base_url
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.max_retries is not None:
            client_kwargs["max_retries"] = self.max_retries
        if self.default_headers is not None:
            client_kwargs["default_headers"] = self.default_headers
        if self.default_query is not None:
            client_kwargs["default_query"] = self.default_query
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = openai.OpenAI(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        client_kwargs = {}
        if self.api_key is not None:
            client_kwargs["api_key"] = self.api_key
        if self.base_url is not None:
            client_kwargs["base_url"] = self.base_url
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.max_retries is not None:
            client_kwargs["max_retries"] = self.max_retries
        if self.default_headers is not None:
            client_kwargs["default_headers"] = self.default_headers
        if self.default_query is not None:
            client_kwargs["default_query"] = self.default_query
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._async_client = openai.AsyncOpenAI(**client_kwargs)
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
        args, json_schema_mode, json_schema_tool_name = self._convert_args(
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
                response = self._client.chat.completions.create(**args)
                result = self._convert_result(
                    response,
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                )
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    response = self._client.chat.completions.create(**args)
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    final_model = None

                    for chunk in response:
                        final_model = chunk.model
                        for event in self._convert_stream_chunk(
                            chunk,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
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
                        model=final_model or args.get("model", self.model),
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _stream_iter()
        except openai.BadRequestError as e:
            raise BadRequestError(str(e.message)) from e

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
        args, json_schema_mode, json_schema_tool_name = self._convert_args(
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
                response = await self._async_client.chat.completions.create(
                    **args
                )
                result = self._convert_result(
                    response,
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                )
                return Response(result=result)
            else:

                async def _poll_aiter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    response = (
                        await self._async_client.chat.completions.create(
                            **args
                        )
                    )
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    final_model = None

                    async for chunk in response:
                        final_model = chunk.model
                        for event in self._convert_stream_chunk(
                            chunk,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
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
                        model=final_model or args.get("model", self.model),
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _poll_aiter()
        except openai.BadRequestError as e:
            raise BadRequestError(str(e.message)) from e

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
    ) -> tuple[dict[str, Any], bool, str | None]:
        """Convert unified args to OpenAI Chat Completions format."""
        args: dict[str, Any] = {}

        # Model
        args["model"] = model or self.model

        # Convert input to messages
        messages = self._convert_input_to_messages(input, instructions)
        args["messages"] = messages

        # Temperature
        if temperature is not None:
            args["temperature"] = temperature

        # Top P
        if top_p is not None:
            args["top_p"] = top_p

        # Max tokens
        if max_output_tokens is not None:
            args["max_tokens"] = max_output_tokens
        else:
            args["max_tokens"] = self.max_tokens

        # Stream
        if stream:
            args["stream"] = True

        # JSON schema mode - use tool-based approach for strict schema
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
                    fmt_description = fmt.get(
                        "description",
                        "Generate structured JSON output matching schema",
                    )
                else:
                    # It's a ResponseFormat object
                    fmt_type = getattr(fmt, "type", None)
                    fmt_name = getattr(fmt, "name", "json_output")
                    # schema_ is the actual attribute name (schema is reserved)
                    fmt_schema = getattr(
                        fmt, "schema_", {"type": "object", "properties": {}}
                    )
                    fmt_description = getattr(
                        fmt,
                        "description",
                        "Generate structured JSON output matching schema",
                    )

                if fmt_type == "json_schema":
                    # Use tool-based approach for strict schema adherence
                    json_schema_mode = True
                    json_schema_tool_name = fmt_name

                    # Create a tool that enforces the schema
                    json_tool = {
                        "type": "function",
                        "function": {
                            "name": json_schema_tool_name,
                            "description": fmt_description or "Generate JSON",
                            "parameters": fmt_schema,
                        },
                    }

                    # Add to tools list
                    if "tools" not in args:
                        args["tools"] = []
                    args["tools"].append(json_tool)

                    # Force the model to use this tool
                    args["tool_choice"] = {
                        "type": "function",
                        "function": {"name": json_schema_tool_name},
                    }
                elif fmt_type == "json_object":
                    args["response_format"] = {"type": "json_object"}

        # Tools
        if tools is not None:
            converted_tools = self._convert_tools(tools)
            if converted_tools:
                args["tools"] = converted_tools

        # Tool choice
        if tool_choice is not None:
            args["tool_choice"] = self._convert_tool_choice(tool_choice)

        # Parallel tool calls
        if parallel_tool_calls is not None:
            args["parallel_tool_calls"] = parallel_tool_calls

        # Native config overrides
        if nconfig:
            args.update(nconfig)

        return args, json_schema_mode, json_schema_tool_name

    def _convert_input_to_messages(
        self,
        input: str | list[dict[str, Any] | InputItem],
        instructions: str | None = None,
    ) -> list[dict[str, Any]]:
        """Convert unified input format to OpenAI messages format."""
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
                        "id": item.get("call_id") or item.get("id"),
                        "type": "function",
                        "function": {
                            "name": item.get("name"),
                            "arguments": (
                                json.dumps(item.get("arguments"))
                                if isinstance(item.get("arguments"), dict)
                                else item.get("arguments", "")
                            ),
                        },
                    }
                    messages.append(
                        {
                            "role": "assistant",
                            "content": None,
                            "tool_calls": [tool_call],
                        }
                    )
                elif item_type == "function_call_output":
                    # Tool result message
                    messages.append(
                        {
                            "role": "tool",
                            "tool_call_id": item.get("call_id"),
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
        """Convert a message item to OpenAI format."""
        role = item.get("role")
        content = item.get("content")

        if role == "system":
            if isinstance(content, str):
                return {"role": "system", "content": content}
            elif isinstance(content, list):
                # Extract text from content list
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
                parts = []
                for c in content:
                    if isinstance(c, dict):
                        c_type = c.get("type")
                        if c_type in ["input_text", "text"]:
                            parts.append(
                                {"type": "text", "text": c.get("text", "")}
                            )
                        elif c_type == "input_image":
                            # Handle image content
                            image_part = self._convert_image_content(c)
                            if image_part:
                                parts.append(image_part)
                        elif c_type == "input_file":
                            # Handle file content (PDF, etc.)
                            file_part = self._convert_file_content(c)
                            if file_part:
                                parts.append(file_part)
                    elif isinstance(c, str):
                        parts.append({"type": "text", "text": c})
                if len(parts) == 1 and parts[0]["type"] == "text":
                    return {"role": "user", "content": parts[0]["text"]}
                elif parts:
                    return {"role": "user", "content": parts}
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
                                    "id": c.get("call_id") or c.get("id"),
                                    "type": "function",
                                    "function": {
                                        "name": c.get("name"),
                                        "arguments": (
                                            json.dumps(c.get("arguments"))
                                            if isinstance(
                                                c.get("arguments"), dict
                                            )
                                            else c.get("arguments", "")
                                        ),
                                    },
                                }
                            )
                result: dict[str, Any] = {"role": "assistant"}
                if text_parts:
                    result["content"] = " ".join(text_parts)
                else:
                    result["content"] = None
                if tool_calls:
                    result["tool_calls"] = tool_calls
                return result
            return None

        return None

    def _convert_image_content(
        self, content: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert input_image content to OpenAI format."""
        image = content.get("image")
        if not isinstance(image, dict):
            return None

        img_content = image.get("content")
        source = image.get("source")
        media_type = image.get("media_type", "image/jpeg")
        detail = content.get("detail", "auto")

        if isinstance(img_content, (bytes, bytearray)):
            # Convert bytes to base64 data URL
            b64_data = base64.b64encode(img_content).decode("utf-8")
            return {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{media_type};base64,{b64_data}",
                    "detail": detail,
                },
            }
        elif isinstance(img_content, str):
            # Already base64 string
            return {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{media_type};base64,{img_content}",
                    "detail": detail,
                },
            }
        elif isinstance(source, str):
            # URL
            return {
                "type": "image_url",
                "image_url": {
                    "url": source,
                    "detail": detail,
                },
            }

        return None

    def _convert_file_content(
        self, content: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert input_file content to OpenAI format (for PDFs, etc.)."""
        file = content.get("file")
        if not isinstance(file, dict):
            return None

        file_content = file.get("content")
        filename = file.get("filename", "document.pdf")

        # Determine media type from filename
        if filename.lower().endswith(".pdf"):
            media_type = "application/pdf"
        else:
            media_type = "application/octet-stream"

        if isinstance(file_content, (bytes, bytearray)):
            # Convert bytes to base64 data URL
            b64_data = base64.b64encode(file_content).decode("utf-8")
            # For Chat Completions API, PDFs need to be sent as images
            # This works with models that support document understanding
            return {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{media_type};base64,{b64_data}",
                },
            }
        elif isinstance(file_content, str):
            # Already base64 string
            return {
                "type": "image_url",
                "image_url": {
                    "url": f"data:{media_type};base64,{file_content}",
                },
            }

        return None

    def _convert_tools(
        self, tools: list[dict | Tool]
    ) -> list[dict[str, Any]] | None:
        """Convert unified tools to OpenAI format."""
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
                    # Chat Completions API doesn't have built-in web search
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

    def _convert_tool_choice(
        self, tool_choice: dict | ToolChoice | str
    ) -> str | dict:
        """Convert unified tool_choice to OpenAI format."""
        if isinstance(tool_choice, str):
            # "auto", "none", "required"
            return tool_choice
        if isinstance(tool_choice, dict):
            tc_type = tool_choice.get("type")
            if tc_type in ["auto", "none", "required"]:
                return tc_type
            if tc_type == "function":
                func = tool_choice.get("function", {})
                return {
                    "type": "function",
                    "function": {"name": func.get("name")},
                }
        if hasattr(tool_choice, "type"):
            if tool_choice.type in ["auto", "none", "required"]:
                return tool_choice.type
            if tool_choice.type == "function" and hasattr(
                tool_choice, "function"
            ):
                return {
                    "type": "function",
                    "function": {"name": tool_choice.function.name},
                }
        return "auto"

    def _convert_result(
        self,
        response: Any,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Convert OpenAI response to unified format."""
        output: list[OutputItem] = []

        choice = response.choices[0] if response.choices else None
        if choice:
            message = choice.message

            # Check for reasoning content (models that support it)
            if (
                hasattr(message, "reasoning_content")
                and message.reasoning_content
            ):
                reasoning_output = OutputReasoning(
                    id=f"reasoning_{response.id}",
                    content=[
                        OutputReasoningContentText(
                            type="reasoning_text",
                            text=message.reasoning_content,
                        )
                    ],
                    summary=None,
                    status="completed",
                )
                output.append(reasoning_output)

            # Check for tool calls
            if message.tool_calls:
                for tc in message.tool_calls:
                    try:
                        arguments = json.loads(tc.function.arguments)
                    except (json.JSONDecodeError, TypeError):
                        arguments = {}

                    # Check if this is a JSON schema response
                    if (
                        json_schema_mode
                        and tc.function.name == json_schema_tool_name
                    ):
                        # Convert to text output
                        output.append(
                            OutputMessage(
                                id=f"msg_{response.id}",
                                role="assistant",
                                content=[
                                    OutputText(
                                        text=tc.function.arguments,
                                        annotations=None,
                                    )
                                ],
                                status="completed",
                            )
                        )
                    else:
                        output.append(
                            FunctionCall(
                                call_id=tc.id,
                                name=tc.function.name,
                                id=tc.id,
                                arguments=arguments,
                                status=None,
                            )
                        )

            # Add text content if present
            if message.content and not (
                json_schema_mode and message.tool_calls
            ):
                output.append(
                    OutputMessage(
                        id=f"msg_{response.id}",
                        role="assistant",
                        content=[
                            OutputText(text=message.content, annotations=None)
                        ],
                        status="completed",
                    )
                )

        # Build usage
        usage = None
        if response.usage:
            output_tokens_details = None
            reasoning_tokens = getattr(
                response.usage, "reasoning_tokens", None
            )
            if reasoning_tokens is not None:
                output_tokens_details = {"reasoning_tokens": reasoning_tokens}
            usage = Usage(
                input_tokens=response.usage.prompt_tokens,
                output_tokens=response.usage.completion_tokens,
                total_tokens=response.usage.total_tokens,
                output_tokens_details=output_tokens_details,
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
        if choice and choice.finish_reason == "length":
            result_status = "incomplete"

        return TextGenerationResult(
            id=response.id,
            model=response.model,
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
    ) -> Iterator[TextGenerationStreamEvent]:
        """Convert a streaming chunk to events."""
        if not chunk.choices:
            return

        choice = chunk.choices[0]
        delta = choice.delta

        # Check for reasoning content (streaming)
        if hasattr(delta, "reasoning_content") and delta.reasoning_content:
            yield ResponseReasoningTextDeltaEvent(
                sequence_number=None,
                delta=delta.reasoning_content,
                item_id=f"reasoning_{chunk.id}",
                output_index=0,
                content_index=0,
            )

        # Text content
        if delta.content:
            yield ResponseOutputTextDeltaEvent(
                sequence_number=None,
                delta=delta.content,
                item_id=f"msg_{chunk.id}",
                output_index=0,
                content_index=0,
            )

        # Tool calls
        if delta.tool_calls:
            for tc in delta.tool_calls:
                idx = tc.index
                if idx not in accumulated_tool_calls:
                    accumulated_tool_calls[idx] = {
                        "id": tc.id or f"call_{idx}",
                        "name": tc.function.name if tc.function else "",
                        "arguments": "",
                    }
                if tc.id:
                    accumulated_tool_calls[idx]["id"] = tc.id
                if tc.function:
                    if tc.function.name:
                        accumulated_tool_calls[idx]["name"] = tc.function.name
                    if tc.function.arguments:
                        yield ResponseFunctionCallArgumentsDeltaEvent(
                            sequence_number=None,
                            delta=tc.function.arguments,
                            item_id=accumulated_tool_calls[idx]["id"],
                            output_index=idx,
                        )

    def _build_final_result(
        self,
        model: str,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Build final result from accumulated streaming data."""
        output: list[OutputItem] = []
        response_id = str(uuid.uuid4())

        # Add reasoning if present
        if accumulated_reasoning:
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

            if json_schema_mode and tc["name"] == json_schema_tool_name:
                output.append(
                    OutputMessage(
                        id=f"msg_{response_id}",
                        role="assistant",
                        content=[
                            OutputText(text=tc["arguments"], annotations=None)
                        ],
                        status="completed",
                    )
                )
            else:
                output.append(
                    FunctionCall(
                        call_id=tc["id"],
                        name=tc["name"],
                        id=tc["id"],
                        arguments=arguments,
                        status=None,
                    )
                )

        # Add text if present and not already added via JSON schema
        if accumulated_text and not (
            json_schema_mode and accumulated_tool_calls
        ):
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
