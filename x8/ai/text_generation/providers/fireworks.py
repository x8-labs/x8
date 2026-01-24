import base64
import json
import sys
import types
from typing import Any, AsyncIterator, Iterator

# Block fireworks __init__.py which has problematic protobuf imports
# that conflict with google-cloud-aiplatform's protobuf definitions.
# We only need the client submodule for the API.
if "fireworks" not in sys.modules:
    # Find the fireworks package path from site-packages
    import site

    _fw_path = None
    for _site_path in site.getsitepackages() + [site.getusersitepackages()]:
        _candidate = f"{_site_path}/fireworks"
        try:
            import os

            if os.path.isdir(_candidate):
                _fw_path = _candidate
                break
        except Exception:
            continue

    if _fw_path:
        _fw_module = types.ModuleType("fireworks")
        _fw_module.__path__ = [_fw_path]  # type: ignore[attr-defined]
        sys.modules["fireworks"] = _fw_module

from fireworks.client import error as fireworks_error
from fireworks.client.api_client_v2 import AsyncFireworks
from fireworks.client.api_client_v2 import Fireworks as FireworksClient

from x8.core import Response
from x8.core._provider import Provider
from x8.core.exceptions import BadRequestError

from .._models import (
    FunctionCall,
    InputItem,
    OutputItem,
    OutputMessage,
    OutputText,
    Reasoning,
    ResponseCompletedEvent,
    ResponseFunctionCallArgumentsDeltaEvent,
    ResponseOutputTextDeltaEvent,
    ResponseText,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
    Usage,
)


class Fireworks(Provider):
    """Fireworks AI provider using the native Fireworks SDK.

    Fireworks AI provides fast inference for open-source models including
    Llama, Qwen, Mistral, DeepSeek, and more.
    """

    api_key: str | None
    base_url: str | None
    timeout: int | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: FireworksClient
    _async_client: AsyncFireworks
    _init: bool
    _ainit: bool

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: int | None = None,
        model: str = "accounts/fireworks/models/llama-v3p3-70b-instruct",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                Fireworks AI API key.
            base_url:
                Fireworks AI API base url (optional).
            timeout:
                Timeout for client requests in seconds.
            model:
                Fireworks AI model to use for text generation.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for Fireworks client.
        """
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
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
            client_kwargs["api_key"] = self.api_key
        if self.base_url is not None:
            client_kwargs["base_url"] = self.base_url
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = FireworksClient(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        client_kwargs: dict[str, Any] = {}
        if self.api_key is not None:
            client_kwargs["api_key"] = self.api_key
        if self.base_url is not None:
            client_kwargs["base_url"] = self.base_url
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._async_client = AsyncFireworks(**client_kwargs)
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
        except fireworks_error.InvalidRequestError as e:
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
                response = await self._async_client.chat.completions.acreate(
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
                    # acreate returns async generator directly, not coroutine
                    response = self._async_client.chat.completions.acreate(
                        **args
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
        except fireworks_error.InvalidRequestError as e:
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
    ) -> tuple[dict[str, Any], bool, str | None]:
        """Convert unified args to Fireworks Chat Completions format."""
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

        # Stream - must explicitly set stream=False for non-streaming
        # because Fireworks SDK's acreate defaults to stream=True
        args["stream"] = bool(stream)

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
                    fmt_type = getattr(fmt, "type", None)
                    fmt_name = getattr(fmt, "name", "json_output")
                    fmt_schema = getattr(
                        fmt, "schema_", {"type": "object", "properties": {}}
                    )
                    fmt_description = getattr(
                        fmt,
                        "description",
                        "Generate structured JSON output matching schema",
                    )

                if fmt_type == "json_schema":
                    json_schema_mode = True
                    json_schema_tool_name = fmt_name

                    json_tool = {
                        "type": "function",
                        "function": {
                            "name": json_schema_tool_name,
                            "description": fmt_description or "Generate JSON",
                            "parameters": fmt_schema,
                        },
                    }

                    if "tools" not in args:
                        args["tools"] = []
                    args["tools"].append(json_tool)

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

        # Native config overrides
        if nconfig:
            args.update(nconfig)

        return args, json_schema_mode, json_schema_tool_name

    def _convert_input_to_messages(
        self,
        input: str | list[dict[str, Any] | InputItem],
        instructions: str | None = None,
    ) -> list[dict[str, Any]]:
        """Convert unified input format to Fireworks messages format."""
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
        """Convert a message item to Fireworks format."""
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
                parts = []
                for c in content:
                    if isinstance(c, dict):
                        c_type = c.get("type")
                        if c_type in ["input_text", "text"]:
                            parts.append(
                                {"type": "text", "text": c.get("text", "")}
                            )
                        elif c_type == "input_image":
                            image_part = self._convert_image_content(c)
                            if image_part:
                                parts.append(image_part)
                    elif isinstance(c, str):
                        parts.append({"type": "text", "text": c})
                if len(parts) == 1 and parts[0]["type"] == "text":
                    return {"role": "user", "content": parts[0]["text"]}
                return {"role": "user", "content": parts}
            return None

        if role == "assistant":
            if isinstance(content, str):
                return {"role": "assistant", "content": content}
            elif isinstance(content, list):
                text_parts = []
                for c in content:
                    if isinstance(c, dict) and c.get("type") in [
                        "output_text",
                        "text",
                    ]:
                        text_parts.append(c.get("text", ""))
                    elif isinstance(c, str):
                        text_parts.append(c)
                return {"role": "assistant", "content": " ".join(text_parts)}
            return None

        return None

    def _convert_image_content(
        self, content: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert image content to Fireworks format."""
        image = content.get("image")
        if not image:
            return None

        # Handle URL
        if "url" in image:
            return {"type": "image_url", "image_url": {"url": image["url"]}}

        # Handle base64 content
        if "content" in image:
            image_data = image["content"]
            media_type = image.get("media_type", "image/jpeg")

            if isinstance(image_data, bytes):
                b64_data = base64.b64encode(image_data).decode("utf-8")
            else:
                b64_data = image_data

            data_url = f"data:{media_type};base64,{b64_data}"
            return {"type": "image_url", "image_url": {"url": data_url}}

        return None

    def _convert_tools(self, tools: list[dict | Tool]) -> list[dict[str, Any]]:
        """Convert tools to Fireworks format."""
        converted = []
        for tool in tools:
            if isinstance(tool, dict):
                tool_type = tool.get("type", "function")
                if tool_type == "function":
                    converted.append(
                        {
                            "type": "function",
                            "function": {
                                "name": tool.get("name"),
                                "description": tool.get("description", ""),
                                "parameters": tool.get(
                                    "parameters", {"type": "object"}
                                ),
                            },
                        }
                    )
            elif hasattr(tool, "type"):
                if tool.type == "function":
                    converted.append(
                        {
                            "type": "function",
                            "function": {
                                "name": tool.name,
                                "description": tool.description or "",
                                "parameters": (
                                    tool.parameters
                                    if tool.parameters
                                    else {"type": "object"}
                                ),
                            },
                        }
                    )
        return converted

    def _convert_tool_choice(
        self, tool_choice: dict | ToolChoice
    ) -> dict[str, Any] | str:
        """Convert tool choice to Fireworks format."""
        if isinstance(tool_choice, str):
            return tool_choice

        if isinstance(tool_choice, dict):
            tc_type = tool_choice.get("type")
            if tc_type in ["auto", "none", "required"]:
                return tc_type
            elif tc_type == "function":
                func = tool_choice.get("function", {})
                return {
                    "type": "function",
                    "function": {"name": func.get("name")},
                }
        elif hasattr(tool_choice, "type"):
            if tool_choice.type in ["auto", "none", "required"]:
                return tool_choice.type
            elif tool_choice.type == "function":
                func = tool_choice.function
                if func:
                    return {
                        "type": "function",
                        "function": {"name": func.name},
                    }

        return "auto"

    def _convert_result(
        self,
        response: Any,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Convert Fireworks response to TextGenerationResult."""
        output: list[OutputItem] = []

        if response.choices:
            choice = response.choices[0]
            message = choice.message

            # Check for tool calls
            if message.tool_calls:
                for tool_call in message.tool_calls:
                    func = tool_call.function

                    # Get arguments - can be str or dict in Fireworks
                    if isinstance(func.arguments, str):
                        try:
                            args = json.loads(func.arguments)
                        except (json.JSONDecodeError, TypeError):
                            args = func.arguments
                    else:
                        args = func.arguments

                    # Handle JSON schema mode
                    if json_schema_mode and func.name == json_schema_tool_name:
                        text_content = (
                            func.arguments
                            if isinstance(func.arguments, str)
                            else json.dumps(args)
                        )
                        output.append(
                            OutputMessage(
                                type="message",
                                role="assistant",
                                status="completed",
                                content=[
                                    OutputText(
                                        type="output_text", text=text_content
                                    )
                                ],
                                id=f"msg_{response.id}",
                            )
                        )
                    else:
                        output.append(
                            FunctionCall(
                                type="function_call",
                                call_id=tool_call.id,
                                name=func.name,
                                arguments=args,
                                id=tool_call.id,
                                status=None,
                            )
                        )
            elif message.content:
                output.append(
                    OutputMessage(
                        type="message",
                        role="assistant",
                        status="completed",
                        content=[
                            OutputText(
                                type="output_text", text=message.content
                            )
                        ],
                        id=f"msg_{response.id}",
                    )
                )

        # Extract usage
        usage = None
        if response.usage:
            usage = Usage(
                input_tokens=response.usage.prompt_tokens,
                output_tokens=response.usage.completion_tokens or 0,
                total_tokens=response.usage.total_tokens,
            )

        # Determine status
        status = "completed"
        if response.choices:
            finish_reason = response.choices[0].finish_reason
            if finish_reason == "length":
                status = "incomplete"

        return TextGenerationResult(
            id=response.id,
            model=response.model,
            created_at=None,
            status=status,
            error=None,
            output=output,
            usage=usage,
        )

    def _convert_stream_chunk(
        self,
        chunk: Any,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
    ) -> list[TextGenerationStreamEvent]:
        """Convert a stream chunk to events."""
        events: list[TextGenerationStreamEvent] = []

        if not chunk.choices:
            return events

        choice = chunk.choices[0]
        delta = choice.delta

        # Handle text content
        if delta.content:
            events.append(
                ResponseOutputTextDeltaEvent(
                    type="output_text_delta",
                    content_index=0,
                    output_index=0,
                    delta=delta.content,
                    sequence_number=None,
                    item_id=None,
                )
            )

        # Handle tool calls
        if delta.tool_calls:
            for tc in delta.tool_calls:
                idx = tc.index
                func = tc.function

                # Get function as dict if needed
                if hasattr(func, "model_dump"):
                    func_dict = func.model_dump()
                elif hasattr(func, "dict"):
                    func_dict = func.dict()
                elif isinstance(func, dict):
                    func_dict = func
                elif isinstance(func, str):
                    # Sometimes function is just a string (arguments chunk)
                    func_dict = {"arguments": func}
                else:
                    func_dict = {
                        "name": getattr(func, "name", None),
                        "arguments": getattr(func, "arguments", None),
                    }

                # Track tool name
                if func_dict.get("name"):
                    if idx not in accumulated_tool_calls:
                        accumulated_tool_calls[idx] = {
                            "id": tc.id or f"call_{idx}",
                            "name": func_dict.get("name"),
                            "arguments": "",
                        }
                    else:
                        accumulated_tool_calls[idx]["name"] = func_dict.get(
                            "name"
                        )
                    if tc.id:
                        accumulated_tool_calls[idx]["id"] = tc.id

                # Handle arguments delta
                if func_dict.get("arguments"):
                    events.append(
                        ResponseFunctionCallArgumentsDeltaEvent(
                            type="function_call_arguments_delta",
                            output_index=idx,
                            delta=func_dict.get("arguments"),
                            sequence_number=None,
                            item_id=(
                                accumulated_tool_calls.get(idx, {}).get("id")
                            ),
                        )
                    )

        return events

    def _build_final_result(
        self,
        model: str,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Build final result from accumulated stream data."""
        output: list[OutputItem] = []

        # Handle JSON schema mode
        if json_schema_mode and accumulated_tool_calls:
            for idx, tc in sorted(accumulated_tool_calls.items()):
                if tc.get("name") == json_schema_tool_name:
                    output.append(
                        OutputMessage(
                            type="message",
                            role="assistant",
                            status="completed",
                            content=[
                                OutputText(
                                    type="output_text",
                                    text=tc.get("arguments", ""),
                                )
                            ],
                            id=tc.get("id"),
                        )
                    )
        elif accumulated_tool_calls:
            for idx, tc in sorted(accumulated_tool_calls.items()):
                try:
                    args = json.loads(tc.get("arguments", "{}"))
                except (json.JSONDecodeError, TypeError):
                    args = tc.get("arguments", {})
                output.append(
                    FunctionCall(
                        type="function_call",
                        call_id=tc.get("id"),
                        name=tc.get("name", ""),
                        arguments=args,
                        id=tc.get("id"),
                        status=None,
                    )
                )
        elif accumulated_text:
            output.append(
                OutputMessage(
                    type="message",
                    role="assistant",
                    status="completed",
                    content=[
                        OutputText(type="output_text", text=accumulated_text)
                    ],
                    id=None,
                )
            )

        return TextGenerationResult(
            id=None,
            model=model,
            created_at=None,
            status="completed",
            error=None,
            output=output,
            usage=None,
        )
