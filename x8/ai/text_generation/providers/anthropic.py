import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

import anthropic

from x8.core import Response
from x8.core._provider import Provider
from x8.core.exceptions import BadRequestError

from .._models import (
    AllowedTools,
    ErrorDetail,
    FunctionCall,
    InputItem,
    OutputItem,
    OutputMessage,
    OutputMessageContent,
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
    ToolChoiceFunction,
    Usage,
)


class Anthropic(Provider):
    api_key: str | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: anthropic.Anthropic
    _async_client: anthropic.AsyncAnthropic
    _init: bool

    def __init__(
        self,
        api_key: str | None = None,
        model: str = "claude-sonnet-4-20250514",
        max_tokens: int = 16384,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        self.api_key = api_key
        self.model = model
        self.max_tokens = max_tokens
        self.nparams = nparams
        self._init = False
        super().__init__(**kwargs)

    def __setup__(self, context=None):
        if self._init:
            return
        self._client = anthropic.Anthropic(
            api_key=self.api_key,
            **(self.nparams or {}),
        )
        self._async_client = anthropic.AsyncAnthropic(
            api_key=self.api_key,
            **(self.nparams or {}),
        )
        self._init = True

    async def __asetup__(self, context=None):
        return self.__setup__(context)

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
        args = self._convert_generate_args(
            input,
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
        # Check if we're using JSON schema mode (tool-based)
        json_schema_mode = args.pop("_json_schema_mode", False)
        json_schema_tool_name = args.pop("_json_schema_tool_name", None)
        try:
            if not stream:
                response = self._client.messages.create(**args)
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
                    with self._client.messages.stream(**args) as stream_ctx:
                        for event in stream_ctx:
                            converted_events = self._convert_stream_event(
                                event,
                                json_schema_mode=json_schema_mode,
                            )
                            for converted_event in converted_events:
                                yield Response(result=converted_event)
                        # Final completed event with the full message
                        final_message = stream_ctx.get_final_message()
                        if final_message:
                            yield Response(
                                result=ResponseCompletedEvent(
                                    response=self._convert_result(
                                        final_message,
                                        json_schema_mode=json_schema_mode,
                                        json_schema_tool_name=json_schema_tool_name,  # noqa
                                    )
                                )
                            )

                return _stream_iter()
        except anthropic.BadRequestError as e:
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
        args = self._convert_generate_args(
            input,
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
        # Check if we're using JSON schema mode (tool-based)
        json_schema_mode = args.pop("_json_schema_mode", False)
        json_schema_tool_name = args.pop("_json_schema_tool_name", None)
        try:
            if not stream:
                response = await self._async_client.messages.create(**args)
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
                    async with self._async_client.messages.stream(
                        **args
                    ) as stream_ctx:
                        async for event in stream_ctx:
                            converted_events = self._convert_stream_event(
                                event,
                                json_schema_mode=json_schema_mode,
                            )
                            for converted_event in converted_events:
                                yield Response(result=converted_event)
                        # Final completed event with the full message
                        final_message = await stream_ctx.get_final_message()
                        if final_message:
                            yield Response(
                                result=ResponseCompletedEvent(
                                    response=self._convert_result(
                                        final_message,
                                        json_schema_mode=json_schema_mode,
                                        json_schema_tool_name=json_schema_tool_name,  # noqa
                                    )
                                )
                            )

                return _poll_aiter()
        except anthropic.BadRequestError as e:
            raise BadRequestError(str(e.message)) from e

    def _convert_stream_event(
        self,
        event: Any,
        json_schema_mode: bool = False,
    ) -> list[TextGenerationStreamEvent]:
        """Convert Anthropic streaming events to unified format."""
        results: list[TextGenerationStreamEvent] = []

        event_type = getattr(event, "type", None)

        # Text delta events
        if event_type == "content_block_delta":
            delta = getattr(event, "delta", None)
            if delta:
                delta_type = getattr(delta, "type", None)
                if delta_type == "text_delta":
                    text = getattr(delta, "text", "")
                    if text:
                        results.append(
                            ResponseOutputTextDeltaEvent(delta=text)
                        )
                elif delta_type == "thinking_delta":
                    thinking = getattr(delta, "thinking", "")
                    if thinking:
                        results.append(
                            ResponseReasoningTextDeltaEvent(delta=thinking)
                        )
                elif delta_type == "input_json_delta":
                    # Tool call arguments delta
                    partial_json = getattr(delta, "partial_json", "")
                    if partial_json:
                        # In JSON schema mode, emit as text delta
                        if json_schema_mode:
                            results.append(
                                ResponseOutputTextDeltaEvent(
                                    delta=partial_json
                                )
                            )
                        else:
                            results.append(
                                ResponseFunctionCallArgumentsDeltaEvent(
                                    delta=partial_json
                                )
                            )

        # Message complete event - handled via get_final_message instead
        # to get the complete message with usage info

        return results

    def _convert_result(
        self,
        response: Any,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Convert Anthropic response to unified TextGenerationResult."""
        r: dict[str, Any] = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response
        )

        output_items: list[OutputItem] = []
        message_content: list[OutputMessageContent] = []
        reasoning_content: list[OutputReasoningContentText] = []
        function_calls: list[FunctionCall] = []

        # Process content blocks
        content = r.get("content", [])
        if isinstance(content, list):
            for block in content:
                if not isinstance(block, dict):
                    continue

                block_type = block.get("type")

                if block_type == "text":
                    text = block.get("text", "")
                    if text:
                        message_content.append(OutputText(text=text))

                elif block_type == "thinking":
                    thinking_text = block.get("thinking", "")
                    if thinking_text:
                        reasoning_content.append(
                            OutputReasoningContentText(text=thinking_text)
                        )

                elif block_type == "tool_use":
                    tool_name = block.get("name", "")
                    tool_input = block.get("input")

                    # Check if this is our JSON schema tool
                    if (
                        json_schema_mode
                        and json_schema_tool_name
                        and tool_name == json_schema_tool_name
                    ):
                        # Convert tool output to text message with JSON
                        json_text = json.dumps(tool_input)
                        message_content.append(OutputText(text=json_text))
                    else:
                        function_calls.append(
                            FunctionCall(
                                name=tool_name,
                                arguments=tool_input,
                                call_id=block.get("id"),
                                id=block.get("id"),
                            )
                        )

        # Add reasoning as separate output item if present
        if reasoning_content:
            output_items.append(
                OutputReasoning(
                    content=reasoning_content,
                    status="completed",
                )
            )

        # Add assistant message if any text content collected
        if message_content:
            output_items.append(
                OutputMessage(
                    role="assistant",
                    content=message_content,
                    status="completed",
                )
            )

        # Add function calls as separate output items
        for fc in function_calls:
            output_items.append(fc)

        # Usage mapping
        usage_obj: Usage | None = None
        usage = r.get("usage")
        if isinstance(usage, dict):
            input_tokens = int(usage.get("input_tokens") or 0)
            output_tokens = int(usage.get("output_tokens") or 0)
            cache_read = int(usage.get("cache_read_input_tokens") or 0)
            cache_creation = int(usage.get("cache_creation_input_tokens") or 0)
            total_tokens = input_tokens + output_tokens

            input_details: dict[str, int] | None = None
            if cache_read or cache_creation:
                input_details = {}
                if cache_read:
                    input_details["cache_read_tokens"] = cache_read
                if cache_creation:
                    input_details["cache_creation_tokens"] = cache_creation

            usage_obj = Usage(
                input_tokens=input_tokens,
                input_tokens_details=input_details,
                output_tokens=output_tokens,
                total_tokens=total_tokens,
            )

        # Error handling
        error_obj: ErrorDetail | None = None

        # Determine status based on stop_reason
        status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        stop_reason = r.get("stop_reason")
        if stop_reason == "max_tokens":
            status = "incomplete"
        elif stop_reason == "end_turn":
            status = "completed"
        elif stop_reason == "tool_use":
            status = "completed"
        elif stop_reason == "stop_sequence":
            status = "completed"

        result = TextGenerationResult(
            id=r.get("id"),
            model=r.get("model"),
            created_at=None,  # Anthropic doesn't provide creation time
            status=status,
            error=error_obj,
            output=output_items or None,
            usage=usage_obj,
        )
        return result

    def _convert_generate_args(
        self,
        input: str | list[dict[str, Any] | InputItem],
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
    ) -> dict[str, Any]:
        args: dict[str, Any] = {}
        args["model"] = model or self.model
        args["max_tokens"] = max_output_tokens or self.max_tokens

        messages: list[dict[str, Any]] = []

        def _convert_content_part(c: dict[str, Any]) -> dict[str, Any] | None:
            """Convert a single content part to Anthropic format."""
            ct = c.get("type")
            if ct == "input_text":
                return {"type": "text", "text": c.get("text")}
            if ct == "output_text":
                return {"type": "text", "text": c.get("text")}
            if ct == "input_image":
                img = c.get("image")
                if not isinstance(img, dict):
                    return None
                media_type = img.get("media_type") or "image/jpeg"
                content = img.get("content")
                source = img.get("source")
                if isinstance(content, (bytes, bytearray)):
                    b64_data = base64.b64encode(content).decode("utf-8")
                    return {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64_data,
                        },
                    }
                elif isinstance(content, str):
                    # Already base64 encoded
                    return {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": content,
                        },
                    }
                if isinstance(source, str):
                    # URL source
                    return {
                        "type": "image",
                        "source": {
                            "type": "url",
                            "url": source,
                        },
                    }
            if ct == "input_file":
                f = c.get("file")
                if isinstance(f, dict):
                    content = f.get("content")
                    mime_type = f.get("media_type") or "application/pdf"
                    if isinstance(content, (bytes, bytearray)):
                        b64_data = base64.b64encode(content).decode("utf-8")
                        return {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": b64_data,
                            },
                        }
                    elif isinstance(content, str):
                        # Already base64 encoded
                        return {
                            "type": "document",
                            "source": {
                                "type": "base64",
                                "media_type": mime_type,
                                "data": content,
                            },
                        }
                    source = f.get("source")
                    if isinstance(source, str):
                        return {
                            "type": "document",
                            "source": {
                                "type": "url",
                                "url": source,
                            },
                        }
            return None

        def _convert_content_to_parts(
            raw_content: str | list | None,
        ) -> list[dict[str, Any]]:
            """Convert message content to Anthropic format."""
            if isinstance(raw_content, str):
                return [{"type": "text", "text": raw_content}]
            if isinstance(raw_content, list):
                parts = []
                for c in raw_content:
                    if isinstance(c, dict):
                        part = _convert_content_part(c)
                        if part:
                            parts.append(part)
                return parts
            return []

        def _convert_input_item(msg: InputItem | dict[str, Any]) -> None:
            m: dict[str, Any] = msg if isinstance(msg, dict) else msg.to_dict()
            item_type = m.get("type")

            if item_type == "function_call":
                # Anthropic represents function calls as tool_use blocks
                # in assistant messages
                tool_use_block = {
                    "type": "tool_use",
                    "id": m.get("call_id") or m.get("id"),
                    "name": m.get("name"),
                    "input": m.get("arguments") or {},
                }
                messages.append(
                    {
                        "role": "assistant",
                        "content": [tool_use_block],
                    }
                )
                return

            if item_type == "function_call_output":
                # Tool results go in user messages with tool_result blocks
                output = m.get("output")
                if isinstance(output, dict):
                    output = json.dumps(output)
                messages.append(
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": m.get("call_id"),
                                "content": str(output) if output else "",
                            }
                        ],
                    }
                )
                return

            # Handle OutputReasoning (skip)
            if item_type == "reasoning":
                return

            if item_type != "message":
                return

            role = m.get("role")
            # Map roles for Anthropic
            if role == "system" or role == "developer":
                # System messages handled via system parameter
                content = m.get("content")
                if isinstance(content, str):
                    if "system" not in args:
                        args["system"] = content
                    else:
                        args["system"] += "\n" + content
                return
            elif role == "assistant":
                a_role = "assistant"
            else:
                a_role = "user"

            parts = _convert_content_to_parts(m.get("content"))
            if parts:
                messages.append({"role": a_role, "content": parts})

        # Input may be a raw string or a sequence of InputItems.
        if isinstance(input, str):
            messages.append(
                {"role": "user", "content": [{"type": "text", "text": input}]}
            )
        else:
            for it in input:
                _convert_input_item(it)

        args["messages"] = messages

        if temperature is not None:
            args["temperature"] = temperature
        if top_p is not None:
            args["top_p"] = top_p
        if instructions:
            args["system"] = instructions

        # Handle JSON output format via tool-based approach
        json_schema_tool_name: str | None = None
        if text:
            text_dict = text if isinstance(text, dict) else text.to_dict()
            fmt = text_dict.get("format")
            if isinstance(fmt, dict) and fmt.get("type") == "json_schema":
                # Create a tool with the JSON schema to force structured output
                json_schema_tool_name = fmt.get("name", "json_output")
                schema = fmt.get(
                    "schema", {"type": "object", "properties": {}}
                )
                description = fmt.get(
                    "description", "Generate structured JSON output"
                )

                # Create the tool
                json_tool = {
                    "name": json_schema_tool_name,
                    "description": description,
                    "input_schema": schema,
                }

                # Add to existing tools or create new tools list
                if "tools" not in args:
                    args["tools"] = []
                args["tools"].append(json_tool)

                # Force the model to use this tool
                args["tool_choice"] = {
                    "type": "tool",
                    "name": json_schema_tool_name,
                }

                # Set markers for result conversion
                args["_json_schema_mode"] = True
                args["_json_schema_tool_name"] = json_schema_tool_name

        # Handle tools
        if tools:
            args["tools"] = self._convert_tools(tools)
        if tool_choice:
            args["tool_choice"] = self._convert_tool_choice(tool_choice)

        # Handle reasoning/thinking
        if reasoning:
            if isinstance(reasoning, dict):
                effort = reasoning.get("effort", "none")
            elif isinstance(reasoning, Reasoning):
                effort = reasoning.effort
            else:
                effort = "none"

            if effort and effort != "none":
                # Enable extended thinking
                budget_map = {
                    "low": 5000,
                    "medium": 10000,
                    "high": 20000,
                }
                budget = budget_map.get(effort, 5000)
                args["thinking"] = {
                    "type": "enabled",
                    "budget_tokens": budget,
                }

        if nconfig:
            args.update(nconfig)

        return args

    def _convert_tools(self, tools: list[dict | Tool]) -> list[dict[str, Any]]:
        """Convert tools to Anthropic format."""
        anthropic_tools: list[dict[str, Any]] = []
        for t in tools:
            if isinstance(t, dict):
                tool_type = t.get("type", "function")
                tool = t
            else:
                tool_type = t.type
                tool = t.to_dict()

            if tool_type == "function":
                anthropic_tools.append(
                    {
                        "name": tool.get("name"),
                        "description": tool.get("description"),
                        "input_schema": tool.get("parameters")
                        or {
                            "type": "object",
                            "properties": {},
                        },
                    }
                )
            elif tool_type == "web_search":
                # Anthropic's web search tool
                web_search_tool: dict[str, Any] = {
                    "type": "web_search_20250305",
                    "name": "web_search",
                }
                # Optional: map search_context_size to max_uses
                search_context_size = tool.get("search_context_size", "medium")
                max_uses_map = {
                    "short": 1,
                    "medium": 3,
                    "long": 5,
                }
                if search_context_size in max_uses_map:
                    web_search_tool["max_uses"] = max_uses_map[
                        search_context_size
                    ]
                # Optional: allowed_domains filter
                filters = tool.get("filters")
                if isinstance(filters, dict):
                    allowed_domains = filters.get("allowed_domains")
                    if allowed_domains:
                        web_search_tool["allowed_domains"] = allowed_domains
                # Optional: user_location
                user_location = tool.get("user_location")
                if isinstance(user_location, dict):
                    web_search_tool["user_location"] = {
                        "type": "approximate",
                        "city": user_location.get("city"),
                        "region": user_location.get("region"),
                        "country": user_location.get("country"),
                        "timezone": user_location.get("timezone"),
                    }
                anthropic_tools.append(web_search_tool)
            else:
                raise BadRequestError(f"Unsupported tool type: {tool_type}")

        return anthropic_tools

    def _convert_tool_choice(
        self,
        choice: dict | ToolChoice | None,
    ) -> dict[str, Any] | None:
        """Convert tool_choice to Anthropic format."""
        if choice is None:
            return None

        if isinstance(choice, (ToolChoiceFunction, AllowedTools)):
            inp = choice.to_dict()
        else:
            inp = choice

        if inp == "none":
            # Anthropic doesn't have a "none" mode - omit tools instead
            return None
        if inp == "auto":
            return {"type": "auto"}
        if inp == "required":
            return {"type": "any"}

        if isinstance(inp, dict):
            if inp.get("type") == "function":
                name = inp.get("name")
                if not name:
                    raise BadRequestError(
                        "Function tool_choice must specify a name"
                    )
                return {"type": "tool", "name": name}
            elif inp.get("type") == "allowed_tools":
                mode = inp.get("mode")
                if mode == "none":
                    return None
                if mode == "auto":
                    return {"type": "auto"}
                if mode == "required":
                    return {"type": "any"}

        raise BadRequestError(f"Unsupported tool_choice: {type(choice)}")
