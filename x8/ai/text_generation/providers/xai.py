import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

import xai_sdk
from xai_sdk import chat as xai_chat
from xai_sdk.chat import SearchParameters

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


class Xai(Provider):
    api_key: str | None
    management_api_key: str | None
    api_host: str | None
    management_api_host: str | None
    metadata: tuple[tuple[str, str], ...] | None
    channel_options: list[tuple[str, Any]] | None
    timeout: float | None
    use_insecure_channel: bool | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: xai_sdk.Client
    _async_client: xai_sdk.AsyncClient
    _init: bool
    _ainit: bool

    def __init__(
        self,
        api_key: str | None = None,
        management_api_key: str | None = None,
        api_host: str | None = None,
        management_api_host: str | None = None,
        metadata: tuple[tuple[str, str], ...] | None = None,
        channel_options: list[tuple[str, Any]] | None = None,
        timeout: float | None = None,
        use_insecure_channel: bool | None = None,
        model: str = "grok-4",
        max_tokens: int = 16384,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                xAI API key.
            management_api_key:
                xAI management API key (for management operations).
            api_host:
                xAI API host for requests.
            management_api_host:
                xAI management API host for management operations.
            metadata:
                gRPC metadata to include in every request.
            channel_options:
                gRPC channel options for the client connection.
            timeout:
                Timeout for client requests.
            use_insecure_channel:
                Whether to use an insecure (non-TLS) channel.
            model:
                xAI model to use for text generation.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native params for xAI client.
        """
        self.api_key = api_key
        self.management_api_key = management_api_key
        self.api_host = api_host
        self.management_api_host = management_api_host
        self.metadata = metadata
        self.channel_options = channel_options
        self.timeout = timeout
        self.use_insecure_channel = use_insecure_channel
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
        if self.management_api_key is not None:
            client_kwargs["management_api_key"] = self.management_api_key
        if self.api_host is not None:
            client_kwargs["api_host"] = self.api_host
        if self.management_api_host is not None:
            client_kwargs["management_api_host"] = self.management_api_host
        if self.metadata is not None:
            client_kwargs["metadata"] = self.metadata
        if self.channel_options is not None:
            client_kwargs["channel_options"] = self.channel_options
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.use_insecure_channel is not None:
            client_kwargs["use_insecure_channel"] = self.use_insecure_channel
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = xai_sdk.Client(**client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        client_kwargs = {}
        if self.api_key is not None:
            client_kwargs["api_key"] = self.api_key
        if self.management_api_key is not None:
            client_kwargs["management_api_key"] = self.management_api_key
        if self.api_host is not None:
            client_kwargs["api_host"] = self.api_host
        if self.management_api_host is not None:
            client_kwargs["management_api_host"] = self.management_api_host
        if self.metadata is not None:
            client_kwargs["metadata"] = self.metadata
        if self.channel_options is not None:
            client_kwargs["channel_options"] = self.channel_options
        if self.timeout is not None:
            client_kwargs["timeout"] = self.timeout
        if self.use_insecure_channel is not None:
            client_kwargs["use_insecure_channel"] = self.use_insecure_channel
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._async_client = xai_sdk.AsyncClient(**client_kwargs)
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
        chat, json_schema_mode = self._create_chat(
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
            reasoning=reasoning,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            if not stream:
                response = chat.sample()
                result = self._convert_result(
                    response,
                    json_schema_mode=json_schema_mode,
                )
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    final_response = None
                    for response, chunk in chat.stream():
                        final_response = response
                        converted_events = self._convert_stream_chunk(
                            chunk,
                            json_schema_mode=json_schema_mode,
                        )
                        for converted_event in converted_events:
                            yield Response(result=converted_event)
                    # Final completed event with the full message
                    if final_response:
                        yield Response(
                            result=ResponseCompletedEvent(
                                response=self._convert_result(
                                    final_response,
                                    json_schema_mode=json_schema_mode,
                                )
                            )
                        )

                return _stream_iter()
        except Exception as e:
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
        chat, json_schema_mode = await self._acreate_chat(
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
            reasoning=reasoning,
            nconfig=nconfig,
            **kwargs,
        )
        try:
            if not stream:
                response = await chat.sample()
                result = self._convert_result(
                    response,
                    json_schema_mode=json_schema_mode,
                )
                return Response(result=result)
            else:

                async def _poll_aiter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    final_response = None
                    async for response, chunk in chat.stream():
                        final_response = response
                        converted_events = self._convert_stream_chunk(
                            chunk,
                            json_schema_mode=json_schema_mode,
                        )
                        for converted_event in converted_events:
                            yield Response(result=converted_event)
                    # Final completed event with the full message
                    if final_response:
                        yield Response(
                            result=ResponseCompletedEvent(
                                response=self._convert_result(
                                    final_response,
                                    json_schema_mode=json_schema_mode,
                                )
                            )
                        )

                return _poll_aiter()
        except Exception as e:
            raise BadRequestError(str(e)) from e

    def _convert_stream_chunk(
        self,
        chunk: Any,
        json_schema_mode: bool = False,
    ) -> list[TextGenerationStreamEvent]:
        """Convert xAI streaming chunk to unified format."""
        results: list[TextGenerationStreamEvent] = []

        # Text content
        content = getattr(chunk, "content", None)
        if content:
            results.append(ResponseOutputTextDeltaEvent(delta=content))

        # Reasoning content
        reasoning_content = getattr(chunk, "reasoning_content", None)
        if reasoning_content:
            results.append(
                ResponseReasoningTextDeltaEvent(delta=reasoning_content)
            )

        # Tool calls
        tool_calls = getattr(chunk, "tool_calls", None)
        if tool_calls and not json_schema_mode:
            for tc in tool_calls:
                func = getattr(tc, "function", None)
                if func:
                    args = getattr(func, "arguments", "")
                    if args:
                        results.append(
                            ResponseFunctionCallArgumentsDeltaEvent(delta=args)
                        )

        return results

    def _convert_result(
        self,
        response: Any,
        json_schema_mode: bool = False,
    ) -> TextGenerationResult:
        """Convert xAI response to unified TextGenerationResult."""
        output_items: list[OutputItem] = []
        message_content: list[OutputMessageContent] = []
        reasoning_content: list[OutputReasoningContentText] = []
        function_calls: list[FunctionCall] = []

        # Extract content
        content = getattr(response, "content", "")
        if content:
            message_content.append(OutputText(text=content))

        # Extract reasoning content
        reasoning_text = getattr(response, "reasoning_content", "")
        if reasoning_text:
            reasoning_content.append(
                OutputReasoningContentText(text=reasoning_text)
            )

        # Extract tool calls
        tool_calls = getattr(response, "tool_calls", [])
        if tool_calls:
            for tc in tool_calls:
                tc_id = getattr(tc, "id", None)
                func = getattr(tc, "function", None)
                if func:
                    name = getattr(func, "name", "")
                    arguments_str = getattr(func, "arguments", "")
                    # Parse arguments JSON
                    try:
                        arguments = (
                            json.loads(arguments_str) if arguments_str else {}
                        )
                    except json.JSONDecodeError:
                        arguments = {}

                    if json_schema_mode:
                        # Convert tool output to text message with JSON
                        json_text = json.dumps(arguments)
                        message_content.append(OutputText(text=json_text))
                    else:
                        function_calls.append(
                            FunctionCall(
                                name=name,
                                arguments=arguments,
                                call_id=tc_id,
                                id=tc_id,
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
        usage = getattr(response, "usage", None)
        if usage:
            input_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
            output_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
            total_tokens = int(
                getattr(usage, "total_tokens", 0)
                or input_tokens + output_tokens
            )
            reasoning_tokens = int(getattr(usage, "reasoning_tokens", 0) or 0)

            output_details: dict[str, int] | None = None
            if reasoning_tokens:
                output_details = {"reasoning_tokens": reasoning_tokens}

            usage_obj = Usage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                output_tokens_details=output_details,
                total_tokens=total_tokens,
            )

        # Error handling
        error_obj: ErrorDetail | None = None

        # Determine status based on finish_reason
        status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        finish_reason = getattr(response, "finish_reason", None)
        if (
            finish_reason == "FINISH_REASON_LENGTH"
            or finish_reason == "length"
        ):
            status = "incomplete"
        elif finish_reason in (
            "FINISH_REASON_STOP",
            "stop",
            "FINISH_REASON_END_TURN",
            "end_turn",
        ):
            status = "completed"
        elif finish_reason in ("FINISH_REASON_TOOL_CALLS", "tool_calls"):
            status = "completed"

        response_id = getattr(response, "id", None)

        result = TextGenerationResult(
            id=response_id,
            model=self.model,
            created_at=None,
            status=status,
            error=error_obj,
            output=output_items or None,
            usage=usage_obj,
        )
        return result

    def _create_chat(
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
        reasoning: dict | Reasoning | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[Any, bool]:
        """Create an xAI chat object."""
        create_kwargs: dict[str, Any] = {}
        create_kwargs["model"] = model or self.model
        create_kwargs["max_tokens"] = max_output_tokens or self.max_tokens

        messages: list[Any] = []

        def _convert_input_item(msg: InputItem | dict[str, Any]) -> None:
            m: dict[str, Any] = msg if isinstance(msg, dict) else msg.to_dict()
            item_type = m.get("type")

            if item_type == "function_call":
                # xAI represents function calls as
                # assistant messages with tool_calls
                # We need to create an assistant message
                # followed by tool result
                # For now, append as assistant response
                return

            if item_type == "function_call_output":
                # Tool results
                output = m.get("output")
                if isinstance(output, dict):
                    output = json.dumps(output)
                messages.append(
                    xai_chat.tool_result(str(output) if output else "")
                )
                return

            # Handle OutputReasoning (skip)
            if item_type == "reasoning":
                return

            if item_type != "message":
                return

            role = m.get("role")
            content = m.get("content")

            # Extract content parts for xAI
            content_parts: list[Any] = []
            if isinstance(content, str):
                content_parts.append(xai_chat.text(content))
            elif isinstance(content, list):
                for c in content:
                    if isinstance(c, dict):
                        ct = c.get("type")
                        if ct == "input_text" or ct == "output_text":
                            text_val = c.get("text", "")
                            if text_val:
                                content_parts.append(xai_chat.text(text_val))
                        elif ct == "input_image":
                            # xAI supports images via image() helper
                            # Accepts URL or base64-encoded string
                            img = c.get("image")
                            if isinstance(img, dict):
                                img_content = img.get("content")
                                source = img.get("source")
                                if img_content:
                                    # Convert bytes to base64 data URL
                                    if isinstance(
                                        img_content, (bytes, bytearray)
                                    ):
                                        b64_data = base64.b64encode(
                                            img_content
                                        ).decode("utf-8")
                                    else:
                                        # Assume already base64 encoded
                                        b64_data = img_content
                                    media_type = img.get(
                                        "media_type", "image/jpeg"
                                    )
                                    data_url = (
                                        f"data:{media_type};base64,{b64_data}"
                                    )
                                    content_parts.append(
                                        xai_chat.image(data_url)
                                    )
                                elif source:
                                    # URL source
                                    content_parts.append(
                                        xai_chat.image(source)
                                    )

            if not content_parts:
                return

            # Map roles and create appropriate messages
            if role == "system" or role == "developer":
                # System messages only accept text, not images
                text_only = [p for p in content_parts if hasattr(p, "text")]
                if text_only:
                    messages.append(xai_chat.system(*content_parts))
            elif role == "assistant":
                messages.append(xai_chat.assistant(*content_parts))
            else:
                messages.append(xai_chat.user(*content_parts))

        # Input may be a raw string or a sequence of InputItems.
        if isinstance(input, str):
            messages.append(xai_chat.user(input))
        else:
            for it in input:
                _convert_input_item(it)

        create_kwargs["messages"] = messages

        if temperature is not None:
            create_kwargs["temperature"] = temperature
        if top_p is not None:
            create_kwargs["top_p"] = top_p

        # Handle instructions as system message
        if instructions:
            # Prepend system message
            create_kwargs["messages"] = [
                xai_chat.system(instructions)
            ] + create_kwargs["messages"]

        # Handle JSON output format
        json_schema_mode = False
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
                json_tool = xai_chat.tool(
                    name=json_schema_tool_name,
                    description=description,
                    parameters=schema,
                )

                # Add to existing tools or create new tools list
                if "tools" not in create_kwargs:
                    create_kwargs["tools"] = []
                create_kwargs["tools"].append(json_tool)

                # Force the model to use this tool
                create_kwargs["tool_choice"] = "required"

                json_schema_mode = True

        # Handle tools
        if tools:
            create_kwargs["tools"] = self._convert_tools(tools)
        if tool_choice:
            create_kwargs["tool_choice"] = self._convert_tool_choice(
                tool_choice
            )
        if parallel_tool_calls is not None:
            create_kwargs["parallel_tool_calls"] = parallel_tool_calls

        # Handle reasoning
        if reasoning:
            if isinstance(reasoning, dict):
                effort = reasoning.get("effort", "none")
            elif isinstance(reasoning, Reasoning):
                effort = reasoning.effort
            else:
                effort = "none"

            # Map our effort levels to xAI's (low/high)
            if effort and effort != "none":
                # xAI only supports "low" and "high"
                # Map medium -> low, high -> high
                xai_effort = "high" if effort == "high" else "low"
                create_kwargs["reasoning_effort"] = xai_effort

        # Handle web search
        if tools:
            for t in tools:
                tool_dict = t if isinstance(t, dict) else t.to_dict()
                tool_type = tool_dict.get("type", "function")
                if tool_type == "web_search":
                    # xAI supports web search via SearchParameters
                    create_kwargs["search_parameters"] = SearchParameters(
                        mode="auto",
                    )
                    break

        if nconfig:
            create_kwargs.update(nconfig)

        chat = self._client.chat.create(**create_kwargs)
        return chat, json_schema_mode

    async def _acreate_chat(
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
        reasoning: dict | Reasoning | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[Any, bool]:
        """Create an xAI async chat object."""
        create_kwargs: dict[str, Any] = {}
        create_kwargs["model"] = model or self.model
        create_kwargs["max_tokens"] = max_output_tokens or self.max_tokens

        messages: list[Any] = []

        def _convert_input_item(msg: InputItem | dict[str, Any]) -> None:
            m: dict[str, Any] = msg if isinstance(msg, dict) else msg.to_dict()
            item_type = m.get("type")

            if item_type == "function_call":
                return

            if item_type == "function_call_output":
                output = m.get("output")
                if isinstance(output, dict):
                    output = json.dumps(output)
                messages.append(
                    xai_chat.tool_result(str(output) if output else "")
                )
                return

            if item_type == "reasoning":
                return

            if item_type != "message":
                return

            role = m.get("role")
            content = m.get("content")

            # Extract content parts for xAI
            content_parts: list[Any] = []
            if isinstance(content, str):
                content_parts.append(xai_chat.text(content))
            elif isinstance(content, list):
                for c in content:
                    if isinstance(c, dict):
                        ct = c.get("type")
                        if ct == "input_text" or ct == "output_text":
                            text_val = c.get("text", "")
                            if text_val:
                                content_parts.append(xai_chat.text(text_val))
                        elif ct == "input_image":
                            # xAI supports images via image() helper
                            # Accepts URL or base64-encoded string
                            img = c.get("image")
                            if isinstance(img, dict):
                                img_content = img.get("content")
                                source = img.get("source")
                                if img_content:
                                    # Convert bytes to base64 data URL
                                    if isinstance(
                                        img_content, (bytes, bytearray)
                                    ):
                                        b64_data = base64.b64encode(
                                            img_content
                                        ).decode("utf-8")
                                    else:
                                        # Assume already base64 encoded
                                        b64_data = img_content
                                    media_type = img.get(
                                        "media_type", "image/jpeg"
                                    )
                                    data_url = (
                                        f"data:{media_type};base64,{b64_data}"
                                    )
                                    content_parts.append(
                                        xai_chat.image(data_url)
                                    )
                                elif source:
                                    # URL source
                                    content_parts.append(
                                        xai_chat.image(source)
                                    )

            if not content_parts:
                return

            # Map roles and create appropriate messages
            if role == "system" or role == "developer":
                # System messages only accept text, not images
                text_only = [p for p in content_parts if hasattr(p, "text")]
                if text_only:
                    messages.append(xai_chat.system(*content_parts))
            elif role == "assistant":
                messages.append(xai_chat.assistant(*content_parts))
            else:
                messages.append(xai_chat.user(*content_parts))

        if isinstance(input, str):
            messages.append(xai_chat.user(input))
        else:
            for it in input:
                _convert_input_item(it)

        create_kwargs["messages"] = messages

        if temperature is not None:
            create_kwargs["temperature"] = temperature
        if top_p is not None:
            create_kwargs["top_p"] = top_p

        if instructions:
            create_kwargs["messages"] = [
                xai_chat.system(instructions)
            ] + create_kwargs["messages"]

        json_schema_mode = False
        if text:
            text_dict = text if isinstance(text, dict) else text.to_dict()
            fmt = text_dict.get("format")
            if isinstance(fmt, dict) and fmt.get("type") == "json_schema":
                json_schema_tool_name = fmt.get("name", "json_output")
                schema = fmt.get(
                    "schema", {"type": "object", "properties": {}}
                )
                description = fmt.get(
                    "description", "Generate structured JSON output"
                )

                json_tool = xai_chat.tool(
                    name=json_schema_tool_name,
                    description=description,
                    parameters=schema,
                )

                if "tools" not in create_kwargs:
                    create_kwargs["tools"] = []
                create_kwargs["tools"].append(json_tool)

                create_kwargs["tool_choice"] = "required"

                json_schema_mode = True

        if tools:
            create_kwargs["tools"] = self._convert_tools(tools)
        if tool_choice:
            create_kwargs["tool_choice"] = self._convert_tool_choice(
                tool_choice
            )
        if parallel_tool_calls is not None:
            create_kwargs["parallel_tool_calls"] = parallel_tool_calls

        if reasoning:
            if isinstance(reasoning, dict):
                effort = reasoning.get("effort", "none")
            elif isinstance(reasoning, Reasoning):
                effort = reasoning.effort
            else:
                effort = "none"

            # Map our effort levels to xAI's (low/high)
            if effort and effort != "none":
                # xAI only supports "low" and "high"
                # Map medium -> low, high -> high
                xai_effort = "high" if effort == "high" else "low"
                create_kwargs["reasoning_effort"] = xai_effort

        if tools:
            for t in tools:
                tool_dict = t if isinstance(t, dict) else t.to_dict()
                tool_type = tool_dict.get("type", "function")
                if tool_type == "web_search":
                    create_kwargs["search_parameters"] = SearchParameters(
                        mode="auto",
                    )
                    break

        if nconfig:
            create_kwargs.update(nconfig)

        chat = self._async_client.chat.create(**create_kwargs)
        return chat, json_schema_mode

    def _convert_tools(self, tools: list[dict | Tool]) -> list[Any]:
        """Convert tools to xAI format."""
        xai_tools: list[Any] = []
        for t in tools:
            if isinstance(t, dict):
                tool_type = t.get("type", "function")
                tool = t
            else:
                tool_type = t.type
                tool = t.to_dict()

            if tool_type == "function":
                xai_tools.append(
                    xai_chat.tool(
                        name=tool.get("name", ""),
                        description=tool.get("description", ""),
                        parameters=tool.get("parameters")
                        or {"type": "object", "properties": {}},
                    )
                )
            elif tool_type == "web_search":
                # Web search is handled via search_parameters, not tools
                pass
            else:
                raise BadRequestError(f"Unsupported tool type: {tool_type}")

        return xai_tools

    def _convert_tool_choice(
        self,
        choice: dict | ToolChoice | None,
    ) -> str | dict[str, Any] | None:
        """Convert tool_choice to xAI format."""
        if choice is None:
            return None

        if isinstance(choice, (ToolChoiceFunction, AllowedTools)):
            inp = choice.to_dict()
        else:
            inp = choice

        if inp == "none":
            return "none"
        if inp == "auto":
            return "auto"
        if inp == "required":
            return "required"

        if isinstance(inp, dict):
            if inp.get("type") == "function":
                name = inp.get("name")
                if not name:
                    raise BadRequestError(
                        "Function tool_choice must specify a name"
                    )
                return {"type": "function", "function": {"name": name}}
            elif inp.get("type") == "allowed_tools":
                mode = inp.get("mode")
                if mode == "none":
                    return "none"
                if mode == "auto":
                    return "auto"
                if mode == "required":
                    return "required"

        raise BadRequestError(f"Unsupported tool_choice: {type(choice)}")
