import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

from cohere import AsyncClientV2, ClientV2

from x8.core import Response
from x8.core._provider import Provider
from x8.core.exceptions import BadRequestError

from .._models import (
    FunctionCall,
    FunctionCallOutput,
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
    ResponseText,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
    ToolChoiceFunction,
    Usage,
)


class Cohere(Provider):
    api_key: str | None
    base_url: str | None
    timeout: float | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: ClientV2
    _async_client: AsyncClientV2
    _init: bool

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = None,
        timeout: float | None = None,
        model: str = "command-a-03-2025",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.api_key = api_key
        self.base_url = base_url
        self.timeout = timeout
        self.model = model
        self.max_tokens = max_tokens
        self.nparams = nparams
        self._init = False
        super().__init__(**kwargs)

    def __setup__(self, context: Any = None) -> None:
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
        self._client = ClientV2(**client_kwargs)
        self._async_client = AsyncClientV2(**client_kwargs)
        self._init = True

    async def __asetup__(self, context: Any = None) -> None:
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
        _ = (max_tool_calls, parallel_tool_calls, nconfig, kwargs)
        args = self._convert_generate_args(
            input=input,
            model=model,
            instructions=instructions,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            text=text,
            tools=tools,
            tool_choice=tool_choice,
            reasoning=reasoning,
            stream=stream,
        )
        try:
            if not stream:
                reasoning_requested = self._reasoning_requested(reasoning)
                resp = self._client.chat(**args)
                return Response(
                    result=self._convert_result(
                        resp,
                        reasoning_requested=reasoning_requested,
                        model=args.get("model"),
                    )
                )

            def _stream_iter() -> (
                Iterator[Response[TextGenerationStreamEvent]]
            ):
                stream_resp = self._client.chat_stream(**args)
                accumulated_text = ""
                accumulated_tool_calls: dict[int, dict[str, Any]] = {}
                last_chunk: Any | None = None
                for chunk in stream_resp:
                    last_chunk = chunk
                    for converted in self._convert_stream_chunk(
                        chunk,
                        accumulated_text=accumulated_text,
                        accumulated_tool_calls=accumulated_tool_calls,
                    ):
                        if converted.type == "output_text_delta":
                            accumulated_text += converted.delta
                        elif converted.type == "function_call_arguments_delta":
                            idx = converted.output_index or 0
                            if idx not in accumulated_tool_calls:
                                accumulated_tool_calls[idx] = {
                                    "id": converted.item_id,
                                    "name": "",
                                    "arguments": "",
                                }
                            accumulated_tool_calls[idx][
                                "arguments"
                            ] += converted.delta
                        yield Response(result=converted)

                final_model = args.get("model")
                final_id = None
                final_usage = None
                if last_chunk is not None:
                    # Extract from message-end event
                    if hasattr(last_chunk, "id"):
                        final_id = last_chunk.id
                    if hasattr(last_chunk, "delta") and hasattr(
                        last_chunk.delta, "usage"
                    ):
                        final_usage = last_chunk.delta.usage
                final_result = self._build_final_result(
                    response_id=final_id,
                    model=str(final_model) if final_model else None,
                    accumulated_text=accumulated_text,
                    accumulated_tool_calls=accumulated_tool_calls,
                    usage=final_usage,
                )
                yield Response(
                    result=ResponseCompletedEvent(
                        sequence_number=None, response=final_result
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
        _ = (max_tool_calls, parallel_tool_calls, nconfig, kwargs)
        args = self._convert_generate_args(
            input=input,
            model=model,
            instructions=instructions,
            temperature=temperature,
            top_p=top_p,
            max_output_tokens=max_output_tokens,
            text=text,
            tools=tools,
            tool_choice=tool_choice,
            reasoning=reasoning,
            stream=stream,
        )
        try:
            if not stream:
                reasoning_requested = self._reasoning_requested(reasoning)
                resp = await self._async_client.chat(**args)
                return Response(
                    result=self._convert_result(
                        resp,
                        reasoning_requested=reasoning_requested,
                        model=args.get("model"),
                    )
                )

            async def _stream_aiter() -> (
                AsyncIterator[Response[TextGenerationStreamEvent]]
            ):
                stream_resp = self._async_client.chat_stream(**args)
                accumulated_text = ""
                accumulated_tool_calls: dict[int, dict[str, Any]] = {}
                last_chunk: Any | None = None
                async for chunk in stream_resp:
                    last_chunk = chunk
                    for converted in self._convert_stream_chunk(
                        chunk,
                        accumulated_text=accumulated_text,
                        accumulated_tool_calls=accumulated_tool_calls,
                    ):
                        if converted.type == "output_text_delta":
                            accumulated_text += converted.delta
                        elif converted.type == "function_call_arguments_delta":
                            idx = converted.output_index or 0
                            if idx not in accumulated_tool_calls:
                                accumulated_tool_calls[idx] = {
                                    "id": converted.item_id,
                                    "name": "",
                                    "arguments": "",
                                }
                            accumulated_tool_calls[idx][
                                "arguments"
                            ] += converted.delta
                        yield Response(result=converted)

                final_model = args.get("model")
                final_id = None
                final_usage = None
                if last_chunk is not None:
                    if hasattr(last_chunk, "id"):
                        final_id = last_chunk.id
                    if hasattr(last_chunk, "delta") and hasattr(
                        last_chunk.delta, "usage"
                    ):
                        final_usage = last_chunk.delta.usage
                final_result = self._build_final_result(
                    response_id=final_id,
                    model=str(final_model) if final_model else None,
                    accumulated_text=accumulated_text,
                    accumulated_tool_calls=accumulated_tool_calls,
                    usage=final_usage,
                )
                yield Response(
                    result=ResponseCompletedEvent(
                        sequence_number=None, response=final_result
                    )
                )

            return _stream_aiter()
        except Exception as e:
            raise BadRequestError(str(e)) from e

    def _reasoning_requested(self, reasoning: dict | Reasoning | None) -> bool:
        if reasoning is None:
            return False
        r = reasoning.to_dict() if hasattr(reasoning, "to_dict") else reasoning
        if not isinstance(r, dict):
            return False
        effort = r.get("effort")
        return bool(effort and effort != "none")

    def _convert_generate_args(
        self,
        *,
        input: str | list[dict[str, Any] | InputItem],
        model: str | None,
        instructions: str | None,
        temperature: float | None,
        top_p: float | None,
        max_output_tokens: int | None,
        text: dict | ResponseText | None,
        tools: list[dict | Tool] | None,
        tool_choice: dict | ToolChoice | None,
        reasoning: dict | Reasoning | None,
        stream: bool | None,
    ) -> dict[str, Any]:
        messages = self._convert_messages(
            input=input, instructions=instructions
        )
        selected_model = model or self.model

        args: dict[str, Any] = {
            "model": selected_model,
            "messages": messages,
        }
        if temperature is not None:
            args["temperature"] = temperature
        if top_p is not None:
            args["p"] = top_p  # Cohere uses "p" instead of "top_p"
        args["max_tokens"] = max_output_tokens or self.max_tokens

        # Handle reasoning (thinking) parameter
        thinking = self._convert_reasoning(reasoning)
        if thinking is not None:
            args["thinking"] = thinking

        rf = self._convert_response_format(text)
        if rf is not None:
            args["response_format"] = rf

        if tools is not None:
            args["tools"] = self._convert_tools(tools)

        if tool_choice is not None:
            args["tool_choice"] = self._convert_tool_choice(tool_choice)

        return args

    def _convert_reasoning(
        self, reasoning: dict | Reasoning | None
    ) -> dict[str, Any] | None:
        if reasoning is None:
            return None
        r = reasoning.to_dict() if hasattr(reasoning, "to_dict") else reasoning
        if not isinstance(r, dict):
            return None
        effort = r.get("effort")
        if effort and effort != "none":
            # Cohere uses thinking parameter with token_budget
            # Map effort levels to token budgets
            budget_map = {
                "low": 1024,
                "medium": 4096,
                "high": 8192,
            }
            budget = budget_map.get(effort, 4096)
            return {"type": "enabled", "token_budget": budget}
        return None

    def _convert_response_format(
        self, text: dict | ResponseText | None
    ) -> dict[str, Any] | None:
        if text is None:
            return None
        t: dict[str, Any]
        if isinstance(text, ResponseText):
            t = text.to_dict()
        elif isinstance(text, dict):
            t = text
        else:
            return None

        fmt = t.get("format")
        if not isinstance(fmt, dict):
            return None
        fmt_type = fmt.get("type")
        if fmt_type == "text":
            return {"type": "text"}
        if fmt_type == "json_schema":
            schema = fmt.get("schema") or fmt.get("schema_")
            if not isinstance(schema, dict):
                schema = {}
            json_schema: dict[str, Any] = {
                "type": "json_object",
                "json_schema": schema,
            }
            return json_schema
        return None

    def _convert_tools(self, tools: list[dict | Tool]) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for tool in tools:
            t = tool.to_dict() if hasattr(tool, "to_dict") else tool
            if not isinstance(t, dict):
                continue
            if t.get("type") != "function":
                # Cohere chat tools only support function tools via v2 API.
                continue
            converted.append(
                {
                    "type": "function",
                    "function": {
                        "name": t.get("name"),
                        "description": t.get("description"),
                        "parameters": t.get("parameters") or {},
                    },
                }
            )
        return converted

    def _convert_tool_choice(self, tool_choice: dict | ToolChoice) -> Any:
        if isinstance(tool_choice, str):
            # Cohere v2 uses REQUIRED, NONE, or omit for auto
            if tool_choice == "required":
                return "REQUIRED"
            if tool_choice == "none":
                return "NONE"
            # "auto" or "any" => don't set tool_choice
            return None
        if isinstance(tool_choice, ToolChoiceFunction):
            # Cohere v2 doesn't support specifying a specific function
            return "REQUIRED"
        if isinstance(tool_choice, dict):
            if tool_choice.get("type") == "function" and tool_choice.get(
                "name"
            ):
                return "REQUIRED"
            tc_type = tool_choice.get("type")
            if tc_type == "required":
                return "REQUIRED"
            if tc_type == "none":
                return "NONE"
            return None
        if hasattr(tool_choice, "to_dict"):
            return self._convert_tool_choice(tool_choice.to_dict())
        return None

    def _convert_messages(
        self,
        *,
        input: str | list[dict[str, Any] | InputItem],
        instructions: str | None,
    ) -> list[dict[str, Any]]:
        items: list[Any]
        if isinstance(input, str):
            items = [
                {
                    "type": "message",
                    "role": "user",
                    "content": input,
                }
            ]
        else:
            items = list(input)

        messages: list[dict[str, Any]] = []
        if instructions:
            messages.append({"role": "system", "content": instructions})

        for item in items:
            d = item.to_dict() if hasattr(item, "to_dict") else item
            if not isinstance(d, dict):
                continue
            t = d.get("type")
            if t == "message":
                role = d.get("role") or "user"
                if role == "developer":
                    role = "system"
                content = d.get("content")
                converted_content = self._convert_message_content(
                    content, role=role
                )
                messages.append(
                    {
                        "role": role,
                        "content": converted_content,
                    }
                )
            elif t == "function_call_output":
                tool_out = FunctionCallOutput.from_dict(d)
                output = tool_out.output
                if isinstance(output, dict):
                    output_str = json.dumps(output)
                else:
                    output_str = "" if output is None else str(output)
                # Cohere v2 expects tool role with tool_call_id
                messages.append(
                    {
                        "role": "tool",
                        "tool_call_id": tool_out.call_id or tool_out.id,
                        "content": [
                            {
                                "type": "document",
                                "document": {"data": output_str},
                            }
                        ],
                    }
                )
            elif t == "function_call":
                # Represent as an assistant message with tool_calls
                fc = FunctionCall.from_dict(d)
                args = fc.arguments
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        pass
                if isinstance(args, dict):
                    args_str = json.dumps(args)
                else:
                    args_str = str(args) if args else "{}"
                tool_call_id = fc.call_id or fc.id
                messages.append(
                    {
                        "role": "assistant",
                        "tool_calls": [
                            {
                                "id": tool_call_id,
                                "type": "function",
                                "function": {
                                    "name": fc.name,
                                    "arguments": args_str,
                                },
                            }
                        ],
                        "tool_plan": f"Calling {fc.name}",
                    }
                )
        return messages

    def _convert_message_content(self, content: Any, *, role: str) -> Any:
        # Cohere accepts either a string or a list of content blocks
        if isinstance(content, str):
            return content
        if not isinstance(content, list):
            return "" if content is None else str(content)

        chunks: list[dict[str, Any]] = []
        for part in content:
            p = part.to_dict() if hasattr(part, "to_dict") else part
            if not isinstance(p, dict):
                continue
            p_type = p.get("type")

            # User inputs
            if p_type == "input_text":
                chunks.append({"type": "text", "text": p.get("text") or ""})
                continue

            if p_type == "input_image":
                image = p.get("image")
                if isinstance(image, dict):
                    img_bytes = image.get("content")
                    media_type = image.get("media_type") or "image/jpeg"
                else:
                    img_bytes = None
                    media_type = "image/jpeg"
                if isinstance(img_bytes, (bytes, bytearray)):
                    url = self._to_data_url(media_type, bytes(img_bytes))
                    chunks.append(
                        {
                            "type": "image_url",
                            "image_url": {"url": url},
                        }
                    )
                continue

            if p_type == "input_file":
                file_obj = p.get("file")
                if isinstance(file_obj, dict):
                    file_bytes = file_obj.get("content")
                    media_type = file_obj.get("media_type") or (
                        "application/pdf"
                    )
                else:
                    file_bytes = None
                    media_type = "application/pdf"
                if isinstance(file_bytes, (bytes, bytearray)):
                    # Cohere supports documents via document type
                    b64 = base64.b64encode(bytes(file_bytes)).decode("utf-8")
                    chunks.append(
                        {
                            "type": "document",
                            "document": {
                                "format": "base64",
                                "source": {"type": "base64", "data": b64},
                                "media_type": media_type,
                            },
                        }
                    )
                continue

            # Assistant outputs used as inputs (multi-turn)
            if p_type == "output_text":
                chunks.append({"type": "text", "text": p.get("text") or ""})

        # If we couldn't build chunks, fall back to empty string
        if not chunks:
            return ""

        return chunks

    def _to_data_url(self, media_type: str, content: bytes) -> str:
        b64 = base64.b64encode(content).decode("utf-8")
        return f"data:{media_type};base64,{b64}"

    def _convert_result(
        self,
        response: Any,
        *,
        reasoning_requested: bool,
        model: str | None = None,
    ) -> TextGenerationResult:
        output: list[OutputItem] = []

        message = getattr(response, "message", None)
        if message is not None:
            # Tool calls
            tool_calls = getattr(message, "tool_calls", None) or []
            for tc in tool_calls:
                func = getattr(tc, "function", None)
                name = getattr(func, "name", None) if func else None
                arguments = getattr(func, "arguments", None) if func else None
                parsed_args: dict[str, Any] | None
                if isinstance(arguments, dict):
                    parsed_args = arguments
                elif isinstance(arguments, str):
                    try:
                        parsed_args = json.loads(arguments)
                    except Exception:
                        parsed_args = {}
                else:
                    parsed_args = {}
                tc_id = getattr(tc, "id", None)
                output.append(
                    FunctionCall(
                        call_id=tc_id,
                        id=tc_id,
                        name=name or "",
                        arguments=parsed_args,
                        status=None,
                    )
                )

            # Content may include text and thinking chunks
            content = getattr(message, "content", None)
            extracted_text, extracted_reasoning = (
                self._extract_text_and_reasoning_from_content(content)
            )
            if extracted_reasoning and reasoning_requested:
                output.append(
                    OutputReasoning(
                        id=f"reasoning_{response.id}",
                        content=[
                            OutputReasoningContentText(
                                type="reasoning_text",
                                text=extracted_reasoning,
                            )
                        ],
                        summary=None,
                        status="completed",
                    )
                )
            if extracted_text:
                output.insert(
                    0,
                    OutputMessage(
                        id=f"msg_{response.id}",
                        role="assistant",
                        content=[
                            OutputText(text=extracted_text, annotations=None)
                        ],
                        status="completed",
                    ),
                )

        usage = None
        u = getattr(response, "usage", None)
        if u is not None:
            # Cohere v2 usage has billed_units and tokens
            tokens = getattr(u, "tokens", None)
            if tokens is not None:
                in_t = getattr(tokens, "input_tokens", None)
                out_t = getattr(tokens, "output_tokens", None)
                if in_t is not None and out_t is not None:
                    usage = Usage(
                        input_tokens=int(in_t),
                        output_tokens=int(out_t),
                        total_tokens=int(in_t) + int(out_t),
                    )

        result_status: Literal["completed", "incomplete"] = "completed"
        finish_reason = getattr(response, "finish_reason", None)
        if finish_reason == "MAX_TOKENS":
            result_status = "incomplete"

        return TextGenerationResult(
            id=getattr(response, "id", None),
            model=model,
            created_at=None,
            status=result_status,
            error=None,
            output=output if output else None,
            usage=usage,
        )

    def _extract_text_and_reasoning_from_content(
        self, content: Any
    ) -> tuple[str, str]:
        if content is None:
            return "", ""
        if isinstance(content, str):
            return content, ""
        if isinstance(content, list):
            text_parts: list[str] = []
            reasoning_parts: list[str] = []
            for p in content:
                if not isinstance(p, dict):
                    if hasattr(p, "model_dump"):
                        p = p.model_dump()
                    elif hasattr(p, "__dict__"):
                        p = vars(p)
                    else:
                        # Try to get type and text attributes
                        p_type = getattr(p, "type", None)
                        p_text = getattr(p, "text", None)
                        if p_type == "text" and p_text:
                            text_parts.append(p_text)
                        elif p_type == "thinking":
                            thinking_text = getattr(p, "thinking", "")
                            if thinking_text:
                                reasoning_parts.append(thinking_text)
                        continue
                p_type = p.get("type")
                if p_type == "text":
                    text_parts.append(p.get("text") or "")
                elif p_type == "thinking":
                    reasoning_parts.append(p.get("thinking") or "")
            return "".join(text_parts), "".join(reasoning_parts)
        return str(content), ""

    def _convert_stream_chunk(
        self,
        chunk: Any,
        *,
        accumulated_text: str,
        accumulated_tool_calls: dict[int, dict[str, Any]],
    ) -> Iterator[TextGenerationStreamEvent]:
        # Cohere v2 streaming events
        chunk_type = getattr(chunk, "type", None)

        if chunk_type == "content-delta":
            delta = getattr(chunk, "delta", None)
            if delta is not None:
                message = getattr(delta, "message", None)
                if message is not None:
                    content = getattr(message, "content", None)
                    if content is not None:
                        text = getattr(content, "text", None)
                        if text:
                            yield ResponseOutputTextDeltaEvent(
                                delta=str(text),
                                item_id=None,
                                output_index=0,
                                content_index=0,
                                sequence_number=None,
                            )

        elif chunk_type == "tool-call-delta":
            delta = getattr(chunk, "delta", None)
            idx = getattr(chunk, "index", None) or 0
            if delta is not None:
                # Cohere structure (using pydantic models with attributes):
                # delta.message.tool_calls.function.arguments
                message = getattr(delta, "message", None)
                if message is not None:
                    tool_calls = getattr(message, "tool_calls", None)
                    if tool_calls is not None:
                        func = getattr(tool_calls, "function", None)
                        if func is not None:
                            arguments = getattr(func, "arguments", None)
                            if arguments:
                                tc_data = accumulated_tool_calls.get(idx, {})
                                item_id = tc_data.get("id") if tc_data else ""
                                yield ResponseFunctionCallArgumentsDeltaEvent(
                                    delta=str(arguments),
                                    item_id=item_id,
                                    output_index=idx,
                                    sequence_number=None,
                                )

        elif chunk_type == "tool-call-start":
            delta = getattr(chunk, "delta", None)
            idx = getattr(chunk, "index", None) or 0
            if delta is not None:
                # Cohere structure (pydantic models with attributes):
                # delta.message.tool_calls = ToolCallV2(id, type, function)
                message = getattr(delta, "message", None)
                if message is not None:
                    tool_calls = getattr(message, "tool_calls", None)
                    if tool_calls is not None:
                        tool_id = getattr(tool_calls, "id", None)
                        func = getattr(tool_calls, "function", None)
                        name = getattr(func, "name", "") if func else ""
                        if idx not in accumulated_tool_calls:
                            accumulated_tool_calls[idx] = {
                                "id": tool_id,
                                "name": name,
                                "arguments": "",
                            }

    def _build_final_result(
        self,
        *,
        response_id: str | None,
        model: str | None,
        accumulated_text: str,
        accumulated_tool_calls: dict[int, dict[str, Any]],
        usage: Any | None,
    ) -> TextGenerationResult:
        output: list[OutputItem] = []

        if accumulated_text:
            output.append(
                OutputMessage(
                    id=f"msg_{response_id or 'stream'}",
                    role="assistant",
                    content=[
                        OutputText(text=accumulated_text, annotations=None)
                    ],
                    status="completed",
                )
            )

        for idx in sorted(accumulated_tool_calls.keys()):
            tc = accumulated_tool_calls[idx]
            args_raw = tc.get("arguments") or ""
            parsed_args: dict[str, Any]
            try:
                parsed_args = json.loads(args_raw) if args_raw else {}
            except Exception:
                parsed_args = {}
            output.append(
                FunctionCall(
                    call_id=tc.get("id"),
                    id=tc.get("id"),
                    name=tc.get("name") or "",
                    arguments=parsed_args,
                    status=None,
                )
            )

        converted_usage = None
        if usage is not None:
            tokens = getattr(usage, "tokens", None)
            if tokens is not None:
                in_t = getattr(tokens, "input_tokens", None)
                out_t = getattr(tokens, "output_tokens", None)
                if in_t is not None and out_t is not None:
                    converted_usage = Usage(
                        input_tokens=max(1, int(in_t)),
                        output_tokens=max(1, int(out_t)),
                        total_tokens=max(1, int(in_t) + int(out_t)),
                    )

        return TextGenerationResult(
            id=response_id,
            model=model,
            status="completed",
            error=None,
            output=output if output else None,
            usage=converted_usage,
        )
