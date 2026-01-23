import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

from mistralai import Mistral as MistralSDK
from mistralai.utils.eventstreaming import EventStream

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


class Mistral(Provider):
    api_key: str | None
    server: str | None
    server_url: str | None
    url_params: dict[str, str] | None
    timeout_ms: int | None
    model: str
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: MistralSDK
    _init: bool

    def __init__(
        self,
        api_key: str | None = None,
        server: str | None = None,
        server_url: str | None = None,
        url_params: dict[str, str] | None = None,
        timeout_ms: int | None = None,
        model: str = "mistral-large-latest",
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs: Any,
    ):
        self.api_key = api_key
        self.server = server
        self.server_url = server_url
        self.url_params = url_params
        self.timeout_ms = timeout_ms
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
        if self.server is not None:
            client_kwargs["server"] = self.server
        if self.server_url is not None:
            client_kwargs["server_url"] = self.server_url
        if self.url_params is not None:
            client_kwargs["url_params"] = self.url_params
        if self.timeout_ms is not None:
            client_kwargs["timeout_ms"] = self.timeout_ms
        if self.nparams:
            client_kwargs.update(self.nparams)
        self._client = MistralSDK(**client_kwargs)
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
        # Mistral doesn't currently support limiting max tool calls via API.
        _ = (max_tool_calls, nconfig, kwargs)
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
            parallel_tool_calls=parallel_tool_calls,
            reasoning=reasoning,
            stream=stream,
        )
        try:
            if not stream:
                reasoning_requested = self._reasoning_requested(reasoning)
                resp = self._client.chat.complete(**args)
                return Response(
                    result=self._convert_result(
                        resp, reasoning_requested=reasoning_requested
                    )
                )

            def _stream_iter() -> (
                Iterator[Response[TextGenerationStreamEvent]]
            ):
                stream_resp: EventStream[Any] = self._client.chat.stream(
                    **args
                )
                accumulated_text = ""
                accumulated_tool_calls: dict[int, dict[str, Any]] = {}
                last_chunk: Any | None = None
                for event in stream_resp:
                    chunk = getattr(event, "data", None)
                    if chunk is None:
                        continue
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

                final_model = (
                    getattr(last_chunk, "model", None)
                    if last_chunk is not None
                    else args.get("model")
                )
                final_id = (
                    getattr(last_chunk, "id", None)
                    if last_chunk is not None
                    else None
                )
                final_usage = (
                    getattr(last_chunk, "usage", None)
                    if last_chunk is not None
                    else None
                )
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
        _ = (max_tool_calls, nconfig, kwargs)
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
            parallel_tool_calls=parallel_tool_calls,
            reasoning=reasoning,
            stream=stream,
        )
        try:
            if not stream:
                reasoning_requested = self._reasoning_requested(reasoning)
                resp = await self._client.chat.complete_async(**args)
                return Response(
                    result=self._convert_result(
                        resp, reasoning_requested=reasoning_requested
                    )
                )

            async def _stream_aiter() -> (
                AsyncIterator[Response[TextGenerationStreamEvent]]
            ):
                stream_resp = await self._client.chat.stream_async(**args)
                accumulated_text = ""
                accumulated_tool_calls: dict[int, dict[str, Any]] = {}
                last_chunk: Any | None = None
                async for event in stream_resp:
                    chunk = getattr(event, "data", None)
                    if chunk is None:
                        continue
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

                final_model = (
                    getattr(last_chunk, "model", None)
                    if last_chunk is not None
                    else args.get("model")
                )
                final_id = (
                    getattr(last_chunk, "id", None)
                    if last_chunk is not None
                    else None
                )
                final_usage = (
                    getattr(last_chunk, "usage", None)
                    if last_chunk is not None
                    else None
                )
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
        parallel_tool_calls: bool | None,
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
            "stream": bool(stream),
        }
        if temperature is not None:
            args["temperature"] = temperature
        if top_p is not None:
            args["top_p"] = top_p
        args["max_tokens"] = max_output_tokens or self.max_tokens

        prompt_mode = self._convert_reasoning(reasoning)
        if prompt_mode is not None:
            args["prompt_mode"] = prompt_mode

        rf = self._convert_response_format(text)
        if rf is not None:
            args["response_format"] = rf

        if tools is not None:
            args["tools"] = self._convert_tools(tools)

        if tool_choice is not None:
            args["tool_choice"] = self._convert_tool_choice(tool_choice)

        if parallel_tool_calls is not None:
            args["parallel_tool_calls"] = parallel_tool_calls

        return args

    def _convert_reasoning(
        self, reasoning: dict | Reasoning | None
    ) -> str | None:
        if reasoning is None:
            return None
        r = reasoning.to_dict() if hasattr(reasoning, "to_dict") else reasoning
        if not isinstance(r, dict):
            return None
        effort = r.get("effort")
        if effort and effort != "none":
            return "reasoning"
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
                "name": fmt.get("name") or "schema",
                "schema_definition": schema,
            }
            if fmt.get("description") is not None:
                json_schema["description"] = fmt.get("description")
            if fmt.get("strict") is not None:
                json_schema["strict"] = fmt.get("strict")
            return {"type": "json_schema", "json_schema": json_schema}
        return None

    def _convert_tools(self, tools: list[dict | Tool]) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for tool in tools:
            t = tool.to_dict() if hasattr(tool, "to_dict") else tool
            if not isinstance(t, dict):
                continue
            if t.get("type") != "function":
                # Mistral chat tools only support function tools.
                continue
            converted.append(
                {
                    "type": "function",
                    "function": {
                        "name": t.get("name"),
                        "description": t.get("description"),
                        "parameters": t.get("parameters") or {},
                        "strict": t.get("strict"),
                    },
                }
            )
        return converted

    def _convert_tool_choice(self, tool_choice: dict | ToolChoice) -> Any:
        if isinstance(tool_choice, str):
            if tool_choice in {"auto", "none", "any", "required"}:
                return tool_choice
            return "auto"
        if isinstance(tool_choice, ToolChoiceFunction):
            return {
                "type": "function",
                "function": {"name": tool_choice.name},
            }
        if isinstance(tool_choice, dict):
            if tool_choice.get("type") == "function" and tool_choice.get(
                "name"
            ):
                return {
                    "type": "function",
                    "function": {"name": tool_choice["name"]},
                }
            if tool_choice.get("type") in {"auto", "none", "required"}:
                return tool_choice.get("type")
            return "auto"
        if hasattr(tool_choice, "to_dict"):
            return self._convert_tool_choice(tool_choice.to_dict())
        return "auto"

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
                messages.append(
                    {
                        "role": role,
                        "content": self._convert_message_content(
                            content, role=role
                        ),
                    }
                )
            elif t == "function_call_output":
                tool_out = FunctionCallOutput.from_dict(d)
                output = tool_out.output
                if isinstance(output, dict):
                    output_str = json.dumps(output)
                else:
                    output_str = "" if output is None else str(output)
                messages.append(
                    {
                        "role": "tool",
                        "content": output_str,
                        "tool_call_id": tool_out.call_id or tool_out.id,
                        "name": tool_out.name,
                    }
                )
            elif t == "function_call":
                # Represent as an assistant tool call message.
                fc = FunctionCall.from_dict(d)
                args = fc.arguments
                if isinstance(args, str):
                    try:
                        args = json.loads(args)
                    except Exception:
                        pass
                tool_call_id = fc.call_id or fc.id
                messages.append(
                    {
                        "role": "assistant",
                        "content": "",
                        "tool_calls": [
                            {
                                "id": tool_call_id,
                                "type": "function",
                                "function": {
                                    "name": fc.name,
                                    "arguments": (
                                        args if args is not None else {}
                                    ),
                                },
                            }
                        ],
                    }
                )
        return messages

    def _convert_message_content(self, content: Any, *, role: str) -> Any:
        # Mistral accepts either a string or a list of typed chunks.
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
                            "image_url": {"url": url, "detail": "auto"},
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
                    file_name = file_obj.get("name")
                else:
                    file_bytes = None
                    media_type = "application/pdf"
                    file_name = None
                if isinstance(file_bytes, (bytes, bytearray)):
                    url = self._to_data_url(media_type, bytes(file_bytes))
                    chunk: dict[str, Any] = {
                        "type": "document_url",
                        "document_url": url,
                    }
                    if file_name:
                        chunk["document_name"] = file_name
                    chunks.append(chunk)
                continue

            # Assistant outputs used as inputs (multi-turn)
            if p_type == "output_text":
                chunks.append({"type": "text", "text": p.get("text") or ""})

        # If we couldn't build chunks (or we're in non-user roles), fall back.
        if not chunks:
            return ""

        # Mistral expects structured chunks for multimodal.
        return chunks

    def _to_data_url(self, media_type: str, content: bytes) -> str:
        b64 = base64.b64encode(content).decode("utf-8")
        return f"data:{media_type};base64,{b64}"

    def _convert_result(
        self, response: Any, *, reasoning_requested: bool
    ) -> TextGenerationResult:
        output: list[OutputItem] = []

        choice = (
            response.choices[0] if getattr(response, "choices", None) else None
        )
        if choice is not None:
            msg = getattr(choice, "message", None)
            if msg is not None:
                # Tool calls
                tool_calls = getattr(msg, "tool_calls", None) or []
                for tc in tool_calls:
                    func = getattr(tc, "function", None)
                    name = getattr(func, "name", None)
                    arguments = getattr(func, "arguments", None)
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

                # Content may include reasoning chunks.
                content = getattr(msg, "content", None)
                extracted_text, extracted_reasoning = (
                    self._extract_text_and_reasoning_from_content(content)
                )
                if extracted_reasoning:
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
                                OutputText(
                                    text=extracted_text, annotations=None
                                )
                            ],
                            status="completed",
                        ),
                    )

        usage = None
        u = getattr(response, "usage", None)
        if u is not None:
            in_t = getattr(u, "prompt_tokens", None)
            out_t = getattr(u, "completion_tokens", None)
            tot_t = getattr(u, "total_tokens", None)
            if in_t is not None and out_t is not None and tot_t is not None:
                usage = Usage(
                    input_tokens=int(in_t),
                    output_tokens=int(out_t),
                    total_tokens=int(tot_t),
                )

        result_status: Literal["completed", "incomplete"] = "completed"
        if (
            choice is not None
            and getattr(choice, "finish_reason", None) == "length"
        ):
            result_status = "incomplete"

        return TextGenerationResult(
            id=getattr(response, "id", None),
            model=getattr(response, "model", None),
            created_at=getattr(response, "created", None),
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
                    else:
                        continue
                p_type = p.get("type")
                if p_type == "text":
                    text_parts.append(p.get("text") or "")
                elif p_type == "thinking":
                    # ThinkChunk uses key 'thinking'
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
        # chunk is mistralai.models.completionchunk.CompletionChunk
        choices = getattr(chunk, "choices", None) or []
        if not choices:
            return
        choice = choices[0]
        delta = getattr(choice, "delta", None)
        if delta is None:
            return

        # Text deltas
        content = getattr(delta, "content", None)
        if content:
            yield ResponseOutputTextDeltaEvent(
                delta=str(content),
                item_id=None,
                output_index=0,
                content_index=0,
                sequence_number=None,
            )

        # Tool call deltas
        tcs = getattr(delta, "tool_calls", None) or []
        for tc in tcs:
            idx = getattr(tc, "index", None) or 0
            tc_id = getattr(tc, "id", None) or accumulated_tool_calls.get(
                idx, {}
            ).get("id")
            func = getattr(tc, "function", None)
            if func is None:
                continue
            name = getattr(func, "name", None)
            arguments = getattr(func, "arguments", None)
            if idx not in accumulated_tool_calls:
                accumulated_tool_calls[idx] = {
                    "id": tc_id,
                    "name": name or "",
                    "arguments": "",
                }
            if name:
                accumulated_tool_calls[idx]["name"] = name
            if arguments is None:
                continue
            if isinstance(arguments, dict):
                arg_delta = json.dumps(arguments)
            else:
                arg_delta = str(arguments)
            yield ResponseFunctionCallArgumentsDeltaEvent(
                delta=arg_delta,
                item_id=tc_id,
                output_index=idx,
                sequence_number=None,
            )

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
            in_t = getattr(usage, "prompt_tokens", None)
            out_t = getattr(usage, "completion_tokens", None)
            tot_t = getattr(usage, "total_tokens", None)
            if in_t is not None and out_t is not None and tot_t is not None:
                converted_usage = Usage(
                    input_tokens=max(1, int(in_t)),
                    output_tokens=max(1, int(out_t)),
                    total_tokens=max(1, int(tot_t)),
                )

        return TextGenerationResult(
            id=response_id,
            model=model,
            status="completed",
            error=None,
            output=output if output else None,
            usage=converted_usage,
        )
