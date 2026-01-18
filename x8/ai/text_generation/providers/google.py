import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

from google.genai import Client
from google.genai import errors as google_errors
from google.genai import types

from x8._common.google_provider import GoogleProvider
from x8.core import Response
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
    ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent,
    ResponseIncompleteEvent,
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


class Google(GoogleProvider):
    vertexai: bool
    project: str | None
    location: str
    model: str
    api_key: str | None
    nparams: dict[str, Any] | None

    _client: Client
    _init: bool

    def __init__(
        self,
        vertexai: bool = True,
        project: str | None = None,
        location: str = "global",
        model: str = "gemini-3-flash-preview",
        api_key: str | None = None,
        service_account_info: str | None = None,
        service_account_file: str | None = None,
        access_token: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        self.vertexai = vertexai
        self.project = project
        self.location = location
        self.model = model
        self.api_key = api_key
        self.nparams = nparams
        self._init = False
        super().__init__(
            service_account_info=service_account_info,
            service_account_file=service_account_file,
            access_token=access_token,
            **kwargs,
        )

    def __setup__(self, context=None):
        if self._init:
            return
        if self.api_key:
            credentials = None
        else:
            credentials = self._get_credentials()
        self._client = Client(
            vertexai=self.vertexai,
            api_key=self.api_key,
            credentials=credentials,
            project=self._get_project_or_default(self.project),
            location=self.location,
            **self.nparams or {},
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
        try:
            if not stream:
                response = self._client.models.generate_content(**args)
                result = self._convert_result(response)
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    response = self._client.models.generate_content_stream(
                        **args
                    )
                    for event in response:
                        converted_events = self._convert_stream_event(event)
                        for converted_event in converted_events:
                            yield Response(result=converted_event)

                return _stream_iter()
        except google_errors.ClientError as e:
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
        try:
            if not stream:
                response = await self._client.aio.models.generate_content(
                    **args
                )
                result = self._convert_result(response)
                return Response(result=result)
            else:

                async def _poll_aiter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    response = (
                        await self._client.aio.models.generate_content_stream(
                            **args
                        )
                    )
                    async for event in response:
                        converted_events = self._convert_stream_event(event)
                        for converted_event in converted_events:
                            yield Response(result=converted_event)

                return _poll_aiter()
        except google_errors.ClientError as e:
            raise BadRequestError(str(e)) from e

    def _convert_stream_event(
        self, event: Any
    ) -> list[TextGenerationStreamEvent]:
        # Google streaming returns GenerateContentResponse chunks.
        # We synthesize unified stream events based on available data.
        # Returns a list because a single chunk
        # may contain both content and finish_reason.
        results: list[TextGenerationStreamEvent] = []

        try:
            e: dict[str, Any] = (
                event.model_dump() if hasattr(event, "model_dump") else event
            )
        except Exception:
            e = {}

        # Check candidates for content and finish state
        candidates = e.get("candidates")
        candidate0: dict[str, Any] | None = None
        if isinstance(candidates, list) and candidates:
            c0 = candidates[0]
            if isinstance(c0, dict):
                candidate0 = c0

        # Process parts for delta events FIRST (before terminal events)
        if candidate0:
            content = candidate0.get("content")
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if not isinstance(part, dict):
                            continue

                        # Text delta (reasoning or regular)
                        txt = part.get("text")
                        if isinstance(txt, str) and txt:
                            if part.get("thought"):
                                results.append(
                                    ResponseReasoningTextDeltaEvent(delta=txt)
                                )
                            else:
                                results.append(
                                    ResponseOutputTextDeltaEvent(delta=txt)
                                )

                        # Function call arguments delta
                        fc = part.get("function_call")
                        if isinstance(fc, dict):
                            args = fc.get("args")
                            if args is not None:
                                args_str = (
                                    json.dumps(args)
                                    if isinstance(args, dict)
                                    else str(args)
                                )
                                results.append(
                                    ResponseFunctionCallArgumentsDeltaEvent(
                                        delta=args_str,
                                        item_id=fc.get("id"),
                                    )
                                )

        # Fallback: try to get text directly from event if no parts found
        if not results:
            try:
                text = getattr(event, "text", None)
            except Exception:
                text = None

            if isinstance(text, str) and text:
                results.append(ResponseOutputTextDeltaEvent(delta=text))

        # Check for finish_reason to emit terminal events AFTER content
        if candidate0:
            finish_reason = candidate0.get("finish_reason")
            if finish_reason:
                if finish_reason == "MAX_TOKENS":
                    results.append(
                        ResponseIncompleteEvent(
                            response=self._convert_result(event)
                        )
                    )
                elif finish_reason in ("SAFETY", "BLOCKED", "RECITATION"):
                    results.append(
                        ResponseFailedEvent(
                            response=self._convert_result(event)
                        )
                    )
                else:
                    # STOP or other normal completion
                    results.append(
                        ResponseCompletedEvent(
                            response=self._convert_result(event)
                        )
                    )

        return results

    def _convert_result(self, response: Any) -> TextGenerationResult:
        r: dict[str, Any] = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response
        )

        output_items: list[OutputItem] = []
        message_content: list[OutputMessageContent] = []
        reasoning_content: list[OutputReasoningContentText] = []

        # Extract first candidate content parts.
        candidates = r.get("candidates")
        candidate0: dict[str, Any] | None = None
        if isinstance(candidates, list) and candidates:
            c0 = candidates[0]
            if isinstance(c0, dict):
                candidate0 = c0

        if candidate0:
            content = candidate0.get("content")
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if not isinstance(part, dict):
                            continue
                        # Text parts - "thought" indicates reasoning text
                        txt = part.get("text")
                        if isinstance(txt, str):
                            if part.get("thought"):
                                reasoning_content.append(
                                    OutputReasoningContentText(text=txt)
                                )
                            else:
                                message_content.append(OutputText(text=txt))
                        # Function call parts
                        fc = part.get("function_call")
                        if isinstance(fc, dict) and fc.get("name"):
                            output_items.append(
                                FunctionCall(
                                    name=fc.get("name") or "",
                                    arguments=fc.get("args"),
                                    call_id=fc.get("id"),
                                    thought_signature=part.get(
                                        "thought_signature"
                                    ),
                                )
                            )

        # Add reasoning as separate output item if present
        if reasoning_content:
            output_items.insert(
                0,
                OutputReasoning(
                    content=reasoning_content,
                    status="completed",
                ),
            )

        # Add assistant message if any content collected
        if message_content:
            output_items.insert(
                0,
                OutputMessage(
                    role="assistant",
                    content=message_content,
                    status="completed",
                ),
            )

        # Usage mapping from usage_metadata
        usage_obj: Usage | None = None
        usage = r.get("usage_metadata")
        if isinstance(usage, dict):
            input_tokens = int(usage.get("prompt_token_count") or 0)
            output_tokens = int(usage.get("candidates_token_count") or 0)
            total_tokens = int(usage.get("total_token_count") or 0)

            def _modality_details(items: Any) -> dict[str, int] | None:
                if not isinstance(items, list) or not items:
                    return None
                details: dict[str, int] = {}
                for it in items:
                    if isinstance(it, dict):
                        mod = it.get("modality")
                        cnt = it.get("token_count")
                        if mod is not None and isinstance(cnt, int):
                            details[str(mod)] = cnt
                return details or None

            usage_obj = Usage(
                input_tokens=input_tokens,
                # input_tokens_details=_modality_details(
                #    usage.get("prompt_tokens_details")
                # ),
                output_tokens=output_tokens,
                output_tokens_details={
                    "reasoning_tokens": int(
                        usage.get("thoughts_token_count") or 0
                    )
                },
                total_tokens=total_tokens,
            )

        # Error handling
        error_obj: ErrorDetail | None = None
        # Check for blocked content or other errors
        if candidate0:
            finish_reason = candidate0.get("finish_reason")
            if finish_reason in ("SAFETY", "BLOCKED", "RECITATION"):
                error_obj = ErrorDetail(
                    code=finish_reason.lower(),
                    message=f"Response blocked due to {finish_reason}",
                )
            # Check for content filter results
            safety_ratings = candidate0.get("safety_ratings")
            if isinstance(safety_ratings, list):
                for rating in safety_ratings:
                    if isinstance(rating, dict) and rating.get("blocked"):
                        error_obj = ErrorDetail(
                            code="content_filter",
                            message=f"Blocked by {rating.get('category')}",
                        )
                        break

        # Determine status
        status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        if candidate0:
            finish_reason = candidate0.get("finish_reason")
            if finish_reason == "MAX_TOKENS":
                status = "incomplete"
            elif finish_reason in ("SAFETY", "BLOCKED", "RECITATION"):
                status = "failed"
        if error_obj:
            status = "failed"

        # created_at: convert datetime to epoch seconds if present
        created_at: int | None = None
        create_time = r.get("create_time")
        try:
            if create_time is not None and hasattr(create_time, "timestamp"):
                created_at = int(create_time.timestamp())
        except Exception:
            created_at = None

        result = TextGenerationResult(
            id=r.get("response_id"),
            model=r.get("model_version"),
            created_at=created_at,
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

        contents: list[dict[str, Any]] = []

        def _convert_content_part(c: dict[str, Any]) -> dict[str, Any] | None:
            """Convert a single content part to Google format."""
            ct = c.get("type")
            if ct == "input_text":
                return {"text": c.get("text")}
            if ct == "output_text":
                # Handle output text from previous assistant messages
                return {"text": c.get("text")}
            if ct == "input_image":
                img = c.get("image")
                if not isinstance(img, dict):
                    return None
                media_type = (
                    img.get("media_type") or "application/octet-stream"
                )
                content = img.get("content")
                source = img.get("source")
                if isinstance(content, (bytes, bytearray)):
                    return {
                        "inline_data": {
                            "data": bytes(content),
                            "mime_type": media_type,
                        }
                    }
                elif isinstance(content, str):
                    return {
                        "inline_data": {
                            "data": base64.b64decode(content),
                            "mime_type": media_type,
                        }
                    }
                if isinstance(source, str):
                    return {
                        "file_data": {
                            "file_uri": source,
                            "mime_type": media_type,
                        }
                    }
            if ct == "input_file":
                f = c.get("file")
                if isinstance(f, dict):
                    content = f.get("content")
                    mime_type = (
                        f.get("media_type") or "application/octet-stream"
                    )
                    if isinstance(content, (bytes, bytearray)):
                        return {
                            "inline_data": {
                                "data": bytes(content),
                                "mime_type": mime_type,
                            }
                        }
                    elif isinstance(content, str):
                        # Treat string content as base64-encoded
                        return {
                            "inline_data": {
                                "data": base64.b64decode(content),
                                "mime_type": mime_type,
                            }
                        }
                    source = f.get("source")
                    if isinstance(source, str):
                        return {
                            "file_data": {
                                "file_uri": source,
                                "mime_type": mime_type,
                            }
                        }
            return None

        def _convert_content_to_parts(
            raw_content: str | list | None,
        ) -> list[dict[str, Any]]:
            """Convert message content to Google parts list."""
            if isinstance(raw_content, str):
                return [{"text": raw_content}]
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
                fc_dict: dict[str, Any] = {
                    "name": m.get("name"),
                    "args": m.get("arguments") or {},
                }
                # Build the part with function_call
                part: dict[str, Any] = {"function_call": fc_dict}
                thought_sig = m.get("thought_signature")
                if thought_sig:
                    part["thought_signature"] = thought_sig
                contents.append(
                    {
                        "role": "model",
                        "parts": [part],
                    }
                )
                return

            if item_type == "function_call_output":
                contents.append(
                    {
                        "role": "user",
                        "parts": [
                            {
                                "function_response": {
                                    "name": m.get("name") or m.get("call_id"),
                                    "response": {"output": m.get("output")},
                                }
                            }
                        ],
                    }
                )
                return

            # Handle OutputReasoning (skip - not sent back to Google)
            if item_type == "reasoning":
                return

            if item_type != "message":
                return

            role = m.get("role")
            # Map roles to Google format
            if role == "assistant":
                g_role = "model"
            elif role in ("system", "developer"):
                g_role = "user"
            else:
                g_role = "user"

            parts = _convert_content_to_parts(m.get("content"))
            contents.append({"role": g_role, "parts": parts})

        # Input may be a raw string or a sequence of InputItems.
        if isinstance(input, str):
            contents.append(
                {"role": "user", "parts": [types.Part.from_text(text=input)]}
            )
        else:
            for it in input:
                _convert_input_item(it)

        args["contents"] = contents
        config: dict[str, Any] = {}
        if temperature:
            config["temperature"] = temperature
        if top_p:
            config["top_p"] = top_p
        if max_output_tokens:
            config["max_output_tokens"] = max_output_tokens
        if text:
            config["response_modalities"] = ["TEXT"]
            text_dict = text if isinstance(text, dict) else text.to_dict()
            fmt = text_dict.get("format")
            if isinstance(fmt, dict) and fmt.get("type") == "json_schema":
                config["response_mime_type"] = "application/json"
                schema = fmt.get("schema")
                if schema:
                    config["response_schema"] = schema
        if tools:
            config["tools"] = self._convert_tools(tools)
        if tool_choice:
            config["tool_config"] = self._convert_tool_choice(tool_choice)
        if instructions:
            config["system_instruction"] = instructions
        if reasoning:
            if isinstance(reasoning, dict):
                effort = reasoning.get("effort", "low")
            elif isinstance(reasoning, Reasoning):
                effort = reasoning.effort
            else:
                effort = "low"
            thinking_level_map: dict[str, types.ThinkingLevel] = {
                "none": types.ThinkingLevel.MINIMAL,
                "low": types.ThinkingLevel.LOW,
                "medium": types.ThinkingLevel.MEDIUM,
                "high": types.ThinkingLevel.HIGH,
            }
            config["thinking_config"] = types.ThinkingConfig(
                include_thoughts=True,
                thinking_level=thinking_level_map.get(
                    effort, types.ThinkingLevel.MINIMAL
                ),
            )
        if config:
            args["config"] = config
        if nconfig:
            args.update(nconfig)
        return args

    def _convert_function(self, fn: dict) -> types.FunctionDeclaration:
        decl = types.FunctionDeclaration(
            name=fn.get("name"),
            description=fn.get("description"),
            parameters_json_schema=fn.get("parameters"),
        )
        return decl

    def _convert_tools(self, tools: list[dict | Tool]) -> list[types.Tool]:
        function_declarations: list[types.FunctionDeclaration] = []
        google_search = None
        for t in tools:
            if isinstance(t, dict):
                tool_type = t.get("type", "function")
                tool = t
            else:
                tool_type = t.type
                tool = t.to_dict()
            if tool_type == "function":
                decl = self._convert_function(tool)
                function_declarations.append(decl)
            elif tool_type == "web_search":
                google_search = types.GoogleSearch()
            else:
                raise BadRequestError(f"Unsupported tool type: {tool_type}")
        return [
            types.Tool(
                function_declarations=function_declarations or None,
                google_search=google_search,
            )
        ]

    def _convert_tool_choice(
        self,
        choice: dict | ToolChoice | None,
    ) -> types.ToolConfig | None:
        if choice is None:
            return None

        if isinstance(choice, ToolChoiceFunction) or isinstance(
            choice, AllowedTools
        ):
            inp = choice.to_dict()
        else:
            inp = choice

        choice_mode_map: dict = {
            "none": types.FunctionCallingConfigMode.NONE,
            "auto": types.FunctionCallingConfigMode.AUTO,
            "required": types.FunctionCallingConfigMode.ANY,
        }
        if inp in ("none", "auto", "required"):
            return types.ToolConfig(
                function_calling_config=types.FunctionCallingConfig(
                    mode=choice_mode_map[inp]
                )
            )
        if isinstance(inp, dict):
            if inp.get("type") == "function":
                name = inp.get("name")
                if not name:
                    raise BadRequestError(
                        "Function tool_choice must specify a name"
                    )
                return types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(
                        mode=types.FunctionCallingConfigMode.ANY,
                        allowed_function_names=[name],
                    )
                )
            elif inp.get("type") == "allowed_tools":
                mode = inp.get("mode")
                allowed = inp.get("tools")
                allowed_function_names = (
                    [t.get("name") for t in allowed]
                    if isinstance(allowed, list)
                    else None
                )
                return types.ToolConfig(
                    function_calling_config=types.FunctionCallingConfig(
                        mode=choice_mode_map[mode],
                        allowed_function_names=allowed_function_names,
                    )
                )

        raise BadRequestError(f"Unsupported tool_choice: {type(choice)}")
