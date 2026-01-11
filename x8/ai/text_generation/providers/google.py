from typing import Any, AsyncIterator, Iterator

from google.genai import Client, types
from x8._common.google_provider import GoogleProvider
from x8.core import Response
from x8.core.exceptions import BadRequestError

from .._models import (
    AllowedTools,
    FunctionCall,
    InputItem,
    OutputItem,
    OutputMessage,
    OutputMessageContent,
    OutputReasoningContentText,
    OutputText,
    Reasoning,
    ResponseCompletedEvent,
    ResponseOutputTextDeltaEvent,
    ResponseReasoningTextDeltaEvent,
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
        location: str = "us-central1",
        model: str = "gemini-3-pro-preview",
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
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            max_tool_calls=max_tool_calls,
            reasoning=reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )
        if not stream:
            response = self._client.models.generate_content(**args)
            result = self._convert_result(response)
            return Response(result=result)
        else:

            def _stream_iter() -> (
                Iterator[Response[TextGenerationStreamEvent]]
            ):
                response = self._client.models.generate_content_stream(**args)
                for event in response:
                    converted_event = self._convert_stream_event(event)
                    if converted_event is None:
                        continue
                    yield Response(result=converted_event)

            return _stream_iter()

    async def agenerate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        instructions: str | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        max_output_tokens: int | None = None,
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
            tools=tools,
            tool_choice=tool_choice,
            parallel_tool_calls=parallel_tool_calls,
            max_tool_calls=max_tool_calls,
            reasoning=reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )
        if not stream:
            response = await self._client.aio.models.generate_content(**args)
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
                    converted_event = self._convert_stream_event(event)
                    if converted_event is None:
                        continue
                    yield Response(result=converted_event)

            return _poll_aiter()

    def _convert_stream_event(
        self, event: Any
    ) -> TextGenerationStreamEvent | None:
        # Google streaming returns GenerateContentResponse chunks, not typed
        # events. We'll synthesize internal events based on available data.
        try:
            # Prefer model_dump to get a dict we can inspect.
            e: dict[str, Any] = (
                event.model_dump() if hasattr(event, "model_dump") else event
            )
        except Exception:
            e = {}

        # Emit text delta if present.
        # Note: event.text is the concatenation of text parts in this chunk;
        # without incremental indices, we provide it as a delta unit.
        try:
            text = getattr(event, "text", None)
        except Exception:
            text = None

        if isinstance(text, str) and text:
            return ResponseOutputTextDeltaEvent(delta=text)

        # Emit reasoning delta parts if present in this chunk.
        # Parts with thought=True are considered reasoning.
        parts = e.get("parts")
        if parts and isinstance(parts, list):
            for p in parts:
                if isinstance(p, dict):
                    if p.get("thought") and isinstance(p.get("text"), str):
                        return ResponseReasoningTextDeltaEvent(
                            delta=p.get("text") or ""
                        )

        # If finish_reason is available in the first candidate, emit completed.
        candidates = e.get("candidates")
        if candidates and isinstance(candidates, list):
            c0 = candidates[0] if candidates else None
            if isinstance(c0, dict) and c0.get("finish_reason"):
                return ResponseCompletedEvent(
                    response=self._convert_result(event)
                )

        # Otherwise no event to emit for this chunk.
        return None

    def _convert_result(self, response: Any) -> TextGenerationResult:
        # Convert Google GenerateContentResponse to internal
        # TextGenerationResult.
        r: dict[str, Any] = (
            response.model_dump()
            if hasattr(response, "model_dump")
            else response
        )

        # Build output items: one assistant message (text + reasoning content),
        # plus any function calls.
        output_items: list[OutputItem] = []

        # Extract first candidate content parts.
        candidates = r.get("candidates")
        candidate0: dict[str, Any] | None = None
        if isinstance(candidates, list) and candidates:
            c0 = candidates[0]
            if isinstance(c0, dict):
                candidate0 = c0

        message_content: list[OutputMessageContent] = []
        if candidate0:
            content = candidate0.get("content")
            if isinstance(content, dict):
                parts = content.get("parts")
                if isinstance(parts, list):
                    for part in parts:
                        if not isinstance(part, dict):
                            continue
                        # Map text parts. "thought" indicates reasoning text.
                        txt = part.get("text")
                        if isinstance(txt, str):
                            if bool(part.get("thought")):
                                message_content.append(
                                    OutputReasoningContentText(text=txt)
                                )
                            else:
                                message_content.append(OutputText(text=txt))
                        # Function call parts become separate output items.
                        fc = part.get("function_call")
                        if isinstance(fc, dict) and isinstance(
                            fc.get("name"), str
                        ):
                            output_items.append(
                                FunctionCall(
                                    name=fc.get("name") or "",
                                    arguments=fc.get("args"),
                                )
                            )

        # Add assistant message if any content collected.
        if message_content:
            output_items.insert(
                0,
                OutputMessage(
                    role="assistant",
                    content=message_content,
                    status="completed",
                ),
            )

        # Usage mapping from usage_metadata.
        usage_obj: Usage | None = None
        usage = r.get("usage_metadata")
        if isinstance(usage, dict):
            input_tokens = int(usage.get("prompt_token_count") or 0)
            output_tokens = int(usage.get("candidates_token_count") or 0)
            total_tokens = int(usage.get("total_token_count") or 0)

            # Details (flatten modality counts if present)
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
                input_tokens_details=_modality_details(
                    usage.get("prompt_tokens_details")
                ),
                output_tokens=output_tokens,
                output_tokens_details=_modality_details(
                    usage.get("candidates_tokens_details")
                ),
                total_tokens=total_tokens,
            )

        # created_at: convert datetime to epoch seconds if present.
        created_at: int | None = None
        create_time = r.get("create_time")
        try:
            if create_time is not None and hasattr(create_time, "timestamp"):
                created_at = int(create_time.timestamp())
        except Exception:
            created_at = None

        # Model info
        model_version = r.get("model_version")

        result = TextGenerationResult(
            id=r.get("response_id"),
            model=model_version,
            created_at=created_at,
            status="completed",
            error=None,
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

        def _convert_input_message(msg: InputItem | dict[str, Any]) -> None:
            m: dict[str, Any]
            if isinstance(msg, dict):
                m = msg
            else:
                m = msg.to_dict()
            if m.get("type") != "message":
                return
            role = m.get("role")
            raw_content = m.get("content")

            # Map system/developer to system_instruction
            if role in ("system", "developer"):
                # Build a single Content for system_instruction
                parts: list[dict[str, Any]] = []
                if isinstance(raw_content, str):
                    parts.append({"text": raw_content})
                elif isinstance(raw_content, list):
                    for c in raw_content:
                        if not isinstance(c, dict):
                            continue
                        ct = c.get("type")
                        if ct == "input_text":
                            parts.append({"text": c.get("text")})
                        elif ct == "input_image":
                            img = c.get("image")
                            detail = c.get("detail")
                            _ = detail  # unused for now
                            # ImageData mapping
                            if isinstance(img, dict):
                                media_type = img.get("media_type")
                                content = img.get("content")
                                source = img.get("source")
                                if isinstance(content, (bytes, bytearray)):
                                    parts.append(
                                        {
                                            "inline_data": {
                                                "data": bytes(content),
                                                "mime_type": media_type
                                                or "application/octet-stream",
                                            }
                                        }
                                    )
                                elif isinstance(source, str):
                                    parts.append(
                                        {
                                            "file_data": {
                                                "file_uri": source,
                                                "mime_type": media_type
                                                or "application/octet-stream",
                                            }
                                        }
                                    )
                        elif ct == "input_file":
                            f = c.get("file")
                            if isinstance(f, dict):
                                parts.append(
                                    {
                                        "file_data": {
                                            "file_uri": (
                                                f.get("file_uri")
                                                or f.get("path")
                                                or f.get("source")
                                            ),
                                            "mime_type": (
                                                f.get("mime_type")
                                                or f.get("media_type")
                                            ),
                                        }
                                    }
                                )
                return

            # Regular user/assistant messages become contents
            role_map: dict[str, str] = {
                "user": "user",
                "assistant": "model",
                "developer": "user",
                "system": "user",
            }
            g_role = role_map.get(
                role if isinstance(role, str) else "user",
                "user",
            )
            msg_parts: list[dict[str, Any]] = []
            if isinstance(raw_content, str):
                msg_parts.append({"text": raw_content})
            elif isinstance(raw_content, list):
                for c in raw_content:
                    if not isinstance(c, dict):
                        continue
                    ct = c.get("type")
                    if ct == "input_text":
                        msg_parts.append({"text": c.get("text")})
                    elif ct == "input_image":
                        img = c.get("image")
                        detail = c.get("detail")
                        _ = detail
                        if isinstance(img, dict):
                            media_type = img.get("media_type")
                            content = img.get("content")
                            source = img.get("source")
                            if isinstance(content, (bytes, bytearray)):
                                msg_parts.append(
                                    {
                                        "inline_data": {
                                            "data": bytes(content),
                                            "mime_type": (
                                                media_type
                                                or "application/octet-stream"
                                            ),
                                        }
                                    }
                                )
                            elif isinstance(source, str):
                                msg_parts.append(
                                    {
                                        "file_data": {
                                            "file_uri": source,
                                            "mime_type": (
                                                media_type
                                                or "application/octet-stream"
                                            ),
                                        }
                                    }
                                )
                    elif ct == "input_file":
                        f = c.get("file")
                        if isinstance(f, dict):
                            msg_parts.append(
                                {
                                    "file_data": {
                                        "file_uri": (
                                            f.get("file_uri")
                                            or f.get("path")
                                            or f.get("source")
                                        ),
                                        "mime_type": (
                                            f.get("mime_type")
                                            or f.get("media_type")
                                        ),
                                    }
                                }
                            )
            contents.append({"role": g_role, "parts": msg_parts})

        # Input may be a raw string or a sequence of InputItems.
        if isinstance(input, str):
            contents.append(
                {"role": "user", "parts": [types.Part.from_text(text=input)]}
            )
        else:
            for it in input:
                _convert_input_message(it)

        args["contents"] = contents
        config: dict[str, Any] = {}
        if temperature:
            config["temperature"] = temperature
        if top_p:
            config["top_p"] = top_p
        if max_output_tokens:
            config["max_output_tokens"] = max_output_tokens
        if tools:
            config["tools"] = [self._convert_tools(tools)]
        if tool_choice:
            config["tool_config"] = self._convert_tool_choice(tool_choice)
        if instructions:
            config["system_instruction"] = instructions
        if reasoning:
            config["thinking_config"] = types.ThinkingConfig(
                include_thoughts=True
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
