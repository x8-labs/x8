"""Amazon Bedrock provider for text generation."""

from __future__ import annotations

import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal

import boto3

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


class AmazonBedrock(Provider):
    """Amazon Bedrock provider for text generation.

    This provider uses Amazon Bedrock's Converse API which provides a
    unified interface for all foundation models available on Bedrock
    (Anthropic Claude, Amazon Titan, Meta Llama, Mistral, etc.).
    """

    model: str
    region: str | None
    profile_name: str | None
    aws_access_key_id: str | None
    aws_secret_access_key: str | None
    aws_session_token: str | None
    max_tokens: int
    nparams: dict[str, Any] | None

    _client: Any
    _init: bool
    _ainit: bool

    def __init__(
        self,
        model: str = "anthropic.claude-sonnet-4-20250514-v1:0",
        region: str | None = None,
        profile_name: str | None = None,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        max_tokens: int = 8192,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize Amazon Bedrock provider.

        Args:
            model:
                Bedrock model ID to use for text generation.
                Examples: "anthropic.claude-sonnet-4-20250514-v1:0",
                "amazon.titan-text-premier-v1:0",
                "meta.llama3-1-70b-instruct-v1:0".
            region:
                AWS region name (e.g., "us-east-1", "us-west-2").
            profile_name:
                AWS profile name from credentials file.
            aws_access_key_id:
                AWS access key ID for authentication.
            aws_secret_access_key:
                AWS secret access key for authentication.
            aws_session_token:
                AWS session token for temporary credentials.
            max_tokens:
                Default maximum tokens for responses.
            nparams:
                Native parameters passed to boto3 client.
        """
        self.model = model
        self.region = region
        self.profile_name = profile_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.max_tokens = max_tokens
        self.nparams = nparams

        self._client = None
        self._init = False
        self._ainit = False
        super().__init__(**kwargs)

    def __setup__(self, context=None):
        if self._init:
            return

        session = boto3.Session(
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=self.aws_session_token,
            profile_name=self.profile_name,
            region_name=self.region,
        )

        client_kwargs = {}
        if self.nparams:
            client_kwargs.update(self.nparams)

        self._client = session.client("bedrock-runtime", **client_kwargs)
        self._init = True

    async def __asetup__(self, context=None):
        # Boto3 doesn't have native async support, so we use the sync client
        # For true async, consider using aioboto3 in the future
        if self._ainit:
            return
        self.__setup__(context)
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
            reasoning=reasoning,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if not stream:
                response = self._client.converse(**args)
                result = self._convert_result(
                    response,
                    model=args.get("modelId", self.model),
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                )
                return Response(result=result)
            else:

                def _stream_iter() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    response = self._client.converse_stream(**args)
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    usage_info: dict[str, Any] = {}
                    stop_reason = None

                    for event in response.get("stream", []):
                        for conv_event in self._convert_stream_event(
                            event,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
                            json_schema_mode,
                        ):
                            if conv_event.type == "output_text_delta":
                                accumulated_text += conv_event.delta
                            elif conv_event.type == "reasoning_text_delta":
                                accumulated_reasoning += conv_event.delta
                            # Note: tool call arguments are accumulated
                            # directly in _convert_stream_event
                            yield Response(result=conv_event)

                        # Capture metadata events
                        if "metadata" in event:
                            metadata = event["metadata"]
                            if "usage" in metadata:
                                usage_info = metadata["usage"]
                        if "messageStop" in event:
                            stop_reason = event["messageStop"].get(
                                "stopReason"
                            )

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=args.get("modelId", self.model),
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        usage_info=usage_info,
                        stop_reason=stop_reason,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _stream_iter()
        except self._client.exceptions.ValidationException as e:
            raise BadRequestError(str(e)) from e
        except Exception as e:
            if "ValidationException" in str(type(e)):
                raise BadRequestError(str(e)) from e
            raise

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
        # Boto3 doesn't have native async, so we call sync methods
        # For production async, consider aioboto3
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
            reasoning=reasoning,
            nconfig=nconfig,
            **kwargs,
        )

        try:
            if not stream:
                response = self._client.converse(**args)
                result = self._convert_result(
                    response,
                    model=args.get("modelId", self.model),
                    json_schema_mode=json_schema_mode,
                    json_schema_tool_name=json_schema_tool_name,
                )
                return Response(result=result)
            else:

                async def _astream_iter() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    response = self._client.converse_stream(**args)
                    accumulated_text = ""
                    accumulated_reasoning = ""
                    accumulated_tool_calls: dict[int, dict] = {}
                    usage_info: dict[str, Any] = {}
                    stop_reason = None

                    for event in response.get("stream", []):
                        for conv_event in self._convert_stream_event(
                            event,
                            accumulated_text,
                            accumulated_reasoning,
                            accumulated_tool_calls,
                            json_schema_mode,
                        ):
                            if conv_event.type == "output_text_delta":
                                accumulated_text += conv_event.delta
                            elif conv_event.type == "reasoning_text_delta":
                                accumulated_reasoning += conv_event.delta
                            # Note: tool call arguments are accumulated
                            # directly in _convert_stream_event
                            yield Response(result=conv_event)

                        # Capture metadata events
                        if "metadata" in event:
                            metadata = event["metadata"]
                            if "usage" in metadata:
                                usage_info = metadata["usage"]
                        if "messageStop" in event:
                            stop_reason = event["messageStop"].get(
                                "stopReason"
                            )

                    # Emit completed event
                    final_result = self._build_final_result(
                        model=args.get("modelId", self.model),
                        accumulated_text=accumulated_text,
                        accumulated_reasoning=accumulated_reasoning,
                        accumulated_tool_calls=accumulated_tool_calls,
                        usage_info=usage_info,
                        stop_reason=stop_reason,
                        json_schema_mode=json_schema_mode,
                        json_schema_tool_name=json_schema_tool_name,
                    )
                    yield Response(
                        result=ResponseCompletedEvent(
                            sequence_number=None, response=final_result
                        )
                    )

                return _astream_iter()
        except Exception as e:
            if "ValidationException" in str(type(e)):
                raise BadRequestError(str(e)) from e
            raise

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
        reasoning: dict | Reasoning | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> tuple[dict[str, Any], bool, str | None]:
        """Convert unified args to Bedrock Converse API format."""
        args: dict[str, Any] = {}

        # Model ID
        args["modelId"] = model or self.model

        # Convert input to messages
        messages = self._convert_input_to_messages(input)
        args["messages"] = messages

        # System prompt
        if instructions:
            args["system"] = [{"text": instructions}]

        # Inference config
        inference_config: dict[str, Any] = {}
        if max_output_tokens is not None:
            inference_config["maxTokens"] = max_output_tokens
        else:
            inference_config["maxTokens"] = self.max_tokens

        if temperature is not None:
            inference_config["temperature"] = temperature
        if top_p is not None:
            inference_config["topP"] = top_p

        if inference_config:
            args["inferenceConfig"] = inference_config

        # JSON schema mode handling
        json_schema_mode = False
        json_schema_tool_name = None

        if text is not None:
            if isinstance(text, dict):
                text = ResponseText(**text)
            if text.format is not None:
                fmt = text.format
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

                    # Create a tool for JSON schema
                    json_tool = {
                        "toolSpec": {
                            "name": json_schema_tool_name,
                            "description": fmt_description or "Generate JSON",
                            "inputSchema": {"json": fmt_schema},
                        }
                    }

                    if "toolConfig" not in args:
                        args["toolConfig"] = {"tools": []}
                    args["toolConfig"]["tools"].append(json_tool)

                    # Force tool use
                    args["toolConfig"]["toolChoice"] = {
                        "tool": {"name": json_schema_tool_name}
                    }

        # Tools
        if tools is not None:
            converted_tools = self._convert_tools(tools)
            if converted_tools:
                if "toolConfig" not in args:
                    args["toolConfig"] = {"tools": []}
                args["toolConfig"]["tools"].extend(converted_tools)

        # Tool choice
        if tool_choice is not None and not json_schema_mode:
            args["toolConfig"] = args.get("toolConfig", {})
            args["toolConfig"]["toolChoice"] = self._convert_tool_choice(
                tool_choice
            )

        # Additional model request fields for reasoning (Claude models only)
        # Only Claude models on Bedrock support extended thinking
        model_id = args.get("modelId", "")
        is_claude_model = "anthropic.claude" in model_id.lower()

        if reasoning is not None and is_claude_model:
            if isinstance(reasoning, dict):
                reasoning = Reasoning(**reasoning)

            # Check if reasoning is enabled
            effort = getattr(reasoning, "effort", None)
            if effort and effort != "none":
                # Enable thinking for Claude models
                additional_fields = args.get(
                    "additionalModelRequestFields", {}
                )
                thinking_config: dict[str, Any] = {"type": "enabled"}

                # Map effort to budget tokens
                if effort == "low":
                    thinking_config["budgetTokens"] = 1024
                elif effort == "medium":
                    thinking_config["budgetTokens"] = 4096
                elif effort == "high":
                    thinking_config["budgetTokens"] = 16384

                additional_fields["thinking"] = thinking_config
                args["additionalModelRequestFields"] = additional_fields

        # Native config overrides
        if nconfig:
            args.update(nconfig)

        return args, json_schema_mode, json_schema_tool_name

    def _convert_input_to_messages(
        self,
        input: str | list[dict[str, Any] | InputItem],
    ) -> list[dict[str, Any]]:
        """Convert unified input format to Bedrock messages format."""
        messages: list[dict[str, Any]] = []

        # Handle simple string input
        if isinstance(input, str):
            messages.append({"role": "user", "content": [{"text": input}]})
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
                    # Assistant message with tool use
                    tool_use = {
                        "toolUseId": item.get("call_id") or item.get("id"),
                        "name": item.get("name"),
                        "input": item.get("arguments", {}),
                    }
                    messages.append(
                        {
                            "role": "assistant",
                            "content": [{"toolUse": tool_use}],
                        }
                    )
                elif item_type == "function_call_output":
                    # Tool result message
                    tool_result = {
                        "toolUseId": item.get("call_id"),
                        "content": [{"text": str(item.get("output", ""))}],
                    }
                    messages.append(
                        {
                            "role": "user",
                            "content": [{"toolResult": tool_result}],
                        }
                    )
            elif hasattr(item, "type"):
                if item.type == "message":
                    msg = self._convert_message_item(item.to_dict())
                    if msg:
                        messages.append(msg)

        return messages

    def _convert_message_item(
        self, item: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert a message item to Bedrock format."""
        role = item.get("role")
        content = item.get("content")

        if role == "system":
            # System messages are handled separately in Bedrock
            return None

        bedrock_role = "user" if role == "user" else "assistant"
        bedrock_content: list[dict[str, Any]] = []

        if isinstance(content, str):
            bedrock_content.append({"text": content})
        elif isinstance(content, list):
            for c in content:
                if isinstance(c, dict):
                    c_type = c.get("type")
                    if c_type in ["input_text", "output_text", "text"]:
                        bedrock_content.append({"text": c.get("text", "")})
                    elif c_type == "input_image":
                        image_data = self._convert_image_content(c)
                        if image_data:
                            bedrock_content.append(image_data)
                    elif c_type == "input_file":
                        file_data = self._convert_file_content(c)
                        if file_data:
                            bedrock_content.append(file_data)
                elif isinstance(c, str):
                    bedrock_content.append({"text": c})

        if not bedrock_content:
            return None

        return {"role": bedrock_role, "content": bedrock_content}

    def _convert_image_content(
        self, content: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert image content to Bedrock format."""
        image = content.get("image", {})
        image_bytes = None
        media_type = image.get("media_type", "image/jpeg")

        if "content" in image:
            img_content = image["content"]
            if isinstance(img_content, bytes):
                image_bytes = img_content
            elif isinstance(img_content, str):
                # Assume base64 encoded
                image_bytes = base64.b64decode(img_content)

        if "url" in image:
            url = image["url"]
            if url.startswith("data:"):
                # Data URL
                parts = url.split(",", 1)
                if len(parts) == 2:
                    header, data = parts
                    if "base64" in header:
                        image_bytes = base64.b64decode(data)
                        # Extract media type from header
                        if ":" in header and ";" in header:
                            media_type = header.split(":")[1].split(";")[0]

        if image_bytes:
            # Map media type to Bedrock format
            format_map = {
                "image/jpeg": "jpeg",
                "image/jpg": "jpeg",
                "image/png": "png",
                "image/gif": "gif",
                "image/webp": "webp",
            }
            img_format = format_map.get(media_type, "jpeg")

            return {
                "image": {
                    "format": img_format,
                    "source": {"bytes": image_bytes},
                }
            }

        return None

    def _convert_file_content(
        self, content: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Convert file content to Bedrock format (PDF documents)."""
        file_info = content.get("file", {})
        file_bytes = None
        media_type = file_info.get("media_type", "application/pdf")

        if "content" in file_info:
            file_content = file_info["content"]
            if isinstance(file_content, bytes):
                file_bytes = file_content
            elif isinstance(file_content, str):
                file_bytes = base64.b64decode(file_content)

        if file_bytes and media_type == "application/pdf":
            # Bedrock requires alphanumeric filenames (no dots)
            # Remove extension and sanitize
            filename = file_info.get("filename", "document")
            # Remove extension and replace dots/special chars with hyphens
            import re

            sanitized_name = re.sub(r"\.[^.]+$", "", filename)  # Remove ext
            sanitized_name = re.sub(
                r"[^a-zA-Z0-9\s\-\(\)\[\]]", "-", sanitized_name
            )
            sanitized_name = re.sub(
                r"\s+", " ", sanitized_name
            )  # Single spaces
            if not sanitized_name:
                sanitized_name = "document"

            return {
                "document": {
                    "format": "pdf",
                    "name": sanitized_name,
                    "source": {"bytes": file_bytes},
                }
            }

        return None

    def _convert_tools(self, tools: list[dict | Tool]) -> list[dict[str, Any]]:
        """Convert tools to Bedrock format."""
        bedrock_tools = []

        for tool in tools:
            if isinstance(tool, dict):
                tool_type = tool.get("type")
                if tool_type == "function":
                    bedrock_tools.append(
                        {
                            "toolSpec": {
                                "name": tool.get("name"),
                                "description": tool.get("description", ""),
                                "inputSchema": {
                                    "json": tool.get("parameters", {})
                                },
                            }
                        }
                    )
            elif hasattr(tool, "type") and tool.type == "function":
                func = getattr(tool, "function", None) or tool
                bedrock_tools.append(
                    {
                        "toolSpec": {
                            "name": getattr(func, "name", ""),
                            "description": getattr(func, "description", ""),
                            "inputSchema": {
                                "json": getattr(func, "parameters", {})
                            },
                        }
                    }
                )

        return bedrock_tools

    def _convert_tool_choice(
        self, tool_choice: dict | ToolChoice
    ) -> dict[str, Any]:
        """Convert tool choice to Bedrock format."""
        if isinstance(tool_choice, dict):
            choice_type = tool_choice.get("type")
            if choice_type == "auto":
                return {"auto": {}}
            elif choice_type == "none":
                return {"none": {}}
            elif choice_type == "required":
                return {"any": {}}
            elif choice_type == "function":
                func = tool_choice.get("function", {})
                return {"tool": {"name": func.get("name")}}
        elif isinstance(tool_choice, str):
            if tool_choice == "auto":
                return {"auto": {}}
            elif tool_choice == "none":
                return {"none": {}}
            elif tool_choice == "required":
                return {"any": {}}

        return {"auto": {}}

    def _convert_stream_event(
        self,
        event: dict[str, Any],
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        json_schema_mode: bool = False,
    ) -> list[TextGenerationStreamEvent]:
        """Convert Bedrock streaming event to unified format."""
        results: list[TextGenerationStreamEvent] = []

        # Content block start - captures tool name
        if "contentBlockStart" in event:
            block_start = event["contentBlockStart"]
            content_block_index = block_start.get("contentBlockIndex", 0)
            start_data = block_start.get("start", {})

            # Tool use start
            if "toolUse" in start_data:
                tool_use = start_data["toolUse"]
                tool_id = tool_use.get("toolUseId", "")
                tool_name = tool_use.get("name", "")

                # Store tool info in accumulated_tool_calls
                accumulated_tool_calls[content_block_index] = {
                    "id": tool_id,
                    "name": tool_name,
                    "arguments": "",
                }

        # Content block delta
        if "contentBlockDelta" in event:
            block_delta = event["contentBlockDelta"]
            content_block_index = block_delta.get("contentBlockIndex", 0)
            delta = block_delta.get("delta", {})

            # Text delta
            if "text" in delta:
                text = delta["text"]
                if json_schema_mode:
                    # In JSON schema mode, tool input is text
                    results.append(
                        ResponseFunctionCallArgumentsDeltaEvent(delta=text)
                    )
                else:
                    results.append(ResponseOutputTextDeltaEvent(delta=text))

            # Reasoning delta (for Claude models with thinking)
            if "reasoningContent" in delta:
                reasoning_text = delta["reasoningContent"].get("text", "")
                if reasoning_text:
                    results.append(
                        ResponseReasoningTextDeltaEvent(delta=reasoning_text)
                    )

            # Tool use delta - contains arguments
            if "toolUse" in delta:
                tool_use = delta["toolUse"]
                input_delta = tool_use.get("input", "")
                if input_delta:
                    # Update accumulated tool call with arguments
                    if content_block_index in accumulated_tool_calls:
                        accumulated_tool_calls[content_block_index][
                            "arguments"
                        ] += input_delta

                    results.append(
                        ResponseFunctionCallArgumentsDeltaEvent(
                            delta=input_delta,
                            output_index=content_block_index,
                            item_id=accumulated_tool_calls.get(
                                content_block_index, {}
                            ).get("id"),
                        )
                    )

        return results

    def _convert_result(
        self,
        response: dict[str, Any],
        model: str,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Convert Bedrock response to unified TextGenerationResult."""
        output_items: list[OutputItem] = []
        message_content: list[OutputText] = []
        reasoning_content: list[OutputReasoningContentText] = []
        function_calls: list[FunctionCall] = []

        # Process output content
        output = response.get("output", {})
        message = output.get("message", {})
        content_blocks = message.get("content", [])

        for block in content_blocks:
            if "text" in block:
                text = block["text"]
                message_content.append(OutputText(text=text))

            elif "reasoningContent" in block:
                reasoning_text = block["reasoningContent"].get("text", "")
                if reasoning_text:
                    reasoning_content.append(
                        OutputReasoningContentText(text=reasoning_text)
                    )

            elif "toolUse" in block:
                tool_use = block["toolUse"]
                tool_name = tool_use.get("name", "")
                tool_input = tool_use.get("input", {})
                tool_id = tool_use.get("toolUseId", "")

                # Check if this is JSON schema tool
                if (
                    json_schema_mode
                    and json_schema_tool_name
                    and tool_name == json_schema_tool_name
                ):
                    json_text = json.dumps(tool_input)
                    message_content.append(OutputText(text=json_text))
                else:
                    function_calls.append(
                        FunctionCall(
                            name=tool_name,
                            arguments=tool_input,
                            call_id=tool_id,
                            id=tool_id,
                        )
                    )

        # Add reasoning output if present
        if reasoning_content:
            output_items.append(
                OutputReasoning(
                    content=reasoning_content,
                    status="completed",
                )
            )

        # Add message output if present
        if message_content:
            output_items.append(
                OutputMessage(
                    role="assistant",
                    content=message_content,
                    status="completed",
                )
            )

        # Add function calls
        for fc in function_calls:
            output_items.append(fc)

        # Usage
        usage_obj: Usage | None = None
        usage = response.get("usage", {})
        if usage:
            input_tokens = usage.get("inputTokens", 0)
            output_tokens = usage.get("outputTokens", 0)
            usage_obj = Usage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
            )

        # Determine status
        stop_reason = response.get("stopReason")
        status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        if stop_reason == "max_tokens":
            status = "incomplete"
        elif stop_reason in ["end_turn", "stop_sequence", "tool_use"]:
            status = "completed"

        return TextGenerationResult(
            id=response.get("ResponseMetadata", {}).get("RequestId"),
            model=model,
            status=status,
            output=output_items,
            usage=usage_obj,
        )

    def _build_final_result(
        self,
        model: str,
        accumulated_text: str,
        accumulated_reasoning: str,
        accumulated_tool_calls: dict[int, dict],
        usage_info: dict[str, Any],
        stop_reason: str | None,
        json_schema_mode: bool = False,
        json_schema_tool_name: str | None = None,
    ) -> TextGenerationResult:
        """Build final result from accumulated stream data."""
        output_items: list[OutputItem] = []

        # Add reasoning if present
        if accumulated_reasoning:
            output_items.append(
                OutputReasoning(
                    content=[
                        OutputReasoningContentText(text=accumulated_reasoning)
                    ],
                    status="completed",
                )
            )

        # Add text message if present
        if accumulated_text:
            output_items.append(
                OutputMessage(
                    role="assistant",
                    content=[OutputText(text=accumulated_text)],
                    status="completed",
                )
            )

        # Add function calls
        for idx, tc in accumulated_tool_calls.items():
            try:
                arguments = json.loads(tc.get("arguments", "{}"))
            except (json.JSONDecodeError, TypeError):
                arguments = {}

            # Check if JSON schema mode
            if (
                json_schema_mode
                and json_schema_tool_name
                and tc.get("name") == json_schema_tool_name
            ):
                output_items.append(
                    OutputMessage(
                        role="assistant",
                        content=[OutputText(text=tc.get("arguments", "{}"))],
                        status="completed",
                    )
                )
            else:
                output_items.append(
                    FunctionCall(
                        name=tc.get("name", ""),
                        arguments=arguments,
                        call_id=tc.get("id"),
                        id=tc.get("id"),
                    )
                )

        # Usage
        usage_obj: Usage | None = None
        if usage_info:
            input_tokens = usage_info.get("inputTokens", 0)
            output_tokens = usage_info.get("outputTokens", 0)
            usage_obj = Usage(
                input_tokens=input_tokens,
                output_tokens=output_tokens,
                total_tokens=input_tokens + output_tokens,
            )

        # Status
        status: Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ] = "completed"
        if stop_reason == "max_tokens":
            status = "incomplete"

        return TextGenerationResult(
            id=None,
            model=model,
            status=status,
            output=output_items,
            usage=usage_obj,
        )
