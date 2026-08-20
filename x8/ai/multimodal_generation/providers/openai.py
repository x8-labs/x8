import base64
import json
from typing import Any, AsyncIterator, Iterator, Literal, Mapping

import openai

from x8.ai._common.openai_provider import OpenAIProvider
from x8.content.image import ImageData
from x8.core import Response
from x8.core.exceptions import BadRequestError

from .._models import (
    ErrorDetail,
    FunctionCall,
    ImageGenerationCall,
    InputItem,
    MultimodalGenerationResult,
    MultimodalGenerationStreamEvent,
    OutputImage,
    OutputItem,
    OutputMessage,
    OutputMessageContent,
    OutputReasoning,
    OutputReasoningContentText,
    OutputText,
    Reasoning,
    Refusal,
    ResponseCompletedEvent,
    ResponseFailedEvent,
    ResponseImagePartialEvent,
    ResponseOutputTextDeltaEvent,
    ResponseText,
    Tool,
    ToolChoice,
    Usage,
    WebSearchCall,
)


class OpenAI(OpenAIProvider):
    def __init__(
        self,
        model: str | None = "gpt-4.1",
        api_key: str | None = None,
        organization: str | None = None,
        project: str | None = None,
        base_url: str | None = None,
        websocket_base_url: str | None = None,
        webhook_secret: str | None = None,
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        nparams: dict | None = None,
        **kwargs: Any,
    ):
        super().__init__(
            model=model,
            api_key=api_key,
            organization=organization,
            project=project,
            base_url=base_url,
            websocket_base_url=websocket_base_url,
            webhook_secret=webhook_secret,
            timeout=timeout,
            max_retries=max_retries,
            default_headers=default_headers,
            default_query=default_query,
            nparams=nparams,
            **kwargs,
        )

    def generate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        modalities: list[Literal["text", "image"]] | None = None,
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
        Response[MultimodalGenerationResult]
        | Iterator[Response[MultimodalGenerationStreamEvent]]
    ):
        args = self._convert_generate_args(
            input=input,
            model=model,
            modalities=modalities,
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
                response = self._client.responses.create(**args)
                return Response(result=self._convert_result(response))

            def _stream_iter() -> (
                Iterator[Response[MultimodalGenerationStreamEvent]]
            ):
                response = self._client.responses.create(**args)
                for event in response:
                    converted = self._convert_stream_event(event)
                    if converted is None:
                        continue
                    yield Response(result=converted)

            return _stream_iter()
        except openai.BadRequestError as e:
            raise BadRequestError(str(e.message)) from e

    async def agenerate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        model: str | None = None,
        modalities: list[Literal["text", "image"]] | None = None,
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
        Response[MultimodalGenerationResult]
        | AsyncIterator[Response[MultimodalGenerationStreamEvent]]
    ):
        args = self._convert_generate_args(
            input=input,
            model=model,
            modalities=modalities,
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
                response = await self._aclient.responses.create(**args)
                return Response(result=self._convert_result(response))

            async def _poll_aiter() -> (
                AsyncIterator[Response[MultimodalGenerationStreamEvent]]
            ):
                response = await self._aclient.responses.create(**args)
                async for event in response:
                    converted = self._convert_stream_event(event)
                    if converted is None:
                        continue
                    yield Response(result=converted)

            return _poll_aiter()
        except openai.BadRequestError as e:
            raise BadRequestError(str(e.message)) from e

    def _convert_stream_event(
        self, event: Any
    ) -> MultimodalGenerationStreamEvent | None:
        e = event.model_dump()
        t = e.get("type")
        seq = e.get("sequence_number")
        if t == "response.output_text.delta":
            return ResponseOutputTextDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta") or "",
            )
        if t == "response.image_generation_call.partial_image":
            return ResponseImagePartialEvent(
                sequence_number=seq,
                content=e.get("partial_image_b64") or "",
                partial_image_index=e.get("partial_image_index"),
            )
        if t == "response.completed":
            return ResponseCompletedEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
            )
        if t == "response.failed":
            return ResponseFailedEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
            )
        return None

    def _convert_result(self, response: Any) -> MultimodalGenerationResult:
        r: dict = response.model_dump()

        output_items: list[OutputItem] | None = None
        output_images: list[OutputImage] = []
        raw_output = r.get("output")
        if isinstance(raw_output, list):
            output_items = []
            for item in raw_output:
                t = item.get("type")
                if t == "image_generation_call":
                    call = ImageGenerationCall.from_dict(item)
                    if call.result:
                        output_images.append(
                            OutputImage(
                                image=ImageData(
                                    source="inline",
                                    content=call.result,
                                    media_type="image/png",
                                )
                            )
                        )
                else:
                    converted = self._convert_output_item(item)
                    if converted is not None:
                        output_items.append(converted)

            if output_images:
                image_content: list[OutputMessageContent] = [*output_images]
                output_items.insert(
                    0,
                    OutputMessage(
                        role="assistant",
                        content=image_content,
                        status="completed",
                    ),
                )

        usage_obj: Usage | None = None
        raw_usage = r.get("usage")
        if isinstance(raw_usage, dict):
            usage_obj = Usage.from_dict(raw_usage)

        error_obj: ErrorDetail | None = None
        raw_error = r.get("error")
        if isinstance(raw_error, dict):
            error_obj = ErrorDetail(
                code=raw_error.get("code"),
                message=raw_error.get("message"),
            )

        return MultimodalGenerationResult(
            id=r.get("id"),
            model=r.get("model"),
            created_at=r.get("created_at"),
            status=r.get("status"),
            error=error_obj,
            output=output_items,
            usage=usage_obj,
        )

    def _convert_output_item(self, item: Any) -> OutputItem:
        t = item.get("type")
        if t == "message":
            return OutputMessage.from_dict(item)
        if t == "function_call":
            arguments = item.get("arguments")
            if isinstance(arguments, str):
                try:
                    item = {**item, "arguments": json.loads(arguments)}
                except json.JSONDecodeError:
                    pass
            return FunctionCall.from_dict(item)
        if t == "reasoning":
            return OutputReasoning.from_dict(item)
        if t == "web_search_call":
            return WebSearchCall.from_dict(item)
        raise BadRequestError(f"Unknown output item type: {t}")

    def _convert_content_part(self, part: Any) -> OutputMessageContent:
        t = part.get("type")
        if t == "output_text":
            return OutputText.from_dict(part)
        if t == "refusal":
            return Refusal.from_dict(part)
        if t == "reasoning_text":
            return OutputReasoningContentText.from_dict(part)
        raise BadRequestError(f"Unknown content part type: {t}")

    def _convert_generate_args(
        self,
        input: str | list[dict[str, Any] | InputItem],
        model: str | None = None,
        modalities: list[Literal["text", "image"]] | None = None,
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

        if isinstance(input, str):
            args["input"] = input
        else:
            items: list[Any] = []
            for it in input:
                if isinstance(it, dict):
                    items.append(self._convert_input_item(it))
                else:
                    items.append(self._convert_input_item(it.to_dict()))
            args["input"] = items

        if instructions is not None:
            args["instructions"] = instructions
        if temperature is not None:
            args["temperature"] = temperature
        if top_p is not None:
            args["top_p"] = top_p
        if max_output_tokens is not None:
            args["max_output_tokens"] = max_output_tokens
        if text is not None:
            args["text"] = text if isinstance(text, dict) else text.to_dict()
        if parallel_tool_calls is not None:
            args["parallel_tool_calls"] = parallel_tool_calls
        if max_tool_calls is not None:
            args["max_tool_calls"] = max_tool_calls
        if stream is not None:
            args["stream"] = stream
        if reasoning is not None:
            args["reasoning"] = (
                reasoning
                if isinstance(reasoning, dict)
                else reasoning.to_dict()
            )

        tool_list: list[Any] = []
        if tools is not None:
            for t in tools:
                tool_list.append(t if isinstance(t, dict) else t.to_dict())

        requested_modalities = modalities or ["text"]
        if "image" in requested_modalities:
            tool_list.append({"type": "image_generation"})

        if tool_list:
            args["tools"] = tool_list

        if tool_choice is not None:
            if isinstance(tool_choice, str):
                args["tool_choice"] = tool_choice
            elif isinstance(tool_choice, dict):
                args["tool_choice"] = tool_choice
            else:
                args["tool_choice"] = tool_choice.to_dict()

        if nconfig:
            args.update(nconfig)
        return args

    def _convert_input_item(self, item: dict[str, Any]) -> dict[str, Any]:
        item_type = item.get("type")

        if item_type == "message":
            content = item.get("content")
            if isinstance(content, list):
                converted_content = [
                    self._convert_input_content(p) for p in content
                ]
                return {**item, "content": converted_content}

        if item_type == "function_call":
            arguments = item.get("arguments")
            if isinstance(arguments, dict):
                arguments = json.dumps(arguments)
            return {
                "type": "function_call",
                "id": item.get("id"),
                "call_id": item.get("call_id"),
                "name": item.get("name"),
                "arguments": arguments,
            }

        if item_type == "function_call_output":
            return {
                "type": "function_call_output",
                "call_id": item.get("call_id"),
                "output": item.get("output"),
                "id": item.get("id"),
                "status": item.get("status"),
            }

        return item

    def _convert_input_content(self, part: dict[str, Any]) -> dict[str, Any]:
        part_type = part.get("type")

        if part_type == "input_image":
            image = part.get("image")
            if image is not None and hasattr(image, "to_dict"):
                image = image.to_dict()
            if isinstance(image, dict):
                content = image.get("content")
                if isinstance(content, (bytes, bytearray)):
                    media_type = image.get("media_type", "image/jpeg")
                    b64_data = base64.b64encode(content).decode("utf-8")
                    return {
                        "type": "input_image",
                        "image_url": f"data:{media_type};base64,{b64_data}",
                        "detail": part.get("detail", "auto"),
                    }
                if isinstance(content, str):
                    media_type = image.get("media_type", "image/jpeg")
                    return {
                        "type": "input_image",
                        "image_url": f"data:{media_type};base64,{content}",
                        "detail": part.get("detail", "auto"),
                    }
                source = image.get("source")
                uri = image.get("uri")
                if source == "uri" and isinstance(uri, str):
                    return {
                        "type": "input_image",
                        "image_url": uri,
                        "detail": part.get("detail", "auto"),
                    }

        if part_type == "input_file":
            file = part.get("file")
            if file is not None and hasattr(file, "to_dict"):
                file = file.to_dict()
            if isinstance(file, dict):
                content = file.get("content")
                if isinstance(content, (bytes, bytearray)):
                    b64_data = base64.b64encode(content).decode("utf-8")
                    return {
                        "type": "input_file",
                        "filename": file.get("filename", "document.pdf"),
                        "file_data": f"data:application/pdf;base64,{b64_data}",
                    }
                if isinstance(content, str):
                    return {
                        "type": "input_file",
                        "filename": file.get("filename", "document.pdf"),
                        "file_data": f"data:application/pdf;base64,{content}",
                    }
                source = file.get("source")
                uri = file.get("uri")
                if source == "uri" and isinstance(uri, str):
                    if uri.startswith("file-"):
                        return {
                            "type": "input_file",
                            "file_id": uri,
                        }
                    return {
                        "type": "input_file",
                        "file_url": uri,
                    }

        return part
