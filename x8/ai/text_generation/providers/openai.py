from typing import Any, AsyncIterator, Iterator, Mapping

from x8.ai._common.openai_provider import OpenAIProvider
from x8.core import Response
from x8.core.exceptions import BadRequestError

from .._models import (
    ErrorDetail,
    ErrorEvent,
    FunctionCall,
    InputItem,
    OutputItem,
    OutputMessage,
    OutputMessageContent,
    OutputReasoning,
    OutputReasoningContentText,
    OutputReasoningSummaryText,
    OutputText,
    Reasoning,
    Refusal,
    ResponseCompletedEvent,
    ResponseContentPartAddedEvent,
    ResponseContentPartDoneEvent,
    ResponseCreatedEvent,
    ResponseFailedEvent,
    ResponseFunctionCallArgumentsDeltaEvent,
    ResponseFunctionCallArgumentsDoneEvent,
    ResponseIncompleteEvent,
    ResponseInProgressEvent,
    ResponseOutputItemAddedEvent,
    ResponseOutputItemDoneEvent,
    ResponseOutputTextDeltaEvent,
    ResponseOutputTextDoneEvent,
    ResponseQueuedEvent,
    ResponseReasoningSummaryPartAddedEvent,
    ResponseReasoningSummaryPartDoneEvent,
    ResponseReasoningSummaryTextDeltaEvent,
    ResponseReasoningSummaryTextDoneEvent,
    ResponseReasoningTextDeltaEvent,
    ResponseReasoningTextDoneEvent,
    ResponseRefusalDeltaEvent,
    ResponseRefusalDoneEvent,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
    Usage,
)


class OpenAI(OpenAIProvider):
    def __init__(
        self,
        model: str | None = "gpt-4o",
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
        """Initialize.

        Args:
            model:
                OpenAI model to use for video generation.
            api_key:
                OpenAI API key.
            organization:
                OpenAI organization.
            project:
                OpenAI project.
            base_url:
                OpenAI base url.
            websocket_base_url:
                OpenAI websocket base url.
            webhook_secret:
                OpenAI webhook secret.
            timeout:
                Timeout for client.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            nparams:
                Native params for OpenAI client.
        """

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
            input=input,
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
            response = self._client.responses.create(**args)
            result = self._convert_result(response)
            return Response(result=result)
        else:

            def _stream_iter() -> (
                Iterator[Response[TextGenerationStreamEvent]]
            ):
                response = self._client.responses.create(**args)
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
            input=input,
            model=model,
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
            response = await self._aclient.responses.create(**args)
            result = self._convert_result(response)
            return Response(result=result)
        else:

            async def _poll_aiter() -> (
                AsyncIterator[Response[TextGenerationStreamEvent]]
            ):
                response = await self._aclient.responses.create(**args)
                async for event in response:
                    converted_event = self._convert_stream_event(event)
                    if converted_event is None:
                        continue
                    yield Response(result=converted_event)

            return _poll_aiter()

    def _convert_stream_event(
        self, event: Any
    ) -> TextGenerationStreamEvent | None:
        e = event.model_dump()
        t = e.get("type")
        seq = e.get("sequence_number")
        if t == "response.queued":
            return ResponseQueuedEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
            )
        if t == "response.created":
            return ResponseCreatedEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
            )
        if t == "response.in_progress":
            return ResponseInProgressEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
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
        if t == "response.incomplete":
            return ResponseIncompleteEvent(
                sequence_number=seq,
                response=self._convert_result(event.response),
            )
        if t == "response.output_item.added":
            return ResponseOutputItemAddedEvent(
                sequence_number=seq,
                item=self._convert_output_item(e.get("item")),
                output_index=e.get("output_index"),
            )
        if t == "response.output_item.done":
            return ResponseOutputItemDoneEvent(
                sequence_number=seq,
                item=self._convert_output_item(e.get("item")),
                output_index=e.get("output_index"),
            )
        if t == "response.output_text.delta":
            return ResponseOutputTextDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.output_text.done":
            return ResponseOutputTextDoneEvent(
                sequence_number=seq,
                text=e.get("text"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.content_part.added":
            return ResponseContentPartAddedEvent(
                sequence_number=seq,
                part=self._convert_content_part(e.get("part")),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.content_part.done":
            return ResponseContentPartDoneEvent(
                sequence_number=seq,
                part=self._convert_content_part(e.get("part")),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.refusal.delta":
            return ResponseRefusalDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.refusal.done":
            return ResponseRefusalDoneEvent(
                sequence_number=seq,
                refusal=e.get("refusal"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.function_call_arguments.delta":
            return ResponseFunctionCallArgumentsDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
            )
        if t == "response.function_call_arguments.done":
            return ResponseFunctionCallArgumentsDoneEvent(
                sequence_number=seq,
                name=e.get("name"),
                arguments=e.get("arguments"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
            )
        if t == "response.reasoning_text.delta":
            return ResponseReasoningTextDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.reasoning_text.done":
            return ResponseReasoningTextDoneEvent(
                sequence_number=seq,
                text=e.get("text"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                content_index=e.get("content_index"),
            )
        if t == "response.reasoning_summary_part.added":
            return ResponseReasoningSummaryPartAddedEvent(
                sequence_number=seq,
                part=OutputReasoningSummaryText.from_dict(e.get("part")),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                summary_index=e.get("summary_index"),
            )
        if t == "response.reasoning_summary_part.done":
            return ResponseReasoningSummaryPartDoneEvent(
                sequence_number=seq,
                part=OutputReasoningSummaryText.from_dict(e.get("part")),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                summary_index=e.get("summary_index"),
            )
        if t == "response.reasoning_summary_text.delta":
            return ResponseReasoningSummaryTextDeltaEvent(
                sequence_number=seq,
                delta=e.get("delta"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                summary_index=e.get("summary_index"),
            )
        if t == "response.reasoning_summary_text.done":
            return ResponseReasoningSummaryTextDoneEvent(
                sequence_number=seq,
                text=e.get("text"),
                item_id=e.get("item_id"),
                output_index=e.get("output_index"),
                summary_index=e.get("summary_index"),
            )
        if t == "error":
            return ErrorEvent(
                type="error",
                code=e.get("code"),
                message=e.get("message"),
                param=e.get("param"),
            )
        return None

    def _convert_result(self, response: Any) -> TextGenerationResult:
        r: dict = response.model_dump()

        output_items: list[Any] | None = None
        raw_output = r.get("output")
        if isinstance(raw_output, list):
            output_items = []
            for item in raw_output:
                converted = self._convert_output_item(item)
                if converted is not None:
                    output_items.append(converted)

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

        result = TextGenerationResult(
            id=r.get("id"),
            model=r.get("model"),
            created_at=r.get("created_at"),
            status=r.get("status"),
            error=error_obj,
            output=output_items,
            usage=usage_obj,
        )
        return result

    def _convert_content_part(self, part: Any) -> OutputMessageContent:
        t = part.get("type")
        if t == "output_text":
            return OutputText.from_dict(part)
        if t == "refusal":
            return Refusal.from_dict(part)
        if t == "reasoning_text":
            return OutputReasoningContentText.from_dict(part)
        raise BadRequestError(f"Unknown content part type: {t}")

    def _convert_output_item(self, item: Any) -> OutputItem:
        t = item.get("type")
        if t == "message":
            return OutputMessage.from_dict(item)
        if t == "function_call":
            return FunctionCall.from_dict(item)
        if t == "reasoning":
            return OutputReasoning.from_dict(item)
        raise BadRequestError(f"Unknown output item type: {t}")

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
        if isinstance(input, str):
            args["input"] = input
        else:
            items: list[Any] = []
            for it in input:
                if isinstance(it, dict):
                    items.append(it)
                else:
                    items.append(it.to_dict())
            args["input"] = items

        if instructions is not None:
            args["instructions"] = instructions
        if temperature is not None:
            args["temperature"] = temperature
        if top_p is not None:
            args["top_p"] = top_p
        if max_output_tokens is not None:
            args["max_output_tokens"] = max_output_tokens
        if parallel_tool_calls is not None:
            args["parallel_tool_calls"] = parallel_tool_calls
        if max_tool_calls is not None:
            args["max_tool_calls"] = max_tool_calls
        if stream is not None:
            args["stream"] = stream
        if reasoning is not None:
            if isinstance(reasoning, dict):
                args["reasoning"] = reasoning
            else:
                args["reasoning"] = reasoning.to_dict()

        if tools is not None:
            tool_list: list[Any] = []
            for t in tools:
                if isinstance(t, dict):
                    tool_list.append(t)
                else:
                    tool_list.append(t.to_dict())
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
