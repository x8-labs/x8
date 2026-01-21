from typing import Any, AsyncIterator, Iterator, Mapping, cast

from x8.core import Response

from .._models import (
    InputItem,
    OutputItem,
    Reasoning,
    ResponseText,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
)
from .openai import OpenAI


class Groq(OpenAI):
    """Groq provider using the OpenAI-compatible Responses API.

    Groq provides extremely fast inference with an OpenAI-compatible
    Responses API, so this provider inherits from OpenAI and sets
    Groq-specific defaults.

    Note: GPT-OSS models have different reasoning behavior:
    - They always produce reasoning output even with `effort: "none"`
    - When `effort: "none"` is specified, we filter out reasoning
    """

    def __init__(
        self,
        api_key: str | None = None,
        base_url: str | None = "https://api.groq.com/openai/v1",
        timeout: float | None = None,
        max_retries: int | None = None,
        default_headers: Mapping[str, str] | None = None,
        default_query: Mapping[str, object] | None = None,
        model: str = "openai/gpt-oss-120b",
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                Groq API key.
            base_url:
                Groq API base url.
            timeout:
                Timeout for client requests.
            max_retries:
                Maximum number of retries for failed requests.
            default_headers:
                Default headers to include in every request.
            default_query:
                Default query parameters to include in every request.
            model:
                Groq model to use for text generation.
                Available models: openai/gpt-oss-120b,
                llama-3.3-70b-versatile, mixtral-8x7b-32768, gemma2-9b-it,
                meta-llama/llama-4-scout-17b-16e-instruct.
            nparams:
                Native params for OpenAI-compatible client.
        """
        super().__init__(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
            max_retries=max_retries,
            default_headers=default_headers,
            default_query=default_query,
            model=model,
            nparams=nparams,
            **kwargs,
        )

    def _convert_reasoning_for_groq(
        self, reasoning: dict | Reasoning | None
    ) -> dict | None:
        """Convert reasoning parameter to Groq-compatible format.

        Groq doesn't support:
        - `reasoning.summary` field
        - `effort: "none"` (use no reasoning parameter instead)
        """
        if reasoning is None:
            return None

        reasoning_dict = (
            reasoning if isinstance(reasoning, dict) else reasoning.to_dict()
        )

        # Remove unsupported 'summary' field
        groq_reasoning = {
            k: v for k, v in reasoning_dict.items() if k != "summary"
        }

        # If only effort="none" remains, return None (don't pass reasoning)
        if groq_reasoning.get("effort") == "none":
            return None

        return groq_reasoning if groq_reasoning else None

    def _should_filter_reasoning(
        self, reasoning: dict | Reasoning | None
    ) -> bool:
        """Check if reasoning output should be filtered from results.

        GPT-OSS models always produce reasoning output. We filter it out unless
        the user explicitly requests reasoning with effort != "none".
        """
        if reasoning is None:
            # No reasoning requested, filter out reasoning from the model
            return True
        reasoning_dict = (
            reasoning if isinstance(reasoning, dict) else reasoning.to_dict()
        )
        effort = reasoning_dict.get("effort")
        # Filter if effort is "none" or not specified
        return effort is None or effort == "none"

    def _filter_reasoning_from_result(
        self, result: TextGenerationResult
    ) -> TextGenerationResult:
        """Filter reasoning items from the result output."""
        if result.output:
            filtered_output: list[OutputItem] = [
                item for item in result.output if item.type != "reasoning"
            ]
            result.output = filtered_output
        return result

    def _filter_stream_event(
        self, event: Response[TextGenerationStreamEvent]
    ) -> Response[TextGenerationStreamEvent] | None:
        """Filter reasoning-related events from stream.

        - Skip reasoning text delta events
        - Filter reasoning from completed event's response
        """
        result = event.result
        event_type = result.type

        # Skip reasoning text deltas
        if event_type in (
            "response.reasoning_text.delta",
            "response.reasoning_text.done",
            "response.reasoning_summary_text.delta",
            "response.reasoning_summary_text.done",
            "response.reasoning_summary_part.added",
            "response.reasoning_summary_part.done",
        ):
            return None

        # Filter reasoning from completed event's response
        if event_type == "completed" and hasattr(result, "response"):
            self._filter_reasoning_from_result(result.response)

        return event

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
        filter_reasoning = self._should_filter_reasoning(reasoning)
        groq_reasoning = self._convert_reasoning_for_groq(reasoning)

        result = super().generate(
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
            reasoning=groq_reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )

        if filter_reasoning:
            if stream:
                # Wrap iterator to filter reasoning events
                def _filtered_stream() -> (
                    Iterator[Response[TextGenerationStreamEvent]]
                ):
                    for event in result:  # type: ignore[union-attr]
                        filtered = self._filter_stream_event(
                            cast(Response[TextGenerationStreamEvent], event)
                        )
                        if filtered is not None:
                            yield filtered

                return _filtered_stream()
            else:
                self._filter_reasoning_from_result(
                    cast(Response[TextGenerationResult], result).result
                )

        return result

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
        filter_reasoning = self._should_filter_reasoning(reasoning)
        groq_reasoning = self._convert_reasoning_for_groq(reasoning)

        result = await super().agenerate(
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
            reasoning=groq_reasoning,
            stream=stream,
            nconfig=nconfig,
            **kwargs,
        )

        if filter_reasoning:
            if stream:
                # Wrap async iterator to filter reasoning events
                async def _filtered_astream() -> (
                    AsyncIterator[Response[TextGenerationStreamEvent]]
                ):
                    async for event in result:  # type: ignore[union-attr]
                        filtered = self._filter_stream_event(
                            cast(Response[TextGenerationStreamEvent], event)
                        )
                        if filtered is not None:
                            yield filtered

                return _filtered_astream()
            else:
                self._filter_reasoning_from_result(
                    cast(Response[TextGenerationResult], result).result
                )

        return result
