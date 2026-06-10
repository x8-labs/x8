from typing import Any, AsyncIterator, Iterator, Literal, overload

from x8.core import Component, Response, operation

from ._models import (
    InputItem,
    MultimodalGenerationResult,
    MultimodalGenerationStreamEvent,
    Reasoning,
    ResponseText,
    Tool,
    ToolChoice,
)


class MultimodalGeneration(Component):
    @overload
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[MultimodalGenerationResult]:
        raise NotImplementedError

    @overload
    def generate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[MultimodalGenerationStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
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
        raise NotImplementedError

    @overload
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[MultimodalGenerationResult]:
        raise NotImplementedError

    @overload
    async def agenerate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Response[MultimodalGenerationStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
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
        raise NotImplementedError
