from typing import Any, AsyncIterator, Iterator, Literal, overload

from x8.core import Component, Response, operation

from ._models import (
    InputItem,
    Reasoning,
    TextGenerationResult,
    TextGenerationStreamEvent,
    Tool,
    ToolChoice,
)


class TextGeneration(Component):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @overload
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[TextGenerationResult]:
        raise NotImplementedError

    @overload
    def generate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[TextGenerationStreamEvent]]:
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
        raise NotImplementedError

    @overload
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
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[TextGenerationResult]:
        raise NotImplementedError

    @overload
    async def agenerate(
        self,
        input: str | list[dict[str, Any] | InputItem],
        *,
        stream: Literal[True],
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Response[TextGenerationStreamEvent]]:
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
        raise NotImplementedError
