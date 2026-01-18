from __future__ import annotations

from typing import Any, Literal

from x8.content.file import FileData
from x8.content.image import ImageData
from x8.core import DataModel
from x8.core.data_model import DataModelField


class ResponseFormatText(DataModel):
    type: Literal["text"] = "text"


class ResponseFormatJSONSchema(DataModel):
    type: Literal["json_schema"] = "json_schema"
    name: str
    schema_: dict[str, Any] = DataModelField(name="schema")
    description: str | None = None
    strict: bool | None = None


class ResponseText(DataModel):
    format: ResponseFormatText | ResponseFormatJSONSchema
    versbosity: Literal["low", "medium", "high"] | None = None


class Function(DataModel):
    type: Literal["function"] = "function"
    name: str
    description: str | None = None
    parameters: dict[str, Any] | None = None
    strict: bool | None = None


class WebSearchFilters(DataModel):
    allowed_domains: list[str] | None = None


class WebSearchUserLocation(DataModel):
    city: str | None = None
    country: str | None = None
    region: str | None = None
    timezone: str | None = None
    type: Literal["approximate"] = "approximate"


class WebSearch(DataModel):
    type: Literal["web_search"] = "web_search"
    search_context_size: Literal["short", "medium", "long"] = "medium"
    filters: WebSearchFilters | None = None
    user_location: WebSearchUserLocation | None = None


Tool = Function | WebSearch


class ToolChoiceFunction(DataModel):
    type: Literal["function"] = "function"
    name: str


class AllowedTools(DataModel):
    type: Literal["allowed_tools"] = "allowed_tools"
    mode: Literal["none", "auto", "required"] = "auto"
    tools: list[ToolChoiceFunction] | None = None


ToolChoice = (
    Literal["none", "auto", "required"] | ToolChoiceFunction | AllowedTools
)


class InputImage(DataModel):
    type: Literal["input_image"] = "input_image"
    detail: Literal["low", "high", "auto"] = "auto"
    image: str | ImageData


class InputText(DataModel):
    type: Literal["input_text"] = "input_text"
    text: str


class InputFile(DataModel):
    type: Literal["input_file"] = "input_file"
    file: str | FileData


InputMessageContent = InputText | InputImage | InputFile


class InputMessage(DataModel):
    type: Literal["message"] = "message"
    role: Literal["user", "system", "assistant", "developer"] = "user"
    content: str | list[InputMessageContent]


class FunctionCall(DataModel):
    type: Literal["function_call"] = "function_call"
    call_id: str | None = None
    name: str
    id: str | None = None
    arguments: str | dict[str, Any] | None = None
    status: Literal["completed", "in_progress", "incomplete"] | None = None
    thought_signature: str | bytes | None = None


class FunctionCallOutput(DataModel):
    type: Literal["function_call_output"] = "function_call_output"
    call_id: str | None = None
    output: str | dict[str, Any] | None = None
    id: str | None = None
    status: Literal["completed", "in_progress", "incomplete"] | None = None
    name: str | None = None


class OutputText(DataModel):
    type: Literal["output_text"] = "output_text"
    text: str
    annotations: list[dict[str, Any]] | None = None


class Refusal(DataModel):
    type: Literal["refusal"] = "refusal"
    refusal: str


class OutputReasoningSummaryText(DataModel):
    type: Literal["summary_text"] = "summary_text"
    text: str


class OutputReasoningContentText(DataModel):
    type: Literal["reasoning_text"] = "reasoning_text"
    text: str


class OutputReasoning(DataModel):
    id: str | None = None
    type: Literal["reasoning"] = "reasoning"
    summary: list[OutputReasoningSummaryText] | None = None
    content: list[OutputReasoningContentText] | None = None
    status: Literal["completed", "in_progress", "incomplete"] | None = (
        "completed"
    )


OutputMessageContent = OutputText | Refusal | OutputReasoningContentText


class OutputMessage(DataModel):
    type: Literal["message"] = "message"
    role: Literal["assistant"] = "assistant"
    status: Literal["completed", "in_progress", "incomplete"] | None = (
        "completed"
    )
    content: list[OutputMessageContent]
    id: str | None = None


class WebSearchAction(DataModel):
    type: Literal["search"] = "search"
    query: str | None = None
    queries: list[str] | None = None
    sources: list[dict[str, Any]] | None = None


class WebSearchCall(DataModel):
    type: Literal["web_search_call"] = "web_search_call"
    id: str | None = None
    status: Literal["completed", "in_progress", "incomplete"] | None = None
    action: WebSearchAction | None = None


InputItem = (
    InputMessage
    | OutputMessage
    | FunctionCall
    | FunctionCallOutput
    | OutputReasoning
)
OutputItem = OutputMessage | FunctionCall | OutputReasoning | WebSearchCall


class ErrorDetail(DataModel):
    code: str | None = None
    message: str | None = None


class Usage(DataModel):
    input_tokens: int
    input_tokens_details: dict[str, int] | None = None
    output_tokens: int
    output_tokens_details: dict[str, int] | None = None
    total_tokens: int


class Reasoning(DataModel):
    effort: Literal["none", "low", "medium", "high"] | None = None
    summary: Literal["auto", "concise", "detailed"] | None = None


class TextGenerationResult(DataModel):
    id: str | None = None
    model: str | None = None
    created_at: int | None = None
    status: (
        Literal[
            "completed",
            "failed",
            "in_progress",
            "cancelled",
            "queued",
            "incomplete",
        ]
        | None
    ) = "completed"
    error: ErrorDetail | None = None
    output: list[OutputItem] | None = None
    usage: Usage | None = None


class StreamEvent(DataModel):
    type: str
    sequence_number: int | None = None


class ResponseQueuedEvent(StreamEvent):
    type: Literal["queued"] = "queued"
    response: TextGenerationResult


class ResponseCreatedEvent(StreamEvent):
    type: Literal["created"] = "created"
    response: TextGenerationResult


class ResponseInProgressEvent(StreamEvent):
    type: Literal["in_progress"] = "in_progress"
    response: TextGenerationResult


class ResponseCompletedEvent(StreamEvent):
    type: Literal["completed"] = "completed"
    response: TextGenerationResult


class ResponseFailedEvent(StreamEvent):
    type: Literal["failed"] = "failed"
    response: TextGenerationResult


class ResponseIncompleteEvent(StreamEvent):
    type: Literal["incomplete"] = "incomplete"
    response: TextGenerationResult


class ResponseOutputTextDeltaEvent(StreamEvent):
    type: Literal["output_text_delta"] = "output_text_delta"
    delta: str
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseOutputTextDoneEvent(StreamEvent):
    type: Literal["output_text_done"] = "output_text_done"
    text: str
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseOutputItemAddedEvent(StreamEvent):
    type: Literal["output_item_added"] = "output_item_added"
    item: OutputItem
    output_index: int | None = None


class ResponseOutputItemDoneEvent(StreamEvent):
    type: Literal["output_item_done"] = "output_item_done"
    item: OutputItem
    output_index: int | None = None


class ResponseContentPartAddedEvent(StreamEvent):
    type: Literal["content_part_added"] = "content_part_added"
    part: OutputMessageContent
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseContentPartDoneEvent(StreamEvent):
    type: Literal["content_part_done"] = "content_part_done"
    part: OutputMessageContent
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseRefusalDeltaEvent(StreamEvent):
    type: Literal["refusal_delta"] = "refusal_delta"
    delta: str
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseRefusalDoneEvent(StreamEvent):
    type: Literal["refusal_done"] = "refusal_done"
    refusal: str
    item_id: str | None = None
    output_index: int | None = None
    content_index: int | None = None


class ResponseFunctionCallArgumentsDeltaEvent(StreamEvent):
    type: Literal["function_call_arguments_delta"] = (
        "function_call_arguments_delta"
    )
    delta: str
    item_id: str | None = None
    output_index: int | None = None


class ResponseFunctionCallArgumentsDoneEvent(StreamEvent):
    type: Literal["function_call_arguments_done"] = (
        "function_call_arguments_done"
    )
    name: str | None = None
    arguments: str
    item_id: str | None = None
    output_index: int | None = None


class ResponseReasoningSummaryPartAddedEvent(StreamEvent):
    type: Literal["reasoning_summary_part_added"] = (
        "reasoning_summary_part_added"
    )
    part: OutputReasoningSummaryText
    item_id: str | None = None
    output_index: int | None = None
    summary_index: int | None = None


class ResponseReasoningSummaryPartDoneEvent(StreamEvent):
    type: Literal["reasoning_summary_part_done"] = (
        "reasoning_summary_part_done"
    )
    part: OutputReasoningSummaryText
    item_id: str | None = None
    output_index: int | None = None
    summary_index: int | None = None


class ResponseReasoningSummaryTextDeltaEvent(StreamEvent):
    type: Literal["reasoning_summary_text_delta"] = (
        "reasoning_summary_text_delta"
    )
    delta: str
    item_id: str | None = None
    output_index: int | None = None
    summary_index: int | None = None


class ResponseReasoningSummaryTextDoneEvent(StreamEvent):
    type: Literal["reasoning_summary_text_done"] = (
        "reasoning_summary_text_done"
    )
    text: str
    item_id: str | None = None
    output_index: int | None = None
    summary_index: int | None = None


class ResponseReasoningTextDeltaEvent(StreamEvent):
    type: Literal["reasoning_text_delta"] = "reasoning_text_delta"
    delta: str
    item_id: str | None = None
    content_index: int | None = None
    output_index: int | None = None


class ResponseReasoningTextDoneEvent(StreamEvent):
    type: Literal["reasoning_text_done"] = "reasoning_text_done"
    text: str
    item_id: str | None = None
    content_index: int | None = None
    output_index: int | None = None


class ErrorEvent(StreamEvent):
    type: Literal["error"] = "error"
    code: str | None = None
    message: str | None = None
    param: str | None = None


TextGenerationStreamEvent = (
    ResponseQueuedEvent
    | ResponseCreatedEvent
    | ResponseInProgressEvent
    | ResponseCompletedEvent
    | ResponseFailedEvent
    | ResponseIncompleteEvent
    | ResponseOutputTextDeltaEvent
    | ResponseOutputTextDoneEvent
    | ResponseOutputItemAddedEvent
    | ResponseOutputItemDoneEvent
    | ResponseContentPartAddedEvent
    | ResponseContentPartDoneEvent
    | ResponseRefusalDeltaEvent
    | ResponseRefusalDoneEvent
    | ResponseFunctionCallArgumentsDeltaEvent
    | ResponseFunctionCallArgumentsDoneEvent
    | ResponseReasoningSummaryPartAddedEvent
    | ResponseReasoningSummaryPartDoneEvent
    | ResponseReasoningSummaryTextDeltaEvent
    | ResponseReasoningSummaryTextDoneEvent
    | ResponseReasoningTextDeltaEvent
    | ResponseReasoningTextDoneEvent
    | ErrorEvent
)
