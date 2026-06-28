from __future__ import annotations

from typing import Any, Literal

from x8.content.file import FileData
from x8.content.image import ImageData
from x8.content.video import VideoData
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


class InputFile(DataModel):
    type: Literal["input_file"] = "input_file"
    file: str | FileData


class InputText(DataModel):
    type: Literal["input_text"] = "input_text"
    text: str


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


class OutputImage(DataModel):
    type: Literal["output_image"] = "output_image"
    image: ImageData


class OutputVideo(DataModel):
    type: Literal["output_video"] = "output_video"
    video: VideoData


class OutputAudio(DataModel):
    type: Literal["output_audio"] = "output_audio"
    source: str | None = None
    content: str | None = None
    media_type: str | None = None


class ImageGenerationCall(DataModel):
    type: Literal["image_generation_call"] = "image_generation_call"
    id: str
    status: Literal["in_progress", "completed", "generating", "failed"]
    result: str | None = None


class OutputReasoning(DataModel):
    id: str | None = None
    type: Literal["reasoning"] = "reasoning"
    summary: list[OutputReasoningSummaryText] | None = None
    content: list[OutputReasoningContentText] | None = None
    status: Literal["completed", "in_progress", "incomplete"] | None = (
        "completed"
    )


OutputMessageContent = (
    OutputText
    | Refusal
    | OutputReasoningContentText
    | OutputImage
    | OutputVideo
    | OutputAudio
)


class OutputMessage(DataModel):
    type: Literal["message"] = "message"
    role: Literal["assistant"] = "assistant"
    status: Literal["completed", "in_progress", "incomplete"] | None = (
        "completed"
    )
    content: list[OutputMessageContent]
    id: str | None = None


class WebSearchAction(DataModel):
    type: Literal["search", "open_page"] = "search"
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
OutputItem = (
    OutputMessage
    | FunctionCall
    | OutputReasoning
    | WebSearchCall
    | ImageGenerationCall
)


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


class MultimodalGenerationResult(DataModel):
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


class ResponseOutputTextDeltaEvent(StreamEvent):
    type: Literal["output_text_delta"] = "output_text_delta"
    delta: str


class ResponseImagePartialEvent(StreamEvent):
    type: Literal["image_partial"] = "image_partial"
    content: str
    partial_image_index: int | None = None


class ResponseCompletedEvent(StreamEvent):
    type: Literal["completed"] = "completed"
    response: MultimodalGenerationResult


class ResponseFailedEvent(StreamEvent):
    type: Literal["failed"] = "failed"
    response: MultimodalGenerationResult


MultimodalGenerationStreamEvent = (
    ResponseOutputTextDeltaEvent
    | ResponseImagePartialEvent
    | ResponseCompletedEvent
    | ResponseFailedEvent
)
