from __future__ import annotations

from typing import Any, AsyncIterator, Iterator, Literal

from x8.core import DataModel

# ── Tool definitions ──────────────────────────────────────────────────


class ToolParameter(DataModel):
    """JSON-Schema style parameter definition for a tool."""

    type: str = "object"
    properties: dict[str, Any] | None = None
    required: list[str] | None = None


class Tool(DataModel):
    """A tool that the agent can invoke."""

    name: str
    description: str | None = None
    parameters: dict[str, Any] | ToolParameter | None = None


# ── MCP server definitions ───────────────────────────────────────────


class MCPStdioServer(DataModel):
    """An MCP server that runs as a subprocess (stdio transport)."""

    type: Literal["stdio"] = "stdio"
    command: str
    args: list[str] | None = None
    env: dict[str, str] | None = None


class MCPServer(DataModel):
    """Wrapper that carries a name + config for an MCP server."""

    name: str
    server: MCPStdioServer | dict[str, Any]


# ── Content blocks ───────────────────────────────────────────────────


class TextBlock(DataModel):
    """A block of text produced by the agent."""

    type: Literal["text"] = "text"
    text: str


class ToolUseBlock(DataModel):
    """A tool invocation made by the agent."""

    type: Literal["tool_use"] = "tool_use"
    tool_name: str
    tool_input: dict[str, Any] | None = None
    call_id: str | None = None


class ToolResultBlock(DataModel):
    """The result returned after a tool executes."""

    type: Literal["tool_result"] = "tool_result"
    tool_name: str | None = None
    call_id: str | None = None
    output: str | dict[str, Any] | None = None
    is_error: bool = False


ContentBlock = TextBlock | ToolUseBlock | ToolResultBlock


# ── Messages ─────────────────────────────────────────────────────────


class Message(DataModel):
    """A single message in the agent conversation."""

    role: Literal["user", "assistant", "system", "tool"] = "assistant"
    content: str | list[ContentBlock] | None = None


# ── Usage ────────────────────────────────────────────────────────────


class Usage(DataModel):
    """Token usage information for an agent run."""

    input_tokens: int | None = None
    output_tokens: int | None = None
    total_tokens: int | None = None


# ── Agent run result ─────────────────────────────────────────────────


class AgentResult(DataModel):
    """The final result of an agent run."""

    id: str | None = None
    model: str | None = None
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
    output: list[Message] | None = None
    usage: Usage | None = None
    error: str | None = None


# ── Streaming events ─────────────────────────────────────────────────


class StreamEvent(DataModel):
    """Base streaming event."""

    type: str


class TextDeltaEvent(StreamEvent):
    """Incremental text produced by the agent."""

    type: Literal["text_delta"] = "text_delta"
    delta: str


class TextDoneEvent(StreamEvent):
    """A text block is complete."""

    type: Literal["text_done"] = "text_done"
    text: str


class ToolCallEvent(StreamEvent):
    """The agent is invoking a tool."""

    type: Literal["tool_call"] = "tool_call"
    tool_name: str
    tool_input: dict[str, Any] | None = None
    call_id: str | None = None


class ToolResultEvent(StreamEvent):
    """A tool execution completed."""

    type: Literal["tool_result"] = "tool_result"
    tool_name: str | None = None
    call_id: str | None = None
    output: str | dict[str, Any] | None = None
    is_error: bool = False


class AgentCompletedEvent(StreamEvent):
    """Agent run completed."""

    type: Literal["completed"] = "completed"
    result: AgentResult


class ErrorEvent(StreamEvent):
    """An error occurred during the agent run."""

    type: Literal["error"] = "error"
    code: str | None = None
    message: str | None = None


AgentStreamEvent = (
    TextDeltaEvent
    | TextDoneEvent
    | ToolCallEvent
    | ToolResultEvent
    | AgentCompletedEvent
    | ErrorEvent
)


# ── Session ──────────────────────────────────────────────────────────


class SessionConfig(DataModel):
    """Configuration for creating an agent session."""

    model: str | None = None
    instructions: str | None = None
    tools: list[Tool] | None = None
    mcp_servers: list[MCPServer | MCPStdioServer] | None = None
    allowed_tools: list[str] | None = None
    max_turns: int | None = None
    cwd: str | None = None
    nconfig: dict[str, Any] | None = None


class AgentSession:
    """A persistent, multi-turn agent session.

    Sessions keep the underlying client alive across multiple
    ``send`` / ``asend`` calls, enabling true conversational
    interaction with the agent.

    Use as a context manager (sync or async)::

        async with agent.acreate_session() as session:
            r1 = await session.asend("Hello")
            r2 = await session.asend("Follow up")

        with agent.create_session() as session:
            r1 = session.send("Hello")
            r2 = session.send("Follow up")
    """

    config: SessionConfig
    history: list[Message]

    def __init__(self, config: SessionConfig | None = None):
        self.config = config or SessionConfig()
        self.history = []

    # ── sync API ──────────────────────────────────────────────────

    def send(
        self,
        prompt: str,
        *,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> AgentResult | Iterator[AgentStreamEvent]:
        """Send a message and get a response."""
        raise NotImplementedError

    def close(self) -> None:
        """Close the session and release resources."""
        raise NotImplementedError

    # ── async API ────────────────────────────────────────────────

    async def asend(
        self,
        prompt: str,
        *,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> AgentResult | AsyncIterator[AgentStreamEvent]:
        """Send a message and get a response (async)."""
        raise NotImplementedError

    async def aclose(self) -> None:
        """Close the session and release resources (async)."""
        raise NotImplementedError

    # ── context managers ─────────────────────────────────────────

    def __enter__(self) -> "AgentSession":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    async def __aenter__(self) -> "AgentSession":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()
