from typing import Any, AsyncIterator, Iterator, Literal, overload

from x8.core import Component, Response, operation

from ._models import (
    AgentResult,
    AgentSession,
    AgentStreamEvent,
    MCPServer,
    MCPStdioServer,
    SessionConfig,
    Tool,
)


class Agent(Component):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    # ── session ──────────────────────────────────────────────────────

    def create_session(
        self,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AgentSession:
        """Create a persistent, multi-turn agent session (sync)."""
        config = SessionConfig(
            model=model,
            instructions=instructions,
            tools=(
                [
                    t if isinstance(t, Tool) else Tool.from_dict(t)
                    for t in tools
                ]
                if tools
                else None
            ),
            allowed_tools=allowed_tools,
            max_turns=max_turns,
            cwd=cwd,
            nconfig=nconfig,
        )
        provider = self.__provider__
        return provider.create_session(  # type: ignore[attr-defined]
            config=config,
        )

    async def acreate_session(
        self,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AgentSession:
        """Create a persistent, multi-turn agent session (async)."""
        config = SessionConfig(
            model=model,
            instructions=instructions,
            tools=(
                [
                    t if isinstance(t, Tool) else Tool.from_dict(t)
                    for t in tools
                ]
                if tools
                else None
            ),
            allowed_tools=allowed_tools,
            max_turns=max_turns,
            cwd=cwd,
            nconfig=nconfig,
        )
        provider = self.__provider__
        return await provider.acreate_session(  # type: ignore[attr-defined]
            config=config,
        )

    # ── one-shot run ─────────────────────────────────────────────────

    @overload
    def run(
        self,
        prompt: str,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[AgentResult]:
        raise NotImplementedError

    @overload
    def run(
        self,
        prompt: str,
        *,
        stream: Literal[True],
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Iterator[Response[AgentStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
    def run(
        self,
        prompt: str,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[AgentResult] | Iterator[Response[AgentStreamEvent]]:
        raise NotImplementedError

    @overload
    async def arun(
        self,
        prompt: str,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        stream: Literal[False] | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[AgentResult]:
        raise NotImplementedError

    @overload
    async def arun(
        self,
        prompt: str,
        *,
        stream: Literal[True],
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Response[AgentStreamEvent]]:
        raise NotImplementedError

    @operation(
        api={
            "path": "",
            "method": "POST",
            "status": 201,
        }
    )
    async def arun(
        self,
        prompt: str,
        *,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> Response[AgentResult] | AsyncIterator[Response[AgentStreamEvent]]:
        raise NotImplementedError
