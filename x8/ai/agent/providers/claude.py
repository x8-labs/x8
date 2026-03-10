from typing import Any, AsyncIterator, Iterator

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKClient,
    ResultMessage,
    SystemMessage,
)
from claude_agent_sdk import TextBlock as SdkTextBlock
from claude_agent_sdk import ToolResultBlock as SdkToolResultBlock
from claude_agent_sdk import ToolUseBlock as SdkToolUseBlock

from x8.core import Response
from x8.core._provider import Provider

from .._models import (
    AgentCompletedEvent,
    AgentResult,
    AgentSession,
    AgentStreamEvent,
    MCPServer,
    MCPStdioServer,
    Message,
    SessionConfig,
    TextBlock,
    TextDoneEvent,
    Tool,
    ToolCallEvent,
    ToolResultBlock,
    ToolUseBlock,
)


class Claude(Provider):
    """Provider that wraps the Claude Agent SDK (claude-agent-sdk).

    Uses ``claude_agent_sdk.ClaudeSDKClient`` to create interactive
    sessions with the Claude Code agent.
    """

    api_key: str | None
    model: str | None
    cli_path: str | None
    system_prompt: str | None
    allowed_tools: list[str] | None
    permission_mode: str | None
    max_turns: int | None
    cwd: str | None
    nparams: dict[str, Any] | None

    _init: bool
    _ainit: bool

    def __init__(
        self,
        api_key: str | None = None,
        model: str | None = None,
        cli_path: str | None = None,
        system_prompt: str | None = None,
        allowed_tools: list[str] | None = None,
        permission_mode: str | None = "acceptEdits",
        max_turns: int | None = None,
        cwd: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            api_key:
                Anthropic API key (passed via ANTHROPIC_API_KEY env
                or here).
            model:
                The model to use in the Claude agent
                (optional – uses CLI default).
            cli_path:
                Path to the Claude Code CLI binary.
            system_prompt:
                Default system prompt for the agent.
            allowed_tools:
                Default set of allowed tool names
                (e.g. ["Read", "Write", "Bash"]).
            permission_mode:
                Permission mode for tool calls
                ("acceptEdits", "full", etc.).
            max_turns:
                Default maximum agentic turns per run.
            cwd:
                Default working directory for the agent.
            nparams:
                Extra native parameters forwarded to
                ClaudeAgentOptions.
        """
        self.api_key = api_key
        self.model = model
        self.cli_path = cli_path
        self.system_prompt = system_prompt
        self.allowed_tools = allowed_tools
        self.permission_mode = permission_mode
        self.max_turns = max_turns
        self.cwd = cwd
        self.nparams = nparams
        self._init = False
        self._ainit = False
        super().__init__(**kwargs)

    def __setup__(self, context=None):
        if self._init:
            return
        self._init = True

    async def __asetup__(self, context=None):
        if self._ainit:
            return
        self._ainit = True

    # ── helpers ───────────────────────────────────────────────────────

    def _build_options(
        self,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        mcp_servers: list[dict | MCPServer | MCPStdioServer] | None = None,
        allowed_tools: list[str] | None = None,
        max_turns: int | None = None,
        cwd: str | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> ClaudeAgentOptions:
        """Build a ``ClaudeAgentOptions`` instance."""
        opts: dict[str, Any] = {}

        # System prompt
        prompt = instructions or self.system_prompt
        if prompt:
            opts["system_prompt"] = prompt

        # Allowed tools
        effective_tools = allowed_tools or self.allowed_tools
        if effective_tools:
            opts["allowed_tools"] = effective_tools

        # Max turns
        effective_max_turns = max_turns or self.max_turns
        if effective_max_turns is not None:
            opts["max_turns"] = effective_max_turns

        # Working directory
        effective_cwd = cwd or self.cwd
        if effective_cwd is not None:
            opts["cwd"] = effective_cwd

        # CLI path
        if self.cli_path:
            opts["cli_path"] = self.cli_path

        # Permission mode
        if self.permission_mode:
            opts["permission_mode"] = self.permission_mode

        # MCP servers
        if mcp_servers:
            servers: dict[str, Any] = {}
            for srv in mcp_servers:
                if isinstance(srv, dict):
                    name = srv.get("name", "default")
                    servers[name] = srv.get("server", srv)
                elif isinstance(srv, MCPServer):
                    server = srv.server
                    if hasattr(server, "to_dict"):
                        server = server.to_dict()
                    servers[srv.name] = server
                elif isinstance(srv, MCPStdioServer):
                    servers["default"] = srv.to_dict()
            if servers:
                opts["mcp_servers"] = servers

        # Native params
        if self.nparams:
            opts.update(self.nparams)
        if nconfig:
            opts.update(nconfig)

        return ClaudeAgentOptions(**opts)

    def _convert_message(self, msg: Any) -> Message | None:
        """Convert a claude_agent_sdk message to unified Message."""
        blocks: list = []
        role: str = "assistant"

        if isinstance(msg, AssistantMessage):
            role = "assistant"
            for block in getattr(msg, "content", []):
                if isinstance(block, SdkTextBlock):
                    blocks.append(TextBlock(text=block.text))
                elif isinstance(block, SdkToolUseBlock):
                    blocks.append(
                        ToolUseBlock(
                            tool_name=getattr(block, "name", ""),
                            tool_input=getattr(block, "input", None),
                            call_id=getattr(block, "id", None),
                        )
                    )
        elif isinstance(msg, ResultMessage):
            role = "tool"
            for block in getattr(msg, "content", []):
                if isinstance(block, SdkToolResultBlock):
                    blocks.append(
                        ToolResultBlock(
                            call_id=getattr(block, "tool_use_id", None),
                            output=getattr(block, "content", None),
                            is_error=getattr(block, "is_error", False),
                        )
                    )
                elif isinstance(block, SdkTextBlock):
                    blocks.append(TextBlock(text=block.text))
        elif isinstance(msg, SystemMessage):
            role = "system"
            for block in getattr(msg, "content", []):
                if isinstance(block, SdkTextBlock):
                    blocks.append(TextBlock(text=block.text))
        else:
            return None

        return Message(
            role=role,  # type: ignore[arg-type]
            content=blocks if blocks else None,
        )

    # ── sync run ─────────────────────────────────────────────────────

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
        """Synchronous agent run.

        Note: The Claude Agent SDK is async-native. The sync path uses
        ``anyio.from_thread.run`` under the hood via the core async helper.
        """
        from x8.core._async_helper import run_sync

        if stream:

            def _stream_iter() -> Iterator[Response[AgentStreamEvent]]:
                import anyio

                events: list[Response[AgentStreamEvent]] = []

                async def _collect():
                    async for ev in self._astream(
                        prompt,
                        model=model,
                        instructions=instructions,
                        tools=tools,
                        mcp_servers=mcp_servers,
                        allowed_tools=allowed_tools,
                        max_turns=max_turns,
                        cwd=cwd,
                        nconfig=nconfig,
                        **kwargs,
                    ):
                        events.append(ev)

                anyio.from_thread.run(_collect)
                yield from events

            return _stream_iter()
        else:
            result = run_sync(
                self.arun,
                prompt,
                model=model,
                instructions=instructions,
                tools=tools,
                mcp_servers=mcp_servers,
                allowed_tools=allowed_tools,
                max_turns=max_turns,
                cwd=cwd,
                stream=False,
                nconfig=nconfig,
                **kwargs,
            )
            return result

    # ── async run ────────────────────────────────────────────────────

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
        if stream:
            return self._astream(
                prompt,
                model=model,
                instructions=instructions,
                tools=tools,
                mcp_servers=mcp_servers,
                allowed_tools=allowed_tools,
                max_turns=max_turns,
                cwd=cwd,
                nconfig=nconfig,
                **kwargs,
            )

        options = self._build_options(
            instructions=instructions,
            tools=tools,
            mcp_servers=mcp_servers,
            allowed_tools=allowed_tools,
            max_turns=max_turns,
            cwd=cwd,
            nconfig=nconfig,
        )

        effective_model = model or self.model
        if effective_model:
            options.model = effective_model

        messages: list[Message] = []

        async with ClaudeSDKClient(options=options) as client:
            await client.query(prompt)
            async for msg in client.receive_response():
                converted = self._convert_message(msg)
                if converted:
                    messages.append(converted)

        result = AgentResult(
            status="completed",
            output=messages if messages else None,
        )
        return Response(result=result)

    async def _astream(
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
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> AsyncIterator[Response[AgentStreamEvent]]:
        options = self._build_options(
            instructions=instructions,
            tools=tools,
            mcp_servers=mcp_servers,
            allowed_tools=allowed_tools,
            max_turns=max_turns,
            cwd=cwd,
            nconfig=nconfig,
        )

        effective_model = model or self.model
        if effective_model:
            options.model = effective_model

        all_messages: list[Message] = []

        async with ClaudeSDKClient(options=options) as client:
            await client.query(prompt)
            async for msg in client.receive_response():
                converted = self._convert_message(msg)
                if converted:
                    all_messages.append(converted)

                # Emit streaming events for each block
                if isinstance(msg, AssistantMessage):
                    for block in getattr(msg, "content", []):
                        if isinstance(block, SdkTextBlock):
                            yield Response(
                                result=TextDoneEvent(
                                    text=block.text,
                                )
                            )
                        elif isinstance(block, SdkToolUseBlock):
                            yield Response(
                                result=ToolCallEvent(
                                    tool_name=getattr(block, "name", ""),
                                    tool_input=getattr(block, "input", None),
                                    call_id=getattr(block, "id", None),
                                )
                            )

        # Final completed event
        result = AgentResult(
            status="completed",
            output=all_messages if all_messages else None,
        )
        yield Response(result=AgentCompletedEvent(result=result))

    # ── session ──────────────────────────────────────────────────────

    def create_session(
        self,
        *,
        config: SessionConfig,
    ) -> "ClaudeSession":
        """Create a persistent session (sync entry-point)."""
        from x8.core._async_helper import run_sync

        return run_sync(self.acreate_session, config=config)

    async def acreate_session(
        self,
        *,
        config: SessionConfig,
    ) -> "ClaudeSession":
        """Create a persistent session (async entry-point)."""
        options = self._build_options(
            instructions=config.instructions,
            tools=(
                [t.to_dict() for t in config.tools] if config.tools else None
            ),
            mcp_servers=config.mcp_servers,  # type: ignore[arg-type]
            allowed_tools=config.allowed_tools,
            max_turns=config.max_turns,
            cwd=config.cwd,
            nconfig=config.nconfig,
        )
        effective_model = config.model or self.model
        if effective_model:
            options.model = effective_model

        session = ClaudeSession(
            config=config,
            options=options,
            message_converter=self._convert_message,
        )
        await session._start()
        return session


# ── ClaudeSession ────────────────────────────────────────────────────


class ClaudeSession(AgentSession):
    """Persistent multi-turn session backed by ``ClaudeSDKClient``.

    The underlying client stays alive across ``send`` / ``asend``
    calls, so conversation context is preserved automatically.
    """

    _options: ClaudeAgentOptions
    _client: ClaudeSDKClient | None
    _convert_message: Any  # callable

    def __init__(
        self,
        config: SessionConfig,
        options: ClaudeAgentOptions,
        message_converter: Any,
    ):
        super().__init__(config)
        self._options = options
        self._client = None
        self._convert_message = message_converter

    async def _start(self) -> None:
        """Start the underlying client (called by the provider)."""
        self._client = ClaudeSDKClient(options=self._options)
        await self._client.__aenter__()

    # ── async API ────────────────────────────────────────────────

    async def asend(
        self,
        prompt: str,
        *,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> AgentResult | AsyncIterator[AgentStreamEvent]:
        if self._client is None:
            raise RuntimeError("Session is not started. Use acreate_session.")

        if stream:
            return self._asend_stream(prompt, **kwargs)

        await self._client.query(prompt)
        messages: list[Message] = []
        async for msg in self._client.receive_response():
            converted = self._convert_message(msg)
            if converted:
                messages.append(converted)

        self.history.extend(messages)
        return AgentResult(
            status="completed",
            output=messages if messages else None,
        )

    async def _asend_stream(
        self,
        prompt: str,
        **kwargs: Any,
    ) -> AsyncIterator[AgentStreamEvent]:
        if self._client is None:
            raise RuntimeError("Session is not started.")

        await self._client.query(prompt)
        all_messages: list[Message] = []

        async for msg in self._client.receive_response():
            converted = self._convert_message(msg)
            if converted:
                all_messages.append(converted)

            if isinstance(msg, AssistantMessage):
                for block in getattr(msg, "content", []):
                    if isinstance(block, SdkTextBlock):
                        yield TextDoneEvent(text=block.text)
                    elif isinstance(block, SdkToolUseBlock):
                        yield ToolCallEvent(
                            tool_name=getattr(block, "name", ""),
                            tool_input=getattr(block, "input", None),
                            call_id=getattr(block, "id", None),
                        )

        self.history.extend(all_messages)
        result = AgentResult(
            status="completed",
            output=all_messages if all_messages else None,
        )
        yield AgentCompletedEvent(result=result)

    async def aclose(self) -> None:
        if self._client is not None:
            await self._client.__aexit__(None, None, None)
            self._client = None

    # ── sync API ─────────────────────────────────────────────────

    def send(
        self,
        prompt: str,
        *,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> AgentResult | Iterator[AgentStreamEvent]:
        from x8.core._async_helper import run_sync

        if stream:

            def _iter() -> Iterator[AgentStreamEvent]:
                import anyio

                events: list[AgentStreamEvent] = []

                async def _collect():
                    async for ev in self._asend_stream(prompt, **kwargs):
                        events.append(ev)

                anyio.from_thread.run(_collect)
                yield from events

            return _iter()

        return run_sync(self.asend, prompt, stream=False, **kwargs)

    def close(self) -> None:
        from x8.core._async_helper import run_sync

        run_sync(self.aclose)
