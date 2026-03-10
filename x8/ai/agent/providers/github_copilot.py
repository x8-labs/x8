import asyncio
from typing import Any, AsyncIterator, Iterator

from x8.core import Response
from x8.core._provider import Provider

from .._models import (
    AgentCompletedEvent,
    AgentResult,
    AgentSession,
    AgentStreamEvent,
    ErrorEvent,
    MCPServer,
    MCPStdioServer,
    Message,
    SessionConfig,
    TextBlock,
    TextDeltaEvent,
    TextDoneEvent,
    Tool,
    ToolCallEvent,
    ToolUseBlock,
)


class GithubCopilot(Provider):
    """Provider that wraps the GitHub Copilot SDK (github-copilot-sdk).

    Uses ``copilot.CopilotClient`` to create sessions and exchange
    messages with the GitHub Copilot agent.
    """

    github_token: str | None
    cli_path: str | None
    model: str
    use_stdio: bool
    log_level: str
    streaming: bool
    cwd: str | None
    nparams: dict[str, Any] | None

    _init: bool
    _ainit: bool

    def __init__(
        self,
        github_token: str | None = None,
        cli_path: str | None = None,
        model: str = "gpt-5",
        use_stdio: bool = True,
        log_level: str = "info",
        streaming: bool = True,
        cwd: str | None = None,
        nparams: dict[str, Any] | None = None,
        **kwargs,
    ):
        """Initialize.

        Args:
            github_token:
                GitHub personal access token for authentication.
            cli_path:
                Path to the Copilot CLI executable.
            model:
                Model to use (e.g. "gpt-5", "claude-sonnet-4.5").
            use_stdio:
                Use stdio transport instead of TCP.
            log_level:
                Log level for the Copilot CLI.
            streaming:
                Enable streaming delta events by default.
            cwd:
                Working directory for the CLI process.
            nparams:
                Extra native parameters forwarded to
                CopilotClient / SessionConfig.
        """
        self.github_token = github_token
        self.cli_path = cli_path
        self.model = model
        self.use_stdio = use_stdio
        self.log_level = log_level
        self.streaming = streaming
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

    def _build_client_opts(self) -> dict[str, Any]:
        """Build kwargs for ``CopilotClient``."""
        opts: dict[str, Any] = {
            "use_stdio": self.use_stdio,
            "log_level": self.log_level,
        }
        if self.cli_path:
            opts["cli_path"] = self.cli_path
        if self.github_token:
            opts["github_token"] = self.github_token
        if self.cwd:
            opts["cwd"] = self.cwd
        return opts

    def _build_session_config(
        self,
        model: str | None = None,
        instructions: str | None = None,
        tools: list[dict | Tool] | None = None,
        max_turns: int | None = None,
        stream: bool | None = None,
        nconfig: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """Build a session config dict for ``create_session``."""
        config: dict[str, Any] = {
            "model": model or self.model,
        }

        effective_streaming = stream if stream is not None else self.streaming
        config["streaming"] = effective_streaming

        if instructions:
            config["system_message"] = {"content": instructions}

        # Convert tools
        if tools:
            config["tools"] = self._convert_tools(tools)

        if self.nparams:
            config.update(self.nparams)
        if nconfig:
            config.update(nconfig)

        return config

    def _convert_tools(self, tools: list[dict | Tool]) -> list[Any]:
        """Convert unified Tool models to Copilot SDK Tool format."""
        from copilot import Tool as CopilotTool

        sdk_tools: list[Any] = []
        for t in tools:
            if isinstance(t, dict):
                name = t.get("name", "")
                description = t.get("description", "")
                parameters = t.get("parameters", {})
            else:
                name = t.name
                description = t.description or ""
                parameters = t.parameters
                if parameters and hasattr(parameters, "to_dict"):
                    parameters = parameters.to_dict()

            sdk_tools.append(
                CopilotTool(
                    name=name,
                    description=description,
                    parameters=parameters
                    or {
                        "type": "object",
                        "properties": {},
                    },
                )
            )
        return sdk_tools

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

        Note: The Copilot SDK is async-native. The sync path collects
        results via ``asyncio.run``.
        """
        from x8.core._async_helper import run_sync

        if stream:

            def _stream_iter() -> Iterator[Response[AgentStreamEvent]]:
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

                run_sync(_collect)
                yield from events

            return _stream_iter()
        else:
            return run_sync(
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

        from copilot import CopilotClient

        client_opts = self._build_client_opts()
        session_config = self._build_session_config(
            model=model,
            instructions=instructions,
            tools=tools,
            max_turns=max_turns,
            stream=False,
            nconfig=nconfig,
        )

        client = CopilotClient(client_opts)
        await client.start()

        try:
            session = await client.create_session(session_config)

            done = asyncio.Event()
            messages: list[Message] = []
            error_msg: str | None = None

            def on_event(event):
                nonlocal error_msg
                etype = (
                    event.type.value
                    if hasattr(event.type, "value")
                    else str(event.type)
                )

                if etype == "assistant.message":
                    content = getattr(event.data, "content", None) or ""
                    messages.append(
                        Message(
                            role="assistant",
                            content=[TextBlock(text=content)],
                        )
                    )
                elif etype == "tool.call":
                    tool_name = getattr(
                        event.data, "tool_name", ""
                    ) or getattr(event.data, "name", "")
                    tool_input = getattr(
                        event.data, "arguments", None
                    ) or getattr(event.data, "input", None)
                    messages.append(
                        Message(
                            role="assistant",
                            content=[
                                ToolUseBlock(
                                    tool_name=tool_name,
                                    tool_input=(
                                        tool_input
                                        if isinstance(tool_input, dict)
                                        else None
                                    ),
                                    call_id=getattr(event.data, "id", None),
                                )
                            ],
                        )
                    )
                elif etype == "error":
                    error_msg = getattr(event.data, "message", str(event.data))
                elif etype == "session.idle":
                    done.set()

            session.on(on_event)
            await session.send({"prompt": prompt})
            await done.wait()

            await session.destroy()

            result = AgentResult(
                status="failed" if error_msg else "completed",
                output=messages if messages else None,
                error=error_msg,
            )
            return Response(result=result)
        finally:
            await client.stop()

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
        from copilot import CopilotClient

        client_opts = self._build_client_opts()
        session_config = self._build_session_config(
            model=model,
            instructions=instructions,
            tools=tools,
            max_turns=max_turns,
            stream=True,
            nconfig=nconfig,
        )

        client = CopilotClient(client_opts)
        await client.start()

        try:
            session = await client.create_session(session_config)

            done = asyncio.Event()
            event_queue: asyncio.Queue[Response[AgentStreamEvent] | None] = (
                asyncio.Queue()
            )
            all_messages: list[Message] = []
            error_msg: str | None = None

            def on_event(event):
                nonlocal error_msg
                etype = (
                    event.type.value
                    if hasattr(event.type, "value")
                    else str(event.type)
                )

                if etype == "assistant.message_delta":
                    delta = getattr(event.data, "delta_content", "") or ""
                    if delta:
                        event_queue.put_nowait(
                            Response(result=TextDeltaEvent(delta=delta))
                        )
                elif etype == "assistant.message":
                    content = getattr(event.data, "content", None) or ""
                    all_messages.append(
                        Message(
                            role="assistant",
                            content=[TextBlock(text=content)],
                        )
                    )
                    event_queue.put_nowait(
                        Response(result=TextDoneEvent(text=content))
                    )
                elif etype == "assistant.reasoning_delta":
                    delta = getattr(event.data, "delta_content", "") or ""
                    if delta:
                        event_queue.put_nowait(
                            Response(result=TextDeltaEvent(delta=delta))
                        )
                elif etype == "tool.call":
                    tool_name = getattr(
                        event.data, "tool_name", ""
                    ) or getattr(event.data, "name", "")
                    tool_input = getattr(
                        event.data, "arguments", None
                    ) or getattr(event.data, "input", None)
                    event_queue.put_nowait(
                        Response(
                            result=ToolCallEvent(
                                tool_name=tool_name,
                                tool_input=(
                                    tool_input
                                    if isinstance(tool_input, dict)
                                    else None
                                ),
                                call_id=getattr(event.data, "id", None),
                            )
                        )
                    )
                elif etype == "error":
                    error_msg = getattr(event.data, "message", str(event.data))
                    event_queue.put_nowait(
                        Response(result=ErrorEvent(message=error_msg))
                    )
                elif etype == "session.idle":
                    done.set()

            session.on(on_event)
            await session.send({"prompt": prompt})

            # Yield events as they arrive
            while not done.is_set():
                try:
                    ev = await asyncio.wait_for(event_queue.get(), timeout=0.1)
                    if ev is not None:
                        yield ev
                except asyncio.TimeoutError:
                    continue

            # Drain remaining events
            while not event_queue.empty():
                ev = event_queue.get_nowait()
                if ev is not None:
                    yield ev

            await session.destroy()

            # Final completed event
            result = AgentResult(
                status="failed" if error_msg else "completed",
                output=all_messages if all_messages else None,
                error=error_msg,
            )
            yield Response(result=AgentCompletedEvent(result=result))
        finally:
            await client.stop()

    # ── session ──────────────────────────────────────────────────────

    def create_session(
        self,
        *,
        config: SessionConfig,
    ) -> "CopilotSession":
        """Create a persistent session (sync entry-point)."""
        from x8.core._async_helper import run_sync

        return run_sync(self.acreate_session, config=config)

    async def acreate_session(
        self,
        *,
        config: SessionConfig,
    ) -> "CopilotSession":
        """Create a persistent session (async entry-point)."""
        from copilot import CopilotClient

        client_opts = self._build_client_opts()
        session_config = self._build_session_config(
            model=config.model,
            instructions=config.instructions,
            tools=(
                [t.to_dict() for t in config.tools] if config.tools else None
            ),
            max_turns=config.max_turns,
            stream=True,
            nconfig=config.nconfig,
        )

        client = CopilotClient(client_opts)
        await client.start()

        sdk_session = await client.create_session(session_config)

        return CopilotSession(
            config=config,
            client=client,
            sdk_session=sdk_session,
        )


# ── CopilotSession ──────────────────────────────────────────────────


class CopilotSession(AgentSession):
    """Persistent multi-turn session backed by ``CopilotClient``.

    The underlying client and SDK session stay alive across
    ``send`` / ``asend`` calls, enabling multi-turn conversations.
    """

    _client: Any  # CopilotClient
    _sdk_session: Any  # copilot session object

    def __init__(
        self,
        config: SessionConfig,
        client: Any,
        sdk_session: Any,
    ):
        super().__init__(config)
        self._client = client
        self._sdk_session = sdk_session

    # ── helpers ───────────────────────────────────────────────────

    @staticmethod
    def _parse_event(event) -> tuple[str, Any]:
        """Return ``(event_type_str, event_data)``."""
        etype = (
            event.type.value
            if hasattr(event.type, "value")
            else str(event.type)
        )
        return etype, getattr(event, "data", None)

    # ── async API ────────────────────────────────────────────────

    async def asend(
        self,
        prompt: str,
        *,
        stream: bool | None = None,
        **kwargs: Any,
    ) -> AgentResult | AsyncIterator[AgentStreamEvent]:
        if self._sdk_session is None:
            raise RuntimeError("Session is closed. Create a new session.")

        if stream:
            return self._asend_stream(prompt, **kwargs)

        done = asyncio.Event()
        messages: list[Message] = []
        error_msg: str | None = None

        def on_event(event):
            nonlocal error_msg
            etype, data = CopilotSession._parse_event(event)

            if etype == "assistant.message":
                content = getattr(data, "content", None) or ""
                messages.append(
                    Message(
                        role="assistant",
                        content=[TextBlock(text=content)],
                    )
                )
            elif etype == "tool.call":
                tool_name = getattr(data, "tool_name", "") or getattr(
                    data, "name", ""
                )
                tool_input = getattr(data, "arguments", None) or getattr(
                    data, "input", None
                )
                messages.append(
                    Message(
                        role="assistant",
                        content=[
                            ToolUseBlock(
                                tool_name=tool_name,
                                tool_input=(
                                    tool_input
                                    if isinstance(tool_input, dict)
                                    else None
                                ),
                                call_id=getattr(data, "id", None),
                            )
                        ],
                    )
                )
            elif etype == "error":
                error_msg = getattr(data, "message", str(data))
            elif etype == "session.idle":
                done.set()

        self._sdk_session.on(on_event)
        await self._sdk_session.send({"prompt": prompt})
        await done.wait()

        self.history.extend(messages)
        return AgentResult(
            status="failed" if error_msg else "completed",
            output=messages if messages else None,
            error=error_msg,
        )

    async def _asend_stream(
        self,
        prompt: str,
        **kwargs: Any,
    ) -> AsyncIterator[AgentStreamEvent]:
        if self._sdk_session is None:
            raise RuntimeError("Session is closed.")

        done = asyncio.Event()
        event_queue: asyncio.Queue[AgentStreamEvent | None] = asyncio.Queue()
        all_messages: list[Message] = []
        error_msg: str | None = None

        def on_event(event):
            nonlocal error_msg
            etype, data = CopilotSession._parse_event(event)

            if etype == "assistant.message_delta":
                delta = getattr(data, "delta_content", "") or ""
                if delta:
                    event_queue.put_nowait(TextDeltaEvent(delta=delta))
            elif etype == "assistant.message":
                content = getattr(data, "content", None) or ""
                all_messages.append(
                    Message(
                        role="assistant",
                        content=[TextBlock(text=content)],
                    )
                )
                event_queue.put_nowait(TextDoneEvent(text=content))
            elif etype == "assistant.reasoning_delta":
                delta = getattr(data, "delta_content", "") or ""
                if delta:
                    event_queue.put_nowait(TextDeltaEvent(delta=delta))
            elif etype == "tool.call":
                tool_name = getattr(data, "tool_name", "") or getattr(
                    data, "name", ""
                )
                tool_input = getattr(data, "arguments", None) or getattr(
                    data, "input", None
                )
                event_queue.put_nowait(
                    ToolCallEvent(
                        tool_name=tool_name,
                        tool_input=(
                            tool_input
                            if isinstance(tool_input, dict)
                            else None
                        ),
                        call_id=getattr(data, "id", None),
                    )
                )
            elif etype == "error":
                error_msg = getattr(data, "message", str(data))
                event_queue.put_nowait(ErrorEvent(message=error_msg))
            elif etype == "session.idle":
                done.set()

        self._sdk_session.on(on_event)
        await self._sdk_session.send({"prompt": prompt})

        while not done.is_set():
            try:
                ev = await asyncio.wait_for(event_queue.get(), timeout=0.1)
                if ev is not None:
                    yield ev
            except asyncio.TimeoutError:
                continue

        while not event_queue.empty():
            ev = event_queue.get_nowait()
            if ev is not None:
                yield ev

        self.history.extend(all_messages)
        result = AgentResult(
            status="failed" if error_msg else "completed",
            output=all_messages if all_messages else None,
            error=error_msg,
        )
        yield AgentCompletedEvent(result=result)

    async def aclose(self) -> None:
        if self._sdk_session is not None:
            await self._sdk_session.destroy()
            self._sdk_session = None
        if self._client is not None:
            await self._client.stop()
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
