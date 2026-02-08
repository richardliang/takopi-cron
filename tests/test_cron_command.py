from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import anyio
import pytest

from takopi.api import (
    CommandContext,
    CommandResult,
    MessageRef,
    RenderedMessage,
    RunContext,
    RunRequest,
    RunResult,
)

from takopi_cron.backend import BACKEND, MANAGER


@dataclass(frozen=True, slots=True)
class _Resolved:
    prompt: str
    resume_token: Any = None
    engine_override: str | None = None
    context: RunContext | None = None


class _FakeRuntime:
    def __init__(self, *, resolved: _Resolved | None = None) -> None:
        self._resolved = resolved

    def resolve_message(
        self,
        *,
        text: str,
        reply_text: str | None,
        ambient_context: RunContext | None = None,
        chat_id: int | None = None,
    ) -> _Resolved:
        _ = reply_text, ambient_context, chat_id
        if self._resolved is not None:
            return self._resolved
        return _Resolved(prompt=text)


class _FakeExecutor:
    def __init__(self, *, answer: str = "ok") -> None:
        self.answer = answer
        self.run_calls: list[tuple[RunRequest, str]] = []
        self.send_calls: list[dict[str, Any]] = []

    async def send(
        self,
        message: RenderedMessage | str,
        *,
        reply_to: MessageRef | None = None,
        notify: bool = True,
    ) -> MessageRef | None:
        rendered = (
            message
            if isinstance(message, RenderedMessage)
            else RenderedMessage(text=message)
        )
        self.send_calls.append(
            {"message": rendered, "reply_to": reply_to, "notify": notify}
        )
        return None

    async def run_one(self, request: RunRequest, *, mode: str = "emit") -> RunResult:
        self.run_calls.append((request, mode))
        engine = request.engine or "codex"
        return RunResult(engine=engine, message=RenderedMessage(text=self.answer))

    async def run_many(self, _requests, *, mode: str = "emit", parallel: bool = False):  # type: ignore[no-untyped-def]
        _ = mode, parallel
        raise NotImplementedError


def _make_ctx(
    *,
    args: tuple[str, ...],
    args_text: str,
    plugin_config: dict[str, Any] | None = None,
    sender_id: int | None = 1,
    channel_id: int = 123,
    thread_id: int | None = None,
    reply_to: MessageRef | None = None,
    reply_text: str | None = None,
    runtime: Any | None = None,
    executor: Any | None = None,
) -> tuple[CommandContext, _FakeExecutor]:
    if runtime is None:
        runtime = _FakeRuntime()
    if executor is None:
        executor = _FakeExecutor()
    msg = MessageRef(
        channel_id=channel_id,
        message_id=1,
        thread_id=thread_id,
        sender_id=sender_id,
    )
    ctx = CommandContext(
        command="cron",
        text=f"/cron {args_text}".strip(),
        args_text=args_text,
        args=args,
        message=msg,
        reply_to=reply_to,
        reply_text=reply_text,
        config_path=None,
        plugin_config=plugin_config or {},
        runtime=runtime,
        executor=executor,
    )
    return ctx, executor


@pytest.mark.anyio
async def test_start_runs_immediately_and_repeats() -> None:
    ctx, exec_ = _make_ctx(
        args=("start", "0.00001", "hello"),
        args_text="start 0.00001 hello",
        executor=_FakeExecutor(answer="RESULT"),
        channel_id=100,
    )
    try:
        result = await BACKEND.handle(ctx)
        assert isinstance(result, CommandResult)
        assert "cron: started" in result.text

        with anyio.fail_after(1):
            while len(exec_.send_calls) < 2:
                await anyio.sleep(0.01)
        assert "cron tick #1" in exec_.send_calls[0]["message"].text
        assert exec_.send_calls[1]["message"].text == "RESULT"

        with anyio.fail_after(1):
            while len(exec_.send_calls) < 4:
                await anyio.sleep(0.01)
        assert "cron tick #2" in exec_.send_calls[2]["message"].text
        assert exec_.send_calls[3]["message"].text == "RESULT"
    finally:
        await MANAGER.stop(key=(ctx.message.channel_id, ctx.message.thread_id))


@pytest.mark.anyio
async def test_stop_prevents_future_ticks() -> None:
    ctx, exec_ = _make_ctx(
        args=("start", "0.0001", "hello"),
        args_text="start 0.0001 hello",
        executor=_FakeExecutor(answer="RESULT"),
        channel_id=101,
    )
    try:
        result = await BACKEND.handle(ctx)
        assert isinstance(result, CommandResult)

        with anyio.fail_after(1):
            while len(exec_.send_calls) < 1:
                await anyio.sleep(0.01)

        stop_ctx, _ = _make_ctx(
            args=("stop",),
            args_text="stop",
            channel_id=101,
        )
        stopped = await BACKEND.handle(stop_ctx)
        assert isinstance(stopped, CommandResult)
        assert stopped.text == "cron: stopped"

        # Sleep longer than the interval; no further sends should happen.
        await anyio.sleep(0.6)
        assert len(exec_.send_calls) == 2
    finally:
        await MANAGER.stop(key=(ctx.message.channel_id, ctx.message.thread_id))


@pytest.mark.anyio
async def test_run_sends_once_and_returns_none() -> None:
    ctx, exec_ = _make_ctx(
        args=("run", "hello"),
        args_text="run hello",
        executor=_FakeExecutor(answer="ONE_SHOT"),
        channel_id=102,
    )
    result = await BACKEND.handle(ctx)
    assert result is None
    assert len(exec_.run_calls) == 1
    assert len(exec_.send_calls) == 1
    assert exec_.send_calls[0]["message"].text == "ONE_SHOT"


@pytest.mark.anyio
async def test_user_not_allowed() -> None:
    ctx, exec_ = _make_ctx(
        args=("start", "1", "hello"),
        args_text="start 1 hello",
        plugin_config={"allowed_user_ids": [1, 2]},
        sender_id=3,
        channel_id=103,
    )
    result = await BACKEND.handle(ctx)
    assert isinstance(result, CommandResult)
    assert result.text == "cron error: user not allowed"
    assert exec_.run_calls == []
    assert exec_.send_calls == []


@pytest.mark.anyio
async def test_notify_config_applies_to_ticks() -> None:
    ctx, exec_ = _make_ctx(
        args=("start", "0.00001", "hello"),
        args_text="start 0.00001 hello",
        plugin_config={"notify": False},
        executor=_FakeExecutor(answer="RESULT"),
        channel_id=104,
    )
    try:
        result = await BACKEND.handle(ctx)
        assert isinstance(result, CommandResult)

        with anyio.fail_after(1):
            while len(exec_.send_calls) < 1:
                await anyio.sleep(0.01)
        assert exec_.send_calls[0]["notify"] is False
    finally:
        await MANAGER.stop(key=(ctx.message.channel_id, ctx.message.thread_id))
