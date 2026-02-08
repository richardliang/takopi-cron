from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from takopi.api import (
    CommandBackend,
    CommandContext,
    CommandResult,
    ConfigError,
    MessageRef,
    RenderedMessage,
    RunContext,
    RunRequest,
    get_logger,
)

logger = get_logger(__name__)


def _coerce_chat_id(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    return None


def _optional_bool(config: dict[str, Any], key: str) -> bool | None:
    raw = config.get(key)
    if raw is None:
        return None
    if isinstance(raw, bool):
        return raw
    raise ConfigError(f"Invalid `plugins.cron.{key}`; expected a boolean.")


def _optional_int_set(config: dict[str, Any], key: str) -> set[int] | None:
    raw = config.get(key)
    if raw is None:
        return None
    if not isinstance(raw, list):
        raise ConfigError(f"Invalid `plugins.cron.{key}`; expected an int[].")
    out: set[int] = set()
    for item in raw:
        if not isinstance(item, int):
            raise ConfigError(f"Invalid `plugins.cron.{key}`; expected an int[].")
        out.add(item)
    return out


def _parse_hours(token: str) -> float:
    try:
        hours = float(token)
    except ValueError as exc:
        raise ConfigError(f"Invalid hours {token!r}; expected a number.") from exc
    if hours <= 0:
        raise ConfigError("Hours must be > 0.")
    return hours


def _format_ts(ts: float) -> str:
    # Local time; transport formatting is plain text.
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def _key_for(ctx: CommandContext) -> tuple[Any, Any]:
    # Per chat + thread (Telegram topics / Slack threads).
    return (ctx.message.channel_id, ctx.message.thread_id)


@dataclass(frozen=True, slots=True)
class CronSpec:
    every_s: float
    prompt: str
    engine: str | None
    context: RunContext | None
    notify: bool


@dataclass(slots=True)
class _CronEntry:
    key: tuple[Any, Any]
    spec: CronSpec
    executor: Any
    reply_to: MessageRef
    stop: asyncio.Event
    task: asyncio.Task[None]
    created_at: float
    tick_count: int = 0
    last_started_at: float | None = None
    last_finished_at: float | None = None
    last_error: str | None = None


class CronManager:
    def __init__(self) -> None:
        self._entries: dict[tuple[Any, Any], _CronEntry] = {}
        self._lock = asyncio.Lock()

    async def start(
        self,
        *,
        ctx: CommandContext,
        spec: CronSpec,
        reply_to: MessageRef,
    ) -> None:
        key = _key_for(ctx)
        await self.stop(key=key)

        stop = asyncio.Event()
        task = asyncio.create_task(self._loop(key=key, ctx=ctx, spec=spec, stop=stop))
        entry = _CronEntry(
            key=key,
            spec=spec,
            executor=ctx.executor,
            reply_to=reply_to,
            stop=stop,
            task=task,
            created_at=time.time(),
        )
        async with self._lock:
            self._entries[key] = entry

    async def stop(self, *, key: tuple[Any, Any]) -> bool:
        async with self._lock:
            entry = self._entries.pop(key, None)
        if entry is None:
            return False
        entry.stop.set()
        try:
            await entry.task
        except asyncio.CancelledError:
            # Stop is best-effort; cancellation should not leak.
            pass
        return True

    async def get(self, *, key: tuple[Any, Any]) -> _CronEntry | None:
        async with self._lock:
            return self._entries.get(key)

    async def list(self) -> list[_CronEntry]:
        async with self._lock:
            return list(self._entries.values())

    async def _loop(
        self,
        *,
        key: tuple[Any, Any],
        ctx: CommandContext,
        spec: CronSpec,
        stop: asyncio.Event,
    ) -> None:
        # Run once immediately, then sleep.
        while not stop.is_set():
            await self._tick(key=key, ctx=ctx, spec=spec)
            try:
                await asyncio.wait_for(stop.wait(), timeout=spec.every_s)
            except asyncio.TimeoutError:
                continue
            break

    async def _tick(
        self,
        *,
        key: tuple[Any, Any],
        ctx: CommandContext,
        spec: CronSpec,
    ) -> None:
        entry = await self.get(key=key)
        if entry is None:
            return
        entry.tick_count += 1
        entry.last_started_at = time.time()
        entry.last_error = None

        try:
            result = await ctx.executor.run_one(
                RunRequest(
                    prompt=spec.prompt,
                    engine=spec.engine,
                    context=spec.context,
                ),
                mode="capture",
            )
            rendered = (
                result.message
                if result.message is not None
                else RenderedMessage(text="(no output)")
            )
            await ctx.executor.send(
                _prefix_tick(rendered, entry=entry, spec=spec),
                reply_to=entry.reply_to,
                notify=spec.notify,
            )
        except Exception as exc:
            entry.last_error = str(exc) or exc.__class__.__name__
            logger.exception(
                "cron.tick_failed",
                error=entry.last_error,
                error_type=exc.__class__.__name__,
            )
            await ctx.executor.send(
                f"cron error:\n{entry.last_error}",
                reply_to=entry.reply_to,
                notify=True,
            )
        finally:
            entry.last_finished_at = time.time()


def _prefix_tick(message: RenderedMessage, *, entry: _CronEntry, spec: CronSpec) -> str:
    hours = spec.every_s / 3600.0
    ts = entry.last_started_at or time.time()
    header = f"cron tick #{entry.tick_count} (every {hours:g}h) @ {_format_ts(ts)}"
    body = message.text.strip()
    if not body:
        return header
    return f"{header}\n\n{body}"


MANAGER = CronManager()


def _is_user_allowed(ctx: CommandContext) -> bool:
    cfg = dict(ctx.plugin_config or {})
    allowed = _optional_int_set(cfg, "allowed_user_ids")
    if allowed is None or not allowed:
        return True
    sender = ctx.message.sender_id
    return sender is not None and sender in allowed


def _default_notify(ctx: CommandContext) -> bool:
    cfg = dict(ctx.plugin_config or {})
    notify = _optional_bool(cfg, "notify")
    return True if notify is None else notify


class CronCommand:
    id = "cron"
    description = "Run a prompt on an interval"

    async def handle(self, ctx: CommandContext) -> CommandResult | None:
        try:
            return await self._handle(ctx)
        except ConfigError as exc:
            return CommandResult(text=f"cron error: {exc}")

    async def _handle(self, ctx: CommandContext) -> CommandResult:
        if not _is_user_allowed(ctx):
            return CommandResult(text="cron error: user not allowed")

        if not ctx.args:
            return CommandResult(text=_help_text())

        sub = ctx.args[0].lower()
        if sub in {"help", "--help", "-h"}:
            return CommandResult(text=_help_text())

        key = _key_for(ctx)

        if sub in {"stop", "off"}:
            stopped = await MANAGER.stop(key=key)
            return CommandResult(text="cron: stopped" if stopped else "cron: not running")

        if sub == "status":
            entry = await MANAGER.get(key=key)
            return CommandResult(text=_format_status(entry))

        if sub == "list":
            entries = await MANAGER.list()
            return CommandResult(text=_format_list(entries))

        if sub == "run":
            prompt_text = " ".join(ctx.args[1:]).strip()
            if not prompt_text:
                raise ConfigError("Usage: /cron run <prompt...>")
            spec = _resolve_spec(ctx, hours=None, prompt_text=prompt_text)
            # One-shot run; do not store in the manager.
            result = await ctx.executor.run_one(
                RunRequest(
                    prompt=spec.prompt,
                    engine=spec.engine,
                    context=spec.context,
                ),
                mode="capture",
            )
            rendered = (
                result.message
                if result.message is not None
                else RenderedMessage(text="(no output)")
            )
            await ctx.executor.send(
                rendered,
                reply_to=ctx.reply_to or ctx.message,
                notify=True,
            )
            return None

        if sub in {"start", "on"}:
            if len(ctx.args) < 3:
                raise ConfigError("Usage: /cron start <hours> <prompt...>")
            hours = _parse_hours(ctx.args[1])
            prompt_text = " ".join(ctx.args[2:]).strip()
            if not prompt_text:
                raise ConfigError("Usage: /cron start <hours> <prompt...>")
            spec = _resolve_spec(ctx, hours=hours, prompt_text=prompt_text)
            reply_to = ctx.reply_to or ctx.message
            await MANAGER.start(ctx=ctx, spec=spec, reply_to=reply_to)
            return CommandResult(text=f"cron: started (every {hours:g}h)")

        raise ConfigError(f"Unknown subcommand {sub!r}.")


def _resolve_spec(
    ctx: CommandContext,
    *,
    hours: float | None,
    prompt_text: str,
) -> CronSpec:
    ambient_context = getattr(ctx.executor, "default_context", None)
    resolved = ctx.runtime.resolve_message(
        text=prompt_text,
        reply_text=ctx.reply_text,
        ambient_context=ambient_context if isinstance(ambient_context, RunContext) else None,
        chat_id=_coerce_chat_id(ctx.message.channel_id),
    )
    notify = _default_notify(ctx)
    every_s = (hours * 3600.0) if hours is not None else 0.0
    return CronSpec(
        every_s=every_s,
        prompt=resolved.prompt or "continue",
        engine=resolved.engine_override,
        context=resolved.context,
        notify=notify,
    )


def _help_text() -> str:
    return (
        "Usage:\n"
        "/cron start <hours> <prompt...>\n"
        "/cron stop\n"
        "/cron status\n"
        "/cron list\n"
        "/cron run <prompt...>\n"
        "\n"
        "Notes:\n"
        "- Runs in-process inside Takopi and does not survive restarts.\n"
        "- Put directives at the start of the prompt (e.g. /claude, /myproj, @branch).\n"
    )


def _format_status(entry: _CronEntry | None) -> str:
    if entry is None:
        return "cron: not running"
    hours = entry.spec.every_s / 3600.0
    lines = [
        "cron: running",
        f"- interval: {hours:g}h",
        f"- notify: {entry.spec.notify}",
    ]
    if entry.spec.engine:
        lines.append(f"- engine: {entry.spec.engine}")
    if entry.spec.context is not None and entry.spec.context.project is not None:
        ctx_line = entry.spec.context.project
        if entry.spec.context.branch:
            ctx_line = f"{ctx_line} @{entry.spec.context.branch}"
        lines.append(f"- context: {ctx_line}")
    if entry.spec.prompt.strip():
        lines.append(f"- prompt: {entry.spec.prompt}")
    lines.append(f"- started_at: {_format_ts(entry.created_at)}")
    if entry.last_started_at is not None:
        lines.append(f"- last_started_at: {_format_ts(entry.last_started_at)}")
    if entry.last_finished_at is not None:
        lines.append(f"- last_finished_at: {_format_ts(entry.last_finished_at)}")
    if entry.last_error:
        lines.append(f"- last_error: {entry.last_error}")
    return "\n".join(lines)


def _format_list(entries: list[_CronEntry]) -> str:
    if not entries:
        return "cron: no jobs"
    lines = ["cron: jobs"]
    for entry in entries:
        hours = entry.spec.every_s / 3600.0
        where = f"{entry.key[0]!r}"
        if entry.key[1] is not None:
            where = f"{where} thread={entry.key[1]!r}"
        lines.append(f"- {where}: every {hours:g}h (ticks={entry.tick_count})")
    return "\n".join(lines)


BACKEND: CommandBackend = CronCommand()
