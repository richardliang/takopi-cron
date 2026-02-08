from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Any

from takopi.api import (
    CommandBackend,
    CommandContext,
    CommandResult,
    CommandExecutor,
    ConfigError,
    MessageRef,
    RenderedMessage,
    RunContext,
    RunRequest,
    get_logger,
)

logger = get_logger(__name__)

type ChannelId = int | str
type ThreadId = int | str | None
type JobKey = tuple[ChannelId, ThreadId]


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


def _key_for(ctx: CommandContext) -> JobKey:
    # Per chat + thread (Telegram topics / Slack threads).
    return (ctx.message.channel_id, ctx.message.thread_id)


@dataclass(frozen=True, slots=True)
class CronSpec:
    every_s: float
    prompt: str
    engine: str | None
    context: RunContext | None
    notify: bool


@dataclass(frozen=True, slots=True)
class SeedPreset:
    id: str
    every_hours: float
    prompt: str
    notify: bool


@dataclass(slots=True)
class _CronEntry:
    key: JobKey
    spec: CronSpec
    executor: CommandExecutor
    reply_to: MessageRef | None
    stop: asyncio.Event
    created_at: float
    task: asyncio.Task[None] | None = None
    tick_count: int = 0
    last_started_at: float | None = None
    last_finished_at: float | None = None
    last_error: str | None = None


class CronManager:
    def __init__(self) -> None:
        self._entries: dict[JobKey, _CronEntry] = {}
        self._lock = asyncio.Lock()

    async def start(
        self,
        *,
        ctx: CommandContext,
        spec: CronSpec,
        reply_to: MessageRef | None,
    ) -> None:
        key = _key_for(ctx)
        await self.stop(key=key)

        stop = asyncio.Event()
        async with self._lock:
            entry = _CronEntry(
                key=key,
                spec=spec,
                executor=ctx.executor,
                reply_to=reply_to,
                stop=stop,
                created_at=time.time(),
            )
            entry.task = asyncio.create_task(self._loop(entry=entry))
            self._entries[key] = entry

    async def stop(self, *, key: JobKey) -> bool:
        async with self._lock:
            entry = self._entries.pop(key, None)
        if entry is None:
            return False
        entry.stop.set()
        if entry.task is not None:
            entry.task.cancel()
        try:
            if entry.task is not None:
                await entry.task
        except asyncio.CancelledError:
            # Stop is best-effort; cancellation should not leak.
            pass
        return True

    async def get(self, *, key: JobKey) -> _CronEntry | None:
        async with self._lock:
            return self._entries.get(key)

    async def list(self) -> list[_CronEntry]:
        async with self._lock:
            return list(self._entries.values())

    async def _loop(
        self,
        *,
        entry: _CronEntry,
    ) -> None:
        # Run once immediately, then sleep.
        while not entry.stop.is_set():
            await self._tick(entry=entry)
            try:
                await asyncio.wait_for(entry.stop.wait(), timeout=entry.spec.every_s)
            except TimeoutError:
                continue
            break

    async def _tick(
        self,
        *,
        entry: _CronEntry,
    ) -> None:
        entry.tick_count += 1
        entry.last_started_at = time.time()
        entry.last_error = None

        try:
            result = await entry.executor.run_one(
                RunRequest(
                    prompt=entry.spec.prompt,
                    engine=entry.spec.engine,
                    context=entry.spec.context,
                ),
                mode="capture",
            )
            rendered = (
                result.message
                if result.message is not None
                else RenderedMessage(text="(no output)")
            )
            await entry.executor.send(
                _tick_header(entry),
                reply_to=entry.reply_to,
                notify=entry.spec.notify,
            )
            await entry.executor.send(
                rendered,
                reply_to=entry.reply_to,
                notify=False,
            )
        except Exception as exc:
            entry.last_error = str(exc) or exc.__class__.__name__
            logger.exception(
                "cron.tick_failed",
                error=entry.last_error,
                error_type=exc.__class__.__name__,
            )
            await entry.executor.send(
                f"cron error:\n{entry.last_error}",
                reply_to=entry.reply_to,
                notify=True,
            )
        finally:
            entry.last_finished_at = time.time()


def _tick_header(entry: _CronEntry) -> str:
    hours = entry.spec.every_s / 3600.0
    ts = entry.last_started_at or time.time()
    return f"cron tick #{entry.tick_count} (every {hours:g}h) @ {_format_ts(ts)}"


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

        if sub == "seed":
            return await _handle_seed(ctx)

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
    notify: bool | None = None,
) -> CronSpec:
    ambient_context = getattr(ctx.executor, "default_context", None)
    resolved = ctx.runtime.resolve_message(
        text=prompt_text,
        reply_text=ctx.reply_text,
        ambient_context=ambient_context if isinstance(ambient_context, RunContext) else None,
        chat_id=_coerce_chat_id(ctx.message.channel_id),
    )
    notify = _default_notify(ctx) if notify is None else notify
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
        "/cron seed list\n"
        "/cron seed start <id|index>\n"
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


def _parse_seed_presets(config: dict[str, Any]) -> list[SeedPreset]:
    seeds = config.get("seed")
    if seeds is None:
        return []
    if not isinstance(seeds, list):
        raise ConfigError("Invalid `plugins.cron.seed`; expected an array of tables.")

    notify_default = _optional_bool(config, "notify")
    notify_default = True if notify_default is None else notify_default

    presets: list[SeedPreset] = []
    seen: set[str] = set()
    for idx, raw in enumerate(seeds):
        if not isinstance(raw, dict):
            raise ConfigError(f"Invalid `plugins.cron.seed[{idx}]`; expected a table.")
        if raw.get("enabled") is False:
            continue

        preset_id = raw.get("id")
        if preset_id is None:
            preset_id = raw.get("name")
        if preset_id is None:
            preset_id = str(idx + 1)
        if not isinstance(preset_id, str) or not preset_id.strip():
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].id`; expected a string."
            )
        preset_id = preset_id.strip()
        key = preset_id.casefold()
        if key in seen:
            raise ConfigError(f"Duplicate `plugins.cron.seed` id: {preset_id!r}.")
        seen.add(key)

        every_hours = raw.get("every_hours")
        if not isinstance(every_hours, (int, float)):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].every_hours`; expected a number."
            )
        if every_hours <= 0:
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].every_hours`; must be > 0."
            )

        prompt = raw.get("prompt")
        if not isinstance(prompt, str) or not prompt.strip():
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].prompt`; expected a string."
            )

        notify = raw.get("notify")
        if notify is None:
            notify = notify_default
        if not isinstance(notify, bool):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].notify`; expected a boolean."
            )

        presets.append(
            SeedPreset(
                id=preset_id,
                every_hours=float(every_hours),
                prompt=prompt.strip(),
                notify=notify,
            )
        )
    return presets


def _select_seed(presets: list[SeedPreset], token: str) -> SeedPreset | None:
    if token.isdigit():
        idx = int(token)
        if 1 <= idx <= len(presets):
            return presets[idx - 1]
    token_key = token.casefold()
    for preset in presets:
        if preset.id.casefold() == token_key:
            return preset
    return None


def _format_seed_list(presets: list[SeedPreset]) -> str:
    if not presets:
        return "cron: no seed presets"
    lines = ["cron: seed presets"]
    for idx, preset in enumerate(presets, start=1):
        lines.append(
            f"- {idx}. {preset.id}: every {preset.every_hours:g}h (notify={preset.notify})"
        )
    return "\n".join(lines)


async def _handle_seed(ctx: CommandContext) -> CommandResult:
    if len(ctx.args) < 2:
        raise ConfigError("Usage: /cron seed list | /cron seed start <id|index>")

    action = ctx.args[1].lower()
    presets = _parse_seed_presets(dict(ctx.plugin_config or {}))

    if action == "list":
        return CommandResult(text=_format_seed_list(presets))

    if action in {"start", "on"}:
        if len(ctx.args) < 3:
            raise ConfigError("Usage: /cron seed start <id|index>")
        preset = _select_seed(presets, ctx.args[2])
        if preset is None:
            raise ConfigError(f"Unknown seed {ctx.args[2]!r}. Use `/cron seed list`.")
        spec = _resolve_spec(
            ctx,
            hours=preset.every_hours,
            prompt_text=preset.prompt,
            notify=preset.notify,
        )
        reply_to = ctx.reply_to or ctx.message
        await MANAGER.start(ctx=ctx, spec=spec, reply_to=reply_to)
        return CommandResult(
            text=f"cron: started seed {preset.id} (every {preset.every_hours:g}h)"
        )

    raise ConfigError("Usage: /cron seed list | /cron seed start <id|index>")
