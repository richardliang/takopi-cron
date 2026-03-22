from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from pathlib import Path
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
type EntryKey = tuple[JobKey, str]


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
    prompt_file: str


@dataclass(slots=True)
class _CronEntry:
    key: JobKey
    entry_id: str
    label: str | None
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
        self._entries: dict[EntryKey, _CronEntry] = {}
        self._lock = asyncio.Lock()

    async def start(
        self,
        *,
        ctx: CommandContext,
        entry_id: str,
        label: str | None,
        spec: CronSpec,
        reply_to: MessageRef | None,
    ) -> None:
        key = _key_for(ctx)
        await self.stop(key=key, entry_id=entry_id)

        stop = asyncio.Event()
        async with self._lock:
            entry = _CronEntry(
                key=key,
                entry_id=entry_id,
                label=label,
                spec=spec,
                executor=ctx.executor,
                reply_to=reply_to,
                stop=stop,
                created_at=time.time(),
            )
            entry.task = asyncio.create_task(self._loop(entry=entry))
            self._entries[(key, entry_id)] = entry

    async def stop(self, *, key: JobKey, entry_id: str) -> bool:
        async with self._lock:
            entry = self._entries.pop((key, entry_id), None)
        if entry is None:
            return False
        await self._stop_entry(entry)
        return True

    async def stop_all(self, *, key: JobKey) -> int:
        async with self._lock:
            selected_keys = [entry_key for entry_key in self._entries if entry_key[0] == key]
            entries = [self._entries.pop(entry_key) for entry_key in selected_keys]
        for entry in entries:
            await self._stop_entry(entry)
        return len(entries)

    async def get(self, *, key: JobKey) -> list[_CronEntry]:
        async with self._lock:
            entries = [entry for (scope, _), entry in self._entries.items() if scope == key]
        return sorted(entries, key=_entry_sort_key)

    async def list(self) -> list[_CronEntry]:
        async with self._lock:
            entries = list(self._entries.values())
        return sorted(entries, key=_entry_sort_key)

    async def _stop_entry(self, entry: _CronEntry) -> None:
        entry.stop.set()
        if entry.task is not None:
            entry.task.cancel()
        try:
            if entry.task is not None:
                await entry.task
        except asyncio.CancelledError:
            # Stop is best-effort; cancellation should not leak.
            pass

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
    prefix = f"cron {entry.label} tick" if entry.label else "cron tick"
    return f"{prefix} #{entry.tick_count} (every {hours:g}h) @ {_format_ts(ts)}"


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
            stopped = await MANAGER.stop_all(key=key)
            return CommandResult(text="cron: stopped" if stopped else "cron: not running")

        if sub == "status":
            entries = await MANAGER.get(key=key)
            return CommandResult(text=_format_status(entries))

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
            if len(ctx.args) >= 2 and ctx.args[1].lower() == "seed":
                return await _start_all_seed_jobs(ctx)
            if len(ctx.args) < 3:
                raise ConfigError("Usage: /cron start <hours> <prompt...>")
            hours = _parse_hours(ctx.args[1])
            prompt_text = " ".join(ctx.args[2:]).strip()
            if not prompt_text:
                raise ConfigError("Usage: /cron start <hours> <prompt...>")
            return await _start_job(
                ctx,
                hours=hours,
                prompt_text=prompt_text,
                notify=None,
                entry_id="default",
                label=None,
            )

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


async def _start_job(
    ctx: CommandContext,
    *,
    hours: float,
    prompt_text: str,
    notify: bool | None,
    entry_id: str,
    label: str | None,
) -> CommandResult:
    spec = _resolve_spec(ctx, hours=hours, prompt_text=prompt_text, notify=notify)
    reply_to = ctx.reply_to or ctx.message
    await MANAGER.start(
        ctx=ctx,
        entry_id=entry_id,
        label=label,
        spec=spec,
        reply_to=reply_to,
    )
    if label is None:
        return CommandResult(text=f"cron: started (every {hours:g}h)")
    return CommandResult(text=f"cron: started {label} (every {hours:g}h)")


def _help_text() -> str:
    return (
        "Usage:\n"
        "/cron start <hours> <prompt...>\n"
        "/cron start seed\n"
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


def _format_status(entries: list[_CronEntry]) -> str:
    if not entries:
        return "cron: not running"
    if len(entries) == 1:
        return _format_single_status(entries[0])
    lines = [
        f"cron: running {len(entries)} jobs",
    ]
    for entry in entries:
        hours = entry.spec.every_s / 3600.0
        label = _entry_label(entry)
        summary = f"- {label}: every {hours:g}h, notify={entry.spec.notify}, ticks={entry.tick_count}"
        if entry.last_error:
            summary += f", last_error={entry.last_error}"
        lines.append(summary)
    return "\n".join(lines)


def _format_single_status(entry: _CronEntry) -> str:
    hours = entry.spec.every_s / 3600.0
    lines = [
        "cron: running",
        f"- job: {_entry_label(entry)}",
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
        lines.append(
            f"- {where} [{_entry_label(entry)}]: every {hours:g}h (ticks={entry.tick_count})"
        )
    return "\n".join(lines)


BACKEND: CommandBackend = CronCommand()


def _entry_label(entry: _CronEntry) -> str:
    return entry.label or "default"


def _entry_sort_key(entry: _CronEntry) -> tuple[str, str]:
    return (repr(entry.key), entry.entry_id)


def _config_dir(ctx: CommandContext) -> Path:
    if ctx.config_path is not None:
        return ctx.config_path.expanduser().resolve().parent
    return (Path.home() / ".takopi").resolve()


def _parse_seed_presets(ctx: CommandContext) -> list[SeedPreset]:
    config = dict(ctx.plugin_config or {})
    seeds = config.get("seed")
    if seeds is None:
        return []
    if not isinstance(seeds, list):
        raise ConfigError("Invalid `plugins.cron.seed`; expected an array of tables.")

    notify_default = _optional_bool(config, "notify")
    notify_default = True if notify_default is None else notify_default

    presets: list[SeedPreset] = []
    seen: set[str] = set()
    base_dir = _config_dir(ctx)
    for idx, raw in enumerate(seeds):
        if not isinstance(raw, dict):
            raise ConfigError(f"Invalid `plugins.cron.seed[{idx}]`; expected a table.")
        enabled = raw.get("enabled")
        if enabled is False:
            continue
        if enabled is not None and not isinstance(enabled, bool):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].enabled`; expected a boolean."
            )

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
            raise ConfigError(f"Duplicate seed id: {preset_id!r}.")
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

        prompt_file = raw.get("prompt_file")
        if not isinstance(prompt_file, str) or not prompt_file.strip():
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].prompt_file`; expected a string."
            )
        prompt_file = prompt_file.strip()
        prompt_path = Path(prompt_file.strip()).expanduser()
        if not prompt_path.is_absolute():
            prompt_path = base_dir / prompt_path
        try:
            prompt = prompt_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise ConfigError(f"Failed to read prompt_file for {preset_id!r}: {exc}") from exc
        if not prompt:
            raise ConfigError(f"Prompt file for {preset_id!r} is empty: {prompt_path}")

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
                prompt=prompt,
                notify=notify,
                prompt_file=prompt_file,
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
            f"- {idx}. {preset.id}: every {preset.every_hours:g}h (notify={preset.notify}, prompt_file={preset.prompt_file})"
        )
    return "\n".join(lines)


async def _start_all_seed_jobs(ctx: CommandContext) -> CommandResult:
    presets = _parse_seed_presets(ctx)
    if not presets:
        return CommandResult(text="cron: no seed presets")
    for preset in presets:
        await _start_job(
            ctx,
            hours=preset.every_hours,
            prompt_text=preset.prompt,
            notify=preset.notify,
            entry_id=f"seed:{preset.id}",
            label=f"seed {preset.id}",
        )
    count = len(presets)
    suffix = "job" if count == 1 else "jobs"
    return CommandResult(text=f"cron: started {count} seed {suffix}")


async def _handle_seed(ctx: CommandContext) -> CommandResult:
    if len(ctx.args) < 2:
        raise ConfigError("Usage: /cron seed list | /cron seed start <id|index>")

    action = ctx.args[1].lower()
    presets = _parse_seed_presets(ctx)

    if action == "list":
        return CommandResult(text=_format_seed_list(presets))

    if action in {"start", "on"}:
        if len(ctx.args) < 3:
            raise ConfigError("Usage: /cron seed start <id|index>")
        preset = _select_seed(presets, ctx.args[2])
        if preset is None:
            raise ConfigError(f"Unknown seed {ctx.args[2]!r}. Use `/cron seed list`.")
        return await _start_job(
            ctx,
            hours=preset.every_hours,
            prompt_text=preset.prompt,
            notify=preset.notify,
            entry_id=f"seed:{preset.id}",
            label=f"seed {preset.id}",
        )

    raise ConfigError("Usage: /cron seed list | /cron seed start <id|index>")
