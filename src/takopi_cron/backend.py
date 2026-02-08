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
    ExecBridgeConfig,
    HOME_CONFIG_PATH,
    IncomingMessage,
    MessageRef,
    RenderedMessage,
    RunContext,
    RunRequest,
    RunResult,
    RunnerUnavailableError,
    SendOptions,
    bind_run_context,
    clear_context,
    get_logger,
    handle_message,
    load_settings,
    read_config,
    reset_run_base_dir,
    set_run_base_dir,
)

logger = get_logger(__name__)

type ChannelId = int | str
type ThreadId = int | str | None
type JobKey = tuple[ChannelId, ThreadId]

_SEED_TASK: asyncio.Task[None] | None = None


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


class _CaptureTransport:
    def __init__(self) -> None:
        self._next_id = 1
        self.last_message: RenderedMessage | None = None

    async def send(
        self,
        *,
        channel_id: int | str,
        message: RenderedMessage,
        options: SendOptions | None = None,
    ) -> MessageRef:
        thread_id = options.thread_id if options is not None else None
        ref = MessageRef(channel_id=channel_id, message_id=self._next_id)
        self._next_id += 1
        self.last_message = message
        return MessageRef(
            channel_id=ref.channel_id,
            message_id=ref.message_id,
            thread_id=thread_id,
        )

    async def edit(
        self, *, ref: MessageRef, message: RenderedMessage, wait: bool = True
    ) -> MessageRef:
        _ = wait
        self.last_message = message
        return ref

    async def delete(self, *, ref: MessageRef) -> bool:
        _ = ref
        return True

    async def close(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class _SeedJob:
    channel_id: ChannelId
    thread_id: ThreadId
    reply_to: MessageRef | None
    every_s: float
    prompt: str
    notify: bool


@dataclass(slots=True)
class _SeedExecutor(CommandExecutor):
    runtime: Any
    transport: Any
    presenter: Any
    channel_id: ChannelId
    thread_id: ThreadId

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
        return await self.transport.send(
            channel_id=self.channel_id,
            message=rendered,
            options=SendOptions(
                reply_to=reply_to,
                notify=notify,
                thread_id=self.thread_id,
            ),
        )

    async def run_one(
        self, request: RunRequest, *, mode: str = "emit"
    ) -> RunResult:
        if mode != "capture":
            raise RuntimeError("Seeded cron jobs only support mode=capture")

        engine = self.runtime.resolve_engine(
            engine_override=request.engine,
            context=request.context,
        )
        try:
            entry = self.runtime.resolve_runner(
                resume_token=None,
                engine_override=engine,
            )
        except RunnerUnavailableError as exc:
            return RunResult(engine=engine, message=RenderedMessage(text=f"error:\n{exc}"))

        if not entry.available:
            reason = entry.issue or "engine unavailable"
            return RunResult(engine=engine, message=RenderedMessage(text=f"error:\n{reason}"))

        try:
            cwd = self.runtime.resolve_run_cwd(request.context)
        except ConfigError as exc:
            return RunResult(engine=engine, message=RenderedMessage(text=f"error:\n{exc}"))

        run_base_token = set_run_base_dir(cwd)
        try:
            bind_run_context(
                engine=engine,
                project=request.context.project if request.context is not None else None,
                branch=request.context.branch if request.context is not None else None,
                cwd=str(cwd) if cwd is not None else None,
            )
            capture = _CaptureTransport()
            exec_cfg = ExecBridgeConfig(
                transport=capture,
                presenter=self.presenter,
                final_notify=False,
            )
            context_line = self.runtime.format_context_line(request.context)
            incoming = IncomingMessage(
                channel_id=self.channel_id,
                message_id=0,
                text=request.prompt,
                thread_id=self.thread_id,
            )
            await handle_message(
                exec_cfg,
                runner=entry.runner,
                incoming=incoming,
                resume_token=None,
                context=request.context,
                context_line=context_line,
                strip_resume_line=self.runtime.is_resume_line,
                running_tasks=None,
                on_thread_known=None,
            )
            return RunResult(engine=engine, message=capture.last_message)
        finally:
            reset_run_base_dir(run_base_token)
            clear_context()

    async def run_many(self, *args: Any, **kwargs: Any) -> list[RunResult]:
        _ = args, kwargs
        raise NotImplementedError


def _parse_seed_jobs(config: dict[str, Any]) -> list[_SeedJob]:
    plugins = config.get("plugins")
    if not isinstance(plugins, dict):
        return []
    cron_cfg = plugins.get("cron")
    if not isinstance(cron_cfg, dict):
        return []
    seeds = cron_cfg.get("seed")
    if seeds is None:
        return []
    if not isinstance(seeds, list):
        raise ConfigError("Invalid `plugins.cron.seed`; expected an array of tables.")

    notify_default = _optional_bool(cron_cfg, "notify")
    notify_default = True if notify_default is None else notify_default

    jobs: list[_SeedJob] = []
    for idx, raw in enumerate(seeds):
        if not isinstance(raw, dict):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}]`; expected a table."
            )
        if raw.get("enabled") is False:
            continue

        channel_id = raw.get("channel_id")
        if channel_id is None:
            channel_id = raw.get("chat_id")
        if not isinstance(channel_id, (int, str)):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].chat_id`; expected int or str."
            )

        thread_id = raw.get("thread_id")
        if thread_id is not None and not isinstance(thread_id, (int, str)):
            raise ConfigError(
                f"Invalid `plugins.cron.seed[{idx}].thread_id`; expected int or str."
            )

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

        reply_to = None
        reply_to_id = raw.get("reply_to_message_id")
        if reply_to_id is not None:
            if not isinstance(reply_to_id, int):
                raise ConfigError(
                    f"Invalid `plugins.cron.seed[{idx}].reply_to_message_id`; expected int."
                )
            reply_to = MessageRef(
                channel_id=channel_id,
                message_id=reply_to_id,
                thread_id=thread_id,
            )

        jobs.append(
            _SeedJob(
                channel_id=channel_id,
                thread_id=thread_id,
                reply_to=reply_to,
                every_s=float(every_hours) * 3600.0,
                prompt=prompt.strip(),
                notify=notify,
            )
        )
    return jobs


async def _start_seeded_jobs() -> None:
    try:
        raw = read_config(HOME_CONFIG_PATH)
    except ConfigError as exc:
        logger.debug("cron.seed.config_unavailable", error=str(exc))
        return
    try:
        jobs = _parse_seed_jobs(raw)
    except ConfigError as exc:
        logger.warning("cron.seed.config_invalid", error=str(exc))
        return
    if not jobs:
        return

    try:
        settings, config_path = load_settings()
    except ConfigError as exc:
        logger.warning("cron.seed.settings_invalid", error=str(exc))
        return

    if settings.transport != "telegram":
        logger.info("cron.seed.skip_transport", transport=settings.transport)
        return

    # Import transport-specific pieces lazily.
    from takopi.runtime_loader import build_runtime_spec
    from takopi.telegram.bridge import TelegramPresenter, TelegramTransport
    from takopi.telegram.client_api import HttpBotClient

    try:
        spec = build_runtime_spec(settings=settings, config_path=config_path)
    except ConfigError as exc:
        logger.warning("cron.seed.runtime_invalid", error=str(exc))
        return
    runtime = spec.to_runtime(config_path=config_path)

    presenter = TelegramPresenter(message_overflow=settings.transports.telegram.message_overflow)
    transport = TelegramTransport(HttpBotClient(settings.transports.telegram.bot_token))

    for job in jobs:
        if not isinstance(job.channel_id, int):
            logger.warning("cron.seed.skip_job_channel_type", channel_id=str(job.channel_id))
            continue
        if job.thread_id is not None and not isinstance(job.thread_id, int):
            logger.warning("cron.seed.skip_job_thread_type", thread_id=str(job.thread_id))
            continue
        resolved = runtime.resolve_message(
            text=job.prompt,
            reply_text=None,
            ambient_context=None,
            chat_id=job.channel_id,
        )
        spec = CronSpec(
            every_s=job.every_s,
            prompt=resolved.prompt or "continue",
            engine=resolved.engine_override,
            context=resolved.context,
            notify=job.notify,
        )
        executor = _SeedExecutor(
            runtime=runtime,
            transport=transport,
            presenter=presenter,
            channel_id=job.channel_id,
            thread_id=job.thread_id,
        )
        ctx = CommandContext(
            command="cron",
            text="",
            args_text="",
            args=(),
            message=MessageRef(
                channel_id=job.channel_id,
                message_id=0,
                thread_id=job.thread_id,
            ),
            reply_to=None,
            reply_text=None,
            config_path=config_path,
            plugin_config={},
            runtime=runtime,
            executor=executor,
        )
        await MANAGER.start(ctx=ctx, spec=spec, reply_to=job.reply_to)


def _maybe_schedule_seeded_jobs() -> None:
    global _SEED_TASK
    if _SEED_TASK is not None:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _SEED_TASK = asyncio.create_task(_start_seeded_jobs())


_maybe_schedule_seeded_jobs()
