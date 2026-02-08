# takopi-cron

cron command plugin for takopi. run a prompt on an interval and post results back into your chat.

This is intentionally simple:

- It runs **in-process** inside Takopi (so Takopi must be running).
- It does **not** survive restarts (you re-run `/cron start` after restart).

## requirements

`uv` for installation (`curl -LsSf https://astral.sh/uv/install.sh | sh`)

python 3.14+ (`uv python install 3.14`)

takopi installed

## install

recommended (installs the plugin into takopi's `uv tool` environment):

```sh
uv tool install -U takopi --with takopi-cron
```

if you installed takopi via `pip` (or you're running takopi from a project venv), install the plugin into the same environment:

```sh
pip install -U takopi-cron
```

enable it:

```toml
[plugins]
enabled = ["takopi-cron"]
```

## usage

Start a job (runs once immediately, then repeats):

```text
/cron start 6 Write a summary of today's PRs and what I should review next.
```

Stop the job for the current chat/thread:

```text
/cron stop
```

Status:

```text
/cron status
```

List all running cron jobs:

```text
/cron list
```

Run once (no scheduling):

```text
/cron run What changed since yesterday?
```

Tips:

- Put an engine directive at the start of the prompt, e.g. `/claude ...`
- Put a project directive at the start of the prompt, e.g. `/myproj ...`
- Put a branch directive at the start of the prompt, e.g. `@feat/foo ...`

## config

In `~/.takopi/takopi.toml`:

```toml
[plugins.cron]
# Optional: restrict who can control cron jobs
allowed_user_ids = [12345678]

# Optional: whether cron ticks should notify (default: true)
notify = true

# Optional: seed jobs that start automatically when takopi starts (telegram only)
[[plugins.cron.seed]]
chat_id = -1001234567890
thread_id = 42 # optional (telegram topics)
every_hours = 6
prompt = "/codex summarize what changed since the last cron tick"
# notify = true            # optional override
# reply_to_message_id = 1  # optional
```
