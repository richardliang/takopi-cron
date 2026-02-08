# takopi-cron

`takopi-cron` is a Takopi **command plugin** that can run a prompt on an interval
and post results back into your chat.

This is intentionally simple:

- It runs **in-process** inside Takopi (so Takopi must be running).
- It does **not** survive restarts (you re-run `/cron start` after restart).

## Install

```sh
pip install takopi-cron
```

Enable it (optional allowlist):

```toml
[plugins]
enabled = ["takopi-cron"]
```

## Usage

Start a job (runs once immediately, then repeats):

```text
/cron start 6 Write a summary of today’s PRs and what I should review next.
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

## Config

In `~/.takopi/takopi.toml`:

```toml
[plugins.cron]
# Optional: restrict who can control cron jobs
allowed_user_ids = [12345678]

# Optional: whether cron ticks should notify (default: true)
notify = true
```

