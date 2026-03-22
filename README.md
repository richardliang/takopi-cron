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

Seed presets (from seed files):

```text
/cron seed list
/cron seed start daily_summary
/cron start seed
```

Seed presets start in the chat/thread where you run the command. `/cron start seed`
starts every seed preset at once in that chat/thread.

If you start multiple seed jobs in one chat/thread, `/cron stop` stops all of them
for that chat/thread.

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

# Optional: where seed preset files live. Defaults to ~/.takopi/cron-seeds.
# Relative paths resolve from the directory containing takopi.toml.
# seed_dir = "cron-seeds"
```

Seed presets are no longer defined inline in `takopi.toml`. Put them in
`~/.takopi/cron-seeds/` instead.

Why use a per-seed `.toml` file plus a prompt file?

- `takopi.toml` only needs one link: `seed_dir`
- each seed still needs metadata like `every_hours`, `notify`, and an optional `id`
- keeping that metadata next to the prompt file avoids re-growing `takopi.toml` as
  you add more seeds

In other words: the directory is linked from the main TOML, not each seed
individually.

Example seed files:

```text
~/.takopi/
  takopi.toml
  cron-seeds/
    daily_summary.toml
    daily_summary.prompt.md
```

`~/.takopi/cron-seeds/daily_summary.toml`

```toml
every_hours = 6
prompt_file = "daily_summary.prompt.md"
# id = "daily_summary"     # optional; defaults to the file name
# notify = true            # optional override
# enabled = false          # optional
```

`~/.takopi/cron-seeds/daily_summary.prompt.md`

```text
/codex summarize what changed since the last cron tick
```
