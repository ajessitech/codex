# Trace spine + rollout+ bundles

Codex records an eval-grade trace spine for each session. You can export a portable
rollout+ bundle, distill a trace segment into a Harbor task, and package tasks into
a dataset for repeated evaluation.

## Export a rollout+ bundle

```bash
codex trace bundle \
  --session-id <thread_uuid> \
  --output /tmp/rollout-plus
```

This produces:

```text
/tmp/rollout-plus/
  bundle.json
  spine/
    segment-000.jsonl
    segment-000.meta.json
  rollout/
    rollout-<timestamp>-<thread_id>.jsonl
  artifacts/
    git_diff.patch
    shell_snapshot.sh
```

## Distill a segment into a Harbor task

```bash
codex trace distill \
  --bundle /tmp/rollout-plus \
  --output /tmp/harbor-task \
  --start-turn-id <turn_id> \
  --end-turn-id <turn_id>
```

You can also select by seq range with `--start-seq` and `--end-seq`.

The output task includes `instruction.md`, `environment/Dockerfile`, `tests/test.sh`,
and `task.toml` with evidence pointers back to the trace spine.

## Package tasks into a dataset (optional Harbor run)

```bash
codex trace dataset \
  --output /tmp/harbor-dataset \
  --task /tmp/harbor-task \
  --agent <agent> \
  --model <model> \
  --run
```

This creates a dataset folder and (if `--run`) executes `harbor run` with the selected
agent/model. A `trace_evidence.json` file records task metadata for later auditing.

## Notes

- Trace data is stored under `~/.codex/trace_spine/<thread_id>`.
- `CODEX_HOME` can override the default `~/.codex` location.
