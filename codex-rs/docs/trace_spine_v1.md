# Trace Spine v1 (codex_trace_spine_v1)

This document defines the local trace spine format used for eval-grade capture and deterministic
distillation into Harbor tasks.

## Goals

- Faithful, append-only capture of Codex sessions (SQ + EQ + rollouts + artifacts).
- Deterministic ordering with a writer-side monotonic `seq`.
- Compaction boundaries that preserve pre/post history for faithful reconstruction.
- First-class provenance for sub-agents and proxying bridges.

## Record envelope (JSONL)

Each line is a JSON object with the following top-level fields:

- `schema_version`: must be `"codex_trace_spine_v1"`.
- `thread_id`: session/thread UUID.
- `seq`: monotonically increasing integer assigned by the writer.
- `timestamp`: RFC3339 UTC timestamp (millisecond precision).
- `type`: record type (see below).
- `payload`: record payload (see below).

Example (shortened):

```json
{
  "schema_version": "codex_trace_spine_v1",
  "thread_id": "thread_123",
  "seq": 42,
  "timestamp": "2026-01-31T08:15:24.123Z",
  "type": "event",
  "payload": { "id": "turn_abc", "msg": { "type": "exec_command_end", "payload": { "exit_code": 0 } } }
}
```

## Record types

### 1) `session_meta`
Payload: `SessionMetaLine` (same structure as rollout, includes optional `git`).

### 2) `turn_context`
Payload:

```json
{
  "turn_id": "turn_abc",
  "context": { /* TurnContextItem */ }
}
```

Note: `TurnContextItem` in rollouts does not include `turn_id`; this record exists to make joins
deterministic.

### 3) `submission`
Payload: raw `Submission { id, op }` as received (SQ).

### 4) `event`
Payload: raw `Event { id, msg }` as emitted (EQ), including direct emitters
(exec output deltas).

### 5) `artifact_ref`
Payload: one of the artifact reference variants. Examples:

```json
{ "kind": "git_info", "repo_root": "/path", "git": { "commit_hash": "...", "branch": "...", "repository_url": "..." } }
```

```json
{ "kind": "git_diff", "base_sha": "abc123", "artifact_path": "artifacts/git_diff.patch", "sha256": "..." }
```

```json
{ "kind": "shell_snapshot", "artifact_path": "artifacts/shell_snapshot.sh", "sha256": "...", "shell": "zsh" }
```

### 6) `compaction_boundary`
Payload:

```json
{
  "turn_id": "turn_abc",
  "kind": "local" | "remote",
  "summary_message": "optional summary",
  "pre_compaction_history": [ /* ResponseItem[] */ ],
  "post_compaction_history": [ /* ResponseItem[] */ ]
}
```

### 7) `bridge`
Payload:

```json
{
  "bridge_kind": "exec_approval" | "patch_approval" | "request_user_input",
  "parent_thread_id": "thread_parent",
  "parent_turn_id": "turn_parent",
  "parent_call_id": "optional",
  "sub_agent_thread_id": "thread_child",
  "sub_agent_turn_id": "turn_child",
  "sub_agent_call_id": "call_id",
  "decision": { /* ReviewDecision (when approval) */ },
  "user_input_response": { /* RequestUserInputResponse (when user input) */ }
}
```

## Segmentation (spine/segment-XXX.jsonl)

Trace spine writers must segment output for crash safety and resumable shipping. Segments are
named `segment-000.jsonl`, `segment-001.jsonl`, ... and stored in a per-session directory.
Each segment also writes a `segment-XXX.meta.json` file with:

- `schema_version`
- `thread_id`
- `segment_index`
- `min_seq` / `max_seq`
- `record_count` / `bytes`
- `sha256`
- `created_at` / `closed_at`

## Rollout+ bundle layout (v0)

```
bundle/
  bundle.json
  spine/
    segment-000.jsonl
    segment-000.meta.json
    ...
  rollout/
    rollout-<timestamp>-<thread_id>.jsonl
  artifacts/
    git_diff.patch
    shell_snapshot.sh
    ...
```

`bundle.json` should include at least:

- `bundle_version`: `"rollout_plus_v0"`
- `thread_id`
- `created_at`
- `spine_schema_version`
- `rollout_path` (original path)

## Validator requirements

A minimal validator should enforce:

- Each JSONL line parses and includes required envelope fields.
- `schema_version` matches `"codex_trace_spine_v1"`.
- `thread_id` is consistent per segment.
- `seq` is strictly increasing within each segment.
- `segment-XXX.meta.json` matches computed `sha256`, `min_seq`, `max_seq`, and counts.
- `compaction_boundary` records include both pre/post history snapshots.

