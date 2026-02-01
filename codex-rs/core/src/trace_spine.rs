//! Trace spine: eval-grade, append-only capture for deterministic distillation.

use std::collections::BTreeMap;
use std::io::Error as IoError;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use codex_protocol::ThreadId;
use codex_protocol::models::BaseInstructions;
use codex_protocol::models::ResponseItem;
use codex_protocol::protocol::Event;
use codex_protocol::protocol::GitInfo;
use codex_protocol::protocol::ReviewDecision;
use codex_protocol::protocol::SessionMeta;
use codex_protocol::protocol::SessionMetaLine;
use codex_protocol::protocol::SessionSource;
use codex_protocol::protocol::Submission;
use codex_protocol::protocol::TurnContextItem;
use codex_protocol::request_user_input::RequestUserInputResponse;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
use time::format_description::FormatItem;
use time::macros::format_description;
use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc;
use tokio::sync::mpsc::Sender;
use tokio::sync::oneshot;
use tracing::warn;

use crate::config::Config;
use crate::default_client::originator;
use crate::git_info::{GitDiffToRemote, collect_git_info, git_diff_to_remote};

pub const TRACE_SPINE_SCHEMA_VERSION: &str = "codex_trace_spine_v1";
pub const ROLLOUT_PLUS_BUNDLE_VERSION: &str = "rollout_plus_v0";
pub const TRACE_SPINE_SUBDIR: &str = "trace_spine";

const SEGMENT_MAX_BYTES: u64 = 5 * 1024 * 1024;
const SEGMENT_MAX_RECORDS: u64 = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceSpineLine {
    pub schema_version: String,
    pub thread_id: ThreadId,
    pub seq: u64,
    pub timestamp: String,
    #[serde(flatten)]
    pub item: TraceSpineItem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload", rename_all = "snake_case")]
pub enum TraceSpineItem {
    SessionMeta(SessionMetaLine),
    TurnContext(TraceTurnContextRecord),
    Submission(Submission),
    Event(Event),
    ArtifactRef(TraceArtifactRef),
    CompactionBoundary(TraceCompactionBoundary),
    Bridge(TraceBridgeRecord),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceTurnContextRecord {
    pub turn_id: String,
    pub context: TurnContextItem,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompactionKind {
    Local,
    Remote,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceCompactionBoundary {
    pub turn_id: String,
    pub kind: CompactionKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary_message: Option<String>,
    pub pre_compaction_history: Vec<ResponseItem>,
    pub post_compaction_history: Vec<ResponseItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceBridgeKind {
    ExecApproval,
    PatchApproval,
    RequestUserInput,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TraceBridgeRecord {
    pub bridge_kind: TraceBridgeKind,
    pub parent_thread_id: ThreadId,
    pub parent_turn_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_call_id: Option<String>,
    pub sub_agent_thread_id: ThreadId,
    pub sub_agent_turn_id: String,
    pub sub_agent_call_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decision: Option<ReviewDecision>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_input_response: Option<RequestUserInputResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TraceArtifactRef {
    GitInfo {
        #[serde(skip_serializing_if = "Option::is_none")]
        repo_root: Option<PathBuf>,
        #[serde(skip_serializing_if = "Option::is_none")]
        git: Option<GitInfo>,
    },
    GitDiff {
        #[serde(skip_serializing_if = "Option::is_none")]
        base_sha: Option<String>,
        artifact_path: String,
        sha256: String,
    },
    ShellSnapshot {
        artifact_path: String,
        sha256: String,
        shell: String,
    },
}

#[derive(Clone)]
pub(crate) struct TraceSpineRecorder {
    state: Arc<TraceSpineRecorderState>,
}

struct TraceSpineRecorderState {
    tx: Sender<TraceSpineCmd>,
    spine_dir: PathBuf,
    artifacts_dir: PathBuf,
    thread_id: ThreadId,
    seq: AtomicU64,
}

enum TraceSpineCmd {
    Add(TraceSpineLine),
    Flush { ack: oneshot::Sender<()> },
    Shutdown { ack: oneshot::Sender<()> },
}

pub(crate) enum TraceSpineRecorderParams {
    Create {
        conversation_id: ThreadId,
        forked_from_id: Option<ThreadId>,
        source: SessionSource,
        base_instructions: BaseInstructions,
    },
    Resume { conversation_id: ThreadId },
}

impl TraceSpineRecorder {
    pub async fn new(config: &Config, params: TraceSpineRecorderParams) -> std::io::Result<Self> {
        let (spine_dir, conversation_id, session_meta, last_seq, next_segment_index) = match params {
            TraceSpineRecorderParams::Create {
                conversation_id,
                forked_from_id,
                source,
                base_instructions,
            } => {
                let spine_dir = create_spine_dir(config, conversation_id)?;
                let timestamp = format_timestamp(OffsetDateTime::now_utc())?;
                let session_meta = SessionMeta {
                    id: conversation_id,
                    forked_from_id,
                    timestamp,
                    cwd: config.cwd.clone(),
                    originator: originator().value,
                    cli_version: env!("CARGO_PKG_VERSION").to_string(),
                    source,
                    model_provider: Some(config.model_provider_id.clone()),
                    base_instructions: Some(base_instructions),
                };
                (spine_dir, conversation_id, Some(session_meta), 0, 0)
            }
            TraceSpineRecorderParams::Resume { conversation_id } => {
                let spine_dir = trace_spine_dir(config, conversation_id);
                let (last_seq, next_segment_index) = read_spine_resume_state(&spine_dir).await?;
                (spine_dir, conversation_id, None, last_seq, next_segment_index)
            }
        };

        tokio::fs::create_dir_all(&spine_dir).await?;
        let artifacts_dir = spine_dir.join("artifacts");
        tokio::fs::create_dir_all(&artifacts_dir).await?;

        let (tx, rx) = mpsc::channel::<TraceSpineCmd>(256);
        tokio::task::spawn(trace_spine_writer(
            spine_dir.clone(),
            rx,
            next_segment_index,
        ));

        let recorder = Self {
            state: Arc::new(TraceSpineRecorderState {
                tx,
                spine_dir: spine_dir.clone(),
                artifacts_dir,
                thread_id: conversation_id,
                seq: AtomicU64::new(last_seq),
            }),
        };

        if let Some(meta) = session_meta {
            let meta_line = TraceSpineItem::SessionMeta(SessionMetaLine { meta, git: None });
            recorder.record_item(meta_line).await?;
        }

        Ok(recorder)
    }

    pub fn spine_dir(&self) -> &Path {
        self.state.spine_dir.as_path()
    }

    pub fn artifacts_dir(&self) -> &Path {
        self.state.artifacts_dir.as_path()
    }

    pub async fn record_item(&self, item: TraceSpineItem) -> std::io::Result<()> {
        let seq = self.state.seq.fetch_add(1, Ordering::SeqCst).saturating_add(1);
        let line = TraceSpineLine {
            schema_version: TRACE_SPINE_SCHEMA_VERSION.to_string(),
            thread_id: self.state.thread_id,
            seq,
            timestamp: format_timestamp(OffsetDateTime::now_utc())?,
            item,
        };
        self.state
            .tx
            .send(TraceSpineCmd::Add(line))
            .await
            .map_err(|e| IoError::other(format!("failed to queue trace spine item: {e}")))
    }

    pub async fn record_submission(&self, submission: &Submission) -> std::io::Result<()> {
        self.record_item(TraceSpineItem::Submission(submission.clone()))
            .await
    }

    pub async fn record_event(&self, event: &Event) -> std::io::Result<()> {
        self.record_item(TraceSpineItem::Event(event.clone())).await
    }

    pub async fn record_turn_context(
        &self,
        turn_id: String,
        context: TurnContextItem,
    ) -> std::io::Result<()> {
        self.record_item(TraceSpineItem::TurnContext(TraceTurnContextRecord {
            turn_id,
            context,
        }))
        .await
    }

    pub async fn record_compaction_boundary(
        &self,
        boundary: TraceCompactionBoundary,
    ) -> std::io::Result<()> {
        self.record_item(TraceSpineItem::CompactionBoundary(boundary))
            .await
    }

    pub async fn record_bridge(&self, record: TraceBridgeRecord) -> std::io::Result<()> {
        self.record_item(TraceSpineItem::Bridge(record)).await
    }

    pub async fn record_git_info(
        &self,
        cwd: &Path,
        repo_root: Option<PathBuf>,
    ) -> std::io::Result<()> {
        let git = collect_git_info(cwd).await;
        self.record_item(TraceSpineItem::ArtifactRef(TraceArtifactRef::GitInfo {
            repo_root,
            git,
        }))
        .await
    }

    pub async fn record_git_diff(&self, cwd: &Path) -> std::io::Result<Option<GitDiffToRemote>> {
        let diff = git_diff_to_remote(cwd).await;
        let Some(diff) = diff else {
            return Ok(None);
        };
        let (artifact_path, sha256) =
            self.write_text_artifact("git_diff.patch", diff.diff.as_str())
                .await?;
        self.record_item(TraceSpineItem::ArtifactRef(TraceArtifactRef::GitDiff {
            base_sha: Some(diff.sha.0.clone()),
            artifact_path,
            sha256,
        }))
        .await?;
        Ok(Some(diff))
    }

    pub async fn record_shell_snapshot(
        &self,
        snapshot_path: &Path,
        shell_name: &str,
    ) -> std::io::Result<()> {
        let ext = snapshot_path
            .extension()
            .and_then(|s| s.to_str())
            .unwrap_or(shell_name);
        let filename = format!("shell_snapshot.{ext}");
        let (artifact_path, sha256) = self.copy_artifact(snapshot_path, &filename).await?;
        self.record_item(TraceSpineItem::ArtifactRef(
            TraceArtifactRef::ShellSnapshot {
                artifact_path,
                sha256,
                shell: shell_name.to_string(),
            },
        ))
        .await
    }

    pub async fn flush(&self) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.state
            .tx
            .send(TraceSpineCmd::Flush { ack: tx })
            .await
            .map_err(|e| IoError::other(format!("failed to queue trace spine flush: {e}")))?;
        rx.await
            .map_err(|e| IoError::other(format!("failed waiting for trace spine flush: {e}")))
    }

    pub async fn shutdown(&self) -> std::io::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.state
            .tx
            .send(TraceSpineCmd::Shutdown { ack: tx })
            .await
            .map_err(|e| IoError::other(format!("failed to queue trace spine shutdown: {e}")))?;
        rx.await
            .map_err(|e| IoError::other(format!("failed waiting for trace spine shutdown: {e}")))
    }

    async fn write_text_artifact(
        &self,
        filename: &str,
        contents: &str,
    ) -> std::io::Result<(String, String)> {
        let path = self.state.artifacts_dir.join(filename);
        tokio::fs::write(&path, contents).await?;
        let sha256 = sha256_hex(contents.as_bytes());
        Ok((format!("artifacts/{filename}"), sha256))
    }

    async fn copy_artifact(
        &self,
        source_path: &Path,
        filename: &str,
    ) -> std::io::Result<(String, String)> {
        let dest_path = self.state.artifacts_dir.join(filename);
        let bytes = tokio::fs::read(source_path).await?;
        tokio::fs::write(&dest_path, &bytes).await?;
        let sha256 = sha256_hex(&bytes);
        Ok((format!("artifacts/{filename}"), sha256))
    }
}

#[derive(Serialize)]
struct SegmentMeta {
    schema_version: String,
    thread_id: ThreadId,
    segment_index: u64,
    min_seq: u64,
    max_seq: u64,
    record_count: u64,
    bytes: u64,
    sha256: String,
    created_at: String,
    closed_at: String,
}

struct SegmentWriter {
    file: tokio::fs::File,
    segment_index: u64,
    segment_min_seq: u64,
    segment_max_seq: u64,
    segment_record_count: u64,
    segment_bytes: u64,
    created_at: String,
    hasher: Sha256,
    spine_dir: PathBuf,
}

impl SegmentWriter {
    async fn new(spine_dir: PathBuf, segment_index: u64) -> std::io::Result<Self> {
        let segment_path = segment_path(&spine_dir, segment_index);
        let file = tokio::fs::OpenOptions::new()
            .append(true)
            .create(true)
            .open(&segment_path)
            .await?;
        Ok(Self {
            file,
            segment_index,
            segment_min_seq: 0,
            segment_max_seq: 0,
            segment_record_count: 0,
            segment_bytes: 0,
            created_at: format_timestamp(OffsetDateTime::now_utc())?,
            hasher: Sha256::new(),
            spine_dir,
        })
    }

    async fn write_line(&mut self, line: &TraceSpineLine) -> std::io::Result<()> {
        let mut json = serde_json::to_string(line)?;
        json.push('\n');
        let bytes = json.as_bytes();
        self.file.write_all(bytes).await?;
        self.file.flush().await?;
        self.segment_bytes = self.segment_bytes.saturating_add(bytes.len() as u64);
        self.segment_record_count = self.segment_record_count.saturating_add(1);
        self.segment_max_seq = line.seq;
        if self.segment_min_seq == 0 {
            self.segment_min_seq = line.seq;
        }
        self.hasher.update(bytes);
        Ok(())
    }

    fn should_rotate(&self) -> bool {
        self.segment_bytes >= SEGMENT_MAX_BYTES || self.segment_record_count >= SEGMENT_MAX_RECORDS
    }

    async fn finish(&mut self, thread_id: ThreadId) -> std::io::Result<()> {
        if self.segment_record_count == 0 {
            return Ok(());
        }
        let sha256 = format!("{:x}", self.hasher.finalize_reset());
        let closed_at = format_timestamp(OffsetDateTime::now_utc())?;
        let meta = SegmentMeta {
            schema_version: TRACE_SPINE_SCHEMA_VERSION.to_string(),
            thread_id,
            segment_index: self.segment_index,
            min_seq: self.segment_min_seq,
            max_seq: self.segment_max_seq,
            record_count: self.segment_record_count,
            bytes: self.segment_bytes,
            sha256,
            created_at: self.created_at.clone(),
            closed_at,
        };
        let meta_path = segment_meta_path(&self.spine_dir, self.segment_index);
        let json = serde_json::to_string_pretty(&meta)?;
        tokio::fs::write(meta_path, json).await?;
        Ok(())
    }
}

async fn trace_spine_writer(
    spine_dir: PathBuf,
    mut rx: mpsc::Receiver<TraceSpineCmd>,
    mut segment_index: u64,
) -> std::io::Result<()> {
    let mut writer = SegmentWriter::new(spine_dir.clone(), segment_index).await?;
    let mut thread_id: Option<ThreadId> = None;
    while let Some(cmd) = rx.recv().await {
        match cmd {
            TraceSpineCmd::Add(line) => {
                if thread_id.is_none() {
                    thread_id = Some(line.thread_id);
                }
                writer.write_line(&line).await?;
                if writer.should_rotate() {
                    writer.finish(line.thread_id).await?;
                    segment_index = segment_index.saturating_add(1);
                    writer = SegmentWriter::new(spine_dir.clone(), segment_index).await?;
                }
            }
            TraceSpineCmd::Flush { ack } => {
                if let Err(e) = writer.file.flush().await {
                    let _ = ack.send(());
                    return Err(e);
                }
                let _ = ack.send(());
            }
            TraceSpineCmd::Shutdown { ack } => {
                if let Some(id) = thread_id {
                    writer.finish(id).await?;
                }
                let _ = ack.send(());
                break;
            }
        }
    }
    Ok(())
}

fn segment_path(spine_dir: &Path, index: u64) -> PathBuf {
    spine_dir.join(format!("segment-{index:03}.jsonl"))
}

fn segment_meta_path(spine_dir: &Path, index: u64) -> PathBuf {
    spine_dir.join(format!("segment-{index:03}.meta.json"))
}

fn trace_spine_dir(config: &Config, conversation_id: ThreadId) -> PathBuf {
    config.codex_home.join(TRACE_SPINE_SUBDIR).join(conversation_id.to_string())
}

fn create_spine_dir(config: &Config, conversation_id: ThreadId) -> std::io::Result<PathBuf> {
    let dir = trace_spine_dir(config, conversation_id);
    std::fs::create_dir_all(&dir)?;
    Ok(dir)
}

async fn read_spine_resume_state(spine_dir: &Path) -> std::io::Result<(u64, u64)> {
    let mut segments = list_segment_files(spine_dir)?;
    if segments.is_empty() {
        return Ok((0, 0));
    }
    segments.sort_by_key(|(index, _)| *index);
    let (last_index, last_path) = segments[segments.len() - 1].clone();
    let content = tokio::fs::read_to_string(&last_path).await?;
    let last_seq = content
        .lines()
        .rev()
        .find_map(|line| serde_json::from_str::<TraceSpineLine>(line).ok())
        .map(|line| line.seq)
        .unwrap_or(0);
    Ok((last_seq, last_index.saturating_add(1)))
}

fn list_segment_files(spine_dir: &Path) -> std::io::Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !spine_dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(spine_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !file_name.starts_with("segment-") || !file_name.ends_with(".jsonl") {
            continue;
        }
        let index_str = file_name
            .trim_start_matches("segment-")
            .trim_end_matches(".jsonl");
        if let Ok(index) = index_str.parse::<u64>() {
            out.push((index, path));
        }
    }
    Ok(out)
}

fn format_timestamp(timestamp: OffsetDateTime) -> std::io::Result<String> {
    let fmt: &[FormatItem] = format_description!(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:3]Z"
    );
    timestamp
        .format(fmt)
        .map_err(|e| IoError::other(format!("failed to format timestamp: {e}")))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn bundle_manifest(
    thread_id: ThreadId,
    rollout_path: &Path,
    spine_schema_version: &str,
) -> BTreeMap<String, String> {
    let mut map = BTreeMap::new();
    map.insert(
        "bundle_version".to_string(),
        ROLLOUT_PLUS_BUNDLE_VERSION.to_string(),
    );
    map.insert("thread_id".to_string(), thread_id.to_string());
    map.insert(
        "created_at".to_string(),
        format_timestamp(OffsetDateTime::now_utc()).unwrap_or_else(|_| "unknown".to_string()),
    );
    map.insert("spine_schema_version".to_string(), spine_schema_version.to_string());
    map.insert("rollout_path".to_string(), rollout_path.display().to_string());
    map
}

pub(crate) async fn record_artifacts_on_startup(
    trace: TraceSpineRecorder,
    cwd: PathBuf,
) -> std::io::Result<()> {
    let repo_root = crate::git_info::get_git_repo_root(&cwd);
    let _ = trace.record_git_info(&cwd, repo_root).await;
    let _ = trace.record_git_diff(&cwd).await;
    Ok(())
}

pub(crate) async fn record_shell_snapshot_artifact(
    trace: TraceSpineRecorder,
    snapshot_path: PathBuf,
    shell_name: String,
) {
    if let Err(err) = trace
        .record_shell_snapshot(snapshot_path.as_path(), shell_name.as_str())
        .await
    {
        warn!("failed to record shell snapshot artifact: {err}");
    }
}

#[derive(Debug, Clone)]
pub struct SegmentSelector {
    pub start_turn_id: Option<String>,
    pub end_turn_id: Option<String>,
    pub start_seq: Option<u64>,
    pub end_seq: Option<u64>,
}

/// Optional metadata for distillation (from codex-loop integration)
#[derive(Debug, Clone, Default)]
pub struct DistillMetadata {
    /// Signal type: "friction" or "delight"
    pub signal_type: Option<String>,
    /// Cluster ID for pattern grouping
    pub cluster_id: Option<String>,
    /// Signal category (e.g., "repeated_denial", "exec_failure")
    pub signal_category: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DistillSummary {
    pub thread_id: ThreadId,
    pub start_seq: u64,
    pub end_seq: u64,
    pub task_dir: PathBuf,
}

pub async fn export_rollout_plus_bundle(
    codex_home: &Path,
    thread_id: ThreadId,
    output_dir: &Path,
) -> std::io::Result<PathBuf> {
    if output_dir.exists() {
        if std::fs::read_dir(output_dir)?.next().is_some() {
            return Err(IoError::other("output directory is not empty"));
        }
    } else {
        tokio::fs::create_dir_all(output_dir).await?;
    }

    let spine_dir = trace_spine_dir_from_home(codex_home, thread_id);
    if !spine_dir.exists() {
        return Err(IoError::other("trace spine directory not found"));
    }

    let rollout_path = crate::find_thread_path_by_id_str(codex_home, &thread_id.to_string())
        .await?
        .ok_or_else(|| IoError::other("rollout file not found"))?;

    let spine_out = output_dir.join("spine");
    let rollout_out = output_dir.join("rollout");
    let artifacts_out = output_dir.join("artifacts");
    tokio::fs::create_dir_all(&spine_out).await?;
    tokio::fs::create_dir_all(&rollout_out).await?;
    tokio::fs::create_dir_all(&artifacts_out).await?;

    for entry in std::fs::read_dir(&spine_dir)? {
        let entry = entry?;
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if name.starts_with("segment-") && name.ends_with(".jsonl") {
            tokio::fs::copy(&path, spine_out.join(name)).await?;
        } else if name.starts_with("segment-") && name.ends_with(".meta.json") {
            tokio::fs::copy(&path, spine_out.join(name)).await?;
        } else if name == "artifacts" {
            copy_dir_recursive(&path, &artifacts_out).await?;
        }
    }

    let rollout_name = rollout_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "rollout.jsonl".to_string());
    tokio::fs::copy(&rollout_path, rollout_out.join(rollout_name)).await?;

    let manifest = bundle_manifest(thread_id, &rollout_path, TRACE_SPINE_SCHEMA_VERSION);
    let manifest_path = output_dir.join("bundle.json");
    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    tokio::fs::write(manifest_path, manifest_json).await?;

    Ok(output_dir.to_path_buf())
}

/// Distill a bundle to a Harbor task (without extra metadata)
pub async fn distill_bundle_to_task(
    bundle_dir: &Path,
    output_dir: &Path,
    selector: SegmentSelector,
) -> std::io::Result<DistillSummary> {
    distill_bundle_to_task_with_metadata(bundle_dir, output_dir, selector, DistillMetadata::default()).await
}

/// Distill a bundle to a Harbor task with additional metadata from codex-loop
pub async fn distill_bundle_to_task_with_metadata(
    bundle_dir: &Path,
    output_dir: &Path,
    selector: SegmentSelector,
    metadata: DistillMetadata,
) -> std::io::Result<DistillSummary> {
    if output_dir.exists() {
        if std::fs::read_dir(output_dir)?.next().is_some() {
            return Err(IoError::other("task output directory is not empty"));
        }
    } else {
        tokio::fs::create_dir_all(output_dir).await?;
    }

    let spine_dir = bundle_dir.join("spine");
    let records = read_spine_records(&spine_dir).await?;
    if records.is_empty() {
        return Err(IoError::other("trace spine is empty"));
    }
    let thread_id = records[0].thread_id;

    let (start_seq, end_seq) = resolve_segment_range(&records, &selector)?;
    let segment_records: Vec<&TraceSpineLine> = records
        .iter()
        .filter(|line| line.seq >= start_seq && line.seq <= end_seq)
        .collect();

    let (instruction, turn_ids) = extract_instruction(&segment_records)?;
    let (git_info, git_diff_ref) = extract_artifacts(&records);
    let baseline_diff = read_baseline_diff(bundle_dir, git_diff_ref.as_ref()).await?;
    let baseline_has_diff = baseline_diff
        .as_ref()
        .is_some_and(|diff| !diff.trim().is_empty());
    let verifier = select_verifier(
        &segment_records,
        bundle_dir,
        git_diff_ref.as_ref(),
        baseline_diff.as_deref(),
    )
    .await?;

    let instruction_path = output_dir.join("instruction.md");
    tokio::fs::write(&instruction_path, instruction).await?;

    let env_dir = output_dir.join("environment");
    tokio::fs::create_dir_all(&env_dir).await?;
    let env_patch = if baseline_has_diff {
        if let Some(diff) = git_diff_ref.as_ref() {
        let diff_path = bundle_dir.join(diff.artifact_path.as_str());
        if diff_path.exists() {
            let env_patch_path = env_dir.join("git_diff.patch");
            tokio::fs::copy(&diff_path, &env_patch_path).await?;
            Some(env_patch_path)
        } else {
            None
        }
        } else {
            None
        }
    } else {
        None
    };
    let dockerfile = render_dockerfile(&git_info, git_diff_ref.as_ref(), env_patch.as_deref());
    tokio::fs::write(env_dir.join("Dockerfile"), dockerfile).await?;

    let tests_dir = output_dir.join("tests");
    tokio::fs::create_dir_all(&tests_dir).await?;
    if let Some(expected_diff) = verifier.expected_diff.as_ref() {
        tokio::fs::write(tests_dir.join("expected.diff"), expected_diff).await?;
    }
    let test_script_path = tests_dir.join("test.sh");
    tokio::fs::write(&test_script_path, verifier.test_script).await?;
    set_executable_if_unix(&test_script_path).await;

    let task_toml = render_task_toml(
        thread_id,
        &turn_ids,
        start_seq,
        end_seq,
        &git_info,
        git_diff_ref.as_ref(),
        verifier.verifier_type,
        &metadata,
    )?;
    tokio::fs::write(output_dir.join("task.toml"), task_toml).await?;

    Ok(DistillSummary {
        thread_id,
        start_seq,
        end_seq,
        task_dir: output_dir.to_path_buf(),
    })
}

#[derive(Debug, Clone, Copy)]
enum VerifierType {
    ExecCommand,
    Diff,
    Missing,
}

struct VerifierPlan {
    verifier_type: VerifierType,
    test_script: String,
    expected_diff: Option<String>,
}

struct GitArtifactInfo {
    repo_url: Option<String>,
    commit_hash: Option<String>,
}

struct GitDiffArtifact {
    base_sha: Option<String>,
    artifact_path: String,
}

fn trace_spine_dir_from_home(codex_home: &Path, thread_id: ThreadId) -> PathBuf {
    codex_home.join(TRACE_SPINE_SUBDIR).join(thread_id.to_string())
}

fn copy_dir_recursive<'a>(
    source: &'a Path,
    dest: &'a Path,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send + 'a>> {
    Box::pin(async move {
        tokio::fs::create_dir_all(dest).await?;
        let mut dir = tokio::fs::read_dir(source).await?;
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            let file_name = entry.file_name();
            let dest_path = dest.join(file_name);
            if path.is_dir() {
                copy_dir_recursive(&path, &dest_path).await?;
            } else {
                tokio::fs::copy(&path, &dest_path).await?;
            }
        }
        Ok(())
    })
}

async fn read_spine_records(spine_dir: &Path) -> std::io::Result<Vec<TraceSpineLine>> {
    let mut segments = list_segment_files(spine_dir)?;
    segments.sort_by_key(|(index, _)| *index);
    let mut records = Vec::new();
    for (_index, path) in segments {
        let text = tokio::fs::read_to_string(&path).await?;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let record: TraceSpineLine = serde_json::from_str(line)
                .map_err(|e| IoError::other(format!("failed to parse spine line: {e}")))?;
            records.push(record);
        }
    }
    Ok(records)
}

fn resolve_segment_range(
    records: &[TraceSpineLine],
    selector: &SegmentSelector,
) -> std::io::Result<(u64, u64)> {
    if let (Some(start_seq), Some(end_seq)) = (selector.start_seq, selector.end_seq) {
        return Ok((start_seq, end_seq));
    }
    let Some(start_turn_id) = selector.start_turn_id.as_ref() else {
        return Err(IoError::other("start_turn_id or start_seq is required"));
    };
    let Some(end_turn_id) = selector.end_turn_id.as_ref() else {
        return Err(IoError::other("end_turn_id or end_seq is required"));
    };
    let start_seq = records
        .iter()
        .find(|line| record_turn_id(&line.item).is_some_and(|id| id == start_turn_id))
        .map(|line| line.seq)
        .ok_or_else(|| IoError::other("start_turn_id not found in trace spine"))?;
    let end_seq = records
        .iter()
        .rev()
        .find(|line| record_turn_id(&line.item).is_some_and(|id| id == end_turn_id))
        .map(|line| line.seq)
        .ok_or_else(|| IoError::other("end_turn_id not found in trace spine"))?;
    Ok((start_seq, end_seq))
}

fn record_turn_id(item: &TraceSpineItem) -> Option<&str> {
    match item {
        TraceSpineItem::Submission(sub) => Some(sub.id.as_str()),
        TraceSpineItem::Event(ev) => Some(ev.id.as_str()),
        TraceSpineItem::TurnContext(ctx) => Some(ctx.turn_id.as_str()),
        TraceSpineItem::CompactionBoundary(boundary) => Some(boundary.turn_id.as_str()),
        _ => None,
    }
}

fn extract_instruction(
    records: &[&TraceSpineLine],
) -> std::io::Result<(String, Vec<String>)> {
    for line in records {
        if let TraceSpineItem::Submission(sub) = &line.item {
            match &sub.op {
                codex_protocol::protocol::Op::UserTurn { items, .. } => {
                    let text = user_inputs_to_text(items);
                    return Ok((text, vec![sub.id.clone()]));
                }
                codex_protocol::protocol::Op::UserInput { items, .. } => {
                    let text = user_inputs_to_text(items);
                    return Ok((text, vec![sub.id.clone()]));
                }
                _ => {}
            }
        }
    }
    Err(IoError::other("no user input found in segment"))
}

fn user_inputs_to_text(items: &[codex_protocol::user_input::UserInput]) -> String {
    let mut lines = Vec::new();
    for item in items {
        match item {
            codex_protocol::user_input::UserInput::Text { text, .. } => {
                if !text.is_empty() {
                    lines.push(text.clone());
                }
            }
            codex_protocol::user_input::UserInput::Image { image_url } => {
                lines.push(format!("[image: {image_url}]"));
            }
            codex_protocol::user_input::UserInput::LocalImage { path } => {
                lines.push(format!("[image: {}]", path.display()));
            }
            codex_protocol::user_input::UserInput::Skill { name, path } => {
                lines.push(format!("[skill: {name} ({})]", path.display()));
            }
            codex_protocol::user_input::UserInput::Mention { name, path } => {
                lines.push(format!("[mention: {name} ({path})]"));
            }
            // Handle any future variants gracefully
            _ => {}
        }
    }
    lines.join("\n")
}

fn extract_artifacts(records: &[TraceSpineLine]) -> (GitArtifactInfo, Option<GitDiffArtifact>) {
    let mut git_info = GitArtifactInfo {
        repo_url: None,
        commit_hash: None,
    };
    let mut git_diff: Option<GitDiffArtifact> = None;
    for line in records {
        if let TraceSpineItem::ArtifactRef(artifact) = &line.item {
            match artifact {
                TraceArtifactRef::GitInfo { git, .. } => {
                    if let Some(git) = git {
                        if git_info.repo_url.is_none() {
                            git_info.repo_url = git.repository_url.clone();
                        }
                        if git_info.commit_hash.is_none() {
                            git_info.commit_hash = git.commit_hash.clone();
                        }
                    }
                }
                TraceArtifactRef::GitDiff {
                    base_sha,
                    artifact_path,
                    ..
                } => {
                    git_diff = Some(GitDiffArtifact {
                        base_sha: base_sha.clone(),
                        artifact_path: artifact_path.clone(),
                    });
                }
                _ => {}
            }
        }
    }
    (git_info, git_diff)
}

async fn read_baseline_diff(
    bundle_dir: &Path,
    git_diff: Option<&GitDiffArtifact>,
) -> std::io::Result<Option<String>> {
    let Some(git_diff) = git_diff else {
        return Ok(None);
    };
    let diff_path = bundle_dir.join(git_diff.artifact_path.as_str());
    if !diff_path.exists() {
        return Ok(None);
    }
    let diff = tokio::fs::read_to_string(diff_path).await?;
    Ok(Some(diff))
}

async fn select_verifier(
    records: &[&TraceSpineLine],
    bundle_dir: &Path,
    git_diff: Option<&GitDiffArtifact>,
    baseline_diff: Option<&str>,
) -> std::io::Result<VerifierPlan> {
    for line in records.iter().rev() {
        if let TraceSpineItem::Event(ev) = &line.item {
            if let codex_protocol::protocol::EventMsg::ExecCommandEnd(end) = &ev.msg {
                if end.exit_code == 0 {
                    let command = shell_join(&end.command);
                    let script = render_exec_verifier(&command);
                    return Ok(VerifierPlan {
                        verifier_type: VerifierType::ExecCommand,
                        test_script: script,
                        expected_diff: None,
                    });
                }
            }
        }
    }

    let baseline_has_diff = baseline_diff.is_some_and(|diff| !diff.trim().is_empty());
    if baseline_has_diff {
        return Ok(VerifierPlan {
            verifier_type: VerifierType::Missing,
            test_script: render_missing_verifier(),
            expected_diff: None,
        });
    }

    for line in records.iter().rev() {
        if let TraceSpineItem::Event(ev) = &line.item {
            if let codex_protocol::protocol::EventMsg::TurnDiff(diff) = &ev.msg {
                let script = render_diff_verifier();
                return Ok(VerifierPlan {
                    verifier_type: VerifierType::Diff,
                    test_script: script,
                    expected_diff: Some(diff.unified_diff.clone()),
                });
            }
        }
    }

    if let Some(git_diff) = git_diff {
        let diff_path = bundle_dir.join(git_diff.artifact_path.as_str());
        if diff_path.exists() {
            let diff = tokio::fs::read_to_string(diff_path).await?;
            if !diff.trim().is_empty() {
                let script = render_diff_verifier();
                return Ok(VerifierPlan {
                    verifier_type: VerifierType::Diff,
                    test_script: script,
                    expected_diff: Some(diff),
                });
            }
        }
    }

    Ok(VerifierPlan {
        verifier_type: VerifierType::Missing,
        test_script: render_missing_verifier(),
        expected_diff: None,
    })
}

fn render_dockerfile(
    git_info: &GitArtifactInfo,
    git_diff: Option<&GitDiffArtifact>,
    env_patch: Option<&Path>,
) -> String {
    let mut lines = Vec::new();
    lines.push("FROM ubuntu:22.04".to_string());
    lines.push(
        "RUN apt-get update && apt-get install -y git ca-certificates && rm -rf /var/lib/apt/lists/*"
            .to_string(),
    );
    lines.push("WORKDIR /workspace".to_string());
    if let Some(repo_url) = git_info.repo_url.as_ref() {
        let quoted = shlex::try_quote(repo_url).map(|s| s.into_owned()).unwrap_or_else(|_| repo_url.clone());
        lines.push(format!("RUN git clone {quoted} repo"));
        lines.push("WORKDIR /workspace/repo".to_string());
        if let Some(base_sha) = git_diff.and_then(|diff| diff.base_sha.clone()) {
            let quoted = shlex::try_quote(&base_sha).map(|s| s.into_owned()).unwrap_or(base_sha);
            lines.push(format!("RUN git checkout {quoted}"));
        } else if let Some(commit) = git_info.commit_hash.as_ref() {
            let quoted = shlex::try_quote(commit).map(|s| s.into_owned()).unwrap_or_else(|_| commit.clone());
            lines.push(format!("RUN git checkout {quoted}"));
        }
        if env_patch.is_some() {
            lines.push("COPY git_diff.patch /tmp/git_diff.patch".to_string());
            lines.push("RUN git apply /tmp/git_diff.patch".to_string());
        }
    } else {
        lines.push("RUN mkdir -p /workspace/repo".to_string());
        lines.push("WORKDIR /workspace/repo".to_string());
    }
    lines.push(String::new());
    lines.join("\n")
}

fn render_exec_verifier(command: &str) -> String {
    format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo
{command}
printf "1" > /logs/verifier/reward.txt
"#
    )
}

fn render_diff_verifier() -> String {
    r#"#!/usr/bin/env bash
set -euo pipefail
cd /workspace/repo
git diff --no-color > /tmp/actual.diff
diff -u "tests/expected.diff" /tmp/actual.diff
printf "1" > /logs/verifier/reward.txt
"#
    .to_string()
}

fn render_missing_verifier() -> String {
    r#"#!/usr/bin/env bash
set -euo pipefail
echo "No verifier could be inferred for this segment." >&2
exit 1
"#
    .to_string()
}

fn render_task_toml(
    thread_id: ThreadId,
    turn_ids: &[String],
    start_seq: u64,
    end_seq: u64,
    git_info: &GitArtifactInfo,
    git_diff: Option<&GitDiffArtifact>,
    verifier_type: VerifierType,
    distill_meta: &DistillMetadata,
) -> std::io::Result<String> {
    let mut metadata = toml::value::Table::new();
    metadata.insert(
        "source_thread_id".to_string(),
        toml::Value::String(thread_id.to_string()),
    );
    metadata.insert(
        "source_turn_ids".to_string(),
        toml::Value::Array(
            turn_ids
                .iter()
                .map(|id| toml::Value::String(id.clone()))
                .collect(),
        ),
    );
    metadata.insert(
        "seq_range".to_string(),
        toml::Value::String(format!("{start_seq}-{end_seq}")),
    );
    if let Some(repo_url) = git_info.repo_url.as_ref() {
        metadata.insert(
            "source_repo_url".to_string(),
            toml::Value::String(repo_url.clone()),
        );
    }
    if let Some(commit_hash) = git_info.commit_hash.as_ref() {
        metadata.insert(
            "source_commit_hash".to_string(),
            toml::Value::String(commit_hash.clone()),
        );
    }
    if let Some(diff) = git_diff {
        metadata.insert(
            "source_git_diff_path".to_string(),
            toml::Value::String(diff.artifact_path.clone()),
        );
        if let Some(base_sha) = diff.base_sha.as_ref() {
            metadata.insert(
                "source_git_diff_base_sha".to_string(),
                toml::Value::String(base_sha.clone()),
            );
        }
    }
    let verifier_label = match verifier_type {
        VerifierType::ExecCommand => "exec_command",
        VerifierType::Diff => "diff",
        VerifierType::Missing => "missing",
    };
    metadata.insert(
        "verifier_type".to_string(),
        toml::Value::String(verifier_label.to_string()),
    );

    // Add codex-loop integration metadata
    if let Some(signal_type) = distill_meta.signal_type.as_ref() {
        metadata.insert(
            "signal_type".to_string(),
            toml::Value::String(signal_type.clone()),
        );
    }
    if let Some(cluster_id) = distill_meta.cluster_id.as_ref() {
        metadata.insert(
            "cluster_id".to_string(),
            toml::Value::String(cluster_id.clone()),
        );
    }
    if let Some(signal_category) = distill_meta.signal_category.as_ref() {
        metadata.insert(
            "signal_category".to_string(),
            toml::Value::String(signal_category.clone()),
        );
    }

    let mut root = toml::value::Table::new();
    root.insert("version".to_string(), toml::Value::String("1.0".to_string()));
    root.insert("metadata".to_string(), toml::Value::Table(metadata));

    toml::to_string(&root).map_err(|e| IoError::other(format!("failed to build task.toml: {e}")))
}

fn shell_join(args: &[String]) -> String {
    args.iter()
        .map(|arg| shlex::try_quote(arg).map(|s| s.to_string()).unwrap_or_else(|_| arg.clone()))
        .collect::<Vec<_>>()
        .join(" ")
}

async fn set_executable_if_unix(path: &Path) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(mut perms) = tokio::fs::metadata(path).await.map(|meta| meta.permissions()) {
            perms.set_mode(0o755);
            let _ = tokio::fs::set_permissions(path, perms).await;
        }
    }
}
