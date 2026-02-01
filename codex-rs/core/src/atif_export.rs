//! ATIF v1.4 trajectory export for Harbor compatibility.
//!
//! Converts trace spine records into ATIF (Agent Trajectory Interchange Format)
//! for use with Harbor evaluations and other trajectory analysis tools.

use std::path::Path;

use codex_protocol::protocol::{Event, EventMsg, Op};
use codex_protocol::user_input::UserInput;
use serde::{Deserialize, Serialize};

use crate::trace_spine::{TraceSpineItem, TraceSpineLine};

/// ATIF schema version
pub const ATIF_SCHEMA_VERSION: &str = "ATIF-v1.4";

/// An ATIF trajectory document
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtifTrajectory {
    pub schema_version: String,
    pub messages: Vec<AtifMessage>,
    pub metadata: AtifMetadata,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metrics: Option<AtifMetrics>,
}

/// A message in the ATIF trajectory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtifMessage {
    pub role: String,
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_calls: Option<Vec<AtifToolCall>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
}

/// A tool call in ATIF format
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtifToolCall {
    pub id: String,
    pub name: String,
    pub arguments: String,
}

/// Metadata for the ATIF trajectory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtifMetadata {
    pub task_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
}

/// Metrics for the ATIF trajectory
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AtifMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens_input: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tokens_output: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub turn_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_call_count: Option<u32>,
}

/// Export spine records to ATIF v1.4 trajectory format.
///
/// If `split_compaction` is true, returns multiple trajectories split at
/// compaction boundaries (per ATIF v1.4 spec: don't pretend post-compaction
/// is continuation of pre-compaction).
pub fn export_atif_trajectory(
    spine_records: &[TraceSpineLine],
    split_compaction: bool,
) -> Vec<AtifTrajectory> {
    if spine_records.is_empty() {
        return vec![];
    }

    // Extract metadata from session_meta
    let (task_id, model, source, start_time) = extract_metadata(spine_records);

    if split_compaction {
        // Split at compaction boundaries
        let segments = split_at_compaction(spine_records);
        segments
            .into_iter()
            .enumerate()
            .map(|(idx, segment)| {
                let suffix = if idx == 0 {
                    String::new()
                } else {
                    format!(".cont-{}", idx)
                };
                build_trajectory(
                    segment,
                    format!("{}{}", task_id, suffix),
                    model.clone(),
                    source.clone(),
                    start_time.clone(),
                )
            })
            .collect()
    } else {
        vec![build_trajectory(
            spine_records,
            task_id,
            model,
            source,
            start_time,
        )]
    }
}

fn extract_metadata(records: &[TraceSpineLine]) -> (String, Option<String>, Option<String>, Option<String>) {
    let mut task_id = String::new();
    let mut model = None;
    let mut source = None;
    let mut start_time = None;

    for record in records {
        if let TraceSpineItem::SessionMeta(meta) = &record.item {
            task_id = meta.meta.id.to_string();
            model = meta.meta.model_provider.clone();
            source = Some(format!("{:?}", meta.meta.source));
            start_time = Some(meta.meta.timestamp.clone());
            break;
        }
    }

    if task_id.is_empty() && !records.is_empty() {
        task_id = records[0].thread_id.to_string();
    }

    (task_id, model, source, start_time)
}

fn split_at_compaction(records: &[TraceSpineLine]) -> Vec<&[TraceSpineLine]> {
    let mut segments = vec![];
    let mut start = 0;

    for (i, record) in records.iter().enumerate() {
        if matches!(record.item, TraceSpineItem::CompactionBoundary(_)) {
            if i > start {
                segments.push(&records[start..i]);
            }
            start = i + 1;
        }
    }

    // Add final segment
    if start < records.len() {
        segments.push(&records[start..]);
    }

    // If no segments (no compaction), return all
    if segments.is_empty() {
        segments.push(records);
    }

    segments
}

fn build_trajectory(
    records: &[TraceSpineLine],
    task_id: String,
    model: Option<String>,
    source: Option<String>,
    start_time: Option<String>,
) -> AtifTrajectory {
    let mut messages = vec![];
    let mut turn_count = 0u32;
    let mut tool_call_count = 0u32;
    let mut end_time = None;

    for record in records {
        // Update end_time
        if !record.timestamp.is_empty() {
            end_time = Some(record.timestamp.clone());
        }

        match &record.item {
            TraceSpineItem::Submission(submission) => {
                match &submission.op {
                    Op::UserTurn { items, .. } => {
                        turn_count += 1;
                        let content = user_inputs_to_text(items);
                        if !content.is_empty() {
                            messages.push(AtifMessage {
                                role: "user".to_string(),
                                content,
                                tool_calls: None,
                                tool_call_id: None,
                            });
                        }
                    }
                    Op::UserInput { items, .. } => {
                        turn_count += 1;
                        let content = user_inputs_to_text(items);
                        if !content.is_empty() {
                            messages.push(AtifMessage {
                                role: "user".to_string(),
                                content,
                                tool_calls: None,
                                tool_call_id: None,
                            });
                        }
                    }
                    Op::ExecApproval { id, decision } => {
                        let decision_str = format!("{:?}", decision);
                        messages.push(AtifMessage {
                            role: "tool".to_string(),
                            content: format!("Approval decision: {}", decision_str),
                            tool_calls: None,
                            tool_call_id: Some(id.clone()),
                        });
                    }
                    Op::PatchApproval { id, decision } => {
                        let decision_str = format!("{:?}", decision);
                        messages.push(AtifMessage {
                            role: "tool".to_string(),
                            content: format!("Patch approval: {}", decision_str),
                            tool_calls: None,
                            tool_call_id: Some(id.clone()),
                        });
                    }
                    _ => {}
                }
            }
            TraceSpineItem::Event(event) => {
                process_event(event, &mut messages, &mut tool_call_count);
            }
            _ => {}
        }
    }

    AtifTrajectory {
        schema_version: ATIF_SCHEMA_VERSION.to_string(),
        messages,
        metadata: AtifMetadata {
            task_id,
            model,
            start_time,
            end_time,
            source,
        },
        metrics: Some(AtifMetrics {
            tokens_input: None,
            tokens_output: None,
            duration_ms: None,
            turn_count: Some(turn_count),
            tool_call_count: Some(tool_call_count),
        }),
    }
}

fn process_event(event: &Event, messages: &mut Vec<AtifMessage>, tool_call_count: &mut u32) {
    match &event.msg {
        EventMsg::AgentMessage(msg) => {
            // Agent message contains the text response
            if !msg.message.is_empty() {
                messages.push(AtifMessage {
                    role: "assistant".to_string(),
                    content: msg.message.clone(),
                    tool_calls: None,
                    tool_call_id: None,
                });
            }
        }
        EventMsg::ExecCommandBegin(exec_begin) => {
            // Track tool call
            *tool_call_count += 1;
            messages.push(AtifMessage {
                role: "assistant".to_string(),
                content: String::new(),
                tool_calls: Some(vec![AtifToolCall {
                    id: exec_begin.call_id.clone(),
                    name: "exec_command".to_string(),
                    arguments: serde_json::json!({
                        "command": exec_begin.command,
                        "cwd": exec_begin.cwd,
                    }).to_string(),
                }]),
                tool_call_id: None,
            });
        }
        EventMsg::ExecCommandEnd(exec_end) => {
            let mut content = format!("Command completed with exit code: {}", exec_end.exit_code);
            if !exec_end.stdout.is_empty() {
                content.push_str(&format!("\nstdout: {}", exec_end.stdout));
            }
            if !exec_end.stderr.is_empty() {
                content.push_str(&format!("\nstderr: {}", exec_end.stderr));
            }
            messages.push(AtifMessage {
                role: "tool".to_string(),
                content,
                tool_calls: None,
                tool_call_id: Some(exec_end.call_id.clone()),
            });
        }
        EventMsg::PatchApplyBegin(patch_begin) => {
            *tool_call_count += 1;
            messages.push(AtifMessage {
                role: "assistant".to_string(),
                content: String::new(),
                tool_calls: Some(vec![AtifToolCall {
                    id: patch_begin.call_id.clone(),
                    name: "apply_patch".to_string(),
                    arguments: serde_json::json!({
                        "changes": format!("{} file(s)", patch_begin.changes.len()),
                    }).to_string(),
                }]),
                tool_call_id: None,
            });
        }
        EventMsg::PatchApplyEnd(patch_end) => {
            let content = format!(
                "Patch applied: success={}",
                patch_end.success
            );
            messages.push(AtifMessage {
                role: "tool".to_string(),
                content,
                tool_calls: None,
                tool_call_id: Some(patch_end.call_id.clone()),
            });
        }
        _ => {}
    }
}

fn user_inputs_to_text(items: &[UserInput]) -> String {
    items
        .iter()
        .filter_map(|item| {
            match item {
                UserInput::Text { text, .. } => Some(text.clone()),
                _ => None,
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Write ATIF trajectories to files
pub async fn write_atif_trajectories(
    trajectories: &[AtifTrajectory],
    output_dir: &Path,
) -> std::io::Result<Vec<std::path::PathBuf>> {
    tokio::fs::create_dir_all(output_dir).await?;
    
    let mut paths = vec![];
    
    for (i, trajectory) in trajectories.iter().enumerate() {
        let filename = if i == 0 {
            "trajectory.json".to_string()
        } else {
            format!("trajectory.cont-{}.json", i)
        };
        
        let path = output_dir.join(&filename);
        let json = serde_json::to_string_pretty(trajectory)?;
        tokio::fs::write(&path, json).await?;
        paths.push(path);
    }
    
    Ok(paths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atif_schema_version() {
        assert_eq!(ATIF_SCHEMA_VERSION, "ATIF-v1.4");
    }

    #[test]
    fn test_empty_records() {
        let records: Vec<TraceSpineLine> = vec![];
        let trajectories = export_atif_trajectory(&records, false);
        assert!(trajectories.is_empty());
    }
}
