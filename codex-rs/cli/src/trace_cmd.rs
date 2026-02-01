use std::path::PathBuf;

use clap::Args;
use clap::Parser;
use clap::Subcommand;
use codex_core::config::find_codex_home;
use codex_core::atif_export::{export_atif_trajectory, write_atif_trajectories};
use codex_core::trace_spine::DistillSummary;
use codex_core::trace_spine::SegmentSelector;
use codex_core::trace_spine::export_rollout_plus_bundle;
use codex_core::trace_spine::TraceSpineLine;
use codex_protocol::ThreadId;
use tokio::process::Command;

#[derive(Debug, Parser)]
pub struct TraceCli {
    #[command(subcommand)]
    pub subcommand: TraceSubcommand,
}

#[derive(Debug, Subcommand)]
pub enum TraceSubcommand {
    /// Export a rollout+ bundle (spine/rollout/artifacts).
    Bundle(BundleArgs),
    /// Distill a bundle segment into a Harbor task.
    Distill(DistillArgs),
    /// Package tasks into a dataset and optionally run Harbor.
    Dataset(DatasetArgs),
    /// Export trace spine to ATIF v1.4 trajectory format.
    Atif(AtifArgs),
}

#[derive(Debug, Args)]
pub struct BundleArgs {
    /// Thread/session ID (UUID)
    #[arg(long)]
    pub session_id: String,
    /// Output directory for the bundle
    #[arg(long)]
    pub output: PathBuf,
    /// Override CODEX_HOME (defaults to ~/.codex)
    #[arg(long)]
    pub codex_home: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub struct DistillArgs {
    /// Bundle directory containing spine/rollout/artifacts
    #[arg(long)]
    pub bundle: PathBuf,
    /// Output directory for the Harbor task
    #[arg(long)]
    pub output: PathBuf,
    /// Segment start turn id
    #[arg(long)]
    pub start_turn_id: Option<String>,
    /// Segment end turn id
    #[arg(long)]
    pub end_turn_id: Option<String>,
    /// Segment start seq
    #[arg(long)]
    pub start_seq: Option<u64>,
    /// Segment end seq
    #[arg(long)]
    pub end_seq: Option<u64>,
    /// Signal type (friction or delight) for codex-loop integration
    #[arg(long)]
    pub signal_type: Option<String>,
    /// Cluster ID for codex-loop integration
    #[arg(long)]
    pub cluster_id: Option<String>,
    /// Signal category (e.g., repeated_denial, exec_failure)
    #[arg(long)]
    pub signal_category: Option<String>,
}

#[derive(Debug, Args)]
pub struct DatasetArgs {
    /// Output dataset directory
    #[arg(long)]
    pub output: PathBuf,
    /// One or more Harbor task directories
    #[arg(long = "task")]
    pub tasks: Vec<PathBuf>,
    /// Optional agent name for harbor run
    #[arg(long)]
    pub agent: Option<String>,
    /// Optional model name for harbor run
    #[arg(long)]
    pub model: Option<String>,
    /// Run harbor after packaging
    #[arg(long, default_value_t = false)]
    pub run: bool,
}

#[derive(Debug, Args)]
pub struct AtifArgs {
    /// Bundle directory or spine directory
    #[arg(long)]
    pub bundle: PathBuf,
    /// Output directory for ATIF trajectory files
    #[arg(long)]
    pub output: PathBuf,
    /// Split trajectory at compaction boundaries
    #[arg(long, default_value_t = false)]
    pub split_compaction: bool,
}

impl TraceCli {
    pub async fn run(self) -> anyhow::Result<()> {
        match self.subcommand {
            TraceSubcommand::Bundle(args) => run_bundle(args).await?,
            TraceSubcommand::Distill(args) => {
                let summary = run_distill(args).await?;
                println!("{}", serde_json::to_string_pretty(&summary)?);
            }
            TraceSubcommand::Dataset(args) => run_dataset(args).await?,
            TraceSubcommand::Atif(args) => run_atif(args).await?,
        }
        Ok(())
    }
}

async fn run_bundle(args: BundleArgs) -> anyhow::Result<()> {
    let codex_home = args.codex_home.unwrap_or(find_codex_home()?);
    let thread_id = ThreadId::from_string(args.session_id.as_str())
        .map_err(|e| anyhow::anyhow!("invalid session id: {e}"))?;
    export_rollout_plus_bundle(codex_home.as_path(), thread_id, args.output.as_path()).await?;
    Ok(())
}

async fn run_distill(args: DistillArgs) -> anyhow::Result<DistillSummary> {
    use codex_core::trace_spine::{DistillMetadata, distill_bundle_to_task_with_metadata};
    
    let selector = SegmentSelector {
        start_turn_id: args.start_turn_id,
        end_turn_id: args.end_turn_id,
        start_seq: args.start_seq,
        end_seq: args.end_seq,
    };
    let metadata = DistillMetadata {
        signal_type: args.signal_type,
        cluster_id: args.cluster_id,
        signal_category: args.signal_category,
    };
    let summary = distill_bundle_to_task_with_metadata(
        args.bundle.as_path(),
        args.output.as_path(),
        selector,
        metadata,
    )
    .await?;
    Ok(summary)
}

async fn run_atif(args: AtifArgs) -> anyhow::Result<()> {
    // Find spine directory
    let spine_dir = if args.bundle.join("spine").exists() {
        args.bundle.join("spine")
    } else {
        args.bundle.clone()
    };

    // Read spine records
    let mut records: Vec<TraceSpineLine> = Vec::new();
    let mut segment_files: Vec<_> = std::fs::read_dir(&spine_dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map_or(false, |n| n.starts_with("segment-") && n.ends_with(".jsonl"))
        })
        .collect();
    segment_files.sort();

    for segment_file in segment_files {
        let content = tokio::fs::read_to_string(&segment_file).await?;
        for line in content.lines() {
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<TraceSpineLine>(line) {
                Ok(record) => records.push(record),
                Err(e) => {
                    eprintln!("Warning: Failed to parse line in {:?}: {}", segment_file, e);
                }
            }
        }
    }

    if records.is_empty() {
        return Err(anyhow::anyhow!("No spine records found in {:?}", spine_dir));
    }

    // Sort by seq
    records.sort_by_key(|r| r.seq);

    // Export to ATIF
    let trajectories = export_atif_trajectory(&records, args.split_compaction);

    // Write output
    let paths = write_atif_trajectories(&trajectories, &args.output).await?;

    println!("Exported {} ATIF trajectory file(s):", paths.len());
    for path in paths {
        println!("  {}", path.display());
    }

    Ok(())
}

async fn run_dataset(args: DatasetArgs) -> anyhow::Result<()> {
    if args.tasks.is_empty() {
        return Err(anyhow::anyhow!("at least one --task is required"));
    }
    if args.output.exists() {
        if std::fs::read_dir(&args.output)?.next().is_some() {
            return Err(anyhow::anyhow!("output directory is not empty"));
        }
    } else {
        tokio::fs::create_dir_all(&args.output).await?;
    }

    let mut evidence = Vec::new();
    for task_dir in &args.tasks {
        let task_name = task_dir
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow::anyhow!("invalid task directory name"))?;
        let dest_dir = args.output.join(task_name);
        copy_dir_recursive(task_dir, &dest_dir).await?;
        let task_toml_path = dest_dir.join("task.toml");
        if task_toml_path.exists() {
            let text = tokio::fs::read_to_string(&task_toml_path).await?;
            let parsed: toml::Value = toml::from_str(&text)?;
            evidence.push(serde_json::json!({
                "task": task_name,
                "metadata": parsed.get("metadata").cloned().unwrap_or(toml::Value::Table(Default::default())),
            }));
        }
    }

    let evidence_path = args.output.join("trace_evidence.json");
    tokio::fs::write(evidence_path, serde_json::to_string_pretty(&evidence)?).await?;

    let run_script_path = args.output.join("harbor_run.sh");
    let agent = args.agent.clone().unwrap_or_else(|| "<agent>".to_string());
    let model = args.model.clone().unwrap_or_else(|| "<model>".to_string());
    let run_script = format!(
        r#"#!/usr/bin/env bash
set -euo pipefail
harbor run -p "{dataset}" -a "{agent}" -m "{model}"
"#,
        dataset = args.output.display(),
        agent = agent,
        model = model
    );
    tokio::fs::write(&run_script_path, run_script).await?;

    set_executable_if_unix(&run_script_path).await;

    if args.run {
        let status = Command::new("harbor")
            .arg("run")
            .arg("-p")
            .arg(args.output.as_os_str())
            .arg("-a")
            .arg(agent.as_str())
            .arg("-m")
            .arg(model.as_str())
            .status()
            .await?;
        let result = serde_json::json!({
            "exit_code": status.code(),
            "success": status.success(),
        });
        let results_dir = args.output.join("results");
        tokio::fs::create_dir_all(&results_dir).await?;
        tokio::fs::write(
            results_dir.join("harbor_run.json"),
            serde_json::to_string_pretty(&result)?,
        )
        .await?;
    }

    Ok(())
}

fn copy_dir_recursive<'a>(
    source: &'a PathBuf,
    dest: &'a PathBuf,
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

async fn set_executable_if_unix(path: &PathBuf) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if let Ok(mut perms) = tokio::fs::metadata(path).await.map(|meta| meta.permissions()) {
            perms.set_mode(0o755);
            let _ = tokio::fs::set_permissions(path, perms).await;
        }
    }
}
