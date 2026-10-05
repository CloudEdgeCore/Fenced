#![forbid(unsafe_code)]

use fenced_runtime_wasm::sandbox::{CHECKPOINT_SCHEMA, PROVIDER_NAME, RUNTIME_ABI, Sandbox};
use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;
use tonic::transport::Channel;
use tonic::{Code, Request};
use uuid::Uuid;

pub mod protocol {
    tonic::include_proto!("fenced.runtime.v1");
}

use protocol::runtime_control_service_client::RuntimeControlServiceClient;
use protocol::{
    AcknowledgeCancellationRequest, ArtifactReference, AttemptIdentity, AttemptPhase,
    CommitCheckpointRequest, CompleteAttemptRequest, HeartbeatRequest, PollAssignmentRequest,
    TransitionAttemptRequest,
};

#[derive(Clone)]
struct Settings {
    control_endpoint: String,
    tenant_id: String,
    runtime_instance_id: String,
    package_root: PathBuf,
    artifact_root: PathBuf,
    poll_interval: Duration,
    heartbeat_ttl: Duration,
    max_artifact_bytes: u64,
}

#[derive(Deserialize)]
struct WorkloadSpec {
    runtime: RuntimeSpec,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeSpec {
    component_path: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct LogicalCheckpoint {
    goal_sha256: String,
    step: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let settings = parse_settings(std::env::args().skip(1).collect())?;
    let sandbox = Arc::new(Sandbox::new(
        &settings.package_root,
        10_000_000,
        Duration::from_secs(30),
        256 << 20,
    )?);
    let mut client = RuntimeControlServiceClient::connect(settings.control_endpoint.clone())
        .await
        .context("connect to Runtime Protocol")?;
    loop {
        match run_once(&mut client, &settings, &sandbox).await {
            Ok(true) => {}
            Ok(false) => sleep(settings.poll_interval).await,
            Err(error) => {
                eprintln!("wasmtime runtime execution failed: {error:#}");
                sleep(settings.poll_interval).await;
            }
        }
    }
}

async fn run_once(
    client: &mut RuntimeControlServiceClient<Channel>,
    settings: &Settings,
    sandbox: &Arc<Sandbox>,
) -> Result<bool> {
    let response = client
        .poll_assignment(Request::new(PollAssignmentRequest {
            tenant_id: settings.tenant_id.clone(),
            runtime_instance_id: settings.runtime_instance_id.clone(),
        }))
        .await;
    let assignment = match response {
        Ok(response) => response
            .into_inner()
            .assignment
            .context("assignment missing")?,
        Err(status) if status.code() == Code::NotFound => return Ok(false),
        Err(status) => return Err(status.into()),
    };
    let identity = assignment
        .identity
        .clone()
        .context("attempt identity missing")?;
    let mut version = transition(
        client,
        &identity,
        assignment.attempt_version,
        AttemptPhase::Starting,
        "starting",
    )
    .await?;
    version = transition(client, &identity, version, AttemptPhase::Running, "running").await?;
    let heartbeat = client
        .heartbeat(Request::new(HeartbeatRequest {
            identity: Some(identity.clone()),
            expected_lease_version: assignment.lease_version,
            idempotency_key: operation_key(&identity, "heartbeat"),
            requested_ttl_seconds: settings.heartbeat_ttl.as_secs() as i64,
        }))
        .await
        .context("renew runtime lease")?
        .into_inner();
    if heartbeat.cancel_requested {
        client
            .acknowledge_cancellation(Request::new(AcknowledgeCancellationRequest {
                identity: Some(identity.clone()),
                expected_attempt_version: heartbeat.attempt_version,
                idempotency_key: operation_key(&identity, "cancel"),
            }))
            .await
            .context("acknowledge runtime cancellation")?;
        return Ok(true);
    }

    let workload: WorkloadSpec = serde_json::from_slice(&assignment.workload_spec_json)
        .context("decode Wasmtime workload specification")?;
    let goal_digest = hex::encode(Sha256::digest(assignment.goal.as_bytes()));
    if let Some(checkpoint) = assignment.resume_checkpoint.as_ref() {
        if checkpoint.agent_version_ref != assignment.agent_version_ref
            || checkpoint.runtime_class != assignment.runtime_class
            || checkpoint.provider != PROVIDER_NAME
            || checkpoint.runtime_abi != RUNTIME_ABI
            || checkpoint.schema_version != CHECKPOINT_SCHEMA
        {
            bail!("checkpoint is incompatible with Wasmtime assignment");
        }
        let state = read_artifact(
            settings,
            checkpoint
                .state
                .as_ref()
                .context("checkpoint state missing")?,
        )?;
        let logical: LogicalCheckpoint =
            serde_json::from_slice(&state).context("decode logical checkpoint")?;
        if logical.goal_sha256 != goal_digest || logical.step != "prepared" {
            bail!("checkpoint logical state does not match assignment");
        }
    } else {
        let logical = serde_json::to_vec(&LogicalCheckpoint {
            goal_sha256: goal_digest,
            step: "prepared".into(),
        })?;
        let state = write_artifact(
            settings,
            "application/vnd.fenced.wasm-state+json",
            &logical,
        )?;
        version = client
            .commit_checkpoint(Request::new(CommitCheckpointRequest {
                identity: Some(identity.clone()),
                expected_attempt_version: version,
                idempotency_key: operation_key(&identity, "checkpoint-prepared"),
                checkpoint_id: Uuid::now_v7().to_string(),
                agent_version_ref: assignment.agent_version_ref.clone(),
                provider: PROVIDER_NAME.into(),
                runtime_abi: RUNTIME_ABI.into(),
                schema_version: CHECKPOINT_SCHEMA.into(),
                state: Some(state),
                confirmed_receipt_ids: vec![],
            }))
            .await
            .context("commit logical checkpoint")?
            .into_inner()
            .attempt_version;
    }

    let input = serde_json::json!({
        "agentVersionRef": assignment.agent_version_ref,
        "attemptId": identity.attempt_id,
        "goal": assignment.goal,
        "workloadSpec": serde_json::from_slice::<serde_json::Value>(&assignment.workload_spec_json)?,
    });
    let component_path = workload.runtime.component_path;
    let component_input = serde_json::to_string(&input)?;
    let execution_sandbox = Arc::clone(sandbox);
    let mut execution = tokio::task::spawn_blocking(move || {
        execution_sandbox.execute(&component_path, &component_input)
    });
    let mut lease_version = heartbeat.lease_version;
    let output = loop {
        tokio::select! {
            execution_result = &mut execution => {
                break execution_result.context("Wasmtime execution task panicked")??;
            }
            () = sleep(Duration::from_secs(10)) => {
                let refreshed = client
                    .heartbeat(Request::new(HeartbeatRequest {
                        identity: Some(identity.clone()),
                        expected_lease_version: lease_version,
                        idempotency_key: format!("{}:heartbeat:{lease_version}", identity.attempt_id),
                        requested_ttl_seconds: settings.heartbeat_ttl.as_secs() as i64,
                    }))
                    .await
                    .context("renew runtime lease during execution")?
                    .into_inner();
                lease_version = refreshed.lease_version;
                if refreshed.cancel_requested {
                    sandbox.interrupt();
                    let _ = execution.await;
                    client
                        .acknowledge_cancellation(Request::new(AcknowledgeCancellationRequest {
                            identity: Some(identity.clone()),
                            expected_attempt_version: refreshed.attempt_version,
                            idempotency_key: operation_key(&identity, "cancel"),
                        }))
                        .await
                        .context("acknowledge runtime cancellation")?;
                    return Ok(true);
                }
            }
        }
    };
    let result = write_artifact(
        settings,
        "application/vnd.fenced.wasm-result+json",
        output.as_bytes(),
    )?;
    client
        .complete_attempt(Request::new(CompleteAttemptRequest {
            identity: Some(identity.clone()),
            expected_attempt_version: version,
            idempotency_key: operation_key(&identity, "complete"),
            result: Some(result),
        }))
        .await
        .context("complete Wasmtime attempt")?;
    Ok(true)
}

async fn transition(
    client: &mut RuntimeControlServiceClient<Channel>,
    identity: &AttemptIdentity,
    expected_version: i64,
    phase: AttemptPhase,
    operation: &str,
) -> Result<i64> {
    Ok(client
        .transition_attempt(Request::new(TransitionAttemptRequest {
            identity: Some(identity.clone()),
            expected_attempt_version: expected_version,
            idempotency_key: operation_key(identity, operation),
            target_phase: phase as i32,
            failure_code: String::new(),
            failure_message: String::new(),
        }))
        .await?
        .into_inner()
        .attempt_version)
}

fn operation_key(identity: &AttemptIdentity, operation: &str) -> String {
    format!("{}:{operation}", identity.attempt_id)
}

fn write_artifact(
    settings: &Settings,
    media_type: &str,
    content: &[u8],
) -> Result<ArtifactReference> {
    if content.len() as u64 > settings.max_artifact_bytes {
        bail!("artifact exceeds configured maximum size");
    }
    let digest = hex::encode(Sha256::digest(content));
    let directory = settings
        .artifact_root
        .join(&settings.tenant_id)
        .join("sha256");
    std::fs::create_dir_all(&directory).context("create artifact directory")?;
    let path = directory.join(&digest);
    if !path.exists() {
        let temporary = directory.join(format!(".{digest}.{}.tmp", Uuid::now_v7()));
        std::fs::write(&temporary, content).context("stage artifact")?;
        match std::fs::rename(&temporary, &path) {
            Ok(()) => {}
            Err(error) if path.exists() => {
                let _ = std::fs::remove_file(temporary);
                drop(error);
            }
            Err(error) => return Err(error).context("commit artifact"),
        }
    }
    Ok(ArtifactReference {
        uri: format!("artifact://{}/sha256/{digest}", settings.tenant_id),
        sha256: digest,
        size_bytes: content.len() as i64,
        media_type: media_type.into(),
    })
}

fn read_artifact(settings: &Settings, reference: &ArtifactReference) -> Result<Vec<u8>> {
    let expected_uri = format!(
        "artifact://{}/sha256/{}",
        settings.tenant_id, reference.sha256
    );
    if reference.uri != expected_uri || reference.sha256.len() != 64 {
        bail!("artifact reference is not canonical for this tenant");
    }
    let path = settings
        .artifact_root
        .join(&settings.tenant_id)
        .join("sha256")
        .join(&reference.sha256);
    let content = std::fs::read(path).context("read checkpoint artifact")?;
    if content.len() as i64 != reference.size_bytes
        || hex::encode(Sha256::digest(&content)) != reference.sha256
    {
        bail!("artifact failed size or SHA-256 verification");
    }
    Ok(content)
}

fn parse_settings(arguments: Vec<String>) -> Result<Settings> {
    let mut values = std::collections::HashMap::new();
    for pair in arguments.chunks_exact(2) {
        values.insert(pair[0].clone(), pair[1].clone());
    }
    if !arguments.len().is_multiple_of(2) {
        bail!("arguments must be --name value pairs");
    }
    let required = |name: &str| {
        values
            .get(name)
            .cloned()
            .with_context(|| format!("{name} is required"))
    };
    if required("--dev-mode")? != "true" {
        bail!("--dev-mode true is required for the plaintext development provider");
    }
    let tenant_id = required("--tenant")?;
    if tenant_id.is_empty()
        || tenant_id.len() > 128
        || !tenant_id
            .bytes()
            .all(|value| value.is_ascii_alphanumeric() || value == b'_' || value == b'-')
    {
        bail!("tenant must be a bounded filesystem-safe token");
    }
    let package_root = Path::new(&required("--package-root")?).canonicalize()?;
    let artifact_root_input = required("--artifact-root")?;
    std::fs::create_dir_all(&artifact_root_input).context("create artifact root")?;
    let artifact_root = Path::new(&artifact_root_input).canonicalize()?;
    // Match the Go tools' CLI contract: a bare host:port is accepted and
    // treated as the plaintext loopback development transport.
    let control_endpoint = required("--control-endpoint")?;
    let control_endpoint = if control_endpoint.contains("://") {
        control_endpoint
    } else {
        format!("http://{control_endpoint}")
    };
    Ok(Settings {
        control_endpoint,
        tenant_id,
        runtime_instance_id: required("--runtime-instance-id")?,
        package_root,
        artifact_root,
        poll_interval: Duration::from_millis(250),
        heartbeat_ttl: Duration::from_secs(60),
        max_artifact_bytes: 64 << 20,
    })
}
