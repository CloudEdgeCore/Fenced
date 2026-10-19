// The checkpoint handling of the adapter worker: committing a logical
// checkpoint and restoring the Agent state a compatible checkpoint carries.
package adapter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	agentversion "github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	agent "github.com/CloudEdgeCore/Fenced/sdk/agent"
	uuid "github.com/google/uuid"
)

func (w *Worker) commitLogicalCheckpoint(
	ctx context.Context,
	client *agent.Client,
	assignment *runtimev1.Assignment,
	target agentversion.RuntimeTarget,
	policy agentversion.CheckpointPolicy,
	version int64,
	operation string,
) (int64, error) {
	checkpoint, err := client.Checkpoint(ctx, assignment.GetIdentity().GetAttemptId())
	if err != nil {
		return version, err
	}
	if checkpoint.Checkpoint.SchemaVersion != policy.SchemaVersion {
		return version, fmt.Errorf("runtime checkpoint schema %q does not match manifest schema %q",
			checkpoint.Checkpoint.SchemaVersion, policy.SchemaVersion)
	}
	trimmedState := bytes.TrimSpace(checkpoint.Checkpoint.State)
	if len(trimmedState) == 0 || bytes.Equal(trimmedState, []byte("{}")) {
		// An empty checkpoint state contains no recoverable progress. Do not
		// commit an empty checkpoint that would clobber a previous confirmed
		// checkpoint.
		return version, nil
	}
	document, err := json.Marshal(checkpoint.Checkpoint)
	if err != nil {
		return version, err
	}
	artifact, err := w.artifacts.Put(ctx, w.tenantID, checkpointMediaType, bytes.NewReader(document))
	if err != nil {
		return version, fmt.Errorf("persist adapter checkpoint: %w", err)
	}
	checkpointID, err := uuid.NewV7()
	if err != nil {
		return version, err
	}
	committed, err := w.control.CommitCheckpoint(ctx, &runtimev1.CommitCheckpointRequest{
		Identity: assignment.GetIdentity(), ExpectedAttemptVersion: version,
		IdempotencyKey: operationKey(assignment, operation), CheckpointId: checkpointID.String(),
		AgentVersionRef: assignment.GetAgentVersionRef(), Provider: ProviderName,
		RuntimeAbi: target.RuntimeABI, SchemaVersion: CheckpointSchema, State: artifactProto(artifact),
	})
	if err != nil {
		return version, fmt.Errorf("commit adapter checkpoint: %w", err)
	}
	return committed.GetAttemptVersion(), nil
}

func (w *Worker) restore(ctx context.Context, client *agent.Client, assignment *runtimev1.Assignment, target agentversion.RuntimeTarget, policy agentversion.CheckpointPolicy) error {
	checkpoint := assignment.GetResumeCheckpoint()
	if checkpoint.GetAgentVersionRef() != assignment.GetAgentVersionRef() ||
		checkpoint.GetRuntimeClass() != assignment.GetRuntimeClass() ||
		checkpoint.GetProvider() != ProviderName ||
		checkpoint.GetRuntimeAbi() != target.RuntimeABI ||
		checkpoint.GetSchemaVersion() != CheckpointSchema {
		return fmt.Errorf("checkpoint is incompatible with adapter assignment")
	}
	reference, err := artifactFromProto(checkpoint.GetState())
	if err != nil {
		return err
	}
	reader, err := w.artifacts.Open(ctx, w.tenantID, reference)
	if err != nil {
		return err
	}
	defer reader.Close()
	decoder := json.NewDecoder(io.LimitReader(reader, 2<<20))
	decoder.DisallowUnknownFields()
	var sdkCheckpoint agent.Checkpoint
	if err := decoder.Decode(&sdkCheckpoint); err != nil {
		return fmt.Errorf("decode adapter checkpoint: %w", err)
	}
	if sdkCheckpoint.SchemaVersion != policy.SchemaVersion {
		return fmt.Errorf("checkpoint logical schema does not match the AgentVersion manifest")
	}
	_, err = client.Restore(ctx, agent.RestoreRequest{
		ExecutionID: assignment.GetIdentity().GetAttemptId(), Checkpoint: sdkCheckpoint,
	})
	return err
}
