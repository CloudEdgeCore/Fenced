package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
)

const toolDescriptorColumns = `
	id::text, tenant_id, name, version, side_effect_risk, actions, resource_patterns, params_schema,
	spec_hash, created_at`

const toolCallColumns = `
	id::text, tenant_id, task_id::text, run_id::text, attempt_id::text, tool_name, tool_version,
	action, resource, args_hash, status, decision_reasons, policy_revision, approval_id::text,
	idempotency_key, request_hash, resource_version, created_at, updated_at`

const toolApprovalColumns = `
	id::text, tenant_id, call_id::text, task_id::text, run_id::text, attempt_id::text, tool_name,
	tool_version, action, resource, args_hash, status, requested_at, expires_at, decided_at, decided_by,
	resource_version`

func (s *Store) RegisterToolDescriptor(ctx context.Context, in kernelstore.RegisterToolDescriptorInput) (kernelstore.ToolDescriptor, error) {
	var zero kernelstore.ToolDescriptor
	descriptor, specHash, err := in.ValidateAndHash()
	if err != nil {
		return zero, err
	}
	existing, err := s.GetToolDescriptor(ctx, in.TenantID, in.Name, in.Version)
	if err == nil {
		if existing.SpecHash != specHash {
			return zero, kernelstore.ErrToolSpecConflict
		}
		return existing, nil
	}
	if !errors.Is(err, kernelstore.ErrNotFound) {
		return zero, err
	}
	descriptor.ID = s.newID()
	descriptor.SpecHash = specHash
	descriptor.CreatedAt = s.now()
	if _, err := s.pool.Exec(ctx, `INSERT INTO tool_descriptors (
		id, tenant_id, name, version, side_effect_risk, actions, resource_patterns, params_schema, spec_hash, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		descriptor.ID.String(), descriptor.TenantID, descriptor.Name, descriptor.Version,
		string(descriptor.SideEffectRisk), descriptor.Actions, descriptor.ResourcePatterns,
		descriptor.ParamsSchema, descriptor.SpecHash[:], descriptor.CreatedAt); err != nil {
		if isUniqueViolation(err) {
			// A concurrent registration won; return the existing descriptor.
			existing, lookupErr := s.GetToolDescriptor(ctx, in.TenantID, in.Name, in.Version)
			if lookupErr != nil {
				return zero, classify(lookupErr)
			}
			if existing.SpecHash != specHash {
				return zero, kernelstore.ErrToolSpecConflict
			}
			return existing, nil
		}
		return zero, classify(err)
	}
	return descriptor, nil
}

func (s *Store) GetToolDescriptor(ctx context.Context, tenantID, name, version string) (kernelstore.ToolDescriptor, error) {
	var zero kernelstore.ToolDescriptor
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(name) == "" {
		return zero, fmt.Errorf("tenant and tool name are required")
	}
	query := `SELECT ` + toolDescriptorColumns + ` FROM tool_descriptors WHERE tenant_id = $1 AND name = $2`
	args := []any{tenantID, name}
	if version == "" {
		query += ` ORDER BY version DESC LIMIT 1`
	} else {
		query += ` AND version = $3`
		args = append(args, version)
	}
	descriptor, err := scanToolDescriptor(s.pool.QueryRow(ctx, query, args...))
	if err != nil {
		return zero, classify(err)
	}
	return descriptor, nil
}

func (s *Store) ListToolDescriptors(ctx context.Context, tenantID string) ([]kernelstore.ToolDescriptor, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+toolDescriptorColumns+` FROM tool_descriptors
		WHERE tenant_id = $1 ORDER BY name, version`, tenantID)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	var descriptors []kernelstore.ToolDescriptor
	for rows.Next() {
		descriptor, err := scanToolDescriptor(rows)
		if err != nil {
			return nil, classify(err)
		}
		descriptors = append(descriptors, descriptor)
	}
	return descriptors, rows.Err()
}

func (s *Store) CreateToolCall(ctx context.Context, in kernelstore.CreateToolCallInput) (kernelstore.CreateToolCallResult, error) {
	var result kernelstore.CreateToolCallResult
	requestHash, err := in.RequestHash()
	if err != nil {
		return result, err
	}
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	now := s.now()
	_, err = s.pool.Exec(ctx, `INSERT INTO tool_calls (
		id, tenant_id, task_id, run_id, attempt_id, tool_name, tool_version, action, resource,
		args_hash, idempotency_key, request_hash, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $13)`,
		in.ID.String(), in.TenantID, in.TaskID.String(), in.RunID.String(), in.AttemptID.String(),
		in.ToolName, in.ToolVersion, in.Action, in.Resource, in.ArgsHash[:], in.IdempotencyKey,
		requestHash[:], now)
	if err != nil {
		if !isUniqueViolation(err) {
			return result, classify(err)
		}
		// Idempotent retry or key reuse: resolve the existing call.
		call, lookupErr := scanToolCall(s.pool.QueryRow(ctx, `SELECT `+toolCallColumns+` FROM tool_calls
			WHERE tenant_id = $1 AND attempt_id = $2 AND tool_name = $3 AND idempotency_key = $4`,
			in.TenantID, in.AttemptID.String(), in.ToolName, in.IdempotencyKey))
		if lookupErr != nil {
			return result, classify(lookupErr)
		}
		if call.RequestHash != requestHash {
			return result, fmt.Errorf("%w: idempotency key reused with a different invocation", kernelstore.ErrIdempotencyConflict)
		}
		result.ToolCall, result.Existing = call, true
		return result, nil
	}
	result.ToolCall, err = s.GetToolCall(ctx, in.TenantID, in.ID)
	return result, err
}

func (s *Store) GetToolCall(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.ToolCall, error) {
	call, err := scanToolCall(s.pool.QueryRow(ctx, `SELECT `+toolCallColumns+` FROM tool_calls
		WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	if err != nil {
		return kernelstore.ToolCall{}, classify(err)
	}
	return call, nil
}

func (s *Store) UpdateToolCall(ctx context.Context, in kernelstore.UpdateToolCallInput) (kernelstore.ToolCall, error) {
	var zero kernelstore.ToolCall
	if in.ToolCallID == uuid.Nil || in.ExpectedVersion <= 0 {
		return zero, fmt.Errorf("tool call ID and expected version are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)
	var current kernelstore.ToolCall
	current, err = scanToolCall(tx.QueryRow(ctx, `SELECT `+toolCallColumns+` FROM tool_calls
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.ToolCallID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return zero, versionConflict("tool call", in.ToolCallID, in.ExpectedVersion, current.ResourceVersion)
	}
	if !kernelstore.CanTransitionTo(current.Status, in.Status) {
		return zero, fmt.Errorf("%w: tool call %s -> %s", kernelstore.ErrInvalidTransition, current.Status, in.Status)
	}
	reasons := in.DecisionReasons
	if reasons == nil {
		reasons = []string{}
	}
	updated, err := scanToolCall(tx.QueryRow(ctx, `UPDATE tool_calls SET status = $1, decision_reasons = $2,
		policy_revision = $3, approval_id = $4, resource_version = resource_version + 1, updated_at = $5
		WHERE tenant_id = $6 AND id = $7 AND resource_version = $8 RETURNING `+toolCallColumns,
		string(in.Status), reasons, in.PolicyRevision, nullableUUID(in.ApprovalID), s.now(),
		in.TenantID, in.ToolCallID.String(), in.ExpectedVersion))
	if err != nil {
		return zero, classifyCAS(err, "tool call", in.ToolCallID, in.ExpectedVersion)
	}
	// Audit the decision points of the tool decision chain (ADR-014);
	// intermediate parking transitions are not audited.
	switch in.Status {
	case kernelstore.ToolCallDenied, kernelstore.ToolCallApproved, kernelstore.ToolCallExecuted, kernelstore.ToolCallFailed:
		if err := auditHook(ctx, tx, in.TenantID, "tool_call.decided", "ToolCall", in.ToolCallID, map[string]any{
			"status": string(in.Status), "tool": current.ToolName + "@" + current.ToolVersion,
			"action": current.Action, "resource": current.Resource, "reasons": in.DecisionReasons,
		}, s.now()); err != nil {
			return zero, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func (s *Store) CreateToolApproval(ctx context.Context, in kernelstore.CreateToolApprovalInput) (kernelstore.CreateToolApprovalResult, error) {
	var result kernelstore.CreateToolApprovalResult
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	if err := validateApprovalInput(in); err != nil {
		return result, err
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)
	// The insert is guarded by a savepoint: a concurrent duplicate on the
	// binding unique key aborts only the insert, and the transaction (which
	// carries the audit append) can still commit.
	if _, err := tx.Exec(ctx, `SAVEPOINT create_approval`); err != nil {
		return result, classify(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO tool_approvals (
		id, tenant_id, call_id, task_id, run_id, attempt_id, tool_name, tool_version, action, resource,
		args_hash, requested_at, expires_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)`,
		in.ID.String(), in.TenantID, in.CallID.String(), in.TaskID.String(), in.RunID.String(),
		in.AttemptID.String(), in.ToolName, in.ToolVersion, in.Action, in.Resource, in.ArgsHash[:],
		in.RequestedAt, in.ExpiresAt)
	if err != nil {
		if !isUniqueViolation(err) {
			return result, classify(err)
		}
		if _, rollbackErr := tx.Exec(ctx, `ROLLBACK TO SAVEPOINT create_approval`); rollbackErr != nil {
			return result, classify(rollbackErr)
		}
		// The same bound approval already exists: return it without re-audit.
		approval, lookupErr := s.GetToolApproval(ctx, in.TenantID, in.ID)
		if lookupErr != nil {
			// The conflict may be on the binding unique key with a different ID.
			approval, lookupErr = scanToolApproval(tx.QueryRow(ctx, `SELECT `+toolApprovalColumns+` FROM tool_approvals
				WHERE tenant_id = $1 AND attempt_id = $2 AND tool_name = $3 AND tool_version = $4 AND action = $5
				AND resource = $6 AND args_hash = $7`,
				in.TenantID, in.AttemptID.String(), in.ToolName, in.ToolVersion, in.Action, in.Resource, in.ArgsHash[:]))
			if lookupErr != nil {
				return result, classify(lookupErr)
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		result.ToolApproval, result.Existing = approval, true
		return result, nil
	}
	if err := auditHook(ctx, tx, in.TenantID, "approval.requested", "ToolApproval", in.ID, map[string]any{
		"tool": in.ToolName + "@" + in.ToolVersion, "action": in.Action, "resource": in.Resource,
		"attemptId": in.AttemptID.String(), "expiresAt": in.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}, in.RequestedAt); err != nil {
		return result, err
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	result.ToolApproval, err = s.GetToolApproval(ctx, in.TenantID, in.ID)
	return result, err
}

func (s *Store) GetToolApproval(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.ToolApproval, error) {
	approval, err := scanToolApproval(s.pool.QueryRow(ctx, `SELECT `+toolApprovalColumns+` FROM tool_approvals
		WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	if err != nil {
		return kernelstore.ToolApproval{}, classify(err)
	}
	return approval, nil
}

func (s *Store) DecideToolApproval(ctx context.Context, in kernelstore.DecideToolApprovalInput) (kernelstore.ToolApproval, error) {
	var zero kernelstore.ToolApproval
	if !in.Valid() {
		return zero, fmt.Errorf("approval decision is invalid")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)
	current, err := scanToolApproval(tx.QueryRow(ctx, `SELECT `+toolApprovalColumns+` FROM tool_approvals
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.ApprovalID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return zero, versionConflict("tool approval", in.ApprovalID, in.ExpectedVersion, current.ResourceVersion)
	}
	if current.Status != kernelstore.ToolApprovalPending {
		return zero, fmt.Errorf("%w: approval is already %s", kernelstore.ErrInvalidTransition, current.Status)
	}
	if !in.Now.Before(current.ExpiresAt) {
		if _, expireErr := scanToolApproval(tx.QueryRow(ctx, `UPDATE tool_approvals
			SET status = 'EXPIRED', resource_version = resource_version + 1
			WHERE tenant_id = $1 AND id = $2 AND resource_version = $3 RETURNING `+toolApprovalColumns,
			in.TenantID, in.ApprovalID.String(), in.ExpectedVersion)); expireErr != nil {
			return zero, classifyCAS(expireErr, "tool approval", in.ApprovalID, in.ExpectedVersion)
		}
		if err := auditHook(ctx, tx, in.TenantID, "approval.expired", "ToolApproval", in.ApprovalID, map[string]any{
			"tool": current.ToolName + "@" + current.ToolVersion, "action": current.Action, "resource": current.Resource,
		}, in.Now); err != nil {
			return zero, err
		}
		if err := tx.Commit(ctx); err != nil {
			return zero, classify(err)
		}
		return zero, fmt.Errorf("%w: approval expired at %s", kernelstore.ErrApprovalNotUsable, current.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z"))
	}
	updated, err := scanToolApproval(tx.QueryRow(ctx, `UPDATE tool_approvals
		SET status = $1, decided_at = $2, decided_by = $3, resource_version = resource_version + 1
		WHERE tenant_id = $4 AND id = $5 AND resource_version = $6 RETURNING `+toolApprovalColumns,
		string(in.Decision), in.Now, in.DecidedBy, in.TenantID, in.ApprovalID.String(), in.ExpectedVersion))
	if err != nil {
		return zero, classifyCAS(err, "tool approval", in.ApprovalID, in.ExpectedVersion)
	}
	if err := auditHook(ctx, tx, in.TenantID, "approval.decided", "ToolApproval", in.ApprovalID, map[string]any{
		"decision": string(in.Decision), "decidedBy": in.DecidedBy,
		"tool": current.ToolName + "@" + current.ToolVersion, "action": current.Action, "resource": current.Resource,
	}, in.Now); err != nil {
		return zero, err
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func (s *Store) GetRuntimeReceipt(ctx context.Context, tenantID string, attemptID uuid.UUID, operation, idempotencyKey string) (kernelstore.RuntimeReceipt, error) {
	var zero kernelstore.RuntimeReceipt
	var requestHash []byte
	var response []byte
	err := s.pool.QueryRow(ctx, `SELECT request_hash, response FROM runtime_operation_receipts
		WHERE tenant_id = $1 AND attempt_id = $2 AND operation = $3 AND idempotency_key = $4`,
		tenantID, attemptID.String(), operation, idempotencyKey).Scan(&requestHash, &response)
	if err != nil {
		return zero, classify(err)
	}
	if len(requestHash) != sha256.Size {
		return zero, fmt.Errorf("stored receipt request hash is invalid")
	}
	copy(zero.RequestHash[:], requestHash)
	zero.Response = json.RawMessage(response)
	return zero, nil
}

func (s *Store) WriteRuntimeReceipt(ctx context.Context, in kernelstore.WriteRuntimeReceiptInput) error {
	if strings.TrimSpace(in.TenantID) == "" || in.AttemptID == uuid.Nil ||
		strings.TrimSpace(in.Operation) == "" || strings.TrimSpace(in.IdempotencyKey) == "" ||
		len(in.Response) == 0 || in.RequestHash == ([sha256.Size]byte{}) {
		return fmt.Errorf("receipt fields are required")
	}
	tag, err := s.pool.Exec(ctx, `INSERT INTO runtime_operation_receipts (
		tenant_id, attempt_id, operation, idempotency_key, request_hash, response, processed_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7)
	ON CONFLICT (tenant_id, attempt_id, operation, idempotency_key) DO NOTHING`,
		in.TenantID, in.AttemptID.String(), in.Operation, in.IdempotencyKey,
		in.RequestHash[:], in.Response, s.now())
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() > 0 {
		return nil
	}
	// A concurrent writer already recorded this operation: the request hash
	// must match, otherwise the idempotency key was reused with different
	// semantics.
	existing, err := s.GetRuntimeReceipt(ctx, in.TenantID, in.AttemptID, in.Operation, in.IdempotencyKey)
	if err != nil {
		return classify(err)
	}
	if existing.RequestHash != in.RequestHash {
		return fmt.Errorf("%w: idempotency key reused with a different invocation", kernelstore.ErrIdempotencyConflict)
	}
	return nil
}

func validateApprovalInput(in kernelstore.CreateToolApprovalInput) error {
	if strings.TrimSpace(in.TenantID) == "" || in.CallID == uuid.Nil || in.TaskID == uuid.Nil ||
		in.RunID == uuid.Nil || in.AttemptID == uuid.Nil ||
		strings.TrimSpace(in.ToolName) == "" || strings.TrimSpace(in.ToolVersion) == "" ||
		strings.TrimSpace(in.Action) == "" || strings.TrimSpace(in.Resource) == "" ||
		in.ArgsHash == ([sha256.Size]byte{}) || in.RequestedAt.IsZero() || in.ExpiresAt.IsZero() {
		return fmt.Errorf("approval binding fields are required")
	}
	if !in.ExpiresAt.After(in.RequestedAt) {
		return fmt.Errorf("approval expiry must be after the request time")
	}
	return nil
}

func nullableUUID(id *uuid.UUID) any {
	if id == nil {
		return nil
	}
	return id.String()
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func scanToolDescriptor(row scanner) (kernelstore.ToolDescriptor, error) {
	var descriptor kernelstore.ToolDescriptor
	var id, tenantID, name, version, risk string
	var actions, resources []string
	var specHash []byte
	err := row.Scan(&id, &tenantID, &name, &version, &risk, &actions, &resources, &descriptor.ParamsSchema, &specHash, &descriptor.CreatedAt)
	if err != nil {
		return descriptor, err
	}
	descriptor.ID, err = uuid.Parse(id)
	if err != nil {
		return descriptor, fmt.Errorf("parse tool descriptor ID: %w", err)
	}
	if len(specHash) != sha256.Size {
		return descriptor, fmt.Errorf("tool descriptor spec hash is invalid")
	}
	copy(descriptor.SpecHash[:], specHash)
	descriptor.TenantID, descriptor.Name, descriptor.Version = tenantID, name, version
	descriptor.SideEffectRisk = kernelstore.ToolRisk(risk)
	descriptor.Actions, descriptor.ResourcePatterns = actions, resources
	return descriptor, nil
}

func scanToolCall(row scanner) (kernelstore.ToolCall, error) {
	var call kernelstore.ToolCall
	var id, tenantID, taskID, runID, attemptID, toolName, toolVersion, action, resource, status string
	var argsHash, requestHash []byte
	var approvalID sql.NullString
	var reasons []string
	err := row.Scan(&id, &tenantID, &taskID, &runID, &attemptID, &toolName, &toolVersion, &action, &resource,
		&argsHash, &status, &reasons, &call.PolicyRevision, &approvalID, &call.IdempotencyKey,
		&requestHash, &call.ResourceVersion, &call.CreatedAt, &call.UpdatedAt)
	if err != nil {
		return call, err
	}
	if call.ID, err = uuid.Parse(id); err != nil {
		return call, fmt.Errorf("parse tool call ID: %w", err)
	}
	if call.TaskID, err = uuid.Parse(taskID); err != nil {
		return call, fmt.Errorf("parse tool call task ID: %w", err)
	}
	if call.RunID, err = uuid.Parse(runID); err != nil {
		return call, fmt.Errorf("parse tool call run ID: %w", err)
	}
	if call.AttemptID, err = uuid.Parse(attemptID); err != nil {
		return call, fmt.Errorf("parse tool call attempt ID: %w", err)
	}
	if len(argsHash) != sha256.Size || len(requestHash) != sha256.Size {
		return call, fmt.Errorf("tool call hashes are invalid")
	}
	copy(call.ArgsHash[:], argsHash)
	copy(call.RequestHash[:], requestHash)
	if approvalID.Valid {
		id, parseErr := uuid.Parse(approvalID.String)
		if parseErr != nil {
			return call, fmt.Errorf("parse tool call approval ID: %w", parseErr)
		}
		call.ApprovalID = &id
	}
	call.TenantID, call.ToolName, call.ToolVersion = tenantID, toolName, toolVersion
	call.Action, call.Resource = action, resource
	call.Status = kernelstore.ToolCallStatus(status)
	call.DecisionReasons = reasons
	return call, nil
}

func scanToolApproval(row scanner) (kernelstore.ToolApproval, error) {
	var approval kernelstore.ToolApproval
	var id, tenantID, callID, taskID, runID, attemptID, toolName, toolVersion, action, resource, status string
	var argsHash []byte
	var decidedAt sql.NullTime
	var decidedBy sql.NullString
	err := row.Scan(&id, &tenantID, &callID, &taskID, &runID, &attemptID, &toolName, &toolVersion,
		&action, &resource, &argsHash, &status, &approval.RequestedAt, &approval.ExpiresAt, &decidedAt, &decidedBy,
		&approval.ResourceVersion)
	if err != nil {
		return approval, err
	}
	if approval.ID, err = uuid.Parse(id); err != nil {
		return approval, fmt.Errorf("parse approval ID: %w", err)
	}
	if approval.CallID, err = uuid.Parse(callID); err != nil {
		return approval, fmt.Errorf("parse approval call ID: %w", err)
	}
	if approval.TaskID, err = uuid.Parse(taskID); err != nil {
		return approval, fmt.Errorf("parse approval task ID: %w", err)
	}
	if approval.RunID, err = uuid.Parse(runID); err != nil {
		return approval, fmt.Errorf("parse approval run ID: %w", err)
	}
	if approval.AttemptID, err = uuid.Parse(attemptID); err != nil {
		return approval, fmt.Errorf("parse approval attempt ID: %w", err)
	}
	if len(argsHash) != sha256.Size {
		return approval, fmt.Errorf("approval args hash is invalid")
	}
	copy(approval.ArgsHash[:], argsHash)
	if decidedAt.Valid {
		approval.DecidedAt = &decidedAt.Time
	}
	if decidedBy.Valid {
		approval.DecidedBy = decidedBy.String
	}
	approval.TenantID, approval.ToolName, approval.ToolVersion = tenantID, toolName, toolVersion
	approval.Action, approval.Resource = action, resource
	approval.Status = kernelstore.ToolApprovalStatus(status)
	return approval, nil
}
