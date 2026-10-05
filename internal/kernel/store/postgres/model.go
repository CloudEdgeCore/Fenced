package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"

	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
)

const modelDescriptorColumns = `
	id::text, tenant_id, provider, model_name, supports_streaming,
	input_price_micro_usd_per_million, output_price_micro_usd_per_million, price_revision, spec_hash, created_at`

const modelCallColumns = `
	id::text, tenant_id, task_id::text, run_id::text, attempt_id::text, model_ref, status,
	idempotency_key, request_hash, input_tokens, output_tokens, cost_micro_usd, price_revision,
	provider_request_id, finish_reason, usage_certainty, resource_version, created_at, updated_at`

func (s *Store) RegisterModelDescriptor(ctx context.Context, in kernelstore.RegisterModelDescriptorInput) (kernelstore.ModelDescriptor, error) {
	var zero kernelstore.ModelDescriptor
	descriptor, specHash, err := in.ValidateAndHash()
	if err != nil {
		return zero, err
	}
	descriptor.ID = s.newID()
	descriptor.SpecHash = specHash
	descriptor.CreatedAt = s.now()
	if _, err := s.pool.Exec(ctx, `INSERT INTO model_descriptors (
		id, tenant_id, provider, model_name, supports_streaming,
		input_price_micro_usd_per_million, output_price_micro_usd_per_million, price_revision, spec_hash, created_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		descriptor.ID.String(), descriptor.TenantID, descriptor.Provider, descriptor.ModelName,
		descriptor.SupportsStreaming, descriptor.InputPriceMicroUSDPerMillion, descriptor.OutputPriceMicroUSDPerMillion,
		descriptor.PriceRevision, descriptor.SpecHash[:], descriptor.CreatedAt); err != nil {
		if !isUniqueViolation(err) {
			return zero, classify(err)
		}
		// The same (tenant, provider, model, price revision) identity already
		// exists: an identical spec replays it, a different spec conflicts.
		existing, lookupErr := scanModelDescriptor(s.pool.QueryRow(ctx, `SELECT `+modelDescriptorColumns+`
			FROM model_descriptors WHERE tenant_id = $1 AND provider = $2 AND model_name = $3 AND price_revision = $4`,
			in.TenantID, in.Provider, in.ModelName, in.PriceRevision))
		if lookupErr != nil {
			return zero, classify(lookupErr)
		}
		if existing.SpecHash != specHash {
			return zero, kernelstore.ErrModelSpecConflict
		}
		return existing, nil
	}
	return descriptor, nil
}

func (s *Store) GetModelDescriptor(ctx context.Context, tenantID, provider, modelName string) (kernelstore.ModelDescriptor, error) {
	var zero kernelstore.ModelDescriptor
	if strings.TrimSpace(tenantID) == "" || strings.TrimSpace(provider) == "" || strings.TrimSpace(modelName) == "" {
		return zero, fmt.Errorf("tenant, provider, and model name are required")
	}
	descriptor, err := scanModelDescriptor(s.pool.QueryRow(ctx, `SELECT `+modelDescriptorColumns+`
		FROM model_descriptors WHERE tenant_id = $1 AND provider = $2 AND model_name = $3
		ORDER BY created_at DESC, price_revision DESC LIMIT 1`,
		tenantID, provider, modelName))
	if err != nil {
		return zero, classify(err)
	}
	return descriptor, nil
}

func (s *Store) CreateModelCall(ctx context.Context, in kernelstore.CreateModelCallInput) (kernelstore.CreateModelCallResult, error) {
	var result kernelstore.CreateModelCallResult
	requestHash, err := in.RequestHash()
	if err != nil {
		return result, err
	}
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	now := s.now()
	_, err = s.pool.Exec(ctx, `INSERT INTO model_calls (
		id, tenant_id, task_id, run_id, attempt_id, model_ref, price_revision,
		idempotency_key, request_hash, created_at, updated_at
	) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $10)`,
		in.ID.String(), in.TenantID, in.TaskID.String(), in.RunID.String(), in.AttemptID.String(),
		in.ModelRef, in.PriceRevision, in.IdempotencyKey, requestHash[:], now)
	if err != nil {
		if !isUniqueViolation(err) {
			return result, classify(err)
		}
		call, lookupErr := scanModelCall(s.pool.QueryRow(ctx, `SELECT `+modelCallColumns+` FROM model_calls
			WHERE tenant_id = $1 AND attempt_id = $2 AND model_ref = $3 AND idempotency_key = $4`,
			in.TenantID, in.AttemptID.String(), in.ModelRef, in.IdempotencyKey))
		if lookupErr != nil {
			return result, classify(lookupErr)
		}
		if call.RequestHash != requestHash {
			return result, fmt.Errorf("%w: idempotency key reused with a different invocation", kernelstore.ErrIdempotencyConflict)
		}
		result.ModelCall, result.Existing = call, true
		return result, nil
	}
	result.ModelCall, err = s.GetModelCall(ctx, in.TenantID, in.ID)
	return result, err
}

func (s *Store) GetModelCall(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.ModelCall, error) {
	call, err := scanModelCall(s.pool.QueryRow(ctx, `SELECT `+modelCallColumns+` FROM model_calls
		WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	if err != nil {
		return kernelstore.ModelCall{}, classify(err)
	}
	return call, nil
}

func (s *Store) FinishModelCall(ctx context.Context, in kernelstore.FinishModelCallInput) (kernelstore.ModelCall, error) {
	var zero kernelstore.ModelCall
	if in.UsageCertainty == "" {
		if in.InputTokens+in.OutputTokens > 0 {
			in.UsageCertainty = kernelstore.ModelUsageKnown
		} else if in.Status == kernelstore.ModelCallCompleted {
			in.UsageCertainty = kernelstore.ModelUsageKnownZero
		} else {
			in.UsageCertainty = kernelstore.ModelUsageUnknown
		}
	}
	if in.ModelCallID == uuid.Nil || in.ExpectedVersion <= 0 || !in.Status.Terminal() ||
		in.InputTokens < 0 || in.OutputTokens < 0 || in.CostMicroUSD < 0 || strings.TrimSpace(in.PriceRevision) == "" || !in.UsageCertainty.Valid() {
		return zero, fmt.Errorf("call ID, expected version, terminal status, non-negative usage, and price revision are required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return zero, err
	}
	defer rollback(ctx, tx)
	current, err := scanModelCall(tx.QueryRow(ctx, `SELECT `+modelCallColumns+` FROM model_calls
		WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, in.TenantID, in.ModelCallID.String()))
	if err != nil {
		return zero, classify(err)
	}
	if current.ResourceVersion != in.ExpectedVersion {
		return zero, versionConflict("model call", in.ModelCallID, in.ExpectedVersion, current.ResourceVersion)
	}
	if !kernelstore.CanTransitionModelCall(current.Status, in.Status) {
		// Terminal -> same terminal is an idempotent replay of an already
		// finished invocation (duplicate delivery of the same idempotency
		// key), not a lifecycle violation: return the recorded outcome.
		if current.Status == in.Status {
			return current, nil
		}
		return zero, fmt.Errorf("%w: model call %s -> %s", kernelstore.ErrInvalidTransition, current.Status, in.Status)
	}
	updated, err := scanModelCall(tx.QueryRow(ctx, `UPDATE model_calls
		SET status = $1, input_tokens = $2, output_tokens = $3, cost_micro_usd = $4, price_revision = $5,
			provider_request_id = $6, finish_reason = $7, usage_certainty = $8,
			resource_version = resource_version + 1, updated_at = $9
		WHERE tenant_id = $10 AND id = $11 AND resource_version = $12 RETURNING `+modelCallColumns,
		string(in.Status), in.InputTokens, in.OutputTokens, in.CostMicroUSD, in.PriceRevision,
		nullableString(in.ProviderRequestID), nullableString(in.FinishReason), in.UsageCertainty, s.now(),
		in.TenantID, in.ModelCallID.String(), in.ExpectedVersion))
	if err != nil {
		return zero, classifyCAS(err, "model call", in.ModelCallID, in.ExpectedVersion)
	}
	if err := tx.Commit(ctx); err != nil {
		return zero, classify(err)
	}
	return updated, nil
}

func nullableString(value string) any {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	return value
}

func scanModelDescriptor(row scanner) (kernelstore.ModelDescriptor, error) {
	var descriptor kernelstore.ModelDescriptor
	var id, tenantID, provider, modelName string
	var specHash []byte
	err := row.Scan(&id, &tenantID, &provider, &modelName, &descriptor.SupportsStreaming,
		&descriptor.InputPriceMicroUSDPerMillion, &descriptor.OutputPriceMicroUSDPerMillion,
		&descriptor.PriceRevision, &specHash, &descriptor.CreatedAt)
	if err != nil {
		return descriptor, err
	}
	descriptor.ID, err = uuid.Parse(id)
	if err != nil {
		return descriptor, fmt.Errorf("parse model descriptor ID: %w", err)
	}
	if len(specHash) != sha256.Size {
		return descriptor, fmt.Errorf("model descriptor spec hash is invalid")
	}
	copy(descriptor.SpecHash[:], specHash)
	descriptor.TenantID, descriptor.Provider, descriptor.ModelName = tenantID, provider, modelName
	return descriptor, nil
}

func scanModelCall(row scanner) (kernelstore.ModelCall, error) {
	var call kernelstore.ModelCall
	var id, tenantID, taskID, runID, attemptID, modelRef, status string
	var requestHash []byte
	var providerRequestID, finishReason sql.NullString
	err := row.Scan(&id, &tenantID, &taskID, &runID, &attemptID, &modelRef, &status,
		&call.IdempotencyKey, &requestHash, &call.InputTokens, &call.OutputTokens, &call.CostMicroUSD,
		&call.PriceRevision, &providerRequestID, &finishReason, &call.UsageCertainty, &call.ResourceVersion,
		&call.CreatedAt, &call.UpdatedAt)
	if err != nil {
		return call, err
	}
	if call.ID, err = uuid.Parse(id); err != nil {
		return call, fmt.Errorf("parse model call ID: %w", err)
	}
	if call.TaskID, err = uuid.Parse(taskID); err != nil {
		return call, fmt.Errorf("parse model call task ID: %w", err)
	}
	if call.RunID, err = uuid.Parse(runID); err != nil {
		return call, fmt.Errorf("parse model call run ID: %w", err)
	}
	if call.AttemptID, err = uuid.Parse(attemptID); err != nil {
		return call, fmt.Errorf("parse model call attempt ID: %w", err)
	}
	if len(requestHash) != sha256.Size {
		return call, fmt.Errorf("model call request hash is invalid")
	}
	copy(call.RequestHash[:], requestHash)
	if providerRequestID.Valid {
		call.ProviderRequestID = providerRequestID.String
	}
	if finishReason.Valid {
		call.FinishReason = finishReason.String
	}
	call.TenantID, call.ModelRef = tenantID, modelRef
	call.Status = kernelstore.ModelCallStatus(status)
	return call, nil
}
