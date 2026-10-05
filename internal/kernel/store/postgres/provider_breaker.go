package postgres

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/model/provider"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// AcquireCircuit coordinates closed/open/half-open state across gateway
// replicas. The row lock makes the half-open probe token globally exclusive.
func (s *Store) AcquireCircuit(ctx context.Context, in provider.CircuitAcquire) (provider.CircuitPermit, error) {
	if in.Provider == "" || in.Threshold < 1 || in.Cooldown <= 0 || in.ProbeTTL <= 0 || in.Now.IsZero() {
		return provider.CircuitPermit{}, fmt.Errorf("valid provider circuit acquire input is required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return provider.CircuitPermit{}, err
	}
	defer rollback(ctx, tx)
	if _, err := tx.Exec(ctx, `INSERT INTO provider_circuit_breakers (provider_name, updated_at)
		VALUES ($1, $2) ON CONFLICT (provider_name) DO NOTHING`, in.Provider, in.Now.UTC()); err != nil {
		return provider.CircuitPermit{}, classify(err)
	}
	var failures int
	var openedAt, probeUntil *time.Time
	var probeToken *uuid.UUID
	if err := tx.QueryRow(ctx, `SELECT consecutive_failures, opened_at, probe_token, probe_until
		FROM provider_circuit_breakers WHERE provider_name = $1 FOR UPDATE`, in.Provider).
		Scan(&failures, &openedAt, &probeToken, &probeUntil); err != nil {
		return provider.CircuitPermit{}, classify(err)
	}
	if failures < in.Threshold {
		if err := tx.Commit(ctx); err != nil {
			return provider.CircuitPermit{}, classify(err)
		}
		return provider.CircuitPermit{}, nil
	}
	now := in.Now.UTC()
	if openedAt != nil && now.Before(openedAt.Add(in.Cooldown)) {
		return provider.CircuitPermit{}, provider.ErrCircuitOpen
	}
	if probeToken != nil && probeUntil != nil && now.Before(*probeUntil) {
		return provider.CircuitPermit{}, provider.ErrCircuitOpen
	}
	token := s.newID()
	if _, err := tx.Exec(ctx, `UPDATE provider_circuit_breakers
		SET probe_token = $2, probe_until = $3, updated_at = $4 WHERE provider_name = $1`,
		in.Provider, token, now.Add(in.ProbeTTL), now); err != nil {
		return provider.CircuitPermit{}, classify(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return provider.CircuitPermit{}, classify(err)
	}
	return provider.CircuitPermit{ProbeToken: token.String()}, nil
}

// RecordCircuit updates shared health. A stale half-open result is fenced so
// it cannot overwrite a newer probe's decision.
func (s *Store) RecordCircuit(ctx context.Context, in provider.CircuitRecord) error {
	if in.Provider == "" || in.Threshold < 1 || in.Now.IsZero() {
		return fmt.Errorf("valid provider circuit record input is required")
	}
	tx, err := s.begin(ctx)
	if err != nil {
		return err
	}
	defer rollback(ctx, tx)
	var failures int
	var currentProbe *uuid.UUID
	err = tx.QueryRow(ctx, `SELECT consecutive_failures, probe_token
		FROM provider_circuit_breakers WHERE provider_name = $1 FOR UPDATE`, in.Provider).Scan(&failures, &currentProbe)
	if errors.Is(err, pgx.ErrNoRows) {
		if _, err = tx.Exec(ctx, `INSERT INTO provider_circuit_breakers
			(provider_name, consecutive_failures, updated_at) VALUES ($1, 0, $2)`, in.Provider, in.Now.UTC()); err != nil {
			return classify(err)
		}
		failures = 0
	} else if err != nil {
		return classify(err)
	}
	if in.ProbeToken != "" {
		token, parseErr := uuid.Parse(in.ProbeToken)
		if parseErr != nil || currentProbe == nil || *currentProbe != token {
			return fmt.Errorf("%w: stale provider circuit probe", provider.ErrCircuitOpen)
		}
	}
	now := in.Now.UTC()
	switch {
	case in.Success:
		_, err = tx.Exec(ctx, `UPDATE provider_circuit_breakers SET consecutive_failures = 0,
			opened_at = NULL, probe_token = NULL, probe_until = NULL, updated_at = $2 WHERE provider_name = $1`, in.Provider, now)
	case !in.Retryable:
		_, err = tx.Exec(ctx, `UPDATE provider_circuit_breakers SET probe_token = NULL,
			probe_until = NULL, updated_at = $2 WHERE provider_name = $1`, in.Provider, now)
	default:
		failures++
		openedAt := any(nil)
		if failures >= in.Threshold {
			openedAt = now
		}
		_, err = tx.Exec(ctx, `UPDATE provider_circuit_breakers SET consecutive_failures = $2,
			opened_at = COALESCE($3, opened_at), probe_token = NULL, probe_until = NULL, updated_at = $4
			WHERE provider_name = $1`, in.Provider, failures, openedAt, now)
	}
	if err != nil {
		return classify(err)
	}
	return classify(tx.Commit(ctx))
}
