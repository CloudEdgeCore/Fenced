// Package migrate applies ordered, checksum-protected SQL migrations.
package migrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const advisoryLockKey int64 = 0x41474E544F534D47 // "AGNTOSMG"

type Result struct {
	Applied []string
}

func Apply(ctx context.Context, pool *pgxpool.Pool, directory string) (Result, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		return Result{}, fmt.Errorf("read migration directory: %w", err)
	}

	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".up.sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	if len(names) == 0 {
		return Result{}, fmt.Errorf("no .up.sql migrations found in %s", directory)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		return Result{}, fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, "SELECT pg_advisory_lock($1)", advisoryLockKey); err != nil {
		return Result{}, fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.WithoutCancel(ctx), "SELECT pg_advisory_unlock($1)", advisoryLockKey)
	}()

	if _, err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS fenced_schema_migrations (
			version text PRIMARY KEY,
			checksum bytea NOT NULL CHECK (octet_length(checksum) = 32),
			applied_at timestamptz NOT NULL DEFAULT now()
		)`); err != nil {
		return Result{}, fmt.Errorf("create migration ledger: %w", err)
	}

	result := Result{}
	for _, name := range names {
		path := filepath.Join(directory, name)
		sqlBytes, err := os.ReadFile(path)
		if err != nil {
			return result, fmt.Errorf("read migration %s: %w", name, err)
		}
		checksum := sha256.Sum256(sqlBytes)

		var stored []byte
		err = conn.QueryRow(ctx,
			"SELECT checksum FROM fenced_schema_migrations WHERE version = $1",
			name,
		).Scan(&stored)
		switch {
		case err == nil:
			if !bytes.Equal(stored, checksum[:]) {
				return result, fmt.Errorf("migration %s checksum changed after application", name)
			}
			continue
		case !errors.Is(err, pgx.ErrNoRows):
			return result, fmt.Errorf("read migration ledger for %s: %w", name, err)
		}

		tx, err := conn.BeginTx(ctx, pgx.TxOptions{})
		if err != nil {
			return result, fmt.Errorf("begin migration %s: %w", name, err)
		}
		if _, err = tx.Exec(ctx, string(sqlBytes)); err == nil {
			_, err = tx.Exec(ctx,
				"INSERT INTO fenced_schema_migrations (version, checksum) VALUES ($1, $2)",
				name, checksum[:],
			)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			return result, fmt.Errorf("apply migration %s: %w", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			return result, fmt.Errorf("commit migration %s: %w", name, err)
		}
		result.Applied = append(result.Applied, name)
	}
	return result, nil
}
