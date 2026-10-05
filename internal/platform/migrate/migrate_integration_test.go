//go:build integration

package migrate_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestApplyIsIdempotentAndRejectsChangedHistory(t *testing.T) {
	url := os.Getenv("FENCED_TEST_DATABASE_URL")
	if url == "" {
		t.Skip("FENCED_TEST_DATABASE_URL is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	t.Cleanup(pool.Close)

	const version = "999999_checksum_probe.up.sql"
	const table = "fenced_migration_checksum_probe"
	directory := t.TempDir()
	path := filepath.Join(directory, version)
	if err := os.WriteFile(path, []byte("CREATE TABLE "+table+" (id bigint PRIMARY KEY);\n"), 0o600); err != nil {
		t.Fatalf("write migration: %v", err)
	}
	_, _ = pool.Exec(ctx, "DROP TABLE IF EXISTS "+table)
	_, _ = pool.Exec(ctx, `DELETE FROM fenced_schema_migrations WHERE version = $1`, version)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DROP TABLE IF EXISTS "+table)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fenced_schema_migrations WHERE version = $1`, version)
	})

	first, err := migrate.Apply(ctx, pool, directory)
	if err != nil {
		t.Fatalf("first apply: %v", err)
	}
	if len(first.Applied) != 1 || first.Applied[0] != version {
		t.Fatalf("unexpected first result: %+v", first)
	}
	second, err := migrate.Apply(ctx, pool, directory)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if len(second.Applied) != 0 {
		t.Fatalf("idempotent apply reran migrations: %+v", second)
	}

	if err := os.WriteFile(path, []byte("CREATE TABLE "+table+" (id bigint PRIMARY KEY, changed boolean);\n"), 0o600); err != nil {
		t.Fatalf("rewrite migration: %v", err)
	}
	if _, err := migrate.Apply(ctx, pool, directory); err == nil || !strings.Contains(err.Error(), "checksum changed") {
		t.Fatalf("expected checksum rejection, got %v", err)
	}
}
