package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/CloudEdgeCore/Fenced/internal/platform/compat"
	"github.com/CloudEdgeCore/Fenced/internal/platform/migrate"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	compat.WarnLegacyEnv(os.Stderr, compat.AliasLegacyEnv())
	databaseURL := flag.String("database-url", os.Getenv("DATABASE_URL"), "PostgreSQL connection URL (or DATABASE_URL)")
	directory := flag.String("directory", "db/migrations", "migration directory")
	flag.Parse()

	if *databaseURL == "" {
		slog.Error("database URL is required")
		os.Exit(2)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, *databaseURL)
	if err != nil {
		slog.Error("create PostgreSQL pool", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	result, err := migrate.Apply(ctx, pool, *directory)
	if err != nil {
		slog.Error("apply migrations", "error", err)
		os.Exit(1)
	}
	for _, version := range result.Applied {
		fmt.Printf("applied %s\n", version)
	}
	if len(result.Applied) == 0 {
		fmt.Println("schema is current")
	}
}
