package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestResolveTarget(t *testing.T) {
	cases := []struct {
		name       string
		args       []string
		wantBinary string
		wantRest   []string
	}{
		{"main cli", []string{"task", "submit"}, "fenced", []string{"task", "submit"}},
		{"no args", nil, "fenced", nil},
		{"server subcommand", []string{"control", "--listen", "127.0.0.1:8080"}, "fenced-control", []string{"--listen", "127.0.0.1:8080"}},
		{"unknown leading arg is forwarded", []string{"bogus", "x"}, "fenced", []string{"bogus", "x"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			binary, rest := resolveTarget(tc.args)
			if binary != tc.wantBinary {
				t.Fatalf("binary = %q, want %q", binary, tc.wantBinary)
			}
			if len(rest) != len(tc.wantRest) {
				t.Fatalf("rest = %v, want %v", rest, tc.wantRest)
			}
			for i := range rest {
				if rest[i] != tc.wantRest[i] {
					t.Fatalf("rest = %v, want %v", rest, tc.wantRest)
				}
			}
		})
	}
}

func TestServerSubcommandsExistInCmdTree(t *testing.T) {
	for legacy, binary := range serverSubcommands {
		dir := filepath.Join("..", binary)
		info, err := os.Stat(dir)
		if err != nil || !info.IsDir() {
			t.Fatalf("legacy subcommand %q maps to %q but %s is not a directory", legacy, binary, dir)
		}
		if _, err := os.Stat(filepath.Join(dir, "main.go")); err != nil {
			t.Fatalf("%s has no main.go: %v", dir, err)
		}
	}
}
