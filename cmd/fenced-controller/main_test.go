package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadPoolsRejectsUnknownFieldsAndDuplicateIDs(t *testing.T) {
	tests := []string{
		`[{"id":"pool","tenantIds":["dev"],"runtimeClass":"oci","runtimeInstanceId":"worker","region":"cn","unknown":true}]`,
		`[
			{"id":"pool","tenantIds":["dev"],"runtimeClass":"oci","runtimeInstanceId":"worker-1","region":"cn"},
			{"id":"pool","tenantIds":["dev"],"runtimeClass":"oci","runtimeInstanceId":"worker-2","region":"cn"}
		]`,
	}
	for i, content := range tests {
		path := filepath.Join(t.TempDir(), "pools.json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write test config: %v", err)
		}
		if _, err := loadPools(path); err == nil {
			t.Fatalf("case %d unexpectedly accepted", i)
		}
	}
}
