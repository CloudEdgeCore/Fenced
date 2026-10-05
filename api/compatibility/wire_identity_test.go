package compatibility_test

import (
	"strings"
	"testing"

	// Blank imports register every generated descriptor in the global registry
	// so the guard below can walk them. Packages already imported by
	// protobuf_test.go are not repeated here.
	_ "github.com/CloudEdgeCore/Fenced/gen/go/fenced/effect/v1"
	_ "github.com/CloudEdgeCore/Fenced/gen/go/fenced/ipc/v1"
	_ "github.com/CloudEdgeCore/Fenced/gen/go/fenced/service/v1"
	_ "github.com/CloudEdgeCore/Fenced/gen/go/fenced/syscall/v1"

	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

// TestGeneratedDescriptorsCarryNoLegacyWireNames guards the AgentOS → Fenced
// rename at the wire boundary.
//
// Renaming the protobuf package changed every gRPC method name and protobuf
// type URL. A single descriptor left on the legacy package would silently
// produce a client or server that cannot talk to its peer, and the failure
// would only surface at run time. This test walks every registered fenced/*
// descriptor and fails if any still declares a legacy package or service name.
func TestGeneratedDescriptorsCarryNoLegacyWireNames(t *testing.T) {
	files := 0
	services := 0

	protoregistry.GlobalFiles.RangeFiles(func(fd protoreflect.FileDescriptor) bool {
		path := string(fd.Path())
		if !strings.HasPrefix(path, "fenced/") {
			return true
		}
		files++

		pkg := string(fd.Package())
		if !strings.HasPrefix(pkg, "fenced.") {
			t.Errorf("descriptor %s declares package %q, want a fenced.* package", path, pkg)
		}
		if strings.Contains(pkg, "agentos") {
			t.Errorf("descriptor %s still declares the legacy package %q", path, pkg)
		}

		declared := fd.Services()
		for i := 0; i < declared.Len(); i++ {
			full := string(declared.Get(i).FullName())
			services++
			if !strings.HasPrefix(full, "fenced.") || strings.Contains(full, "agentos") {
				t.Errorf("service %s in %s does not use a fenced.* wire name", full, path)
			}
		}
		return true
	})

	if files == 0 {
		t.Fatal("no fenced/* descriptors were registered; the generated packages were not imported")
	}
	if services == 0 {
		t.Fatal("no services were found in the fenced/* descriptors")
	}
}
