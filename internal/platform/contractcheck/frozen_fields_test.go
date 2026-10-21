package contractcheck_test

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// The frozen contracts are a promise. A field the implementation ignores is a
// promise the product does not keep, and nothing noticed when the Go address
// dropped agent_version_ref or when auto_rollback was stored and never read.
//
// This check parses the frozen protos, derives the Go name of every field,
// and requires one of:
//
//   - the field is honored: a hand-written type carries it and the code uses it,
//   - it is listed in api/frozen-fields.json as divergent (implemented under
//     another name, which must itself be used) or pending (with a reason).
//
// A type-level mirror that drops a field fails: that is how an address loses
// the version it was addressed with.

const ledgerPath = "../../../api/frozen-fields.json"

// frozenProtos are the contracts the v1.2 freeze pins.
var frozenProtos = []string{
	"../../../proto/fenced/ipc/v1/ipc.proto",
	"../../../proto/fenced/service/v1/service.proto",
	"../../../proto/fenced/syscall/v1/syscall.proto",
	"../../../proto/fenced/effect/v1/effect.proto",
}

// implementationRoots are the hand-written packages. The generated bindings
// are excluded on purpose: they always carry every field, and checking them
// would prove nothing.
var implementationRoots = []string{
	"../../../internal",
	"../../../cmd",
	"../../../sdk/agent",
	"../../../sdk/conformance",
}

type ledger struct {
	// Mirrors maps a frozen message to the hand-written type that mirrors it.
	// Every field of the message must exist in that type.
	Mirrors map[string]string `json:"mirrors"`
	// Fields records the fields that are not honored under their own name.
	Fields map[string]ledgerEntry `json:"fields"`
}

type ledgerEntry struct {
	// Status is "divergent" (implemented under another Go name) or
	// "pending" (not implemented).
	Status string `json:"status"`
	// Go is the Go name that implements a divergent field.
	Go string `json:"go,omitempty"`
	// Reason explains a pending field.
	Reason string `json:"reason,omitempty"`
}

type protoMessage struct {
	name   string // fenced.service.v1.RolloutConfig
	fields map[string]int
}

func TestFrozenFieldsAreHonoredOrListed(t *testing.T) {
	messages := map[string]*protoMessage{}
	for _, path := range frozenProtos {
		if err := parseProto(path, messages); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
	}
	if len(messages) == 0 {
		t.Fatal("no frozen messages were parsed")
	}

	defined, used := parseImplementation(t)
	current := loadLedger(t)

	var problems []string

	// 1. Every field is honored, or listed with a reason.
	names := make([]string, 0, len(messages))
	for name := range messages {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		message := messages[name]
		fields := make([]string, 0, len(message.fields))
		for field := range message.fields {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		for _, field := range fields {
			key := name + "." + field
			if entry, ok := current.Fields[key]; ok {
				switch entry.Status {
				case "divergent":
					if entry.Go == "" {
						problems = append(problems, fmt.Sprintf("%s: divergent without a Go name", key))
						continue
					}
					if !used[normalize(entry.Go)] && !defined[normalize(entry.Go)] {
						problems = append(problems, fmt.Sprintf("%s: claims the Go name %s, which nothing carries", key, entry.Go))
					}
				case "pending":
					if strings.TrimSpace(entry.Reason) == "" {
						problems = append(problems, fmt.Sprintf("%s: pending without a reason", key))
					}
				default:
					problems = append(problems, fmt.Sprintf("%s: unknown status %q", key, entry.Status))
				}
				continue
			}
			goName := goFieldName(field)
			if !used[normalize(goName)] && !defined[normalize(goName)] {
				problems = append(problems, fmt.Sprintf(
					"%s: not honored (no Go field named %s) and not listed in api/frozen-fields.json", key, goName))
			}
		}
	}

	// 2. A mirror must carry every field of the message it mirrors.
	for messageName, typeName := range current.Mirrors {
		message, ok := messages[messageName]
		if !ok {
			problems = append(problems, fmt.Sprintf("mirror of %s: the message does not exist in the frozen protos", messageName))
			continue
		}
		fields, ok := structFields(t, typeName)
		if !ok {
			problems = append(problems, fmt.Sprintf("mirror of %s: the type %s was not found", messageName, typeName))
			continue
		}
		for field := range message.fields {
			key := messageName + "." + field
			entry, listed := current.Fields[key]
			if listed && entry.Status == "pending" {
				// The ledger says the field is not implemented; the mirror
				// dropping it is the acknowledged state, not a new gap.
				continue
			}
			if listed && entry.Status == "divergent" {
				// A divergence may live on another type (the spec keeps the
				// drain timeout, the status keeps the rollout target); it has
				// to exist, and the ledger says where.
				if !fields[normalize(entry.Go)] && !defined[normalize(entry.Go)] {
					problems = append(problems, fmt.Sprintf("%s: the mirror %s does not carry %s", key, typeName, entry.Go))
				}
				continue
			}
			if !fields[normalize(goFieldName(field))] {
				problems = append(problems, fmt.Sprintf(
					"%s: the mirror %s drops the field", key, typeName))
			}
		}
	}

	// 3. The ledger cannot rot: every entry names a field that exists.
	for key := range current.Fields {
		separator := strings.LastIndex(key, ".")
		if separator <= 0 {
			problems = append(problems, fmt.Sprintf("%s: not <message>.<field>", key))
			continue
		}
		message, ok := messages[key[:separator]]
		if !ok {
			problems = append(problems, fmt.Sprintf("%s: the message does not exist in the frozen protos", key))
			continue
		}
		if _, ok := message.fields[key[separator+1:]]; !ok {
			problems = append(problems, fmt.Sprintf("%s: the field does not exist in the frozen protos", key))
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		t.Fatalf("the frozen contracts are not honored:\n  %s", strings.Join(problems, "\n  "))
	}
}

// ---------------------------------------------------------------- protos

var (
	commentLine  = regexp.MustCompile(`//[^\n]*`)
	commentBlock = regexp.MustCompile(`(?s)/\*.*?\*/`)
	messageLine  = regexp.MustCompile(`message\s+(\w+)\s*\{`)
	fieldLine    = regexp.MustCompile(`(?m)^\s*(?:repeated\s+|optional\s+)?[\w.<>, ]+?\s+(\w+)\s*=\s*\d+\s*;`)
)

func parseProto(path string, into map[string]*protoMessage) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	text := commentBlock.ReplaceAllString(commentLine.ReplaceAllString(string(raw), ""), "")
	packageName := protoPackage(text)

	// Walk every message body, including the inline one-line form.
	for _, match := range messageLine.FindAllStringSubmatchIndex(text, -1) {
		name := text[match[2]:match[3]]
		bodyStart := match[1]
		bodyEnd := matchBody(text, bodyStart)
		if bodyEnd < 0 {
			return fmt.Errorf("message %s is not closed", name)
		}
		body := text[bodyStart:bodyEnd]
		message := &protoMessage{name: packageName + "." + name, fields: map[string]int{}}
		for _, field := range fieldLine.FindAllStringSubmatch(body, -1) {
			message.fields[field[1]] = 1
		}
		into[message.name] = message
	}
	return nil
}

func protoPackage(text string) string {
	re := regexp.MustCompile(`(?m)^\s*package\s+([\w.]+)\s*;`)
	if match := re.FindStringSubmatch(text); match != nil {
		return match[1]
	}
	return "unknown"
}

// matchBody returns the index of the brace that closes the body opened at
// start.
func matchBody(text string, start int) int {
	// start is just past the opening brace of the message.
	depth := 1
	for i := start; i < len(text); i++ {
		switch text[i] {
		case '{':
			depth++
		case '}':
			depth--
			if depth == 0 {
				return i
			}
		}
	}
	return -1
}

// ---------------------------------------------------------------- Go code

// parseImplementation collects the struct field names (defined) and the names
// the code actually uses as a field (used).
func parseImplementation(t *testing.T) (defined, used map[string]bool) {
	t.Helper()
	defined, used = map[string]bool{}, map[string]bool{}
	fileSet := token.NewFileSet()
	for _, root := range implementationRoots {
		err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			file, parseErr := parser.ParseFile(fileSet, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			ast.Inspect(file, func(node ast.Node) bool {
				switch typed := node.(type) {
				case *ast.StructType:
					for _, field := range typed.Fields.List {
						for _, name := range field.Names {
							defined[normalize(name.Name)] = true
						}
					}
				case *ast.SelectorExpr:
					used[normalize(typed.Sel.Name)] = true
				case *ast.KeyValueExpr:
					if ident, ok := typed.Key.(*ast.Ident); ok {
						used[normalize(ident.Name)] = true
					}
				}
				return true
			})
			return nil
		})
		if err != nil {
			t.Fatalf("parse %s: %v", root, err)
		}
	}
	if len(defined) == 0 || len(used) == 0 {
		t.Fatal("the implementation was not parsed")
	}
	return defined, used
}

// structFields returns the field names of a hand-written type addressed as
// <import path suffix>.<Type>.
func structFields(t *testing.T, typeName string) (map[string]bool, bool) {
	t.Helper()
	separator := strings.LastIndex(typeName, ".")
	if separator <= 0 {
		return nil, false
	}
	packagePath, name := typeName[:separator], typeName[separator+1:]
	directory := filepath.Join("../../../", packagePath)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return nil, false
	}
	fields := map[string]bool{}
	found := false
	fileSet := token.NewFileSet()
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".go") || strings.HasSuffix(entry.Name(), "_test.go") {
			continue
		}
		file, parseErr := parser.ParseFile(fileSet, filepath.Join(directory, entry.Name()), nil, 0)
		if parseErr != nil {
			return nil, false
		}
		ast.Inspect(file, func(node ast.Node) bool {
			typeSpec, ok := node.(*ast.TypeSpec)
			if !ok || typeSpec.Name.Name != name {
				return true
			}
			structType, ok := typeSpec.Type.(*ast.StructType)
			if !ok {
				return true
			}
			found = true
			for _, field := range structType.Fields.List {
				for _, fieldName := range field.Names {
					fields[normalize(fieldName.Name)] = true
				}
			}
			return false
		})
	}
	return fields, found
}

// goFieldName renders the Go name protoc-gen-go gives a proto field.
func goFieldName(field string) string {
	var builder strings.Builder
	upper := true
	for _, r := range field {
		if r == '_' {
			upper = true
			continue
		}
		if upper {
			builder.WriteRune(toUpper(r))
			upper = false
			continue
		}
		builder.WriteRune(r)
	}
	return builder.String()
}

func toUpper(r rune) rune {
	if r >= 'a' && r <= 'z' {
		return r - ('a' - 'A')
	}
	return r
}

func normalize(name string) string {
	return strings.ToLower(name)
}

// ---------------------------------------------------------------- ledger

func loadLedger(t *testing.T) ledger {
	t.Helper()
	raw, err := os.ReadFile(ledgerPath)
	if err != nil {
		t.Fatalf("read %s: %v", ledgerPath, err)
	}
	var current ledger
	if err := json.Unmarshal(raw, &current); err != nil {
		t.Fatalf("parse %s: %v", ledgerPath, err)
	}
	if current.Fields == nil {
		current.Fields = map[string]ledgerEntry{}
	}
	if current.Mirrors == nil {
		current.Mirrors = map[string]string{}
	}
	return current
}
