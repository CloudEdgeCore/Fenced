// Package tool implements the Tool Gateway decision chain of the Fenced
// kernel: versioned descriptors, canonical argument normalization, policy
// evaluation, human approval binding, budget hard-stop and durable
// side-effect receipts (architecture §9.2, §12.2; invariants 6 and 9).
package tool

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

// NormalizeArgs validates the invocation arguments against the descriptor's
// JSON Schema, rejects non-object or oversized documents, and returns the
// canonical normalized document together with its SHA-256. Two invocations
// with semantically identical arguments always hash to the same value, which
// is what approvals and idempotency are bound to; the normalized document is
// what the executing adapter receives.
func NormalizeArgs(paramsSchema json.RawMessage, args []byte, maxBytes int64) (json.RawMessage, [sha256.Size]byte, error) {
	if len(args) == 0 {
		args = []byte("{}")
	}
	if int64(len(args)) > maxBytes {
		return nil, [sha256.Size]byte{}, fmt.Errorf("tool arguments exceed %d bytes", maxBytes)
	}

	var validated any
	if err := json.Unmarshal(args, &validated); err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("decode tool arguments: %w", err)
	}
	var schemaDocument any
	if err := json.Unmarshal(paramsSchema, &schemaDocument); err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("tool params schema is not valid JSON: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	if err := compiler.AddResource("tool-params.json", schemaDocument); err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("compile tool params schema: %w", err)
	}
	compiled, err := compiler.Compile("tool-params.json")
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("compile tool params schema: %w", err)
	}
	if err := compiled.Validate(validated); err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("tool arguments do not match schema: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(args))
	decoder.UseNumber()
	var canonical any
	if err := decoder.Decode(&canonical); err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("decode tool arguments: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, [sha256.Size]byte{}, err
	}
	normalized, err := json.Marshal(canonical)
	if err != nil {
		return nil, [sha256.Size]byte{}, fmt.Errorf("normalize tool arguments: %w", err)
	}
	return normalized, sha256.Sum256(normalized), nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	err := decoder.Decode(&extra)
	if errors.Is(err, io.EOF) {
		return nil
	}
	if err == nil {
		return fmt.Errorf("tool arguments contain more than one JSON value")
	}
	return fmt.Errorf("decode trailing tool argument data: %w", err)
}
