package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
)

// AuditSchema identifies the canonical audit event encoding.
const AuditSchema = "fenced.audit/v1"

// AuditGenesisHash anchors the first event of every tenant's chain: the
// first event's PrevHash equals this constant, so a truncated chain (events
// deleted from the head) is detectable.
var AuditGenesisHash = sha256.Sum256([]byte("fenced-audit-genesis-v1"))

// ErrAuditChainBroken reports a hash-chain integrity failure at a known seq.
var ErrAuditChainBroken = errors.New("audit chain integrity check failed")

// AuditEvent is one transactional append-only audit record.
type AuditEvent struct {
	ID           uuid.UUID
	TenantID     string
	Seq          int64
	PrevHash     [sha256.Size]byte
	ChainHash    [sha256.Size]byte
	EventType    string
	ResourceType string
	ResourceID   uuid.UUID
	Actor        string
	Details      json.RawMessage
	OccurredAt   time.Time
}

// ComputeChainHash returns the canonical chain hash of the event: the value
// the next event's PrevHash must equal. The encoding is length-prefixed so
// the concatenation is unambiguous regardless of field content.
func (e AuditEvent) ComputeChainHash() [sha256.Size]byte {
	hasher := sha256.New()
	writeField(hasher, e.ID[:])
	writeInt64(hasher, e.Seq)
	writeField(hasher, e.PrevHash[:])
	writeField(hasher, []byte(e.EventType))
	writeField(hasher, []byte(e.ResourceType))
	writeField(hasher, e.ResourceID[:])
	writeField(hasher, []byte(e.Actor))
	writeField(hasher, canonicalDetails(e.Details))
	writeField(hasher, []byte(e.OccurredAt.UTC().Format(time.RFC3339Nano)))
	var sum [sha256.Size]byte
	copy(sum[:], hasher.Sum(nil))
	return sum
}

// ListAuditInput pages a tenant's ledger forward by seq.
type ListAuditInput struct {
	TenantID string
	// AfterSeq returns events with seq > AfterSeq (cursor semantics).
	AfterSeq int64
	Limit    int
}

// AuditVerification is the result of a full chain integrity walk.
type AuditVerification struct {
	Valid          bool
	Checked        int64
	FirstBrokenSeq int64 // 0 when the chain is valid
	HeadSeq        int64
	HeadHash       [sha256.Size]byte
}

// AuditStore is the read surface of the audit ledger.
type AuditStore interface {
	ListAudit(context.Context, ListAuditInput) ([]AuditEvent, error)
	GetAuditEvent(context.Context, string, uuid.UUID) (AuditEvent, error)
	VerifyAuditChain(context.Context, string) (AuditVerification, error)
	ExportAuditChain(context.Context, string) ([]AuditEvent, error)
}

// canonicalDetails normalizes the details document for hashing. PostgreSQL
// jsonb orders object keys by length and Go's encoder by byte order, and the
// two differ in whitespace, so the document is decoded (with numbers kept
// verbatim) and re-marshaled: both sources converge on Go's canonical
// encoding.
func canonicalDetails(details json.RawMessage) []byte {
	if len(details) == 0 || string(details) == "null" {
		return []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(details))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return details
	}
	normalized, err := json.Marshal(value)
	if err != nil {
		return details
	}
	return normalized
}

func writeField(hasher interface{ Write([]byte) (int, error) }, field []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(field)))
	_, _ = hasher.Write(length[:])
	_, _ = hasher.Write(field)
}

func writeInt64(hasher interface{ Write([]byte) (int, error) }, value int64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], uint64(value))
	_, _ = hasher.Write(encoded[:])
}
