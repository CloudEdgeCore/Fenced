// Package security is the v0.3 negative test suite: it proves that the four
// security boundaries hold under attack, not just under happy paths:
//
//  1. Secret leak — a scoped credential handle must never reach the Agent,
//     the result, or the receipt, even when the executing tool echoes it.
//  2. Approval binding — a human approval for one invocation must not
//     authorize a different tool, resource, argument set, or attempt.
//  3. Tenant escape — a request claiming another tenant must be rejected
//     before any store access on every fenced boundary.
//  4. Fencing replay — a replayed stale fencing token must be rejected after
//     the attempt's ownership moved on.
//
// This package contains no production code. It exists so the negative suite
// is a first-class citizen of the repository instead of an afterthought
// hidden inside the packages it attacks: the boundaries are only as strong
// as the tests that hold them, and a suite with its own package is easy to
// run, review and extend.
//
// The production controls these tests exercise live elsewhere:
//
//   - Capability grants and the publish-time tool freeze:
//     internal/kernel/capability
//   - Admission, tool, model and peer policy (Rego, default deny):
//     internal/kernel/policy
//   - Operator identity (OIDC) and machine identity (X.509-SVID, mTLS):
//     internal/control/auth and internal/platform/spiffe, internal/platform/grpcx
//   - Tenant binding at the agent-facing gateways: internal/gateway
//   - Fencing predicates, the hash-chained audit ledger and the budget
//     ledger: internal/kernel/store
//   - Secret and credential redaction: internal/platform/redact
//
// The unit leg runs without a database. The integration leg (real
// PostgreSQL, real takeover sequence) lives in negative_integration_test.go
// and is guarded by the "integration" build tag.
package security
