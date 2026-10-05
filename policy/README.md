# Policy

Shared, versioned Rego policy sources for the Agent OS kernel.

Status:

- v0.1 embeds a single default-deny module set at
  `internal/kernel/policy/fenced.rego`, compiled into the kernel binary with a
  pinned revision (see ADR-008).
- Signed external bundles and multi-service distribution arrive in a later
  Gateway slice; this directory is the canonical home for those versioned
  policy bundles, fixtures and `opa test` suites.
- Admission chains the Go limits engine and the Rego engine; each engine owns
  disjoint checks so no check has two sources of truth.

Do not duplicate policy modules: the embedded module set stays authoritative
until bundle delivery lands.
