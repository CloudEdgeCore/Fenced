package postgres

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/agentversion"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var _ kernelstore.AgentVersionStore = (*Store)(nil)

func (s *Store) CreateAgentVersion(ctx context.Context, in kernelstore.CreateAgentVersionInput) (kernelstore.CreateAgentVersionResult, error) {
	var result kernelstore.CreateAgentVersionResult
	normalized, digest, err := in.ValidateAndHash()
	if err != nil {
		return result, err
	}
	if in.ID == uuid.Nil {
		in.ID = s.newID()
	}
	now := s.now()
	tx, err := s.begin(ctx)
	if err != nil {
		return result, err
	}
	defer rollback(ctx, tx)

	keyID, signature, manifestDigest := packageSignatureFields(in.PackageSignature)
	row := tx.QueryRow(ctx, `
		INSERT INTO agent_versions (
			id, tenant_id, namespace, name, version, spec, spec_digest,
			package_key_id, package_signature, package_manifest_digest,
			resource_version, created_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 1, $11)
		ON CONFLICT (tenant_id, namespace, name, version) DO NOTHING
		RETURNING `+agentVersionColumns,
		in.ID.String(), in.TenantID, in.Namespace, in.Name, in.Version,
		normalized, digest[:], keyID, signature, manifestDigest, now,
	)
	created, scanErr := scanAgentVersion(row)
	if scanErr == nil {
		if err := insertEvent(ctx, tx, created.TenantID, "AgentVersion", created.ID, created.ResourceVersion, "AgentVersionPublished", map[string]any{
			"agentVersionId": created.ID, "name": created.Name, "version": created.Version,
			"namespace": created.Namespace, "specDigest": fmt.Sprintf("%x", created.SpecDigest),
		}, now, s.newID()); err != nil {
			return result, err
		}
		packageDetails := map[string]any{"ref": created.Ref(), "specDigest": fmt.Sprintf("%x", created.SpecDigest)}
		if created.PackageSignature != nil {
			packageDetails["packageKeyId"] = created.PackageSignature.KeyID
			packageDetails["packageManifestDigest"] = created.PackageSignature.ManifestDigest
		}
		if err := auditHook(ctx, tx, created.TenantID, "agent_version.published", "AgentVersion", created.ID, packageDetails, now); err != nil {
			return result, err
		}
		if err := tx.Commit(ctx); err != nil {
			return result, classify(err)
		}
		return kernelstore.CreateAgentVersionResult{AgentVersion: created}, nil
	}
	if !errors.Is(scanErr, pgx.ErrNoRows) {
		return result, classify(scanErr)
	}

	existing, err := scanAgentVersion(tx.QueryRow(ctx, `SELECT `+agentVersionColumns+`
		FROM agent_versions WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4
		FOR UPDATE`, in.TenantID, in.Namespace, in.Name, in.Version))
	if err != nil {
		return result, classify(err)
	}
	if subtle.ConstantTimeCompare(existing.SpecDigest[:], digest[:]) != 1 {
		return result, fmt.Errorf("%w: tenant=%s namespace=%s name=%s version=%s", kernelstore.ErrAgentVersionConflict, in.TenantID, in.Namespace, in.Name, in.Version)
	}
	if err := tx.Commit(ctx); err != nil {
		return result, classify(err)
	}
	return kernelstore.CreateAgentVersionResult{AgentVersion: existing, Existing: true}, nil
}

func (s *Store) GetAgentVersion(ctx context.Context, tenantID string, id uuid.UUID) (kernelstore.AgentVersion, error) {
	if strings.TrimSpace(tenantID) == "" || id == uuid.Nil {
		return kernelstore.AgentVersion{}, kernelstore.ErrNotFound
	}
	version, err := scanAgentVersion(s.pool.QueryRow(ctx, `SELECT `+agentVersionColumns+`
		FROM agent_versions WHERE tenant_id = $1 AND id = $2`, tenantID, id.String()))
	return version, classify(err)
}

func (s *Store) GetAgentVersionByRef(ctx context.Context, tenantID, ref string) (kernelstore.AgentVersion, error) {
	namespace, name, version, err := agentversion.ParseRef(ref)
	if err != nil {
		return kernelstore.AgentVersion{}, fmt.Errorf("%w: %v", kernelstore.ErrAgentVersionRefInvalid, err)
	}
	if strings.TrimSpace(tenantID) == "" {
		return kernelstore.AgentVersion{}, kernelstore.ErrNotFound
	}
	versionRow, err := scanAgentVersion(s.pool.QueryRow(ctx, `SELECT `+agentVersionColumns+`
		FROM agent_versions WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4`,
		tenantID, namespace, name, version))
	return versionRow, classify(err)
}

// resolveAgentVersionID resolves a canonical reference inside a transaction
// and returns nil when the referenced publication does not exist.
func resolveAgentVersionID(ctx context.Context, tx pgx.Tx, tenantID, ref string) (*uuid.UUID, error) {
	namespace, name, version, err := agentversion.ParseRef(ref)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", kernelstore.ErrAgentVersionRefInvalid, err)
	}
	var id string
	err = tx.QueryRow(ctx, `SELECT id::text FROM agent_versions
		WHERE tenant_id = $1 AND namespace = $2 AND name = $3 AND version = $4`, tenantID, namespace, name, version).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, classify(err)
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("parse agent version id: %w", err)
	}
	return &parsed, nil
}

const agentVersionColumns = `
	id::text, tenant_id, namespace, name, version, spec, spec_digest,
	package_key_id, package_signature, package_manifest_digest,
	resource_version, created_at`

// packageSignatureFields renders the atomic signature envelope (all fields or
// none, enforced by the agent_versions_package_signature_shape check).
func packageSignatureFields(signature *kernelstore.PackageSignature) (any, any, any) {
	if signature == nil {
		return nil, nil, nil
	}
	return nullableString(signature.KeyID), nullableString(signature.Signature), nullableString(signature.ManifestDigest)
}

func scanAgentVersion(row scanner) (kernelstore.AgentVersion, error) {
	var version kernelstore.AgentVersion
	var id string
	var digest []byte
	var packageKeyID, packageSignature, packageManifestDigest *string
	if err := row.Scan(&id, &version.TenantID, &version.Namespace, &version.Name, &version.Version,
		&version.Spec, &digest, &packageKeyID, &packageSignature, &packageManifestDigest,
		&version.ResourceVersion, &version.CreatedAt); err != nil {
		return version, err
	}
	parsed, err := uuid.Parse(id)
	if err != nil {
		return version, fmt.Errorf("parse agent version id: %w", err)
	}
	version.ID = parsed
	if len(digest) != len(version.SpecDigest) {
		return version, fmt.Errorf("agent version spec digest has length %d", len(digest))
	}
	copy(version.SpecDigest[:], digest)
	if packageKeyID != nil {
		version.PackageSignature = &kernelstore.PackageSignature{
			KeyID: *packageKeyID, Signature: *packageSignature, ManifestDigest: *packageManifestDigest,
		}
	}
	return version, nil
}
