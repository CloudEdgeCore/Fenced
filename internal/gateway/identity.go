package gateway

import (
	"context"
	"strings"

	"github.com/CloudEdgeCore/Fenced/internal/platform/spiffe"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const peerBoundTenant = "*"

// authorizeTenant accepts a fixed tenant only for isolated development
// endpoints. Production services use "*", which means the request claim must
// exactly match the tenant encoded in the verified peer X.509-SVID.
func authorizeTenant(ctx context.Context, allowedTenant, claimedTenant string) error {
	if strings.TrimSpace(claimedTenant) == "" {
		return status.Error(codes.PermissionDenied, "tenant is not authorized by this gateway endpoint")
	}
	if allowedTenant != peerBoundTenant {
		if claimedTenant != allowedTenant {
			return status.Error(codes.PermissionDenied, "tenant is not authorized by this gateway endpoint")
		}
		return nil
	}
	_, peerTenant, _, err := spiffe.PeerWorkerClaims(ctx)
	if err != nil || peerTenant != claimedTenant {
		return status.Error(codes.PermissionDenied, "tenant claim does not match the verified workload identity")
	}
	return nil
}
