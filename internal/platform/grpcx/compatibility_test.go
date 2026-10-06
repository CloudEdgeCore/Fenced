package grpcx_test

import (
	"context"
	"net"
	"testing"

	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	runtimev1alpha1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1alpha1"
	"github.com/CloudEdgeCore/Fenced/internal/platform/grpcx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

type stableRuntimeServer struct {
	runtimev1.UnimplementedRuntimeControlServiceServer
}

func (stableRuntimeServer) PollAssignment(_ context.Context, request *runtimev1.PollAssignmentRequest) (*runtimev1.PollAssignmentResponse, error) {
	return &runtimev1.PollAssignmentResponse{Assignment: &runtimev1.Assignment{
		Identity:          &runtimev1.AttemptIdentity{TenantId: request.GetTenantId(), AttemptId: "wire-compatible", FencingToken: 1},
		RuntimeInstanceId: request.GetRuntimeInstanceId(),
	}}, nil
}

func TestLegacyAlphaClientUsesStableImplementation(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	implementation := stableRuntimeServer{}
	runtimev1.RegisterRuntimeControlServiceServer(server, implementation)
	if err := grpcx.RegisterLegacyServiceAlias(server, runtimev1.RuntimeControlService_ServiceDesc, implementation,
		"fenced.runtime.v1alpha1.RuntimeControlService"); err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	reply, err := runtimev1alpha1.NewRuntimeControlServiceClient(connection).PollAssignment(context.Background(),
		&runtimev1alpha1.PollAssignmentRequest{TenantId: "tenant-a", RuntimeInstanceId: "worker-a"})
	if err != nil || reply.GetAssignment().GetIdentity().GetAttemptId() != "wire-compatible" ||
		reply.GetAssignment().GetRuntimeInstanceId() != "worker-a" {
		t.Fatalf("legacy reply=%+v err=%v", reply, err)
	}
}
