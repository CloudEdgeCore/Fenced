package compatibility_test

import (
	"testing"

	gatewayv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1"
	gatewayv1alpha1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/gateway/v1alpha1"
	modelv1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1"
	modelv1alpha1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/model/v1alpha1"
	runtimev1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1"
	runtimev1alpha1 "github.com/CloudEdgeCore/Fenced/gen/go/fenced/runtime/v1alpha1"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func TestPromotedProtobufsRemainAlphaWireCompatible(t *testing.T) {
	assertWireCompatible(t, runtimev1.File_fenced_runtime_v1_runtime_proto, runtimev1alpha1.File_fenced_runtime_v1alpha1_runtime_proto)
	assertWireCompatible(t, gatewayv1.File_fenced_gateway_v1_gateway_proto, gatewayv1alpha1.File_fenced_gateway_v1alpha1_gateway_proto)
	assertWireCompatible(t, modelv1.File_fenced_model_v1_model_proto, modelv1alpha1.File_fenced_model_v1alpha1_model_proto)
}

func assertWireCompatible(t *testing.T, stable, legacy protoreflect.FileDescriptor) {
	t.Helper()
	if stable.Messages().Len() < legacy.Messages().Len() || stable.Services().Len() < legacy.Services().Len() || stable.Enums().Len() < legacy.Enums().Len() {
		t.Fatalf("descriptor shape drift: stable=%s legacy=%s", stable.Path(), legacy.Path())
	}
	// Stable contracts may add new field numbers without breaking legacy wire
	// readers. Every legacy shape must remain present and unchanged.
	for index := 0; index < legacy.Messages().Len(); index++ {
		previous := legacy.Messages().Get(index)
		current := stable.Messages().ByName(previous.Name())
		if current == nil || current.Fields().Len() < previous.Fields().Len() {
			t.Fatalf("message %s shape drift", previous.Name())
		}
		for fieldIndex := 0; fieldIndex < previous.Fields().Len(); fieldIndex++ {
			old := previous.Fields().Get(fieldIndex)
			field := current.Fields().ByNumber(old.Number())
			if field == nil || field.Name() != old.Name() || field.Kind() != old.Kind() || field.Cardinality() != old.Cardinality() {
				t.Fatalf("message %s field %d wire drift", previous.Name(), old.Number())
			}
		}
	}
	for index := 0; index < legacy.Services().Len(); index++ {
		old := legacy.Services().Get(index)
		service := stable.Services().ByName(old.Name())
		if service == nil || service.Methods().Len() < old.Methods().Len() {
			t.Fatalf("service %s drift", old.Name())
		}
		for methodIndex := 0; methodIndex < old.Methods().Len(); methodIndex++ {
			previous := old.Methods().Get(methodIndex)
			method := service.Methods().ByName(previous.Name())
			if method == nil || method.Input().Name() != previous.Input().Name() || method.Output().Name() != previous.Output().Name() {
				t.Fatalf("service %s method %s drift", old.Name(), previous.Name())
			}
		}
	}
	for index := 0; index < legacy.Enums().Len(); index++ {
		old := legacy.Enums().Get(index)
		enum := stable.Enums().ByName(old.Name())
		if enum == nil || enum.Values().Len() < old.Values().Len() {
			t.Fatalf("enum %s drift", old.Name())
		}
		for valueIndex := 0; valueIndex < old.Values().Len(); valueIndex++ {
			previous := old.Values().Get(valueIndex)
			value := enum.Values().ByNumber(previous.Number())
			if value == nil || value.Name() != previous.Name() {
				t.Fatalf("enum %s value %d drift", old.Name(), previous.Number())
			}
		}
	}
}
