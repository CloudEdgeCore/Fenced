package store

import (
	"testing"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/money"
)

func TestSetTenantQuotaInputValid(t *testing.T) {
	valid := SetTenantQuotaInput{
		TenantID: "tenant-a", WindowSeconds: 86400,
		Limits: TaskBudget{Tokens: 1000, CostMicroUSD: money.MustFromUSD(10), ToolCalls: 100, WallSeconds: 3600},
	}
	if !valid.Valid() {
		t.Fatalf("valid quota rejected: %+v", valid)
	}
	cases := []struct {
		name  string
		input SetTenantQuotaInput
	}{
		{"empty tenant", SetTenantQuotaInput{WindowSeconds: 86400, Limits: TaskBudget{Tokens: 1}}},
		{"window too short", SetTenantQuotaInput{TenantID: "tenant-a", WindowSeconds: 59, Limits: TaskBudget{Tokens: 1}}},
		{"zero window", SetTenantQuotaInput{TenantID: "tenant-a", WindowSeconds: 0, Limits: TaskBudget{Tokens: 1}}},
		{"negative token limit", SetTenantQuotaInput{TenantID: "tenant-a", WindowSeconds: 86400, Limits: TaskBudget{Tokens: -1}}},
		{"negative cost limit", SetTenantQuotaInput{TenantID: "tenant-a", WindowSeconds: 86400, Limits: TaskBudget{CostMicroUSD: -1}}},
	}
	for _, tc := range cases {
		if tc.input.Valid() {
			t.Errorf("%s: invalid quota accepted: %+v", tc.name, tc.input)
		}
	}
}

func TestQuotaExceeded(t *testing.T) {
	limits := TaskBudget{Tokens: 100, CostMicroUSD: money.MustFromUSD(1), ToolCalls: 10, WallSeconds: 60}
	if QuotaExceeded(limits, TaskBudget{Tokens: 90}, TaskBudget{Tokens: 9}) {
		t.Fatal("within token limit rejected")
	}
	if !QuotaExceeded(limits, TaskBudget{Tokens: 90}, TaskBudget{Tokens: 11}) {
		t.Fatal("token overshoot not detected")
	}
	if !QuotaExceeded(limits, TaskBudget{Tokens: 90, CostMicroUSD: money.MustFromUSD(0.9)}, TaskBudget{CostMicroUSD: money.MustFromUSD(0.2)}) {
		t.Fatal("cost overshoot not detected")
	}
	if !QuotaExceeded(limits, TaskBudget{Tokens: 101}, TaskBudget{}) {
		t.Fatal("already-over window not detected")
	}
	// A zero limit means the dimension is unlimited.
	unlimited := TaskBudget{Tokens: 0, CostMicroUSD: 0, ToolCalls: 0, WallSeconds: 0}
	if QuotaExceeded(unlimited, TaskBudget{Tokens: 1 << 40}, TaskBudget{Tokens: 1 << 40}) {
		t.Fatal("unlimited quota rejected consumption")
	}
	// A zero additional usage (unbudgeted task) is rejected only when the
	// window is already over.
	if QuotaExceeded(limits, TaskBudget{Tokens: 99}, TaskBudget{}) {
		t.Fatal("below-limit window rejected an unbudgeted task")
	}
	if !QuotaExceeded(limits, TaskBudget{Tokens: 101}, TaskBudget{}) {
		t.Fatal("over-limit window admitted an unbudgeted task")
	}
	// The boundary is strict: at exactly the limit, only positive additional
	// usage is rejected.
	if QuotaExceeded(limits, TaskBudget{Tokens: 100}, TaskBudget{}) {
		t.Fatal("at-limit window rejected zero additional usage")
	}
	if !QuotaExceeded(limits, TaskBudget{Tokens: 100}, TaskBudget{Tokens: 1}) {
		t.Fatal("at-limit window admitted positive additional usage")
	}
}
