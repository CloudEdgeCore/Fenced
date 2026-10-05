// Package slo evaluates immutable v1 GA service objectives from a measured
// observation window. It deliberately does not collect metrics; production
// collectors export the Sample document so certification stays reproducible.
package slo

import "fmt"

const Schema = "fenced.slo/v1"

type Sample struct {
	Schema                         string  `json:"schema"`
	WindowSeconds                  int64   `json:"windowSeconds"`
	ControlRequests                int64   `json:"controlRequests"`
	ControlFailures                int64   `json:"controlFailures"`
	AcceptedTasks                  int64   `json:"acceptedTasks"`
	PersistentlyLostTasks          int64   `json:"persistentlyLostTasks"`
	SchedulingMeasurements         int64   `json:"schedulingMeasurements"`
	SchedulingDecisionP99Seconds   float64 `json:"schedulingDecisionP99Seconds"`
	RecoveryMeasurements           int64   `json:"recoveryMeasurements"`
	RuntimeRecoveryP99Seconds      float64 `json:"runtimeRecoveryP99Seconds"`
	BudgetStopMeasurements         int64   `json:"budgetStopMeasurements"`
	BudgetHardStopP99Seconds       float64 `json:"budgetHardStopP99Seconds"`
	ExpectedAuditEvents            int64   `json:"expectedAuditEvents"`
	RecordedAuditEvents            int64   `json:"recordedAuditEvents"`
	TenantStateRPOSeconds          float64 `json:"tenantStateRpoSeconds"`
	ControlPlaneRTOSeconds         float64 `json:"controlPlaneRtoSeconds"`
	UnauthorizedHighRiskExecutions int64   `json:"unauthorizedHighRiskExecutions"`
}

type Check struct {
	Name     string  `json:"name"`
	Measured float64 `json:"measured"`
	Target   string  `json:"target"`
	Passed   bool    `json:"passed"`
	Evidence int64   `json:"evidence"`
}

type Report struct {
	Schema string  `json:"schema"`
	Passed bool    `json:"passed"`
	Checks []Check `json:"checks"`
}

func Evaluate(sample Sample) (Report, error) {
	if sample.Schema != Schema || sample.WindowSeconds <= 0 || sample.ControlRequests <= 0 || sample.AcceptedTasks <= 0 ||
		sample.SchedulingMeasurements <= 0 || sample.RecoveryMeasurements <= 0 || sample.BudgetStopMeasurements <= 0 ||
		sample.ExpectedAuditEvents <= 0 {
		return Report{}, fmt.Errorf("SLO sample has insufficient or invalid evidence")
	}
	if sample.ControlFailures < 0 || sample.ControlFailures > sample.ControlRequests || sample.PersistentlyLostTasks < 0 ||
		sample.PersistentlyLostTasks > sample.AcceptedTasks || sample.RecordedAuditEvents < 0 || sample.RecordedAuditEvents > sample.ExpectedAuditEvents {
		return Report{}, fmt.Errorf("SLO sample counters are inconsistent")
	}
	availability := 1 - float64(sample.ControlFailures)/float64(sample.ControlRequests)
	auditCompleteness := float64(sample.RecordedAuditEvents) / float64(sample.ExpectedAuditEvents)
	checks := []Check{
		{Name: "controlApiAvailability", Measured: availability, Target: ">=0.999", Passed: availability >= 0.999, Evidence: sample.ControlRequests},
		{Name: "acceptedTaskPersistentLoss", Measured: float64(sample.PersistentlyLostTasks), Target: "=0", Passed: sample.PersistentlyLostTasks == 0, Evidence: sample.AcceptedTasks},
		{Name: "schedulingDecisionP99Seconds", Measured: sample.SchedulingDecisionP99Seconds, Target: "<2", Passed: sample.SchedulingDecisionP99Seconds < 2, Evidence: sample.SchedulingMeasurements},
		{Name: "runtimeRecoveryP99Seconds", Measured: sample.RuntimeRecoveryP99Seconds, Target: "<60", Passed: sample.RuntimeRecoveryP99Seconds < 60, Evidence: sample.RecoveryMeasurements},
		{Name: "budgetHardStopP99Seconds", Measured: sample.BudgetHardStopP99Seconds, Target: "<5", Passed: sample.BudgetHardStopP99Seconds < 5, Evidence: sample.BudgetStopMeasurements},
		{Name: "auditEventCompleteness", Measured: auditCompleteness, Target: "=1", Passed: auditCompleteness == 1, Evidence: sample.ExpectedAuditEvents},
		{Name: "tenantStateRPOSeconds", Measured: sample.TenantStateRPOSeconds, Target: "=0", Passed: sample.TenantStateRPOSeconds == 0, Evidence: sample.WindowSeconds},
		{Name: "controlPlaneRTOSeconds", Measured: sample.ControlPlaneRTOSeconds, Target: "<900", Passed: sample.ControlPlaneRTOSeconds < 900, Evidence: sample.WindowSeconds},
		{Name: "unauthorizedHighRiskExecutions", Measured: float64(sample.UnauthorizedHighRiskExecutions), Target: "=0", Passed: sample.UnauthorizedHighRiskExecutions == 0, Evidence: sample.AcceptedTasks},
	}
	report := Report{Schema: Schema, Passed: true, Checks: checks}
	for _, check := range checks {
		if !check.Passed {
			report.Passed = false
		}
	}
	return report, nil
}
