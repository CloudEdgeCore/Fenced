// Package domain defines the stable lifecycle vocabulary enforced by the
// Fenced kernel. It has no infrastructure dependencies.
package domain

import "fmt"

type TaskPhase string

const (
	TaskQueued    TaskPhase = "QUEUED"
	TaskAdmitted  TaskPhase = "ADMITTED"
	TaskRunning   TaskPhase = "RUNNING"
	TaskSucceeded TaskPhase = "SUCCEEDED"
	TaskFailed    TaskPhase = "FAILED"
	TaskCancelled TaskPhase = "CANCELLED"
	TaskTimedOut  TaskPhase = "TIMED_OUT"
	TaskRejected  TaskPhase = "REJECTED"
)

type RunPhase string

const (
	RunPending   RunPhase = "PENDING"
	RunRunning   RunPhase = "RUNNING"
	RunCompleted RunPhase = "COMPLETED"
	RunFailed    RunPhase = "FAILED"
	RunCancelled RunPhase = "CANCELLED"
	RunTimedOut  RunPhase = "TIMED_OUT"
)

type AttemptPhase string

const (
	AttemptPending         AttemptPhase = "PENDING"
	AttemptPlaced          AttemptPhase = "PLACED"
	AttemptStarting        AttemptPhase = "STARTING"
	AttemptRunning         AttemptPhase = "RUNNING"
	AttemptWaitingTool     AttemptPhase = "WAITING_TOOL"
	AttemptWaitingAgent    AttemptPhase = "WAITING_AGENT"
	AttemptWaitingApproval AttemptPhase = "WAITING_APPROVAL"
	AttemptCheckpointing   AttemptPhase = "CHECKPOINTING"
	AttemptCompleted       AttemptPhase = "COMPLETED"
	AttemptFailed          AttemptPhase = "ATTEMPT_FAILED"
	AttemptCancelRequested AttemptPhase = "CANCEL_REQUESTED"
	AttemptCancelled       AttemptPhase = "CANCELLED"
)

func (p TaskPhase) Terminal() bool {
	switch p {
	case TaskSucceeded, TaskFailed, TaskCancelled, TaskTimedOut, TaskRejected:
		return true
	default:
		return false
	}
}

func (p RunPhase) Terminal() bool {
	switch p {
	case RunCompleted, RunFailed, RunCancelled, RunTimedOut:
		return true
	default:
		return false
	}
}

func (p AttemptPhase) Terminal() bool {
	switch p {
	case AttemptCompleted, AttemptFailed, AttemptCancelled:
		return true
	default:
		return false
	}
}

func ValidateTaskTransition(from, to TaskPhase) error {
	allowed := map[TaskPhase]map[TaskPhase]struct{}{
		TaskQueued: {
			TaskAdmitted: {}, TaskRejected: {}, TaskCancelled: {}, TaskTimedOut: {},
		},
		TaskAdmitted: {
			TaskRunning: {}, TaskCancelled: {}, TaskTimedOut: {},
		},
		TaskRunning: {
			TaskSucceeded: {}, TaskFailed: {}, TaskCancelled: {}, TaskTimedOut: {},
		},
	}
	_, ok := allowed[from][to]
	return validateTransition("task", string(from), string(to), ok)
}

func ValidateRunTransition(from, to RunPhase) error {
	allowed := map[RunPhase]map[RunPhase]struct{}{
		RunPending: {
			RunRunning: {}, RunFailed: {}, RunCancelled: {}, RunTimedOut: {},
		},
		RunRunning: {
			RunCompleted: {}, RunFailed: {}, RunCancelled: {}, RunTimedOut: {},
		},
	}
	_, ok := allowed[from][to]
	return validateTransition("run", string(from), string(to), ok)
}

func ValidateAttemptTransition(from, to AttemptPhase) error {
	allowed := map[AttemptPhase]map[AttemptPhase]struct{}{
		AttemptPending: {
			AttemptPlaced: {}, AttemptFailed: {}, AttemptCancelled: {},
		},
		AttemptPlaced: {
			AttemptStarting: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptStarting: {
			AttemptRunning: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptRunning: {
			AttemptWaitingTool: {}, AttemptWaitingAgent: {}, AttemptWaitingApproval: {},
			AttemptCheckpointing: {}, AttemptCompleted: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptWaitingTool: {
			AttemptRunning: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptWaitingAgent: {
			AttemptRunning: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptWaitingApproval: {
			AttemptRunning: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptCheckpointing: {
			AttemptRunning: {}, AttemptFailed: {}, AttemptCancelRequested: {},
		},
		AttemptCancelRequested: {
			AttemptCancelled: {}, AttemptFailed: {},
		},
	}
	_, ok := allowed[from][to]
	return validateTransition("attempt", string(from), string(to), ok)
}

func validateTransition(kind, from, to string, allowed bool) error {
	if from == to {
		return fmt.Errorf("%s phase transition %s -> %s is not a transition", kind, from, to)
	}
	if !allowed {
		return fmt.Errorf("invalid %s phase transition %s -> %s", kind, from, to)
	}
	return nil
}
