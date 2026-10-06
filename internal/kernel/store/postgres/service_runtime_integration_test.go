//go:build integration

package postgres_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/domain"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/ipc"
	kernelstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store"
	postgresstore "github.com/CloudEdgeCore/Fenced/internal/kernel/store/postgres"
	"github.com/CloudEdgeCore/Fenced/internal/kernel/supervisor"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestServiceRuntimeMigrationResetsLegacyReadiness(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx := context.Background()
	svc := runtimeService("tenant-a", "service-legacy-runtime")
	svc.Status.Phase = supervisor.ServiceActive
	svc.Status.ReadyReplicas = 2
	svc.Status.AvailableReplicas = 2
	if err := repository.CreateService(ctx, svc); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	apply := func(direction string) {
		t.Helper()
		sql, err := os.ReadFile(filepath.Join("..", "..", "..", "..", "db", "migrations", "000037_service_runtime."+direction+".sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, string(sql)); err != nil {
			t.Fatalf("apply service runtime %s migration: %v", direction, err)
		}
	}
	apply("down")
	for _, phase := range []string{"Running", "Draining"} {
		if _, err := tx.Exec(ctx, `INSERT INTO agent_service_instances
			(id, service_id, tenant_id, namespace, agent_id, address, phase, last_heartbeat, next_restart_at)
			VALUES ($1, $2, $3, $4, $5, '{}', $6, $7, $7)`,
			"legacy-"+phase, svc.ID, svc.TenantID, svc.Namespace, svc.AgentID, phase, clock.Now()); err != nil {
			t.Fatal(err)
		}
	}
	apply("up")
	var phase string
	var heartbeat time.Time
	var next *time.Time
	if err := tx.QueryRow(ctx, `SELECT phase, last_heartbeat, next_restart_at FROM agent_service_instances WHERE id = 'legacy-Running'`).Scan(&phase, &heartbeat, &next); err != nil {
		t.Fatal(err)
	}
	if phase != "Starting" || !heartbeat.IsZero() || next != nil {
		t.Fatalf("legacy readiness survived migration: phase=%s heartbeat=%v next=%v", phase, heartbeat, next)
	}
	var reason string
	var terminated *time.Time
	if err := tx.QueryRow(ctx, `SELECT phase, exit_reason, terminated_at FROM agent_service_instances WHERE id = 'legacy-Draining'`).Scan(&phase, &reason, &terminated); err != nil {
		t.Fatal(err)
	}
	if phase != "Stopped" || reason != "drained" || terminated == nil {
		t.Fatalf("legacy drain remained stuck: phase=%s reason=%s terminated=%v", phase, reason, terminated)
	}
	var status []byte
	if err := tx.QueryRow(ctx, `SELECT status FROM agent_services WHERE id = $1`, svc.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	var observed supervisor.ServiceStatus
	if err := json.Unmarshal(status, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Phase != supervisor.ServiceDegraded || observed.ReadyReplicas != 0 || observed.AvailableReplicas != 0 {
		t.Fatalf("legacy service still advertises readiness: %+v", observed)
	}
}

func runtimeService(tenant, id string) *supervisor.Service {
	return &supervisor.Service{
		ID: id, TenantID: tenant, Namespace: "default", Name: id, AgentID: "agent",
		Spec:   supervisor.ServiceSpec{Replicas: 1, AgentVersionRef: "agent@1", RestartPolicy: supervisor.RestartNever},
		Status: supervisor.ServiceStatus{Phase: supervisor.ServicePending},
	}
}

func TestServiceRuntimeFieldsPersist(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	svc := runtimeService("tenant-a", "service-runtime-fields")
	if err := repository.CreateService(ctx, svc); err != nil {
		t.Fatal(err)
	}
	created, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		TenantID: svc.TenantID, Namespace: svc.Namespace, AgentVersionRef: "agent@1",
		Goal: "service generation", Spec: []byte(`{"generation":1}`), IdempotencyKey: "service-runtime-fields",
	})
	if err != nil {
		t.Fatal(err)
	}
	draining := clock.Now()
	deadline := draining.Add(time.Minute)
	inst := &supervisor.Instance{
		ID: "instance-runtime-fields", ServiceID: svc.ID, TenantID: svc.TenantID,
		Namespace: svc.Namespace, AgentID: svc.AgentID,
		Address: ipc.NewInstanceAddress(svc.TenantID, svc.Namespace, svc.AgentID, "instance-runtime-fields"),
		Phase:   supervisor.InstanceDraining, AgentVersion: "agent@1", RuntimeClass: "reference",
		FencingToken: 9, TaskID: &created.Task.ID, LaunchSpec: created.Task.Spec,
		DrainingAt: &draining, DrainDeadline: &deadline, RestartCount: 3,
	}
	if err := repository.CreateInstance(ctx, inst); err != nil {
		t.Fatal(err)
	}
	assertFields := func(got *supervisor.Instance) {
		t.Helper()
		if got.AgentVersion != inst.AgentVersion || got.RuntimeClass != inst.RuntimeClass ||
			got.FencingToken != inst.FencingToken || got.TaskID == nil || *got.TaskID != *inst.TaskID ||
			got.RestartCount != inst.RestartCount || got.DrainingAt == nil || !got.DrainingAt.Equal(draining) ||
			got.DrainDeadline == nil || !got.DrainDeadline.Equal(deadline) {
			t.Fatalf("execution fields did not round trip: %+v", got)
		}
		var spec map[string]int
		if err := json.Unmarshal(got.LaunchSpec, &spec); err != nil || spec["generation"] != 1 {
			t.Fatalf("frozen launch spec = %s, err=%v", got.LaunchSpec, err)
		}
	}
	got, err := repository.GetInstance(ctx, svc.TenantID, svc.ID, inst.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertFields(got)
	listed, err := repository.ListInstances(ctx, svc.TenantID, svc.ID)
	if err != nil || len(listed) != 1 {
		t.Fatalf("list instances: len=%d err=%v", len(listed), err)
	}
	assertFields(listed[0])
	all, err := repository.ListAllInstances(ctx, svc.TenantID)
	if err != nil || len(all) != 1 {
		t.Fatalf("list tenant instances: len=%d err=%v", len(all), err)
	}
	assertFields(all[0])
	got.AgentVersion, got.RuntimeClass, got.FencingToken = "agent@2", "oci", 10
	got.TaskID, got.LaunchSpec, got.DrainingAt, got.DrainDeadline = nil, nil, nil, nil
	if err := repository.UpdateInstance(ctx, got); err != nil {
		t.Fatal(err)
	}
	updated, err := repository.GetInstance(ctx, svc.TenantID, svc.ID, inst.ID)
	if err != nil || updated.AgentVersion != "agent@2" || updated.RuntimeClass != "oci" ||
		updated.FencingToken != 10 || updated.TaskID != nil || len(updated.LaunchSpec) != 0 ||
		updated.DrainingAt != nil || updated.DrainDeadline != nil {
		t.Fatalf("updated nullable fields: %+v err=%v", updated, err)
	}
	other := *inst
	other.ID, other.TenantID = "cross-tenant-task-link", "tenant-b"
	if err := repository.CreateInstance(ctx, &other); err == nil {
		t.Fatal("cross-tenant task linkage was accepted")
	}
}

func TestServiceTaskExecutionTracksCurrentOwnerAndTerminalTask(t *testing.T) {
	clock := newFakeClock()
	_, repository := prepare(t, clock.Now)
	ctx := context.Background()
	queued, err := repository.CreateTask(ctx, kernelstore.CreateTaskInput{
		TenantID: "tenant-a", Namespace: "default", AgentVersionRef: "agent@1", Goal: "queued service",
		Spec: []byte(`{}`), IdempotencyKey: "queued-service",
	})
	if err != nil {
		t.Fatal(err)
	}
	state, err := repository.GetServiceTaskExecution(ctx, "tenant-a", queued.Task.ID)
	if err != nil || state.Task.ID != queued.Task.ID || state.Attempt != nil || state.Lease != nil {
		t.Fatalf("queued observation: %+v err=%v", state, err)
	}
	if _, err := repository.GetServiceTaskExecution(ctx, "tenant-b", queued.Task.ID); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("cross-tenant observation: %v", err)
	}
	if _, err := repository.RequestTaskCancellation(ctx, "tenant-a", queued.Task.ID, queued.Task.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	assignment := scheduleRuntimeTask(t, ctx, repository, "service-owner-observation", 2)
	state, err = repository.GetServiceTaskExecution(ctx, "tenant-a", assignment.Task.ID)
	if err != nil || state.Attempt == nil || state.Lease == nil || state.Attempt.ID != assignment.Attempt.ID ||
		state.Attempt.FencingToken != 1 || state.Lease.FencingToken != state.Attempt.FencingToken ||
		state.Attempt.Phase != domain.AttemptPlaced {
		t.Fatalf("placed observation: %+v err=%v", state, err)
	}
	clock.Advance(31 * time.Second)
	recovered, err := repository.RecoverExpiredAttempt(ctx, kernelstore.RecoverExpiredAttemptInput{
		TenantID: "tenant-a", AttemptID: assignment.Attempt.ID, FencingToken: 1,
		NewAttemptID: uuid.New(), NewLeaseID: uuid.New(), LeaseTTL: time.Minute, MaxAttempts: 2,
	})
	if err != nil || !recovered.Retried {
		t.Fatalf("recover owner: %+v err=%v", recovered, err)
	}
	state, err = repository.GetServiceTaskExecution(ctx, "tenant-a", assignment.Task.ID)
	if err != nil || state.Attempt == nil || state.Lease == nil || state.Attempt.ID != recovered.Lease.Attempt.ID ||
		state.Attempt.FencingToken != 2 || state.Lease.FencingToken != 2 || !state.Lease.HeartbeatAt.Equal(clock.Now()) {
		t.Fatalf("recovered observation: %+v err=%v", state, err)
	}
	starting, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: state.Attempt.ID, FencingToken: 2,
		ExpectedAttemptVersion: state.Attempt.ResourceVersion, To: domain.AttemptStarting,
	})
	if err != nil {
		t.Fatal(err)
	}
	running, err := repository.TransitionAttempt(ctx, kernelstore.TransitionAttemptInput{
		TenantID: "tenant-a", AttemptID: starting.ID, FencingToken: 2,
		ExpectedAttemptVersion: starting.ResourceVersion, To: domain.AttemptRunning,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.CompleteAttempt(ctx, kernelstore.CompleteAttemptInput{
		TenantID: "tenant-a", AttemptID: running.ID, FencingToken: 2,
		ExpectedAttemptVersion: running.ResourceVersion, IdempotencyKey: "complete-service-owner",
		Result: artifactReference("artifact://tenant-a/service-owner-result", "service result"),
	}); err != nil {
		t.Fatal(err)
	}
	state, err = repository.GetServiceTaskExecution(ctx, "tenant-a", assignment.Task.ID)
	if err != nil || state.Task.Phase != domain.TaskSucceeded || state.Lease != nil {
		t.Fatalf("terminal task must remain observable without a lease: %+v err=%v", state, err)
	}
}

func TestServiceLockSerializesIndependentStoresAndEnumeratesTenants(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	svc := runtimeService("tenant-a", "serialized-service")
	svc.Spec.Replicas = 0
	if err := repository.CreateService(ctx, svc); err != nil {
		t.Fatal(err)
	}
	if err := repository.CreateService(ctx, runtimeService("tenant-b", "second-tenant-service")); err != nil {
		t.Fatal(err)
	}
	stores := []*postgresstore.Store{repository, postgresstore.NewWithClock(pool, clock.Now)}
	var group sync.WaitGroup
	errs := make(chan error, len(stores))
	for _, store := range stores {
		group.Add(1)
		go func(store *postgresstore.Store) {
			defer group.Done()
			for range 15 {
				if err := store.WithServiceLock(ctx, svc.TenantID, svc.ID, func(scoped supervisor.Store) error {
					current, err := scoped.GetService(ctx, svc.TenantID, svc.ID)
					if err != nil {
						return err
					}
					current.Spec.Replicas++
					return scoped.UpdateService(ctx, current)
				}); err != nil {
					errs <- err
					return
				}
			}
		}(store)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	got, err := repository.GetService(ctx, svc.TenantID, svc.ID)
	if err != nil || got.Spec.Replicas != 30 {
		t.Fatalf("concurrent mutations lost an update: %+v err=%v", got, err)
	}
	all, err := repository.ListAllServices(ctx)
	if err != nil || len(all) != 2 {
		t.Fatalf("controller enumeration: len=%d err=%v", len(all), err)
	}
	emptyTenant, err := repository.ListServices(ctx, "", "")
	if err != nil || len(emptyTenant) != 0 {
		t.Fatalf("empty tenant must not enumerate other tenants: len=%d err=%v", len(emptyTenant), err)
	}
}

func TestServiceLockNestedTaskOperationsUseOneConnectionAndRollback(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	svc := runtimeService("tenant-a", "single-connection-service")
	if err := repository.CreateService(ctx, svc); err != nil {
		t.Fatal(err)
	}
	config := pool.Config()
	config.MaxConns = 1
	single, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatal(err)
	}
	defer single.Close()
	repository = postgresstore.NewWithClock(single, clock.Now)
	taskID := uuid.New()
	input := kernelstore.CreateTaskInput{
		ID: taskID, TenantID: svc.TenantID, Namespace: svc.Namespace, AgentVersionRef: "agent@1",
		Goal: "single connection service task", Spec: []byte(`{}`), IdempotencyKey: "single-connection-service-task",
	}
	if err := repository.WithServiceLock(ctx, svc.TenantID, svc.ID, func(scoped supervisor.Store) error {
		// Persist the prepared task ID first: the FK is deferred to this transaction's commit.
		if err := scoped.CreateInstance(ctx, &supervisor.Instance{
			ID: "single-instance", ServiceID: svc.ID, TenantID: svc.TenantID,
			Namespace: svc.Namespace, AgentID: svc.AgentID, TaskID: &taskID, LaunchSpec: input.Spec,
			Address: ipc.NewInstanceAddress(svc.TenantID, svc.Namespace, svc.AgentID, "single-instance"),
			Phase:   supervisor.InstanceStarting,
		}); err != nil {
			return err
		}
		tasks := scoped.(*postgresstore.Store)
		created, err := tasks.CreateTask(ctx, input)
		if err != nil {
			return err
		}
		_, err = tasks.RequestTaskCancellation(ctx, svc.TenantID, created.Task.ID, created.Task.ResourceVersion)
		return err
	}); err != nil {
		t.Fatalf("nested task lifecycle on a single connection: %v", err)
	}
	state, err := repository.GetServiceTaskExecution(ctx, svc.TenantID, taskID)
	if err != nil || state.Task.Phase != domain.TaskCancelled {
		t.Fatalf("nested task cancellation: %+v err=%v", state, err)
	}
	rollbackTaskID := uuid.New()
	rollbackError := errors.New("reject reconciliation")
	err = repository.WithServiceLock(ctx, svc.TenantID, svc.ID, func(scoped supervisor.Store) error {
		tasks := scoped.(*postgresstore.Store)
		rollbackInput := input
		rollbackInput.ID, rollbackInput.IdempotencyKey = rollbackTaskID, "rollback-service-task"
		if _, err := tasks.CreateTask(ctx, rollbackInput); err != nil {
			return err
		}
		current, err := scoped.GetService(ctx, svc.TenantID, svc.ID)
		if err != nil {
			return err
		}
		current.Spec.Replicas = 99
		if err := scoped.UpdateService(ctx, current); err != nil {
			return err
		}
		return rollbackError
	})
	if !errors.Is(err, rollbackError) {
		t.Fatalf("callback error = %v, want %v", err, rollbackError)
	}
	if _, err := repository.GetTask(ctx, svc.TenantID, rollbackTaskID); !errors.Is(err, kernelstore.ErrNotFound) {
		t.Fatalf("nested task survived outer rollback: %v", err)
	}
	current, err := repository.GetService(ctx, svc.TenantID, svc.ID)
	if err != nil || current.Spec.Replicas != 1 {
		t.Fatalf("service mutation survived callback rollback: %+v err=%v", current, err)
	}
}

func TestServiceTaskSupervisorsConvergeOnOneInstanceAndTask(t *testing.T) {
	clock := newFakeClock()
	pool, repository := prepare(t, clock.Now)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	publishVersion(t, ctx, repository, "tenant-a", "agent", "1", `{"runtimeClassPolicy":{"allowed":["reference"]}}`)
	stores := []*postgresstore.Store{repository, postgresstore.NewWithClock(pool, clock.Now)}
	supervisors := make([]*supervisor.Supervisor, len(stores))
	for index, store := range stores {
		supervisors[index] = supervisor.NewSupervisor(store,
			supervisor.WithSpawner(supervisor.NewTaskSpawner(store)), supervisor.WithClock(clock.Now))
	}
	start := make(chan struct{})
	results := make(chan error, len(supervisors))
	var group sync.WaitGroup
	for _, engine := range supervisors {
		group.Add(1)
		go func(engine *supervisor.Supervisor) {
			defer group.Done()
			<-start
			svc := runtimeService("tenant-a", "concurrent-task-service")
			svc.Spec.RuntimeClass = "reference"
			svc.Spec.WorkloadSpec = []byte(`{"budget":{"tokens":100,"costUsd":1,"toolCalls":1,"wallSeconds":60},"placement":{"runtimeClasses":["reference"],"region":"cn-east"}}`)
			_, err := engine.CreateService(ctx, svc)
			results <- err
		}(engine)
	}
	close(start)
	group.Wait()
	close(results)
	created, duplicate := 0, 0
	for err := range results {
		switch {
		case err == nil:
			created++
		case errors.Is(err, supervisor.ErrServiceAlreadyExists):
			duplicate++
		default:
			t.Fatalf("concurrent service creation: %v", err)
		}
	}
	if created != 1 || duplicate != 1 {
		t.Fatalf("create results: success=%d duplicate=%d", created, duplicate)
	}
	errs := make(chan error, len(supervisors))
	for _, engine := range supervisors {
		group.Add(1)
		go func(engine *supervisor.Supervisor) {
			defer group.Done()
			for range 8 {
				if err := engine.Reconcile(ctx, ""); err != nil {
					errs <- err
					return
				}
			}
		}(engine)
	}
	group.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	instances, err := repository.ListInstances(ctx, "tenant-a", "concurrent-task-service")
	if err != nil || len(instances) != 1 || instances[0].TaskID == nil || instances[0].Phase != supervisor.InstanceStarting {
		t.Fatalf("supervisors overprovisioned or advertised queued task as running: %+v err=%v", instances, err)
	}
	var taskCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE tenant_id = 'tenant-a'`).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 1 {
		t.Fatalf("concurrent supervisors created %d tasks, want 1", taskCount)
	}
	task, err := repository.GetTask(ctx, "tenant-a", *instances[0].TaskID)
	if err != nil || task.Phase != domain.TaskQueued || task.AgentVersionRef != "agent@1" {
		t.Fatalf("instance is not bound to its queued task: %+v err=%v", task, err)
	}
}
