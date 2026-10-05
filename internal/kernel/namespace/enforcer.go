package namespace

import (
	"context"
	"fmt"

	"github.com/CloudEdgeCore/Fenced/internal/kernel/store"
)

// Enforcer performs admission and policy enforcement on namespaces and resource quotas.
type Enforcer struct {
	store Store
}

// NewEnforcer creates a new Enforcer backed by a namespace Store.
func NewEnforcer(store Store) *Enforcer {
	return &Enforcer{
		store: store,
	}
}

// CheckNamespaceActive verifies that a namespace exists and is in the Active phase.
func (e *Enforcer) CheckNamespaceActive(ctx context.Context, tenantID, nsName string) (*Namespace, error) {
	if nsName == "" {
		nsName = DefaultNamespace
	}
	ns, err := e.store.GetNamespace(ctx, tenantID, nsName)
	if err != nil {
		return nil, err
	}
	if ns.Phase == NamespacePhaseTerminating {
		return nil, fmt.Errorf("%w: namespace %q is terminating and cannot accept workloads", ErrNamespaceTerminating, nsName)
	}
	return ns, nil
}

// CheckTaskAdmission validates whether a new task can be admitted into the namespace.
func (e *Enforcer) CheckTaskAdmission(ctx context.Context, tenantID, nsName string, budget store.TaskBudget) error {
	ns, err := e.CheckNamespaceActive(ctx, tenantID, nsName)
	if err != nil {
		return err
	}

	usage, err := e.store.GetUsage(ctx, tenantID, ns.Name)
	if err != nil {
		return err
	}

	q := ns.Quota
	if q.MaxTasks > 0 && usage.ActiveTasks >= q.MaxTasks {
		return fmt.Errorf("%w: max active tasks limit (%d) reached in namespace %s", ErrQuotaExceeded, q.MaxTasks, ns.Name)
	}
	if q.MaxTokens > 0 && (usage.ConsumedTokens+budget.Tokens) > q.MaxTokens {
		return fmt.Errorf("%w: token ceiling (%d) exceeded in namespace %s", ErrQuotaExceeded, q.MaxTokens, ns.Name)
	}
	if q.MaxCostMicroUSD > 0 && (usage.ConsumedCostMicroUSD+int64(budget.CostMicroUSD)) > q.MaxCostMicroUSD {
		return fmt.Errorf("%w: cost ceiling (%d micro-USD) exceeded in namespace %s", ErrQuotaExceeded, q.MaxCostMicroUSD, ns.Name)
	}
	if q.MaxToolCalls > 0 && (usage.ConsumedToolCalls+budget.ToolCalls) > q.MaxToolCalls {
		return fmt.Errorf("%w: tool calls ceiling (%d) exceeded in namespace %s", ErrQuotaExceeded, q.MaxToolCalls, ns.Name)
	}
	if q.MaxWallSeconds > 0 && (usage.ConsumedWallSeconds+budget.WallSeconds) > q.MaxWallSeconds {
		return fmt.Errorf("%w: wall seconds ceiling (%d) exceeded in namespace %s", ErrQuotaExceeded, q.MaxWallSeconds, ns.Name)
	}

	return nil
}

// CheckServiceAdmission validates whether a new service can be created or scaled in the namespace.
func (e *Enforcer) CheckServiceAdmission(ctx context.Context, tenantID, nsName string, isNewService bool, replicas int) error {
	ns, err := e.CheckNamespaceActive(ctx, tenantID, nsName)
	if err != nil {
		return err
	}

	usage, err := e.store.GetUsage(ctx, tenantID, ns.Name)
	if err != nil {
		return err
	}

	q := ns.Quota
	if isNewService && q.MaxServices > 0 && usage.ActiveServices >= q.MaxServices {
		return fmt.Errorf("%w: max active services limit (%d) reached in namespace %s", ErrQuotaExceeded, q.MaxServices, ns.Name)
	}
	if q.MaxReplicasPerService > 0 && replicas > q.MaxReplicasPerService {
		return fmt.Errorf("%w: replicas request (%d) exceeds max replicas per service limit (%d) in namespace %s", ErrQuotaExceeded, replicas, q.MaxReplicasPerService, ns.Name)
	}

	return nil
}

// CheckIPCPermission evaluates cross-namespace rules between sender and receiver.
func (e *Enforcer) CheckIPCPermission(ctx context.Context, senderTenant, senderNS, receiverTenant, receiverNS string) error {
	if senderTenant != receiverTenant {
		return ErrCrossNamespaceForbidden
	}
	if senderNS == "" {
		senderNS = DefaultNamespace
	}
	if receiverNS == "" {
		receiverNS = DefaultNamespace
	}

	sender, err := e.CheckNamespaceActive(ctx, senderTenant, senderNS)
	if err != nil {
		return fmt.Errorf("sender namespace check: %w", err)
	}

	if senderNS == receiverNS {
		return nil
	}

	// Cross-namespace communication requires permission in both namespaces
	if !sender.Quota.AllowCrossNamespaceIPC {
		return fmt.Errorf("%w: sender namespace %s does not allow cross-namespace IPC", ErrCrossNamespaceForbidden, senderNS)
	}

	receiver, err := e.CheckNamespaceActive(ctx, receiverTenant, receiverNS)
	if err != nil {
		return fmt.Errorf("receiver namespace check: %w", err)
	}
	if !receiver.Quota.AllowCrossNamespaceIPC {
		return fmt.Errorf("%w: receiver namespace %s does not allow cross-namespace IPC", ErrCrossNamespaceForbidden, receiverNS)
	}

	return nil
}

// CheckMailboxAdmission checks if a mailbox message can be admitted without overflowing quota.
func (e *Enforcer) CheckMailboxAdmission(ctx context.Context, tenantID, nsName string) error {
	ns, err := e.CheckNamespaceActive(ctx, tenantID, nsName)
	if err != nil {
		return err
	}

	usage, err := e.store.GetUsage(ctx, tenantID, ns.Name)
	if err != nil {
		return err
	}

	if ns.Quota.MaxIPCMailboxMessages > 0 && usage.MailboxMessages >= ns.Quota.MaxIPCMailboxMessages {
		return fmt.Errorf("%w: mailbox messages limit (%d) reached in namespace %s", ErrQuotaExceeded, ns.Quota.MaxIPCMailboxMessages, ns.Name)
	}

	return nil
}
