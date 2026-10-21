// The fleet view.
//
// An operator who administers several tenants holds one token per tenant.
// The fleet view asks every tenant the operator holds about itself and puts
// the answers side by side. The aggregation happens in the browser: the
// kernel is never asked about a tenant the caller does not hold a token for.

import type { AgentService, Approval, AuditEvent, Task } from './api';

export interface TenantRollout {
  serviceId: string;
  serviceName: string;
  phase: string;
  target?: string;
  reason?: string;
}

export interface TenantSummary {
  tenantId: string;
  label?: string;
  tasks: number;
  services: number;
  runningRollouts: number;
  approvals: number;
  auditEvents: number;
  rollouts: TenantRollout[];
  error?: string;
}

export interface TenantPayload {
  tasks: Task[];
  services: AgentService[];
  approvals: Approval[];
  auditEvents: AuditEvent[];
}

/** The rollouts of a tenant that are still moving. */
export function runningRollouts(services: AgentService[]): TenantRollout[] {
  const moving = new Set(['PROGRESSING', 'ROLLING_BACK']);
  return services
    .filter((service) => moving.has(service.status?.rolloutPhase ?? ''))
    .map((service) => {
      const rollout: TenantRollout = {
        serviceId: service.id,
        serviceName: service.name,
        phase: service.status?.rolloutPhase ?? '',
      };
      if (service.status?.rolloutTargetVersion) {
        rollout.target = service.status.rolloutTargetVersion;
      }
      if (service.status?.rolloutFailureReason) {
        rollout.reason = service.status.rolloutFailureReason;
      }
      return rollout;
    });
}

/** Reduce the answers of one tenant to the numbers the fleet view shows. */
export function summarizeTenant(tenantId: string, payload: TenantPayload, label?: string): TenantSummary {
  const rollouts = runningRollouts(payload.services);
  const summary: TenantSummary = {
    tenantId,
    tasks: payload.tasks.length,
    services: payload.services.length,
    runningRollouts: rollouts.length,
    approvals: payload.approvals.filter((approval) => approval.status === 'PENDING').length,
    auditEvents: payload.auditEvents.length,
    rollouts,
  };
  if (label) {
    summary.label = label;
  }
  return summary;
}

/** Fetch one tenant's answers. Injected so the fleet view stays testable. */
export type TenantReader = (tenantId: string) => Promise<TenantPayload>;

export async function collectFleet(
  tenantIds: string[],
  read: TenantReader,
  label?: (tenantId: string) => string | undefined,
): Promise<TenantSummary[]> {
  const summaries = await Promise.all(
    tenantIds.map(async (tenantId) => {
      try {
        const payload = await read(tenantId);
        return summarizeTenant(tenantId, payload, label?.(tenantId));
      } catch (error) {
        const summary: TenantSummary = {
          tenantId,
          tasks: 0,
          services: 0,
          runningRollouts: 0,
          approvals: 0,
          auditEvents: 0,
          rollouts: [],
          error: error instanceof Error ? error.message : String(error),
        };
        const name = label?.(tenantId);
        if (name) {
          summary.label = name;
        }
        return summary;
      }
    }),
  );
  return summaries;
}
