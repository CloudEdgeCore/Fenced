// The views of the console. Every view reads the control plane of the tenant
// that is in focus; the fleet view reads several tenants at once.

import { useCallback, useEffect, useMemo, useState, type ReactNode } from 'react';
import { ControlClient } from './api';
import { collectFleet, type TenantSummary } from './fleet';
import { streamTaskEvents } from './events';
import { Badge, Card, Chip, Empty, ErrorBanner, Field, Notice, Table, when } from './ui';
import { column, type Language } from './i18n';

type T = (key: Parameters<ReturnType<typeof import('./i18n').translator>>[0]) => string;

interface Async<T> {
  data: T | undefined;
  error: string | undefined;
  loading: boolean;
  reload: () => void;
}

function useAsync<T>(load: () => Promise<T>, deps: unknown[]): Async<T> {
  const [data, setData] = useState<T | undefined>(undefined);
  const [error, setError] = useState<string | undefined>(undefined);
  const [loading, setLoading] = useState(true);
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    load()
      .then((value) => {
        if (!cancelled) {
          setData(value);
          setError(undefined);
        }
      })
      .catch((failure: unknown) => {
        if (!cancelled) {
          setError(failure instanceof Error ? failure.message : String(failure));
        }
      })
      .finally(() => {
        if (!cancelled) {
          setLoading(false);
        }
      });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [...deps, nonce]);

  const reload = useCallback(() => setNonce((value) => value + 1), []);
  return { data, error, loading, reload };
}

// ---------------------------------------------------------------- fleet

export function FleetView(props: {
  tenantIds: string[];
  labelFor: (tenantId: string) => string | undefined;
  clientFor: (tenantId: string) => ControlClient;
  onFocus: (tenantId: string) => void;
  t: T;
  language: Language;
}): ReactNode {
  const { tenantIds, labelFor, clientFor, onFocus, t, language } = props;
  const [nonce, setNonce] = useState(0);

  useEffect(() => {
    void nonce;
  }, [nonce]);

  const summaries = useAsync<TenantSummary[]>(
    () =>
      collectFleet(tenantIds, async (tenantId) => {
        const client = clientFor(tenantId);
        const [tasks, services, approvals, audit] = await Promise.all([
          client.tasks(50),
          client.services(),
          client.approvals(50),
          client.audit(100),
        ]);
        return {
          tasks: tasks.tasks ?? [],
          services: services ?? [],
          approvals: approvals.approvals ?? [],
          auditEvents: audit.events ?? [],
        };
      }, labelFor),
    [tenantIds.join(','), nonce],
  );

  const rows = summaries.data ?? [];
  return (
    <Card
      title={t('fleet')}
      subtitle={t('addTenantHint')}
      actions={<Chip onClick={() => setNonce((value) => value + 1)}>{t('refresh')}</Chip>}
    >
      {summaries.error && <ErrorBanner message={summaries.error} />}
      {rows.length === 0 ? (
        <Empty text={t('noTenant')} />
      ) : (
        <Table
          headers={[
            column(language, 'tenant'),
            column(language, 'tasks'),
            column(language, 'services'),
            column(language, 'rollouts'),
            column(language, 'approvals'),
            column(language, 'auditEvents'),
            '',
          ]}
        >
          {rows.map((summary) => (
            <tr key={summary.tenantId}>
              <td>
                <strong>{summary.label ?? summary.tenantId}</strong>
                <div className="muted">{summary.tenantId}</div>
              </td>
              <td>{summary.error ? '—' : summary.tasks}</td>
              <td>{summary.error ? '—' : summary.services}</td>
              <td>{summary.error ? '—' : summary.runningRollouts}</td>
              <td>{summary.error ? '—' : summary.approvals}</td>
              <td>{summary.error ? '—' : summary.auditEvents}</td>
              <td>
                {summary.error ? (
                  <span className="muted">{summary.error}</span>
                ) : (
                  <Chip onClick={() => onFocus(summary.tenantId)}>{t('focus')}</Chip>
                )}
              </td>
            </tr>
          ))}
        </Table>
      )}
    </Card>
  );
}

// ---------------------------------------------------------------- tasks

export function TasksView(props: { client: ControlClient; t: T; language: Language }): ReactNode {
  const { client, t, language } = props;
  const tasks = useAsync(() => client.tasks(50), [client]);
  const [goal, setGoal] = useState('');
  const [agentVersionRef, setAgentVersionRef] = useState('');
  const [namespace, setNamespace] = useState('default');
  const [notice, setNotice] = useState('');
  const [failure, setFailure] = useState('');
  const [events, setEvents] = useState<string[]>([]);
  const [followed, setFollowed] = useState('');

  const follow = useCallback(
    (taskId: string) => {
      setFollowed(taskId);
      setEvents([]);
      const controller = new AbortController();
      streamTaskEvents(
        client,
        taskId,
        (frame) => {
          setEvents((current) => [...current, `${frame.name ? `[${frame.name}] ` : ''}${frame.data}`]);
        },
        controller.signal,
      ).catch((error: unknown) => {
        setEvents((current) => [...current, `event stream failed: ${String(error)}`]);
      });
      return () => controller.abort();
    },
    [client],
  );

  const submit = async () => {
    setFailure('');
    try {
      const task = await client.submitTask({ agentVersionRef, namespace, goal });
      setNotice(`${t('taskSubmitted')} ${task.id}`);
      setGoal('');
      tasks.reload();
      follow(task.id);
    } catch (error) {
      setFailure(error instanceof Error ? error.message : String(error));
    }
  };

  return (
    <>
      <Card title={t('tasks')}>
        <div className="grid-2">
          <Field label={t('agentVersion')}>
            <input
              value={agentVersionRef}
              placeholder="summarizer@1.0.0"
              onChange={(event) => setAgentVersionRef(event.target.value)}
            />
          </Field>
          <Field label={t('namespace')}>
            <input value={namespace} onChange={(event) => setNamespace(event.target.value)} />
          </Field>
        </div>
        <Field label={t('goal')}>
          <textarea value={goal} onChange={(event) => setGoal(event.target.value)} />
        </Field>
        <div className="row">
          <Chip onClick={() => void submit()}>{t('submit')}</Chip>
          {notice && <span className="muted">{notice}</span>}
        </div>
        {failure && <ErrorBanner message={failure} onDismiss={() => setFailure('')} />}
      </Card>

      <Card
        title={t('taskLedger')}
        actions={<Chip onClick={tasks.reload}>{t('refresh')}</Chip>}
      >
        {tasks.error && <ErrorBanner message={tasks.error} />}
        <Table
          headers={[
            column(language, 'task'),
            column(language, 'phase'),
            column(language, 'goal'),
            column(language, 'namespace'),
            column(language, 'tokens'),
            column(language, 'created'),
            '',
          ]}
        >
          {(tasks.data?.tasks ?? []).map((task) => (
            <tr key={task.id}>
              <td className="mono">{task.id}</td>
              <td>
                <Badge tone={task.phase === 'SUCCEEDED' ? 'ok' : task.phase === 'FAILED' ? 'bad' : undefined}>
                  {task.phase}
                </Badge>
              </td>
              <td>{task.goal}</td>
              <td>{task.namespace}</td>
              <td>{task.usage?.tokens ?? '—'}</td>
              <td>{when(task.createdAt)}</td>
              <td>
                <Chip onClick={() => follow(task.id)}>{t('events')}</Chip>
              </td>
            </tr>
          ))}
        </Table>
        {(tasks.data?.tasks ?? []).length === 0 && !tasks.loading && <Empty text={t('noRecords')} />}
      </Card>

      <Card title={t('events')} subtitle={followed}>
        {events.length === 0 ? (
          <Empty text={t('noTask')} />
        ) : (
          <div className="event-stream">
            {events.map((line, index) => (
              <div key={`${index}-${line.slice(0, 24)}`}>{line}</div>
            ))}
          </div>
        )}
      </Card>
    </>
  );
}

// ---------------------------------------------------------------- services

export function ServicesView(props: { client: ControlClient; t: T; language: Language }): ReactNode {
  const { client, t, language } = props;
  const services = useAsync(() => client.services(), [client]);
  const [failure, setFailure] = useState('');

  const act = async (action: () => Promise<unknown>) => {
    setFailure('');
    try {
      await action();
      services.reload();
    } catch (error) {
      setFailure(error instanceof Error ? error.message : String(error));
    }
  };

  const rollOut = (id: string) => {
    const version = window.prompt(t('targetVersion'), '');
    if (!version) {
      return;
    }
    void act(() => client.rollout(id, version.trim()));
  };

  const scale = (id: string, current: number) => {
    const raw = window.prompt(t('scale'), String(current));
    if (!raw) {
      return;
    }
    const replicas = Number.parseInt(raw, 10);
    if (Number.isNaN(replicas) || replicas < 0) {
      return;
    }
    void act(() => client.scale(id, replicas));
  };

  return (
    <Card
      title={t('servicesTitle')}
      actions={<Chip onClick={services.reload}>{t('refresh')}</Chip>}
    >
      {services.error && <ErrorBanner message={services.error} />}
      {failure && <ErrorBanner message={failure} onDismiss={() => setFailure('')} />}
      <Table
        headers={[
          column(language, 'service'),
          column(language, 'agent'),
          column(language, 'version'),
          column(language, 'phase'),
          column(language, 'rollout'),
          column(language, 'ready'),
          '',
        ]}
      >
        {(services.data ?? []).map((service) => (
          <tr key={service.id}>
            <td>
              <strong>{service.name}</strong>
              <div className="muted mono">{service.id}</div>
            </td>
            <td>{service.agentId}</td>
            <td>{service.status?.activeVersion ?? '—'}</td>
            <td>
              <Badge tone={service.status?.phase === 'Active' ? 'ok' : 'warn'}>{service.status?.phase ?? '—'}</Badge>
            </td>
            <td>
              {service.status?.rolloutPhase ? (
                <>
                  <Badge tone={service.status.rolloutPhase === 'FAILED' ? 'bad' : 'info'}>
                    {service.status.rolloutPhase}
                  </Badge>
                  {service.status.rolloutFailureReason && (
                    <div className="muted">{service.status.rolloutFailureReason}</div>
                  )}
                </>
              ) : (
                '—'
              )}
            </td>
            <td>
              {service.status?.readyReplicas ?? 0}/{service.status?.desiredReplicas ?? 0}
            </td>
            <td className="row">
              <Chip onClick={() => rollOut(service.id)}>{t('rollOut')}</Chip>
              <Chip onClick={() => scale(service.id, service.status?.desiredReplicas ?? 0)}>{t('scale')}</Chip>
              <Chip
                tone="danger"
                onClick={() => void act(() => client.rollback(service.id))}
              >
                {t('rollBack')}
              </Chip>
            </td>
          </tr>
        ))}
      </Table>
      {(services.data ?? []).length === 0 && !services.loading && <Empty text={t('noRecords')} />}
    </Card>
  );
}

// ---------------------------------------------------------------- governance

export function GovernanceView(props: { client: ControlClient; t: T; language: Language }): ReactNode {
  const { client, t, language } = props;
  const governance = useAsync(
    async () => {
      const [versions, workflows, approvals, pools] = await Promise.all([
        client.agentVersions(50),
        client.workflows(50),
        client.approvals(50),
        client.runtimePools(),
      ]);
      return {
        versions: versions.agentVersions ?? [],
        workflows: workflows.workflows ?? [],
        approvals: approvals.approvals ?? [],
        pools: pools.runtimePools ?? [],
      };
    },
    [client],
  );

  const data = governance.data;
  return (
    <>
      <Card
        title={t('governanceTitle')}
        actions={<Chip onClick={governance.reload}>{t('refresh')}</Chip>}
      >
        {governance.error && <ErrorBanner message={governance.error} />}
        <div className="grid-2">
          <div>
            <h3 className="sub-title">{t('services')}</h3>
            <Table headers={[column(language, 'object'), column(language, 'kind'), column(language, 'value')]}>
              {(data?.versions ?? []).map((version, index) => (
                <tr key={index}>
                  <td className="mono">{String((version as { ref?: string }).ref ?? '—')}</td>
                  <td>AgentVersion</td>
                  <td className="mono">{String((version as { specDigest?: string }).specDigest ?? '').slice(0, 16)}</td>
                </tr>
              ))}
            </Table>
          </div>
          <div>
            <h3 className="sub-title">{column(language, 'rollouts')}</h3>
            <Table headers={[column(language, 'object'), column(language, 'kind'), column(language, 'value')]}>
              {(data?.workflows ?? []).map((workflow, index) => (
                <tr key={index}>
                  <td className="mono">{String((workflow as { id?: string }).id ?? '—')}</td>
                  <td>Workflow</td>
                  <td>{String((workflow as { status?: string }).status ?? '')}</td>
                </tr>
              ))}
            </Table>
          </div>
        </div>
      </Card>

      <Card title={t('approvalsTitle')}>
        <Table
          headers={[
            column(language, 'tool'),
            column(language, 'action'),
            column(language, 'resource'),
            column(language, 'task'),
            column(language, 'requested'),
            column(language, 'expires'),
            column(language, 'decision'),
          ]}
        >
          {(data?.approvals ?? []).map((approval) => (
            <tr key={approval.id}>
              <td>
                {approval.toolName}
                {approval.toolVersion ? `@${approval.toolVersion}` : ''}
              </td>
              <td>{approval.action ?? '—'}</td>
              <td>{approval.resource ?? '—'}</td>
              <td className="mono">{approval.taskId ?? '—'}</td>
              <td>{when(approval.requestedAt)}</td>
              <td>{when(approval.expiresAt)}</td>
              <td>
                <Badge tone={approval.status === 'PENDING' ? 'warn' : 'ok'}>{approval.status}</Badge>
              </td>
            </tr>
          ))}
        </Table>
        {(data?.approvals ?? []).length === 0 && <Empty text={t('noRecords')} />}
      </Card>

      <Card title="Runtime Pools">
        <Table headers={['Pool', 'Class', 'Region', 'Ready', 'CPU', 'Memory']}>
          {(data?.pools ?? []).map((pool, index) => {
            const record = pool as Record<string, unknown>;
            return (
              <tr key={index}>
                <td className="mono">{String(record['id'] ?? '')}</td>
                <td>{String(record['runtimeClass'] ?? '')}</td>
                <td>{String(record['region'] ?? '')}</td>
                <td>{String(record['ready'] ?? '')}</td>
                <td>{String(record['availableCpuMillis'] ?? '')}</td>
                <td>{String(record['availableMemoryMiB'] ?? '')}</td>
              </tr>
            );
          })}
        </Table>
        {(data?.pools ?? []).length === 0 && <Empty text={t('noRecords')} />}
      </Card>
    </>
  );
}

// ---------------------------------------------------------------- audit

export function AuditView(props: { client: ControlClient; t: T; language: Language }): ReactNode {
  const { client, t, language } = props;
  const audit = useAsync(() => client.audit(100), [client]);
  return (
    <Card title={t('auditLedger')} actions={<Chip onClick={audit.reload}>{t('refresh')}</Chip>}>
      {audit.error && <ErrorBanner message={audit.error} />}
      <Table
        headers={[
          column(language, 'seq'),
          column(language, 'event'),
          column(language, 'object'),
          column(language, 'actor'),
          column(language, 'occurred'),
          column(language, 'chainHash'),
        ]}
      >
        {(audit.data?.events ?? []).map((event, index) => (
          <tr key={index}>
            <td>{event.seq ?? '—'}</td>
            <td>{event.eventType ?? '—'}</td>
            <td>
              {event.resourceType ?? '—'} {event.resourceId ?? ''}
            </td>
            <td>{event.actor ?? '—'}</td>
            <td>{when(event.occurredAt)}</td>
            <td className="mono">{(event.chainHash ?? '').slice(0, 16)}</td>
          </tr>
        ))}
      </Table>
      {(audit.data?.events ?? []).length === 0 && !audit.loading && <Empty text={t('noRecords')} />}
    </Card>
  );
}

export function useMemoizedClients(
  tokens: Map<string, string>,
): (tenantId: string) => ControlClient {
  return useMemo(() => {
    const clients = new Map<string, ControlClient>();
    for (const [tenantId, token] of tokens) {
      clients.set(tenantId, new ControlClient({ token }));
    }
    return (tenantId: string) => {
      const client = clients.get(tenantId);
      if (!client) {
        throw new Error(`no token for tenant ${tenantId}`);
      }
      return client;
    };
  }, [tokens]);
}

export { Notice };
