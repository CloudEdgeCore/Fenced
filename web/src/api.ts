// The typed Control API client.
//
// Every call carries the bearer token of one tenant. The kernel scopes every
// resource by the tenant of the authenticated principal, so the client never
// carries a tenant of its own: the token decides what the caller sees.

export interface Problem {
  type?: string;
  title?: string;
  status?: number;
  detail?: string;
  reasonCode?: string;
}

export class ApiError extends Error {
  constructor(
    readonly status: number,
    readonly reasonCode: string,
    message: string,
  ) {
    super(message);
    this.name = 'ApiError';
  }
}

export type FetchLike = (
  input: string,
  init?: { method?: string; headers?: Record<string, string>; body?: string; signal?: AbortSignal },
) => Promise<Response>;

export interface ClientOptions {
  baseUrl?: string;
  token: string;
  fetchImpl?: FetchLike;
}

export interface Task {
  id: string;
  namespace: string;
  goal: string;
  phase: string;
  agentVersionRef: string;
  createdAt: string;
  updatedAt: string;
  usage?: { tokens?: number; toolCalls?: number; costUsd?: number };
}

export interface ServiceStatus {
  phase?: string;
  desiredReplicas?: number;
  currentReplicas?: number;
  readyReplicas?: number;
  updatedReplicas?: number;
  activeVersion?: string;
  previousVersion?: string;
  rolloutPhase?: string;
  rolloutTargetVersion?: string;
  rolloutFailureReason?: string;
  message?: string;
}

export interface AgentService {
  id: string;
  name: string;
  namespace: string;
  agentId: string;
  spec: { replicas?: number; agentVersion?: string };
  status: ServiceStatus;
}

export interface AuditEvent {
  seq?: number;
  eventType?: string;
  resourceType?: string;
  resourceId?: string;
  actor?: string;
  occurredAt?: string;
  chainHash?: string;
}

export interface Approval {
  id: string;
  toolName: string;
  toolVersion?: string;
  action?: string;
  resource?: string;
  taskId?: string;
  status: string;
  requestedAt?: string;
  expiresAt?: string;
}

export interface RolloutConfig {
  maxSurge?: number;
  maxUnavailable?: number;
  minReadyTime?: number;
  autoRollback?: boolean;
}

export class ControlClient {
  private readonly baseUrl: string;
  private readonly fetchImpl: FetchLike;

  constructor(private readonly options: ClientOptions) {
    this.baseUrl = (options.baseUrl ?? '').replace(/\/$/, '');
    this.fetchImpl = options.fetchImpl ?? ((input, init) => fetch(input, init));
  }

  get token(): string {
    return this.options.token;
  }

  async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    const headers: Record<string, string> = { Accept: 'application/json' };
    if (body !== undefined) {
      headers['Content-Type'] = 'application/json';
    }
    if (this.options.token) {
      headers['Authorization'] = `Bearer ${this.options.token}`;
    }
    const init: { method: string; headers: Record<string, string>; body?: string } = { method, headers };
    if (body !== undefined) {
      init.body = JSON.stringify(body);
    }
    const response = await this.fetchImpl(this.baseUrl + path, init);
    const text = await response.text();
    let payload: unknown = null;
    if (text) {
      try {
        payload = JSON.parse(text);
      } catch {
        payload = null;
      }
    }
    if (!response.ok) {
      const problem = (payload ?? {}) as Problem;
      throw new ApiError(
        response.status,
        problem.reasonCode ?? 'HTTP_ERROR',
        problem.detail ?? problem.title ?? `HTTP ${response.status}`,
      );
    }
    return payload as T;
  }

  get<T>(path: string): Promise<T> {
    return this.request<T>('GET', path);
  }

  post<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>('POST', path, body);
  }

  put<T>(path: string, body?: unknown): Promise<T> {
    return this.request<T>('PUT', path, body);
  }

  // ------------------------------------------------------------- resources

  metrics(): Promise<Record<string, number>> {
    return this.get('/v1/metrics');
  }

  tasks(limit = 50): Promise<{ tasks: Task[] }> {
    return this.get(`/v1/tasks?limit=${limit}`);
  }

  task(id: string): Promise<Task> {
    return this.get(`/v1/tasks/${encodeURIComponent(id)}`);
  }

  submitTask(body: {
    agentVersionRef: string;
    namespace: string;
    goal: string;
    spec?: Record<string, unknown>;
  }): Promise<Task> {
    return this.post('/v1/tasks', { spec: {}, ...body });
  }

  cancelTask(id: string): Promise<Task> {
    return this.post(`/v1/tasks/${encodeURIComponent(id)}:cancel`);
  }

  services(): Promise<AgentService[]> {
    return this.get('/v1/services');
  }

  agentVersions(limit = 50): Promise<{ agentVersions: Record<string, unknown>[] }> {
    return this.get(`/v1/agent-versions?limit=${limit}`);
  }

  workflows(limit = 50): Promise<{ workflows: Record<string, unknown>[] }> {
    return this.get(`/v1/workflows?limit=${limit}`);
  }

  approvals(limit = 50): Promise<{ approvals: Approval[] }> {
    return this.get(`/v1/approvals?limit=${limit}`);
  }

  audit(limit = 100): Promise<{ events: AuditEvent[] }> {
    return this.get(`/v1/audit?limit=${limit}`);
  }

  runtimePools(): Promise<{ runtimePools: Record<string, unknown>[] }> {
    return this.get('/v1/runtime-pools');
  }

  rollout(serviceId: string, newVersion: string, config?: RolloutConfig): Promise<AgentService> {
    return this.post(`/v1/services/${encodeURIComponent(serviceId)}/rollout`, {
      newVersion,
      ...(config ? { config } : {}),
    });
  }

  rollback(serviceId: string): Promise<AgentService> {
    return this.post(`/v1/services/${encodeURIComponent(serviceId)}/rollback`);
  }

  scale(serviceId: string, replicas: number): Promise<AgentService> {
    return this.post(`/v1/services/${encodeURIComponent(serviceId)}/scale`, { replicas });
  }

  version(): Promise<{ product?: string; productVersion?: string }> {
    return this.get('/versionz');
  }
}
