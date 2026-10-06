import type {
  AgentManifest,
  CreateTaskRequest,
  CreateWorkflowRequest,
  JSONValue,
  RuntimeEvent,
  RuntimeResult,
  StartRequest,
  Task,
  Workflow,
} from "./types.js";

const MAX_RESPONSE_BYTES = 2 * 1024 * 1024;

export class FencedError extends Error {
  readonly status: number;
  readonly code: string | undefined;
  readonly traceId: string | undefined;

  constructor(status: number, message: string, details?: { code?: string; traceId?: string }) {
    super(message);
    this.name = "FencedError";
    this.status = status;
    this.code = details?.code;
    this.traceId = details?.traceId;
  }
}

export interface ClientOptions {
  readonly token?: string | (() => string | Promise<string>);
  readonly fetch?: typeof globalThis.fetch;
  readonly signal?: AbortSignal;
}

export interface ResourceResponse<T> {
  readonly value: T;
  readonly etag?: string;
  readonly replayed: boolean;
}

abstract class JSONClient {
  protected readonly baseURL: string;
  private readonly token: ClientOptions["token"] | undefined;
  private readonly fetcher: typeof globalThis.fetch;
  private readonly signal: AbortSignal | undefined;

  protected constructor(baseURL: string, options: ClientOptions = {}) {
    const parsed = new URL(baseURL);
    if (!/^https?:$/.test(parsed.protocol) || parsed.username || parsed.password || parsed.search || parsed.hash || (parsed.pathname !== "/" && parsed.pathname !== "")) {
      throw new TypeError("base URL must be an absolute HTTP(S) origin without credentials, query, or fragment");
    }
    this.baseURL = parsed.origin;
    this.token = options.token;
    this.fetcher = options.fetch ?? globalThis.fetch.bind(globalThis);
    this.signal = options.signal;
  }

  protected async request<T>(method: string, path: string, body?: unknown, headers: Record<string, string> = {}, accepted = [200]): Promise<ResourceResponse<T>> {
    const token = typeof this.token === "function" ? await this.token() : this.token;
    const requestHeaders = new Headers(headers);
    requestHeaders.set("Accept", "application/json");
    if (body !== undefined) requestHeaders.set("Content-Type", "application/json");
    if (token) requestHeaders.set("Authorization", `Bearer ${token}`);
    const init: RequestInit = { method, headers: requestHeaders };
    if (body !== undefined) init.body = JSON.stringify(body);
    if (this.signal !== undefined) init.signal = this.signal;
    const response = await this.fetcher(this.baseURL + path, init);
    const bytes = new Uint8Array(await response.arrayBuffer());
    if (bytes.byteLength > MAX_RESPONSE_BYTES) throw new Error("Fenced response exceeds 2 MiB");
    let document: unknown = {};
    if (bytes.byteLength > 0) {
      try {
        document = JSON.parse(new TextDecoder().decode(bytes));
      } catch {
        throw new Error("Fenced returned invalid JSON");
      }
    }
    if (!accepted.includes(response.status)) {
      const problem = document as { detail?: string; title?: string; code?: string; traceId?: string };
      throw new FencedError(response.status, problem.detail ?? problem.title ?? `HTTP ${response.status}`, problem);
    }
    const etag = response.headers.get("ETag") ?? undefined;
    return {
      value: document as T,
      ...(etag === undefined ? {} : { etag }),
      replayed: response.headers.get("Idempotent-Replayed") === "true",
    };
  }
}

export class ControlClient extends JSONClient {
  async publishAgent(manifest: AgentManifest, idempotencyKey: string): Promise<ResourceResponse<JSONValue>> {
    return this.request("POST", "/v1/agent-versions", { manifest }, { "Idempotency-Key": requireKey(idempotencyKey) }, [200, 201]);
  }

  async createTask(input: CreateTaskRequest, idempotencyKey: string): Promise<ResourceResponse<Task>> {
    return this.request("POST", "/v1/tasks", input, { "Idempotency-Key": requireKey(idempotencyKey) }, [200, 202]);
  }

  async getTask(taskId: string): Promise<ResourceResponse<Task>> {
    return this.request("GET", `/v1/tasks/${segment(taskId)}`);
  }

  async cancelTask(taskId: string, resourceVersion: number): Promise<ResourceResponse<Task>> {
    return this.request("POST", `/v1/tasks/${segment(taskId)}:cancel`, {}, { "If-Match": etag(resourceVersion) }, [202]);
  }

  async createWorkflow(input: CreateWorkflowRequest, idempotencyKey: string): Promise<ResourceResponse<Workflow>> {
    return this.request("POST", "/v1/workflows", input, { "Idempotency-Key": requireKey(idempotencyKey) }, [200, 202]);
  }

  async getWorkflow(workflowId: string): Promise<ResourceResponse<Workflow>> {
    return this.request("GET", `/v1/workflows/${segment(workflowId)}`);
  }

  async cancelWorkflow(workflowId: string, resourceVersion: number): Promise<ResourceResponse<Workflow>> {
    return this.request("POST", `/v1/workflows/${segment(workflowId)}/cancel`, {}, { "If-Match": etag(resourceVersion) }, [202]);
  }

  async decideWorkflowStep(workflowId: string, stepName: string, resourceVersion: number, decision: "approve" | "reject"): Promise<ResourceResponse<Workflow>> {
    return this.request("POST", `/v1/workflows/${segment(workflowId)}/steps/${segment(stepName)}/approval`, { decision }, { "If-Match": etag(resourceVersion) }, [202]);
  }
}

export class RuntimeClient extends JSONClient {
  readonly protocol = "fenced.runtime.interface/v1";

  private async runtimeRequest<T>(method: string, path: string, body?: unknown, accepted = [200]): Promise<ResourceResponse<T>> {
    return this.request<T>(method, `/v1${path}`, body, { "Fenced-Runtime-Interface": this.protocol }, accepted);
  }

  async health(): Promise<ResourceResponse<{ status: string; adapter: string; protocolVersions: readonly string[] }>> {
    return this.runtimeRequest("GET", "/health");
  }

  async start(input: StartRequest): Promise<ResourceResponse<{ executionId: string; replayed?: boolean }>> {
    return this.runtimeRequest("POST", "/executions:start", input, [200, 202]);
  }

  async stop(executionId: string): Promise<ResourceResponse<JSONValue>> {
    return this.runtimeRequest("POST", `/executions/${segment(executionId)}:stop`, {}, [202]);
  }

  async events(executionId: string, after = 0): Promise<ResourceResponse<{ events: readonly RuntimeEvent[]; nextAfter: number; truncated?: boolean }>> {
    return this.runtimeRequest("GET", `/executions/${segment(executionId)}/events?after=${encodeURIComponent(String(after))}`);
  }

  async result(executionId: string): Promise<ResourceResponse<RuntimeResult>> {
    return this.runtimeRequest("GET", `/executions/${segment(executionId)}/result`, undefined, [200, 202]);
  }

  async checkpoint(executionId: string): Promise<ResourceResponse<JSONValue>> {
    return this.runtimeRequest("POST", `/executions/${segment(executionId)}:checkpoint`, {});
  }

  async restore(executionId: string, checkpoint: JSONValue): Promise<ResourceResponse<JSONValue>> {
    return this.runtimeRequest("POST", `/executions/${segment(executionId)}:restore`, { executionId, checkpoint });
  }
}

function segment(value: string): string {
  if (!value.trim()) throw new TypeError("identifier must not be empty");
  return encodeURIComponent(value);
}

function requireKey(value: string): string {
  if (!/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/.test(value)) throw new TypeError("invalid idempotency key");
  return value;
}

function etag(version: number): string {
  if (!Number.isSafeInteger(version) || version < 1) throw new TypeError("resource version must be a positive safe integer");
  return `W/"${version}"`;
}
