export type JSONPrimitive = string | number | boolean | null;
export type JSONValue = JSONPrimitive | JSONValue[] | { readonly [key: string]: JSONValue };

export interface AgentManifest {
  readonly apiVersion: "fenced.dev/v1" | string;
  readonly kind: "AgentManifest" | string;
  readonly metadata: {
    readonly name: string;
    readonly version: string;
    readonly namespace?: string;
  };
  readonly spec: {
    readonly runtimeClassPolicy: { readonly allowed: readonly string[]; readonly preferred?: string };
    readonly runtimes: readonly RuntimeTarget[];
    readonly capabilities: CapabilityGrant;
    readonly resources?: { readonly cpuMillis?: number; readonly memoryMiB?: number };
    readonly budget?: Budget;
    readonly checkpoint?: { readonly mode: string; readonly schemaVersion?: string };
  };
}

export interface RuntimeTarget {
  readonly class: string;
  readonly interface: "fenced.runtime.interface/v1" | string;
  readonly runtimeABI: string;
  readonly entrypoint: readonly string[];
}

export interface CapabilityGrant {
  readonly tools: readonly string[];
  readonly models: readonly string[];
  readonly memory: readonly string[];
  readonly secrets: readonly string[];
}

export interface Budget {
  readonly tokens?: number;
  readonly costUsd?: number;
  readonly toolCalls?: number;
  readonly wallSeconds?: number;
}

export interface CreateTaskRequest {
  readonly agentVersionRef: string;
  readonly goal: string;
  readonly namespace?: string;
  readonly spec?: JSONValue;
}

export interface Task {
  readonly id: string;
  readonly namespace: string;
  readonly agentVersionRef: string;
  readonly goal: string;
  readonly phase: string;
  readonly resourceVersion: number;
  readonly traceId?: string;
  readonly failureCode?: string;
  readonly usageSummary?: Readonly<Record<string, JSONValue>>;
}

export interface WorkflowSpec {
  readonly apiVersion: string;
  readonly kind: string;
  readonly steps: readonly WorkflowStepSpec[];
  readonly budget?: { readonly maxTasks?: number; readonly maxTokens?: number; readonly maxCostUsd?: number };
  readonly deadline?: string;
  readonly runtime?: Readonly<Record<string, JSONValue>>;
}

export interface WorkflowStepSpec {
  readonly name: string;
  readonly agentVersionRef: string;
  readonly goal: string;
  readonly dependsOn?: readonly string[];
  readonly condition?: string;
  readonly requiresApproval?: boolean;
  readonly maxAttempts?: number;
  readonly spec?: JSONValue;
}

export interface CreateWorkflowRequest {
  readonly namespace?: string;
  readonly goal: string;
  readonly workflow: WorkflowSpec;
}

export interface WorkflowStep {
  readonly name: string;
  readonly ordinal: number;
  readonly status: string;
  readonly attemptCount: number;
  readonly resourceVersion: number;
  readonly taskId?: string;
  readonly failureCode?: string;
  readonly decidedBy?: string;
  readonly approvalDecision?: "approved" | "rejected";
  readonly parentStepName?: string;
  readonly isDynamic?: boolean;
  readonly spawnDepth?: number;
  readonly spawnKey?: string;
}

export interface Workflow {
  readonly id: string;
  readonly namespace: string;
  readonly goal: string;
  readonly status: string;
  readonly resourceVersion: number;
  readonly steps: readonly WorkflowStep[];
  readonly traceId?: string;
  readonly failureCode?: string;
}

export interface StartRequest {
  readonly executionId: string;
  readonly agentVersionRef: string;
  readonly goal: string;
  readonly input: JSONValue;
  readonly capabilities: CapabilityGrant;
  readonly mcpEndpoint?: string;
  readonly [key: string]: JSONValue | CapabilityGrant | undefined;
}

export interface RuntimeEvent {
  readonly sequence: number;
  readonly type: string;
  readonly payload: JSONValue;
  readonly occurredAt?: string;
}

export interface RuntimeResult {
  readonly executionId: string;
  readonly status: "RUNNING" | "SUCCEEDED" | "FAILED" | "CANCELLED" | string;
  readonly output?: JSONValue;
  readonly error?: string;
}
