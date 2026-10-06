# Fenced SDK for TypeScript

Typed, dependency-free clients for Node.js 20+ and modern browsers. SDK `1.x`
supports the stable Fenced Control API and `fenced.runtime.interface/v1`.

The package includes manifest, budget, capability, task, workflow, dynamic-step,
runtime event, and result types. Mutating operations expose idempotency keys and
resource-version ETags instead of hiding the platform's concurrency contract.

```ts
import { ControlClient, type WorkflowSpec } from "@fenced/sdk";

const client = new ControlClient("https://control.example.com", {
  token: () => obtainShortLivedToken(),
});

const workflow: WorkflowSpec = {
  apiVersion: "fenced.dev/workflow/v1",
  kind: "Workflow",
  steps: [{
    name: "research",
    agentVersionRef: "researcher@1.0.0",
    goal: "Research the requested topic",
  }],
};

const created = await client.createWorkflow(
  { namespace: "default", goal: "Prepare a brief", workflow },
  crypto.randomUUID(),
);

await client.cancelWorkflow(created.value.id, created.value.resourceVersion);
```

Runtime adapters and conformance tooling can use `RuntimeClient` for health,
start, stop, event, result, checkpoint, and restore calls. Agent processes must
not receive model-provider credentials; use the execution-scoped MCP endpoint
in the Runtime Interface start request.

Build and test locally:

```bash
cd sdk/typescript
npm ci
npm test
```
