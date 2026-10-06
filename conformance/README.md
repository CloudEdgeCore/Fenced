# Fenced Conformance Certification Kit

The Fenced Conformance Suite is the authoritative, black-box verification kit certifying that an agent runtime or framework adapter complies with the **Fenced Runtime Interface v1** specification.

---

## 1. Overview

Any framework adapter (LangGraph, AutoGen, CrewAI, OpenAI Agents SDK, custom proprietary runtime) that passes this suite provides guaranteed compatibility with the Fenced Kernel, including:
- **Durable Lifecycle**: Idempotent execution startup, non-preemptible execution, cancellation signaling.
- **Durable Checkpointing & Recovery**: State capture and resumption across worker failures.
- **Audit & Observability**: Real-time event streaming and cursor pagination.
- **Fail-Closed Security**: Default-deny capability enforcement and unauthorized resource rejection.

---

## 2. Running Certification

### 2.1 Single-Command Verification (Live Endpoint)

If your adapter server is already running (e.g. on port `8088`):

```bash
fenced-conformance -endpoint http://127.0.0.1:8088
```

Or via the unified CLI:

```bash
fenced conformance -endpoint http://127.0.0.1:8088
```

### 2.2 Auto-Spawning Candidate Servers (`-cmd`)

You can provide the command to start your adapter. `fenced-conformance` will launch the server, discover the listening port, run all checks, and cleanly shut down the server:

```bash
fenced-conformance -cmd "python examples/agents/langgraph/server.py --port 0"
```

### 2.3 Machine-Readable JSON Export (`-json`)

```bash
fenced-conformance -endpoint http://127.0.0.1:8088 -json
```

### 2.4 Automated GitHub Actions CI Integration & Badge

Third-party framework authors can automatically certify on every pull request using the official composite action:

```yaml
# In your repository: .github/workflows/conformance.yml
name: Fenced Compatibility
on: [push, pull_request]

jobs:
  certify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Certify against Fenced
        uses: CloudEdgeCore/Fenced/.github/actions/conformance@v1.3.0
        with:
          command: "python server.py --port 0"
```

Once certified, display the official **Fenced Compatible** badge in your README:

```markdown
[![Fenced Compatible](https://img.shields.io/badge/Fenced-Compatible-brightgreen.svg)](https://github.com/CloudEdgeCore/Fenced)
```

---

## 3. Certification Report Format

A successful conformance run outputs:

```text
=== Fenced Runtime Interface Conformance Suite ===
Adapter:  langgraph
Protocol: fenced.runtime.interface/v1
Endpoint: http://127.0.0.1:8089

Checks executed:
  [PASS] health
  [PASS] protocol-negotiation
  [PASS] start
  [PASS] idempotency
  [PASS] conflict
  [PASS] event
  [PASS] event-cursor
  [PASS] result
  [PASS] checkpoint
  [PASS] restore
  [PASS] stop
  [PASS] default-deny-capabilities
--------------------------------------------------
Fenced Compatible = PASS
--------------------------------------------------
```

---

## 4. Supported Framework Ecosystem

The repository provides verified adapters and reference servers for:

1. **LangGraph**: `adapters/langgraph/fenced_langgraph.py` & `examples/agents/langgraph/server.py`
2. **AutoGen**: `adapters/autogen/fenced_autogen.py` & `examples/agents/autogen/server.py`
3. **CrewAI**: `adapters/crewai/fenced_crewai.py` & `examples/agents/crewai/server.py`
4. **OpenAI Agents SDK**: `adapters/openai_agents/fenced_openai_agents.py` & `examples/agents/openai_agents/server.py`
5. **Custom Enterprise Agent**: `adapters/custom_agent/fenced_custom.py` & `examples/agents/custom/server.py`
