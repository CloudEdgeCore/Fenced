# Fenced Docker Runtime Adapter

An official reference runtime adapter that runs Agent tasks inside isolated Docker containers while conforming to `fenced.runtime.interface/v1`.

## Features
- Container-level task isolation
- State checkpointing and volume snapshot restore
- Event streaming via Server-Sent Events (SSE) and HTTP polling
- Cooperative cancellation signaling

## Running & Conformance Testing

```bash
# 1. Start Docker Runtime on port 8089
go run examples/runtimes/docker-runtime/main.go --port 8089

# 2. Run Fenced Conformance Certification
fenced conformance -endpoint http://127.0.0.1:8089
```

Certification result:
```text
Fenced Compatible = PASS
```
