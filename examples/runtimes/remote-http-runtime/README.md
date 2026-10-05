# Fenced Remote HTTP Runtime Adapter

An official reference runtime adapter that proxies task execution, event streaming, and checkpoints to remote HTTP agent microservices over `fenced.runtime.interface/v1`.

## Features
- Connects existing microservices and webhooks to Fenced
- Supports upstream forwarding with automatic JSON marshalling
- Checkpoint persistence and graceful failover
- Certified with Fenced Conformance Suite

## Running & Conformance Testing

```bash
# 1. Start Remote HTTP Runtime on port 8086
go run examples/runtimes/remote-http-runtime/main.go --port 8086

# 2. Run Fenced Conformance Certification
fenced conformance -endpoint http://127.0.0.1:8086
```

Certification result:
```text
Fenced Compatible = PASS
```
