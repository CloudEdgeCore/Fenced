# Fenced Python Runtime Adapter

An official reference runtime adapter written in Python that conforms to `fenced.runtime.interface/v1`.

## Features
- Pure Python execution environment
- Built-in multi-threaded request processing
- Native checkpointing & state restoration
- Certified with Fenced Conformance Suite

## Running & Conformance Testing

```bash
# 1. Start Python Runtime on port 8087
python server.py --port 8087

# 2. Run Fenced Conformance Certification
fenced conformance -endpoint http://127.0.0.1:8087
```

Certification result:
```text
Fenced Compatible = PASS
```
