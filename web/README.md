# Web / Dashboard

Status: **the operator console exists** — it lives in
[`internal/control/console`](../internal/control/console) and the control
plane serves it at `GET /console`.

The console is a single static document: the design system, the three themes
and the bilingual strings, plus a small client that reads the control plane's
own endpoints with the operator's bearer token. It holds no state of its own,
loads no external asset, and invents no number - every value it shows is the
value the kernel holds.

A full multi-tenant management product (TypeScript + React, with its own
build) is still planned; this directory is reserved for it.
