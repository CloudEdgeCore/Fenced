# Migrating a deployment from AgentOS to Fenced

This directory is a worked example of moving a running deployment across the
AgentOS → Fenced rename. It is deliberately small: two deployment shapes
(systemd, Docker Compose) shown **before** and **after**, plus the environment
files that go with them.

Read [`docs/COMPATIBILITY.md`](../../docs/COMPATIBILITY.md) first — it states
the full boundary of what the compatibility layer can and cannot absorb. The
short version:

| Changed | Absorbed automatically? |
| --- | --- |
| `AGENTOS_*` environment variables | **Yes** — the binary aliases them to `FENCED_*` at start-up |
| `X-Agentos-Execution` request header | **Yes** — the MCP server falls back to it |
| `agentos.*` identifiers in stored manifests | **Yes** — normalised on decode |
| gRPC full method names / protobuf type URLs | **No** — both ends must be rebuilt together |
| Signed manifest digests | **No** — re-sign with `scripts/resign-manifests.sh` |

## Layout

```
examples/migration/
├── README.md                          # this file
├── systemd/
│   ├── agentos-control.service        # BEFORE
│   └── fenced-control.service         # AFTER
├── env/
│   ├── agentos.env                    # BEFORE
│   └── fenced.env                     # AFTER
└── compose/
    ├── docker-compose.agentos.yml     # BEFORE
    └── docker-compose.fenced.yml      # AFTER
```

Every `*.agentos.*` / `agentos-*` file is kept only as the migration source of
truth. Do not deploy from it.

## The ordering trick

The compatibility layer lets you migrate the **configuration** and the
**binary** independently. Pick one of two orders and the deployment stays up
across the change:

1. **Config first** — rename `AGENTOS_*` → `FENCED_*` in the environment file,
   restart nothing yet. The old binary ignores `FENCED_*`… so do **not** do
   this. This order requires the new binary.
2. **Binary first (recommended)** — upgrade the binary. It aliases the legacy
   `AGENTOS_*` names at start-up and prints one deprecation notice, so the old
   environment file keeps working. Then rename the environment file on your own
   schedule and remove the notice.

Because of that, the safe order is always **binary, then config, then
identifiers**. The example below follows it.

## 1. Systemd

The unit file is renamed, but the environment file does not have to be renamed
at the same time.

```bash
# 1a. Install the new binary next to the old one, then swap the unit.
sudo systemctl disable --now agentos-control
sudo install -m 0755 bin/fenced-control /opt/fenced/bin/fenced-control
sudo cp examples/migration/systemd/fenced-control.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fenced-control

# 1b. The service now runs with the OLD /etc/agentos/control.env. The binary
#     aliases AGENTOS_* -> FENCED_* and logs one deprecation line. Confirm:
sudo journalctl -u fenced-control --since "5 min ago" | grep "deprecated AGENTOS_"

# 1c. Migrate the environment file when convenient.
sudo install -d -m 0750 /etc/fenced
sudo sed -e 's/^AGENTOS_/FENCED_/' \
         -e 's/^\(User\|Group\)=agentos/\1=fenced/' \
         /etc/agentos/control.env > /tmp/fenced.env
sudo install -m 0640 -o root -g fenced /tmp/fenced.env /etc/fenced/control.env
sudo sed -i 's#/etc/agentos/control.env#/etc/fenced/control.env#' /etc/systemd/system/fenced-control.service
sudo systemctl daemon-reload && sudo systemctl restart fenced-control

# 1d. The deprecation line is now gone; drop the old unit and directory.
sudo rm -f /etc/systemd/system/agentos-control.service
sudo rm -rf /etc/agentos
```

Note that `DATABASE_URL` has no prefix and is **not** rewritten by the `sed`
above — it is unchanged by the rename.

The same rewrite is available as a script, which also covers `AgentOS`/`Agentos`
in unit descriptions and paths. It rewrites file **contents** — it does not
rename files — so rename the unit yourself first:

```bash
sudo mv /etc/systemd/system/agentos-control.service /etc/systemd/system/fenced-control.service
scripts/migrate-agentos-to-fenced.sh --dry-run /etc/systemd/system
scripts/migrate-agentos-to-fenced.sh /etc/systemd/system
```

## 2. Docker Compose

```bash
# 2a. Bring the new stack up; the old one keeps serving until you cut over.
docker compose -f examples/migration/compose/docker-compose.fenced.yml up -d --wait

# 2b. Confirm the aliased variables, then stop the old stack.
docker compose -f examples/migration/compose/docker-compose.fenced.yml logs control | grep "deprecated AGENTOS_"
docker compose -f examples/migration/compose/docker-compose.agentos.yml down
```

The `fenced` compose file keeps the legacy `AGENTOS_*` names in a commented
block so you can see exactly which variables moved.

## 3. Identifiers and signatures

Environment variables and headers are the easy half. Two things the
compatibility layer cannot absorb:

* **gRPC wire format.** Service names and type URLs carry the protobuf package,
  so a client built before the rename cannot call a server built after it.
  Rebuild and redeploy both ends from the same `proto/` revision — see
  `scripts/rebuild-and-verify.sh`.
* **Signed manifests.** Renormalising the canonical JSON changes its SHA-256,
  which invalidates every existing signature. Re-sign with
  `scripts/resign-manifests.sh`; manifests still carrying `apiVersion:
  agentos.dev/v1` are normalised on decode and signed as `fenced.dev/v1`.

## 4. Rollback

Everything here is reversible until step 3 runs. If the new stack misbehaves,
`systemctl disable --now fenced-control && systemctl enable --now agentos-control`
(or `docker compose -f …agentos.yml up -d`) restores the previous state, because
the old binary and the old config are untouched.

Once you have re-signed manifests and rebuilt both ends, rollback means
redeploying the previous release, not just restarting a unit.
