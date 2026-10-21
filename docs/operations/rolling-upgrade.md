# Rolling Upgrade and Automatic Rollback

This runbook is for the operator who moves a supervised service to another
agent version, and for the operator who upgrades the control plane itself.

Two different things are called "rolling upgrade" here. The **service
rollout** changes the agent version a supervised service runs; the
**control plane upgrade** changes the control plane binary. Both are
described here because they share one rule: *the old one leaves only after
the new one serves.*

---

## 1. The service rollout

### What the kernel guarantees

| Guarantee | How it is enforced |
| :--- | :--- |
| A serving instance is never replaced by one that has not proven itself | The reconciler drains an outdated instance only when a replacement of the target version has been observed healthy for `rollout.minReadyTime` (`readyAt` on the instance) |
| The rollout does not block on a broken version | A target version whose instance fails ends the rollout; the failure reason is recorded in the service status |
| The service returns to a working version | With `rollout.autoRollback` the target version is reverted and the reconciler rolls the service back |
| The rollout survives a controller restart | The rollout state lives in the durable service status, not in memory |
| One movement at a time | A second rollout while one runs is refused with `ROLLOUT_IN_PROGRESS` |

### Starting a rollout

```bash
curl -sS -X POST "$FENCED_CONTROL_URL/v1/services/$SERVICE_ID/rollout" \
  -H "Authorization: Bearer $FENCED_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
        "newVersion": "summarizer@2.0.0",
        "config": {
          "maxSurge": 1,
          "maxUnavailable": 1,
          "minReadyTime": 30000000000,
          "autoRollback": true
        }
      }'
```

The response is the service with its rollout state. The configuration is
optional: without it the rollout settings of the service are used. Durations
are nanoseconds, like every duration in the Control API.

The frozen service contract carries the same operation
(`ServiceManagementService.RolloutUpgrade`) and the same configuration
(`RolloutConfig.max_surge`, `max_unavailable`, `min_ready_seconds`,
`auto_rollback`).

### Reading the state

```bash
curl -sS "$FENCED_CONTROL_URL/v1/services/$SERVICE_ID/rollout" \
  -H "Authorization: Bearer $FENCED_TOKEN"
```

| Phase | Meaning |
| :--- | :--- |
| `PROGRESSING` | The service is moving to the target version |
| `SUCCEEDED` | Every replica runs the target version and is healthy |
| `FAILED` | The rollout failed and the service kept the target version |
| `ROLLING_BACK` | The service is returning to the version it ran before |
| `ROLLED_BACK` | The service is back on its previous version |

The service status carries the same fields, plus `failureReason` and a
message that explains the movement.

### When a rollout fails

With `autoRollback` the kernel reverts the target version by itself. The
service comes back on the previous version; the reason stays in the status:

```json
{
  "rolloutPhase": "ROLLED_BACK",
  "rolloutFailureReason": "instance svc-…-a91c failed: task FAILED: the agent crashed on startup",
  "message": "Rolled back to summarizer@1.0.0"
}
```

Without `autoRollback` the service stays on the failed version and the
rollout is marked `FAILED`. The operator decides: roll back by hand
(`POST /v1/services/{id}/rollback`) or fix the version and roll out again.

> A rollback needs the previous version to be launchable. If the previous
> version was withdrawn, the rollout fails with that reason and the operator
> has to intervene.

### The rules the kernel follows

- `maxSurge` (default 1): how many replicas may exist above the desired
  count. A surge is what keeps the service available during the rollout.
- `maxUnavailable` (default 1): how many replicas may be unavailable. The
  kernel never drops below one serving replica.
- `minReadyTime` (default 0): how long a replacement has to stay healthy
  before it may take the place of the instance it replaces. Zero means "as
  soon as it reports in".
- `autoRollback` (default false): return to the previous version when the
  rollout fails.

---

## 2. The control plane upgrade

The Helm chart upgrades the control plane without downtime:

| Setting | Default | Why |
| :--- | :--- | :--- |
| `upgrade.maxUnavailable` | 0 | The old replica is only taken away when the new one serves |
| `upgrade.maxSurge` | 1 | The new replica joins before the old one leaves |
| `upgrade.minReadySeconds` | 10 | A replica counts as available only after it stayed ready |
| `upgrade.progressDeadlineSeconds` | 600 | The rollout fails instead of waiting forever |
| `upgrade.terminationGracePeriodSeconds` | 30 | The replica may finish its work; the control plane drains on SIGTERM |
| `upgrade.podDisruptionBudget.minAvailable` | 1 | A node drain may not take the last replica down |
| `upgrade.preStopSleepSeconds` | 0 | Optional: delay between the removal from the Service and the SIGTERM. Needs a cluster with the Sleep lifecycle handler (Kubernetes 1.29 or newer) |

```bash
helm upgrade fenced deploy/helm/fenced \
  --set image.tag=1.5.0 \
  --wait --timeout 15m
```

The schema of the control plane is migrated forward before the new replica
starts (`fenced-migrate`). Migrations are forward-only and additive, so the
old replica keeps working against the new schema.

---

## 3. What this does not cover

- **Weighted traffic.** The traffic follows the version binding: a task that
  was admitted against one version talks to that version, so a rollout
  splits the traffic by the versions the senders are bound to. There is no
  weight: the share of the traffic that a version receives is the share of
  the tasks that were admitted against it.
- **Database rollback.** A rollout reverts the agent version, not the
  schema. The schema only moves forward.
- **Multi-cluster.** A rollout moves the replicas of one service in one
  control plane.
