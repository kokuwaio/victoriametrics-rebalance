# victoriametrics-rebalance

`vmrebalance` moves the data of a VictoriaMetrics cluster to a new cluster with more vmstorage nodes, so that the
data ends up spread evenly across all of them.

VictoriaMetrics doesn't rebalance existing data when you add a vmstorage node, and moving data between the nodes of
one cluster isn't safe. So `vmrebalance` does a blue/green migration: you create a second VMCluster ("green") with
the target number of vmstorage nodes, the tool copies the data from the old one ("blue") through green's vminsert,
and you switch readers and writers over. [PLAN.md](PLAN.md) explains the design and its trade-offs.

The tool never deletes data and never changes VMCluster, VMAgent or Service objects. You make those changes,
usually in Git. The tool copies the data, checks every step and tells you what's due next.

## How a migration runs

| Step | Who | What |
|---|---|---|
| 1. Prepare | you | Put stable aliases (Services or VMAuth) in front of vmselect and vminsert. See [Requirements](#requirements). |
| 2. Create green | you | A new VMCluster with more vmstorage nodes, the same version, retention and replication factor, plus `dedup.minScrapeInterval: 1ms` on vmstorage and vmselect. |
| 3. Dual write | you | Add green as a second `remoteWrite` URL on your VMAgent(s). |
| 4. `preflight` | tool | Checks versions, flags, capacity, dual write and pod labels. |
| 5. `sync` | tool | Backfill: copies `[now - retention, now]` in resumable chunks. This can take hours to days. |
| 6. `verify` | tool | Compares blue and green per day. |
| 7. `sync` | tool | Catch-up: copies from the last synced point to now. Repeat until a run takes only minutes. |
| 8. Cutover | you | Point the read and write aliases at green. |
| 9. `sync --final` | tool | Copies `[last synced point - 1h, cutover + 15m]`, resets green's rollup cache, checks that blue no longer receives writes, and verifies the copied days. |
| 10. Delete blue | you | Keep blue for a few days so you can roll back, then delete it and its PVCs. |

Rollback until step 10: switch the aliases back to blue. To copy data written to green after the cutover back to
blue, run `sync` with `--from` and `--to` swapped.

## Usage

```sh
vmrebalance preflight --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance sync      --namespace monitoring --from vmcluster --to vmcluster-b --rate-limit 50MiB/s
vmrebalance verify    --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance status    --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance sync      --namespace monitoring --from vmcluster --to vmcluster-b --final --cutover-at 2026-01-02T10:00:00Z
```

Run `vmrebalance <command> --help` for all flags. The most important ones:

- `sync --step 24h`: the chunk size. Every finished chunk is recorded, so a restarted run continues with the first
  unfinished one.
- `sync --rate-limit 50MiB/s --concurrency 2`: the export reads go through blue's vmselect and vmstorage and compete
  with real queries. Start low and watch blue's load.
- `--state-file state.json`: keep the state in a local file instead of the ConfigMap `vmrebalance-<from>-<to>`.
  This is fine on a workstation. A Job needs the ConfigMap, because its pod loses local files when it restarts.
- `sync --final --cutover-at <time>`: the time you switched the aliases. The final window and the drain check are
  measured from it. `--drain-interval` (default 5m) must be longer than the push interval of your slowest writer:
  blue passes only when it receives no rows at all during one whole interval.
- `--access proxy|direct`: how the tool reaches vmselect and the pods. `proxy` goes through the Kubernetes API
  server and works from any workstation, but the API server cuts requests off after its request timeout (1 minute
  by default), which `verify` can hit on large clusters. `direct` uses Service DNS names and pod IPs. The default
  (`auto`) picks `direct` inside a pod, `proxy` otherwise.

### Where to run it

`sync` runs [vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/) once per chunk. vmctl needs network
access to blue's vmselect and green's vminsert, so run `sync` as a Job in the cluster:
[deploy/rbac.yaml](deploy/rbac.yaml) holds the permissions, [deploy/job.yaml](deploy/job.yaml) is an example Job.
No image is published yet, so build one from the [Dockerfile](Dockerfile), which bundles vmctl.

From a workstation, port-forward both Services and pass `--from-select-url http://localhost:8481` and
`--to-insert-url http://localhost:8480`, with `vmctl` in your `PATH`. `preflight`, `verify` and `status` work
from a workstation without port-forwarding.

## Requirements

1. **Stable endpoints for readers and writers.** The operator names Services after the VMCluster
   (`vmselect-<name>`, `vminsert-<name>`), so clients that use those names can't be switched in one step. Add
   aliases you control, for example plain Services that select `app.kubernetes.io/name` and
   `app.kubernetes.io/instance`, or a VMAuth in front.
2. **Know every writer.** Writers that go through VMAgent get dual write. Every other writer (vmalert remote write,
   OpenTelemetry senders, jobs that push to `/api/v1/import`) is covered only by the final sync, so keep the time
   between the last catch-up and the cutover short, or route them through VMAgent.
3. **Green must not block blue.** With `remoteWrite.disableOnDiskQueue`, a slow target can stall writes to all
   targets. Set `remoteWrite.dropSamplesOnOverload` for green, or enable the on-disk queue for the migration.
4. **Pod labels must not overlap.** PodDisruptionBudgets and `topologySpreadConstraints` that select pods of both
   clusters count them together. Select on `app.kubernetes.io/instance` too. `preflight` checks this.
5. **Capacity for a full second copy** of the data, plus green's own memory and CPU, until blue is deleted.
6. **Name-agnostic monitoring.** Dashboards and alerts that match `job="vmstorage-<name>"` exactly lose green.

## Checks

- `preflight`:
  - both clusters are operational, and green has more vmstorage nodes;
  - all components run the same image tags (the native format is version-coupled);
  - same retention and replication factor;
  - green deduplicates, with exactly `1ms` (a larger value also merges real samples, so it warns);
  - green's vmstorage nodes have free space for blue's used bytes plus headroom;
  - green receives live writes, and which share of blue's;
  - no PodDisruptionBudget or topology spread selector matches pods of both clusters.
- `verify` only reads from the clusters, so it can run at any time, also during a sync. Per UTC day it compares the
  series count from `/api/v1/status/tsdb` and `count_over_time` sample counts for the top N metric names plus a
  random set. Green passes with at least as many series and samples as blue, minus a tolerance (default 1%).
- `sync --final`: rollup cache reset on every green vmselect pod; blue's vminsert receives no rows during one
  `--drain-interval` within `--drain-timeout`; `verify` for the days the final sync copied.

## Development

```sh
go test ./...
./test/e2e/run.sh
```

The end-to-end test creates a kind cluster with its own kubeconfig (your current context stays untouched),
installs the operator, two small VMClusters and a VMAgent writing to both, and runs a full migration. Set
`KEEP_CLUSTER=1` to keep the cluster for another run.
