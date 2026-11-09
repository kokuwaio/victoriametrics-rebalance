# victoriametrics-rebalance: plan

## Goal

Add vmstorage nodes to a VictoriaMetrics cluster and end up with the data spread evenly across all of them, the way
the ECK operator moves Elasticsearch data to a new nodeset.

VictoriaMetrics doesn't rebalance existing data when a vmstorage node is added: vminsert sends new series to the new
node, but old data stays where it is until retention removes it. Moving data between nodes inside one cluster isn't
safe today, because metricIDs can collide across partitions (upstream issues
[#2324](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2324) and
[#10792](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10792)).

So this tool does a blue/green migration instead:

1. Create a second cluster ("green") with the target number of vmstorage nodes.
2. Copy the data from the old cluster ("blue") into green through vminsert, so it is hashed evenly across all green
   nodes.
3. Switch readers and writers to green, run a last sync, and delete blue.

### Non-goals

- Rebalancing in place (moving data between nodes of one cluster). That is blocked inside vmstorage.
- Freeing blue's disks during the migration. Blue is deleted as a whole at the end.
- Growing PVCs. That's a separate operation, which the operator already supports.
- Changing Kubernetes objects managed by GitOps. The tool moves and checks data. The operator objects (VMCluster,
  VMAgent, Services) are changed by the user, usually in Git. The tool says when each change is due and checks that it
  took effect.

## How a migration runs

```text
time ─────────────────────────────────────────────────────────────────────────────────────────▶
                     T0                                    Tc  Tcut
blue   ═══ reads + writes ═══════════════════════════════════════╪═ soak (no writes) ══ delete
green                ├─ dual write from vmagent ─────────────────┼═ all reads + writes ═══════▶
sync   [ backfill: now-retention ── T0+1h ][ catch-up ── Tc ]    [ final: Tc-1h ── Tcut+15m ]
                                                                  └ cache reset, verify, done
```

| Step | Who | What |
|---|---|---|
| 1. Prepare | user | Set up the stable aliases (see [Requirements](#requirements-on-the-setup)). Done once per setup, not per migration. |
| 2. Create green | user | New VMCluster with N+k vmstorage nodes, the same version, retention and replication factor, plus `dedup.minScrapeInterval=1ms`. |
| 3. Dual write | user | Add green as a second `remoteWrite` URL on the VMAgent(s). Green now receives most new data live. This start time is T0. |
| 4. `preflight` | tool | Checks capacity, flag parity, labels and the share of writes green receives (see [Checks](#checks)). |
| 5. `sync` (backfill) | tool | Copies `[now - retention, T0 + 1h]` from blue to green in resumable chunks. This takes hours to days. |
| 6. `verify` | tool | Compares blue and green per day. |
| 7. `sync` (catch-up) | tool | Copies from the last synced point to now. Repeat until one run takes only minutes. |
| 8. Cutover | user | Point the read and write aliases at green. This time is Tcut. |
| 9. `sync --final` | tool | Copies `[last end - 1h, Tcut + 15m]` (this is the "last sync"), resets green's rollup cache on every vmselect pod, verifies, and checks that blue no longer receives writes. |
| 10. Soak and delete | user | Keep blue for a few days so you can roll back, then delete blue and its PVCs. |

Why dual write *and* a final sync:

- Without dual write, every sample written between the last sync and cutover is missing on green until the final sync
  finishes. Dashboards and alerts would see a gap.
- Dual write through VMAgent covers the bulk of the data with no gap. The final sync covers everything that doesn't go
  through VMAgent (vmalert recording rules, OpenTelemetry collectors, jobs that push to `/api/v1/import`) and fills
  any samples green dropped during dual write.
- Overlapping windows are safe because green runs with `-dedup.minScrapeInterval=1ms`, which drops samples that share
  an exact timestamp. Without it, overlaps would be stored twice and would skew `count_over_time` and similar
  functions.

Rollback: until step 10, switch the aliases back to blue. Blue still has everything up to Tcut. Data written to green
after Tcut can be synced back with the same tool, swapping `--from` and `--to`.

## Requirements on the setup

The tool checks these where it can, but some must be in place before the first migration:

1. **Stable endpoints for readers and writers.** The operator names Services after the VMCluster (`vmselect-<name>`,
   `vminsert-<name>`), and green has a different name. Clients that use those names directly can't be switched in one
   step. Add aliases that you control:
   - plain Services such as `vmselect-active` and `vminsert-active`, selecting
     `app.kubernetes.io/name: vmselect|vminsert` and `app.kubernetes.io/instance: <active cluster>`, or
   - a VMAuth in front (VMUser `targetRefs` then point at the alias or the active cluster).

   Cutover then means changing the alias's selector or target, not every client.
2. **Know every writer.** Writers through VMAgent get dual write. Every other writer (vmalert `remoteWrite`, OTLP
   senders, push jobs) is covered only by the final sync, so keep the time between catch-up and cutover short. Or
   route them through VMAgent, which also accepts `/api/v1/write`, `/api/v1/import/*` and `/opentelemetry/v1/metrics`.
3. **Green must not block blue.** With `remoteWrite.disableOnDiskQueue`, a slow remote-write target can stall the
   writes to every target. Set `remoteWrite.dropSamplesOnOverload` for the green URL (verify that the flag can be set
   per URL in the deployed version), or enable the on-disk queue for the migration. Samples dropped there are filled
   in by the final sync.
4. **Pod labels must not overlap.** If blue and green pods share the labels used in `topologySpreadConstraints` or
   PodDisruptionBudgets (for example a custom `app: vmstorage`), green pods are counted together with blue pods.
   Pods can then fail to schedule, or an eviction can be blocked. Use `app.kubernetes.io/instance` in those
   selectors.
5. **Capacity for a full second copy.** Green needs at least blue's used disk space plus headroom, as well as its
   own vmstorage memory and CPU, until blue is deleted.
6. **Name-agnostic monitoring.** Dashboards, alerts and SLOs that match `job="vmstorage-<name>"` exactly lose green.
   Use regexes or the instance label.

## Tool design

Written in Go: one CLI built with Cobra, which runs locally (via kubeconfig) or as a Kubernetes Job. It reads the
VMCluster objects through `github.com/VictoriaMetrics/operator/api` and talks to vmselect, vminsert and vmstorage over
HTTP.

### Commands

```text
vmrebalance preflight --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance sync      --namespace monitoring --from vmcluster --to vmcluster-b [--until now|<RFC3339>] [--final]
                      [--step 24h] [--concurrency 2] [--rate-limit 50MiB/s]
vmrebalance verify    --namespace monitoring --from vmcluster --to vmcluster-b [--since ...] [--until ...]
vmrebalance status    --namespace monitoring --from vmcluster --to vmcluster-b
```

- `sync` splits its time range into chunks (default: one day). It records each finished chunk in the state, so a run
  restarted after a crash or pod eviction continues where it stopped. The first run starts at `now - retention`.
  Later runs continue from the last finished chunk. `--final` also resets the rollup cache and runs the post-cutover
  checks.
- `verify` is read-only and can run at any time.
- The tool never deletes data and never modifies VMCluster, VMAgent or Service objects.

### State

One ConfigMap per migration (`vmrebalance-<from>-<to>`) holding: T0, the backfill start, finished chunks (start,
end, bytes, duration), verification results per day, and the cutover and final-sync times. This needs Kubernetes API
access anyway (to find pods and read the VMCluster objects), so a ConfigMap costs nothing extra and survives Job
restarts.

### Data copy engine

Copy each chunk with `GET /api/v1/export/native` on blue's vmselect, streamed into `POST /api/v1/import/native` on
green's vminsert.

- **Option A (MVP): wrap `vmctl vm-native`.** Run it once per chunk with `--vm-native-filter-time-start/-end`,
  `--vm-concurrency` and `--vm-rate-limit`. It's proven, splits the work per metric name, and retries. The tool image
  ships the vmctl binary.
- **Option B: stream directly in Go.** About a hundred lines, with no dependency on the vmctl binary or its flags, and
  per-metric-name checkpoints become possible. Worth it only if Option A's per-chunk calls turn out too coarse or too
  slow.

Start with A. The engine sits behind a small interface (`Copy(ctx, window) error`), so it can be swapped for B.

The native format is version-coupled, so `preflight` requires the same VictoriaMetrics version on blue and green.

### Checks

`preflight`:

- Both VMClusters exist and are operational. Green has more vmstorage replicas than blue (otherwise there's nothing to
  rebalance: warn).
- Same version, `retentionPeriod` and `replicationFactor`. Green has `dedup.minScrapeInterval` set.
- Capacity: green's total PVC size is at least blue's used bytes × (1 + headroom). Used bytes come from
  `vm_data_size_bytes` and `vm_free_disk_space_bytes` on each blue vmstorage pod, read via the API server's pod proxy.
  This doesn't depend on self-monitoring.
- Dual write is active: green's vminsert ingestion rate is above 0. The ratio of green to blue ingestion shows the
  share of writes that bypass VMAgent and will rely on the final sync.
- Green's pod labels don't match blue's `topologySpreadConstraints` or PDB selectors.

`verify`, per day:

- Series count: `/api/v1/status/tsdb?date=<day>` (`totalSeries`) on blue and green. This is cheap: it reads the
  per-day index.
- Sample count for a sample of metrics: `sum(count_over_time(<metric>[1d]))` for the top-N and a random set of metric
  names from the same TSDB status. Counting every series is too expensive on large clusters.
- The result is pass/fail per day with the differences, and is stored in the state.

`sync --final`:

- Rollup cache reset (`/internal/resetRollupResultCache`) on every green vmselect pod, found by label. Without it,
  green can serve cached results computed before the backfilled data arrived.
- Blue's vminsert ingestion rate (excluding the tool's own export reads) drops to about 0 within N minutes after
  cutover. If it doesn't, some writer still points at blue.
- `verify` for the days the final sync touched.

## Milestones

1. **Manual run-through.** Go through the steps by hand on a test cluster, with `vmctl` run directly. Measure: GB/h
   copied, CPU and I/O load on blue's vmstorage and vmselect, disk growth on green, and the gap the final sync has to
   close. This decides the default chunk size, concurrency and rate limit, and whether the approach is fast enough at
   all.
2. **MVP CLI.** `preflight`, `sync` with the vmctl engine, the ConfigMap state, and `status`. Unit tests with fake
   HTTP servers. An end-to-end test on kind with two small VMClusters.
3. **Verify and final.** `verify`, `sync --final` (cache reset and the blue drain check), Job manifests and a
   container image.
4. **Hardening.** Retries with backoff per chunk, rate limits that adapt to blue's load (back off when blue's vmselect
   latency or concurrency limits are hit), and Prometheus metrics for the tool itself.
5. **Optional.** `--apply` mode for setups without GitOps (patch VMAgent `remoteWrite` and the alias Services
   directly), the Go copy engine (Option B), and a proposal to the operator project once this has run in production.

## Open decisions

- **vmctl engine vs. a Go stream** for the MVP. Recommendation: vmctl (see above).
- **Chunk size**: per day is the default. Large clusters may need per hour or per metric-name shard. Decide after
  milestone 1.
- **Should the tool create green?** For now, no: the user's GitOps repo owns the objects. Revisit for `--apply`.
- **Multi-tenancy**: the MVP handles one tenant path (`/select/<id>/prometheus`). Copying all tenants (vmctl's
  `--vm-intercluster`) comes later, when someone needs it.

## Risks

- **Copy speed.** A year of retention at several TiB may take days. The final sync must stay short, so the catch-up
  runs are what keep the cutover window small.
- **Load on blue.** Export reads go through blue's vmselect and vmstorage and compete with real queries. Rate limits
  and off-peak scheduling are needed. Milestone 1 measures the impact.
- **Cost.** A full second copy of disks, plus vmstorage memory and CPU, for the duration of the migration.
- **Missed writers.** Anything that bypasses both the aliases and VMAgent writes to blue only and is lost after blue is
  deleted. The drain check after cutover is what catches it.
