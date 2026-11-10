# Architecture

`vmrebalance` is one Go binary. `migrate` runs a whole migration, usually as a Kubernetes Job from the Helm chart.
`preflight`, `verify` and `status` look at a migration from a workstation, and `sync` copies a time range outside a
migration (for example back to blue after a rollback). It reads the two VMCluster objects through the Kubernetes API,
talks to vmselect, vminsert and the pods over HTTP, and copies data by running
[vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/) once per chunk.

It never changes VMCluster, VMAgent or Service objects and never deletes data. The only object it writes is its
state ConfigMap. The [README](../README.md) describes a migration from the operator's side; this page describes how
the tool works and why it is built this way.

## Design decisions

- **Blue/green instead of rebalancing in place.** vmstorage can't safely take over another node's partitions,
  because metricIDs can collide across partitions
  ([#2324](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2324),
  [#10792](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10792)). Writing everything again through
  green's vminsert lets VictoriaMetrics itself spread the series across the new set of nodes.
- **The tool doesn't change Kubernetes objects.** In most setups a GitOps repository owns the VMCluster, VMAgent and
  Service objects, and would revert changes made in the cluster. So creating green, enabling dual write, the cutover
  and deleting blue stay your changes. The tool says when each change is due and checks that it took effect;
  `migrate` waits for them and notices the cutover by itself.
- **vmctl as the copy engine.** Each chunk is one `vmctl vm-native` run: `GET /api/v1/export/native` on blue's
  vmselect, streamed into `POST /api/v1/import/native` on green's vminsert, one request per hour of the chunk,
  spread over `--concurrency` workers. The tool turns off vmctl's per-metric mode: that mode first lists metric names
  through `/api/v1/label/__name__/values`, which vmselect may answer partially when a vmstorage node is down, and
  vmctl would then skip the missing names and still succeed. A native export fails instead of returning partial
  data. vmctl doesn't retry requests in this mode, so the tool retries the whole chunk. vmctl applies its rate limit
  per request, so the tool divides `--rate-limit` by the number of requests that run at once. A copy engine written
  in Go would remove the vmctl binary and could change the rate within a chunk. It sits behind the same
  `migration.Copier` interface, so it can replace vmctl without other changes, if chunks turn out too coarse or too
  slow.
- **Same version on blue and green.** The native format is version-coupled, so `preflight` compares the image tags
  of all running components.
- **Dual write plus a final sync, with deduplication.** Dual write keeps green current without a gap, and the final
  sync covers writers that bypass VMAgent. The windows overlap on purpose (`[last sync - --final-overlap, blue
  drained]`, 1h by default). That is safe only because green runs with `-dedup.minScrapeInterval=1ms`, which
  `preflight` checks.
- **Time-ordered chunks with a high-water mark.** Chunks run oldest first and one at a time, so the state needs only
  the end of the last finished chunk to resume. Backfill and catch-up are the same operation: copy from the
  high-water mark to now.
- **State in a ConfigMap.** The tool needs Kubernetes API access anyway (to find pods and read the VMClusters), so
  a ConfigMap costs nothing extra and survives restarts of a Job's pod.
- **One tenant per run.** The tool copies the tenant in `--tenant` (default `0`), through
  `/select/<tenant>/prometheus` on blue and `/insert/<tenant>/prometheus` on green. Copying all tenants at once
  (vmctl's `--vm-intercluster`) isn't supported; run one migration per tenant.

## System context

```mermaid
flowchart LR
    user(["You, usually via Git"])

    subgraph k8s["Kubernetes cluster"]
        api["API server"]
        cm[("ConfigMap<br/>vmrebalance.&lt;from&gt;.&lt;to&gt;")]
        agent["VMAgent"]

        subgraph blue["blue VMCluster (old)"]
            bsel["vmselect"]
            bins["vminsert"]
            bsto["vmstorage × N"]
        end

        subgraph green["green VMCluster (new)"]
            gsel["vmselect"]
            gins["vminsert"]
            gsto["vmstorage × N+k"]
        end

        subgraph tool["vmrebalance (CLI or Job)"]
            cli["vmrebalance"]
            vmctl["vmctl<br/>(child process)"]
        end
    end

    user -- "create green, enable dual write,<br/>switch aliases, delete blue" --> api
    agent -- "remote write (dual write)" --> bins
    agent -- "remote write (dual write)" --> gins
    bins --> bsto
    gins -- "hashes series across<br/>all nodes" --> gsto
    bsel --> bsto
    gsel --> gsto

    cli -- "get VMClusters, list pods and PDBs,<br/>pod and service proxy" --> api
    cli -- "read and update state" --> cm
    cli -- "/metrics: query load;<br/>/api/v1/status/tsdb, /api/v1/query" --> bsel
    cli -- "/api/v1/status/tsdb, /api/v1/query,<br/>reset rollup cache" --> gsel
    cli -- "/metrics: ingestion rate<br/>(dual write, drain check)" --> bins
    cli -- "/metrics: ingestion rate<br/>(dual write)" --> gins
    cli -- "/metrics: data size" --> bsto
    cli -- "/metrics: free disk" --> gsto
    cli -- "one run per chunk" --> vmctl
    vmctl -- "GET /api/v1/export/native" --> bsel
    vmctl -- "POST /api/v1/import/native" --> gins
```

The data path is vmctl's: it streams blue's native export into green's vminsert, which spreads the series across
all green vmstorage nodes. Everything else the tool does is reading and checking.

## Packages

```mermaid
flowchart TD
    main["cmd/vmrebalance"] --> cli["internal/cli<br/>commands, flags, wiring"]
    cli --> check["internal/check<br/>preflight, verify, drain, load"]
    cli --> migration["internal/migration<br/>Syncer, Governor, Metrics"]
    cli --> copier["internal/copier<br/>vmctl engine"]
    cli --> cluster["internal/cluster<br/>VMCluster facts, pods, HTTP access"]
    cli --> state["internal/state<br/>ConfigMap and file stores"]
    check --> cluster
    check --> migration
    check --> state
    check --> vm["internal/vm<br/>/metrics parser, query API"]
    copier --> migration
    migration --> state
    cluster --> op[["VictoriaMetrics operator API"]]
    cluster --> k8s[["client-go"]]
    state --> k8s
```

| Package | Responsibility |
|---|---|
| `internal/cli` | Cobra commands and flags. It connects to Kubernetes, opens both clusters and the state, wires the other packages together and prints the reports. It holds no migration logic. |
| `internal/migration` | `Syncer` splits a time range into step-aligned chunks, copies them in order with retries, and records each finished chunk. It also runs the final sync window. `Governor` picks each chunk's rate limit. `Metrics` holds the tool's own Prometheus metrics. |
| `internal/copier` | `VMCtl` implements `migration.Copier`. It builds the `vmctl vm-native` command line for a window and rate, and reads the transferred bytes from vmctl's output. |
| `internal/check` | Read-only checks over `cluster.Client`: `Preflight`, `VerifyDay`, `WaitDrained` (blue gets no more writes), `WatchWrites` (have the writers switched to green?), and `SelectLoad` (blue's vmselect load, which the Governor reads). They scrape each pod's `/metrics` directly, so they don't depend on self-monitoring being set up. |
| `internal/cluster` | `Client` is everything the checks need from one cluster: facts from the VMCluster spec, running pods, GET on a pod, GET on vmselect, PodDisruptionBudgets. `Kube` implements it through the Kubernetes API, using the API server proxy or direct connections. |
| `internal/state` | The migration's state and two `Store` implementations: a ConfigMap (default) and a local file (`--state-file`). |
| `internal/vm` | Parses VictoriaMetrics' `/metrics` text output, and calls the TSDB status and instant query endpoints. |

The seams that the unit tests cover are `migration.Copier` and `migration.Pacer` (faked in the Syncer tests),
`cluster.Client` (faked in the check tests), `copier.Runner` (faked in the vmctl tests), and `kubernetes.Interface`
(client-go's fake clientset, in the ConfigMap store tests). `cluster.Kube` and the CLI wiring are covered by the
end-to-end test on kind ([test/e2e](../test/e2e)).

## Copying

`migrate` copies through `migration.Syncer`: the backfill, every catch-up and the final sync window. `sync` uses the
same Syncer for a single run.

```mermaid
sequenceDiagram
    autonumber
    participant CLI as cli (migrate)
    participant S as migration.Syncer
    participant St as state.Store
    participant G as Governor (--adaptive)
    participant B as blue vmselect
    participant V as vmctl
    participant GI as green vminsert

    CLI->>S: Sync(now - retention, until)
    S->>St: Load
    St-->>S: SyncedUntil (if set, continue from there)
    loop every chunk [start, end)
        S->>G: Next
        G->>B: GET /metrics on every pod
        B-->>G: query slots in use, waits, p99 latency
        G-->>S: rate limit (halved if busy, +10% of max if not)
        S->>V: vm-native, one request per hour of the chunk, --vm-rate-limit = rate / parallel requests
        V->>B: GET /api/v1/export/native
        B-->>V: native blocks
        V->>GI: POST /api/v1/import/native
        V-->>S: exit status, "total bytes"
        alt vmctl failed
            S->>S: wait (backoff doubles), retry up to --retries
        end
        S->>St: Update(AddChunk): SyncedUntil = end
    end
```

Every finished chunk is saved before the next one starts, so a crashed or evicted run restarts with the first
unfinished chunk. The backfill starts at `now - retention`. Every catch-up continues from `SyncedUntil`.
Chunk boundaries are aligned to `--step`, so a day step cuts at UTC midnight.

## A migrate run

`migrate` chains the same pieces that the single commands use, and records every step in the state, so a
restarted pod continues where the last one stopped. It never changes the clusters either: it waits for the changes
you make and detects them.

```mermaid
flowchart TD
    start(["migrate"]) --> done0{"completedAt<br/>in the state?"}
    done0 -- "yes" --> report(["report: done"])
    done0 -- "no" --> pf{"backfill started?"}
    pf -- "no" --> preflight["open both VMClusters and run preflight,<br/>every --poll-interval until no check fails"]
    pf -- "yes" --> open["open both VMClusters<br/>(preflight would fail on capacity now)"]
    preflight --> cut0{"cutoverAt<br/>in the state?"}
    open --> cut0
    cut0 -- "no" --> sync["sync up to now"]
    sync --> verify["verify full days without a pass;<br/>copy a failed day again once,<br/>stop if it still fails"]
    verify --> ready["catch-up shorter than --ready-after:<br/>record readyAt, log 'ready for the cutover'"]
    ready --> watch{"over one --drain-interval:<br/>blue gets no rows<br/>and green gets live rows?"}
    watch -- "no" --> sync
    watch -- "yes, at Tcut" --> final["final sync for Tcut"]
    cut0 -- "yes" --> final
    final -- "blue still receives writes" --> sync
    final -- "checks fail" --> fail(["FAIL: don't delete blue"])
    final -- "checks pass" --> ok(["record completedAt: done"])
```

The cutover signal needs both halves: blue at zero alone also happens while VMAgent is down. Green's rate leaves out
native imports, which are the tool's own copies. The measurement is a hint, not proof, so the final sync's drain
check follows as before, after `--final-tail`. If it fails because blue still receives rows, nothing was copied or
recorded, and migrate goes back to catching up.

## The final sync

The final sync refuses a cutover time in the future. It checks that blue is drained before it copies, and copies
up to the moment the check passed, so samples from a writer that stopped late are not left on blue. If a catch-up
already ran past the cutover, its window starts at `Tcut - 1h` instead of `SyncedUntil - 1h` (1h is the default of
`--final-overlap`, which must cover the oldest timestamps that writers bypassing dual write still send), so the time
around the cutover is always copied and verified. A rerun with the same cutover keeps the first run's window start,
so a day that failed to verify is copied and verified again. `migrate` records the first cutover it notices as
`cutoverSeenAt` before the final sync starts. A failed drain check copies nothing. When blue still received rows,
that was no cutover: `migrate` clears `cutoverSeenAt` and goes back to catching up. Any other drain failure fails the
run; the restarted pod (also one evicted during the final sync) catches up, notices the cutover again and starts the
final sync at `cutoverSeenAt`, because rows blue delivers late carry timestamps from around it. Failed checks after
the copy fail the run, and the restarted pod runs the final sync again for the recorded cutover. Every catch-up and
the completion first open both VMClusters again, so one created again during the migration stops it. Green
counts as ready only while every full day has a whole-day pass, and before it completes, `migrate` verifies every
day without one (trying again while queries fail), so a day whose verify timed out is never left unverified.

```mermaid
flowchart TD
    start(["final sync for Tcut"]) --> wait["wait until Tcut + --final-tail (15m)"]
    wait --> drain{"does blue's vminsert accept<br/>no rows for one --drain-interval<br/>and deliver every row it accepted,<br/>within --drain-timeout?"}
    drain -- "no: blue still receives rows" --> catchup(["nothing copied: back to catch-up,<br/>a writer still points at blue"])
    drain -- "no: rows not delivered, or pods<br/>restarted in every interval" --> fail(["FAIL: nothing copied,<br/>the restarted pod tries again"])
    drain -- "yes, at Tdrained" --> copy["copy [min(SyncedUntil, Tcut) - overlap, Tdrained]<br/>(overlap is safe: green deduplicates)"]
    copy --> settle["wait 30s until green<br/>can search the imported rows"]
    settle --> reset["reset the rollup result cache<br/>on every green vmselect pod"]
    reset --> verify["verify the days the final sync copied<br/>(partial responses fail)"]
    verify --> earlier{"any stored day that failed<br/>verify and hasn't passed since?"}
    earlier -- "yes" --> again(["FAIL: verify those days first"])
    earlier -- "no" --> ok(["Done: keep blue for rollback, then delete it"])
```

## State

The state is one JSON document per migration and tenant. By default it lives in the ConfigMap
`vmrebalance.<from>.<to>` (with `.tenant-<tenant>` appended for tenants other than 0) in the namespace of both clusters,
which survives restarts of a Job's pod. With `--state-file` it lives in a local file instead. A state saved for
another pair of clusters or another tenant is refused, and so is one saved for a VMCluster that was deleted and
created again under the same name (the state records both UIDs).

| Field | Written by | Meaning |
|---|---|---|
| `from`, `to`, `tenant` | every command | Which migration the state belongs to. A state for another one is refused. |
| `fromUID`, `toUID` | every command that opens the clusters | The VMClusters' UIDs, recorded on first use. A VMCluster that was deleted and created again is refused. |
| `dualWriteSeenAt` | `preflight`, `migrate` | When green was first seen receiving live writes (T0, or a little after it). |
| `backfillStart` | `migrate`, `sync` | Where the first sync started. |
| `syncedUntil` | `migrate`, `sync` | End of the last finished chunk. The next sync continues here. |
| `chunks` | `migrate`, `sync` | History of finished chunks (window, bytes, seconds), capped at the last 500. Only `status` reads it. |
| `verifications` | `verify`, `migrate` | The latest result per UTC day. |
| `readyAt` | `migrate` | When a catch-up (copy plus verify) first took less than `--ready-after`. |
| `cutoverSeenAt` | `migrate` | The first cutover noticed, saved before the final sync. A final sync after a restart starts there; cleared when blue receives writes again. |
| `cutoverAt`, `finalStart`, `finalEnd`, `finalSyncAt` | `migrate` | The cutover time and the window the final sync copied. A rerun clears `finalSyncAt` until its copy finishes. |
| `completedAt` | `migrate` | When the final sync passed every check. A later `migrate` only reports that it is done. |

Several commands can run at once, for example `verify` from a workstation while the migrate Job runs. So no command
writes back a state it loaded earlier. Each one calls `Store.Update` with a function that changes only its own fields.
The ConfigMap store applies that function to the latest version, and if the resourceVersion changed in between, it
reads the state again and retries.

## Adaptive rate limit

With `--adaptive`, a `Governor` picks each chunk's rate limit, up to `--rate-limit`. Before every chunk it reads
the `/metrics` of all of blue's vmselect pods:

- **Busy:** queries waited for a free slot (`vm_concurrent_select_limit_reached_total` went up), more than
  `--max-select-utilization` of the query slots are in use (`vm_concurrent_select_current / _capacity`), or the
  p99 latency of instant or range queries is above `--max-query-latency` (only when that flag is set; latency is
  ignored by default). The governor halves the rate, but never below `--min-rate-limit`.
- **Not busy:** the governor raises the rate by a tenth of `--rate-limit`, up to `--rate-limit`.
- **Can't be read:** it keeps the current rate and logs a warning.

The wait counter is summed over all vmselect pods, so a newly added vmselect pod can make one reading look busy. A
restarted pod (its counter starts again at 0) doesn't. The rate never drops below 1 byte/s, because 0 means "no
limit" to vmctl.

```mermaid
stateDiagram-v2
    state "at --rate-limit" as AtMax
    state "below --rate-limit" as Below
    [*] --> AtMax: first reading, not busy
    [*] --> Below: first reading, busy (halve)
    AtMax --> AtMax: not busy
    AtMax --> Below: busy (halve)
    Below --> Below: busy (halve, floor at --min-rate-limit)
    Below --> Below: not busy (+10% of --rate-limit)
    Below --> AtMax: not busy, reached --rate-limit
```

The governor reads blue's load before every chunk, the first one included.

vmctl reads its rate limit once at startup, so the rate changes only between chunks. With `--adaptive`, use a
small `--step` such as `1h`. The latency signal comes from vmselect's 5-minute summary window, so it reacts with
some delay. Requests from the tool itself don't count: vmctl's exports are counted under their own path. `verify`'s
queries do count as instant queries.

## Metrics

`--metrics-addr :8080` (set by the chart) serves these metrics, plus Go and process metrics:

| Metric | Type | Meaning |
|---|---|---|
| `vmrebalance_chunks_total` | counter | Finished chunks. |
| `vmrebalance_copied_bytes_total` | counter | Bytes vmctl reported as transferred. |
| `vmrebalance_chunk_retries_total` | counter | Failed chunk attempts that were retried. |
| `vmrebalance_chunk_duration_seconds` | histogram (`le` buckets from 1s to 24h) | Time per chunk, including retries. |
| `vmrebalance_synced_until_timestamp_seconds` | gauge | End of the last finished chunk, as a Unix time. `time() - this` is how far the sync is behind. |
| `vmrebalance_rate_limit_bytes_per_second` | gauge | Rate limit of the current chunk (0: none). |
| `vmrebalance_blue_select_concurrency_ratio` | gauge | With `--adaptive`: share of blue's vmselect query slots in use at the last reading. |
| `vmrebalance_blue_query_duration_seconds{quantile="0.99"}` | gauge | With `--adaptive`: highest query p99 latency on blue at the last reading. |
| `vmrebalance_phase{phase="..."}` | gauge | 1 for the phase `migrate` is in (`preflight`, `backfill`, `catch-up`, `ready`, `final`, `done`), 0 for the others. |

## How the tool reaches the clusters

`--access` decides how HTTP requests to vmselect and to single pods travel:

- `proxy`: through the API server's service and pod proxy. This works from any workstation with a kubeconfig, but
  the API server ends requests after its request timeout (1 minute by default).
- `direct`: Service DNS names and pod IPs. This needs to run inside the cluster, and NetworkPolicies must allow it.
- `auto` (the default) picks `direct` inside a pod and `proxy` otherwise.

vmctl always connects directly, to the Service URLs from the VMCluster objects, or to `--from-select-url` and
`--to-insert-url`, for example after a port-forward.
