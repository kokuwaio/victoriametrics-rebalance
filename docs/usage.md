# Usage

This guide walks through one migration from start to end: what to change in your cluster, which command to run
when, what the output means and what to do when a check fails. The [README](../README.md) explains why the
migration works this way, and [architecture.md](architecture.md) how the tool works inside.

The examples migrate the VMCluster `vmcluster` (blue) to a new VMCluster `vmcluster-b` (green) in the namespace
`monitoring`. Replace the names with yours.

## Before you start

Read the [requirements](../README.md#requirements) and plan for them before you create green. In short:

- Readers and writers reach VictoriaMetrics through aliases you can switch in one step, not through the operator's
  `vmselect-<name>` and `vminsert-<name>` Services.
- You know every writer, and which ones don't go through VMAgent.
- There is room for a full second copy of the data: disk, memory and CPU for green until blue is deleted.
- Blue's vmselect allows exports of all series active in one hour (`-search.maxExportSeries`, default 10 million).

Timeline: the backfill takes hours to days, depending on the data volume and the rate limit. Catch-up runs and the
cutover take minutes. Plan a few days of soak time before you delete blue.

## Install

`sync` runs vmctl, which needs network access to blue's vmselect and green's vminsert, so it usually runs as a Job
in the cluster. No image is published yet. Build one from the [Dockerfile](../Dockerfile), which bundles vmctl, and
push it to a registry your cluster can pull from:

```sh
make image
docker tag "vmrebalance:$(git describe --tags --always --dirty)" registry.example.com/vmrebalance:dev
docker push registry.example.com/vmrebalance:dev
```

For `preflight`, `verify` and `status` from a workstation, install the binary with mise or build it, see
[Install](../README.md#install). It uses your kubeconfig (`--kubeconfig`, `--context`), and needs read access to the VMClusters, pods and
PodDisruptionBudgets, and write access to ConfigMaps in the namespace. [deploy/rbac.yaml](../deploy/rbac.yaml)
lists the exact permissions.

## Step 1: Put aliases in front of vmselect and vminsert

Create Services you control that select the active cluster's pods, and point every reader and writer at them. A
cutover then means changing two selectors instead of every client.

```yaml
apiVersion: v1
kind: Service
metadata:
  name: vmselect-active
  namespace: monitoring
spec:
  selector:
    app.kubernetes.io/name: vmselect
    app.kubernetes.io/instance: vmcluster
  ports:
    - name: http
      port: 8481
      targetPort: 8481
---
apiVersion: v1
kind: Service
metadata:
  name: vminsert-active
  namespace: monitoring
spec:
  selector:
    app.kubernetes.io/name: vminsert
    app.kubernetes.io/instance: vmcluster
  ports:
    - name: http
      port: 8480
      targetPort: 8480
```

If you use VMAuth, point the VMUsers' `targetRefs` at these aliases (or at the active cluster) instead. This step is
done once per setup and stays useful for later migrations.

## Step 2: Create green

Green has more vmstorage nodes, and otherwise matches blue: the same version, retention and replication factor. It
also needs deduplication with exactly `1ms` on vmstorage and vmselect, because the sync windows overlap on purpose.

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMCluster
metadata:
  name: vmcluster-b
  namespace: monitoring
spec:
  retentionPeriod: "12"
  replicationFactor: 1
  clusterVersion: v1.154.1-cluster
  vmstorage:
    replicaCount: 9
    extraArgs:
      dedup.minScrapeInterval: 1ms
    storage:
      volumeClaimTemplate:
        spec:
          resources:
            requests:
              storage: 830Gi
  vmselect:
    replicaCount: 2
    extraArgs:
      dedup.minScrapeInterval: 1ms
  vminsert:
    replicaCount: 2
```

Size the disks for blue's used bytes divided by green's node count, plus headroom (20% by default). `preflight`
checks the total and every single node. Copy blue's other settings (resources, affinity, extra arguments) as well,
but give green's pods their own labels wherever selectors use them, for example in topology spread constraints and
PodDisruptionBudgets. Don't set `-relabelConfig` on green's vminsert unless applying the rules twice changes
nothing: the copied data was already relabeled by blue.

Wait until green is operational (`kubectl -n monitoring get vmcluster vmcluster-b` shows `operational`).

## Step 3: Enable dual write

Add green as a second `remoteWrite` URL on every VMAgent. From now on green receives most new data live, so the
copy only has to cover the past.

```yaml
apiVersion: operator.victoriametrics.com/v1beta1
kind: VMAgent
metadata:
  name: vmagent
  namespace: monitoring
spec:
  remoteWrite:
    - url: http://vminsert-active.monitoring.svc:8480/insert/0/prometheus/api/v1/write
    - url: http://vminsert-vmcluster-b.monitoring.svc:8480/insert/0/prometheus/api/v1/write
```

The second URL points at green's own Service, not at the alias, because the alias still selects blue. If VMAgent
runs with `remoteWrite.disableOnDiskQueue`, a slow green can stall writes to blue: drop samples on overload for the
green URL, or enable the on-disk queue for the migration. The final sync fills in what green dropped.

## Step 4: Check the setup with `preflight`

```sh
vmrebalance preflight -n monitoring --from vmcluster --to vmcluster-b
```

Every check prints one line with `PASS`, `WARN` or `FAIL`:

```text
PASS  operational         both operational
PASS  storage nodes       blue has 8 vmstorage nodes, green 9
PASS  version             same image tags on all components
PASS  retention           both "12"
PASS  replication factor  both 1
PASS  deduplication       green: -dedup.minScrapeInterval=1ms
PASS  relabeling          green's vminsert doesn't relabel
PASS  capacity            green has 7.3 TiB free, blue uses 5.5 TiB
PASS  dual write          green receives 41250 rows/s, 100% of blue's 41250 rows/s
PASS  pod labels          no selector matches pods of both clusters

Ready. Next: vmrebalance sync (backfill).
```

Fix every `FAIL` before you go on; the command exits with status 1 while one is left. A `WARN` needs a decision:
for example, a dual write share below 100% means some writers bypass VMAgent, and their data reaches green only
through the final sync. See [Troubleshooting](#troubleshooting) for what each failure means.

`preflight` records when it first saw dual write in the state.

## Step 5: Copy the past with `sync` (backfill)

Apply the permissions once, then the sync Job:

```sh
kubectl apply -f deploy/rbac.yaml
kubectl apply -f deploy/job.yaml
kubectl -n monitoring logs -f job/vmrebalance-sync
```

Edit [deploy/job.yaml](../deploy/job.yaml) first: the image, `--from`, `--to`, and the rate settings. The first run
copies from `now - retention` up to now, one chunk at a time. Every finished chunk is saved in the state, so if the
pod is evicted or restarted, the next one continues with the first unfinished chunk.

How to choose the settings:

- **`--rate-limit`** is the total transfer rate. The export reads go through blue's vmselect and vmstorage and
  compete with real queries. Start low, for example `20MiB/s`, watch blue's CPU, disk and query latency, and raise
  it when blue copes.
- **`--adaptive`** halves the rate while blue's vmselect is busy (queries wait for a slot, or more than
  `--max-select-utilization` of the slots are in use) and raises it by a tenth of `--rate-limit` when it isn't.
  Set `--max-query-latency`, for example `5s`, to count slow queries as busy too. The rate changes only between
  chunks, so use it with `--step 1h`.
- **`--step`** is the chunk size (default `24h`). Smaller chunks lose less work on a failure and let `--adaptive`
  react faster.
- **`--concurrency`** is the number of vmctl workers (default 2). Each worker copies one hour of the chunk at a time,
  so more workers than hours in a chunk don't help.

A failed chunk is retried `--retries` times (default 3), with a wait that starts at `--backoff` (default 1m) and
doubles. When a chunk still fails, the pod exits with an error, and the Job restarts it (up to its `backoffLimit`),
which continues with the failed chunk. If the Job gives up, fix the cause, then delete the Job and apply it again; it
continues where it stopped.

### Watching the progress

`status` reads the state and works from anywhere, also while the Job runs:

```sh
vmrebalance status -n monitoring --from vmcluster --to vmcluster-b
```

```text
Migration:        vmcluster → vmcluster-b
Dual write seen:  2026-01-02T08:12:40Z
Backfill start:   2025-01-02T08:30:00Z
Synced until:     2025-07-13T00:00:00Z (4184h0m0s behind now)
Chunks:           193 recorded, 2.6 TiB copied in 40h12m0s
Throughput:       18.8 MiB/s
Catch-up ETA:     ~36h53m27.3s at the recorded pace
Verified days:    -
Cutover:          -
Final sync:       -
```

The ETA accounts for time moving on while the copy runs. If it says the copy won't catch up, raise the rate limit.

With `--metrics-addr :8080` (set in the example Job), the tool serves Prometheus metrics. Scrape them with a
VMPodScrape for the label `app.kubernetes.io/name: vmrebalance` and port `http-metrics`. Useful queries:

| What | Query |
|---|---|
| How far the copy is behind | `time() - vmrebalance_synced_until_timestamp_seconds` |
| Copy throughput | `rate(vmrebalance_copied_bytes_total[15m])` |
| Current rate limit (with `--adaptive`) | `vmrebalance_rate_limit_bytes_per_second` |
| Blue's vmselect load as the tool sees it | `vmrebalance_blue_select_concurrency_ratio` |
| Failing chunks | `increase(vmrebalance_chunk_retries_total[1h])` |

## Step 6: Compare blue and green with `verify`

```sh
vmrebalance verify -n monitoring --from vmcluster --to vmcluster-b
```

For every UTC day from the first full day of the backfill up to the last synced point, `verify` compares the number
of series and, for the 10 metric names with the most series plus 10 random ones, the number of samples. Green passes
with at least as much as blue, minus 1%. On a partial day (the last one, usually) only the sample counts are
compared:

```text
PASS  verify 2026-01-06   series: blue 2104311, green 2104311
PASS  verify 2026-01-07   series: blue 2111840, green 2111840
PASS  verify 2026-01-08   samples 00:00:00–09:00:00 match (partial day, series not compared)
```

`verify` only reads, so it can run at any time, also during a sync. Run it after the backfill, and again whenever you
want. Use `--since` and `--until` to check other days, and `--top-n`, `--random-n` and `--tolerance` to tune it. On
large clusters the Kubernetes API server may cut off its queries after its request timeout (1 minute by default);
then run it inside the cluster, where it connects directly (see [Access modes](#access-modes)).

## Step 7: Catch up

Run the sync Job again, as often as needed: delete it and apply it again. Each run copies from the last synced point
to now. When a run takes only a few minutes, the copy has caught up, and you are ready for the cutover. Keep the time
between the last catch-up and the cutover short: data from writers that bypass VMAgent waits for the final sync.

## Step 8: Cut over

Switch readers and writers to green, usually in one Git change:

- Change the selector of both aliases from `app.kubernetes.io/instance: vmcluster` to `vmcluster-b`.
- Remove the second `remoteWrite` URL on the VMAgents, since the first one now reaches green through the alias.
  Until you do, green receives the samples twice and deduplicates them.

Note the time the change took effect, in UTC: this is the cutover time.

## Step 9: Run the final sync

Edit `--cutover-at` in [deploy/job-final.yaml](../deploy/job-final.yaml) to the cutover time, then apply it right
after the cutover:

```sh
kubectl apply -f deploy/job-final.yaml
kubectl -n monitoring logs -f job/vmrebalance-final
```

The final sync:

1. Waits until 15 minutes after the cutover, so writers that switched late have stopped writing to blue.
2. Checks that blue is drained: its vminsert accepts no rows for one whole `--drain-interval` (default 5m) and has
   delivered every row it accepted to vmstorage. It tries for up to `--drain-timeout` (default 20m). If blue still
   receives writes, it stops here and copies nothing.
3. Copies from one hour before the last synced point (`--final-overlap`) up to the moment blue was drained.
4. Waits 30 seconds until green can search the copied rows, then resets the rollup result cache on every green
   vmselect pod.
5. Verifies the days it copied, and checks that no earlier verified day still fails.

```text
PASS  blue drained        blue receives no more writes and has delivered every row
PASS  rollup cache        reset on every green vmselect pod
PASS  verify 2026-01-09   samples 08:12:00–10:20:31 match (partial day, series not compared)
PASS  earlier verify      no failed days stored

Done. Keep blue for a few days so you can roll back, then delete it and its PVCs.
```

The Job doesn't restart on failure, because a failed check needs a person to look. Fix the cause, then delete the
Job and apply it again with the same `--cutover-at`: a rerun covers at least the first run's window again.

Set `--drain-interval` longer than the push interval of your slowest writer (and at least 1m): blue passes only when
it receives no rows at all during one whole interval. Set `--final-overlap` longer if a writer that bypasses VMAgent
sends samples with old timestamps (see [requirement 2](../README.md#requirements)).

## Step 10: Soak, then delete blue

Keep blue for a few days, so you can roll back. Then:

- Delete the blue VMCluster. The vmstorage PVCs usually stay after their StatefulSet is gone: list them with
  `kubectl -n monitoring get pvc` and delete blue's (named like `vmstorage-db-vmstorage-vmcluster-0`).
- Delete the state ConfigMap, the Jobs and, if you don't need them any more, the RBAC objects.
- Check dashboards, alerts and SLOs that name blue (for example `job="vmstorage-vmcluster"`).

## Rolling back

Until you delete blue, switch the aliases (and the VMAgents' `remoteWrite`) back to blue. Blue has everything up to
the cutover. To copy what was written to green after the cutover back to blue, run a sync in the other direction,
starting at the cutover:

```sh
vmrebalance sync -n monitoring --from vmcluster-b --to vmcluster --since 2026-01-09T10:00:00Z
```

Without `--since`, the reverse run starts with an empty state and copies green's whole retention. The reverse
direction has its own state, so the forward migration's state stays as it was.

## Running from a workstation

`preflight`, `verify` and `status` work from a workstation as they are: they reach the clusters through the
Kubernetes API server. `sync` also needs vmctl in your `PATH` and direct access to blue's vmselect and green's
vminsert. Port-forward both and pass the local URLs:

```sh
kubectl -n monitoring port-forward svc/vmselect-vmcluster 8481:8481 &
kubectl -n monitoring port-forward svc/vminsert-vmcluster-b 8480:8480 &
vmrebalance sync -n monitoring --from vmcluster --to vmcluster-b --rate-limit 20MiB/s --from-select-url http://localhost:8481 --to-insert-url http://localhost:8480
```

This is fine for a test, but a backfill that takes days belongs in a Job.

### Access modes

`--access` sets how the tool reaches vmselect and the pods:

- `proxy` goes through the Kubernetes API server and works from anywhere you can use `kubectl`. The API server cuts
  requests off after its request timeout (1 minute by default), which `verify` can hit on large clusters.
- `direct` uses Service DNS names and pod IPs, so it works only inside the cluster.
- `auto` (the default) picks `direct` inside a pod and `proxy` everywhere else.

Inside the cluster, NetworkPolicies must let the tool's pod reach vmselect (8481), vminsert (8480) and vmstorage
(8482).

## The state

The tool remembers the progress in one JSON document: when dual write started, where the backfill started, the last
synced point, the finished chunks, the verify results per day, and the cutover and final sync. By default it lives
in the ConfigMap `vmrebalance.<from>.<to>` in the clusters' namespace:

```sh
kubectl -n monitoring get configmap vmrebalance.vmcluster.vmcluster-b -o 'jsonpath={.data.state\.json}'
```

- **A local file instead:** `--state-file state.json` keeps the state on your machine. Use it only for runs from a
  workstation; a Job's pod loses its local files when it restarts.
- **Several tenants:** the tool copies one tenant per run (`--tenant`, default `0`). Each tenant has its own state,
  in the ConfigMap `vmrebalance.<from>.<to>.tenant-<tenant>`.
- **Starting over:** delete the ConfigMap (or the file). The next sync starts a new backfill. The tool refuses a
  state that belongs to other clusters, another tenant, or a VMCluster that was deleted and created again under the
  same name.

Several commands may run at once, for example `verify` from a workstation during a sync Job. Each one changes only
its own fields, so they don't overwrite each other.

## Flag reference

Run `vmrebalance <command> --help` for the full list. Flags for every command:

| Flag | Default | Meaning |
|---|---|---|
| `--from`, `--to` | (required) | Names of blue and green. |
| `-n`, `--namespace` | the context's namespace | Namespace of both VMClusters. |
| `--tenant` | `0` | Tenant to copy and check, e.g. `0` or `1:2`. |
| `--access` | `auto` | `proxy`, `direct` or `auto`, see [Access modes](#access-modes). |
| `--state-file` | (ConfigMap) | Keep the state in a local file. |
| `--kubeconfig`, `--context` | kubeconfig defaults | Which cluster to talk to. |
| `-v`, `--verbose` | off | Debug logs. |

`preflight`:

| Flag | Default | Meaning |
|---|---|---|
| `--headroom` | `0.2` | Free space green needs on top of blue's used bytes. |
| `--rate-window` | `30s` | Time between the two scrapes that measure ingestion rates. |

`sync`:

| Flag | Default | Meaning |
|---|---|---|
| `--until` | `now` | Copy up to this time. |
| `--since` | now - retention | On the first run, start here instead (used for a rollback). |
| `--step` | `24h` | Chunk size. |
| `--rate-limit` | none | Total transfer rate, e.g. `50MiB/s`; with `--adaptive` the highest rate. |
| `--concurrency` | `2` | vmctl workers. |
| `--adaptive` | off | Lower the rate while blue's vmselect is busy. |
| `--min-rate-limit` | a tenth of `--rate-limit` | With `--adaptive`: the lowest rate. |
| `--max-select-utilization` | `0.8` | With `--adaptive`: share of query slots in use above which blue is busy. |
| `--max-query-latency` | off | With `--adaptive`: p99 query latency above which blue is busy. |
| `--retries`, `--backoff` | `3`, `1m` | Retries per failed chunk, and the first wait (it doubles). |
| `--metrics-addr` | off | Serve the tool's Prometheus metrics, e.g. `:8080`. |
| `--from-select-url`, `--to-insert-url` | the Service URLs | vmctl's endpoints, e.g. after a port-forward. |
| `--vmctl`, `--vmctl-arg` | `vmctl`, none | vmctl binary and extra arguments (not `--vm-concurrency` or `--vm-rate-limit`). |
| `--final` | off | Run the final sync. |
| `--cutover-at` | (required with `--final`) | When the aliases were switched (RFC 3339). |
| `--final-overlap` | `1h` | How far before the last synced point the final window starts. |
| `--drain-interval` | `5m` | Blue must receive no rows for this long (at least `1m`). |
| `--drain-timeout` | `20m` | How long blue may still receive writes after cutover + 15m. |

`verify` (and `sync --final`, which verifies too):

| Flag | Default | Meaning |
|---|---|---|
| `--since`, `--until` | the backfilled days | Days to check (`verify` only). |
| `--top-n` | `10` | Count the samples of this many metric names with the most series. |
| `--random-n` | `10` | Plus this many names picked at random. |
| `--pool` | `1000` | Pick the random names from this many names below the top N. |
| `--tolerance` | `0.01` | Share by which green may fall short of blue. |

## Troubleshooting

The commands print a reason with every failure. The most common ones:

| Message | What it means | What to do |
|---|---|---|
| `green has no -dedup.minScrapeInterval` or `... disabled or invalid` | Overlapping windows would store samples twice. | Set `dedup.minScrapeInterval: 1ms` in the `extraArgs` of green's vmstorage and vmselect. |
| `version` fails | Blue and green run different image tags; the native format is version-coupled. | Run the same version on both. |
| `capacity` fails | Green has too little free disk, in total or on one node. | Larger PVCs, or more vmstorage nodes. |
| `green receives no live writes` | Dual write isn't set up. | Add green's URL to every VMAgent's `remoteWrite` (step 3). |
| `PodDisruptionBudget ... selects pods of both clusters` or `topologySpreadConstraints` | Blue and green pods are counted together. | Add `app.kubernetes.io/instance` to the selector. |
| `relabeling` warns | Green's vminsert relabels the copied data again. | Make sure the rules don't change their own output, or remove them for the migration. |
| A chunk fails with an error about `maxExportSeries` | One hour has more series than blue's vmselect exports at once. | Raise `-search.maxExportSeries` on blue's vmselect. |
| `nothing synced yet` | `verify` or `sync --final` ran before the first sync. | Run `sync` first. |
| `nothing to copy: start ... is not before ...` | `--since` is after `--until`. | Fix the times. |
| `state belongs to the migration ...` | The state is for other clusters or another tenant. | Check `--from`, `--to`, `--tenant` and `--state-file`. |
| `VMCluster ... was deleted and created again` | The saved progress belongs to a cluster that no longer exists. | Delete the state to start over. |
| `cutover ... is in the future` | `--cutover-at` is later than now. | Run the final sync after the cutover, with the time it took effect. |
| `blue still received N rows/s ... point it at green` | Some writer still sends to blue. Its data would be lost with blue. | Find it (blue's vminsert metric `vm_rows_inserted_total` shows the protocol), point it at the alias, rerun with the same `--cutover-at`. |
| `vminsert pods restarted or changed during every measurement` | The drain check couldn't measure. | Wait until blue's vminsert pods are stable, then rerun. |
| `still fail to send to a vmstorage node` or `were still delivering rows` | Blue's vminsert still holds rows it accepted. | Fix blue's vmstorage nodes, then rerun. |
| `verify` fails with `series: ...` or `samples of ...` | Green has less than blue for that day. | Run `verify` for the day again later. If it still fails, copy the day again: a `sync` with its own `--state-file`, `--since` and `--until`. |
| `earlier verify` fails | A day failed verify earlier and hasn't passed a whole-day verify since. | `verify --since <day> --until <next day>`, then rerun the final sync. |
| `verify` times out | The API server cut the request off (`--access=proxy`). | Run it inside the cluster, where it connects directly. |
