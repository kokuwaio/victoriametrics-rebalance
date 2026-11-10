# Usage

This guide walks through one migration from start to end: what to change in your cluster, what the tool does on its
own, what the output means and what to do when a check fails. The [README](../README.md) explains why the
migration works this way, and [architecture.md](architecture.md) how the tool works inside.

The examples migrate the VMCluster `vmcluster` (blue) to a new VMCluster `vmcluster-b` (green) in the namespace
`monitoring`. Replace the names with yours.

In short: you make three changes (aliases, green, dual write), install the chart once with `from` and `to`, switch
the aliases when the Job says green is ready, and delete blue after a few days. The Job, which runs
`vmrebalance migrate`, does everything in between and notices the cutover by itself.

## Before you start

Read the [requirements](../README.md#requirements) and plan for them before you create green. In short:

- Readers and writers reach VictoriaMetrics through aliases you can switch in one step, not through the operator's
  `vmselect-<name>` and `vminsert-<name>` Services.
- You know every writer, and which ones don't go through VMAgent.
- There is room for a full second copy of the data: disk, memory and CPU for green until blue is deleted.
- Blue's vmselect allows exports of all series active in one hour (`-search.maxExportSeries`, default 10 million).

Timeline: the backfill takes hours to days, depending on the data volume and the rate limit. Catch-up and the
cutover take minutes. Plan a few days of soak time before you delete blue.

## Install

The migration runs as a Job in the cluster, installed with the [Helm chart](../README.md#helm-chart), because vmctl
needs network access to blue's vmselect and green's vminsert. The Job uses the image
`ghcr.io/kokuwaio/victoriametrics-rebalance`, which bundles vmctl. The cluster needs an image pull secret for
`ghcr.io` (the chart value `imagePullSecrets`) until the package is made public.

To look at a migration from a workstation (`preflight`, `verify`, `status`), install the binary with mise or build
it, see [Install](../README.md#install). It uses your kubeconfig (`--kubeconfig`, `--context`), and needs read
access to the VMClusters, pods, Services and PodDisruptionBudgets, and write access to ConfigMaps in the namespace.
The chart's [Role](../charts/vmrebalance/templates/rbac.yaml) lists the exact permissions.

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

Keep them with your other manifests, next to the VMCluster, not in the vmrebalance release: they outlive the
migration, and uninstalling the chart afterwards must not remove them. This step is done once per setup and stays
useful for later migrations.

Tell the tool their names (chart values `aliases.select` and `aliases.insert`, or `--select-alias` and
`--insert-alias`). Then preflight fails until they select blue alone, the Job waits for them to select green before it
takes the cutover as done, and the final sync fails while one still selects blue. If you use VMAuth instead, point
the VMUsers' `targetRefs` at the aliases (or at the active cluster); without alias Services, leave the values empty,
and the tool relies on the writes alone.

## Step 2: Create green

Green has more vmstorage nodes, and otherwise matches blue: the same version, retention and replication factor. It
also needs deduplication with exactly `1ms` on vmstorage and vmselect, because the copy windows overlap on purpose.

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

Size the disks for blue's used bytes divided by green's node count, plus headroom (20% by default). Preflight checks
the total and every single node. Copy blue's other settings (resources, affinity, extra arguments) as well, but give
green's pods their own labels wherever selectors use them, for example in topology spread constraints and
PodDisruptionBudgets. Don't set `-relabelConfig` on green's vminsert unless applying the rules twice changes
nothing: the copied data was already relabeled by blue.

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

Steps 2 and 3 can go into the same change as step 4: the Job waits for them.

## Step 4: Install the chart

```sh
helm install vmrebalance oci://ghcr.io/kokuwaio/charts/vmrebalance -n monitoring --set from=vmcluster --set to=vmcluster-b --set aliases.select=vmselect-active --set aliases.insert=vminsert-active
kubectl -n monitoring logs -f job/vmrebalance-1
```

With Flux, put the same values into a HelmRelease. The Job is named `<release>-<revision>`; `helm install` prints
the name. It runs the whole migration:

1. **Preflight.** It waits until both VMClusters exist and no preflight check fails, and tries again every
   `--poll-interval` (1m), logging each failed check. When all pass, it prints the report:

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
   PASS  alias vmselect      Service vmselect-active selects vmcluster's vmselect pods
   PASS  alias vminsert      Service vminsert-active selects vmcluster's vminsert pods
   ```

   A `WARN` doesn't stop it, but read it: a dual write share below 100% means some writers bypass VMAgent, and
   their data reaches green only through the final sync. See [Troubleshooting](#troubleshooting) for what each
   failure means. Run `vmrebalance preflight` from a workstation to see the report before you install the chart.
2. **Backfill.** It copies from `now - retention` up to now, one chunk at a time. Every finished chunk is saved in
   the state, so if the pod is evicted or restarted, the next one continues with the first unfinished chunk.
3. **Verify.** It verifies every full day it copied. A day that fails is copied again once; if it still fails, the
   Job stops.
4. **Catch-up.** It copies from the last synced point to now, again and again, and measures blue's and green's
   writes between the runs. When a run (copy plus verify) takes less than `--ready-after` (10m), it logs
   `green is ready for the cutover`, and keeps catching up until you cut over.

How to choose the copy settings (chart values under `copy`; defaults `50MiB/s`, adaptive, 1h chunks):

- **`rateLimit`** is the total transfer rate. The export reads go through blue's vmselect and vmstorage and compete
  with real queries. Start low, for example `20MiB/s`, watch blue's CPU, disk and query latency, and raise it when
  blue copes.
- **`adaptive`** halves the rate while blue's vmselect is busy (queries wait for a slot, or more than
  `maxSelectUtilization` of the slots are in use) and raises it by a tenth of `rateLimit` when it isn't. Set
  `maxQueryLatency`, for example `5s`, to count slow queries as busy too. The rate changes only between chunks, so
  keep the chunks small.
- **`step`** is the chunk size. Smaller chunks lose less work on a failure and let `adaptive` react faster.
- **`concurrency`** is the number of vmctl workers (default 2). Each worker copies one hour of the chunk at a time,
  so more workers than hours in a chunk don't help.

To change a setting while the Job runs, `helm upgrade` with the new value (`--reset-then-reuse-values` keeps your
other values). The upgrade replaces the Job, and the new one continues from the state.

A failed chunk is retried `retries` times (default 3), with a wait that starts at `backoff` (default 1m) and
doubles. When a chunk still fails, the pod exits with an error, and the Job restarts it (up to `job.backoffLimit`),
which continues with the failed chunk. If the Job gives up, fix the cause, then `helm upgrade` to start a new one.

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
Verified days:    192 pass, 0 fail
Ready:            -
Cutover:          -
Final sync:       -
Completed:        -
```

The ETA accounts for time moving on while the copy runs. If it says the copy won't catch up, raise the rate limit.

The Job serves Prometheus metrics on port `http-metrics`. Set `metrics.podScrape.enabled=true` to let the chart
create a VMPodScrape for them. Useful queries:

| What | Query |
|---|---|
| Phase (`preflight`, `backfill`, `catch-up`, `ready`, `final`, `done`); alert on `ready` to know when to cut over | `vmrebalance_phase == 1` |
| How far the copy is behind | `time() - vmrebalance_synced_until_timestamp_seconds` |
| Copy throughput | `rate(vmrebalance_copied_bytes_total[15m])` |
| Current rate limit (with adaptive) | `vmrebalance_rate_limit_bytes_per_second` |
| Blue's vmselect load as the tool sees it | `vmrebalance_blue_select_concurrency_ratio` |
| Failing chunks | `increase(vmrebalance_chunk_retries_total[1h])` |

`verify` compares blue and green from a workstation at any time, also while the Job runs:

```sh
vmrebalance verify -n monitoring --from vmcluster --to vmcluster-b
```

For every UTC day from the first full day of the backfill up to the last synced point, it compares the number of
series and, for the 10 metric names with the most series plus 10 random ones, the number of samples. Green passes
with at least as much as blue, minus 1%. On a partial day (the last one, usually) only the sample counts are
compared:

```text
PASS  verify 2026-01-06   series: blue 2104311, green 2104311
PASS  verify 2026-01-07   series: blue 2111840, green 2111840
PASS  verify 2026-01-08   samples 00:00:00–09:00:00 match (partial day, series not compared)
```

Use `--since` and `--until` to check other days, and `--top-n`, `--random-n` and `--tolerance` to tune it. On large
clusters the Kubernetes API server may cut off its queries after its request timeout (1 minute by default); the Job
connects directly and isn't affected (see [Access modes](#access-modes)).

## Step 5: Cut over

When the Job logs `green is ready for the cutover`, switch readers and writers to green, usually in one Git change:

- Change the selector of both aliases from `app.kubernetes.io/instance: vmcluster` to `vmcluster-b`.
- Remove the second `remoteWrite` URL on the VMAgents, since the first one now reaches green through the alias.
  Until you do, green receives the samples twice and deduplicates them.

Keep the time between the Job's ready message and the cutover short: data from writers that bypass VMAgent waits for
the final sync. You don't need to note the cutover time.

## Step 6: The final sync

The Job measures the writes for `--drain-interval` (5m) after every catch-up. Once blue receives no rows while green
still does (and the aliases, if set, select green), it takes the start of that measurement as the cutover time and
runs the final sync:

1. Waits until `--final-tail` (15m) after the cutover, so writers that switched late have stopped writing to blue.
2. Checks that blue is drained: its vminsert accepts no rows for one whole `--drain-interval` and has delivered every
   row it accepted to vmstorage. It tries for up to `--drain-timeout` (20m). If blue still receives writes, it
   copies nothing and goes back to catching up until every writer points at green. If the check fails for another
   reason (blue can't deliver rows it accepted, or its vminsert pods keep restarting), it copies nothing and the
   run fails; the restarted pod catches up and tries again, still from the cutover it noticed first.
3. Copies from one hour before the last synced point (`--final-overlap`) up to the moment blue was drained.
4. Waits 30 seconds until green can search the copied rows, then resets the rollup result cache on every green
   vmselect pod.
5. Checks the aliases, verifies the days it copied, and checks that no earlier verified day still fails.

```text
PASS  blue drained        blue receives no more writes and has delivered every row
PASS  rollup cache        reset on every green vmselect pod
PASS  alias vmselect      Service vmselect-active selects vmcluster-b's vmselect pods
PASS  alias vminsert      Service vminsert-active selects vmcluster-b's vminsert pods
PASS  verify 2026-01-09   samples 08:12:00–10:20:31 match (partial day, series not compared)
PASS  earlier verify      no failed days stored

Done. Keep blue for a few days so you can roll back, then delete it and its PVCs.
```

Then the Job completes. If a check fails, the pod exits with an error and the Job restarts it (up to
`job.backoffLimit`), which runs the final sync again for the same cutover and covers at least the first run's
window again.

Set `--drain-interval` (chart: `final.drainInterval`) longer than the push interval of your slowest writer (and at
least 1m): blue passes only when it receives no rows at all during one whole interval. Set `--final-overlap`
(`final.overlap`) longer if a writer that bypasses VMAgent sends samples with old timestamps (see
[requirement 2](../README.md#requirements)).

## Step 7: Soak, then delete blue

Keep blue for a few days, so you can roll back. Then:

- Delete the blue VMCluster. The vmstorage PVCs usually stay after their StatefulSet is gone: list them with
  `kubectl -n monitoring get pvc` and delete blue's (named like `vmstorage-db-vmstorage-vmcluster-0`).
- Uninstall the chart (`helm uninstall vmrebalance -n monitoring`), which removes the Job, Role and ServiceAccount, and
  delete the state ConfigMap, which the chart doesn't own.
- Check dashboards, alerts and SLOs that name blue (for example `job="vmstorage-vmcluster"`).

## Rolling back

Until you delete blue, switch the aliases (and the VMAgents' `remoteWrite`) back to blue. Blue has everything up to
the switch. To copy what was written to green since then back to blue, run `sync` in the other direction. Start
before the switch, not at the cutover: the cutover is when migrate noticed the switch, which can be a few minutes
after it. If you switched after migrate logged that green is ready, as step 5 says, the time `status` shows as Ready comes
before the switch; otherwise start at the time you merged the switch, or earlier. The copy overlaps what blue
already has up to the switch, so give blue `dedup.minScrapeInterval: 1ms` on vmstorage and vmselect first, as for
green in step 2; otherwise those samples are stored twice.

```sh
vmrebalance sync -n monitoring --from vmcluster-b --to vmcluster --since 2026-01-09T10:00:00Z --rate-limit 20MiB/s --from-select-url http://localhost:8481 --to-insert-url http://localhost:8480
```

`sync` needs vmctl and access to green's vmselect and blue's vminsert, see
[Running from a workstation](#running-from-a-workstation). Without `--since`, it starts with an empty state and
copies green's whole retention. The reverse direction has its own state, so the forward migration's state stays as
it was.

## Running from a workstation

`preflight`, `verify` and `status` work from a workstation as they are: they reach the clusters through the
Kubernetes API server. `migrate` and `sync` also need vmctl in your `PATH` and direct access to the source's
vmselect and the target's vminsert. Port-forward both and pass the local URLs:

```sh
kubectl -n monitoring port-forward svc/vmselect-vmcluster-b 8481:8481 &
kubectl -n monitoring port-forward svc/vminsert-vmcluster 8480:8480 &
```

This is fine for a test or a short copy, but a migration that takes days belongs in the Job.

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
synced point, the finished chunks, the verify results per day, when green was ready, the cutover, the final sync and
when the migration completed. By default it lives in the ConfigMap `vmrebalance.<from>.<to>` in the clusters'
namespace:

```sh
kubectl -n monitoring get configmap vmrebalance.vmcluster.vmcluster-b -o 'jsonpath={.data.state\.json}'
```

- **A local file instead:** `--state-file state.json` keeps the state on your machine. Use it only for runs from a
  workstation; a Job's pod loses its local files when it restarts.
- **Several tenants:** the tool copies one tenant per run (`--tenant`, default `0`). Each tenant has its own state,
  in the ConfigMap `vmrebalance.<from>.<to>.tenant-<tenant>`, and needs its own release of the chart.
- **Starting over:** delete the ConfigMap (or the file). The next run starts a new backfill. The tool refuses a
  state that belongs to other clusters, another tenant, or a VMCluster that was deleted and created again under the
  same name.
- **After completion:** a new `migrate` run for the same pair only reports that the migration is done.

Several commands may run at once, for example `verify` from a workstation while the Job runs. Each one changes only
its own fields, so they don't overwrite each other.

## Flag reference

Run `vmrebalance <command> --help` for the full list. The chart sets the flags from its
[values](../charts/vmrebalance/values.yaml). Flags for every command:

| Flag | Default | Meaning |
|---|---|---|
| `--from`, `--to` | (required) | Names of blue and green. |
| `-n`, `--namespace` | the context's namespace | Namespace of both VMClusters. |
| `--tenant` | `0` | Tenant to copy and check, e.g. `0` or `1:2`. |
| `--select-alias`, `--insert-alias` | not checked | The alias Services, see [step 1](#step-1-put-aliases-in-front-of-vmselect-and-vminsert). |
| `--access` | `auto` | `proxy`, `direct` or `auto`, see [Access modes](#access-modes). |
| `--state-file` | (ConfigMap) | Keep the state in a local file. |
| `--kubeconfig`, `--context` | kubeconfig defaults | Which cluster to talk to. |
| `-v`, `--verbose` | off | Debug logs. |

`migrate` and `sync` (copying):

| Flag | Default | Meaning |
|---|---|---|
| `--since` | now - retention | On the first run, start here instead. |
| `--step` | `24h` | Chunk size. |
| `--rate-limit` | none | Total transfer rate, e.g. `50MiB/s`; with `--adaptive` the highest rate. |
| `--concurrency` | `2` | vmctl workers. |
| `--adaptive` | off | Lower the rate while the source's vmselect is busy. |
| `--min-rate-limit` | a tenth of `--rate-limit` | With `--adaptive`: the lowest rate. |
| `--max-select-utilization` | `0.8` | With `--adaptive`: share of query slots in use above which the source is busy. |
| `--max-query-latency` | off | With `--adaptive`: p99 query latency above which the source is busy. |
| `--retries`, `--backoff` | `3`, `1m` | Retries per failed chunk, and the first wait (it doubles). |
| `--metrics-addr` | off | Serve the tool's Prometheus metrics, e.g. `:8080`. |
| `--from-select-url`, `--to-insert-url` | the Service URLs | vmctl's endpoints, e.g. after a port-forward. |
| `--vmctl`, `--vmctl-arg` | `vmctl`, none | vmctl binary and extra arguments (not `--vm-concurrency` or `--vm-rate-limit`). |

`migrate` only:

| Flag | Default | Meaning |
|---|---|---|
| `--headroom`, `--rate-window` | `0.2`, `30s` | The `preflight` flags. |
| `--poll-interval` | `1m` | How often to try again while a VMCluster is missing or a preflight check fails. |
| `--ready-after` | `10m` | A catch-up (copy plus verify) that takes less than this makes green ready for the cutover. |
| `--drain-interval` | `5m` | Blue must receive no rows for this long (at least `1m`); also the window over which the writes are measured to notice the cutover. |
| `--drain-timeout` | `20m` | How long blue may still receive writes after cutover + `--final-tail`. |
| `--final-tail` | `15m` | How long after the cutover the final sync waits before the drain check. |
| `--final-overlap` | `1h` | How far before the last synced point the final window starts. |

`sync` only:

| Flag | Default | Meaning |
|---|---|---|
| `--until` | `now` | Copy up to this time. |

`preflight`:

| Flag | Default | Meaning |
|---|---|---|
| `--headroom` | `0.2` | Free space green needs on top of blue's used bytes. |
| `--rate-window` | `30s` | Time between the two scrapes that measure ingestion rates. |

`verify` (and `migrate`, which verifies too):

| Flag | Default | Meaning |
|---|---|---|
| `--since`, `--until` | the backfilled days | Days to check (`verify` only). |
| `--top-n` | `10` | Count the samples of this many metric names with the most series. |
| `--random-n` | `10` | Plus this many names picked at random. |
| `--pool` | `1000` | Pick the random names from this many names below the top N. |
| `--tolerance` | `0.01` | Share by which green may fall short of blue. |

## Troubleshooting

The Job logs a reason with every failure. The most common ones:

| Message | What it means | What to do |
|---|---|---|
| `waiting for both VMClusters` | A VMCluster doesn't exist (yet), or can't be read. | Create green; check the names, the namespace and the Role. |
| `preflight check failed`, again and again | The Job waits for the change that fixes the check. | Fix the check as described below; the Job goes on by itself. |
| `green has no -dedup.minScrapeInterval` or `... disabled or invalid` | Overlapping windows would store samples twice. | Set `dedup.minScrapeInterval: 1ms` in the `extraArgs` of green's vmstorage and vmselect. |
| `version` fails | Blue and green run different image tags; the native format is version-coupled. | Run the same version on both. |
| `capacity` fails | Green has too little free disk, in total or on one node. | Larger PVCs, or more vmstorage nodes. |
| `green receives no live writes` | Dual write isn't set up. | Add green's URL to every VMAgent's `remoteWrite` (step 3). |
| `PodDisruptionBudget ... selects pods of both clusters` or `topologySpreadConstraints` | Blue and green pods are counted together. | Add `app.kubernetes.io/instance` to the selector. |
| `alias ...` fails | The alias Service is missing, has no selector, or selects the wrong cluster's pods (or both), or also pods of another component. | Fix its selector: `app.kubernetes.io/name` plus `app.kubernetes.io/instance` of the active cluster. |
| `relabeling` warns | Green's vminsert relabels the copied data again. | Make sure the rules don't change their own output, or remove them for the migration. |
| A chunk fails with an error about `maxExportSeries` | One hour has more series than blue's vmselect exports at once. | Raise `-search.maxExportSeries` on blue's vmselect. |
| `Green still misses data on these days after copying them again` | A day failed verify, and still fails after copying it again. | Find the cause (for example `-search.maxExportSeries`, or a writer that backfills blue). A new run verifies and copies the failed days again. |
| `can't verify the day, trying again after the next catch-up` | A query to blue or green failed (for example a timeout), so the day is unverified, not known to miss data. | Nothing: the next catch-up verifies it again, without copying it again. |
| `waiting for the cutover` long after you switched | Blue still receives rows, or green receives none. The log line shows both rates. | Point the remaining writers at green. Blue's vminsert metric `vm_rows_inserted_total` shows the protocol. |
| `the writers point at green, waiting for the aliases` | The writes moved, but an alias still selects blue. | Switch the alias selectors to green. |
| `the aliases select green, but blue still receives rows` | Some writer bypasses the aliases. | Find it as above and point it at the alias. |
| `blue still receives writes, so the cutover isn't done` | The final sync's drain check found writes on blue after `--final-tail`. Nothing was copied. | The Job goes back to catching up; point the remaining writers at green. |
| `vminsert pods restarted, changed or were terminating during every measurement` | The drain check couldn't measure: a pod restarted, or is shutting down and may still send rows. Nothing was copied, and the run fails. | Wait until blue's vminsert pods are stable; the restarted pod tries again. A pod stuck terminating (e.g. on a lost node) blocks the check until it is deleted. |
| `still fail to send to a vmstorage node` or `were still delivering rows` | Blue's vminsert still holds rows it accepted. Nothing was copied, and the run fails. | Fix blue's vmstorage nodes; the restarted pod tries again. |
| `forbidden`, `Unauthorized`, `401` or `403` while opening the VMClusters, in a preflight check, while measuring the writes, reading an alias or verifying | The ServiceAccount lacks a permission, or a VictoriaMetrics component refused the request. Waiting can't fix that, so the run fails at once (and every restart again). | Fix the Role (`rbac.create`), or the auth key of the VictoriaMetrics component that refused the request. |
| `earlier verify` fails | A day failed verify earlier, and no later verify of that time range passed. | `verify --since <day> --until <next day>`; if green really misses data, copy the day again with `sync` and its own `--state-file`, `--since` and `--until`. |
| `state belongs to the migration ...` | The state is for other clusters or another tenant. | Check `from`, `to`, `tenant` and `--state-file`. |
| `VMCluster ... was deleted and created again` | The saved progress belongs to a cluster that no longer exists. The run fails at once (and every restart again). | Delete the state to start over. |
| `verify` times out | The API server cut the request off (`--access=proxy`). | Run it inside the cluster, where it connects directly. |
