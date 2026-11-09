# victoriametrics-rebalance

`vmrebalance` moves the data of a VictoriaMetrics cluster to a new cluster with more vmstorage nodes, so that the
data ends up spread evenly across all of them, the way the ECK operator moves Elasticsearch data to a new nodeset.

VictoriaMetrics doesn't rebalance existing data when you add a vmstorage node: vminsert sends new series to the new
node, but old data stays where it is until retention removes it. Moving data between the nodes of one cluster isn't
safe, because metricIDs can collide across partitions (upstream issues
[#2324](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/2324) and
[#10792](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10792)).

So `vmrebalance` does a blue/green migration:

1. You create a second VMCluster ("green") with the target number of vmstorage nodes.
2. The tool copies the data from the old cluster ("blue") into green through green's vminsert, which spreads it
   evenly across all green nodes.
3. You switch readers and writers to green, the tool runs a last sync, and you delete blue.

The tool never deletes data and never changes VMCluster, VMAgent or Service objects. You make those changes,
usually in Git. The tool copies the data, checks every step and tells you what's due next.

**Not in scope:** rebalancing in place (blocked inside vmstorage), freeing blue's disks during the migration (blue
is deleted as a whole at the end), growing PVCs (the operator already supports that), and changing the Kubernetes
objects for you.

### Logs and traces

The tool works only for VictoriaMetrics metrics clusters (`VMCluster`). VictoriaLogs and VictoriaTraces clusters
don't need a blue/green migration. Their storage nodes keep the data in self-contained per-day partitions and can
detach and attach a partition while they run (`/internal/partition/detach` and `/internal/partition/attach`). So
historical data can be moved to new storage nodes in place: snapshot a day's partition on an old node, copy it into
the new node's `partitions` directory, detach it on the old node and attach it on the new one. vmstorage has no such
mechanism, and moving data between its nodes is unsafe because of the metricID collisions above, which is why this
tool copies the data into a new cluster instead.

This may change upstream. Since [#7599](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/7599), every
vmstorage partition has its own index, so partitions are self-contained on disk. What's still missing is
partition-dependent metricIDs, proposed in [#10792](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10792)
(open, no pull request yet). Once metricIDs can't collide across nodes, vmstorage partitions could be moved in place
like VictoriaLogs ones, without a blue/green copy.

See the VictoriaLogs docs on [rebalancing](https://docs.victoriametrics.com/victorialogs/cluster/#rebalancing) and
the [partitions lifecycle](https://docs.victoriametrics.com/victorialogs/#partitions-lifecycle). VictoriaTraces has
the [same endpoints](https://docs.victoriametrics.com/victoriatraces/#partitions-lifecycle).

Documentation:

- [docs/usage.md](docs/usage.md): a step-by-step migration with example manifests, how to watch the progress, all
  flags, rollback and troubleshooting.
- [docs/architecture.md](docs/architecture.md): how the tool works, with diagrams, design decisions, state, the
  adaptive rate limit and metrics.
- [docs/roadmap.md](docs/roadmap.md): what's done, what's next and the open decisions.

## How a migration runs

```text
time ─────────────────────────────────────────────────────────────────────────────────────────▶
                     T0                                    Tc  Tcut
blue   ═══ reads + writes ═══════════════════════════════════════╪═ soak (no writes) ══ delete
green                ├─ dual write from vmagent ─────────────────┼═ all reads + writes ═══════▶
sync   [ backfill: now-retention ── now ][ catch-up ── Tc ]      [ final: Tc-overlap ── drained ]
                                                                  └ drain check first, then copy, cache reset, verify
```

| Step | Who | What |
|---|---|---|
| 1. Prepare | you | Put stable aliases (Services or VMAuth) in front of vmselect and vminsert. Done once per setup. See [Requirements](#requirements). |
| 2. Create green | you | A new VMCluster with more vmstorage nodes, the same version, retention and replication factor, plus `dedup.minScrapeInterval: 1ms` on vmstorage and vmselect. |
| 3. Dual write | you | Add green as a second `remoteWrite` URL on your VMAgent(s). Green now receives most new data live. This is T0. |
| 4. `preflight` | tool | Checks versions, flags, capacity, dual write and pod labels. |
| 5. `sync` | tool | Backfill: copies `[now - retention, now]` in resumable chunks. This can take hours to days. |
| 6. `verify` | tool | Compares blue and green per day. |
| 7. `sync` | tool | Catch-up: copies from the last synced point to now. Repeat until a run takes only minutes. |
| 8. Cutover | you | Point the read and write aliases at green. This is Tcut. |
| 9. `sync --final` | tool | Waits until `Tcut + 15m` and checks that blue no longer receives writes (if it still does, it stops without copying). Then copies `[last synced point - --final-overlap (1h), the moment blue was drained]` (from `Tcut - --final-overlap` if a catch-up already ran past the cutover), waits 30s until green can search the rows, resets green's rollup cache, verifies the copied days, and checks that no earlier verified day still fails. |
| 10. Soak and delete | you | Keep blue for a few days so you can roll back, then delete blue and its PVCs. |

Why dual write *and* a final sync:

- Without dual write, every sample written between the last sync and the cutover would be missing on green until
  the final sync finishes. Dashboards and alerts would see a gap.
- Dual write through VMAgent covers most of the data with no gap. The final sync covers everything that doesn't go
  through VMAgent (vmalert recording rules, OpenTelemetry collectors, jobs that push to `/api/v1/import`), and fills
  in any samples green dropped during dual write.
- Overlapping windows are safe because green runs with `-dedup.minScrapeInterval=1ms`, which drops samples that
  share an exact timestamp. Without it, overlaps would be stored twice and would skew `count_over_time` and similar
  functions.

Rollback until step 10: switch the aliases back to blue. Blue still has everything up to the cutover. To copy data
written to green after the cutover back to blue, swap `--from` and `--to` and start at the cutover:
`vmrebalance sync --from vmcluster-b --to vmcluster --since <cutover>`. Without `--since`, the swapped run has a new
state and would copy green's whole retention.

## Requirements

1. **Stable endpoints for readers and writers.** The operator names Services after the VMCluster
   (`vmselect-<name>`, `vminsert-<name>`), and green has a different name, so clients that use those names can't
   be switched in one step. Add aliases you control: plain Services such as `vmselect-active` and
   `vminsert-active` that select `app.kubernetes.io/name: vmselect|vminsert` and
   `app.kubernetes.io/instance: <active cluster>`, or a VMAuth in front, whose VMUser `targetRefs` point at the
   alias or the active cluster. The cutover then means changing the alias's selector or target, not every client.
2. **Know every writer.** Writers that go through VMAgent get dual write. Every other writer (vmalert remote write,
   OpenTelemetry senders, jobs that push to `/api/v1/import`) is covered only by the final sync, so keep the time
   between the last catch-up and the cutover short. Or route them through VMAgent, which also accepts
   `/api/v1/write`, `/api/v1/import/*` and `/opentelemetry/v1/metrics`. If such a writer sends samples with old
   timestamps (for example a job that imports yesterday's data), the final sync reaches back only `--final-overlap`
   before the last synced point, and catch-up runs don't overlap. So set `--final-overlap` to at least how far back
   such a writer backdates plus how long the catch-up phase ran.
3. **Green must not block blue.** With `remoteWrite.disableOnDiskQueue`, a slow target can stall writes to all
   targets. Set `remoteWrite.dropSamplesOnOverload` for the green URL (check that your version allows it per URL),
   or enable the on-disk queue for the migration. The final sync fills in samples dropped there.
4. **Pod labels must not overlap.** If blue and green pods share the labels used in `topologySpreadConstraints` or
   PodDisruptionBudgets (for example a custom `app: vmstorage`), they are counted together: pods can fail to
   schedule, or evictions can be blocked. Select on `app.kubernetes.io/instance` too. `preflight` checks this.
5. **Capacity for a full second copy.** Green needs free disk for blue's used bytes plus headroom, and its own
   vmstorage memory and CPU, until blue is deleted. `preflight` checks the disk.
6. **Name-agnostic monitoring.** Dashboards, alerts and SLOs that match `job="vmstorage-<name>"` exactly lose green.
   Use regexes or the instance label.

## Install

Releases exist for Linux and macOS on amd64 and arm64; on other platforms, [build from source](#build-from-source).

### mise

Install the release with [mise](https://mise.jdx.dev), which picks the archive for your OS and architecture and
unpacks it:

```sh
mise use -g github:kokuwaio/victoriametrics-rebalance@latest
```

Upgrade with `mise upgrade github:kokuwaio/victoriametrics-rebalance`. mise skips releases younger than 24 hours by
default (`minimum_release_age`). To install a new release right away, pin it, e.g.
`mise use -g github:kokuwaio/victoriametrics-rebalance@0.1.0`.

GitHub limits API requests without a token. If mise fails with a rate limit, or with `404 Not Found` while the
repository is private, give it a token, for example the GitHub CLI's:

```sh
export MISE_GITHUB_TOKEN="$(gh auth token)"
```

`sync` also needs [vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/) in your `PATH` when it runs
outside the container image. vmctl ships in the `vmutils` archives of the VictoriaMetrics releases.

### Build from source

```sh
make build
```

This builds `./bin/vmrebalance` and needs the Go version from [go.mod](go.mod). `make help` lists the other targets.

### Container image

No image is published yet. `make image` builds one from the [Dockerfile](Dockerfile), with vmctl included, for
running `sync` as a Job.

## Usage

```sh
vmrebalance preflight --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance sync      --namespace monitoring --from vmcluster --to vmcluster-b --rate-limit 50MiB/s --adaptive --step 1h
vmrebalance verify    --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance status    --namespace monitoring --from vmcluster --to vmcluster-b
vmrebalance sync      --namespace monitoring --from vmcluster --to vmcluster-b --final --cutover-at 2026-01-02T10:00:00Z
```

[docs/usage.md](docs/usage.md) walks through every step, explains how to choose the rate limit and chunk size,
and lists all flags. `vmrebalance <command> --help` shows them too.

### Where to run it

`sync` runs [vmctl](https://docs.victoriametrics.com/victoriametrics/vmctl/) once per chunk. vmctl needs network
access to blue's vmselect and green's vminsert, so run `sync` as a Job in the cluster:
[deploy/rbac.yaml](deploy/rbac.yaml) holds the permissions, [deploy/job.yaml](deploy/job.yaml) is an example sync
Job and [deploy/job-final.yaml](deploy/job-final.yaml) one for the final sync.
No image is published yet, so build one from the [Dockerfile](Dockerfile), which bundles vmctl.

From a workstation, port-forward both Services and pass `--from-select-url http://localhost:8481` and
`--to-insert-url http://localhost:8480`, with `vmctl` in your `PATH`. `preflight`, `verify` and `status` work
from a workstation without port-forwarding.

## Checks

- `preflight`:
  - both clusters are operational;
  - green has more vmstorage nodes (otherwise it warns: there is nothing to rebalance);
  - all components run the same image tags (the native format is version-coupled);
  - same retention and replication factor;
  - green deduplicates, with exactly `1ms` (a larger value also merges real samples, so it warns);
  - green's vminsert doesn't relabel (it warns otherwise: the copied data, already relabeled by blue, would be
    relabeled again, so the rules must not change their own output);
  - green's vmstorage nodes have free space for blue's used bytes plus headroom, in total and each node for its
    share;
  - green receives live writes, and which share of blue's (the rest relies on the final sync);
  - no PodDisruptionBudget or topology spread selector matches pods of both clusters.
- `verify` only reads from the clusters, so it can run at any time, also during a sync. Per UTC day it compares the
  series count from `/api/v1/status/tsdb` and `count_over_time` sample counts for the top N metric names plus a
  random set (counting every series is too expensive on large clusters). Green passes with at least as many series
  and samples as blue, minus a tolerance (default 1%). On a partial first or last day only the sample counts within
  the range are compared, because the day's series count also includes series outside it.
- `sync --final`: before copying, blue's vminsert receives no rows during one `--drain-interval` within
  `--drain-timeout`, and has delivered every row it accepted (nothing buffered for vmstorage, no rows sent and no
  failed sends during the window, every vmstorage node reachable); otherwise some writer still points at blue or rows are
  still on their way (a window in which a vminsert pod restarted counts as unknown and is measured again). After
  copying, it waits 30 seconds until green has made the rows searchable, then resets the rollup cache on every green
  vmselect pod, so no pod serves results cached before the backfill, and runs `verify` for the days the final sync
  copied, failing on partial responses. It also fails while any day stored in the state failed verify and hasn't
  passed since.

## Risks

- **Copy speed.** A year of retention at several TiB can take days. The final sync must stay short, so the catch-up
  runs are what keep the cutover window small.
- **Series per export.** Each vmctl request exports all series of one hour, so blue's vmselect needs
  `-search.maxExportSeries` (default 10 million) at least as high as the series active in an hour. Otherwise every
  chunk fails with an error that names the limit.
- **Load on blue.** Export reads go through blue's vmselect and vmstorage and compete with real queries. Use rate
  limits, `--adaptive` and off-peak hours.
- **Cost.** A full second copy of the disks, plus vmstorage memory and CPU, for the duration of the migration.
- **Missed writers.** Anything that bypasses both the aliases and VMAgent writes to blue only, and its data is lost
  once blue is deleted. The drain check in `sync --final` is what catches it.

## Development

```sh
make check
make e2e
```

`make check` runs `go vet`, the unit tests with the race detector, and golangci-lint (if installed). `make e2e`
creates a kind cluster with its own kubeconfig (your current context stays untouched), installs the operator, two
small VMClusters and a VMAgent writing to both, and runs a full migration. It needs docker, kind, kubectl and helm.
Set `KEEP_CLUSTER=1` to keep the cluster for another run. Pull requests run both.

### Releasing

Every push to `main` updates a draft release (Release Drafter, notes grouped by pull request label;
`skip-changelog` hides a pull request). Publishing the draft creates the `vX.Y.Z` tag, which runs `make check` and
builds the binaries for Linux and macOS (amd64, arm64) with GoReleaser and attaches them with a checksum file.
`vmrebalance --version` prints the release version.
