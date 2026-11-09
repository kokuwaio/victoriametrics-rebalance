# Roadmap

## Milestones

| # | Milestone | Status |
|---|---|---|
| 1 | **Manual run-through.** Run the steps by hand on a test cluster with `vmctl`, and measure GB/h copied, the CPU and I/O load on blue's vmstorage and vmselect, disk growth on green, and the gap the final sync has to close. This sets the default chunk size, concurrency and rate limit, and shows whether the approach is fast enough at all. | open |
| 2 | **MVP CLI.** `preflight`, `sync` with the vmctl engine, state in a ConfigMap (or a local file), `status`. Unit tests with fakes, and an end-to-end test on kind with two small VMClusters. | done |
| 3 | **Verify and final sync.** `verify`, `sync --final` (rollup cache reset, blue drain check, verify), the Helm chart and a container image. | done |
| 4 | **Hardening.** Retries with backoff per chunk, a rate limit that adapts to blue's load (`--adaptive`), and Prometheus metrics for the tool (`--metrics-addr`). | done |
| 5 | **Optional.** An `--apply` mode for setups without GitOps (patch the VMAgent `remoteWrite` and the alias Services directly), a copy engine written in Go, and a proposal to the operator project once this has run in production. | open |

## Open decisions

- **Chunk size.** One day is the default. Large clusters may need one hour. Splitting per metric name (vmctl's
  per-metric mode) is off, because its metric discovery accepts partial results. With `--adaptive`, smaller chunks
  react faster to load on blue. Decide after milestone 1.
- **Adaptive rate within a chunk.** vmctl reads its rate limit once per run, so `--adaptive` can only change the
  rate between chunks. A copy engine written in Go could throttle continuously.
- **Should the tool create green?** For now it doesn't, because a GitOps repository owns the objects. Revisit with
  `--apply`.
- **Multi-tenancy.** The tool copies one tenant per run (`--tenant`). Copying all tenants (vmctl's
  `--vm-intercluster`) comes when someone needs it.
- **Moving partitions in place.** If upstream makes metricIDs partition-dependent
  ([#10792](https://github.com/VictoriaMetrics/VictoriaMetrics/issues/10792)), vmstorage partitions could be moved
  between nodes like VictoriaLogs partitions, which would be much faster than copying through vmctl. Then this tool
  could offer that as a second engine, or no longer be needed.
- **Default for `--adaptive`.** It is off, so the rate stays exactly as configured. Turn it on by default once
  milestone 1 has shown sensible thresholds.
