# AGENTS.md

`vmrebalance` migrates a VictoriaMetrics VMCluster (blue) to a new one with more vmstorage nodes (green): it copies
the data with vmctl and checks the cutover, and the Helm chart runs the whole migration (`migrate`) as one Job.

- What it does and how to use it: [README.md](README.md), [docs/usage.md](docs/usage.md).
- How it works (phases, state, final sync, metrics): read [docs/architecture.md](docs/architecture.md) before changing
  `internal/cli/migrate.go`, `final.go` or `internal/migration`.

## Invariants

- The tool only reads the cluster. It writes its state ConfigMap and resets green's rollup cache, nothing else:
  people create green, switch dual write and the aliases, and delete blue themselves, usually through GitOps.
- Every run resumes from the state. Each command changes only its own fields through `Store.Update`.
- A migration completes only when no data can be missing: every full day has a whole-day verify pass, blue is
  drained, the aliases select green. Loss of data is the failure this tool exists to prevent; weigh every change
  against it.
- Errors split into two kinds. Retry what waiting can fix (a VMCluster not created yet, a timeout). Fail at once on
  what it can't: `permanent()` in `internal/cli/migrate.go` (a VMCluster created again, 401/403, missing RBAC).

## Before a pull request

- `make check` (vet, race tests, golangci-lint) must pass. Run `make chart` when the chart changes, and `make e2e`
  (kind, needs docker, kind, kubectl, helm) when the migrate flow or the chart's Job changes.
- Tests sit at public seams: `migrator.run` (internal/cli/migrate_test.go), `finalSync` (commands_test.go),
  `check.Preflight`, `check.CheckAliases`. The fakes in `fake_test.go` and the fake clock drive time; blue and green
  are measured at once, so test hooks that count calls use atomics.
- Conventional commits (`feat(migrate): ...`, `fix(chart): ...`, `docs: ...`); `!` and a `BREAKING CHANGE:` footer
  for removed flags or chart values. Label the PR for Release Drafter: `enhancement`, `bug`, `documentation`,
  `dependencies`.

## Keep docs and code in step

One behaviour is usually described in several places. When it changes, update all that apply:

- the command's `Long` help and flag help in `internal/cli`,
- README.md (steps table, Usage, Checks),
- docs/usage.md (steps, flag reference, the troubleshooting table, which quotes log messages verbatim),
- docs/architecture.md (flowcharts and the state table),
- `charts/vmrebalance/values.yaml` comments and `templates/NOTES.txt`.

## Conventions

- Prose: short, plain, active sentences, wrapped at 120 columns (tables, code and mermaid excepted).
- This repository is public: names, examples and test data stay generic (`vmcluster`, `vmcluster-b`,
  `vmselect-active`).
- Chart: bump `version` in `Chart.yaml` with every chart change (chart-testing enforces it), and `appVersion` when the
  chart should run a new release. Keys in `values.yaml` stay sorted, each with a `# --` comment; `ci/*-values.yaml`
  cover the values that render something.
- Shell scripts run on Linux and macOS: POSIX tools and flags, bash 3.2.
