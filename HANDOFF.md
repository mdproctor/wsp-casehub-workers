# Handoff — 2026-06-19 (Ecosystem CI wiring)

**Head commit (project):** c12655b — fix(#13): align WorkerRetriesExhaustedEvent with published 5-arg constructor
**Head commit (workspace):** f9a3f6b — docs: add project blog entry 2026-06-19

---

## What Happened

Set up CI for casehub-workers (#13) and audited the full ecosystem. Migrated 15 repo origins from `mdproctor/` to `casehubio/`. Added 6 missing repos (iot, neural-text, desiredstate, ras, ops, quarkmind) to build-all.sh, aggregator.xml, and update-pointers.yml. Removed stale `flow` entry (renamed to `scaffold`). Upgraded claudony and aml to full publish workflows with `<distributionManagement>`. Fixed three CI failures: GitHub Packages redirect alias, broken pipe on subprocess stdin, SNAPSHOT constructor drift. Updated parent README with full ecosystem tables and trigger chain docs. CI is green.

---

## Immediate Next Step

**Close #13** (CI setup issue) — work is complete and CI is green. Then pick the next work item.

---

## Cross-Module

**Blocked by:**
- `casehubio/engine` — engine#461 composite `WorkerExecutionManager` for co-deployment · M · Med
- `casehubio/engine` — engine#530 add tenancyId to ProvisionContext · XS · Low
- `casehubio/engine` — engine#531 remove getCapabilities() hard gate · XS · Low

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| #10 | workers-k8s — Kubernetes Job dispatch worker | L | Med | Watch-based completion model |
| — | Composite WorkerExecutionManager in engine | M | Med | engine#461 — required for co-deployment |
| — | engine#530 + engine#531 | XS | Low | Prerequisite for full tenant-aware provisioning |

---

## Key References

- Blog: workspace `blog/2026-06-19-mdp06-ecosystem-ci-wiring.md`
- Garden: GE-20260619-31e6e4 (GitHub Packages redirect), GE-20260619-c99452 (broken pipe), GE-20260619-f8b50c (SNAPSHOT commit install)
