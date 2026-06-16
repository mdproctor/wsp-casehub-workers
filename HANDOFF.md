# Handoff — 2026-06-16 (Script Worker + Fault Pipeline Extraction)

**Head commit (project):** 35cc685 — fix(#9): migrate test Worker construction to builder API
**Head commit (workspace):** fb4ee4c — archive plans

---

## What Happened

Designed and implemented `workers-script` (#9) — subprocess execution worker. 3-cycle spec review (16 findings). Prerequisite: extracted fault pipeline (publisher, handler, CDI observers) from 4 per-module copies to workers-common generics, fixing Camel PermanentFaultException bug. Deleted 8 classes, -1,391 lines. Script worker: 8 classes, config-driven subprocess execution via ProcessBuilder, stdin JSON delivery, bounded stdout/stderr capture, exit code classification. 25 tests. Squashed 18 commits → 3 + 1 (upstream SNAPSHOT fix). Issue #9 closed.

---

## Immediate Next Step

**Pick the next worker type or cross-cutting work.** Options: composite `WorkerExecutionManager` in engine (engine#461 — required for co-deploying multiple workers), or a new worker type.

---

## Cross-Module

**Blocked by:**
- `casehub-platform` — platform#73 `EndpointRegistry` SPI for named endpoint resolution (HTTP Tier 3 + future MCP Tier 2 auth) · M · Med
- `casehub-engine` — engine#461 composite `WorkerExecutionManager` for co-deploying HTTP + Camel + GitHub Actions + MCP + Script + Quartz · M · Med

---

## What's Left

- `casehubio/parent` — parent#225 sync PLATFORM.md for workers-github-actions + workers-mcp + workers-script modules · XS · Low
- Upstream SNAPSHOT break: `Worker` constructor went private — fixed in tests, but `WorkerTestSupport` should be reviewed if engine-api publishes · XS · Low

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| — | Composite WorkerExecutionManager in engine | M | Med | engine#461 — required for co-deployment |

---

## Key References

- Spec: `docs/superpowers/specs/2026-06-16-casehub-workers-script-design.md`
- Blog: workspace `blog/2026-06-16-mdp04-fifth-worker-extraction.md`
- Plan: workspace `plans/attic/issue-9-workers-script/`
- Garden: GE-20260529-b994c2 (revised — Mutiny runSubscriptionOn timer variant)
