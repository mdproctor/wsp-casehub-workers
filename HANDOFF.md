# Handoff — 2026-06-25 (#14 closed, worker-api migration complete)

**Head commit (project):** 342069f — refactor(#14): migrate Worker imports to casehub-worker-api
**Head commit (workspace):** see `git log -1` on workspace main

---

## What Happened

Migrated all Worker/Capability imports from `io.casehub.api.model` to `io.casehub.worker.api` (records from casehub-worker-api) and governance types (ExecutionPolicy, RetryPolicy, BackoffStrategy) to `io.casehub.platform.api.governance`. 35 files, 44 changed overall. #14 closed. CI green after rebuilding engine locally to get the latest engine-common SNAPSHOT (engine#543 was merged in source but the SNAPSHOT wasn't published to m2 — now resolved).

---

## Immediate Next Step

Pick the next work item from What's Next. #10 (workers-k8s) is the remaining workers-side feature work. engine#461 is engine-side.

---

## Cross-Module

**Blocked by:**
- `casehubio/engine` — engine#461 composite `WorkerExecutionManager` for co-deployment · M · Med

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| #10 | workers-k8s — Kubernetes Job dispatch worker | L | Med | Watch-based completion model |
| — | Composite WorkerExecutionManager in engine | M | Med | engine#461 — required for co-deployment |

---

## Key References

- Spec: `docs/superpowers/specs/2026-06-25-migrate-worker-api-imports-design.md`
- Blog: workspace `blog/2026-06-25-mdp08-worker-foundation-extraction.md`
- Garden: GE-20260624-0b931d (Capability null schema), GE-20260624-3324b6 (Builder lambda ambiguity)
