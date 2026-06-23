# Handoff — 2026-06-23 (engine#530/531 wired, #15 closed)

**Head commit (project):** 3f766a2 — docs: sync ARC42STORIES.MD — stale engine#530 reference updated
**Head commit (workspace):** see `git log -1` on workspace main

---

## What Happened

Wired engine#530 (`tenancyId` in `ProvisionContext`) into all 4 endpoint-resolving provisioners. Replaced `PLATFORM_TENANT_ID` workaround with `context.tenancyId()`. Removed dead `WorkerProvisionerSupport.tenancyId()` helper. Fixed 3 test files passing null `ProvisionContext`. engine#531 (getCapabilities hard gate) required no workers-side changes. Both cross-repo blockers resolved. #15 closed, CI green.

---

## Immediate Next Step

Pick the next work item from What's Next. #10 (workers-k8s) is the only remaining workers-side feature work. engine#461 is engine-side.

---

## Cross-Module

**Blocked by:**
- `casehubio/engine` — engine#461 composite `WorkerExecutionManager` for co-deployment · M · Med

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| #14 | Migrate Worker imports to casehub-worker-api | M | Low | Depends on worker-api being published |
| #10 | workers-k8s — Kubernetes Job dispatch worker | L | Med | Watch-based completion model |
| — | Composite WorkerExecutionManager in engine | M | Med | engine#461 — required for co-deployment |

---

## Key References

- Blog: workspace `blog/2026-06-23-mdp07-cross-repo-blockers-resolved.md`
- Garden: GE-20260601-a35fb3 (tenancyId null pattern — hit again in tests this session)
