# Handoff — 2026-07-06 (K8s restart recovery shipped)

**Head commit (project):** da93efe — feat(#17): implement workers-k8s restart recovery
**Head commit (workspace):** see `git log -1` on workspace main

---

## What Happened

K8s restart recovery (#17) shipped. Four changes: enriched Job labels (case-id, worker-name, event-log-id, idempotency), recovery path in processTerminal() with at-most-once guard, schedulePersistedEvent() for crash-before-Job-creation, eager resolver init via @PostConstruct. Design review ran 4 rounds / 17 issues — surfaced the startup priority race (engine recovery at @Priority(22) vs worker init at @Priority(2010)) and the at-most-once guard requirement. Garden entry GE-20260704-294d67 submitted for the startup ordering gotcha. 80 tests in workers-k8s.

Pre-existing test failures found in workers-common: `EventLogRepository.findById()` and `CaseInstanceRepository.findByUuid()` changed from `Uni<T>` to `T` in engine-common — 6 tests in workers-common reference the old Uni signatures.

---

## Immediate Next Step

Fix the pre-existing workers-common test failures (6 tests — WorkerFaultHandlerTest, WorkerRetrySupportTest). Same pattern as the K8s fix: replace `Uni.createFrom().item(x)` mocks with direct returns. File an issue if not already tracked.

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| #16 | Multi-cluster K8s dispatch | M | Med | Per-cluster KubernetesClient, per-cluster informers |
| — | Fix workers-common test failures (EventLogRepository API migration) | XS | Low | 6 tests, mechanical Uni→blocking migration |

---

## Key References

- Spec: `docs/superpowers/specs/2026-07-03-k8s-restart-recovery-design.md`
- Blog: workspace `blog/2026-07-03-mdp08-k8s-restart-recovery.md`
- Plan: workspace `plans/attic/issue-17-k8s-restart-recovery/2026-07-03-k8s-restart-recovery.md`
- Garden: GE-20260704-294d67 — startup ordering gotcha (engine recovery vs worker init priority race)
