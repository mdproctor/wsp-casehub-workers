# Handoff — 2026-06-09 (HTTP Worker Brainstorm In Progress)

**Head commit (project):** c1f77f5 — chore: cleanup and code review fixes
**Head commit (workspace):** 91f7518 — archive plans to attic

## Cross-Repo Commits This Session

- `casehubio/parent` main: `079d72d` — docs: add outbound authentication coherence policy to PLATFORM.md

---

## What Happened

Implemented `workers-common` and `workers-camel` per the approved spec (7 review cycles). 47 tests, code review with 3 fixes (submitAsync event-loop block, @ConsumeEvent return type, negative TTL guard). Squashed 13 commits to 3, PR #4 merged to main. Issues #1, #2, #3 closed. Branch closed and stamped.

---

## Immediate Next Step

**Brainstorm the HTTP worker** (`workers-http`). Simpler than Camel — implements the same `WorkerExecutionManager` + `ReactiveWorkerProvisioner` SPIs but dispatches via HTTP POST. `workers-common` infrastructure (completion registry, callback endpoint, fault events) is ready to reuse. Run brainstorming to produce a spec.

---

## Cross-Module

**Blocked by:**
- `casehub-engine` — engine#447 `NoOpWorkerExecutionManager @DefaultBean` must land before deployment without both scheduler-quartz AND workers-camel · S · Low

---

## What's Left

- `casehubio/parent` — add casehub-workers to build dependency order in PLATFORM.md · XS · Low
- `casehubio/parent` — add casehub-workers to Cross-Repo Dependency Map in PLATFORM.md · XS · Low

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| — | Brainstorm workers-http | S | Low | Immediate — simpler SPI impl, reuses workers-common |
| — | MCP worker design | M | Med | Priority 3 — any MCP server's tools become dispatchable |
| — | Script worker | S | Low | Shell/Python/JS subprocess execution |
| — | GitHub Actions worker | S | Low | REST API trigger — relevant to devtown |
| — | Composite WorkerExecutionManager in engine | M | Med | Required for co-deployment of Camel + Quartz |

---

## Key References

- Spec: `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md`
- Blog: workspace `blog/2026-06-08-mdp01-workers-common-camel.md`
- Plan: workspace `plans/attic/issue-1-implement-workers-common-camel/`
