---
layout: post
title: "Worker Foundation Extraction — the workers side"
date: 2026-06-25
type: phase-update
entry_type: note
subtype: diary
projects: [casehub-workers]
tags: [migration, worker-api, dependency-topology]
series: issue-14-migrate-worker-api-imports
---

*Part of a series on [#14 — migrate Worker imports to casehub-worker-api](https://github.com/casehubio/workers/issues/14). Previous: [Cross-repo blockers resolved](2026-06-23-mdp07-cross-repo-blockers-resolved.md).*

The issue said "migrate Worker imports to casehub-worker-api." I expected a mechanical find-and-replace — swap five import paths, done in an hour. It wasn't.

The first thing I checked was whether the engine SPIs had already moved to the new types. They had. `WorkerExecutionManager.submit()` in engine-common already takes `io.casehub.worker.api.Worker` and `io.casehub.worker.api.Capability` — the new records. Every workers implementation still imported the old `io.casehub.api.model` classes. These are different types. The build was one SNAPSHOT pull away from failing to compile.

So not a cleanup — a mandatory migration that was already overdue.

The issue's own mapping table listed six types. The codebase uses eleven from `io.casehub.api.model.*`. Five were missing: `ProvisionContext` (stays in engine-api — it's the engine's SPI parameter), `BackoffStrategy`, `RetryPolicy` (both now canonical in `io.casehub.platform.api.governance`), `WorkResult` (stays — it's an orchestration-level type, not a worker primitive), and the event types `CaseHubEventType` and `EventStreamType` (stay — engine event model). The governance types were the interesting ones. ExecutionPolicy, RetryPolicy, BackoffStrategy all existed in both engine-api and platform-api. This migration commits to platform-api as their canonical home — governance is a platform concern, not an engine concern.

Three breaks weren't in the issue description at all.

First: `Capability` changed from a class with a 3-arg constructor to a record with a 4-arg constructor (adds `description`). Every `new Capability(tag, "", "")` in tests — 28 of them — stopped compiling. The record provides `Capability.of(name, inputSchema, outputSchema)` which fills `description = null`. The garden already had a gotcha (GE-20260624-0b931d) about the null-rejection constraint, but the constructor signature change itself was undocumented.

Second: `Worker.Builder.function()` gained a second overload. The old builder had `function(Function<Map,Map>)`. The new builder has both `function(WorkerFunction)` and `function(Function<Map,WorkerResult>)`. Pass a lambda and Java can't pick between them. Twenty call sites across test code all used `.function(ctx -> null)` — every one needed wrapping: `new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of()))`. The garden had this one too (GE-20260624-3324b6), and it was the right fix.

Third: Worker is now an immutable record. Two test files had been calling `worker.setExecutionPolicy(policy)` after construction — post-build mutation that records don't support. Claude caught this during implementation and refactored those tests to use the builder chain instead. A consequence nobody had mapped because the tests compiled fine against the old mutable class.

The migration touched 35 files (13 production, 22 test) across all seven modules. 70 import swaps, 34 new imports for `WorkerFunction` and `WorkerResult`, 31 getter renames (`getName()` → `name()`), 28 Capability constructor fixes, 20 builder function fixes. Four commits: POMs, workers-common production, test fixtures + workers-common tests, per-module code. Full build green.

The interesting part isn't the migration itself — that's mechanical. It's the dependency topology that emerges. Workers now depends on three distinct API artifacts: `casehub-worker-api` for Worker/Capability records, `casehub-engine-api` for the provisioner SPI and event types, and `casehub-platform-api` for governance types and endpoint resolution. Each artifact owns a different concern. Before this migration, engine-api was a grab-bag that exported all three. Now the boundaries are explicit, and a module that only needs Worker primitives doesn't pull in the engine at all.
