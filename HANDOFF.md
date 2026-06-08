# Handoff — 2026-06-08

**Head commit (project):** 63b0816 — docs: spec v8 — package conventions, Camel component hierarchy, engine#447
**Head commit (workspace):** (update after committing this file)

---

## What Was Done This Session

The full `workers-camel` + `workers-common` design was brainstormed, specced, and reviewed through **7 review cycles** in the `casehubio/parent` session. The spec is approved and ready for implementation.

Key decisions made:
- `workers-common` is the shared async worker infrastructure layer (all worker types use it); future migration target is `casehub-engine` alongside Drools and Flow
- Camel implements **both** `ReactiveWorkerProvisioner` (capability probe) and `WorkerExecutionManager` (actual dispatch) — two separate call sites in the engine
- Completion fires `WorkflowExecutionCompleted` on `WORKER_EXECUTION_FINISHED` via `eventBus.publish()` — never `request()`
- Camel faults use a **separate address** `CAMEL_WORKER_FAULT` (not `WORKFLOW_EXECUTION_FAILED`) to avoid double-handling by `QuartzWorkerExecutionJobListener`
- `FaultCallbackEvent` CDI async (not a SPI) for REST callback fault routing — no `@DefaultBean` needed
- `PendingCompletion.workerType` discriminator for multi-worker-module co-deployment safety
- Retry mirrors Quartz exactly: `failureCount < retryPolicy.maxAttempts()`, `computeBackoffDelayMs()`, Vert.x timer with `emitOn` (not `runSubscriptionOn`)
- engine#447 filed: `NoOpWorkerExecutionManager @DefaultBean` must be added to `casehub-engine` runtime

---

## Immediate Next Step

**Implement `workers-common` and `workers-camel` per the approved spec.**

Spec: `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md`

Before implementing, invoke `superpowers:writing-plans` to turn the spec into an implementation plan. The plan lands in workspace `plans/`.

**engine#447 dependency:** `NoOpWorkerExecutionManager @DefaultBean` is a required engine deliverable. The `workers-common` and `workers-camel` modules can be implemented independently without it — the CDI gap only affects a deployment with neither Quartz nor Camel. Do not block implementation on it, but note it in the plan.

---

## What's Left

| Item | Repo | Status |
|------|------|--------|
| Implement `workers-common` | casehub-workers | Not started — spec complete |
| Implement `workers-camel` | casehub-workers | Not started — spec complete |
| Add `workers-common` to parent POM | casehub-workers | Not started (skeleton has http, camel, testing only) |
| `NoOpWorkerExecutionManager @DefaultBean` | casehub-engine | engine#447 — open |
| `casehub-endpoints` module | casehub-platform | platform#73 — open |
| Update PLATFORM.md with casehub-workers build order | casehub-parent | Not started |
| `workers-http` spec + implementation | casehub-workers | Future — no spec yet |
| MCP worker, Script worker | casehub-workers | Future |

---

## Spec Summary (key contracts for implementors)

### workers-common — what to build

- `PendingCompletion` record — `dispatchId` (UUID), `workerType` (String discriminator), `callbackToken` (UUID), `correlationContext`, `capability`, `eventLogId`, `registeredAt`, `expiresAt`, `provisionerMeta`
- `AsyncWorkerCompletionRegistry` — ConcurrentHashMap; `register(workerType, ctx, capability, eventLogId, ttl, meta)`; `complete(dispatchId)`; `@Scheduled(every="${casehub.workers.async.expiry-check-interval:5m}") @Blocking expireStale()` using `computeIfPresent` atomicity
- `WorkflowCompletionPublisher` — `eventBus.publish(WORKER_EXECUTION_FINISHED, WorkflowExecutionCompleted.approved(...))` — `publish()` not `request()`
- `WorkerCallbackResource` — `POST /workers/complete/{dispatchId}` with `X-Casehub-Callback-Token` header; fires `FaultCallbackEvent` CDI async on fault
- `FaultCallbackEvent(PendingCompletion, Throwable)` — CDI async event
- `CompletionExpiredEvent(PendingCompletion)` — CDI async event fired by `expireStale()`
- `CasehubWorkerHeaders` — constants: `casehub-worker-id`, `casehub-idempotency`, `casehub-case-id`, `casehub-tenancy-id`, `casehub-task-type`, `casehub-callback-token`, `casehub-work-status`

### workers-camel — what to build

- `CamelWorkerConstants.WORKER_TYPE = "camel"` and `CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT = "casehub.workers.camel.fault"`
- `CamelReactiveWorkerProvisioner @ApplicationScoped` — validates route exists, returns `ProvisionResult.empty()`; reads capability cache from `CamelCapabilityResolver`
- `CamelWorkerExecutionManager @ApplicationScoped` — dispatches sync (InOut) and async (InOnly) routes; fires fault on `CAMEL_WORKER_FAULT` via `CamelWorkerFaultPublisher`; no-op `schedulePersistedEvent()`
- `CamelCapabilityResolver` — three-tier: SPI (`CamelWorkerRoute` CDI beans) → config (`casehub.workers.camel.capabilities.<tag>`) → convention (route ID = capability tag AND `from: direct:{tag}`); initialised via `@Observes @Priority(APPLICATION) StartupEvent`
- `CasehubCamelComponent` → `CasehubEndpoint` → `CasehubProducer` — reads `casehub-worker-id` header (= dispatchId), calls registry.complete(), fires completion or fault
- `CamelWorkerFaultPublisher` — fires `WorkflowExecutionFailed` on `CAMEL_WORKER_FAULT`
- `CamelWorkerFaultEventHandler @ConsumeEvent(CAMEL_WORKER_FAULT, blocking=true)` — persists `WORKER_EXECUTION_FAILED` event log, counts via `findByCaseAndWorkerAndType` filtered by `metadata.inputDataHash`, retries with `emitOn(workerPool)` after Vert.x timer, exhausts with `WORKER_RETRIES_EXHAUSTED`
- `CamelCompletionExpiryObserver` — `@ObservesAsync CompletionExpiredEvent`, filters `workerType == "camel"`, calls `CamelWorkerFaultPublisher.fault()`
- `CamelFaultCallbackObserver` — `@ObservesAsync FaultCallbackEvent`, filters `workerType == "camel"`, calls `CamelWorkerFaultPublisher.fault()`

### Critical implementation details

- `resolveRetryPolicy()` always returns non-null — `null` case returns `new RetryPolicy()` (3 attempts, 10s FIXED), matching Quartz
- `computeBackoffDelayMs()`: FIXED (constant), EXPONENTIAL (delayMs × 2^(attempt-1), cap 30s), EXPONENTIAL_WITH_JITTER — identical logic to `QuartzWorkerExecutionJobListener`
- `reloadAndResubmit()` delay uses `emitOn(Infrastructure.getDefaultWorkerPool())` after Vert.x timer (`setTimer` + `onTermination` cancel) — NOT `runSubscriptionOn`
- `countFailedAttempts()` uses `eventLogRepository.findByCaseAndWorkerAndType(caseId, workerName, WORKER_EXECUTION_FAILED, tenancyId)` filtered by `metadata.get("inputDataHash").asText().equals(inputDataHash)`
- `WorkerRetriesExhaustedEvent(caseId, workerId, inputDataHash)` — third arg maps to `idempotency` record component

---

## Key References

- **Spec:** `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md` — read this in full before writing a plan
- **Engine reference impl:** `casehubio/claudony` casehub/ module — `ClaudonyWorkerExecutionManager`, `ClaudonyReactiveWorkerProvisioner`
- **Quartz retry reference:** `casehubio/engine` `scheduler-quartz/QuartzWorkerExecutionJobListener.java` — mirrors all retry logic
- **engine#447:** `NoOpWorkerExecutionManager @DefaultBean` engine deliverable
- **platform#73:** `casehub-endpoints` EndpointRegistry (workers work without it; inline Camel URIs are the fallback)
- **Research doc:** `casehubio/parent` `docs/superpowers/research/2026-06-07-desired-state-management-research.md`
