# bindingName Propagation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use
> subagent-driven-development (recommended) or executing-plans to
> implement this plan task-by-task. Each task follows TDD
> (test-driven-development) and uses ide-tooling for structural
> editing. Steps use checkbox (`- [ ]`) syntax for tracking.

**Focal issue:** #18 — K8s: propagate bindingName through worker completion path
**Issue group:** #18

**Goal:** Thread `bindingName` from engine dispatch through all worker completion, fault, and recovery paths so the engine resolves the correct capability binding without fallback iteration.

**Architecture:** Add `bindingName` to `WorkerCorrelationContext` (workers-common). Since `PendingCompletion` holds the context, all completion/callback/recovery paths get `bindingName` automatically. Each worker overrides the new 6-arg `submit()` to receive `bindingName` from the engine. K8s persists `bindingName` as a Job annotation for restart recovery. `WorkerFaultEvent` gains `bindingName` so the fault handler can thread it through retry re-dispatch and retries-exhausted.

**Tech Stack:** Java 21, Quarkus 3.x, Mutiny, fabric8 K8s client, Mockito, AssertJ

## Global Constraints

- `bindingName` is nullable throughout — null means "engine falls back to `findMatchingCapabilityBinding()`"
- K8s uses an **annotation** (not label) for `bindingName` — annotations have no 63-char limit
- All 6 worker `submit()` overrides ship together (PP-20260530-88cdf9)
- Engine dependency: casehubio/engine#676 must ship before `bindingName` flows end-to-end. Workers-side code compiles and tests pass with `bindingName = null` until then.
- No backward-compat shims (PP-20260522-3b1ccd) — callers break explicitly
- Build: `mvn --batch-mode install` from repo root
- Tests: `mvn --batch-mode test -pl <module>` for module-scoped test runs

---

### Task 1: workers-common Foundation — Records and Publishers

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerCorrelationContext.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkflowCompletionPublisher.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultEvent.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultPublisher.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerCorrelationContextTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkflowCompletionPublisherTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultPublisherTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/PendingCompletionTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerCallbackResourceTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultCallbackObserverTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerCompletionExpiryObserverTest.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/AsyncWorkerCompletionRegistryTest.java`

**Interfaces:**
- Produces: `WorkerCorrelationContext(CaseInstance, Worker, String idempotency, String tenancyId, String bindingName)` — all downstream tasks construct this
- Produces: `WorkerFaultEvent(CaseInstance, Worker, Capability, String inputDataHash, String eventLogId, Throwable cause, String bindingName)` — Task 2 depends on this
- Produces: `WorkflowCompletionPublisher.complete(WorkerCorrelationContext, Map<String, Object>)` — signature unchanged, behavior now passes `ctx.bindingName()`
- Produces: `WorkerFaultPublisher.fault(String, WorkerCorrelationContext, Capability, Long, Throwable)` — signature unchanged, behavior now passes `ctx.bindingName()`
- Produces: `WorkerFaultPublisher.fault(PendingCompletion, Throwable)` — signature unchanged, behavior now passes `correlationContext().bindingName()`

#### Step 1: Write failing tests for WorkerCorrelationContext

Add a test verifying the `bindingName` field. This won't compile until the record is updated.

```java
// WorkerCorrelationContextTest.java — add test
@Test
void bindingName_carriedThrough() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-123", "tenant-1", "my-binding");
    assertThat(ctx.bindingName()).isEqualTo("my-binding");
}

@Test
void bindingName_nullable() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-123", "tenant-1", null);
    assertThat(ctx.bindingName()).isNull();
}
```

Update existing test construction site (line 19) to pass null as 5th arg:
```java
new WorkerCorrelationContext(instance, worker, "hash-123", "tenant-1", null)
```

#### Step 2: Update WorkerCorrelationContext record

```java
public record WorkerCorrelationContext(
    CaseInstance caseInstance,
    Worker worker,
    String idempotency,
    String tenancyId,
    String bindingName
) {}
```

#### Step 3: Write failing test for WorkflowCompletionPublisher

Update the existing test (line 29) to pass a bindingName through the context and verify it reaches `WorkflowExecutionCompleted`:

```java
// WorkflowCompletionPublisherTest.java — update existing test
WorkerCorrelationContext ctx = new WorkerCorrelationContext(
    instance, worker, "hash-1", "t1", "my-binding");
// ... existing test body ...
// Add assertion that the published event has bindingName = "my-binding"
```

Add a new test:
```java
@Test
void complete_passesBindingNameFromContext() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-1", "t1", "binding-x");
    publisher.complete(ctx, Map.of());

    ArgumentCaptor<WorkflowExecutionCompleted> captor =
        ArgumentCaptor.forClass(WorkflowExecutionCompleted.class);
    verify(eventBus).publish(eq(EventBusAddresses.WORKER_EXECUTION_FINISHED), captor.capture());
    assertThat(captor.getValue().bindingName()).isEqualTo("binding-x");
}

@Test
void complete_nullBindingName_passesNull() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-1", "t1", null);
    publisher.complete(ctx, Map.of());

    ArgumentCaptor<WorkflowExecutionCompleted> captor =
        ArgumentCaptor.forClass(WorkflowExecutionCompleted.class);
    verify(eventBus).publish(eq(EventBusAddresses.WORKER_EXECUTION_FINISHED), captor.capture());
    assertThat(captor.getValue().bindingName()).isNull();
}
```

#### Step 4: Update WorkflowCompletionPublisher

Replace `null` with `ctx.bindingName()` on line 19:

```java
WorkflowExecutionCompleted.approved(
    ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output, ctx.bindingName()));
```

#### Step 5: Write failing tests for WorkerFaultEvent

Add tests in a new or existing test class verifying the `bindingName` field:

```java
@Test
void bindingName_carriedThrough() {
    WorkerFaultEvent event = new WorkerFaultEvent(
        instance, worker, capability, "hash", "42", cause, "my-binding");
    assertThat(event.bindingName()).isEqualTo("my-binding");
}

@Test
void bindingName_nullable() {
    WorkerFaultEvent event = new WorkerFaultEvent(
        instance, worker, capability, "hash", "42", cause, null);
    assertThat(event.bindingName()).isNull();
}
```

#### Step 6: Update WorkerFaultEvent record

```java
public record WorkerFaultEvent(
    CaseInstance caseInstance,
    Worker worker,
    Capability capability,
    String inputDataHash,
    String eventLogId,
    Throwable cause,
    String bindingName) {}
```

#### Step 7: Write failing tests for WorkerFaultPublisher

Update WorkerFaultPublisherTest to verify `bindingName` flows from context to fault event in both overloads:

```java
// Test for fault(String, WorkerCorrelationContext, Capability, Long, Throwable)
@Test
void fault_address_passesBindingNameFromContext() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-1", "t1", "binding-x");
    publisher.fault("test.fault", ctx, capability, 42L, cause);

    ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
    verify(eventBus).publish(eq("test.fault"), captor.capture());
    assertThat(captor.getValue().bindingName()).isEqualTo("binding-x");
}

// Test for fault(PendingCompletion, Throwable)
@Test
void fault_pending_passesBindingNameFromContext() {
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        instance, worker, "hash-1", "t1", "binding-y");
    PendingCompletion pending = new PendingCompletion(
        "dispatch-1", "http", "casehub.workers.http.fault",
        ctx, "token", capability, 42L,
        Instant.now(), Instant.now().plusSeconds(3600), Map.of());
    publisher.fault(pending, cause);

    ArgumentCaptor<WorkerFaultEvent> captor = ArgumentCaptor.forClass(WorkerFaultEvent.class);
    verify(eventBus).publish(eq("casehub.workers.http.fault"), captor.capture());
    assertThat(captor.getValue().bindingName()).isEqualTo("binding-y");
}
```

#### Step 8: Update WorkerFaultPublisher

Add `ctx.bindingName()` as the 7th arg in both `fault()` methods:

```java
public void fault(String faultAddress, WorkerCorrelationContext ctx,
                  Capability capability, Long eventLogId, Throwable cause) {
    eventBus.publish(faultAddress, new WorkerFaultEvent(
        ctx.caseInstance(), ctx.worker(), capability,
        ctx.idempotency(), eventLogId.toString(), cause, ctx.bindingName()));
}

public void fault(PendingCompletion pending, Throwable cause) {
    eventBus.publish(pending.faultAddress(), new WorkerFaultEvent(
        pending.correlationContext().caseInstance(),
        pending.correlationContext().worker(),
        pending.capability(),
        pending.correlationContext().idempotency(),
        pending.eventLogId().toString(),
        cause,
        pending.correlationContext().bindingName()));
}
```

#### Step 9: Fix all remaining workers-common test construction sites

Every test file in workers-common that constructs `WorkerCorrelationContext` needs the 5th arg (`null` unless the test specifically tests bindingName). Every test constructing `WorkerFaultEvent` needs the 7th arg (`null`).

**WorkerCorrelationContext sites (pass `null` as 5th arg):**
- `AsyncWorkerCompletionRegistryTest.java` line 81: `testContext()` helper
- `WorkerCallbackResourceTest.java` line 91
- `WorkerFaultCallbackObserverTest.java` line 26
- `WorkerCompletionExpiryObserverTest.java` line 26
- `WorkerFaultPublisherTest.java` lines 30 and 56

**WorkerFaultEvent sites (pass `null` as 7th arg):**
- `WorkerFaultHandlerTest.java` lines 72-73, 107-108, 136-137, 168-169

#### Step 10: Run workers-common tests and verify green

```bash
mvn --batch-mode test -pl workers-common
```

Expected: all tests pass.

#### Step 11: Commit

```bash
git add workers-common/
git commit -m "feat(#18): add bindingName to WorkerCorrelationContext, WorkerFaultEvent, and publishers"
```

---

### Task 2: WorkerFaultHandler — Retry and Exhaustion Paths

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultHandler.java`
- Modify: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultHandlerTest.java`

**Interfaces:**
- Consumes: `WorkerFaultEvent.bindingName()` from Task 1
- Consumes: `WorkerExecutionManager.submit(Long, CaseInstance, Worker, Capability, Map, String)` — the 6-arg overload (default method on SPI, overridden by workers in Task 3)
- Consumes: `WorkerRetrySupport.publishRetriesExhausted(UUID, String, String, String, String)` — already accepts `bindingName` as 4th param

#### Step 1: Write failing tests

**Test 1: retry re-dispatch passes bindingName to 6-arg submit:**

```java
@Test
void retryableFault_passesBindingNameToSubmit() {
    CaseInstance instance = testCaseInstance();
    RetryPolicy retryPolicy = new RetryPolicy(3, 100, BackoffStrategy.FIXED);
    ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
    Worker worker = Worker.builder().name("w1").capabilityNames().executionPolicy(ep)
        .function(new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of()))).build();
    Capability cap = testCapability("cap");
    RuntimeException cause = new RuntimeException("transient");

    when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
        .thenReturn(Uni.createFrom().item(1L));

    EventLog eventLog = new EventLog();
    eventLog.setPayload(OBJECT_MAPPER.createObjectNode().put("k", "v"));
    when(eventLogRepository.findById(42L, instance.tenancyId)).thenReturn(eventLog);
    when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Uni.createFrom().voidItem());

    WorkerFaultEvent event = new WorkerFaultEvent(
        instance, worker, cap, "hash-1", "42", cause, "binding-x");

    handler.handleFault(event).await().indefinitely();

    verify(workerExecutionManager).submit(
        eq(42L), eq(instance), eq(worker), eq(cap), any(), eq("binding-x"));
}
```

**Test 2: permanent fault passes bindingName to publishRetriesExhausted:**

```java
@Test
void permanentFault_passesBindingNameToRetriesExhausted() {
    CaseInstance instance = testCaseInstance();
    Worker worker = testWorker("w1");
    Capability cap = testCapability("cap");
    PermanentFaultException cause = new PermanentFaultException(400, "Bad Request");

    WorkerFaultEvent event = new WorkerFaultEvent(
        instance, worker, cap, "hash-1", "42", cause, "binding-y");

    handler.handleFault(event).await().indefinitely();

    verify(retrySupport).publishRetriesExhausted(
        instance.getUuid(), "w1", "hash-1", "binding-y", instance.tenancyId);
}
```

**Test 3: null bindingName passes null through both paths:**

```java
@Test
void nullBindingName_passesNullToRetriesExhausted() {
    CaseInstance instance = testCaseInstance();
    Worker worker = testWorker("w1");
    Capability cap = testCapability("cap");
    PermanentFaultException cause = new PermanentFaultException(400, "Bad");

    WorkerFaultEvent event = new WorkerFaultEvent(
        instance, worker, cap, "hash-1", "42", cause, null);

    handler.handleFault(event).await().indefinitely();

    verify(retrySupport).publishRetriesExhausted(
        instance.getUuid(), "w1", "hash-1", null, instance.tenancyId);
}
```

#### Step 2: Run tests to verify they fail

```bash
mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultHandlerTest
```

Expected: new tests fail (old submit 5-arg is called, wrong bindingName in exhaustion path).

#### Step 3: Update WorkerFaultHandler

**`handleFault()` — permanent fault path (line 42-44):** Replace `worker.name()` with `event.bindingName()`:

```java
retrySupport.publishRetriesExhausted(
    instance.getUuid(), worker.name(), inputDataHash,
    event.bindingName(), tenancyId);
```

**`handleFault()` — exhaustion path (line 63-64):** Same change:

```java
retrySupport.publishRetriesExhausted(
    instance.getUuid(), worker.name(), inputDataHash,
    event.bindingName(), tenancyId);
```

**`reloadAndResubmit()` (line 86-88):** Call 6-arg `submit()` with `event.bindingName()`:

```java
.flatMap(ignored -> workerExecutionManager.submit(
    Long.parseLong(event.eventLogId()),
    event.caseInstance(), event.worker(), event.capability(), inputData,
    event.bindingName()));
```

The method signature also needs `event` passed through — currently `reloadAndResubmit` takes `WorkerFaultEvent event` so `event.bindingName()` is already accessible.

#### Step 4: Update existing tests

Existing tests construct `WorkerFaultEvent` without `bindingName` (6 args). All sites need the 7th arg added (null for non-bindingName tests):

- `permanentFault_skipsRetry` (line 72-73): add `null` as 7th arg
- `retryableFault_underMaxRetries_callsSubmit` (line 107-108): add `null` as 7th arg
- `retriesExhausted_neverCallsSubmit` (line 136-137): add `null` as 7th arg
- `retryAfterException_usesRetryAfterDelay` (line 168-169): add `null` as 7th arg

Update existing verify assertions for `publishRetriesExhausted`:
- Line 78-79: change `"w1"` to `null` (4th arg) — was the worker.name() surrogate, now correctly null
- Line 141-142: change `"w1"` to `null` (4th arg)

#### Step 5: Run tests and verify green

```bash
mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultHandlerTest
```

Expected: all tests pass.

#### Step 6: Commit

```bash
git add workers-common/
git commit -m "feat(#18): thread bindingName through WorkerFaultHandler retry and exhaustion paths"
```

---

### Task 3: All 6 Worker Execution Managers — submit() Override

**Files:**
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerExecutionManager.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerExecutionManager.java`
- Modify: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerExecutionManager.java`
- Modify: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManager.java`
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerExecutionManager.java`
- Modify: all 6 corresponding test files
- Modify: `workers-camel/src/test/java/io/casehub/workers/camel/component/CasehubProducerTest.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobInformerManagerTest.java`

**Interfaces:**
- Consumes: `WorkerCorrelationContext(CaseInstance, Worker, String, String, String)` from Task 1
- Produces: 6-arg `submit(Long, CaseInstance, Worker, Capability, Map, String)` override on each worker

**Pattern (identical for all 6 workers):**

For workers with `buildCtx()` (K8s, MCP, Script, GitHub Actions):

```java
@Override
public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                        Capability capability, Map<String, Object> inputData) {
    return submit(eventLogId, instance, worker, capability, inputData, null);
}

@Override
public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                        Capability capability, Map<String, Object> inputData,
                        String bindingName) {
    // existing submit() body, with buildCtx(instance, worker, capability, inputData, bindingName)
}

private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                          Capability capability,
                                          Map<String, Object> inputData,
                                          String bindingName) {
    String idempotency = WorkerExecutionKeys.inputDataHash(
        instance.getUuid(), worker.name(), capability.name(), inputData);
    return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId, bindingName);
}
```

For workers with inline construction (HTTP, Camel): extract to `buildCtx()` following the same pattern, replacing all inline `new WorkerCorrelationContext(...)` calls.

#### Step 1: Write failing tests for each worker

For each worker module, add a test verifying the 6-arg `submit()` threads `bindingName` through to the correlation context (verified via `completionPublisher.complete()` or `faultPublisher.fault()`).

Example pattern (adapt per worker's test setup):

```java
@Test
void submit_6arg_passesBindingNameThroughCompletion() {
    // ... setup ...
    manager.submit(1L, instance, worker, capability, inputData, "binding-x")
        .await().indefinitely();

    ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
        ArgumentCaptor.forClass(WorkerCorrelationContext.class);
    verify(completionPublisher).complete(ctxCaptor.capture(), any());
    assertThat(ctxCaptor.getValue().bindingName()).isEqualTo("binding-x");
}

@Test
void submit_5arg_passesNullBindingName() {
    // ... setup ...
    manager.submit(1L, instance, worker, capability, inputData)
        .await().indefinitely();

    ArgumentCaptor<WorkerCorrelationContext> ctxCaptor =
        ArgumentCaptor.forClass(WorkerCorrelationContext.class);
    verify(completionPublisher).complete(ctxCaptor.capture(), any());
    assertThat(ctxCaptor.getValue().bindingName()).isNull();
}
```

#### Step 2: Implement the override pattern in all 6 workers

Apply the pattern from above. Each worker:
1. Rename existing `submit()` to the 6-arg signature with `String bindingName`
2. Add 5-arg `submit()` that delegates to 6-arg with `null`
3. Update `buildCtx()` (or extract one) to accept and pass `bindingName`
4. Update all inline `new WorkerCorrelationContext(...)` in exception paths to use `buildCtx()`

#### Step 3: Fix all per-module test WorkerCorrelationContext construction sites

Every test that constructs `WorkerCorrelationContext` directly needs `null` as the 5th arg. Every test that calls the (now-delegating) 5-arg `submit()` continues to work — verify calls still match.

**Per-module test files requiring construction site fixes:**
- `HttpWorkerExecutionManagerTest.java` — lines 502, 546-547
- `McpWorkerExecutionManagerTest.java` — check for inline construction
- `ScriptWorkerExecutionManagerTest.java` — check for inline construction
- `GitHubActionsWorkerExecutionManagerTest.java` — check for inline construction
- `K8sWorkerExecutionManagerTest.java` — check for inline construction
- `K8sJobInformerManagerTest.java` — line 415
- `CasehubProducerTest.java` — line 52

#### Step 4: Run full build

```bash
mvn --batch-mode install
```

Expected: all 48+ tests pass across all 8 modules.

#### Step 5: Commit

```bash
git add .
git commit -m "feat(#18): override 6-arg submit() in all workers — thread bindingName to WorkerCorrelationContext"
```

---

### Task 4: K8s-specific — Annotation, Recovery, schedulePersistedEvent

**Files:**
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerConstants.java`
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobBuilder.java`
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobInformerManager.java`
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerExecutionManager.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobBuilderTest.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobInformerManagerTest.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sWorkerExecutionManagerTest.java`

**Interfaces:**
- Consumes: `WorkerCorrelationContext.bindingName()` from Task 1
- Consumes: 6-arg `submit()` from Task 3
- Produces: `K8sWorkerConstants.BINDING_NAME_ANNOTATION = "casehub.io/binding-name"`
- Produces: `K8sJobBuilder.build(...)` gains `String bindingName` parameter (last position)

#### Step 1: Add constant

```java
// K8sWorkerConstants.java — add
public static final String BINDING_NAME_ANNOTATION = "casehub.io/binding-name";
```

#### Step 2: Write failing tests for K8sJobBuilder

```java
@Test
void build_withBindingName_addsAnnotation() {
    JobDefinition def = testDefinition();
    Job job = K8sJobBuilder.build(def, "dispatch-1", "case-1", "t1",
        "k8s:test", "hash", "{}", "w1", 1L, "my-binding");
    assertThat(job.getMetadata().getAnnotations())
        .containsEntry("casehub.io/binding-name", "my-binding");
}

@Test
void build_nullBindingName_noAnnotation() {
    JobDefinition def = testDefinition();
    Job job = K8sJobBuilder.build(def, "dispatch-1", "case-1", "t1",
        "k8s:test", "hash", "{}", "w1", 1L, null);
    assertThat(job.getMetadata().getAnnotations())
        .doesNotContainKey("casehub.io/binding-name");
}
```

#### Step 3: Update K8sJobBuilder

Add `String bindingName` as the last parameter to `build()`, `buildFromImage()`, and `buildFromTemplate()`. Add annotation logic after label application:

```java
if (bindingName != null) {
    Map<String, String> annotations = new LinkedHashMap<>();
    annotations.put(K8sWorkerConstants.BINDING_NAME_ANNOTATION, bindingName);
    // for buildFromImage: add to builder
    // for buildFromTemplate: merge with existing annotations
}
```

Update all `build()` call sites — `K8sWorkerExecutionManager.submit()` passes `ctx.bindingName()`:

```java
Job job = K8sJobBuilder.build(definition, pending.dispatchId(),
    instance.getUuid().toString(), instance.tenancyId,
    capability.name(), ctx.idempotency(), inputDataJson,
    worker.name(), eventLogId, ctx.bindingName());
```

#### Step 4: Write failing test for recovery — recoverFromJob reads annotation

```java
@Test
void recoverFromJob_readsBindingNameAnnotation() {
    Job job = buildTestJob("dispatch-1");
    job.getMetadata().getAnnotations().put("casehub.io/binding-name", "recovered-binding");

    when(caseInstanceRepository.findByUuid(any(), any())).thenReturn(buildCaseInstance());

    Optional<PendingCompletion> result = informerManager.recoverFromJob(job, "dispatch-1");

    assertThat(result).isPresent();
    assertThat(result.get().correlationContext().bindingName()).isEqualTo("recovered-binding");
}

@Test
void recoverFromJob_preUpgradeJob_nullBindingName() {
    Job job = buildTestJob("dispatch-1");
    // No annotation — pre-upgrade Job

    when(caseInstanceRepository.findByUuid(any(), any())).thenReturn(buildCaseInstance());

    Optional<PendingCompletion> result = informerManager.recoverFromJob(job, "dispatch-1");

    assertThat(result).isPresent();
    assertThat(result.get().correlationContext().bindingName()).isNull();
}
```

#### Step 5: Update K8sJobInformerManager.recoverFromJob()

After existing label reads (line 177), add annotation read:

```java
Map<String, String> annotations = job.getMetadata().getAnnotations();
String bindingName = annotations != null
    ? annotations.get(K8sWorkerConstants.BINDING_NAME_ANNOTATION) : null;
```

Pass to WorkerCorrelationContext constructor (line 206-207):

```java
WorkerCorrelationContext ctx = new WorkerCorrelationContext(
    caseInstance, worker, idempotency, tenancyId, bindingName);
```

#### Step 6: Write failing test for schedulePersistedEvent reads bindingName from metadata

```java
@Test
void schedulePersistedEvent_readsBindingNameFromMetadata() {
    EventLog eventLog = buildScheduledEventLog();
    ((ObjectNode) eventLog.getMetadata()).put("bindingName", "spe-binding");
    // ... setup resolver, kubernetesClient (no existing Job found), caseInstanceRepository ...

    when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any(), any()))
        .thenReturn(Uni.createFrom().voidItem());

    manager.schedulePersistedEvent(eventLog).await().indefinitely();

    verify(workerExecutionManager).submit(
        anyLong(), any(), any(), any(), any(), eq("spe-binding"));
}
```

#### Step 7: Update K8sWorkerExecutionManager.schedulePersistedEvent()

After existing metadata reads (line 150-153), add:

```java
String bindingName = scheduledEventLog.getMetadata().has("bindingName")
    ? scheduledEventLog.getMetadata().get("bindingName").asText() : null;
```

Update the `submit()` call (line 217) to use the 6-arg overload:

```java
return submit(eventLogId, instance, worker, capability, inputData, bindingName);
```

Note: since `schedulePersistedEvent` is on the same class, this calls the 6-arg directly.

But wait — `schedulePersistedEvent` currently calls `submit(eventLogId, instance, worker, capability, inputData)` which now delegates to `submit(..., null)`. We want it to call `submit(..., bindingName)` instead. Since the method is on the same class, pass the `bindingName` read from metadata.

#### Step 8: Fix all K8s test construction sites

Update all existing K8s tests that construct `WorkerCorrelationContext` to pass null as 5th arg (already done in Task 3 for the informer test).

Update existing `K8sJobBuilder` tests to pass `null` as the new last arg to `build()`.

#### Step 9: Run K8s module tests

```bash
mvn --batch-mode test -pl workers-k8s
```

Expected: all tests pass.

#### Step 10: Run full build

```bash
mvn --batch-mode install
```

Expected: clean build, all tests pass.

#### Step 11: Commit

```bash
git add workers-k8s/
git commit -m "feat(#18): K8s annotation persistence and recovery for bindingName"
```

---

### Task 5: Housekeeping — Update Issue Title

**Files:** None (GitHub API only)

#### Step 1: Update issue #18 title to reflect full scope

```bash
gh issue edit 18 --repo casehubio/workers --title "feat: propagate bindingName through worker completion path"
```

#### Step 2: Commit is not needed (GitHub-only change)

---

## Self-Review Checklist

1. **Spec coverage:**
   - §1 WorkerCorrelationContext: Task 1 ✓
   - §2 WorkflowCompletionPublisher: Task 1 ✓
   - §3 WorkerFaultEvent + WorkerFaultPublisher: Task 1 ✓
   - §4 SPI Override Pattern: Task 3 ✓
   - §5 WorkerFaultHandler: Task 2 ✓
   - §6 K8s-specific: Task 4 ✓
   - §7 Unchanged paths: verified transitively ✓
   - Engine Dependency: out of scope (engine#676) ✓

2. **Placeholder scan:** No TBDs, TODOs, or vague instructions.

3. **Type consistency:** `WorkerCorrelationContext` 5-arg constructor used consistently. `WorkerFaultEvent` 7-arg constructor used consistently. `K8sJobBuilder.build()` 10-arg used consistently. `submit()` 6-arg pattern identical across all workers.
