# K8s Restart Recovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make K8s worker completions survive process restarts by enriching Job labels, adding a recovery path in `processTerminal()`, eagerly initializing the resolver, and implementing `schedulePersistedEvent()`.

**Architecture:** K8s Jobs already survive restarts — they are the durable store. The in-memory `PendingCompletion` registry is a performance cache. This implementation enriches Jobs with recovery metadata (4 new labels), makes `processTerminal()` reconstruct correlation context from Job metadata when the cache is empty, eagerly initializes the resolver via `@PostConstruct`, and implements `schedulePersistedEvent()` for engine recovery re-dispatch.

**Tech Stack:** Java 21, Quarkus 3.x, fabric8 Kubernetes client, Mutiny, JUnit 5, Mockito, AssertJ

## Global Constraints

- All changes confined to `workers-k8s` module — no engine-common SPI changes
- `CaseInstanceRepository` (in `engine-common`) is already a transitive dependency — no new module deps
- K8s label values ≤63 chars, regex `[a-z0-9A-Z._-]*`
- Tests use Mockito field injection (existing pattern: `manager.field = mock(...)`)
- TDD: failing test first, then implementation
- `WorkerFunction.NONE` for recovery Workers — completion handler only uses `name()` and `capabilityNames()`

---

### Task 1: Enriched Job Labels — Constants + Builder

**Files:**
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerConstants.java`
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobBuilder.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobBuilderTest.java`

**Interfaces:**
- Consumes: nothing new
- Produces: `K8sWorkerConstants.CASE_ID_LABEL`, `.WORKER_NAME_LABEL`, `.EVENT_LOG_ID_LABEL`, `.IDEMPOTENCY_LABEL`; `K8sJobBuilder.build()` gains `workerName`, `eventLogId`, `idempotency` params

- [ ] **Step 1: Write failing tests for new labels**

Add to `K8sJobBuilderTest`:

```java
@Test
void build_includesRecoveryLabels() {
    JobDefinition def = imageDef("report-gen");
    Job job = K8sJobBuilder.build(def, "dispatch-1", "case-uuid-1", "t1",
        "k8s:report-gen", "hash-abc", "{}", "w1", 42L, "idem-xyz");

    Map<String, String> labels = job.getMetadata().getLabels();
    assertThat(labels).containsEntry(K8sWorkerConstants.CASE_ID_LABEL, "case-uuid-1");
    assertThat(labels).containsEntry(K8sWorkerConstants.WORKER_NAME_LABEL, "w1");
    assertThat(labels).containsEntry(K8sWorkerConstants.EVENT_LOG_ID_LABEL, "42");
    assertThat(labels).containsEntry(K8sWorkerConstants.IDEMPOTENCY_LABEL, "idem-xyz");
}

@Test
void build_templatePath_includesRecoveryLabels() {
    JobDefinition def = templateDef("templated");
    Job job = K8sJobBuilder.build(def, "dispatch-2", "case-uuid-2", "t1",
        "k8s:templated", "hash-def", "{}", "w2", 99L, "idem-456");

    Map<String, String> labels = job.getMetadata().getLabels();
    assertThat(labels).containsEntry(K8sWorkerConstants.CASE_ID_LABEL, "case-uuid-2");
    assertThat(labels).containsEntry(K8sWorkerConstants.WORKER_NAME_LABEL, "w2");
    assertThat(labels).containsEntry(K8sWorkerConstants.EVENT_LOG_ID_LABEL, "99");
    assertThat(labels).containsEntry(K8sWorkerConstants.IDEMPOTENCY_LABEL, "idem-456");
}
```

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn -pl workers-k8s test -Dtest=K8sJobBuilderTest -Dsurefire.failIfNoSpecifiedTests=false --batch-mode`
Expected: compilation failure — `build()` signature doesn't match

- [ ] **Step 3: Add label constants to `K8sWorkerConstants`**

```java
public static final String CASE_ID_LABEL = "casehub.io/case-id";
public static final String WORKER_NAME_LABEL = "casehub.io/worker-name";
public static final String EVENT_LOG_ID_LABEL = "casehub.io/event-log-id";
public static final String IDEMPOTENCY_LABEL = "casehub.io/idempotency";
```

- [ ] **Step 4: Update `K8sJobBuilder.build()` signature and `buildLabels()`**

Change `build()` signature from:
```java
public static Job build(JobDefinition definition, String dispatchId,
                        String caseId, String tenancyId, String capabilityTag,
                        String idempotency, String inputDataJson)
```
to:
```java
public static Job build(JobDefinition definition, String dispatchId,
                        String caseId, String tenancyId, String capabilityTag,
                        String idempotency, String inputDataJson,
                        String workerName, Long eventLogId, String idempotencyHash)
```

Pass the new params through `buildFromImage()` and `buildFromTemplate()` to `buildLabels()`.

Update `buildLabels()` signature from:
```java
private static Map<String, String> buildLabels(JobDefinition def, String dispatchId,
                                                String capabilityTag, String tenancyId)
```
to:
```java
private static Map<String, String> buildLabels(JobDefinition def, String dispatchId,
                                                String capabilityTag, String tenancyId,
                                                String caseId, String workerName,
                                                Long eventLogId, String idempotencyHash)
```

Add the four labels:
```java
labels.put(K8sWorkerConstants.CASE_ID_LABEL, caseId);
labels.put(K8sWorkerConstants.WORKER_NAME_LABEL, workerName);
labels.put(K8sWorkerConstants.EVENT_LOG_ID_LABEL, String.valueOf(eventLogId));
labels.put(K8sWorkerConstants.IDEMPOTENCY_LABEL, idempotencyHash);
```

- [ ] **Step 5: Fix all existing callers of `K8sJobBuilder.build()`**

`K8sWorkerExecutionManager.submit()` is the only production caller. Update:
```java
Job job = K8sJobBuilder.build(definition, pending.dispatchId(),
    instance.getUuid().toString(), instance.tenancyId,
    capability.name(), ctx.idempotency(), inputDataJson,
    worker.name(), eventLogId, ctx.idempotency());
```

Fix all test callers in `K8sJobBuilderTest` and `K8sWorkerExecutionManagerTest` to pass the new params.

- [ ] **Step 6: Run all K8s tests**

Run: `mvn -pl workers-k8s test --batch-mode`
Expected: all tests PASS

- [ ] **Step 7: Commit**

```bash
git add workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerConstants.java \
      workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobBuilder.java \
      workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobBuilderTest.java \
      workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerExecutionManager.java \
      workers-k8s/src/test/java/io/casehub/workers/k8s/K8sWorkerExecutionManagerTest.java
git commit -m "feat(#17): enrich K8s Job labels with recovery metadata — case-id, worker-name, event-log-id, idempotency"
```

---

### Task 2: Eager Resolver Initialization

**Files:**
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/JobDefinitionResolver.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/JobDefinitionResolverTest.java`

**Interfaces:**
- Consumes: existing `JobDefinitionResolver.initialize()` (no-arg, reads Config)
- Produces: `@PostConstruct` ensures `capabilities()` returns correct values from bean creation time

- [ ] **Step 1: Write failing test — capabilities available without explicit `initialize()` call**

Add to `JobDefinitionResolverTest` (or create if it doesn't exist):

```java
@Test
void capabilities_availableAfterPostConstruct() {
    JobDefinitionResolver resolver = new JobDefinitionResolver();
    resolver.config = buildConfig(Map.of(
        "casehub.workers.k8s.jobs.report-gen.image", "acme/report:latest"));
    resolver.defaultNamespace = "default";
    resolver.defaultTimeoutSeconds = 3600;
    resolver.defaultTtlAfterFinished = 600;
    resolver.defaultBackoffLimit = 0;
    resolver.defaultCleanup = "delete";
    resolver.defaultMaxOutputBytes = 1048576;
    resolver.defaultMaxInputBytes = 262144;

    resolver.init();

    assertThat(resolver.capabilities()).containsExactly("k8s:report-gen");
}
```

Where `buildConfig(Map)` returns a `SmallRyeConfig` with the given properties (check existing test for pattern, or use `SmallRyeConfigBuilder`).

- [ ] **Step 2: Run test — verify it fails**

Run: `mvn -pl workers-k8s test -Dtest=JobDefinitionResolverTest#capabilities_availableAfterPostConstruct --batch-mode`
Expected: fail — `init()` method doesn't exist

- [ ] **Step 3: Add `@PostConstruct` to `JobDefinitionResolver`**

Add import and method:
```java
import jakarta.annotation.PostConstruct;

@PostConstruct
void init() {
    initialize();
}
```

- [ ] **Step 4: Run test — verify it passes**

Run: `mvn -pl workers-k8s test -Dtest=JobDefinitionResolverTest --batch-mode`
Expected: PASS

- [ ] **Step 5: Run full module tests**

Run: `mvn -pl workers-k8s test --batch-mode`
Expected: all PASS (existing tests that call `initialize(Map)` directly still work — `init()` populates from Config, then `initialize(Map)` overwrites with the test map)

- [ ] **Step 6: Commit**

```bash
git add workers-k8s/src/main/java/io/casehub/workers/k8s/JobDefinitionResolver.java \
      workers-k8s/src/test/java/io/casehub/workers/k8s/JobDefinitionResolverTest.java
git commit -m "fix(#17): eager resolver initialization via @PostConstruct — eliminates startup race with engine recovery"
```

---

### Task 3: Recovery Path in `processTerminal()`

**Files:**
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobInformerManager.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobInformerManagerTest.java`

**Interfaces:**
- Consumes: `K8sWorkerConstants.*_LABEL` (from Task 1), `CaseInstanceRepository.findByUuid(UUID, String)`, `JobDefinitionResolver.resolve(String, String)`, `Worker.builder()`, `PendingCompletion` record constructor
- Produces: `recoverFromJob(Job, String)` returns `Optional<PendingCompletion>`, `recoveredDispatchIds` set for at-most-once guard

- [ ] **Step 1: Write failing test — recovery succeeds for terminal Job with all labels**

```java
@Test
void processTerminal_registryEmpty_recoversFromJobLabels() {
    Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-1",
        "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
    when(registry.complete("dispatch-1")).thenReturn(Optional.empty());
    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().item(testCaseInstance));
    when(labelOp.list()).thenReturn(emptyPodList());

    manager.processTerminal(job, "dispatch-1");

    verify(completionPublisher).complete(any(WorkerCorrelationContext.class), any());
}
```

Where `buildRecoverableJob()` builds a Job with all 8 labels (4 existing + 4 new), and `CASE_ID` is a constant `UUID`, and `testCaseInstance` is a pre-built `CaseInstance` with that UUID.

- [ ] **Step 2: Write failing test — recovery skips Job with missing labels**

```java
@Test
void processTerminal_registryEmpty_missingCaseIdLabel_skips() {
    Job job = buildJob("batch", "casehub-test-abc", "dispatch-2", "Complete");
    when(registry.complete("dispatch-2")).thenReturn(Optional.empty());

    manager.processTerminal(job, "dispatch-2");

    verifyNoInteractions(completionPublisher, faultPublisher);
}
```

- [ ] **Step 3: Write failing test — at-most-once guard prevents duplicate recovery**

```java
@Test
void processTerminal_registryEmpty_secondCallSameDispatch_skips() {
    Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-3",
        "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
    when(registry.complete("dispatch-3")).thenReturn(Optional.empty());
    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().item(testCaseInstance));
    when(labelOp.list()).thenReturn(emptyPodList());

    manager.processTerminal(job, "dispatch-3");
    manager.processTerminal(job, "dispatch-3");

    verify(completionPublisher, times(1)).complete(any(), any());
}
```

- [ ] **Step 4: Write failing test — recovery for failed Job publishes fault**

```java
@Test
void processTerminal_registryEmpty_failedJob_publishesFault() {
    Job job = buildRecoverableJob("batch", "casehub-test-fail", "dispatch-4",
        "Failed", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
    job.getStatus().getConditions().get(0).setReason("DeadlineExceeded");
    when(registry.complete("dispatch-4")).thenReturn(Optional.empty());
    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().item(testCaseInstance));
    when(labelOp.list()).thenReturn(emptyPodList());

    manager.processTerminal(job, "dispatch-4");

    ArgumentCaptor<Throwable> captor = ArgumentCaptor.forClass(Throwable.class);
    verify(faultPublisher).fault(any(PendingCompletion.class), captor.capture());
    assertThat(captor.getValue()).isInstanceOf(PermanentFaultException.class);
}
```

- [ ] **Step 5: Write failing test — CaseInstance not found skips gracefully**

```java
@Test
void processTerminal_registryEmpty_caseInstanceNotFound_skips() {
    Job job = buildRecoverableJob("batch", "casehub-test-abc", "dispatch-5",
        "Complete", CASE_ID, "t1", "w1", "k8s:test", 1L, "idem-hash");
    when(registry.complete("dispatch-5")).thenReturn(Optional.empty());
    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().nullItem());

    manager.processTerminal(job, "dispatch-5");

    verifyNoInteractions(completionPublisher, faultPublisher);
}
```

- [ ] **Step 6: Run tests — verify they all fail**

Run: `mvn -pl workers-k8s test -Dtest=K8sJobInformerManagerTest --batch-mode`
Expected: compilation failure — `caseInstanceRepository` field doesn't exist, `buildRecoverableJob` doesn't exist, etc.

- [ ] **Step 7: Add test helper `buildRecoverableJob()`**

```java
private static final UUID CASE_ID = UUID.fromString("11111111-1111-1111-1111-111111111111");
private CaseInstance testCaseInstance;
private CaseInstanceRepository caseInstanceRepository;
private JobDefinitionResolver resolver;
```

In `setUp()`:
```java
caseInstanceRepository = mock(CaseInstanceRepository.class);
resolver = new JobDefinitionResolver();
resolver.initialize(Map.of("test", imageDef("test")));
testCaseInstance = new CaseInstance();
testCaseInstance.setUuid(CASE_ID);
testCaseInstance.tenancyId = "t1";

manager.caseInstanceRepository = caseInstanceRepository;
manager.resolver = resolver;
```

Helper:
```java
private Job buildRecoverableJob(String namespace, String name, String dispatchId,
        String conditionType, UUID caseId, String tenancyId,
        String workerName, String capability, Long eventLogId, String idempotency) {
    Job job = buildJob(namespace, name, dispatchId, conditionType);
    Map<String, String> labels = new LinkedHashMap<>(job.getMetadata().getLabels());
    labels.put(K8sWorkerConstants.CASE_ID_LABEL, caseId.toString());
    labels.put(K8sWorkerConstants.WORKER_NAME_LABEL, workerName);
    labels.put(K8sWorkerConstants.CAPABILITY_LABEL, capability);
    labels.put(K8sWorkerConstants.TENANCY_ID_LABEL, tenancyId);
    labels.put(K8sWorkerConstants.EVENT_LOG_ID_LABEL, String.valueOf(eventLogId));
    labels.put(K8sWorkerConstants.IDEMPOTENCY_LABEL, idempotency);
    job.getMetadata().setLabels(labels);
    return job;
}

private static JobDefinition imageDef(String name) {
    return new JobDefinition(name, "batch", "acme/" + name + ":latest",
        List.of(), List.of(), null, null, null, null, null,
        3600, 600, 0, 1_048_576, null, Map.of(), Map.of(), CleanupPolicy.DELETE);
}
```

- [ ] **Step 8: Implement recovery in `K8sJobInformerManager`**

Add fields:
```java
@Inject CaseInstanceRepository caseInstanceRepository;
@Inject JobDefinitionResolver resolver;

private final Set<String> recoveredDispatchIds = ConcurrentHashMap.newKeySet();
```

Add `recoverFromJob()`:
```java
Optional<PendingCompletion> recoverFromJob(Job job, String dispatchId) {
    if (!recoveredDispatchIds.add(dispatchId)) {
        return Optional.empty();
    }

    Map<String, String> labels = job.getMetadata().getLabels();
    String caseIdStr = labels.get(K8sWorkerConstants.CASE_ID_LABEL);
    String workerName = labels.get(K8sWorkerConstants.WORKER_NAME_LABEL);
    String capabilityTag = labels.get(K8sWorkerConstants.CAPABILITY_LABEL);
    String tenancyId = labels.get(K8sWorkerConstants.TENANCY_ID_LABEL);
    String eventLogIdStr = labels.get(K8sWorkerConstants.EVENT_LOG_ID_LABEL);
    String idempotency = labels.get(K8sWorkerConstants.IDEMPOTENCY_LABEL);

    if (caseIdStr == null || workerName == null || idempotency == null) {
        LOG.warnf("Job %s/%s missing recovery labels — cannot recover (pre-upgrade Job?)",
            job.getMetadata().getNamespace(), job.getMetadata().getName());
        recoveredDispatchIds.remove(dispatchId);
        return Optional.empty();
    }

    CaseInstance caseInstance;
    try {
        caseInstance = caseInstanceRepository.findByUuid(
            UUID.fromString(caseIdStr), tenancyId).await().indefinitely();
    } catch (Exception e) {
        LOG.warnf("Recovery: failed to load CaseInstance %s: %s", caseIdStr, e.getMessage());
        recoveredDispatchIds.remove(dispatchId);
        return Optional.empty();
    }

    if (caseInstance == null) {
        LOG.warnf("Recovery: CaseInstance %s not found — case may have been closed", caseIdStr);
        recoveredDispatchIds.remove(dispatchId);
        return Optional.empty();
    }

    Worker worker = Worker.builder()
        .name(workerName)
        .capabilityName(capabilityTag)
        .noFunction()
        .build();
    WorkerCorrelationContext ctx = new WorkerCorrelationContext(
        caseInstance, worker, idempotency, tenancyId);

    Map<String, String> provisionerMeta;
    try {
        JobDefinition def = resolver.resolve(capabilityTag, tenancyId);
        provisionerMeta = Map.of(
            "cleanup", def.cleanup().name(),
            "maxOutputBytes", String.valueOf(def.maxOutputBytes()));
    } catch (Exception e) {
        provisionerMeta = Map.of("cleanup", "DELETE", "maxOutputBytes", "1048576");
    }

    Long eventLogId = eventLogIdStr != null ? Long.parseLong(eventLogIdStr) : null;
    Capability capability = Capability.of(capabilityTag, "", "");

    LOG.infof("Recovery: reconstructed PendingCompletion for dispatch %s (case %s, worker %s)",
        dispatchId, caseIdStr, workerName);

    return Optional.of(new PendingCompletion(
        dispatchId, K8sWorkerConstants.WORKER_TYPE,
        K8sWorkerEventBusAddresses.K8S_WORKER_FAULT,
        ctx, "", capability, eventLogId,
        Instant.now(), Instant.MAX, provisionerMeta));
}
```

Modify `processTerminal()` — replace the early return with recovery:
```java
void processTerminal(Job job, String dispatchId) {
    Optional<PendingCompletion> maybePending = registry.complete(dispatchId);

    if (maybePending.isEmpty()) {
        maybePending = recoverFromJob(job, dispatchId);
    }

    if (maybePending.isEmpty()) return;

    // ... existing code unchanged from here
}
```

- [ ] **Step 9: Run tests — verify they all pass**

Run: `mvn -pl workers-k8s test -Dtest=K8sJobInformerManagerTest --batch-mode`
Expected: all PASS

- [ ] **Step 10: Run full module tests**

Run: `mvn -pl workers-k8s test --batch-mode`
Expected: all PASS

- [ ] **Step 11: Commit**

```bash
git add workers-k8s/src/main/java/io/casehub/workers/k8s/K8sJobInformerManager.java \
      workers-k8s/src/test/java/io/casehub/workers/k8s/K8sJobInformerManagerTest.java
git commit -m "feat(#17): recovery path in processTerminal — reconstruct PendingCompletion from Job labels when registry is empty"
```

---

### Task 4: `schedulePersistedEvent()` Implementation

**Files:**
- Modify: `workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerExecutionManager.java`
- Modify: `workers-k8s/src/test/java/io/casehub/workers/k8s/K8sWorkerExecutionManagerTest.java`

**Interfaces:**
- Consumes: `CaseInstanceRepository.findByUuid()`, `JobDefinitionResolver.resolve()`, `K8sWorkerConstants.*_LABEL`, `EventLog` fields (`getCaseId()`, `tenancyId`, `id`, `getMetadata()`, `getPayload()`)
- Produces: `schedulePersistedEvent(EventLog)` override — checks K8s for existing Job, re-dispatches if none found

- [ ] **Step 1: Write failing test — no matching Job, re-dispatches**

```java
@Test
void schedulePersistedEvent_noExistingJob_reDispatches() {
    resolver.initialize(Map.of("test", imageDef("test")));
    EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
    CaseInstance instance = WorkerTestSupport.testCaseInstance("t1");
    instance.setUuid(CASE_ID);

    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().item(instance));
    mockK8sJobListEmpty();
    mockJobCreation();

    manager.schedulePersistedEvent(eventLog).await().indefinitely();

    verify(registry).register(eq(K8sWorkerConstants.WORKER_TYPE),
        eq(K8sWorkerEventBusAddresses.K8S_WORKER_FAULT),
        any(), any(), eq(1L), any(), any());
}
```

- [ ] **Step 2: Write failing test — matching running Job exists, returns voidItem**

```java
@Test
void schedulePersistedEvent_existingRunningJob_skips() {
    resolver.initialize(Map.of("test", imageDef("test")));
    EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
    mockK8sJobListReturns(buildRunningK8sJob());

    manager.schedulePersistedEvent(eventLog).await().indefinitely();

    verify(registry, never()).register(any(), any(), any(), any(), any(), any(), any());
}
```

- [ ] **Step 3: Write failing test — CaseInstance not found, returns voidItem**

```java
@Test
void schedulePersistedEvent_caseNotFound_skips() {
    resolver.initialize(Map.of("test", imageDef("test")));
    EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:test", 1L);
    when(caseInstanceRepository.findByUuid(CASE_ID, "t1"))
        .thenReturn(Uni.createFrom().nullItem());
    mockK8sJobListEmpty();

    manager.schedulePersistedEvent(eventLog).await().indefinitely();

    verify(registry, never()).register(any(), any(), any(), any(), any(), any(), any());
}
```

- [ ] **Step 4: Write failing test — capability not resolvable, returns voidItem**

```java
@Test
void schedulePersistedEvent_capabilityNotResolvable_skips() {
    resolver.initialize(Map.of());
    EventLog eventLog = buildScheduledEventLog(CASE_ID, "t1", "w1", "k8s:missing", 1L);

    manager.schedulePersistedEvent(eventLog).await().indefinitely();

    verify(registry, never()).register(any(), any(), any(), any(), any(), any(), any());
}
```

- [ ] **Step 5: Run tests — verify they fail**

Run: `mvn -pl workers-k8s test -Dtest=K8sWorkerExecutionManagerTest --batch-mode`
Expected: compilation failure — `caseInstanceRepository` field, helper methods, etc.

- [ ] **Step 6: Add test helpers**

```java
private static final UUID CASE_ID = UUID.fromString("22222222-2222-2222-2222-222222222222");
private CaseInstanceRepository caseInstanceRepository;

// In setUp():
caseInstanceRepository = mock(CaseInstanceRepository.class);
manager.caseInstanceRepository = caseInstanceRepository;
```

```java
private EventLog buildScheduledEventLog(UUID caseId, String tenancyId,
        String workerName, String capabilityName, Long eventLogId) {
    EventLog eventLog = new EventLog();
    eventLog.setCaseId(caseId);
    eventLog.tenancyId = tenancyId;
    eventLog.id = eventLogId;
    ObjectMapper mapper = new ObjectMapper();
    try {
        eventLog.setMetadata(mapper.readTree(
            "{\"workerName\":\"" + workerName + "\",\"capabilityName\":\"" + capabilityName + "\"}"));
        eventLog.setPayload(mapper.readTree("{\"key\":\"value\"}"));
    } catch (Exception e) { throw new RuntimeException(e); }
    return eventLog;
}

private void mockK8sJobListEmpty() {
    // Mock the label-selector chain: client.resources(Job.class).inNamespace(ns).withLabels(map).list()
    var jobList = new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of());
    when(client.resources(Job.class).inNamespace(anyString())
        .withLabels(anyMap()).list()).thenReturn(jobList);
}

private void mockK8sJobListReturns(Job job) {
    var jobList = new io.fabric8.kubernetes.api.model.batch.v1.JobList();
    jobList.setItems(List.of(job));
    when(client.resources(Job.class).inNamespace(anyString())
        .withLabels(anyMap()).list()).thenReturn(jobList);
}

private Job buildRunningK8sJob() {
    Job job = new Job();
    job.setMetadata(new ObjectMeta());
    job.getMetadata().setName("casehub-test-running");
    job.setStatus(new JobStatus());
    return job;
}

private void mockJobCreation() {
    PendingCompletion pending = mock(PendingCompletion.class);
    when(pending.dispatchId()).thenReturn("dispatch-recovery");
    when(registry.register(anyString(), anyString(), any(), any(), any(), any(), any()))
        .thenReturn(pending);
    NamespaceableResource<Job> resource = mock(NamespaceableResource.class);
    when(client.resource(any(Job.class))).thenReturn(resource);
    when(resource.create()).thenReturn(new Job());
}
```

- [ ] **Step 7: Implement `schedulePersistedEvent()` in `K8sWorkerExecutionManager`**

Add field:
```java
@Inject CaseInstanceRepository caseInstanceRepository;
```

Add override:
```java
@Override
public Uni<Void> schedulePersistedEvent(EventLog scheduledEventLog) {
    if (scheduledEventLog.getMetadata() == null) {
        return Uni.createFrom().voidItem();
    }
    String capabilityName = scheduledEventLog.getMetadata().has("capabilityName")
        ? scheduledEventLog.getMetadata().get("capabilityName").asText() : null;
    String workerName = scheduledEventLog.getMetadata().has("workerName")
        ? scheduledEventLog.getMetadata().get("workerName").asText() : null;

    if (capabilityName == null || workerName == null) {
        LOG.warnf("schedulePersistedEvent: missing metadata — capabilityName=%s workerName=%s",
            capabilityName, workerName);
        return Uni.createFrom().voidItem();
    }

    JobDefinition definition;
    try {
        definition = resolver.resolve(capabilityName, scheduledEventLog.tenancyId);
    } catch (Exception e) {
        LOG.warnf("schedulePersistedEvent: capability '%s' not resolvable — config removed?",
            capabilityName);
        return Uni.createFrom().voidItem();
    }

    UUID caseId = scheduledEventLog.getCaseId();
    String tenancyId = scheduledEventLog.tenancyId;
    Long eventLogId = scheduledEventLog.id;

    return Uni.createFrom().item(() -> {
        Map<String, String> labelSelector = Map.of(
            K8sWorkerConstants.MANAGED_BY_LABEL, K8sWorkerConstants.MANAGED_BY_VALUE,
            K8sWorkerConstants.CASE_ID_LABEL, caseId.toString(),
            K8sWorkerConstants.CAPABILITY_LABEL, capabilityName,
            K8sWorkerConstants.WORKER_NAME_LABEL, workerName);

        for (String ns : resolver.namespaces()) {
            var existing = kubernetesClient.resources(Job.class)
                .inNamespace(ns).withLabels(labelSelector).list();
            if (!existing.getItems().isEmpty()) {
                LOG.infof("schedulePersistedEvent: existing Job found for case %s capability %s — informer handles it",
                    caseId, capabilityName);
                return null;
            }
        }
        return "no-match";
    }).runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
      .onItem().ifNotNull().transformToUni(flag ->
          caseInstanceRepository.findByUuid(caseId, tenancyId)
              .onItem().ifNotNull().transformToUni(instance -> {
                  Worker worker = Worker.builder()
                      .name(workerName)
                      .capabilityName(capabilityName)
                      .noFunction()
                      .build();
                  Capability capability = Capability.of(capabilityName, "", "");
                  Map<String, Object> inputData;
                  try {
                      inputData = scheduledEventLog.getPayload() != null
                          ? new ObjectMapper().convertValue(scheduledEventLog.getPayload(),
                              new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {})
                          : Map.of();
                  } catch (Exception e) {
                      inputData = Map.of();
                  }
                  LOG.infof("schedulePersistedEvent: re-dispatching case %s capability %s",
                      caseId, capabilityName);
                  return submit(eventLogId, instance, worker, capability, inputData);
              })
              .onItem().ifNull().continueWith(() -> {
                  LOG.warnf("schedulePersistedEvent: CaseInstance %s not found — case closed?", caseId);
                  return null;
              })
      )
      .onItem().ifNull().continueWith(Uni.createFrom()::voidItem)
      .replaceWithVoid();
}
```

- [ ] **Step 8: Run tests — verify they all pass**

Run: `mvn -pl workers-k8s test -Dtest=K8sWorkerExecutionManagerTest --batch-mode`
Expected: all PASS

- [ ] **Step 9: Run full module tests**

Run: `mvn -pl workers-k8s test --batch-mode`
Expected: all PASS

- [ ] **Step 10: Run full project build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS — all modules pass

- [ ] **Step 11: Commit**

```bash
git add workers-k8s/src/main/java/io/casehub/workers/k8s/K8sWorkerExecutionManager.java \
      workers-k8s/src/test/java/io/casehub/workers/k8s/K8sWorkerExecutionManagerTest.java
git commit -m "feat(#17): implement schedulePersistedEvent — check K8s for existing Jobs, re-dispatch if none found"
```

---

### Task 5: CLAUDE.md and Spec Updates

**Files:**
- Modify: `CLAUDE.md`
- Modify: `docs/superpowers/specs/2026-07-03-k8s-restart-recovery-design.md`

**Interfaces:**
- Consumes: completed implementation from Tasks 1-4
- Produces: updated documentation

- [ ] **Step 1: Update CLAUDE.md `workers-k8s Key Types` table**

Add to the `workers-k8s Key Types` table (or update existing entries):

In `K8sWorkerConstants` row, add the 4 new label constants.

In `K8sJobInformerManager` row, add: `recoveredDispatchIds` for at-most-once recovery guard, `recoverFromJob()` for Job-metadata recovery, injects `CaseInstanceRepository`

In `K8sWorkerExecutionManager` row, add: implements `schedulePersistedEvent()`, injects `CaseInstanceRepository`

In `JobDefinitionResolver` row, add: `@PostConstruct` eager initialization from Config

- [ ] **Step 2: Update Key Rules**

Add to `## Key Rules`:
- K8s recovery on restart: `processTerminal()` reconstructs `PendingCompletion` from Job labels when registry is empty. `recoveredDispatchIds` provides at-most-once guard via atomic `add()`. `CaseInstanceRepository.findByUuid()` loads the CaseInstance; `Worker` is reconstructed from labels with `WorkerFunction.NONE`.
- K8s `schedulePersistedEvent()`: checks K8s for existing Job (label selector with case-id, capability, worker-name). If found → voidItem (informer handles). If not found → re-dispatches via `submit()`.
- K8s Job labels carry recovery metadata: `casehub.io/case-id`, `casehub.io/worker-name`, `casehub.io/event-log-id`, `casehub.io/idempotency`. Pre-upgrade Jobs lacking these labels cannot be recovered — they expire via `ttlSecondsAfterFinished`.
- `JobDefinitionResolver` initializes eagerly via `@PostConstruct` — `capabilities()` returns correct results before any startup observer fires. Eliminates race between engine recovery (`@Priority(22)`) and worker initialization (`@Priority(APPLICATION + 10)`).

- [ ] **Step 3: Commit doc updates**

```bash
git add CLAUDE.md docs/superpowers/specs/2026-07-03-k8s-restart-recovery-design.md
git commit -m "docs(#17): update CLAUDE.md and spec — K8s restart recovery types, rules, and labels"
```
