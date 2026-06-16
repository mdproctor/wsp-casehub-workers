# workers-script Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `workers-script` — a subprocess execution worker that dispatches case steps to local shell/Python/JS scripts.

**Architecture:** Two-phase implementation. Phase 1 extracts the duplicated fault pipeline infrastructure (publisher, handler body, CDI event observers) from 4 per-module copies to workers-common generics — prerequisite before adding the 5th worker. Phase 2 builds the script worker module itself: config-driven script definitions, ProcessBuilder execution with stdin/stdout, bounded output capture, exit code classification. Follows the established worker module pattern.

**Tech Stack:** Java 21, Quarkus 3.32, Mutiny, Vert.x event bus, CDI, ProcessBuilder, JUnit 5, Mockito, AssertJ

---

## Phase 1: Fault Pipeline Extraction (workers-common)

### Task 1: Add `faultAddress` to `PendingCompletion` and `AsyncWorkerCompletionRegistry`

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/PendingCompletion.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/AsyncWorkerCompletionRegistry.java`

- [ ] **Step 1: Add `faultAddress` field to `PendingCompletion`**

```java
// workers-common/src/main/java/io/casehub/workers/common/PendingCompletion.java
package io.casehub.workers.common;

import io.casehub.api.model.Capability;
import java.time.Instant;
import java.util.Map;

public record PendingCompletion(
    String dispatchId,
    String workerType,
    String faultAddress,
    WorkerCorrelationContext correlationContext,
    String callbackToken,
    Capability capability,
    Long eventLogId,
    Instant registeredAt,
    Instant expiresAt,
    Map<String, String> provisionerMeta
) {}
```

- [ ] **Step 2: Update `AsyncWorkerCompletionRegistry.register()` to accept `faultAddress`**

```java
// In AsyncWorkerCompletionRegistry.java — update register() signature and constructor call
public PendingCompletion register(String workerType, String faultAddress,
                                  WorkerCorrelationContext ctx,
                                  Capability capability, Long eventLogId,
                                  Duration ttl, Map<String, String> provisionerMeta) {
    Instant now = Instant.now();
    PendingCompletion entry = new PendingCompletion(
        UUID.randomUUID().toString(),
        workerType,
        faultAddress,
        ctx,
        UUID.randomUUID().toString(),
        capability,
        eventLogId,
        now,
        now.plus(ttl),
        provisionerMeta);
    pending.put(entry.dispatchId(), entry);
    return entry;
}
```

- [ ] **Step 3: Fix all callers of `register()` — pass fault address**

Update each call site to pass its module's fault address as the second argument:

`HttpWorkerExecutionManager.submitAsync()`:
```java
PendingCompletion pending = asyncWorkerCompletionRegistry.register(
    HttpWorkerConstants.WORKER_TYPE,
    HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT,
    ctx, capability, eventLogId,
    Duration.ofMinutes(asyncTimeoutMinutes), Map.of());
```

`CamelWorkerExecutionManager.submitAsync()`:
```java
PendingCompletion pending = asyncWorkerCompletionRegistry.register(
    CamelWorkerConstants.WORKER_TYPE,
    CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
    ctx, capability, eventLogId,
    Duration.ofMinutes(asyncTimeoutMinutes), Map.of());
```

- [ ] **Step 4: Fix all test code that constructs `PendingCompletion` directly**

Every test that constructs a `PendingCompletion` literal needs the new `faultAddress` field. Search for `new PendingCompletion(` across all test files. The field goes after `workerType`:

```java
// Example — in any test constructing PendingCompletion
PendingCompletion pending = new PendingCompletion(
    "dispatch-1", "http",
    HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT,  // new field
    ctx, "token", capability, 42L,
    Instant.now(), Instant.now().plusSeconds(3600), Map.of());
```

- [ ] **Step 5: Build to verify compilation**

Run: `mvn --batch-mode compile -pl workers-common,workers-http,workers-camel,workers-mcp,workers-github-actions`
Expected: BUILD SUCCESS

- [ ] **Step 6: Run all tests to verify nothing broke**

Run: `mvn --batch-mode test`
Expected: All tests pass

- [ ] **Step 7: Commit**

```
git add -A
git commit -m "refactor(#9): add faultAddress to PendingCompletion — self-routing for generic observers"
```

---

### Task 2: Create `WorkerFaultPublisher` in workers-common

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultPublisher.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultPublisherTest.java`

- [ ] **Step 1: Write the failing test**

```java
// workers-common/src/test/java/io/casehub/workers/common/WorkerFaultPublisherTest.java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.vertx.mutiny.core.eventbus.EventBus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class WorkerFaultPublisherTest {

    @Test
    void fault_fromContext_publishesToGivenAddress() {
        EventBus eventBus = mock(EventBus.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("run-script", "", "");

        publisher.fault("casehub.workers.test.fault", ctx, capability, 99L,
            new RuntimeException("boom"));

        ArgumentCaptor<WorkflowExecutionFailed> captor = ArgumentCaptor.forClass(WorkflowExecutionFailed.class);
        verify(eventBus).publish(eq("casehub.workers.test.fault"), captor.capture());

        WorkflowExecutionFailed event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("99");
    }

    @Test
    void fault_fromPendingCompletion_publishesToFaultAddress() {
        EventBus eventBus = mock(EventBus.class);
        WorkerFaultPublisher publisher = new WorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("send-webhook", "", "");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "http", "casehub.workers.http.fault",
            ctx, "token", capability, 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("timeout");

        publisher.fault(pending, cause);

        ArgumentCaptor<WorkflowExecutionFailed> captor = ArgumentCaptor.forClass(WorkflowExecutionFailed.class);
        verify(eventBus).publish(eq("casehub.workers.http.fault"), captor.capture());

        WorkflowExecutionFailed event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("42");
        assertThat(event.cause()).isSameAs(cause);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultPublisherTest`
Expected: FAIL — `WorkerFaultPublisher` does not exist

- [ ] **Step 3: Write the implementation**

```java
// workers-common/src/main/java/io/casehub/workers/common/WorkerFaultPublisher.java
package io.casehub.workers.common;

import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class WorkerFaultPublisher {

    @Inject
    EventBus eventBus;

    public void fault(String faultAddress, WorkerCorrelationContext ctx,
                      Capability capability, Long eventLogId, Throwable cause) {
        eventBus.publish(faultAddress, new WorkflowExecutionFailed(
            ctx.caseInstance(), ctx.worker(), capability,
            ctx.idempotency(), eventLogId.toString(), cause));
    }

    public void fault(PendingCompletion pending, Throwable cause) {
        eventBus.publish(pending.faultAddress(), new WorkflowExecutionFailed(
            pending.correlationContext().caseInstance(),
            pending.correlationContext().worker(),
            pending.capability(),
            pending.correlationContext().idempotency(),
            pending.eventLogId().toString(),
            cause));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultPublisherTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "refactor(#9): extract WorkerFaultPublisher to workers-common"
```

---

### Task 3: Create generic CDI event observers in workers-common

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerCompletionExpiryObserver.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultCallbackObserver.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerCompletionExpiryObserverTest.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultCallbackObserverTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-common/src/test/java/io/casehub/workers/common/WorkerCompletionExpiryObserverTest.java
package io.casehub.workers.common;

import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerCompletionExpiryObserverTest {

    @Test
    void onExpiry_publishesFaultToAddressFromPendingCompletion() {
        WorkerFaultPublisher faultPublisher = mock(WorkerFaultPublisher.class);
        WorkerCompletionExpiryObserver observer = new WorkerCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "http", "casehub.workers.http.fault",
            ctx, "token", new Capability("cap", "", ""), 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verify(faultPublisher).fault(eq(pending), any(RuntimeException.class));
    }
}
```

```java
// workers-common/src/test/java/io/casehub/workers/common/WorkerFaultCallbackObserverTest.java
package io.casehub.workers.common;

import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerFaultCallbackObserverTest {

    @Test
    void onFaultCallback_publishesFaultToAddressFromPendingCompletion() {
        WorkerFaultPublisher faultPublisher = mock(WorkerFaultPublisher.class);
        WorkerFaultCallbackObserver observer = new WorkerFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "camel", "casehub.workers.camel.fault",
            ctx, "token", new Capability("cap", "", ""), 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("callback fault");

        observer.onFaultCallback(new FaultCallbackEvent(pending, cause));

        verify(faultPublisher).fault(pending, cause);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-common -Dtest="WorkerCompletionExpiryObserverTest,WorkerFaultCallbackObserverTest"`
Expected: FAIL — classes do not exist

- [ ] **Step 3: Write the implementations**

```java
// workers-common/src/main/java/io/casehub/workers/common/WorkerCompletionExpiryObserver.java
package io.casehub.workers.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class WorkerCompletionExpiryObserver {

    @Inject
    WorkerFaultPublisher faultPublisher;

    void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
        faultPublisher.fault(event.pending(),
            new RuntimeException("Async timeout — no completion received within TTL"));
    }
}
```

```java
// workers-common/src/main/java/io/casehub/workers/common/WorkerFaultCallbackObserver.java
package io.casehub.workers.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class WorkerFaultCallbackObserver {

    @Inject
    WorkerFaultPublisher faultPublisher;

    void onFaultCallback(@ObservesAsync FaultCallbackEvent event) {
        faultPublisher.fault(event.pending(), event.cause());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-common -Dtest="WorkerCompletionExpiryObserverTest,WorkerFaultCallbackObserverTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "refactor(#9): extract generic CDI event observers to workers-common"
```

---

### Task 4: Create `WorkerFaultHandler` in workers-common

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultHandler.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerFaultHandlerTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-common/src/test/java/io/casehub/workers/common/WorkerFaultHandlerTest.java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkerFaultHandlerTest {

    WorkerFaultHandler handler;
    WorkerRetrySupport retrySupport;
    WorkerExecutionManager workerExecutionManager;
    Vertx vertx;
    EventLogRepository eventLogRepository;

    @BeforeEach
    void setUp() {
        handler = new WorkerFaultHandler();
        handler.retrySupport = mock(WorkerRetrySupport.class);
        handler.workerExecutionManager = mock(WorkerExecutionManager.class);
        handler.vertx = Vertx.vertx();
        handler.eventLogRepository = mock(EventLogRepository.class);

        retrySupport = handler.retrySupport;
        workerExecutionManager = handler.workerExecutionManager;
        vertx = handler.vertx;
        eventLogRepository = handler.eventLogRepository;
    }

    @Test
    void handleFault_permanentFault_skipsRetry() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("w1", List.of(), (ctx) -> null);
        Capability capability = new Capability("cap", "", "");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, capability, "hash-1", "1",
            new PermanentFaultException(400, "bad request"));

        handler.handleFault(event).await().indefinitely();

        verify(retrySupport).persistFailureLog(instance, worker, "hash-1", "bad request", "t1");
        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(workerExecutionManager, never()).submit(any(), any(), any(), any(), any());
    }

    @Test
    void handleFault_retryableAndUnderMax_retriesWithBackoff() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        RetryPolicy retryPolicy = new RetryPolicy(3, 100L, null);
        Worker worker = new Worker("w1", List.of(),
            new ExecutionPolicy(retryPolicy), (ctx) -> null);
        Capability capability = new Capability("cap", "", "");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        eventLog.setPayload(new com.fasterxml.jackson.databind.ObjectMapper()
            .valueToTree(Map.of("key", "value")));
        when(eventLogRepository.findById(1L, "t1"))
            .thenReturn(Uni.createFrom().item(eventLog));
        when(workerExecutionManager.submit(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, capability, "hash-1", "1",
            new RuntimeException("transient error"));

        handler.handleFault(event).await().indefinitely();

        verify(retrySupport).persistFailureLog(instance, worker, "hash-1", "transient error", "t1");
        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any());
        verify(workerExecutionManager).submit(eq(1L), eq(instance), eq(worker), eq(capability), any());
    }

    @Test
    void handleFault_retriesExhausted_publishesExhausted() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        RetryPolicy retryPolicy = new RetryPolicy(2, 100L, null);
        Worker worker = new Worker("w1", List.of(),
            new ExecutionPolicy(retryPolicy), (ctx) -> null);
        Capability capability = new Capability("cap", "", "");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(2L));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, capability, "hash-1", "1",
            new RuntimeException("still failing"));

        handler.handleFault(event).await().indefinitely();

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(workerExecutionManager, never()).submit(any(), any(), any(), any(), any());
    }

    @Test
    void handleFault_retryAfterException_usesOverriddenDelay() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        RetryPolicy retryPolicy = new RetryPolicy(3, 10_000L, null);
        Worker worker = new Worker("w1", List.of(),
            new ExecutionPolicy(retryPolicy), (ctx) -> null);
        Capability capability = new Capability("cap", "", "");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());
        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(0L));

        EventLog eventLog = new EventLog();
        eventLog.setPayload(new com.fasterxml.jackson.databind.ObjectMapper()
            .valueToTree(Map.of("key", "value")));
        when(eventLogRepository.findById(1L, "t1"))
            .thenReturn(Uni.createFrom().item(eventLog));
        when(workerExecutionManager.submit(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, capability, "hash-1", "1",
            new RetryAfterException(500, "429 rate limited"));

        handler.handleFault(event).await().indefinitely();

        verify(workerExecutionManager).submit(eq(1L), eq(instance), eq(worker), eq(capability), any());
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultHandlerTest`
Expected: FAIL — `WorkerFaultHandler` does not exist

- [ ] **Step 3: Write the implementation**

```java
// workers-common/src/main/java/io/casehub/workers/common/WorkerFaultHandler.java
package io.casehub.workers.common;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class WorkerFaultHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(WorkerFaultHandler.class);

    @Inject WorkerRetrySupport retrySupport;
    @Inject WorkerExecutionManager workerExecutionManager;
    @Inject Vertx vertx;
    @Inject EventLogRepository eventLogRepository;

    public Uni<Void> handleFault(WorkflowExecutionFailed event) {
        CaseInstance instance = event.caseInstance();
        Worker worker = event.worker();
        String inputDataHash = event.inputDataHash();
        String tenancyId = instance.tenancyId;
        String errorMsg = (event.cause() != null && event.cause().getMessage() != null)
            ? event.cause().getMessage() : "unknown";

        return retrySupport.persistFailureLog(instance, worker, inputDataHash, errorMsg, tenancyId)
            .flatMap(ignored -> {
                if (event.cause() instanceof PermanentFaultException) {
                    retrySupport.publishRetriesExhausted(
                        instance.getUuid(), worker.getName(), inputDataHash);
                    return Uni.createFrom().voidItem();
                }

                return retrySupport.countFailedAttempts(
                        instance.getUuid(), worker.getName(), inputDataHash, tenancyId)
                    .flatMap(failureCount -> {
                        RetryPolicy retryPolicy = WorkerRetrySupport.resolveRetryPolicy(worker);
                        if (failureCount < retryPolicy.maxAttempts()) {
                            long delayMs;
                            if (event.cause() instanceof RetryAfterException ra) {
                                delayMs = ra.retryAfterMs();
                            } else {
                                delayMs = WorkerRetrySupport.computeBackoffDelayMs(
                                    retryPolicy, failureCount + 1);
                            }
                            return reloadAndResubmit(event, delayMs);
                        } else {
                            retrySupport.publishRetriesExhausted(
                                instance.getUuid(), worker.getName(), inputDataHash);
                            return Uni.createFrom().voidItem();
                        }
                    });
            })
            .onFailure().recoverWithUni(ex -> {
                LOG.errorf(ex, "Fault handling failed for worker %s case %s — case may stall",
                           worker.getName(), instance.getUuid());
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> reloadAndResubmit(WorkflowExecutionFailed event, long delayMs) {
        return eventLogRepository
            .findById(Long.parseLong(event.eventLogId()), event.caseInstance().tenancyId)
            .flatMap(eventLog -> {
                Map<String, Object> inputData =
                    OBJECT_MAPPER.convertValue(eventLog.getPayload(), MAP_TYPE);
                return Uni.createFrom().<Void>emitter(em -> {
                        long timerId = vertx.setTimer(delayMs, id -> em.complete(null));
                        em.onTermination(() -> vertx.cancelTimer(timerId));
                    })
                    .emitOn(Infrastructure.getDefaultWorkerPool())
                    .flatMap(ignored -> workerExecutionManager.submit(
                        Long.parseLong(event.eventLogId()),
                        event.caseInstance(), event.worker(), event.capability(), inputData));
            });
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerFaultHandlerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "refactor(#9): extract WorkerFaultHandler to workers-common — shared retry logic"
```

---

### Task 5: Delete per-module publishers and observers, shrink fault handlers

**Files:**
- Delete: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultPublisher.java`
- Delete: `workers-http/src/main/java/io/casehub/workers/http/HttpCompletionExpiryObserver.java`
- Delete: `workers-http/src/main/java/io/casehub/workers/http/HttpFaultCallbackObserver.java`
- Delete: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultPublisherTest.java`
- Delete: `workers-http/src/test/java/io/casehub/workers/http/HttpCompletionExpiryObserverTest.java`
- Delete: `workers-http/src/test/java/io/casehub/workers/http/HttpFaultCallbackObserverTest.java`
- Delete: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultPublisher.java`
- Delete: `workers-camel/src/main/java/io/casehub/workers/camel/CamelCompletionExpiryObserver.java`
- Delete: `workers-camel/src/main/java/io/casehub/workers/camel/CamelFaultCallbackObserver.java`
- Delete: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultPublisherTest.java`
- Delete: `workers-camel/src/test/java/io/casehub/workers/camel/CamelCompletionExpiryObserverTest.java`
- Delete: `workers-camel/src/test/java/io/casehub/workers/camel/CamelFaultCallbackObserverTest.java`
- Delete: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerFaultPublisher.java`
- Delete: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultPublisher.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerExecutionManager.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerExecutionManager.java`
- Modify: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManager.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultEventHandler.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultEventHandler.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerFaultEventHandler.java`
- Modify: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultEventHandler.java`

- [ ] **Step 1: Update all 4 execution managers — replace per-module fault publisher with generic**

In each execution manager, change:
- `@Inject XxxWorkerFaultPublisher faultPublisher;` → `@Inject WorkerFaultPublisher faultPublisher;`
- All `faultPublisher.fault(ctx, capability, eventLogId, t)` → `faultPublisher.fault(XxxWorkerEventBusAddresses.XXX_WORKER_FAULT, ctx, capability, eventLogId, t)`
- Update imports: remove per-module publisher import, add `io.casehub.workers.common.WorkerFaultPublisher`

For `HttpWorkerExecutionManager`:
```java
@Inject WorkerFaultPublisher faultPublisher;
// ...
faultPublisher.fault(HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT, ctx, capability, eventLogId, e);
```

For `CamelWorkerExecutionManager`:
```java
@Inject WorkerFaultPublisher faultPublisher;
// ...
faultPublisher.fault(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT, ctx, capability, eventLogId, e);
```

For `McpWorkerExecutionManager`:
```java
@Inject WorkerFaultPublisher faultPublisher;
// ...
faultPublisher.fault(McpWorkerEventBusAddresses.MCP_WORKER_FAULT, ctx, capability, eventLogId, t);
```

For `GitHubActionsWorkerExecutionManager`:
```java
@Inject WorkerFaultPublisher faultPublisher;
// ...
faultPublisher.fault(GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT, ctx, capability, eventLogId, e);
```

- [ ] **Step 2: Shrink all 4 fault event handlers to stubs**

Replace each ~90-line handler body with a delegation to `WorkerFaultHandler`:

`HttpWorkerFaultEventHandler`:
```java
package io.casehub.workers.http;

import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerFaultHandler;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class HttpWorkerFaultEventHandler {

    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        return workerFaultHandler.handleFault(event);
    }
}
```

`CamelWorkerFaultEventHandler`:
```java
package io.casehub.workers.camel;

import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerFaultHandler;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelWorkerFaultEventHandler {

    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        return workerFaultHandler.handleFault(event);
    }
}
```

`McpWorkerFaultEventHandler`:
```java
package io.casehub.workers.mcp;

import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerFaultHandler;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class McpWorkerFaultEventHandler {

    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = McpWorkerEventBusAddresses.MCP_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        return workerFaultHandler.handleFault(event);
    }
}
```

`GitHubActionsWorkerFaultEventHandler`:
```java
package io.casehub.workers.githubactions;

import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerFaultHandler;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class GitHubActionsWorkerFaultEventHandler {

    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        return workerFaultHandler.handleFault(event);
    }
}
```

- [ ] **Step 3: Delete the 8 per-module production classes**

Delete these files:
- `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultPublisher.java`
- `workers-http/src/main/java/io/casehub/workers/http/HttpCompletionExpiryObserver.java`
- `workers-http/src/main/java/io/casehub/workers/http/HttpFaultCallbackObserver.java`
- `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultPublisher.java`
- `workers-camel/src/main/java/io/casehub/workers/camel/CamelCompletionExpiryObserver.java`
- `workers-camel/src/main/java/io/casehub/workers/camel/CamelFaultCallbackObserver.java`
- `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerFaultPublisher.java`
- `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultPublisher.java`

- [ ] **Step 4: Delete the corresponding test classes**

Delete these test files:
- `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultPublisherTest.java`
- `workers-http/src/test/java/io/casehub/workers/http/HttpCompletionExpiryObserverTest.java`
- `workers-http/src/test/java/io/casehub/workers/http/HttpFaultCallbackObserverTest.java`
- `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultPublisherTest.java`
- `workers-camel/src/test/java/io/casehub/workers/camel/CamelCompletionExpiryObserverTest.java`
- `workers-camel/src/test/java/io/casehub/workers/camel/CamelFaultCallbackObserverTest.java`

- [ ] **Step 5: Simplify existing per-module fault handler tests**

The remaining `HttpWorkerFaultEventHandlerTest`, `CamelWorkerFaultEventHandlerTest`, `McpWorkerFaultEventHandlerTest`, and `GitHubActionsWorkerFaultEventHandlerTest` should be simplified to verify delegation only (the logic is now tested in `WorkerFaultHandlerTest`). Each test should verify that `onFault()` delegates to `workerFaultHandler.handleFault()`.

- [ ] **Step 6: Build and test everything**

Run: `mvn --batch-mode test`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 7: Commit**

```
git add -A
git commit -m "refactor(#9): delete 8 per-module fault classes — replaced by workers-common generics

Deletes: HttpWorkerFaultPublisher, CamelWorkerFaultPublisher,
McpWorkerFaultPublisher, GitHubActionsWorkerFaultPublisher,
HttpCompletionExpiryObserver, CamelCompletionExpiryObserver,
HttpFaultCallbackObserver, CamelFaultCallbackObserver.

Shrinks all 4 per-module fault handlers to 5-line stubs.
Fixes Camel missing PermanentFaultException check."
```

---

## Phase 2: Script Worker Module

### Task 6: Module skeleton

**Files:**
- Create: `workers-script/pom.xml`
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerConstants.java`
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerEventBusAddresses.java`
- Modify: `pom.xml` (parent — add module)

- [ ] **Step 1: Create `workers-script/pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 https://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>io.casehub</groupId>
        <artifactId>casehub-workers-parent</artifactId>
        <version>0.2-SNAPSHOT</version>
    </parent>

    <artifactId>casehub-workers-script</artifactId>

    <name>CaseHub Workers :: Script</name>
    <description>WorkerProvisioner implementation for script workers — dispatch case steps to local subprocesses</description>

    <dependencies>
        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-workers-common</artifactId>
        </dependency>
        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-engine-api</artifactId>
        </dependency>
        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-engine-common</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-arc</artifactId>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-vertx</artifactId>
        </dependency>

        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-workers-testing</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>io.quarkus</groupId>
            <artifactId>quarkus-junit</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.assertj</groupId>
            <artifactId>assertj-core</artifactId>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>io.smallrye</groupId>
                <artifactId>jandex-maven-plugin</artifactId>
                <version>${jandex-maven-plugin.version}</version>
                <executions>
                    <execution>
                        <id>make-index</id>
                        <goals><goal>jandex</goal></goals>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
</project>
```

- [ ] **Step 2: Add module to parent POM**

In `pom.xml`, add `<module>workers-script</module>` before `<module>workers-testing</module>`:

```xml
<modules>
    <module>workers-common</module>
    <module>workers-http</module>
    <module>workers-camel</module>
    <module>workers-github-actions</module>
    <module>workers-mcp</module>
    <module>workers-script</module>
    <module>workers-testing</module>
</modules>
```

- [ ] **Step 3: Create constants classes**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerConstants.java
package io.casehub.workers.script;

public final class ScriptWorkerConstants {
    public static final String WORKER_TYPE = "script";
    private ScriptWorkerConstants() {}
}
```

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerEventBusAddresses.java
package io.casehub.workers.script;

public final class ScriptWorkerEventBusAddresses {
    public static final String SCRIPT_WORKER_FAULT = "casehub.workers.script.fault";
    private ScriptWorkerEventBusAddresses() {}
}
```

- [ ] **Step 4: Build to verify module structure**

Run: `mvn --batch-mode compile -pl workers-script`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(#9): workers-script module skeleton — pom, constants, event bus addresses"
```

---

### Task 7: `ScriptDefinition` and `ScriptDefinitionResolver`

**Files:**
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptDefinition.java`
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptDefinitionResolver.java`
- Create: `workers-script/src/test/java/io/casehub/workers/script/ScriptDefinitionResolverTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-script/src/test/java/io/casehub/workers/script/ScriptDefinitionResolverTest.java
package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScriptDefinitionResolverTest {

    @Test
    void initialize_parsesConfigIntoDefinitions() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "data-pipeline", new ScriptDefinition(
                "data-pipeline", "python3",
                List.of("/opt/scripts/pipeline.py", "--mode", "batch"),
                "/opt/data",
                Map.of("PYTHONPATH", "/opt/lib"),
                600, 2_097_152)
        ));

        assertThat(resolver.capabilities()).containsExactly("script:data-pipeline");
    }

    @Test
    void resolve_knownTag_returnsDefinition() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "run-tests", new ScriptDefinition(
                "run-tests", "/bin/sh",
                List.of("-c", "echo test"), null,
                Map.of(), 300, 1_048_576)
        ));

        ScriptDefinition def = resolver.resolve("script:run-tests");

        assertThat(def.name()).isEqualTo("run-tests");
        assertThat(def.command()).isEqualTo("/bin/sh");
        assertThat(def.args()).containsExactly("-c", "echo test");
    }

    @Test
    void resolve_unknownTag_throws() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of());

        assertThatThrownBy(() -> resolver.resolve("script:missing"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void resolve_tagWithoutPrefix_throws() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "run-tests", new ScriptDefinition(
                "run-tests", "/bin/sh",
                List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThatThrownBy(() -> resolver.resolve("run-tests"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstKnownCapability() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "build", new ScriptDefinition(
                "build", "make", List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThat(resolver.firstMatch(Set.of("script:build", "script:unknown")))
            .contains("script:build");
    }

    @Test
    void firstMatch_noMatch_returnsEmpty() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of());

        assertThat(resolver.firstMatch(Set.of("script:unknown"))).isEmpty();
    }

    @Test
    void capabilities_returnsAllPrefixedTags() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "a", new ScriptDefinition("a", "cmd", List.of(), null, Map.of(), 300, 1_048_576),
            "b", new ScriptDefinition("b", "cmd", List.of(), null, Map.of(), 300, 1_048_576)
        ));

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("script:a", "script:b");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptDefinitionResolverTest`
Expected: FAIL — classes do not exist

- [ ] **Step 3: Write `ScriptDefinition`**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptDefinition.java
package io.casehub.workers.script;

import java.util.List;
import java.util.Map;

public record ScriptDefinition(
    String name,
    String command,
    List<String> args,
    String workingDirectory,
    Map<String, String> environment,
    int timeoutSeconds,
    long maxOutputBytes
) {}
```

- [ ] **Step 4: Write `ScriptDefinitionResolver`**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptDefinitionResolver.java
package io.casehub.workers.script;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.eclipse.microprofile.config.Config;
import jakarta.inject.Inject;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@ApplicationScoped
public class ScriptDefinitionResolver implements WorkerCapabilityResolver<ScriptDefinition> {

    private static final String TAG_PREFIX = "script:";

    @Inject
    Config config;

    @ConfigProperty(name = "casehub.workers.script.default-timeout-seconds", defaultValue = "300")
    int defaultTimeoutSeconds;

    @ConfigProperty(name = "casehub.workers.script.max-output-bytes", defaultValue = "1048576")
    long defaultMaxOutputBytes;

    private final Map<String, ScriptDefinition> definitions = new HashMap<>();

    void initialize() {
        Map<String, Map<String, String>> configScripts = loadConfigScripts();
        Map<String, ScriptDefinition> parsed = new HashMap<>();
        configScripts.forEach((name, props) -> {
            parsed.put(name, buildFromConfig(name, props));
        });
        initialize(parsed);
    }

    void initialize(Map<String, ScriptDefinition> scriptDefinitions) {
        definitions.clear();
        definitions.putAll(scriptDefinitions);
    }

    @Override
    public ScriptDefinition resolve(String capabilityTag) {
        if (!capabilityTag.startsWith(TAG_PREFIX)) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        String name = capabilityTag.substring(TAG_PREFIX.length());
        ScriptDefinition def = definitions.get(name);
        if (def == null) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        return def;
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities) {
        return capabilities.stream()
            .filter(cap -> {
                if (!cap.startsWith(TAG_PREFIX)) return false;
                return definitions.containsKey(cap.substring(TAG_PREFIX.length()));
            })
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(definitions.keySet().stream()
            .map(name -> TAG_PREFIX + name)
            .toList());
    }

    private ScriptDefinition buildFromConfig(String name, Map<String, String> props) {
        String command = props.get("command");
        List<String> args = parseArgs(props.get("args"));
        String workingDirectory = props.get("working-directory");
        Map<String, String> env = extractEnvironment(props);
        int timeout = parseIntOrDefault(props.get("timeout-seconds"), defaultTimeoutSeconds);
        long maxOutput = parseLongOrDefault(props.get("max-output-bytes"), defaultMaxOutputBytes);
        return new ScriptDefinition(name, command, args, workingDirectory, env, timeout, maxOutput);
    }

    private List<String> parseArgs(String value) {
        if (value == null || value.isBlank()) return List.of();
        return List.of(value.split(","));
    }

    private Map<String, String> extractEnvironment(Map<String, String> props) {
        Map<String, String> env = new LinkedHashMap<>();
        String prefix = "environment.";
        props.forEach((key, value) -> {
            if (key.startsWith(prefix)) {
                env.put(key.substring(prefix.length()), value);
            }
        });
        return env.isEmpty() ? Map.of() : Map.copyOf(env);
    }

    private Map<String, Map<String, String>> loadConfigScripts() {
        if (config == null) return Map.of();
        String prefix = "casehub.workers.script.scripts.";
        Map<String, Map<String, String>> result = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String name = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        result.computeIfAbsent(name, k -> new LinkedHashMap<>()).put(prop, value)
                    );
                }
            }
        }
        return result;
    }

    private static int parseIntOrDefault(String value, int defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try { return Integer.parseInt(value); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    private static long parseLongOrDefault(String value, long defaultValue) {
        if (value == null || value.isBlank()) return defaultValue;
        try { return Long.parseLong(value); }
        catch (NumberFormatException e) { return defaultValue; }
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptDefinitionResolverTest`
Expected: PASS

- [ ] **Step 6: Commit**

```
git add -A
git commit -m "feat(#9): ScriptDefinition + ScriptDefinitionResolver — config-driven script registry"
```

---

### Task 8: `ScriptWorkerRuntime`

**Files:**
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerRuntime.java`
- Create: `workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerRuntimeTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerRuntimeTest.java
package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.workers.common.WorkerRuntimeStatus;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ScriptWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("script");
    }

    @Test
    void initialize_withScripts_transitionsToRunning() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("script:test");
    }

    @Test
    void initialize_noScripts_transitionsToFaulted() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of());
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_whenFaulted_retriesAndRecovers() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of());
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "test", new ScriptDefinition("test", "echo", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptWorkerRuntime runtime = new ScriptWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerRuntimeTest`
Expected: FAIL — `ScriptWorkerRuntime` does not exist

- [ ] **Step 3: Write the implementation**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerRuntime.java
package io.casehub.workers.script;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ScriptWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(ScriptWorkerRuntime.class);

    private final ScriptDefinitionResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    ScriptWorkerRuntime(ScriptDefinitionResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return ScriptWorkerConstants.WORKER_TYPE;
    }

    @Override
    public WorkerRuntimeStatus status() {
        return status;
    }

    @Override
    public Uni<Void> initialize() {
        if (status == WorkerRuntimeStatus.RUNNING) {
            return Uni.createFrom().voidItem();
        }
        return Uni.createFrom().item(() -> {
            try {
                if (resolver.capabilities().isEmpty()) {
                    resolver.initialize();
                }
                if (resolver.capabilities().isEmpty()) {
                    LOG.warn("No scripts configured — status FAULTED");
                    status = WorkerRuntimeStatus.FAULTED;
                } else {
                    status = WorkerRuntimeStatus.RUNNING;
                }
            } catch (Exception e) {
                status = WorkerRuntimeStatus.FAULTED;
                throw e;
            }
            return null;
        }).replaceWithVoid();
    }

    @Override
    public Uni<Void> shutdown() {
        status = WorkerRuntimeStatus.STOPPED;
        return Uni.createFrom().voidItem();
    }

    @Override
    public Set<String> capabilities() {
        return resolver.capabilities();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerRuntimeTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(#9): ScriptWorkerRuntime — lifecycle SPI, zero-scripts FAULTED"
```

---

### Task 9: `ScriptReactiveWorkerProvisioner`

**Files:**
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptReactiveWorkerProvisioner.java`
- Create: `workers-script/src/test/java/io/casehub/workers/script/ScriptReactiveWorkerProvisionerTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-script/src/test/java/io/casehub/workers/script/ScriptReactiveWorkerProvisionerTest.java
package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ScriptReactiveWorkerProvisionerTest {

    @Test
    void provision_knownCapability_returnsEmpty() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "build", new ScriptDefinition("build", "make", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptReactiveWorkerProvisioner provisioner = new ScriptReactiveWorkerProvisioner();
        provisioner.scriptDefinitionResolver = resolver;

        ProvisionResult result = provisioner
            .provision(Set.of("script:build"), new ProvisionContext(null))
            .await().indefinitely();

        assertThat(result).isEqualTo(ProvisionResult.empty());
    }

    @Test
    void provision_unknownCapability_throws() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of());
        ScriptReactiveWorkerProvisioner provisioner = new ScriptReactiveWorkerProvisioner();
        provisioner.scriptDefinitionResolver = resolver;

        assertThatThrownBy(() ->
            provisioner.provision(Set.of("script:missing"), new ProvisionContext(null))
                .await().indefinitely())
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void getCapabilities_delegatesToResolver() {
        ScriptDefinitionResolver resolver = new ScriptDefinitionResolver();
        resolver.initialize(Map.of(
            "a", new ScriptDefinition("a", "cmd", List.of(), null, Map.of(), 300, 1_048_576),
            "b", new ScriptDefinition("b", "cmd", List.of(), null, Map.of(), 300, 1_048_576)
        ));
        ScriptReactiveWorkerProvisioner provisioner = new ScriptReactiveWorkerProvisioner();
        provisioner.scriptDefinitionResolver = resolver;

        Set<String> caps = provisioner.getCapabilities().await().indefinitely();

        assertThat(caps).containsExactlyInAnyOrder("script:a", "script:b");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptReactiveWorkerProvisionerTest`
Expected: FAIL

- [ ] **Step 3: Write the implementation**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptReactiveWorkerProvisioner.java
package io.casehub.workers.script;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.api.spi.ReactiveWorkerProvisioner;
import io.casehub.workers.common.WorkerProvisioningException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class ScriptReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    @Inject
    ScriptDefinitionResolver scriptDefinitionResolver;

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        String capability = scriptDefinitionResolver.firstMatch(capabilities)
            .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
        scriptDefinitionResolver.resolve(capability);
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId, String tenancyId) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        return Uni.createFrom().item(scriptDefinitionResolver.capabilities());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptReactiveWorkerProvisionerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(#9): ScriptReactiveWorkerProvisioner — capability probe"
```

---

### Task 10: `ScriptWorkerExecutionManager`

**Files:**
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerExecutionManager.java`
- Create: `workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerExecutionManagerTest.java`

- [ ] **Step 1: Write the failing tests**

```java
// workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerExecutionManagerTest.java
package io.casehub.workers.script;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCompletionPayload;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class ScriptWorkerExecutionManagerTest {

    ScriptWorkerExecutionManager manager;
    ScriptDefinitionResolver resolver;
    WorkerFaultPublisher faultPublisher;
    WorkflowCompletionPublisher completionPublisher;

    @BeforeEach
    void setUp() {
        manager = new ScriptWorkerExecutionManager();
        resolver = new ScriptDefinitionResolver();
        faultPublisher = mock(WorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);

        manager.scriptDefinitionResolver = resolver;
        manager.faultPublisher = faultPublisher;
        manager.completionPublisher = completionPublisher;
        manager.init();
    }

    @Test
    void submit_jsonObjectStdout_parsedAsStructuredOutput() {
        resolver.initialize(Map.of(
            "json-script", new ScriptDefinition("json-script", "/bin/sh",
                List.of("-c", "echo '{\"result\":\"ok\"}'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:json-script"),
            WorkerTestSupport.testCapability("script:json-script"),
            Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue()).containsEntry("result", "ok");
    }

    @Test
    void submit_nonJsonStdout_wrappedAsRaw() {
        resolver.initialize(Map.of(
            "text-script", new ScriptDefinition("text-script", "/bin/sh",
                List.of("-c", "echo hello world"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:text-script"),
            WorkerTestSupport.testCapability("script:text-script"),
            Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        Map<String, Object> output = captor.getValue();
        assertThat(output.get("stdout").toString()).contains("hello world");
        assertThat(output).containsKey("stderr");
        assertThat(output).containsEntry("exitCode", 0);
    }

    @Test
    void submit_jsonArrayStdout_fallsThroughToRawWrapper() {
        resolver.initialize(Map.of(
            "array-script", new ScriptDefinition("array-script", "/bin/sh",
                List.of("-c", "echo '[1,2,3]'"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:array-script"),
            WorkerTestSupport.testCapability("script:array-script"),
            Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        Map<String, Object> output = captor.getValue();
        assertThat(output.get("stdout").toString()).contains("[1,2,3]");
        assertThat(output).containsEntry("exitCode", 0);
    }

    @Test
    void submit_nonZeroExit_faults() {
        resolver.initialize(Map.of(
            "fail-script", new ScriptDefinition("fail-script", "/bin/sh",
                List.of("-c", "exit 1"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:fail-script"),
            WorkerTestSupport.testCapability("script:fail-script"),
            Map.of()).await().indefinitely();

        verify(faultPublisher).fault(
            eq(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(RuntimeException.class));
        verify(completionPublisher, never()).complete(any(), any());
    }

    @Test
    void submit_timeout_permanentFault() {
        resolver.initialize(Map.of(
            "slow-script", new ScriptDefinition("slow-script", "/bin/sh",
                List.of("-c", "sleep 60"),
                null, Map.of(), 1, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:slow-script"),
            WorkerTestSupport.testCapability("script:slow-script"),
            Map.of()).await().indefinitely();

        verify(faultPublisher).fault(
            eq(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_stdinDelivery_inputDataReachesProcess() {
        resolver.initialize(Map.of(
            "stdin-script", new ScriptDefinition("stdin-script", "/bin/sh",
                List.of("-c", "cat"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:stdin-script"),
            WorkerTestSupport.testCapability("script:stdin-script"),
            Map.of("key", "value")).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue()).containsEntry("key", "value");
    }

    @Test
    void submit_environmentVariablesSet() {
        resolver.initialize(Map.of(
            "env-script", new ScriptDefinition("env-script", "/bin/sh",
                List.of("-c", "echo $CASEHUB_CAPABILITY"),
                null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:env-script"),
            WorkerTestSupport.testCapability("script:env-script"),
            Map.of()).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> captor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), captor.capture());
        assertThat(captor.getValue().get("stdout").toString()).contains("script:env-script");
    }

    @Test
    void submit_commandNotFound_permanentFault() {
        resolver.initialize(Map.of(
            "missing-cmd", new ScriptDefinition("missing-cmd",
                "/nonexistent/command/xyz123",
                List.of(), null, Map.of(), 30, 1_048_576)
        ));

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:missing-cmd"),
            WorkerTestSupport.testCapability("script:missing-cmd"),
            Map.of()).await().indefinitely();

        verify(faultPublisher).fault(
            eq(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }

    @Test
    void submit_unknownCapability_permanentFault() {
        resolver.initialize(Map.of());

        manager.submit(1L,
            WorkerTestSupport.testCaseInstance(),
            WorkerTestSupport.testWorker("w1", "script:missing"),
            WorkerTestSupport.testCapability("script:missing"),
            Map.of()).await().indefinitely();

        verify(faultPublisher).fault(
            eq(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT),
            any(WorkerCorrelationContext.class),
            any(Capability.class), eq(1L), any(PermanentFaultException.class));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerExecutionManagerTest`
Expected: FAIL

- [ ] **Step 3: Write the implementation**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerExecutionManager.java
package io.casehub.workers.script;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerFaultPublisher;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.jboss.logging.Logger;

@ApplicationScoped
public class ScriptWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(ScriptWorkerExecutionManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final int READ_BUFFER_SIZE = 8192;

    @Inject ScriptDefinitionResolver scriptDefinitionResolver;
    @Inject WorkerFaultPublisher faultPublisher;
    @Inject WorkflowCompletionPublisher completionPublisher;

    private ExecutorService stderrDrainExecutor;

    @PostConstruct
    void init() {
        stderrDrainExecutor = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "script-stderr-drain");
            t.setDaemon(true);
            return t;
        });
    }

    @PreDestroy
    void shutdown() {
        if (stderrDrainExecutor != null) {
            stderrDrainExecutor.shutdownNow();
        }
    }

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        ScriptDefinition definition;
        try {
            definition = scriptDefinitionResolver.resolve(capability.getName());
        } catch (WorkerProvisioningException e) {
            WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData);
            faultPublisher.fault(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT,
                ctx, capability, eventLogId,
                new PermanentFaultException(0, e.getMessage()));
            return Uni.createFrom().voidItem();
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData);

        return Uni.createFrom()
            .item(() -> executeProcess(definition, inputData, ctx, capability))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .flatMap(output -> {
                completionPublisher.complete(ctx, output);
                return Uni.createFrom().<Void>voidItem();
            })
            .onFailure().recoverWithUni(t -> {
                faultPublisher.fault(ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT,
                    ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    @Override
    public Uni<Void> schedulePersistedEvent(EventLog scheduledEventLog) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return 0;
    }

    Map<String, Object> executeProcess(ScriptDefinition definition,
                                        Map<String, Object> inputData,
                                        WorkerCorrelationContext ctx,
                                        Capability capability) {
        List<String> command = new ArrayList<>();
        command.add(definition.command());
        command.addAll(definition.args());

        ProcessBuilder pb = new ProcessBuilder(command);
        pb.redirectErrorStream(false);

        if (definition.workingDirectory() != null) {
            pb.directory(new java.io.File(definition.workingDirectory()));
        }

        Map<String, String> env = pb.environment();
        if (definition.environment() != null) {
            env.putAll(definition.environment());
        }
        env.put("CASEHUB_CASE_ID", ctx.caseInstance().getUuid().toString());
        env.put("CASEHUB_TENANCY_ID", ctx.tenancyId() != null ? ctx.tenancyId() : "");
        env.put("CASEHUB_CAPABILITY", capability.getName());
        env.put("CASEHUB_IDEMPOTENCY", ctx.idempotency());

        Process process;
        try {
            process = pb.start();
        } catch (IOException e) {
            throw new PermanentFaultException(0, "Command not found: " + definition.command());
        }

        try {
            byte[] inputJson = OBJECT_MAPPER.writeValueAsBytes(inputData);
            try (OutputStream stdin = process.getOutputStream()) {
                stdin.write(inputJson);
            }
        } catch (IOException e) {
            process.destroyForcibly();
            throw new RuntimeException("Failed to write inputData to stdin: " + e.getMessage());
        }

        CompletableFuture<String> stderrFuture = CompletableFuture.supplyAsync(
            () -> drainStream(process.getErrorStream(), definition.maxOutputBytes()),
            stderrDrainExecutor);

        String stdout = drainStream(process.getInputStream(), definition.maxOutputBytes());

        boolean completed;
        try {
            completed = process.waitFor(definition.timeoutSeconds(), TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
            throw new RuntimeException("Interrupted waiting for process");
        }

        if (!completed) {
            process.destroyForcibly();
            try { process.waitFor(); } catch (InterruptedException ignored) {
                Thread.currentThread().interrupt();
            }
            throw new PermanentFaultException(0,
                "Process timed out after " + definition.timeoutSeconds() + "s");
        }

        String stderr;
        try {
            stderr = stderrFuture.join();
        } catch (Exception e) {
            stderr = "";
        }

        int exitCode = process.exitValue();
        if (exitCode != 0) {
            throw new RuntimeException("Process exited with code " + exitCode
                + ": " + (stderr.isBlank() ? stdout : stderr).trim());
        }

        return parseOutput(stdout, stderr, exitCode);
    }

    private Map<String, Object> parseOutput(String stdout, String stderr, int exitCode) {
        String trimmed = stdout.trim();
        if (!trimmed.isEmpty() && trimmed.startsWith("{")) {
            try {
                return OBJECT_MAPPER.readValue(trimmed, MAP_TYPE);
            } catch (Exception ignored) {
                // fall through to raw wrapper
            }
        }
        return Map.of(
            "stdout", stdout.trim(),
            "stderr", stderr.trim(),
            "exitCode", exitCode);
    }

    static String drainStream(InputStream stream, long maxBytes) {
        try {
            ByteArrayOutputStream captured = new ByteArrayOutputStream();
            byte[] buffer = new byte[READ_BUFFER_SIZE];
            long totalCaptured = 0;
            int bytesRead;

            while ((bytesRead = stream.read(buffer)) != -1) {
                if (totalCaptured < maxBytes) {
                    int toWrite = (int) Math.min(bytesRead, maxBytes - totalCaptured);
                    captured.write(buffer, 0, toWrite);
                    totalCaptured += toWrite;
                }
                // continue reading to prevent pipe deadlock even after cap
            }

            return captured.toString(StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "";
        }
    }

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerExecutionManagerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(#9): ScriptWorkerExecutionManager — subprocess dispatch, stdin/stdout, bounded capture"
```

---

### Task 11: `ScriptWorkerFaultEventHandler` (stub)

**Files:**
- Create: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerFaultEventHandler.java`
- Create: `workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerFaultEventHandlerTest.java`

- [ ] **Step 1: Write the failing test**

```java
// workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerFaultEventHandlerTest.java
package io.casehub.workers.script;

import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.WorkerFaultHandler;
import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class ScriptWorkerFaultEventHandlerTest {

    @Test
    void onFault_delegatesToWorkerFaultHandler() {
        WorkerFaultHandler faultHandler = mock(WorkerFaultHandler.class);
        ScriptWorkerFaultEventHandler handler = new ScriptWorkerFaultEventHandler();
        handler.workerFaultHandler = faultHandler;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("w1", List.of(), (ctx) -> null);
        Capability capability = new Capability("script:test", "", "");

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, capability, "hash-1", "1",
            new RuntimeException("script failed"));

        when(faultHandler.handleFault(event)).thenReturn(Uni.createFrom().voidItem());

        handler.onFault(event).await().indefinitely();

        verify(faultHandler).handleFault(event);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerFaultEventHandlerTest`
Expected: FAIL

- [ ] **Step 3: Write the implementation**

```java
// workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerFaultEventHandler.java
package io.casehub.workers.script;

import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerFaultHandler;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class ScriptWorkerFaultEventHandler {

    @Inject WorkerFaultHandler workerFaultHandler;

    @ConsumeEvent(value = ScriptWorkerEventBusAddresses.SCRIPT_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
        return workerFaultHandler.handleFault(event);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn --batch-mode test -pl workers-script -Dtest=ScriptWorkerFaultEventHandlerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add -A
git commit -m "feat(#9): ScriptWorkerFaultEventHandler — 5-line stub delegating to shared handler"
```

---

### Task 12: Full build verification and CLAUDE.md update

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Run full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS, all tests pass across all modules

- [ ] **Step 2: Update CLAUDE.md**

Add `workers-script` to the Module Structure table, add the workers-script key types section, update Key Rules and Co-deployment sections. Follow the pattern of existing worker entries.

- [ ] **Step 3: Commit**

```
git add -A
git commit -m "docs(#9): update CLAUDE.md — workers-script module, key types, rules"
```
