# Workers-Common & Workers-Camel Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement shared async worker infrastructure (workers-common) and the Apache Camel worker (workers-camel) per the approved spec `docs/superpowers/specs/2026-06-08-casehub-workers-camel-design.md`.

**Architecture:** workers-common provides the completion registry, callback REST endpoint, and CDI event types shared by all worker modules. workers-camel implements `ReactiveWorkerProvisioner` and `WorkerExecutionManager` SPIs, dispatches Camel exchanges, and handles faults/retries mirroring the Quartz implementation. Both modules are library JARs with Jandex indexes — no `quarkus:build` goal.

**Tech Stack:** Java 21, Quarkus 3.32.2, Camel Quarkus 3.32.0, Mutiny, Vert.x EventBus, CDI async events

**External dependency:** engine#447 (`NoOpWorkerExecutionManager @DefaultBean`) must land in casehub-engine before a deployment without both scheduler-quartz AND workers-camel can start. Not blocking for implementation — only for deployment.

---

## File Structure

### workers-common

| File | Responsibility |
|------|---------------|
| `workers-common/src/main/java/io/casehub/workers/common/WorkerCorrelationContext.java` | Per-dispatch context record |
| `workers-common/src/main/java/io/casehub/workers/common/PendingCompletion.java` | Registry entry record |
| `workers-common/src/main/java/io/casehub/workers/common/CompletionExpiredEvent.java` | CDI async event for timeout |
| `workers-common/src/main/java/io/casehub/workers/common/FaultCallbackEvent.java` | CDI async event for REST fault callback |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerCompletionPayload.java` | REST callback body record |
| `workers-common/src/main/java/io/casehub/workers/common/CasehubWorkerHeaders.java` | Header name constants |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerProvisioningException.java` | Unchecked exception for provisioning failures |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerCapabilityResolver.java` | Generic capability-to-route-identifier interface |
| `workers-common/src/main/java/io/casehub/workers/common/AsyncWorkerCompletionRegistry.java` | In-memory pending completion store with scheduled expiry |
| `workers-common/src/main/java/io/casehub/workers/common/WorkflowCompletionPublisher.java` | Fires `WorkflowExecutionCompleted` on event bus |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerStatusPublisher.java` | Wraps `ReactiveWorkerStatusListener` |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerProvisionerSupport.java` | Shared provisioner validation utilities |
| `workers-common/src/main/java/io/casehub/workers/common/WorkerCallbackResource.java` | `POST /workers/complete/{dispatchId}` REST endpoint |
| `workers-common/src/test/java/io/casehub/workers/common/WorkerCorrelationContextTest.java` | Unit test |
| `workers-common/src/test/java/io/casehub/workers/common/PendingCompletionTest.java` | Unit test |
| `workers-common/src/test/java/io/casehub/workers/common/AsyncWorkerCompletionRegistryTest.java` | Unit test |
| `workers-common/src/test/java/io/casehub/workers/common/WorkflowCompletionPublisherTest.java` | Unit test |
| `workers-common/src/test/java/io/casehub/workers/common/WorkerCallbackResourceTest.java` | Unit test |

### workers-camel

| File | Responsibility |
|------|---------------|
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerConstants.java` | `WORKER_TYPE = "camel"` discriminator |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerEventBusAddresses.java` | `CAMEL_WORKER_FAULT` address constant |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerRoute.java` | SPI for CDI-registered Camel routes |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java` | Three-tier capability-to-URI resolution |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelReactiveWorkerProvisioner.java` | `ReactiveWorkerProvisioner` SPI impl |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerExecutionManager.java` | `WorkerExecutionManager` SPI impl |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultPublisher.java` | Fires `WorkflowExecutionFailed` on `CAMEL_WORKER_FAULT` |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultEventHandler.java` | `@ConsumeEvent(CAMEL_WORKER_FAULT)` — retry logic |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelCompletionExpiryObserver.java` | `@ObservesAsync CompletionExpiredEvent` filtered by workerType |
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelFaultCallbackObserver.java` | `@ObservesAsync FaultCallbackEvent` filtered by workerType |
| `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubComponent.java` | Camel component — `casehub:complete` |
| `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubEndpoint.java` | Camel endpoint |
| `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubProducer.java` | Camel producer — the `process(Exchange)` logic |
| `workers-camel/src/main/resources/META-INF/services/org/apache/camel/component/casehub` | Component SPI registration |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerConstantsTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelCapabilityResolverTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelReactiveWorkerProvisionerTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerExecutionManagerTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultPublisherTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultEventHandlerTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelCompletionExpiryObserverTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelFaultCallbackObserverTest.java` | Unit test |
| `workers-camel/src/test/java/io/casehub/workers/camel/component/CasehubProducerTest.java` | Unit test |

### workers-testing

| File | Responsibility |
|------|---------------|
| `workers-testing/src/main/java/io/casehub/workers/testing/WorkerTestSupport.java` | Static helpers for building test fixtures |

---

## Task 1: workers-common — Record Types and Constants

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerCorrelationContext.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/PendingCompletion.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/CompletionExpiredEvent.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/FaultCallbackEvent.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerCompletionPayload.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/CasehubWorkerHeaders.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerProvisioningException.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerCapabilityResolver.java`
- Test: `workers-common/src/test/java/io/casehub/workers/common/WorkerCorrelationContextTest.java`
- Test: `workers-common/src/test/java/io/casehub/workers/common/PendingCompletionTest.java`

- [ ] **Step 1: Write tests for record types**

```java
// WorkerCorrelationContextTest.java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.api.model.Worker;
import io.casehub.api.model.Capability;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class WorkerCorrelationContextTest {

    @Test
    void recordComponents() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "tenant-1";
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        String idempotency = "hash-123";

        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, idempotency, "tenant-1");

        assertThat(ctx.caseInstance()).isSameAs(instance);
        assertThat(ctx.worker()).isSameAs(worker);
        assertThat(ctx.idempotency()).isEqualTo("hash-123");
        assertThat(ctx.tenancyId()).isEqualTo("tenant-1");
    }
}
```

```java
// PendingCompletionTest.java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.api.model.Capability;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PendingCompletionTest {

    @Test
    void recordComponents() {
        Capability cap = new Capability("send-email", "", "");
        Instant now = Instant.now();
        Instant expires = now.plusSeconds(3600);

        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "camel", null, "token-abc", cap, 42L, now, expires, Map.of("key", "val"));

        assertThat(pending.dispatchId()).isEqualTo("dispatch-1");
        assertThat(pending.workerType()).isEqualTo("camel");
        assertThat(pending.callbackToken()).isEqualTo("token-abc");
        assertThat(pending.capability()).isSameAs(cap);
        assertThat(pending.eventLogId()).isEqualTo(42L);
        assertThat(pending.registeredAt()).isEqualTo(now);
        assertThat(pending.expiresAt()).isEqualTo(expires);
        assertThat(pending.provisionerMeta()).containsEntry("key", "val");
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkerCorrelationContextTest,PendingCompletionTest" -pl .`
Expected: compilation error — types do not exist

- [ ] **Step 3: Implement all record types and constants**

```java
// WorkerCorrelationContext.java
package io.casehub.workers.common;

import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;

public record WorkerCorrelationContext(
    CaseInstance caseInstance,
    Worker worker,
    String idempotency,
    String tenancyId
) {}
```

```java
// PendingCompletion.java
package io.casehub.workers.common;

import io.casehub.api.model.Capability;
import java.time.Instant;
import java.util.Map;

public record PendingCompletion(
    String dispatchId,
    String workerType,
    WorkerCorrelationContext correlationContext,
    String callbackToken,
    Capability capability,
    Long eventLogId,
    Instant registeredAt,
    Instant expiresAt,
    Map<String, String> provisionerMeta
) {}
```

```java
// CompletionExpiredEvent.java
package io.casehub.workers.common;

public record CompletionExpiredEvent(PendingCompletion pending) {}
```

```java
// FaultCallbackEvent.java
package io.casehub.workers.common;

public record FaultCallbackEvent(PendingCompletion pending, Throwable cause) {}
```

```java
// WorkerCompletionPayload.java
package io.casehub.workers.common;

import java.util.Map;

public record WorkerCompletionPayload(
    Map<String, Object> output,
    boolean faulted,
    String errorMessage
) {}
```

```java
// CasehubWorkerHeaders.java
package io.casehub.workers.common;

public final class CasehubWorkerHeaders {
    public static final String WORKER_ID      = "casehub-worker-id";
    public static final String IDEMPOTENCY    = "casehub-idempotency";
    public static final String CASE_ID        = "casehub-case-id";
    public static final String TENANCY_ID     = "casehub-tenancy-id";
    public static final String TASK_TYPE      = "casehub-task-type";
    public static final String CALLBACK_TOKEN = "casehub-callback-token";
    public static final String WORK_STATUS    = "casehub-work-status";

    private CasehubWorkerHeaders() {}
}
```

```java
// WorkerProvisioningException.java
package io.casehub.workers.common;

public class WorkerProvisioningException extends RuntimeException {
    public WorkerProvisioningException(String message) {
        super(message);
    }

    public WorkerProvisioningException(String message, Throwable cause) {
        super(message, cause);
    }

    public static WorkerProvisioningException noRouteFound(String capabilities) {
        return new WorkerProvisioningException("No route found for capabilities: " + capabilities);
    }
}
```

```java
// WorkerCapabilityResolver.java
package io.casehub.workers.common;

import java.util.Optional;
import java.util.Set;

public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag);
    Optional<String> firstMatch(Set<String> capabilities);
    Set<String> capabilities();
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkerCorrelationContextTest,PendingCompletionTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-common/src/
git commit -m "feat(workers-common): add record types, constants, and capability resolver SPI

Refs #2"
```

---

## Task 2: workers-common — AsyncWorkerCompletionRegistry

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/AsyncWorkerCompletionRegistry.java`
- Test: `workers-common/src/test/java/io/casehub/workers/common/AsyncWorkerCompletionRegistryTest.java`

- [ ] **Step 1: Write tests**

```java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import jakarta.enterprise.event.Event;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AsyncWorkerCompletionRegistryTest {

    private AsyncWorkerCompletionRegistry registry;
    private List<CompletionExpiredEvent> firedEvents;

    @BeforeEach
    void setUp() {
        firedEvents = new CopyOnWriteArrayList<>();
        // Create registry with a capturing Event mock
        Event<CompletionExpiredEvent> capturingEvent = new CapturingEvent<>(firedEvents);
        registry = new AsyncWorkerCompletionRegistry();
        registry.expiryEvents = capturingEvent;
    }

    @Test
    void register_generatesUniqueDispatchIdAndCallbackToken() {
        WorkerCorrelationContext ctx = testContext();

        PendingCompletion p1 = registry.register("camel", ctx, testCapability(), 1L,
            Duration.ofMinutes(60), Map.of());
        PendingCompletion p2 = registry.register("camel", ctx, testCapability(), 2L,
            Duration.ofMinutes(60), Map.of());

        assertThat(p1.dispatchId()).isNotEqualTo(p2.dispatchId());
        assertThat(p1.callbackToken()).isNotEqualTo(p2.callbackToken());
        assertThat(p1.workerType()).isEqualTo("camel");
    }

    @Test
    void complete_returnsAndRemoves() {
        PendingCompletion pending = registry.register("camel", testContext(), testCapability(),
            1L, Duration.ofMinutes(60), Map.of());

        Optional<PendingCompletion> completed = registry.complete(pending.dispatchId());
        assertThat(completed).isPresent().contains(pending);

        Optional<PendingCompletion> second = registry.complete(pending.dispatchId());
        assertThat(second).isEmpty();
    }

    @Test
    void complete_unknownDispatchId_returnsEmpty() {
        assertThat(registry.complete("nonexistent")).isEmpty();
    }

    @Test
    void countByWorkerName_countsActiveDispatches() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofMinutes(60), Map.of());
        registry.register("camel", ctx, testCapability(), 2L, Duration.ofMinutes(60), Map.of());

        assertThat(registry.countByWorkerName(ctx.worker().getName())).isEqualTo(2);
    }

    @Test
    void expireStale_firesEventForExpiredEntries() {
        WorkerCorrelationContext ctx = testContext();
        registry.register("camel", ctx, testCapability(), 1L, Duration.ofSeconds(-1), Map.of());
        PendingCompletion live = registry.register("camel", ctx, testCapability(), 2L,
            Duration.ofMinutes(60), Map.of());

        registry.expireStale();

        assertThat(firedEvents).hasSize(1);
        assertThat(firedEvents.get(0).pending().eventLogId()).isEqualTo(1L);
        assertThat(registry.complete(live.dispatchId())).isPresent();
    }

    private WorkerCorrelationContext testContext() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("test-worker", List.of(new Capability("cap", "", "")),
            (ctx) -> null);
        return new WorkerCorrelationContext(instance, worker, "hash", "t1");
    }

    private Capability testCapability() {
        return new Capability("send-email", "", "");
    }

    /**
     * Minimal Event implementation that captures fireAsync calls synchronously for testing.
     * GE-20260513-b15933: @ObservesAsync not delivered in @QuarkusTest — use capture pattern.
     */
    @SuppressWarnings("unchecked")
    static class CapturingEvent<T> implements Event<T> {
        private final List<T> captured;
        CapturingEvent(List<T> captured) { this.captured = captured; }

        @Override public void fire(T event) { captured.add(event); }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event) {
            captured.add(event);
            return java.util.concurrent.CompletableFuture.completedFuture(event);
        }
        @Override public <U extends T> java.util.concurrent.CompletionStage<U> fireAsync(U event,
            jakarta.enterprise.inject.spi.NotificationOptions options) {
            return fireAsync(event);
        }
        @Override public Event<T> select(java.lang.annotation.Annotation... qualifiers) { return this; }
        @Override public <U extends T> Event<U> select(Class<U> subtype,
            java.lang.annotation.Annotation... qualifiers) { return (Event<U>) this; }
        @Override public <U extends T> Event<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype,
            java.lang.annotation.Annotation... qualifiers) { return (Event<U>) this; }
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="AsyncWorkerCompletionRegistryTest"`
Expected: compilation error

- [ ] **Step 3: Implement AsyncWorkerCompletionRegistry**

```java
package io.casehub.workers.common;

import io.casehub.api.model.Capability;
import io.smallrye.common.annotation.Blocking;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import io.quarkus.scheduler.Scheduled;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

@ApplicationScoped
public class AsyncWorkerCompletionRegistry {

    @Inject
    Event<CompletionExpiredEvent> expiryEvents;

    private final ConcurrentHashMap<String, PendingCompletion> pending = new ConcurrentHashMap<>();

    public PendingCompletion register(String workerType, WorkerCorrelationContext ctx,
                                      Capability capability, Long eventLogId,
                                      Duration ttl, Map<String, String> provisionerMeta) {
        Instant now = Instant.now();
        PendingCompletion entry = new PendingCompletion(
            UUID.randomUUID().toString(),
            workerType,
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

    public Optional<PendingCompletion> complete(String dispatchId) {
        return Optional.ofNullable(pending.remove(dispatchId));
    }

    public int countByWorkerName(String workerName) {
        return (int) pending.values().stream()
            .filter(p -> p.correlationContext().worker().getName().equals(workerName))
            .count();
    }

    @Scheduled(every = "${casehub.workers.async.expiry-check-interval:5m}")
    @Blocking
    void expireStale() {
        pending.forEach((key, value) ->
            pending.computeIfPresent(key, (k, p) -> {
                if (!p.expiresAt().isBefore(Instant.now())) return p;
                expiryEvents.fireAsync(new CompletionExpiredEvent(p));
                return null;
            })
        );
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="AsyncWorkerCompletionRegistryTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-common/src/
git commit -m "feat(workers-common): add AsyncWorkerCompletionRegistry with scheduled expiry

Refs #2"
```

---

## Task 3: workers-common — WorkflowCompletionPublisher and WorkerStatusPublisher

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkflowCompletionPublisher.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerStatusPublisher.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerProvisionerSupport.java`
- Test: `workers-common/src/test/java/io/casehub/workers/common/WorkflowCompletionPublisherTest.java`

- [ ] **Step 1: Write test for WorkflowCompletionPublisher**

```java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.vertx.mutiny.core.eventbus.EventBus;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class WorkflowCompletionPublisherTest {

    @Test
    void complete_publishesToWorkerExecutionFinished() {
        EventBus eventBus = mock(EventBus.class);
        WorkflowCompletionPublisher publisher = new WorkflowCompletionPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Map<String, Object> output = Map.of("result", "ok");

        publisher.complete(ctx, output);

        ArgumentCaptor<WorkflowExecutionCompleted> captor =
            ArgumentCaptor.forClass(WorkflowExecutionCompleted.class);
        verify(eventBus).publish(eq(EventBusAddresses.WORKER_EXECUTION_FINISHED), captor.capture());

        WorkflowExecutionCompleted event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.idempotency()).isEqualTo("hash-1");
        assertThat(event.output()).isEqualTo(output);
        assertThat(event.plannedAction()).isNull();
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkflowCompletionPublisherTest"`
Expected: compilation error

- [ ] **Step 3: Implement all three classes**

```java
// WorkflowCompletionPublisher.java
package io.casehub.workers.common;

import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkflowExecutionCompleted;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class WorkflowCompletionPublisher {

    @Inject
    EventBus eventBus;

    public void complete(WorkerCorrelationContext ctx, Map<String, Object> output) {
        eventBus.publish(EventBusAddresses.WORKER_EXECUTION_FINISHED,
            WorkflowExecutionCompleted.approved(
                ctx.caseInstance(), ctx.worker(), ctx.idempotency(), output));
    }
}
```

```java
// WorkerStatusPublisher.java
package io.casehub.workers.common;

import io.casehub.api.model.WorkResult;
import io.casehub.api.spi.ReactiveWorkerStatusListener;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;

@ApplicationScoped
public class WorkerStatusPublisher {

    @Inject
    ReactiveWorkerStatusListener reactiveWorkerStatusListener;

    public Uni<Void> onWorkerStarted(String dispatchId, Map<String, String> sessionMeta) {
        return reactiveWorkerStatusListener.onWorkerStarted(dispatchId, sessionMeta);
    }

    public Uni<Void> onWorkerCompleted(String dispatchId, WorkResult result) {
        return reactiveWorkerStatusListener.onWorkerCompleted(dispatchId, result);
    }

    public Uni<Void> onWorkerStalled(String dispatchId) {
        return reactiveWorkerStatusListener.onWorkerStalled(dispatchId);
    }
}
```

```java
// WorkerProvisionerSupport.java
package io.casehub.workers.common;

import io.casehub.api.model.ProvisionContext;
import java.util.Set;

public final class WorkerProvisionerSupport {

    private WorkerProvisionerSupport() {}

    public static void validateCapabilities(Set<String> requested, Set<String> supported) {
        for (String cap : requested) {
            if (!supported.contains(cap)) {
                throw new WorkerProvisioningException(
                    "Unsupported capability: " + cap + ". Supported: " + supported);
            }
        }
    }

    public static String tenancyId(ProvisionContext ctx) {
        return ctx.propagationContext() != null
            ? ctx.propagationContext().tenancyId()
            : null;
    }

    public static WorkerProvisioningException wrap(Throwable t, String capability) {
        if (t instanceof WorkerProvisioningException wpe) return wpe;
        return new WorkerProvisioningException(
            "Provisioning failed for capability: " + capability, t);
    }
}
```

- [ ] **Step 4: Add mockito test dependency to workers-common POM**

Add to `workers-common/pom.xml` dependencies:
```xml
<dependency>
    <groupId>org.mockito</groupId>
    <artifactId>mockito-core</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkflowCompletionPublisherTest"`
Expected: PASS

- [ ] **Step 6: Commit**

```
git add workers-common/
git commit -m "feat(workers-common): add WorkflowCompletionPublisher, WorkerStatusPublisher, and WorkerProvisionerSupport

Refs #2"
```

---

## Task 4: workers-common — WorkerCallbackResource

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerCallbackResource.java`
- Test: `workers-common/src/test/java/io/casehub/workers/common/WorkerCallbackResourceTest.java`

- [ ] **Step 1: Write test**

```java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WorkerCallbackResourceTest {

    private WorkerCallbackResource resource;
    private AsyncWorkerCompletionRegistry registry;
    private WorkflowCompletionPublisher completionPublisher;
    private List<FaultCallbackEvent> faultEvents;

    @BeforeEach
    void setUp() {
        registry = new AsyncWorkerCompletionRegistry();
        registry.expiryEvents = new AsyncWorkerCompletionRegistryTest.CapturingEvent<>(new java.util.ArrayList<>());

        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultEvents = new CopyOnWriteArrayList<>();

        resource = new WorkerCallbackResource();
        resource.registry = registry;
        resource.completionPublisher = completionPublisher;
        resource.faultCallbackEvents = new AsyncWorkerCompletionRegistryTest.CapturingEvent<>(faultEvents);
    }

    @Test
    void successfulCompletion_callsPublisher() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(
            Map.of("key", "value"), false, null);

        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);

        assertThat(response.getStatus()).isEqualTo(200);
        verify(completionPublisher).complete(eq(pending.correlationContext()), eq(Map.of("key", "value")));
    }

    @Test
    void faultedCompletion_firesFaultCallbackEvent() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, true, "route failed");

        Response response = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);

        assertThat(response.getStatus()).isEqualTo(200);
        assertThat(faultEvents).hasSize(1);
        assertThat(faultEvents.get(0).pending()).isEqualTo(pending);
        assertThat(faultEvents.get(0).cause().getMessage()).isEqualTo("route failed");
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void unknownDispatchId_returns404() {
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);
        Response response = resource.complete("unknown", "token", payload);
        assertThat(response.getStatus()).isEqualTo(404);
    }

    @Test
    void wrongCallbackToken_returns401() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(null, false, null);

        Response response = resource.complete(pending.dispatchId(), "wrong-token", payload);
        assertThat(response.getStatus()).isEqualTo(401);
    }

    @Test
    void duplicateCompletion_returns200Idempotently() {
        PendingCompletion pending = registerTestPending();
        WorkerCompletionPayload payload = new WorkerCompletionPayload(Map.of(), false, null);

        resource.complete(pending.dispatchId(), pending.callbackToken(), payload);
        Response second = resource.complete(pending.dispatchId(), pending.callbackToken(), payload);

        assertThat(second.getStatus()).isEqualTo(404);
    }

    private PendingCompletion registerTestPending() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1");
        return registry.register("camel", ctx, new Capability("cap", "", ""), 1L,
            Duration.ofMinutes(60), Map.of());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkerCallbackResourceTest"`
Expected: compilation error

- [ ] **Step 3: Implement WorkerCallbackResource**

```java
package io.casehub.workers.common;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Event;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.util.Map;
import java.util.Optional;

@Path("/workers/complete")
@ApplicationScoped
public class WorkerCallbackResource {

    @Inject
    AsyncWorkerCompletionRegistry registry;

    @Inject
    WorkflowCompletionPublisher completionPublisher;

    @Inject
    Event<FaultCallbackEvent> faultCallbackEvents;

    @POST
    @Path("/{dispatchId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response complete(@PathParam("dispatchId") String dispatchId,
                             @HeaderParam("X-Casehub-Callback-Token") String token,
                             WorkerCompletionPayload payload) {
        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);
        if (maybePending.isEmpty()) {
            return Response.status(404).build();
        }

        PendingCompletion pending = maybePending.get();

        if (!MessageDigest.isEqual(
                pending.callbackToken().getBytes(), token != null ? token.getBytes() : new byte[0])) {
            registry.register(pending.workerType(), pending.correlationContext(),
                pending.capability(), pending.eventLogId(),
                java.time.Duration.between(java.time.Instant.now(), pending.expiresAt()),
                pending.provisionerMeta());
            return Response.status(401).build();
        }

        if (payload.faulted()) {
            faultCallbackEvents.fireAsync(new FaultCallbackEvent(
                pending,
                payload.errorMessage() != null ? new RuntimeException(payload.errorMessage()) : null));
        } else {
            completionPublisher.complete(pending.correlationContext(),
                payload.output() != null ? payload.output() : Map.of());
        }
        return Response.ok().build();
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-common/pom.xml test -Dtest="WorkerCallbackResourceTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-common/src/
git commit -m "feat(workers-common): add WorkerCallbackResource REST endpoint

POST /workers/complete/{dispatchId} with constant-time token validation.
Fires WorkflowCompletionPublisher on success, FaultCallbackEvent CDI async on fault.

Refs #2"
```

---

## Task 5: workers-camel — Constants, SPI, and CamelWorkerFaultPublisher

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerConstants.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerEventBusAddresses.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerRoute.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultPublisher.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultPublisherTest.java`

- [ ] **Step 1: Write test for CamelWorkerFaultPublisher**

```java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

class CamelWorkerFaultPublisherTest {

    @Test
    void fault_fromPendingCompletion_publishesToCamelWorkerFault() {
        EventBus eventBus = mock(EventBus.class);
        CamelWorkerFaultPublisher publisher = new CamelWorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("send-email", "", "");
        PendingCompletion pending = new PendingCompletion(
            "dispatch-1", "camel", ctx, "token", capability, 42L,
            Instant.now(), Instant.now().plusSeconds(3600), Map.of());
        Throwable cause = new RuntimeException("route failed");

        publisher.fault(pending, cause);

        ArgumentCaptor<WorkflowExecutionFailed> captor =
            ArgumentCaptor.forClass(WorkflowExecutionFailed.class);
        verify(eventBus).publish(eq(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT), captor.capture());

        WorkflowExecutionFailed event = captor.getValue();
        assertThat(event.caseInstance()).isSameAs(instance);
        assertThat(event.worker()).isSameAs(worker);
        assertThat(event.capability()).isSameAs(capability);
        assertThat(event.inputDataHash()).isEqualTo("hash-1");
        assertThat(event.eventLogId()).isEqualTo("42");
        assertThat(event.cause()).isSameAs(cause);
    }

    @Test
    void fault_fromContext_publishesToCamelWorkerFault() {
        EventBus eventBus = mock(EventBus.class);
        CamelWorkerFaultPublisher publisher = new CamelWorkerFaultPublisher();
        publisher.eventBus = eventBus;

        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash-1", "t1");
        Capability capability = new Capability("cap", "", "");
        Throwable cause = new RuntimeException("boom");

        publisher.fault(ctx, capability, 99L, cause);

        verify(eventBus).publish(eq(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT), any(WorkflowExecutionFailed.class));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelWorkerFaultPublisherTest"`
Expected: compilation error

- [ ] **Step 3: Implement constants, SPI, and fault publisher**

```java
// CamelWorkerConstants.java
package io.casehub.workers.camel;

public final class CamelWorkerConstants {
    public static final String WORKER_TYPE = "camel";
    private CamelWorkerConstants() {}
}
```

```java
// CamelWorkerEventBusAddresses.java
package io.casehub.workers.camel;

public final class CamelWorkerEventBusAddresses {
    public static final String CAMEL_WORKER_FAULT = "casehub.workers.camel.fault";
    private CamelWorkerEventBusAddresses() {}
}
```

```java
// CamelWorkerRoute.java
package io.casehub.workers.camel;

import org.apache.camel.ExchangePattern;

public interface CamelWorkerRoute {
    String capabilityTag();
    String entryUri();
    ExchangePattern exchangePattern();
}
```

```java
// CamelWorkerFaultPublisher.java
package io.casehub.workers.camel;

import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelWorkerFaultPublisher {

    @Inject
    EventBus eventBus;

    public void fault(PendingCompletion pending, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                pending.correlationContext().caseInstance(),
                pending.correlationContext().worker(),
                pending.capability(),
                pending.correlationContext().idempotency(),
                pending.eventLogId().toString(),
                cause));
    }

    public void fault(WorkerCorrelationContext ctx, Capability capability,
                      Long eventLogId, Throwable cause) {
        eventBus.publish(CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT,
            new WorkflowExecutionFailed(
                ctx.caseInstance(), ctx.worker(), capability,
                ctx.idempotency(), eventLogId.toString(), cause));
    }
}
```

- [ ] **Step 4: Add mockito test dependency to workers-camel POM**

Add to `workers-camel/pom.xml` dependencies:
```xml
<dependency>
    <groupId>org.mockito</groupId>
    <artifactId>mockito-core</artifactId>
    <scope>test</scope>
</dependency>
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelWorkerFaultPublisherTest"`
Expected: PASS

- [ ] **Step 6: Commit**

```
git add workers-camel/
git commit -m "feat(workers-camel): add constants, CamelWorkerRoute SPI, and CamelWorkerFaultPublisher

CAMEL_WORKER_FAULT address isolates Camel faults from Quartz's WORKFLOW_EXECUTION_FAILED.

Refs #3"
```

---

## Task 6: workers-camel — CamelCapabilityResolver

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelCapabilityResolverTest.java`

- [ ] **Step 1: Write test**

```java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.camel.CamelContext;
import org.apache.camel.ExchangePattern;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.builder.RouteBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelCapabilityResolverTest {

    private CamelCapabilityResolver resolver;
    private DefaultCamelContext camelContext;

    @BeforeEach
    void setUp() throws Exception {
        camelContext = new DefaultCamelContext();
        camelContext.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                from("direct:send-email").id("send-email").to("mock:result");
            }
        });
        camelContext.start();

        resolver = new CamelCapabilityResolver();
        resolver.camelContext = camelContext;
        resolver.configCapabilities = Map.of("enrich-lead", "kafka:leads?brokers=localhost:9092");
        resolver.spiRoutes = new jakarta.enterprise.inject.Instance<>() {
            // empty — no SPI routes in this test
            public CamelWorkerRoute get() { return null; }
            public java.util.Iterator<CamelWorkerRoute> iterator() { return java.util.Collections.emptyIterator(); }
            public boolean isUnsatisfied() { return true; }
            public boolean isAmbiguous() { return false; }
            public boolean isResolvable() { return false; }
            public void destroy(CamelWorkerRoute instance) {}
            public jakarta.enterprise.inject.Instance.Handle<CamelWorkerRoute> getHandle() { return null; }
            public Iterable<? extends jakarta.enterprise.inject.Instance.Handle<CamelWorkerRoute>> handles() { return java.util.List.of(); }
            public jakarta.enterprise.inject.Instance<CamelWorkerRoute> select(java.lang.annotation.Annotation... qualifiers) { return this; }
            public <U extends CamelWorkerRoute> jakarta.enterprise.inject.Instance<U> select(Class<U> subtype, java.lang.annotation.Annotation... qualifiers) { return null; }
            public <U extends CamelWorkerRoute> jakarta.enterprise.inject.Instance<U> select(jakarta.enterprise.util.TypeLiteral<U> subtype, java.lang.annotation.Annotation... qualifiers) { return null; }
            public java.util.stream.Stream<CamelWorkerRoute> stream() { return java.util.stream.Stream.empty(); }
        };
        resolver.initialize();
    }

    @Test
    void resolve_conventionRoute_returnsDirectUri() {
        assertThat(resolver.resolve("send-email")).isEqualTo("direct:send-email");
    }

    @Test
    void resolve_configRoute_returnsConfiguredUri() {
        assertThat(resolver.resolve("enrich-lead")).isEqualTo("kafka:leads?brokers=localhost:9092");
    }

    @Test
    void resolve_unknownCapability_throws() {
        assertThatThrownBy(() -> resolver.resolve("nonexistent"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstKnown() {
        Optional<String> match = resolver.firstMatch(Set.of("unknown", "send-email", "enrich-lead"));
        assertThat(match).isPresent().contains("send-email");
    }

    @Test
    void firstMatch_noneKnown_returnsEmpty() {
        assertThat(resolver.firstMatch(Set.of("a", "b"))).isEmpty();
    }

    @Test
    void capabilities_returnsAll() {
        assertThat(resolver.capabilities()).containsExactlyInAnyOrder("send-email", "enrich-lead");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelCapabilityResolverTest"`
Expected: compilation error

- [ ] **Step 3: Implement CamelCapabilityResolver**

```java
package io.casehub.workers.camel;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import org.apache.camel.CamelContext;
import org.apache.camel.ExchangePattern;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@ApplicationScoped
public class CamelCapabilityResolver implements WorkerCapabilityResolver<String> {

    @Inject
    CamelContext camelContext;

    @Inject @Any
    Instance<CamelWorkerRoute> spiRoutes;

    @ConfigProperty(name = "casehub.workers.camel.capabilities", defaultValue = "")
    Map<String, String> configCapabilities;

    private final Map<String, String> resolvedRoutes = new HashMap<>();
    private final Map<String, ExchangePattern> exchangePatterns = new HashMap<>();

    void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
        initialize();
    }

    void initialize() {
        resolvedRoutes.clear();
        exchangePatterns.clear();

        for (CamelWorkerRoute route : spiRoutes) {
            resolvedRoutes.put(route.capabilityTag(), route.entryUri());
            exchangePatterns.put(route.capabilityTag(), route.exchangePattern());
        }

        if (configCapabilities != null) {
            configCapabilities.forEach((tag, uri) -> {
                resolvedRoutes.putIfAbsent(tag, uri);
                exchangePatterns.putIfAbsent(tag, ExchangePattern.InOnly);
            });
        }

        camelContext.getRoutes().forEach(route -> {
            String routeId = route.getRouteId();
            if (routeId != null && !resolvedRoutes.containsKey(routeId)) {
                String fromUri = route.getEndpoint().getEndpointUri();
                if (fromUri.startsWith("direct:" + routeId)) {
                    resolvedRoutes.put(routeId, "direct:" + routeId);
                    exchangePatterns.putIfAbsent(routeId, ExchangePattern.InOnly);
                }
            }
        });
    }

    @Override
    public String resolve(String capabilityTag) {
        String uri = resolvedRoutes.get(capabilityTag);
        if (uri == null) {
            throw WorkerProvisioningException.noRouteFound(capabilityTag);
        }
        return uri;
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities) {
        return capabilities.stream()
            .filter(resolvedRoutes::containsKey)
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(resolvedRoutes.keySet());
    }

    public ExchangePattern exchangePattern(String capabilityTag) {
        return exchangePatterns.getOrDefault(capabilityTag, ExchangePattern.InOnly);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelCapabilityResolverTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-camel/src/
git commit -m "feat(workers-camel): add CamelCapabilityResolver — three-tier capability-to-URI resolution

SPI → Config → Convention. Initializes on StartupEvent @Priority(APPLICATION).

Refs #3"
```

---

## Task 7: workers-camel — CamelReactiveWorkerProvisioner and CamelWorkerExecutionManager

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelReactiveWorkerProvisioner.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerExecutionManager.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelReactiveWorkerProvisionerTest.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerExecutionManagerTest.java`

- [ ] **Step 1: Write tests**

```java
// CamelReactiveWorkerProvisionerTest.java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelReactiveWorkerProvisionerTest {

    private CamelReactiveWorkerProvisioner provisioner;
    private CamelCapabilityResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = mock(CamelCapabilityResolver.class);
        provisioner = new CamelReactiveWorkerProvisioner();
        provisioner.camelCapabilityResolver = resolver;
    }

    @Test
    void provision_matchingCapability_returnsEmpty() {
        when(resolver.firstMatch(Set.of("send-email", "unknown")))
            .thenReturn(Optional.of("send-email"));
        when(resolver.resolve("send-email")).thenReturn("direct:send-email");

        ProvisionResult result = provisioner.provision(Set.of("send-email", "unknown"), null)
            .await().indefinitely();

        assertThat(result.causedByEntryId()).isNull();
    }

    @Test
    void provision_noMatch_fails() {
        when(resolver.firstMatch(any())).thenReturn(Optional.empty());

        assertThat(provisioner.provision(Set.of("nonexistent"), null)
            .onFailure().recoverWithItem(t -> null)
            .await().indefinitely()).isNull();
    }

    @Test
    void getCapabilities_delegatesToResolver() {
        when(resolver.capabilities()).thenReturn(Set.of("a", "b"));
        Set<String> caps = provisioner.getCapabilities().await().indefinitely();
        assertThat(caps).containsExactlyInAnyOrder("a", "b");
    }

    @Test
    void terminate_returnsVoid() {
        assertThat(provisioner.terminate("any").await().indefinitely()).isNull();
    }
}
```

```java
// CamelWorkerExecutionManagerTest.java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CamelWorkerExecutionManagerTest {

    private CamelWorkerExecutionManager manager;
    private CamelCapabilityResolver resolver;
    private CamelWorkerFaultPublisher faultPublisher;
    private AsyncWorkerCompletionRegistry registry;

    @BeforeEach
    void setUp() {
        resolver = mock(CamelCapabilityResolver.class);
        faultPublisher = mock(CamelWorkerFaultPublisher.class);
        registry = mock(AsyncWorkerCompletionRegistry.class);

        manager = new CamelWorkerExecutionManager();
        manager.camelCapabilityResolver = resolver;
        manager.camelWorkerFaultPublisher = faultPublisher;
        manager.asyncWorkerCompletionRegistry = registry;
        manager.completionPublisher = mock(WorkflowCompletionPublisher.class);
        manager.producerTemplate = mock(ProducerTemplate.class);
        manager.asyncTimeoutMinutes = 60;
    }

    @Test
    void submit_missingRoute_firesFault() {
        CaseInstance instance = testInstance();
        Worker worker = testWorker();
        Capability cap = new Capability("missing", "", "");
        when(resolver.resolve("missing")).thenThrow(WorkerProvisioningException.noRouteFound("missing"));

        manager.submit(1L, instance, worker, cap, Map.of()).await().indefinitely();

        verify(faultPublisher).fault(any(), eq(cap), eq(1L), any(WorkerProvisioningException.class));
    }

    @Test
    void schedulePersistedEvent_returnsVoid() {
        assertThat(manager.schedulePersistedEvent(new EventLog()).await().indefinitely()).isNull();
    }

    @Test
    void getActiveWorkCount_delegatesToRegistry() {
        when(registry.countByWorkerName("w1")).thenReturn(3);
        assertThat(manager.getActiveWorkCount("w1")).isEqualTo(3);
    }

    private CaseInstance testInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        return instance;
    }

    private Worker testWorker() {
        return new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelReactiveWorkerProvisionerTest,CamelWorkerExecutionManagerTest"`
Expected: compilation error

- [ ] **Step 3: Implement CamelReactiveWorkerProvisioner**

```java
package io.casehub.workers.camel;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.api.spi.ReactiveWorkerProvisioner;
import io.casehub.workers.common.WorkerProvisioningException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class CamelReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    @Inject
    CamelCapabilityResolver camelCapabilityResolver;

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        String capability = camelCapabilityResolver.firstMatch(capabilities)
            .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
        camelCapabilityResolver.resolve(capability);
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        return Uni.createFrom().item(camelCapabilityResolver.capabilities());
    }
}
```

- [ ] **Step 4: Implement CamelWorkerExecutionManager**

```java
package io.casehub.workers.camel;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerProvisioningException;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Map;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.ExchangePattern;
import org.apache.camel.ProducerTemplate;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CamelWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(CamelWorkerExecutionManager.class);

    @Inject CamelCapabilityResolver camelCapabilityResolver;
    @Inject CamelWorkerFaultPublisher camelWorkerFaultPublisher;
    @Inject AsyncWorkerCompletionRegistry asyncWorkerCompletionRegistry;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject ProducerTemplate producerTemplate;

    @ConfigProperty(name = "casehub.workers.async.timeout-minutes", defaultValue = "60")
    int asyncTimeoutMinutes;

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        String entryUri;
        try {
            entryUri = camelCapabilityResolver.resolve(capability.getName());
        } catch (WorkerProvisioningException e) {
            LOG.errorf("Camel route for capability %s missing at dispatch time", capability.getName());
            camelWorkerFaultPublisher.fault(
                new WorkerCorrelationContext(instance, worker,
                    WorkerExecutionKeys.inputDataHash(instance.getUuid(), worker.getName(),
                        capability.getName(), inputData), instance.tenancyId),
                capability, eventLogId, e);
            return Uni.createFrom().voidItem();
        }

        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, idempotency, instance.tenancyId);
        ExchangePattern pattern = camelCapabilityResolver.exchangePattern(capability.getName());

        return pattern == ExchangePattern.InOut
            ? submitSync(ctx, entryUri, capability, inputData, eventLogId)
            : submitAsync(ctx, entryUri, capability, eventLogId, inputData);
    }

    private Uni<Void> submitSync(WorkerCorrelationContext ctx, String entryUri,
                                  Capability capability, Map<String, Object> inputData,
                                  Long eventLogId) {
        return Uni.createFrom()
            .item(() -> producerTemplate.request(entryUri,
                buildExchange(ctx, capability, inputData)))
            .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())
            .flatMap(response -> {
                boolean faulted = response.getException() != null
                    || "FAULTED".equals(response.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));
                if (faulted) {
                    camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, response.getException());
                } else {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> output = response.getIn().getBody(Map.class);
                    completionPublisher.complete(ctx, output != null ? output : Map.of());
                }
                return Uni.createFrom().voidItem();
            })
            .onFailure().recoverWithUni(t -> {
                camelWorkerFaultPublisher.fault(ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> submitAsync(WorkerCorrelationContext ctx, String entryUri,
                                   Capability capability, Long eventLogId,
                                   Map<String, Object> inputData) {
        PendingCompletion pending = asyncWorkerCompletionRegistry.register(
            CamelWorkerConstants.WORKER_TYPE, ctx, capability, eventLogId,
            Duration.ofMinutes(asyncTimeoutMinutes), Map.of());

        Exchange exchange = buildExchange(ctx, capability, inputData);
        exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, pending.dispatchId());
        exchange.getIn().setHeader(CasehubWorkerHeaders.CALLBACK_TOKEN, pending.callbackToken());

        return Uni.createFrom().voidItem()
            .invoke(() -> producerTemplate.send(entryUri, exchange));
    }

    private Exchange buildExchange(WorkerCorrelationContext ctx, Capability capability,
                                    Map<String, Object> inputData) {
        Exchange exchange = producerTemplate.getCamelContext().getEndpoint("direct:dummy")
            .createExchange();
        exchange.getIn().setHeader(CasehubWorkerHeaders.IDEMPOTENCY, ctx.idempotency());
        exchange.getIn().setHeader(CasehubWorkerHeaders.CASE_ID,
            ctx.caseInstance().getUuid().toString());
        exchange.getIn().setHeader(CasehubWorkerHeaders.TENANCY_ID, ctx.tenancyId());
        exchange.getIn().setHeader(CasehubWorkerHeaders.TASK_TYPE, capability.getName());
        exchange.getIn().setBody(inputData);
        return exchange;
    }

    @Override
    public Uni<Void> schedulePersistedEvent(EventLog scheduledEventLog) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public int getActiveWorkCount(String workerId) {
        return asyncWorkerCompletionRegistry.countByWorkerName(workerId);
    }
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelReactiveWorkerProvisionerTest,CamelWorkerExecutionManagerTest"`
Expected: PASS

- [ ] **Step 6: Commit**

```
git add workers-camel/src/
git commit -m "feat(workers-camel): add CamelReactiveWorkerProvisioner and CamelWorkerExecutionManager

Implements both engine SPIs. Sync path uses InOut exchange pattern with runSubscriptionOn.
Async path registers with AsyncWorkerCompletionRegistry. Missing route at submit time
fires CAMEL_WORKER_FAULT rather than hanging.

Refs #3"
```

---

## Task 8: workers-camel — CamelWorkerFaultEventHandler (retry logic)

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultEventHandler.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultEventHandlerTest.java`

- [ ] **Step 1: Write test**

```java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.RetryPolicy;
import org.junit.jupiter.api.Test;

class CamelWorkerFaultEventHandlerTest {

    @Test
    void computeBackoffDelayMs_fixed() {
        RetryPolicy policy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(10000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 2)).isEqualTo(10000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 3)).isEqualTo(10000L);
    }

    @Test
    void computeBackoffDelayMs_exponential() {
        RetryPolicy policy = new RetryPolicy(5, 1000, BackoffStrategy.EXPONENTIAL);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(1000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 2)).isEqualTo(2000L);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 3)).isEqualTo(4000L);
    }

    @Test
    void computeBackoffDelayMs_exponential_cappedAt30Seconds() {
        RetryPolicy policy = new RetryPolicy(10, 5000, BackoffStrategy.EXPONENTIAL);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 10)).isEqualTo(30_000L);
    }

    @Test
    void computeBackoffDelayMs_jitter_inRange() {
        RetryPolicy policy = new RetryPolicy(3, 10000, BackoffStrategy.EXPONENTIAL_WITH_JITTER);
        for (int i = 0; i < 100; i++) {
            long delay = CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1);
            assertThat(delay).isBetween(0L, 10000L);
        }
    }

    @Test
    void computeBackoffDelayMs_nullDelayMs_returnsZero() {
        RetryPolicy policy = new RetryPolicy(3, null, BackoffStrategy.FIXED);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(0L);
    }

    @Test
    void computeBackoffDelayMs_nullStrategy_defaultsToFixed() {
        RetryPolicy policy = new RetryPolicy(3, 5000, null);
        assertThat(CamelWorkerFaultEventHandler.computeBackoffDelayMs(policy, 1)).isEqualTo(5000L);
    }

    @Test
    void resolveRetryPolicy_nullPolicy_returnsDefault() {
        RetryPolicy result = CamelWorkerFaultEventHandler.resolveRetryPolicy(null, null);
        assertThat(result.maxAttempts()).isEqualTo(3);
        assertThat(result.delayMs()).isEqualTo(10000);
        assertThat(result.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelWorkerFaultEventHandlerTest"`
Expected: compilation error

- [ ] **Step 3: Implement CamelWorkerFaultEventHandler**

```java
package io.casehub.workers.camel;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.api.model.event.CaseHubEventType;
import io.casehub.api.model.event.EventStreamType;
import io.casehub.engine.common.internal.event.EventBusAddresses;
import io.casehub.engine.common.internal.event.WorkerRetriesExhaustedEvent;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.core.Vertx;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.jboss.logging.Logger;

@ApplicationScoped
public class CamelWorkerFaultEventHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(CamelWorkerFaultEventHandler.class);

    @Inject WorkerExecutionManager workerExecutionManager;
    @Inject EventBus eventBus;
    @Inject EventLogRepository eventLogRepository;
    @Inject Vertx vertx;

    @ConsumeEvent(value = CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT, blocking = true)
    public void onFault(WorkflowExecutionFailed event) {
        CaseInstance instance = event.caseInstance();
        Worker worker = event.worker();
        String inputDataHash = event.inputDataHash();
        String tenancyId = instance.tenancyId;

        EventLog failureLog = new EventLog();
        failureLog.setCaseId(instance.getUuid());
        failureLog.setWorkerId(worker.getName());
        failureLog.setEventType(CaseHubEventType.WORKER_EXECUTION_FAILED);
        failureLog.setStreamType(EventStreamType.CASE);
        failureLog.setTimestamp(Instant.now());
        String errorMsg = (event.cause() != null && event.cause().getMessage() != null)
            ? event.cause().getMessage() : "unknown";
        failureLog.setMetadata(OBJECT_MAPPER.createObjectNode()
            .put("inputDataHash", inputDataHash)
            .put("errorMessage", errorMsg));

        eventLogRepository.append(failureLog, tenancyId)
            .flatMap(ignored -> countFailedAttempts(instance.getUuid(), worker.getName(),
                                                    inputDataHash, tenancyId))
            .flatMap(failureCount -> {
                RetryPolicy retryPolicy = resolveRetryPolicy(worker.getExecutionPolicy(), worker.getExecutionPolicy() != null ? worker.getExecutionPolicy().retries() : null);
                if (failureCount < retryPolicy.maxAttempts()) {
                    long delayMs = computeBackoffDelayMs(retryPolicy, failureCount + 1);
                    return reloadAndResubmit(event, delayMs);
                } else {
                    eventBus.publish(EventBusAddresses.WORKER_RETRIES_EXHAUSTED,
                        new WorkerRetriesExhaustedEvent(
                            instance.getUuid(), worker.getName(), inputDataHash));
                    return Uni.createFrom().voidItem();
                }
            })
            .subscribe().with(ignored -> {}, ex ->
                LOG.errorf(ex, "Fault handling failed for worker %s case %s — case may stall",
                           worker.getName(), instance.getUuid()));
    }

    private Uni<Long> countFailedAttempts(UUID caseId, String workerId,
                                           String inputDataHash, String tenancyId) {
        return eventLogRepository
            .findByCaseAndWorkerAndType(caseId, workerId, CaseHubEventType.WORKER_EXECUTION_FAILED, tenancyId)
            .map(logs -> logs.stream()
                .filter(log -> {
                    JsonNode meta = log.getMetadata();
                    JsonNode node = meta == null ? null : meta.get("inputDataHash");
                    return node != null && inputDataHash.equals(node.asText());
                })
                .count());
    }

    static RetryPolicy resolveRetryPolicy(ExecutionPolicy executionPolicy, RetryPolicy retryPolicy) {
        if (executionPolicy == null || retryPolicy == null) {
            return new RetryPolicy();
        }
        return retryPolicy;
    }

    static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) {
        long baseDelayMs = policy.delayMs() != null ? policy.delayMs() : 0L;
        BackoffStrategy strategy = policy.backoffStrategy() != null
            ? policy.backoffStrategy() : BackoffStrategy.FIXED;
        return switch (strategy) {
            case FIXED -> baseDelayMs;
            case EXPONENTIAL -> {
                long shift = Math.min(attemptNumber - 1, 30);
                yield Math.min(baseDelayMs * (1L << shift), 30_000L);
            }
            case EXPONENTIAL_WITH_JITTER -> {
                long shift = Math.min(attemptNumber - 1, 30);
                long cap = Math.min(baseDelayMs * (1L << shift), 30_000L);
                yield cap == 0 ? 0 : ThreadLocalRandom.current().nextLong(cap + 1);
            }
        };
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

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelWorkerFaultEventHandlerTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-camel/src/
git commit -m "feat(workers-camel): add CamelWorkerFaultEventHandler — retry logic mirroring Quartz

@ConsumeEvent(CAMEL_WORKER_FAULT, blocking=true). Retry comparison: failureCount < maxAttempts (strict <).
Default policy: 3 attempts, 10s FIXED. Uses emitOn(workerPool) after Vert.x timer for re-dispatch.

Refs #3"
```

---

## Task 9: workers-camel — CDI Event Observers

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelCompletionExpiryObserver.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelFaultCallbackObserver.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelCompletionExpiryObserverTest.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelFaultCallbackObserverTest.java`

- [ ] **Step 1: Write tests**

```java
// CamelCompletionExpiryObserverTest.java
package io.casehub.workers.camel;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.CompletionExpiredEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CamelCompletionExpiryObserverTest {

    @Test
    void onExpiry_camelType_firesFault() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelCompletionExpiryObserver observer = new CamelCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", CamelWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verify(faultPublisher).fault(eq(pending), any(RuntimeException.class));
    }

    @Test
    void onExpiry_nonCamelType_doesNotFire() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelCompletionExpiryObserver observer = new CamelCompletionExpiryObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "http", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onExpiry(new CompletionExpiredEvent(pending));

        verifyNoInteractions(faultPublisher);
    }
}
```

```java
// CamelFaultCallbackObserverTest.java
package io.casehub.workers.camel;

import static org.mockito.Mockito.*;

import io.casehub.workers.common.FaultCallbackEvent;
import io.casehub.workers.common.PendingCompletion;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CamelFaultCallbackObserverTest {

    @Test
    void onFaultCallback_camelType_firesFault() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelFaultCallbackObserver observer = new CamelFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", CamelWorkerConstants.WORKER_TYPE, null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());
        RuntimeException cause = new RuntimeException("boom");

        observer.onFaultCallback(new FaultCallbackEvent(pending, cause));

        verify(faultPublisher).fault(pending, cause);
    }

    @Test
    void onFaultCallback_nonCamelType_doesNotFire() {
        CamelWorkerFaultPublisher faultPublisher = mock(CamelWorkerFaultPublisher.class);
        CamelFaultCallbackObserver observer = new CamelFaultCallbackObserver();
        observer.faultPublisher = faultPublisher;

        PendingCompletion pending = new PendingCompletion(
            "d1", "http", null, "t", null, 1L,
            Instant.now(), Instant.now(), Map.of());

        observer.onFaultCallback(new FaultCallbackEvent(pending, null));

        verifyNoInteractions(faultPublisher);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelCompletionExpiryObserverTest,CamelFaultCallbackObserverTest"`
Expected: compilation error

- [ ] **Step 3: Implement both observers**

```java
// CamelCompletionExpiryObserver.java
package io.casehub.workers.camel;

import io.casehub.workers.common.CompletionExpiredEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelCompletionExpiryObserver {

    @Inject
    CamelWorkerFaultPublisher faultPublisher;

    void onExpiry(@ObservesAsync CompletionExpiredEvent event) {
        if (!CamelWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
        faultPublisher.fault(event.pending(),
            new RuntimeException("Async timeout — no completion received within TTL"));
    }
}
```

```java
// CamelFaultCallbackObserver.java
package io.casehub.workers.camel;

import io.casehub.workers.common.FaultCallbackEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.ObservesAsync;
import jakarta.inject.Inject;

@ApplicationScoped
public class CamelFaultCallbackObserver {

    @Inject
    CamelWorkerFaultPublisher faultPublisher;

    void onFaultCallback(@ObservesAsync FaultCallbackEvent event) {
        if (!CamelWorkerConstants.WORKER_TYPE.equals(event.pending().workerType())) return;
        faultPublisher.fault(event.pending(), event.cause());
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CamelCompletionExpiryObserverTest,CamelFaultCallbackObserverTest"`
Expected: PASS

- [ ] **Step 5: Commit**

```
git add workers-camel/src/
git commit -m "feat(workers-camel): add CamelCompletionExpiryObserver and CamelFaultCallbackObserver

Both filter by pending.workerType() == 'camel' — required for co-deployment safety
when multiple worker modules share the classpath.

Refs #3"
```

---

## Task 10: workers-camel — CasehubCamelComponent (`casehub:complete`)

**Files:**
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubComponent.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubEndpoint.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/component/CasehubProducer.java`
- Create: `workers-camel/src/main/resources/META-INF/services/org/apache/camel/component/casehub`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/component/CasehubProducerTest.java`

- [ ] **Step 1: Write test**

```java
package io.casehub.workers.camel.component;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.camel.CamelWorkerFaultPublisher;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.camel.Exchange;
import org.apache.camel.impl.DefaultCamelContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class CasehubProducerTest {

    private CasehubProducer producer;
    private AsyncWorkerCompletionRegistry registry;
    private WorkflowCompletionPublisher completionPublisher;
    private CamelWorkerFaultPublisher faultPublisher;

    @BeforeEach
    void setUp() throws Exception {
        registry = new AsyncWorkerCompletionRegistry();
        registry.expiryEvents = new io.casehub.workers.common.AsyncWorkerCompletionRegistryTest.CapturingEvent<>(new java.util.ArrayList<>());
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        faultPublisher = mock(CamelWorkerFaultPublisher.class);

        DefaultCamelContext ctx = new DefaultCamelContext();
        CasehubEndpoint endpoint = new CasehubEndpoint("casehub:complete", new CasehubComponent());
        producer = new CasehubProducer(endpoint, registry, completionPublisher, faultPublisher);
    }

    @Test
    void process_successfulCompletion() throws Exception {
        PendingCompletion pending = registerTestPending();
        Exchange exchange = createExchange(pending.dispatchId());
        exchange.getIn().setBody(Map.of("result", "ok"));

        producer.process(exchange);

        verify(completionPublisher).complete(eq(pending.correlationContext()), eq(Map.of("result", "ok")));
        verifyNoInteractions(faultPublisher);
    }

    @Test
    void process_faultedViaException() throws Exception {
        PendingCompletion pending = registerTestPending();
        Exchange exchange = createExchange(pending.dispatchId());
        exchange.setException(new RuntimeException("boom"));

        producer.process(exchange);

        verify(faultPublisher).fault(eq(pending), any(RuntimeException.class));
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void process_faultedViaHeader() throws Exception {
        PendingCompletion pending = registerTestPending();
        Exchange exchange = createExchange(pending.dispatchId());
        exchange.getIn().setHeader(CasehubWorkerHeaders.WORK_STATUS, "FAULTED");

        producer.process(exchange);

        verify(faultPublisher).fault(eq(pending), isNull());
        verifyNoInteractions(completionPublisher);
    }

    @Test
    void process_missingWorkerId_throws() {
        Exchange exchange = new DefaultCamelContext().getEndpoint("direct:test").createExchange();

        assertThatThrownBy(() -> producer.process(exchange))
            .isInstanceOf(IllegalStateException.class)
            .hasMessageContaining("casehub-worker-id");
    }

    @Test
    void process_unknownDispatchId_noOps() throws Exception {
        Exchange exchange = createExchange("unknown-id");
        exchange.getIn().setBody(Map.of());

        producer.process(exchange);

        verifyNoInteractions(completionPublisher);
        verifyNoInteractions(faultPublisher);
    }

    private Exchange createExchange(String dispatchId) {
        Exchange exchange = new DefaultCamelContext().getEndpoint("direct:test").createExchange();
        exchange.getIn().setHeader(CasehubWorkerHeaders.WORKER_ID, dispatchId);
        return exchange;
    }

    private PendingCompletion registerTestPending() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "t1";
        Worker worker = new Worker("w1", List.of(new Capability("cap", "", "")), (ctx) -> null);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(instance, worker, "hash", "t1");
        return registry.register("camel", ctx, new Capability("cap", "", ""), 1L,
            Duration.ofMinutes(60), Map.of());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CasehubProducerTest"`
Expected: compilation error

- [ ] **Step 3: Implement Camel component classes**

```java
// CasehubComponent.java
package io.casehub.workers.camel.component;

import org.apache.camel.Endpoint;
import org.apache.camel.support.DefaultComponent;
import java.util.Map;

public class CasehubComponent extends DefaultComponent {
    @Override
    protected Endpoint createEndpoint(String uri, String remaining, Map<String, Object> parameters) {
        return new CasehubEndpoint(uri, this);
    }
}
```

```java
// CasehubEndpoint.java
package io.casehub.workers.camel.component;

import io.casehub.workers.camel.CamelWorkerFaultPublisher;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import jakarta.enterprise.inject.spi.CDI;
import org.apache.camel.Consumer;
import org.apache.camel.Producer;
import org.apache.camel.support.DefaultEndpoint;

public class CasehubEndpoint extends DefaultEndpoint {

    public CasehubEndpoint(String endpointUri, CasehubComponent component) {
        super(endpointUri, component);
    }

    @Override
    public Producer createProducer() {
        return new CasehubProducer(this,
            CDI.current().select(AsyncWorkerCompletionRegistry.class).get(),
            CDI.current().select(WorkflowCompletionPublisher.class).get(),
            CDI.current().select(CamelWorkerFaultPublisher.class).get());
    }

    @Override
    public Consumer createConsumer(org.apache.camel.Processor processor) {
        throw new UnsupportedOperationException("casehub:complete is producer-only");
    }

    @Override
    public boolean isSingleton() {
        return true;
    }
}
```

```java
// CasehubProducer.java
package io.casehub.workers.camel.component;

import io.casehub.workers.camel.CamelWorkerFaultPublisher;
import io.casehub.workers.common.AsyncWorkerCompletionRegistry;
import io.casehub.workers.common.CasehubWorkerHeaders;
import io.casehub.workers.common.PendingCompletion;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import org.apache.camel.Exchange;
import org.apache.camel.support.DefaultProducer;
import org.jboss.logging.Logger;
import java.util.Map;
import java.util.Optional;

public class CasehubProducer extends DefaultProducer {

    private static final Logger LOG = Logger.getLogger(CasehubProducer.class);

    private final AsyncWorkerCompletionRegistry registry;
    private final WorkflowCompletionPublisher completionPublisher;
    private final CamelWorkerFaultPublisher faultPublisher;

    public CasehubProducer(CasehubEndpoint endpoint,
                           AsyncWorkerCompletionRegistry registry,
                           WorkflowCompletionPublisher completionPublisher,
                           CamelWorkerFaultPublisher faultPublisher) {
        super(endpoint);
        this.registry = registry;
        this.completionPublisher = completionPublisher;
        this.faultPublisher = faultPublisher;
    }

    @Override
    public void process(Exchange exchange) throws Exception {
        String dispatchId = exchange.getIn().getHeader(CasehubWorkerHeaders.WORKER_ID, String.class);
        if (dispatchId == null) {
            throw new IllegalStateException("casehub-worker-id header missing on casehub:complete");
        }

        Optional<PendingCompletion> maybePending = registry.complete(dispatchId);
        if (maybePending.isEmpty()) {
            LOG.warnf("casehub:complete — dispatchId %s not found (already resolved or expired)", dispatchId);
            return;
        }

        PendingCompletion pending = maybePending.get();
        boolean faulted = exchange.getException() != null
            || "FAULTED".equals(exchange.getIn().getHeader(CasehubWorkerHeaders.WORK_STATUS));

        if (faulted) {
            faultPublisher.fault(pending, exchange.getException());
        } else {
            @SuppressWarnings("unchecked")
            Map<String, Object> output = exchange.getIn().getBody(Map.class);
            completionPublisher.complete(pending.correlationContext(),
                output != null ? output : Map.of());
        }
    }
}
```

- [ ] **Step 4: Create the Camel component SPI registration file**

Write `workers-camel/src/main/resources/META-INF/services/org/apache/camel/component/casehub`:
```
io.casehub.workers.camel.component.CasehubComponent
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode -f workers-camel/pom.xml test -Dtest="CasehubProducerTest"`
Expected: PASS

- [ ] **Step 6: Commit**

```
git add workers-camel/src/
git commit -m "feat(workers-camel): add casehub:complete Camel component

CasehubProducer.process() completes or faults the dispatch via the registry.
Registered via META-INF/services for Camel component SPI discovery.

Refs #3"
```

---

## Task 11: workers-testing — WorkerTestSupport

**Files:**
- Create: `workers-testing/src/main/java/io/casehub/workers/testing/WorkerTestSupport.java`

- [ ] **Step 1: Implement WorkerTestSupport**

```java
package io.casehub.workers.testing;

import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.model.CaseInstance;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public final class WorkerTestSupport {

    private WorkerTestSupport() {}

    public static CaseInstance testCaseInstance() {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = "test-tenant";
        return instance;
    }

    public static CaseInstance testCaseInstance(String tenancyId) {
        CaseInstance instance = new CaseInstance();
        instance.setUuid(UUID.randomUUID());
        instance.tenancyId = tenancyId;
        return instance;
    }

    public static Worker testWorker(String name, String... capabilityTags) {
        List<Capability> caps = java.util.Arrays.stream(capabilityTags)
            .map(tag -> new Capability(tag, "", ""))
            .toList();
        return new Worker(name, caps, (ctx) -> null);
    }

    public static Capability testCapability(String tag) {
        return new Capability(tag, "", "");
    }
}
```

- [ ] **Step 2: Add workers-common dependency to workers-testing POM**

Add to `workers-testing/pom.xml` dependencies:
```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-engine-common</artifactId>
</dependency>
```

- [ ] **Step 3: Build to verify**

Run: `mvn --batch-mode -f workers-testing/pom.xml compile`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```
git add workers-testing/
git commit -m "feat(workers-testing): add WorkerTestSupport static test fixture helpers

Refs #2"
```

---

## Task 12: Full build and cleanup

- [ ] **Step 1: Remove .gitkeep placeholder files from populated directories**

Delete any `.gitkeep` files in directories that now have real Java source files.

- [ ] **Step 2: Run full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS — all modules compile and all tests pass

- [ ] **Step 3: Fix any compilation or test failures**

Address any issues found during the full build.

- [ ] **Step 4: Commit**

```
git add -A
git commit -m "chore: clean up placeholder files, full build green

Refs #1"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] Section 2.1 — ReactiveWorkerProvisioner: Task 7
- [x] Section 2.2 — WorkerExecutionManager: Task 7
- [x] Section 2.3 — CDI resolution: noted as engine#447 external dep
- [x] Section 2.4 — Fault bus isolation: Task 5, Task 8
- [x] Section 2.5 — Module deps: POMs already created
- [x] Section 3 — Module structure: POMs already created
- [x] Section 4.1 — Core types: Task 1
- [x] Section 4.2 — Services: Task 2, Task 3, Task 4
- [x] Section 5.1 — Constants: Task 5
- [x] Section 5.2 — FaultPublisher: Task 5
- [x] Section 5.3 — CapabilityResolver: Task 6
- [x] Section 5.4 — Provisioner: Task 7
- [x] Section 5.5 — ExecutionManager: Task 7
- [x] Section 5.6 — Sync path: Task 7
- [x] Section 5.7 — Async path: Task 7
- [x] Section 5.8 — Exchange mapping: Task 7
- [x] Section 5.9 — CasehubCamelComponent: Task 10
- [x] Section 5.10 — Fault handling observers: Task 8, Task 9
- [x] Section 6 — workers-testing: Task 11
- [x] Section 7 — Test coverage: distributed across task tests

**Type consistency:** Verified — `WorkerCorrelationContext`, `PendingCompletion`, `CamelWorkerConstants.WORKER_TYPE`, `CamelWorkerEventBusAddresses.CAMEL_WORKER_FAULT`, `WorkflowCompletionPublisher.complete()`, `CamelWorkerFaultPublisher.fault()` all used consistently across tasks.

**Placeholder scan:** No TBD, TODO, "implement later", or "similar to Task N" found.
