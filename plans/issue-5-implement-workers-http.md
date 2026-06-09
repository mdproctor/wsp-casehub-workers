# Workers-HTTP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the HTTP worker module that dispatches case steps to external services via HTTP POST, reusing workers-common infrastructure.

**Architecture:** Vert.x WebClient (reactive-native) dispatches HTTP requests. Three-tier endpoint resolution (SPI bean > config > EndpointRegistry). Shared retry infrastructure extracted to WorkerRetrySupport in workers-common; Camel handler refactored to use it. PermanentFaultException and RetryAfterException provide HTTP-specific fault classification.

**Tech Stack:** Quarkus 3.32, Vert.x Mutiny WebClient, CDI, Mutiny reactive, Jackson

**Spec:** `docs/superpowers/specs/2026-06-09-casehub-workers-http-design.md` (rev 3)

---

## File Map

### workers-common (modified)

| File | Action | Responsibility |
|---|---|---|
| `workers-common/src/main/java/io/casehub/workers/common/WorkerRetrySupport.java` | Create | Shared retry building blocks — persist failure, count attempts, resolve policy, compute backoff, publish exhaustion |
| `workers-common/src/test/java/io/casehub/workers/common/WorkerRetrySupportTest.java` | Create | Unit tests for all shared retry operations |

### workers-camel (modified)

| File | Action | Responsibility |
|---|---|---|
| `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultEventHandler.java` | Modify | Replace private retry methods with WorkerRetrySupport injection |
| `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultEventHandlerTest.java` | Verify | Existing tests must pass unchanged after refactoring |

### workers-http (new + POM update)

| File | Action | Responsibility |
|---|---|---|
| `workers-http/pom.xml` | Modify | Add/remove dependencies per spec Section 3.3 |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerConstants.java` | Create | `WORKER_TYPE = "http"` |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerEventBusAddresses.java` | Create | `HTTP_WORKER_FAULT` address |
| `workers-http/src/main/java/io/casehub/workers/http/ExchangeMode.java` | Create | Enum: SYNC, ASYNC |
| `workers-http/src/main/java/io/casehub/workers/http/ResolvedEndpoint.java` | Create | Resolution result record |
| `workers-http/src/main/java/io/casehub/workers/http/PermanentFaultException.java` | Create | Marks 4xx faults as non-retryable |
| `workers-http/src/main/java/io/casehub/workers/http/RetryAfterException.java` | Create | Carries retryAfterMs from 429 |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerRoute.java` | Create | SPI interface for Tier 1 endpoint registration |
| `workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java` | Create | 3-tier tag → ResolvedEndpoint resolution |
| `workers-http/src/main/java/io/casehub/workers/http/HttpReactiveWorkerProvisioner.java` | Create | Capability probe via resolver |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultPublisher.java` | Create | Publishes WorkflowExecutionFailed to HTTP_WORKER_FAULT |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java` | Create | Sync/async dispatch via WebClient |
| `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultEventHandler.java` | Create | Retry logic with PermanentFault + RetryAfter |
| `workers-http/src/main/java/io/casehub/workers/http/HttpCompletionExpiryObserver.java` | Create | Routes async timeout → fault pipeline |
| `workers-http/src/main/java/io/casehub/workers/http/HttpFaultCallbackObserver.java` | Create | Routes REST callback fault → fault pipeline |
| `workers-http/src/test/java/io/casehub/workers/http/HttpEndpointResolverTest.java` | Create | 3-tier resolution, priority, defaults |
| `workers-http/src/test/java/io/casehub/workers/http/HttpReactiveWorkerProvisionerTest.java` | Create | Provision/capabilities/terminate |
| `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerExecutionManagerTest.java` | Create | Sync 2xx/4xx/429/5xx, async, URI templates, headers |
| `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultEventHandlerTest.java` | Create | PermanentFault, RetryAfter, retry logic |
| `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultPublisherTest.java` | Create | Event bus publishing |
| `workers-http/src/test/java/io/casehub/workers/http/HttpCompletionExpiryObserverTest.java` | Create | workerType filter |
| `workers-http/src/test/java/io/casehub/workers/http/HttpFaultCallbackObserverTest.java` | Create | workerType filter |

---

## Task 1: POM updates + simple types

**Files:**
- Modify: `workers-http/pom.xml`
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerConstants.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerEventBusAddresses.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/ExchangeMode.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/ResolvedEndpoint.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/PermanentFaultException.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/RetryAfterException.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerRoute.java`

- [ ] **Step 1: Update POM**

Replace `workers-http/pom.xml` contents. Add `casehub-workers-common`, `casehub-engine-common`, `quarkus-vertx`, `smallrye-mutiny-vertx-web-client`. Remove `quarkus-rest-client-jackson`. Add `mockito-core` test scope. Add Jandex plugin. Reference the Camel module's POM structure as the template.

- [ ] **Step 2: Create constants and enums**

`HttpWorkerConstants.java`:
```java
package io.casehub.workers.http;

public final class HttpWorkerConstants {
    public static final String WORKER_TYPE = "http";
    private HttpWorkerConstants() {}
}
```

`HttpWorkerEventBusAddresses.java`:
```java
package io.casehub.workers.http;

public final class HttpWorkerEventBusAddresses {
    public static final String HTTP_WORKER_FAULT = "casehub.workers.http.fault";
    private HttpWorkerEventBusAddresses() {}
}
```

`ExchangeMode.java`:
```java
package io.casehub.workers.http;

public enum ExchangeMode { SYNC, ASYNC }
```

- [ ] **Step 3: Create records and exceptions**

`ResolvedEndpoint.java`:
```java
package io.casehub.workers.http;

import java.util.Map;

public record ResolvedEndpoint(
    String url, String method, ExchangeMode mode,
    Map<String, String> headers, int timeoutSeconds
) {}
```

`PermanentFaultException.java`:
```java
package io.casehub.workers.http;

public class PermanentFaultException extends RuntimeException {
    private final int statusCode;
    public PermanentFaultException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }
    public int statusCode() { return statusCode; }
}
```

`RetryAfterException.java`:
```java
package io.casehub.workers.http;

public class RetryAfterException extends RuntimeException {
    private final long retryAfterMs;
    public RetryAfterException(long retryAfterMs, String message) {
        super(message);
        this.retryAfterMs = retryAfterMs;
    }
    public long retryAfterMs() { return retryAfterMs; }
}
```

- [ ] **Step 4: Create HttpWorkerRoute SPI interface**

```java
package io.casehub.workers.http;

import java.util.Map;

public interface HttpWorkerRoute {
    String capabilityTag();
    String url();
    default String method() { return "POST"; }
    default ExchangeMode exchangeMode() { return ExchangeMode.SYNC; }
    default Map<String, String> headers() { return Map.of(); }
    default int timeoutSeconds() { return -1; }
}
```

- [ ] **Step 5: Build to verify compilation**

Run: `mvn --batch-mode compile -pl workers-http -am`
Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```
feat(workers-http): POM updates and foundation types

Add dependencies (workers-common, engine-common, vertx, webclient).
Remove quarkus-rest-client-jackson. Create HttpWorkerConstants,
HttpWorkerEventBusAddresses, ExchangeMode, ResolvedEndpoint,
PermanentFaultException, RetryAfterException, HttpWorkerRoute SPI.

Refs: #5
```

---

## Task 2: WorkerRetrySupport in workers-common

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerRetrySupport.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerRetrySupportTest.java`

- [ ] **Step 1: Write WorkerRetrySupport tests**

Test `resolveRetryPolicy()` (static) and `computeBackoffDelayMs()` (static) first — they have no CDI dependencies:

```java
package io.casehub.workers.common;

import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.workers.testing.WorkerTestSupport;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class WorkerRetrySupportTest {

    @Test
    void resolveRetryPolicy_nullExecutionPolicy_returnsDefault() {
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        worker.setExecutionPolicy(null);
        RetryPolicy policy = WorkerRetrySupport.resolveRetryPolicy(worker);
        assertThat(policy.maxAttempts()).isEqualTo(3);
        assertThat(policy.delayMs()).isEqualTo(10000);
        assertThat(policy.backoffStrategy()).isEqualTo(BackoffStrategy.FIXED);
    }

    @Test
    void resolveRetryPolicy_nullRetries_returnsDefault() {
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        worker.setExecutionPolicy(new ExecutionPolicy(null, null));
        RetryPolicy policy = WorkerRetrySupport.resolveRetryPolicy(worker);
        assertThat(policy.maxAttempts()).isEqualTo(3);
    }

    @Test
    void resolveRetryPolicy_explicitPolicy_returnsIt() {
        Worker worker = WorkerTestSupport.testWorker("w", "cap");
        RetryPolicy explicit = new RetryPolicy(5, 2000, BackoffStrategy.EXPONENTIAL);
        worker.setExecutionPolicy(new ExecutionPolicy(null, explicit));
        RetryPolicy policy = WorkerRetrySupport.resolveRetryPolicy(worker);
        assertThat(policy.maxAttempts()).isEqualTo(5);
        assertThat(policy.delayMs()).isEqualTo(2000);
    }

    @Test
    void computeBackoff_fixed_returnsBaseDelay() {
        assertThat(WorkerRetrySupport.computeBackoffDelayMs(
            new RetryPolicy(3, 5000, BackoffStrategy.FIXED), 1)).isEqualTo(5000);
        assertThat(WorkerRetrySupport.computeBackoffDelayMs(
            new RetryPolicy(3, 5000, BackoffStrategy.FIXED), 3)).isEqualTo(5000);
    }

    @Test
    void computeBackoff_exponential_capsAt30s() {
        long delay = WorkerRetrySupport.computeBackoffDelayMs(
            new RetryPolicy(10, 10000, BackoffStrategy.EXPONENTIAL), 5);
        assertThat(delay).isEqualTo(30_000L); // 10000 * 16 = 160000, capped to 30000
    }

    @Test
    void computeBackoff_exponentialWithJitter_inRange() {
        long delay = WorkerRetrySupport.computeBackoffDelayMs(
            new RetryPolicy(3, 5000, BackoffStrategy.EXPONENTIAL_WITH_JITTER), 1);
        assertThat(delay).isBetween(0L, 5000L);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerRetrySupportTest`
Expected: FAIL — `WorkerRetrySupport` class does not exist

- [ ] **Step 3: Implement WorkerRetrySupport**

Extract from `CamelWorkerFaultEventHandler`. The class has:
- Static `resolveRetryPolicy(Worker)` — unwraps ExecutionPolicy, falls back to default
- Static `computeBackoffDelayMs(RetryPolicy, long)` — FIXED/EXPONENTIAL/EXPONENTIAL_WITH_JITTER with 30s cap
- Instance `persistFailureLog(...)` — builds EventLog, calls `eventLogRepository.append()`
- Instance `countFailedAttempts(...)` — queries EventLogRepository, filters by inputDataHash metadata
- Instance `publishRetriesExhausted(...)` — publishes `WorkerRetriesExhaustedEvent` on event bus

Reference: `CamelWorkerFaultEventHandler.java` lines 44-105 for the logic to extract.

```java
package io.casehub.workers.common;

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
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.smallrye.mutiny.Uni;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

@ApplicationScoped
public class WorkerRetrySupport {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Inject EventLogRepository eventLogRepository;
    @Inject EventBus eventBus;

    public Uni<Void> persistFailureLog(CaseInstance instance, Worker worker,
                                        String inputDataHash, String errorMsg,
                                        String tenancyId) {
        EventLog failureLog = new EventLog();
        failureLog.setCaseId(instance.getUuid());
        failureLog.setWorkerId(worker.getName());
        failureLog.setEventType(CaseHubEventType.WORKER_EXECUTION_FAILED);
        failureLog.setStreamType(EventStreamType.CASE);
        failureLog.setTimestamp(Instant.now());
        failureLog.setMetadata(OBJECT_MAPPER.createObjectNode()
            .put("inputDataHash", inputDataHash)
            .put("errorMessage", errorMsg));
        return eventLogRepository.append(failureLog, tenancyId);
    }

    public Uni<Long> countFailedAttempts(UUID caseId, String workerId,
                                          String inputDataHash, String tenancyId) {
        return eventLogRepository
            .findByCaseAndWorkerAndType(caseId, workerId,
                CaseHubEventType.WORKER_EXECUTION_FAILED, tenancyId)
            .map(logs -> logs.stream()
                .filter(log -> {
                    JsonNode meta = log.getMetadata();
                    JsonNode node = meta == null ? null : meta.get("inputDataHash");
                    return node != null && inputDataHash.equals(node.asText());
                })
                .count());
    }

    public void publishRetriesExhausted(UUID caseId, String workerId,
                                         String inputDataHash) {
        eventBus.publish(EventBusAddresses.WORKER_RETRIES_EXHAUSTED,
            new WorkerRetriesExhaustedEvent(caseId, workerId, inputDataHash));
    }

    public static RetryPolicy resolveRetryPolicy(Worker worker) {
        ExecutionPolicy policy = worker.getExecutionPolicy();
        if (policy == null || policy.retries() == null) {
            return new RetryPolicy();
        }
        return policy.retries();
    }

    public static long computeBackoffDelayMs(RetryPolicy policy, long attemptNumber) {
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
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerRetrySupportTest`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```
feat(workers-common): extract WorkerRetrySupport — shared retry infrastructure

Extracts retry building blocks from CamelWorkerFaultEventHandler into
workers-common so both Camel and HTTP fault handlers can share them:
persistFailureLog, countFailedAttempts, publishRetriesExhausted,
resolveRetryPolicy, computeBackoffDelayMs.

Refs: #5
```

---

## Task 3: Refactor CamelWorkerFaultEventHandler to use WorkerRetrySupport

**Files:**
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerFaultEventHandler.java`
- Verify: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerFaultEventHandlerTest.java`

- [ ] **Step 1: Run existing Camel tests to establish baseline**

Run: `mvn --batch-mode test -pl workers-camel -Dtest=CamelWorkerFaultEventHandlerTest`
Expected: PASS (9 tests)

- [ ] **Step 2: Refactor CamelWorkerFaultEventHandler**

Inject `WorkerRetrySupport`. Replace:
- Private `resolveRetryPolicy()` → `WorkerRetrySupport.resolveRetryPolicy(worker)`
- Private `computeBackoffDelayMs()` → `WorkerRetrySupport.computeBackoffDelayMs(policy, attempt)`
- Private `countFailedAttempts()` → `retrySupport.countFailedAttempts(...)`
- Inline EventLog construction + `eventLogRepository.append()` → `retrySupport.persistFailureLog(...)`
- Inline `eventBus.publish(WORKER_RETRIES_EXHAUSTED, ...)` → `retrySupport.publishRetriesExhausted(...)`

Keep `reloadAndResubmit()` — it owns the Camel-specific `emitOn(workerPool)` orchestration.

Remove direct `@Inject EventLogRepository` and `@Inject EventBus` — these are now accessed through `WorkerRetrySupport`. Keep `@Inject Vertx` (for timer) and `@Inject WorkerExecutionManager` (for re-submit).

The `onFault()` method body becomes:
```java
return retrySupport.persistFailureLog(instance, worker, inputDataHash, errorMsg, tenancyId)
    .flatMap(ignored -> retrySupport.countFailedAttempts(
        instance.getUuid(), worker.getName(), inputDataHash, tenancyId))
    .flatMap(failureCount -> {
        RetryPolicy retryPolicy = WorkerRetrySupport.resolveRetryPolicy(worker);
        if (failureCount < retryPolicy.maxAttempts()) {
            long delayMs = WorkerRetrySupport.computeBackoffDelayMs(retryPolicy, failureCount + 1);
            return reloadAndResubmit(event, delayMs);
        } else {
            retrySupport.publishRetriesExhausted(
                instance.getUuid(), worker.getName(), inputDataHash);
            return Uni.createFrom().voidItem();
        }
    })
    .onFailure().recoverWithUni(ex -> { ... });
```

Note: `reloadAndResubmit()` still needs `eventLogRepository` for `findById()` — keep a direct injection for that, or expose `findById` on WorkerRetrySupport. Since `findById` is not a retry concern, keep the direct injection of `EventLogRepository` for this one use. Alternatively, use `retrySupport.eventLogRepository` if the field is package-private. The cleanest option: keep `@Inject EventLogRepository` on the Camel handler for `reloadAndResubmit()`. WorkerRetrySupport handles the retry-specific operations; the handler keeps the reload dependency.

- [ ] **Step 3: Run Camel tests to verify no regression**

Run: `mvn --batch-mode test -pl workers-camel -Dtest=CamelWorkerFaultEventHandlerTest`
Expected: PASS (9 tests — same count, same behaviour)

- [ ] **Step 4: Run full build to verify no compilation issues**

Run: `mvn --batch-mode test -pl workers-common,workers-camel -am`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```
refactor(workers-camel): use WorkerRetrySupport for shared retry logic

CamelWorkerFaultEventHandler now delegates persistFailureLog,
countFailedAttempts, publishRetriesExhausted, resolveRetryPolicy,
and computeBackoffDelayMs to WorkerRetrySupport in workers-common.
reloadAndResubmit stays — owns Camel-specific emitOn(workerPool).

No behaviour change. All 9 existing tests pass unchanged.

Refs: #5
```

---

## Task 4: HttpEndpointResolver

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpEndpointResolverTest.java`

- [ ] **Step 1: Write resolver tests**

```java
package io.casehub.workers.http;

import io.casehub.workers.common.WorkerProvisioningException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import java.util.Map;
import java.util.Set;
import static org.assertj.core.api.Assertions.*;

class HttpEndpointResolverTest {

    HttpEndpointResolver resolver;

    // Tier 1 SPI bean
    HttpWorkerRoute spiRoute = new HttpWorkerRoute() {
        @Override public String capabilityTag() { return "send-email"; }
        @Override public String url() { return "https://mail.example.com/send"; }
        @Override public ExchangeMode exchangeMode() { return ExchangeMode.ASYNC; }
        @Override public String method() { return "PUT"; }
        @Override public Map<String, String> headers() { return Map.of("X-Api", "key"); }
        @Override public int timeoutSeconds() { return 45; }
    };

    @BeforeEach
    void setUp() {
        // Create resolver with SPI routes, config endpoints, and global timeout
        // Details depend on constructor/initialisation approach — see implementation
    }

    @Test
    void tier1_winsOverTier2_forSameTag() { /* SPI bean wins for "send-email" */ }

    @Test
    void tier2_resolvesFromConfig() { /* config-only tag resolves */ }

    @Test
    void allTiers_merged() { /* Tier 1 + Tier 2 tags both present in capabilities() */ }

    @Test
    void resolve_unknownTag_throwsException() {
        assertThatThrownBy(() -> resolver.resolve("unknown"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void firstMatch_returnsFirstMatchingTag() { /* from a Set of tags */ }

    @Test
    void capabilities_returnsAllResolvedTags() { /* union of all tiers */ }

    @Test
    void configDefaults_methodPostModeSync() { /* Tier 2 without explicit method/mode */ }

    @Test
    void spiTimeout_negativeOne_inheritsGlobalDefault() { /* -1 → global default */ }
}
```

The test creates the resolver directly (not via CDI) with injected SPI routes and config maps. Tier 3 (EndpointRegistry) is tested with an `Instance.isUnsatisfied()` check — when absent, it's a no-op.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpEndpointResolverTest`
Expected: FAIL — class does not exist

- [ ] **Step 3: Implement HttpEndpointResolver**

Implements `WorkerCapabilityResolver<ResolvedEndpoint>`. Initialises eagerly at startup via `@Observes @Priority(APPLICATION) StartupEvent`. Three-tier resolution with `putIfAbsent` semantics. Reference `CamelCapabilityResolver.java` for the startup pattern. Config properties via `@ConfigProperty(name = "casehub.workers.http.endpoints", defaultValue = "")` with Quarkus nested map binding. Global timeout via `@ConfigProperty(name = "casehub.workers.http.default-timeout-seconds", defaultValue = "30")`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpEndpointResolverTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(workers-http): HttpEndpointResolver — 3-tier endpoint resolution

Implements WorkerCapabilityResolver<ResolvedEndpoint>. Three tiers:
SPI beans (Tier 1), config properties (Tier 2), EndpointRegistry
(Tier 3, no-op until platform#73 ships). putIfAbsent priority.
Initialises eagerly at startup.

Refs: #5
```

---

## Task 5: HttpReactiveWorkerProvisioner

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpReactiveWorkerProvisioner.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpReactiveWorkerProvisionerTest.java`

- [ ] **Step 1: Write provisioner tests**

Test: `getCapabilities()` returns resolved tags, `provision()` with resolvable cap returns `ProvisionResult.empty()`, `provision()` with unresolvable cap throws, `terminate()` returns void.

Reference `CamelReactiveWorkerProvisionerTest.java` for test structure.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpReactiveWorkerProvisionerTest`
Expected: FAIL

- [ ] **Step 3: Implement HttpReactiveWorkerProvisioner**

`@ApplicationScoped`. Injects `HttpEndpointResolver`. `provision()` calls `firstMatch()` then `resolve()`. Returns `ProvisionResult.empty()`. Mirror `CamelReactiveWorkerProvisioner.java`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpReactiveWorkerProvisionerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(workers-http): HttpReactiveWorkerProvisioner — capability probe

Validates endpoint is registered (not reachable) at provision time.
Returns ProvisionResult.empty(). Delegates to HttpEndpointResolver.

Refs: #5
```

---

## Task 6: HttpWorkerFaultPublisher

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultPublisher.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultPublisherTest.java`

- [ ] **Step 1: Write fault publisher tests**

Test both overloads: `fault(PendingCompletion, Throwable)` and `fault(WorkerCorrelationContext, Capability, Long, Throwable)`. Verify `eventBus.publish()` is called with `HTTP_WORKER_FAULT` address and correct `WorkflowExecutionFailed` payload.

Reference `CamelWorkerFaultPublisherTest.java`.

- [ ] **Step 2: Run tests, verify fail, implement, verify pass**

Mirror `CamelWorkerFaultPublisher.java` exactly but publish to `HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT`.

- [ ] **Step 3: Commit**

```
feat(workers-http): HttpWorkerFaultPublisher — publishes to HTTP_WORKER_FAULT

Same structure as CamelWorkerFaultPublisher but uses the HTTP-specific
fault event bus address to prevent cross-worker fault handling.

Refs: #5
```

---

## Task 7: HttpWorkerExecutionManager — sync dispatch

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerExecutionManagerTest.java`

This is the largest task. Build incrementally: sync first, async added in Task 8.

- [ ] **Step 1: Write sync dispatch tests**

Tests for: 2xx success with response body, empty response body, non-JSON response, 4xx → PermanentFaultException, 429 with Retry-After, 429 without Retry-After, 5xx → fault, connection timeout, headers set correctly, endpoint headers override CaseHub headers. URI template tests in Task 8.

Use Mockito to mock `WebClient`, `HttpRequest`, `HttpResponse`. Mock `WorkflowCompletionPublisher` and `HttpWorkerFaultPublisher` to capture calls. Mock `HttpEndpointResolver` to return test `ResolvedEndpoint` values.

Key test patterns:
```java
@Test
void sync_2xx_completesWithResponseBody() {
    // Mock WebClient to return 200 with {"result": "ok"}
    // Verify completionPublisher.complete(ctx, Map.of("result", "ok"))
}

@Test
void sync_4xx_throwsPermanentFault() {
    // Mock WebClient to return 400
    // Verify faultPublisher.fault() called with PermanentFaultException
}

@Test
void sync_429_withRetryAfter_throwsRetryAfterException() {
    // Mock WebClient to return 429 with Retry-After: 30
    // Verify faultPublisher.fault() called with RetryAfterException(30000)
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerExecutionManagerTest`
Expected: FAIL

- [ ] **Step 3: Implement sync dispatch path**

`@ApplicationScoped implements WorkerExecutionManager`. Injects `HttpEndpointResolver`, `WorkflowCompletionPublisher`, `HttpWorkerFaultPublisher`, `AsyncWorkerCompletionRegistry`, `WebClient`.

`submit()` method: resolve endpoint, branch on `mode`. Sync path: `webClient.requestAbs(method, url).timeout(...).putHeaders(...).sendJson(inputData)` → reactive chain handling 2xx/429/4xx/5xx/failure.

`schedulePersistedEvent()` returns `Uni.createFrom().voidItem()`.
`getActiveWorkCount()` delegates to registry.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerExecutionManagerTest`
Expected: PASS (sync tests only)

- [ ] **Step 5: Commit**

```
feat(workers-http): HttpWorkerExecutionManager — sync dispatch

Reactive-native via Vert.x WebClient. No runSubscriptionOn needed.
Handles 2xx (complete), 429 (RetryAfterException), 4xx (PermanentFault),
5xx (transient fault), connection failures.

Refs: #5
```

---

## Task 8: Async dispatch + URI template interpolation

**Files:**
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Modify: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerExecutionManagerTest.java`

- [ ] **Step 1: Write async dispatch tests**

Tests for: async 2xx fire-and-forget with PendingCompletion registered, async non-2xx immediate fault, async headers include worker-id and callback-token, sync headers do NOT include worker-id/callback-token.

- [ ] **Step 2: Write URI template tests**

Tests for: `{orderId}` interpolated from inputData, `{missing}` throws PermanentFaultException, URL without templates passed through unchanged.

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerExecutionManagerTest`
Expected: new tests FAIL

- [ ] **Step 4: Implement async dispatch + URI interpolation**

Async path in `submit()`: register `PendingCompletion`, add dispatch ID and callback token headers, `webClient.requestAbs(...).sendJson(inputData)` fire-and-forget. On non-2xx response: fault immediately.

URI template interpolation: regex `\{(\w+)\}` to find placeholders in URL. For each placeholder, look up key in `inputData`. If missing → throw `PermanentFaultException`. Values URL-encoded via `URLEncoder.encode(value.toString(), UTF_8)`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerExecutionManagerTest`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```
feat(workers-http): async dispatch and URI template interpolation

Async: register PendingCompletion, fire-and-forget, completion via
callback. URI templates: {fieldName} interpolated from inputData
at dispatch time; missing key throws PermanentFaultException.

Refs: #5
```

---

## Task 9: HttpWorkerFaultEventHandler

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerFaultEventHandler.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerFaultEventHandlerTest.java`

- [ ] **Step 1: Write fault handler tests**

Tests for: PermanentFaultException → immediate exhaustion, RetryAfterException → timer uses retryAfterMs, normal fault → count + backoff, exhaustion after maxAttempts, default policy (3 attempts), no emitOn (verify submit called without workerPool hop).

Reference `CamelWorkerFaultEventHandlerTest.java` for patterns. Key difference: no `emitOn` check, PermanentFault/RetryAfter checks.

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerFaultEventHandlerTest`
Expected: FAIL

- [ ] **Step 3: Implement HttpWorkerFaultEventHandler**

`@ApplicationScoped`. `@ConsumeEvent(value = HTTP_WORKER_FAULT, blocking = true)`. Injects `WorkerRetrySupport`, `WorkerExecutionManager`, `Vertx`.

`onFault(WorkflowExecutionFailed event)`:
1. `retrySupport.persistFailureLog(...)` — always
2. Check `cause instanceof PermanentFaultException` → `retrySupport.publishRetriesExhausted(...)`, return
3. `retrySupport.countFailedAttempts(...)`
4. `resolveRetryPolicy(worker)`, check `failureCount < maxAttempts`
5. If retry: compute delay (RetryAfterException → `ra.retryAfterMs()`, else `computeBackoffDelayMs()`). Vert.x timer + direct `submit()` (no `emitOn`)
6. If exhausted: `retrySupport.publishRetriesExhausted(...)`

`reloadAndResubmit()`: same as Camel but WITHOUT `emitOn(workerPool)`. Timer fires on event loop → `submit()` on event loop → WebClient is non-blocking.

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerFaultEventHandlerTest`
Expected: PASS

- [ ] **Step 5: Commit**

```
feat(workers-http): HttpWorkerFaultEventHandler — retry with PermanentFault + RetryAfter

Uses WorkerRetrySupport for shared operations. PermanentFaultException
skips retry. RetryAfterException overrides backoff delay. No emitOn —
WebClient is event-loop native. Same retry semantics as Camel otherwise.

Refs: #5
```

---

## Task 10: CDI event observers

**Files:**
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpCompletionExpiryObserver.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpFaultCallbackObserver.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpCompletionExpiryObserverTest.java`
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpFaultCallbackObserverTest.java`

- [ ] **Step 1: Write observer tests**

For each observer: test that workerType="http" fires the fault publisher, workerType="camel" returns without firing. Reference `CamelCompletionExpiryObserverTest.java` and `CamelFaultCallbackObserverTest.java`.

- [ ] **Step 2: Run tests, verify fail, implement, verify pass**

Mirror the Camel observers exactly — same structure, filter on `HttpWorkerConstants.WORKER_TYPE`, inject `HttpWorkerFaultPublisher`.

- [ ] **Step 3: Commit**

```
feat(workers-http): CDI event observers with workerType filter

HttpCompletionExpiryObserver routes async timeouts to fault pipeline.
HttpFaultCallbackObserver routes REST callback faults. Both filter
on WORKER_TYPE="http" for co-deployment safety.

Refs: #5
```

---

## Task 11: Full build + CLAUDE.md cleanup

**Files:**
- Modify: `CLAUDE.md` (remove stale engine#447 reference)

- [ ] **Step 1: Run full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS — all modules compile, all tests pass

- [ ] **Step 2: Clean up stale engine#447 reference in CLAUDE.md**

Remove the row referencing engine#447 from the `## Cross-Repo Dependencies` table.

- [ ] **Step 3: Final commit**

```
chore: cleanup stale engine#447 reference and verify full build

engine#447 (NoOpWorkerExecutionManager @DefaultBean) is CLOSED —
exists in engine runtime. Remove stale reference from CLAUDE.md.

Refs: #5
```
