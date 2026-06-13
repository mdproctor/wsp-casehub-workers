# Worker Runtime Lifecycle + MCP Dynamic Tool Discovery — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a common worker runtime lifecycle SPI in workers-common, implement MCP dynamic tool discovery via `tools/list`, and migrate all four worker types to the lifecycle.

**Architecture:** `WorkerRuntime` interface + `WorkerRuntimeStatus` enum in `io.casehub.workers.common`. `WorkerLifecycleOrchestrator` discovers all `WorkerRuntime` beans via CDI and calls `initialize()` at startup, `shutdown()` at application stop. MCP runtime calls `tools/list` during init to auto-register discovered tools. All worker types adopt the lifecycle; ad-hoc `@Observes StartupEvent` and `@PreDestroy` patterns are removed.

**Tech Stack:** Java 22, Quarkus 3.32, Mutiny, Vert.x WebClient, JUnit 5 + Mockito + AssertJ

**Spec:** `docs/superpowers/specs/2026-06-13-worker-runtime-lifecycle-mcp-discovery-design.md`

---

### Task 1: WorkerRuntimeStatus enum + WorkerRuntime interface (workers-common)

**Files:**
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerRuntimeStatus.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerRuntime.java`

- [ ] **Step 1: Create WorkerRuntimeStatus enum**

```java
package io.casehub.workers.common;

/**
 * Lifecycle status of a {@link WorkerRuntime}.
 *
 * <p>Aligned with the status vocabulary used across CaseHub and Serverless
 * Workflow 1.0 where applicable. Workflow/task instances use
 * {@code WorkflowStatus} (PENDING, RUNNING, WAITING, COMPLETED, FAULTED,
 * CANCELLED, SUSPENDED); worker runtimes use a subset appropriate to an
 * executor lifecycle rather than a task instance lifecycle.
 *
 * <p>Current states cover the initialization and shutdown lifecycle.
 * Future states may include:
 * <ul>
 *   <li>{@code SUSPENDED} — temporarily not accepting dispatches (e.g.,
 *       backpressure, maintenance window). Transitions: RUNNING → SUSPENDED
 *       → RUNNING.</li>
 *   <li>{@code DRAINING} — no new dispatches accepted, in-flight work
 *       completing before shutdown. Transition: RUNNING → DRAINING →
 *       STOPPED.</li>
 * </ul>
 *
 * <p>When adding states, preserve the convention: states that accept new
 * dispatches are "active" (currently only {@code RUNNING}); states that
 * reject new dispatches are "inactive" ({@code PENDING}, {@code FAULTED},
 * {@code STOPPED}).
 */
public enum WorkerRuntimeStatus {
    /** Configured but not yet initialized. Initial state. */
    PENDING,
    /** Initialized and accepting dispatches. */
    RUNNING,
    /** Initialization failed or a runtime error made the worker unavailable. */
    FAULTED,
    /** Shutdown completed. Terminal state. */
    STOPPED
}
```

- [ ] **Step 2: Create WorkerRuntime interface**

```java
package io.casehub.workers.common;

import io.smallrye.mutiny.Uni;
import java.util.Set;

/**
 * Lifecycle contract for a worker runtime — the infrastructure that executes
 * dispatched work for a specific worker type.
 *
 * <p>A {@code WorkerRuntime} is an <em>executor</em>, not a task instance.
 * It boots, discovers what it can execute, accepts dispatches, and eventually
 * shuts down. Contrast with {@code WorkerStatusListener}, which tracks
 * individual dispatch (task-instance) lifecycle events
 * ({@code onWorkerStarted}, {@code onWorkerCompleted}, {@code onWorkerStalled}).
 *
 * <h3>Relationship to other SPIs</h3>
 * <ul>
 *   <li>{@code ReactiveWorkerProvisioner} — capability probe at case
 *       planning time. Provisioner implementations delegate to their
 *       module's resolver, which is populated during
 *       {@link #initialize()}. The provisioner does not call
 *       {@link #capabilities()} directly.</li>
 *   <li>{@code WorkerExecutionManager} — dispatch at execution time. Uses
 *       the worker module's internal resolver (e.g., {@code McpServerResolver})
 *       to route a capability tag to a concrete target.</li>
 *   <li>{@code WorkerStatusListener} / {@code ReactiveWorkerStatusListener}
 *       — per-dispatch status callbacks. Orthogonal to runtime lifecycle.</li>
 * </ul>
 *
 * <h3>Terminology alignment</h3>
 * <p>The status vocabulary draws from Serverless Workflow 1.0
 * ({@code WorkflowStatus}) and CaseHub's own {@code CaseStatus} /
 * {@code PlanItemStatus}. Where a concept maps directly (PENDING, RUNNING,
 * FAULTED), the same name is used. Where worker runtimes have concerns
 * that task instances do not (capability discovery, connection pooling,
 * session management), worker-specific terms are introduced.
 *
 * <h3>Future lifecycle methods</h3>
 * <p>Methods that may be added as consumers emerge:
 * <ul>
 *   <li>{@code suspend()} / {@code resume()} — RUNNING ↔ SUSPENDED,
 *       for backpressure or maintenance windows.</li>
 *   <li>{@code healthCheck()} — liveness/readiness probe, returning
 *       current status plus diagnostics.</li>
 *   <li>{@code drain()} — stop accepting new dispatches, wait for
 *       in-flight work to complete, then transition to STOPPED.</li>
 * </ul>
 *
 * <h3>Implementation notes</h3>
 * <p>Implementations must be {@code @ApplicationScoped}. The runtime
 * orchestrator discovers all {@code WorkerRuntime} beans via CDI and
 * calls {@link #initialize()} at application startup. Implementations
 * must be safe to call from the Vert.x event loop — avoid blocking
 * operations or use {@code emitOn(Infrastructure.getDefaultWorkerPool())}
 * where necessary.
 */
public interface WorkerRuntime {

    /**
     * Worker type discriminator — e.g., {@code "mcp"}, {@code "http"},
     * {@code "camel"}, {@code "github-actions"}. Must match the value
     * used in {@code PendingCompletion.workerType()} and CDI event
     * filtering.
     */
    String workerType();

    /**
     * Current lifecycle status. Reflects initialization outcome and
     * shutdown state only. Post-initialization failures (server
     * unreachability, connection errors) are handled by the per-dispatch
     * fault pipeline and do not change runtime status. A future
     * {@code healthCheck()} method could surface runtime-level
     * degradation.
     *
     * @see WorkerRuntimeStatus
     */
    WorkerRuntimeStatus status();

    /**
     * Boot the worker runtime: load configuration, establish connections,
     * discover capabilities.
     *
     * <p>Transitions: {@code PENDING → RUNNING} on success,
     * {@code PENDING → FAULTED} on failure. Calling {@code initialize()}
     * on a runtime that is already {@code RUNNING} is a no-op. Calling
     * {@code initialize()} on a {@code FAULTED} runtime retries
     * initialization (enabling recovery without application restart).
     *
     * <p>For workers with external connectivity (e.g., MCP session
     * initialization, remote endpoint health checks), this method
     * performs the initial handshake. Capability discovery (e.g.,
     * MCP {@code tools/list}) also happens inside this method.
     * After {@code initialize()} completes, {@link #capabilities()}
     * returns the full set.
     */
    Uni<Void> initialize();

    /**
     * Release resources held by this worker runtime: close sessions,
     * return connections, cancel timers.
     *
     * <p>Transitions to {@code STOPPED} on completion. Called at
     * application shutdown. Implementations should be best-effort —
     * log failures but do not throw.
     */
    Uni<Void> shutdown();

    /**
     * Returns the set of capability tags this worker can handle.
     *
     * <p>Valid after {@link #initialize()} succeeds. The orchestrator
     * calls this after initialization to log discovered capabilities.
     * Provisioner implementations typically delegate to their module's
     * resolver (e.g., {@code McpServerResolver.capabilities()}) which
     * is populated during {@link #initialize()}.
     *
     * <p>For config-driven workers this returns the statically
     * configured set. For discovery-capable workers (e.g., MCP via
     * {@code tools/list}), this returns dynamically discovered
     * capabilities merged with any config-declared ones.
     *
     * <p>Synchronous — reads from an in-memory map populated during
     * {@link #initialize()}. Consistent with {@link #workerType()} and
     * {@link #status()} which are also synchronous state queries.
     */
    Set<String> capabilities();
}
```

- [ ] **Step 3: Build to verify compilation**

Run: `mvn --batch-mode compile -pl workers-common -q`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```bash
git add workers-common/src/main/java/io/casehub/workers/common/WorkerRuntimeStatus.java workers-common/src/main/java/io/casehub/workers/common/WorkerRuntime.java
git commit -m "feat(#7): add WorkerRuntime SPI and WorkerRuntimeStatus enum to workers-common"
```

---

### Task 2: WorkerLifecycleOrchestrator (workers-common)

**Files:**
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerLifecycleOrchestratorTest.java`
- Create: `workers-common/src/main/java/io/casehub/workers/common/WorkerLifecycleOrchestrator.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.smallrye.mutiny.Uni;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class WorkerLifecycleOrchestratorTest {

    @Test
    void initializeAll_callsInitializeOnEachRuntime() {
        WorkerRuntime rt1 = mockRuntime("http", WorkerRuntimeStatus.PENDING);
        WorkerRuntime rt2 = mockRuntime("mcp", WorkerRuntimeStatus.PENDING);
        when(rt1.initialize()).thenReturn(Uni.createFrom().voidItem());
        when(rt2.initialize()).thenReturn(Uni.createFrom().voidItem());
        when(rt1.status()).thenReturn(WorkerRuntimeStatus.PENDING, WorkerRuntimeStatus.RUNNING);
        when(rt2.status()).thenReturn(WorkerRuntimeStatus.PENDING, WorkerRuntimeStatus.RUNNING);
        when(rt1.capabilities()).thenReturn(Set.of("http:send-email"));
        when(rt2.capabilities()).thenReturn(Set.of("mcp:slack:send-message"));

        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator();
        orchestrator.initializeAll(List.of(rt1, rt2));

        verify(rt1).initialize();
        verify(rt2).initialize();
    }

    @Test
    void initializeAll_faultedRuntime_doesNotPreventOthers() {
        WorkerRuntime rt1 = mockRuntime("http", WorkerRuntimeStatus.PENDING);
        WorkerRuntime rt2 = mockRuntime("mcp", WorkerRuntimeStatus.PENDING);
        when(rt1.initialize()).thenReturn(Uni.createFrom().failure(new RuntimeException("boom")));
        when(rt2.initialize()).thenReturn(Uni.createFrom().voidItem());
        when(rt1.status()).thenReturn(WorkerRuntimeStatus.FAULTED);
        when(rt2.status()).thenReturn(WorkerRuntimeStatus.PENDING, WorkerRuntimeStatus.RUNNING);
        when(rt2.capabilities()).thenReturn(Set.of("mcp:slack:send-message"));

        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator();
        orchestrator.initializeAll(List.of(rt1, rt2));

        verify(rt1).initialize();
        verify(rt2).initialize();
    }

    @Test
    void initializeAll_emptyList_noErrors() {
        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator();
        orchestrator.initializeAll(List.of());
    }

    @Test
    void shutdownAll_callsShutdownOnRunningAndFaulted() {
        WorkerRuntime running = mockRuntime("http", WorkerRuntimeStatus.RUNNING);
        WorkerRuntime faulted = mockRuntime("mcp", WorkerRuntimeStatus.FAULTED);
        WorkerRuntime pending = mockRuntime("camel", WorkerRuntimeStatus.PENDING);
        when(running.shutdown()).thenReturn(Uni.createFrom().voidItem());
        when(faulted.shutdown()).thenReturn(Uni.createFrom().voidItem());

        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator();
        orchestrator.shutdownAll(List.of(running, faulted, pending));

        verify(running).shutdown();
        verify(faulted).shutdown();
        verify(pending, never()).shutdown();
    }

    @Test
    void shutdownAll_failureDoesNotPreventOthers() {
        WorkerRuntime rt1 = mockRuntime("http", WorkerRuntimeStatus.RUNNING);
        WorkerRuntime rt2 = mockRuntime("mcp", WorkerRuntimeStatus.RUNNING);
        when(rt1.shutdown()).thenReturn(Uni.createFrom().failure(new RuntimeException("shutdown error")));
        when(rt2.shutdown()).thenReturn(Uni.createFrom().voidItem());

        WorkerLifecycleOrchestrator orchestrator = new WorkerLifecycleOrchestrator();
        orchestrator.shutdownAll(List.of(rt1, rt2));

        verify(rt1).shutdown();
        verify(rt2).shutdown();
    }

    private WorkerRuntime mockRuntime(String workerType, WorkerRuntimeStatus initialStatus) {
        WorkerRuntime rt = mock(WorkerRuntime.class);
        when(rt.workerType()).thenReturn(workerType);
        when(rt.status()).thenReturn(initialStatus);
        return rt;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerLifecycleOrchestratorTest -q`
Expected: COMPILATION FAILURE — `WorkerLifecycleOrchestrator` does not exist

- [ ] **Step 3: Implement WorkerLifecycleOrchestrator**

```java
package io.casehub.workers.common;

import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.PreDestroy;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import java.util.List;
import org.jboss.logging.Logger;

import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

/**
 * Discovers all {@link WorkerRuntime} beans via CDI and drives their
 * lifecycle at application startup and shutdown.
 *
 * <p>Startup: calls {@link WorkerRuntime#initialize()} on each bean.
 * Workers that reach RUNNING have their capabilities logged. Workers
 * that fail go FAULTED — they do not prevent other workers from starting.
 *
 * <p>Shutdown: calls {@link WorkerRuntime#shutdown()} on all beans
 * that were initialized (status != PENDING). Best-effort — failures
 * are logged, not thrown.
 *
 * <p>Initialization order across worker types is undefined (CDI
 * {@code Instance} does not guarantee iteration order). No runtime
 * may depend on another runtime's initialization having completed.
 */
@ApplicationScoped
public class WorkerLifecycleOrchestrator {

    private static final Logger LOG = Logger.getLogger(WorkerLifecycleOrchestrator.class);

    @Inject @Any
    Instance<WorkerRuntime> runtimes;

    void onStartup(@Observes @Priority(APPLICATION + 10) StartupEvent ev) {
        if (runtimes == null || runtimes.isUnsatisfied()) {
            LOG.info("No WorkerRuntime beans discovered — no worker modules on classpath");
            return;
        }
        initializeAll(runtimes.stream().toList());
    }

    @PreDestroy
    void onShutdown() {
        if (runtimes == null || runtimes.isUnsatisfied()) {
            return;
        }
        shutdownAll(runtimes.stream().toList());
    }

    void initializeAll(List<WorkerRuntime> workerRuntimes) {
        if (workerRuntimes.isEmpty()) {
            LOG.info("No WorkerRuntime beans discovered — no worker modules on classpath");
            return;
        }
        for (WorkerRuntime runtime : workerRuntimes) {
            try {
                runtime.initialize().await().indefinitely();
                if (runtime.status() == WorkerRuntimeStatus.RUNNING) {
                    LOG.infof("Worker '%s' initialized — capabilities: %s",
                        runtime.workerType(), runtime.capabilities());
                } else {
                    LOG.warnf("Worker '%s' did not reach RUNNING after initialize() — status: %s",
                        runtime.workerType(), runtime.status());
                }
            } catch (Exception e) {
                LOG.warnf("Worker '%s' failed to initialize: %s",
                    runtime.workerType(), e.getMessage());
            }
        }
    }

    void shutdownAll(List<WorkerRuntime> workerRuntimes) {
        for (WorkerRuntime runtime : workerRuntimes) {
            if (runtime.status() == WorkerRuntimeStatus.PENDING) {
                continue;
            }
            try {
                runtime.shutdown().await().indefinitely();
            } catch (Exception e) {
                LOG.warnf("Worker '%s' shutdown failed: %s",
                    runtime.workerType(), e.getMessage());
            }
        }
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerLifecycleOrchestratorTest -q`
Expected: All 5 tests PASS

- [ ] **Step 5: Commit**

```bash
git add workers-common/src/main/java/io/casehub/workers/common/WorkerLifecycleOrchestrator.java workers-common/src/test/java/io/casehub/workers/common/WorkerLifecycleOrchestratorTest.java
git commit -m "feat(#7): add WorkerLifecycleOrchestrator — discovers and drives worker lifecycle"
```

---

### Task 3: Fix terminate() signature on all provisioners

**Files:**
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpReactiveWorkerProvisioner.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpReactiveWorkerProvisioner.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelReactiveWorkerProvisioner.java`
- Modify: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisioner.java`

- [ ] **Step 1: Fix all four provisioners**

In each file, change:
```java
public Uni<Void> terminate(String workerId) {
```
to:
```java
public Uni<Void> terminate(String workerId, String tenancyId) {
```

All four implementations return `Uni.createFrom().voidItem()` — the body stays the same. This matches the engine-api interface `ReactiveWorkerProvisioner.terminate(String workerId, String tenancyId)`.

- [ ] **Step 2: Build to verify compilation**

Run: `mvn --batch-mode compile -q`
Expected: BUILD SUCCESS

- [ ] **Step 3: Run all existing tests**

Run: `mvn --batch-mode test -q`
Expected: All tests pass (no test calls `terminate()` with the old signature)

- [ ] **Step 4: Commit**

```bash
git add workers-mcp/src/main/java/io/casehub/workers/mcp/McpReactiveWorkerProvisioner.java workers-http/src/main/java/io/casehub/workers/http/HttpReactiveWorkerProvisioner.java workers-camel/src/main/java/io/casehub/workers/camel/CamelReactiveWorkerProvisioner.java workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisioner.java
git commit -m "fix(#7): align terminate() signature with engine-api — add tenancyId parameter"
```

---

### Task 4: HttpWorkerRuntime (workers-http)

**Files:**
- Create: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerRuntimeTest.java`
- Create: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerRuntime.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.http;

import static org.assertj.core.api.Assertions.assertThat;

import io.casehub.workers.common.WorkerRuntimeStatus;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class HttpWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("http");
    }

    @Test
    void initialize_transitionsToRunning() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_populatesCapabilities() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        resolver.initialize(List.of(), Map.of(
            "send-email", Map.of("url", "https://mail.example.com/send", "method", "POST")
        ), 30);

        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.capabilities()).contains("send-email");
    }

    @Test
    void initialize_whenAlreadyRunning_isNoOp() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    @Test
    void capabilities_beforeInit_isEmpty() {
        HttpEndpointResolver resolver = new HttpEndpointResolver();
        HttpWorkerRuntime runtime = new HttpWorkerRuntime(resolver);

        assertThat(runtime.capabilities()).isEmpty();
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpWorkerRuntimeTest -q`
Expected: COMPILATION FAILURE — `HttpWorkerRuntime` does not exist

- [ ] **Step 3: Implement HttpWorkerRuntime**

```java
package io.casehub.workers.http;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class HttpWorkerRuntime implements WorkerRuntime {

    private final HttpEndpointResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    HttpWorkerRuntime(HttpEndpointResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return HttpWorkerConstants.WORKER_TYPE;
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
            resolver.initialize();
            status = WorkerRuntimeStatus.RUNNING;
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

- [ ] **Step 4: Remove onStartup from HttpEndpointResolver**

In `workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java`, remove:
```java
void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
    initialize();
}
```
And remove the unused imports: `io.quarkus.runtime.StartupEvent`, `jakarta.enterprise.event.Observes`, `jakarta.annotation.Priority`, `static jakarta.interceptor.Interceptor.Priority.APPLICATION`.

The `initialize()` method stays — it is now called by `HttpWorkerRuntime`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-http -q`
Expected: All tests pass (both new and existing)

- [ ] **Step 6: Commit**

```bash
git add workers-http/src/main/java/io/casehub/workers/http/HttpWorkerRuntime.java workers-http/src/test/java/io/casehub/workers/http/HttpWorkerRuntimeTest.java workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java
git commit -m "feat(#7): add HttpWorkerRuntime — lifecycle adapter for HTTP worker"
```

---

### Task 5: CamelWorkerRuntime (workers-camel)

**Files:**
- Create: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerRuntimeTest.java`
- Create: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerRuntime.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.camel;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.casehub.workers.common.WorkerRuntimeStatus;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.camel.CamelContext;
import org.junit.jupiter.api.Test;

class CamelWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        CamelCapabilityResolver resolver = createResolver();
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(resolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("camel");
    }

    @Test
    void initialize_transitionsToRunning() {
        CamelCapabilityResolver resolver = createResolver();
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_populatesCapabilities() {
        CamelCapabilityResolver resolver = createResolver();
        resolver.configCapabilities = Map.of("send-email", "direct:send-email");
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();

        assertThat(runtime.capabilities()).contains("send-email");
    }

    @Test
    void shutdown_transitionsToStopped() {
        CamelCapabilityResolver resolver = createResolver();
        CamelWorkerRuntime runtime = new CamelWorkerRuntime(resolver);

        runtime.initialize().await().indefinitely();
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    private CamelCapabilityResolver createResolver() {
        CamelCapabilityResolver resolver = new CamelCapabilityResolver();
        resolver.camelContext = mock(CamelContext.class);
        when(resolver.camelContext.getRoutes()).thenReturn(List.of());
        resolver.spiRoutes = null;
        resolver.configCapabilities = Map.of();
        return resolver;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-camel -Dtest=CamelWorkerRuntimeTest -q`
Expected: COMPILATION FAILURE — `CamelWorkerRuntime` does not exist

- [ ] **Step 3: Implement CamelWorkerRuntime**

```java
package io.casehub.workers.camel;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class CamelWorkerRuntime implements WorkerRuntime {

    private final CamelCapabilityResolver resolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    CamelWorkerRuntime(CamelCapabilityResolver resolver) {
        this.resolver = resolver;
    }

    @Override
    public String workerType() {
        return CamelWorkerConstants.WORKER_TYPE;
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
            resolver.initialize();
            status = WorkerRuntimeStatus.RUNNING;
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

- [ ] **Step 4: Remove onStartup from CamelCapabilityResolver**

In `workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java`, remove:
```java
void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
    initialize();
}
```
And remove unused imports: `io.quarkus.runtime.StartupEvent`, `jakarta.enterprise.event.Observes`, `jakarta.annotation.Priority`, `static jakarta.interceptor.Interceptor.Priority.APPLICATION`.

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-camel -q`
Expected: All tests pass

- [ ] **Step 6: Commit**

```bash
git add workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerRuntime.java workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerRuntimeTest.java workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java
git commit -m "feat(#7): add CamelWorkerRuntime — lifecycle adapter for Camel worker"
```

---

### Task 6: GitHubActionsWorkerRuntime (workers-github-actions)

**Files:**
- Create: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerRuntimeTest.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerRuntime.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.casehub.workers.common.WorkerRuntimeStatus;
import org.junit.jupiter.api.Test;

class GitHubActionsWorkerRuntimeTest {

    @Test
    void initialStatus_isPending() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("github-actions");
    }

    @Test
    void initialize_withToken_transitionsToRunning() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void initialize_withoutToken_transitionsToFaulted() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(false);

        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
    }

    @Test
    void initialize_faultedThenTokenAdded_recoversToRunning() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(false, true);

        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void capabilities_returnsStaticSet() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.capabilities()).containsExactlyInAnyOrder(
            "github-actions:workflow-dispatch",
            "github-actions:repository-dispatch"
        );
    }

    @Test
    void capabilities_beforeInit_isEmpty() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);

        assertThat(runtime.capabilities()).isEmpty();
    }

    @Test
    void shutdown_transitionsToStopped() {
        GitHubActionsTokenResolver tokenResolver = mock(GitHubActionsTokenResolver.class);
        when(tokenResolver.hasToken()).thenReturn(true);

        GitHubActionsWorkerRuntime runtime = new GitHubActionsWorkerRuntime(tokenResolver);
        runtime.initialize().await().indefinitely();
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsWorkerRuntimeTest -q`
Expected: COMPILATION FAILURE — `GitHubActionsWorkerRuntime` does not exist

- [ ] **Step 3: Implement GitHubActionsWorkerRuntime**

```java
package io.casehub.workers.githubactions;

import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class GitHubActionsWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(GitHubActionsWorkerRuntime.class);

    private final GitHubActionsTokenResolver tokenResolver;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    GitHubActionsWorkerRuntime(GitHubActionsTokenResolver tokenResolver) {
        this.tokenResolver = tokenResolver;
    }

    @Override
    public String workerType() {
        return GitHubActionsWorkerConstants.WORKER_TYPE;
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
            if (tokenResolver.hasToken()) {
                status = WorkerRuntimeStatus.RUNNING;
            } else {
                LOG.warn("GitHub Actions worker has no configured token — status FAULTED");
                status = WorkerRuntimeStatus.FAULTED;
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
        if (status != WorkerRuntimeStatus.RUNNING) {
            return Set.of();
        }
        return Set.of(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH,
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH
        );
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-github-actions -q`
Expected: All tests pass

- [ ] **Step 5: Commit**

```bash
git add workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerRuntime.java workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerRuntimeTest.java
git commit -m "feat(#7): add GitHubActionsWorkerRuntime — lifecycle adapter for GitHub Actions worker"
```

---

### Task 7: McpServerResolver — discovery mode + registerDiscoveredTools (workers-mcp)

**Files:**
- Modify: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java` (ServerConfig record)

- [ ] **Step 1: Add discovery field to ServerConfig and loadFromConfig**

In `McpServerResolver.java`, update the `ServerConfig` record:
```java
record ServerConfig(String name, String url, String tools, int timeoutSeconds,
                    Map<String, String> headers, String discovery) {}
```

Update `buildServerConfig`:
```java
private ServerConfig buildServerConfig(String name, Map<String, String> props) {
    String url = props.get("url");
    String tools = props.getOrDefault("tools", "");
    int timeout = parseTimeout(props.get("timeout-seconds"));
    Map<String, String> headers = extractHeaders(props);
    String discovery = props.getOrDefault("discovery", "auto");
    return new ServerConfig(name, url, tools, timeout, headers, discovery);
}
```

- [ ] **Step 2: Write tests for registerDiscoveredTools**

Add to `McpServerResolverTest.java`:

```java
@Test
void registerDiscoveredTools_fullDiscovery_registersAllTools() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30);

    resolver.registerDiscoveredTools("slack", Set.of("send-message", "list-channels"));

    assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
        "mcp:slack:send-message",
        "mcp:slack:list-channels"
    );
}

@Test
void registerDiscoveredTools_withAllowlist_registersOnlyConfigTools() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30);

    resolver.registerDiscoveredTools("slack", Set.of("send-message", "list-channels", "delete-message"));

    assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
        "mcp:slack:send-message"
    );
}

@Test
void registerDiscoveredTools_allowlistToolNotInDiscovery_keptWithWarning() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "send-message,custom-tool", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30);

    resolver.registerDiscoveredTools("slack", Set.of("send-message"));

    assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
        "mcp:slack:send-message",
        "mcp:slack:custom-tool"
    );
}

@Test
void registerDiscoveredTools_rebuildsTags() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30);
    assertThat(resolver.capabilities()).isEmpty();

    resolver.registerDiscoveredTools("slack", Set.of("send-message"));

    ResolvedMcpServer resolved = resolver.resolve("mcp:slack:send-message");
    assertThat(resolved.name()).isEqualTo("slack");
    assertThat(resolved.tools()).contains("send-message");
}

@Test
void isDiscoveryEnabled_autoMode_returnsTrue() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30);
    assertThat(resolver.isDiscoveryEnabled("slack")).isTrue();
}

@Test
void isDiscoveryEnabled_manualMode_returnsFalse() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of(), "manual")
    );
    resolver.initialize(servers, 30);
    assertThat(resolver.isDiscoveryEnabled("slack")).isFalse();
}

@Test
void isDiscoveryEnabled_defaultMode_returnsTrue() {
    McpServerResolver resolver = new McpServerResolver();
    List<ServerConfig> servers = List.of(
        new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "")
    );
    resolver.initialize(servers, 30);
    assertThat(resolver.isDiscoveryEnabled("slack")).isTrue();
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest -q`
Expected: COMPILATION FAILURE — `registerDiscoveredTools` and `isDiscoveryEnabled` do not exist

- [ ] **Step 4: Implement registerDiscoveredTools and isDiscoveryEnabled**

Add to `McpServerResolver.java`:

Add a field:
```java
private final Map<String, ServerConfig> configByName = new HashMap<>();
```

In `initialize()`, after the for loop, store configs:
```java
configByName.put(config.name(), config);
```

Add methods:
```java
boolean isDiscoveryEnabled(String serverName) {
    ServerConfig config = configByName.get(serverName);
    if (config == null) return false;
    String mode = config.discovery();
    return mode == null || mode.isBlank() || "auto".equalsIgnoreCase(mode);
}

void registerDiscoveredTools(String serverName, Set<String> discoveredToolNames) {
    ResolvedMcpServer existing = serversByName.get(serverName);
    if (existing == null) {
        throw new WorkerProvisioningException("No MCP server found with name: " + serverName);
    }

    ServerConfig config = configByName.get(serverName);
    Set<String> configTools = parseTools(config.tools(), serverName);
    Set<String> finalTools;

    if (configTools.isEmpty()) {
        finalTools = Set.copyOf(discoveredToolNames);
    } else {
        for (String configTool : configTools) {
            if (!discoveredToolNames.contains(configTool)) {
                LOG.warnf("MCP server '%s': config-declared tool '%s' not found in tools/list response",
                    serverName, configTool);
            }
        }
        finalTools = configTools;
    }

    // Remove old tags
    Set<String> oldTags = new HashSet<>();
    capabilityToServerName.forEach((tag, name) -> {
        if (name.equals(serverName)) oldTags.add(tag);
    });
    oldTags.forEach(capabilityToServerName::remove);

    // Replace server with updated tools
    ResolvedMcpServer updated = new ResolvedMcpServer(
        existing.name(), existing.url(), existing.timeoutSeconds(),
        existing.headers(), finalTools
    );
    serversByName.put(serverName, updated);

    // Rebuild tags
    for (String tool : finalTools) {
        capabilityToServerName.put(buildCapabilityTag(serverName, tool), serverName);
    }
}
```

Add a Logger field:
```java
private static final Logger LOG = Logger.getLogger(McpServerResolver.class);
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest -q`
Expected: All tests pass (existing + new)

- [ ] **Step 6: Commit**

```bash
git add workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java workers-mcp/src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java
git commit -m "feat(#7): add discovery mode and registerDiscoveredTools to McpServerResolver"
```

---

### Task 8: McpWorkerRuntime — session init + tools/list discovery (workers-mcp)

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/ServerInitResult.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpWorkerRuntimeTest.java`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerRuntime.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpSessionManager.java` (remove `@PreDestroy`)

- [ ] **Step 1: Create ServerInitResult record**

```java
package io.casehub.workers.mcp;

import java.util.Set;

record ServerInitResult(String serverName, boolean success, McpSession session,
                        Set<String> discoveredTools, Throwable error) {

    static ServerInitResult success(String serverName, McpSession session, Set<String> discoveredTools) {
        return new ServerInitResult(serverName, true, session, discoveredTools, null);
    }

    static ServerInitResult failure(String serverName, Throwable error) {
        return new ServerInitResult(serverName, false, null, Set.of(), error);
    }
}
```

- [ ] **Step 2: Write the failing tests**

```java
package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.casehub.workers.common.WorkerRuntimeStatus;
import io.casehub.workers.mcp.McpServerResolver.ServerConfig;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class McpWorkerRuntimeTest {

    private McpServerResolver serverResolver;
    private McpSessionManager sessionManager;
    private WebClient webClient;
    private McpWorkerRuntime runtime;

    @BeforeEach
    void setUp() {
        serverResolver = new McpServerResolver();
        sessionManager = mock(McpSessionManager.class);
        webClient = mock(WebClient.class);
    }

    @Test
    void initialStatus_isPending() {
        initResolver("slack", "https://slack.internal/mcp", "", "auto");
        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.PENDING);
        assertThat(runtime.workerType()).isEqualTo("mcp");
    }

    @Test
    void initialize_discoveryAuto_callsToolsList() {
        initResolver("slack", "https://slack.internal/mcp", "", "auto");
        McpSession session = new McpSession("sess-1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        HttpRequest<Buffer> request = mock(HttpRequest.class);
        when(webClient.requestAbs(HttpMethod.POST, "https://slack.internal/mcp")).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(any(Long.class))).thenReturn(request);

        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(response.bodyAsString()).thenReturn(
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":["
            + "{\"name\":\"send-message\",\"description\":\"Send a message\"},"
            + "{\"name\":\"list-channels\",\"description\":\"List channels\"}"
            + "]}}"
        );
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactlyInAnyOrder(
            "mcp:slack:send-message",
            "mcp:slack:list-channels"
        );
    }

    @Test
    void initialize_discoveryManual_skipsToolsList() {
        initResolver("slack", "https://slack.internal/mcp", "send-message", "manual");
        McpSession session = new McpSession("sess-1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactlyInAnyOrder("mcp:slack:send-message");
    }

    @Test
    void initialize_sessionFails_faulted() {
        initResolver("slack", "https://slack.internal/mcp", "", "auto");
        when(sessionManager.getOrInitialize("slack"))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("connection refused")));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.FAULTED);
        assertThat(runtime.capabilities()).isEmpty();
    }

    @Test
    void initialize_partialFailure_running() {
        serverResolver.initialize(List.of(
            new ServerConfig("slack", "https://slack.internal/mcp", "", 30, Map.of(), "auto"),
            new ServerConfig("jira", "https://jira.internal/mcp", "", 30, Map.of(), "auto")
        ), 30);

        McpSession slackSession = new McpSession("s1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(slackSession));
        when(sessionManager.getOrInitialize("jira"))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("jira down")));

        HttpRequest<Buffer> request = mock(HttpRequest.class);
        when(webClient.requestAbs(HttpMethod.POST, "https://slack.internal/mcp")).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(any(Long.class))).thenReturn(request);

        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(response.bodyAsString()).thenReturn(
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"tools\":[{\"name\":\"send-message\"}]}}"
        );
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("mcp:slack:send-message");
    }

    @Test
    void initialize_toolsListError_fallsBackToConfig() {
        initResolver("slack", "https://slack.internal/mcp", "send-message", "auto");
        McpSession session = new McpSession("sess-1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        HttpRequest<Buffer> request = mock(HttpRequest.class);
        when(webClient.requestAbs(HttpMethod.POST, "https://slack.internal/mcp")).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(any(Long.class))).thenReturn(request);

        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(200);
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(response.bodyAsString()).thenReturn(
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-32601,\"message\":\"Method not found\"}}"
        );
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
        assertThat(runtime.capabilities()).containsExactly("mcp:slack:send-message");
    }

    @Test
    void initialize_alreadyRunning_isNoOp() {
        initResolver("slack", "https://slack.internal/mcp", "send-message", "manual");
        McpSession session = new McpSession("sess-1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);

        runtime.initialize().await().indefinitely();
        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.RUNNING);
    }

    @Test
    void shutdown_transitionsToStopped() {
        initResolver("slack", "https://slack.internal/mcp", "send-message", "manual");
        McpSession session = new McpSession("sess-1", "2025-06-18");
        when(sessionManager.getOrInitialize("slack")).thenReturn(Uni.createFrom().item(session));

        runtime = new McpWorkerRuntime(serverResolver, sessionManager, webClient);
        runtime.initialize().await().indefinitely();

        when(sessionManager.shutdown()).thenReturn(Uni.createFrom().voidItem());
        runtime.shutdown().await().indefinitely();

        assertThat(runtime.status()).isEqualTo(WorkerRuntimeStatus.STOPPED);
    }

    private void initResolver(String name, String url, String tools, String discovery) {
        serverResolver.initialize(List.of(
            new ServerConfig(name, url, tools, 30, Map.of(), discovery)
        ), 30);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpWorkerRuntimeTest -q`
Expected: COMPILATION FAILURE — `McpWorkerRuntime` does not exist

- [ ] **Step 4: Implement McpWorkerRuntime**

```java
package io.casehub.workers.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.workers.common.WorkerRuntime;
import io.casehub.workers.common.WorkerRuntimeStatus;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.jboss.logging.Logger;

@ApplicationScoped
public class McpWorkerRuntime implements WorkerRuntime {

    private static final Logger LOG = Logger.getLogger(McpWorkerRuntime.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final McpServerResolver serverResolver;
    private final McpSessionManager sessionManager;
    private final WebClient webClient;
    private volatile WorkerRuntimeStatus status = WorkerRuntimeStatus.PENDING;

    @Inject
    McpWorkerRuntime(McpServerResolver serverResolver, McpSessionManager sessionManager,
                     WebClient webClient) {
        this.serverResolver = serverResolver;
        this.sessionManager = sessionManager;
        this.webClient = webClient;
    }

    @Override
    public String workerType() {
        return McpWorkerConstants.WORKER_TYPE;
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

        List<String> serverNames = serverResolver.serverNames();
        if (serverNames.isEmpty()) {
            LOG.warn("No MCP servers configured — status FAULTED");
            status = WorkerRuntimeStatus.FAULTED;
            return Uni.createFrom().voidItem();
        }

        List<Uni<ServerInitResult>> initUnis = serverNames.stream()
            .map(name -> initServer(name)
                .onFailure().recoverWithItem(err -> ServerInitResult.failure(name, err)))
            .toList();

        return Uni.join().all(initUnis).andFailFast()
            .invoke(results -> {
                long successes = results.stream().filter(ServerInitResult::success).count();
                long failures = results.size() - successes;

                for (ServerInitResult result : results) {
                    if (result.success()) {
                        if (serverResolver.isDiscoveryEnabled(result.serverName())
                                && !result.discoveredTools().isEmpty()) {
                            serverResolver.registerDiscoveredTools(
                                result.serverName(), result.discoveredTools());
                        }
                    } else {
                        LOG.warnf("MCP server '%s' failed to initialize: %s",
                            result.serverName(),
                            result.error() != null ? result.error().getMessage() : "unknown");
                    }
                }

                if (successes > 0) {
                    status = WorkerRuntimeStatus.RUNNING;
                    if (failures > 0) {
                        LOG.warnf("MCP runtime partially initialized: %d/%d servers succeeded",
                            successes, results.size());
                    }
                } else {
                    LOG.warn("All MCP servers failed to initialize — status FAULTED");
                    status = WorkerRuntimeStatus.FAULTED;
                }
            })
            .replaceWithVoid();
    }

    @Override
    public Uni<Void> shutdown() {
        return sessionManager.shutdown()
            .invoke(() -> status = WorkerRuntimeStatus.STOPPED)
            .onFailure().invoke(err -> {
                LOG.warnf("MCP shutdown error: %s", err.getMessage());
                status = WorkerRuntimeStatus.STOPPED;
            })
            .onFailure().recoverWithUni(err -> Uni.createFrom().voidItem());
    }

    @Override
    public Set<String> capabilities() {
        return serverResolver.capabilities();
    }

    private Uni<ServerInitResult> initServer(String serverName) {
        return sessionManager.getOrInitialize(serverName)
            .flatMap(session -> {
                if (!serverResolver.isDiscoveryEnabled(serverName)) {
                    return Uni.createFrom().item(
                        ServerInitResult.success(serverName, session, Set.of()));
                }
                return callToolsList(serverName, session)
                    .onItem().transform(tools ->
                        ServerInitResult.success(serverName, session, tools))
                    .onFailure().recoverWithItem(err -> {
                        LOG.warnf("MCP server '%s': tools/list failed, falling back to config: %s",
                            serverName, err.getMessage());
                        return ServerInitResult.success(serverName, session, Set.of());
                    });
            });
    }

    private Uni<Set<String>> callToolsList(String serverName, McpSession session) {
        ResolvedMcpServer server = serverResolver.serverByName(serverName);
        long requestId = session.nextRequestId();

        ObjectNode body = OBJECT_MAPPER.createObjectNode();
        body.put("jsonrpc", "2.0");
        body.put("id", requestId);
        body.put("method", "tools/list");

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        request.putHeader("MCP-Protocol-Version", session.protocolVersion());
        if (session.hasSessionId()) {
            request.putHeader("Mcp-Session-Id", session.sessionId());
        }
        server.headers().forEach(request::putHeader);
        request.timeout(server.timeoutSeconds() * 1000L);

        return request.sendJson(body)
            .map(response -> {
                if (response.statusCode() < 200 || response.statusCode() >= 300) {
                    throw new RuntimeException("tools/list returned " + response.statusCode());
                }
                return parseToolsListResponse(response.bodyAsString(), requestId);
            });
    }

    private Set<String> parseToolsListResponse(String body, long requestId) {
        try {
            JsonNode json = OBJECT_MAPPER.readTree(body);

            if (json.has("error")) {
                String msg = json.path("error").path("message").asText("Unknown error");
                throw new RuntimeException("tools/list JSON-RPC error: " + msg);
            }

            JsonNode result = json.get("result");
            if (result == null || !result.has("tools")) {
                return Set.of();
            }

            JsonNode toolsArray = result.get("tools");
            if (!toolsArray.isArray()) {
                return Set.of();
            }

            Set<String> tools = new HashSet<>();
            for (JsonNode tool : toolsArray) {
                if (tool.has("name")) {
                    tools.add(tool.get("name").asText());
                }
            }
            return Set.copyOf(tools);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException("Failed to parse tools/list response: " + e.getMessage(), e);
        }
    }
}
```

- [ ] **Step 5: Add serverNames() to McpServerResolver**

Add to `McpServerResolver.java`:
```java
List<String> serverNames() {
    return List.copyOf(serversByName.keySet());
}
```

- [ ] **Step 6: Add shutdown() method to McpSessionManager (returns Uni instead of @PreDestroy)**

In `McpSessionManager.java`:
- Remove the `@PreDestroy` annotation from the existing `shutdown()` method
- Change its return type from `void` to `Uni<Void>`:

```java
public Uni<Void> shutdown() {
    return Uni.createFrom().item(() -> {
        for (Map.Entry<String, Uni<McpSession>> entry : sessions.entrySet()) {
            try {
                McpSession session = entry.getValue()
                    .await().atMost(java.time.Duration.ofMillis(100));
                if (session != null && session.hasSessionId()) {
                    ResolvedMcpServer server = serverResolver.serverByName(entry.getKey());
                    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.DELETE, server.url());
                    request.putHeader("Mcp-Session-Id", session.sessionId());
                    request.send().subscribe().with(
                        resp -> LOG.debugf("Shutdown DELETE for %s: %d", entry.getKey(), resp.statusCode()),
                        err -> LOG.debugf("Shutdown DELETE for %s failed: %s", entry.getKey(), err.getMessage())
                    );
                }
            } catch (Exception e) {
                LOG.debugf("Skipping shutdown for %s: %s", entry.getKey(), e.getMessage());
            }
        }
        return null;
    }).replaceWithVoid();
}
```

- [ ] **Step 7: Remove onStartup from McpServerResolver**

In `McpServerResolver.java`, remove:
```java
void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
    List<ServerConfig> servers = loadFromConfig();
    initialize(servers, defaultTimeoutSeconds);
}
```

The `McpWorkerRuntime.initialize()` will call `loadFromConfig()` → `initialize()` instead. Make `loadFromConfig()` package-private (it already is).

Add a method to McpServerResolver that McpWorkerRuntime can call:
```java
void initializeFromConfig() {
    List<ServerConfig> servers = loadFromConfig();
    initialize(servers, defaultTimeoutSeconds);
}
```

Update `McpWorkerRuntime.initialize()` to call `serverResolver.initializeFromConfig()` at the start of init.

- [ ] **Step 8: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-mcp -q`
Expected: All tests pass (existing McpServerResolverTest, McpSessionManagerTest, McpWorkerExecutionManagerTest, McpReactiveWorkerProvisionerTest, and new McpWorkerRuntimeTest)

- [ ] **Step 9: Commit**

```bash
git add workers-mcp/src/main/java/io/casehub/workers/mcp/ServerInitResult.java workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerRuntime.java workers-mcp/src/test/java/io/casehub/workers/mcp/McpWorkerRuntimeTest.java workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java workers-mcp/src/main/java/io/casehub/workers/mcp/McpSessionManager.java
git commit -m "feat(#7): add McpWorkerRuntime — session init + tools/list discovery with parallel server init"
```

---

### Task 9: Full build verification

**Files:** None — verification only

- [ ] **Step 1: Full build**

Run: `mvn --batch-mode install -q`
Expected: BUILD SUCCESS across all modules

- [ ] **Step 2: Verify all tests pass**

Run: `mvn --batch-mode test -q`
Expected: All tests pass

- [ ] **Step 3: Commit any remaining fixes if needed**

---

### Task 10: Documentation updates

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Update CLAUDE.md**

Add `WorkerRuntime` and `WorkerRuntimeStatus` to the workers-common Key Types table. Add `McpWorkerRuntime` and `ServerInitResult` to the workers-mcp Key Types table. Update MCP Key Rules to document:
- MCP discovery mode: `auto` (default) calls `tools/list`, `manual` is config-only
- `tools` config becomes an allowlist when `discovery=auto`
- Eager session initialization at startup (shift from v1 lazy model)
- Parallel per-server initialization within the MCP runtime

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs(#7): update CLAUDE.md — worker runtime lifecycle, MCP discovery"
```
