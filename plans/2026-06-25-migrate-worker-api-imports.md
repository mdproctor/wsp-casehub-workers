# Migrate Worker Imports to casehub-worker-api — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate all Worker/Capability imports from `io.casehub.api.model` to `io.casehub.worker.api`, and governance types to `io.casehub.platform.api.governance`, aligning workers with the engine-common SPI contracts that already reference the new types.

**Architecture:** Mechanical migration — swap imports, update record accessors (`getName()` → `name()`), replace Capability constructors with `Capability.of()`, and resolve Worker.Builder lambda ambiguity with explicit `WorkerFunction.Sync` wrapping. No behavioral changes.

**Tech Stack:** Java 21 records, Maven dependency management, Quarkus CDI

**Spec:** `docs/superpowers/specs/2026-06-25-migrate-worker-api-imports-design.md`

## Global Constraints

- `casehub-worker-api` version: `${version.io.casehub}` (0.2-SNAPSHOT)
- Worker and Capability are now Java records — use record accessors (`name()` not `getName()`)
- Capability has 4-arg record constructor — always use `Capability.of(name, inputSchema, outputSchema)` factory for 3-arg construction
- Worker.Builder.function() has two overloads — always use `new WorkerFunction.Sync(lambda)` to avoid lambda ambiguity
- ProvisionContext, WorkResult, CaseHubEventType, EventStreamType stay in `io.casehub.api.model` — do NOT change these imports
- Provisioner `getCapabilities()` is an SPI method on ReactiveWorkerProvisioner — NOT a Worker record accessor. Do not rename it.

---

### Task 1: Add casehub-worker-api dependency to all POMs

**Files:**
- Modify: `pom.xml` (parent)
- Modify: `workers-common/pom.xml`
- Modify: `workers-http/pom.xml`
- Modify: `workers-camel/pom.xml`
- Modify: `workers-github-actions/pom.xml`
- Modify: `workers-mcp/pom.xml`
- Modify: `workers-script/pom.xml`
- Modify: `workers-testing/pom.xml`

**Interfaces:**
- Consumes: nothing
- Produces: `casehub-worker-api` available as a direct compile dependency in all modules

- [ ] **Step 1: Add to parent POM dependencyManagement**

In `pom.xml`, add inside `<dependencyManagement><dependencies>`, after the `casehub-engine-testing` block:

```xml
            <!-- Worker API — Worker/Capability record types -->
            <dependency>
                <groupId>io.casehub</groupId>
                <artifactId>casehub-worker-api</artifactId>
                <version>${version.io.casehub}</version>
            </dependency>
```

- [ ] **Step 2: Add direct dependency to workers-common**

In `workers-common/pom.xml`, add after the `casehub-engine-common` dependency:

```xml
        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-worker-api</artifactId>
        </dependency>
```

- [ ] **Step 3: Add direct dependency to the remaining 6 modules**

Add the same `<dependency>` block to each of these POMs, after their `casehub-engine-common` or `casehub-workers-common` dependency:

- `workers-http/pom.xml`
- `workers-camel/pom.xml`
- `workers-github-actions/pom.xml`
- `workers-mcp/pom.xml`
- `workers-script/pom.xml`
- `workers-testing/pom.xml`

```xml
        <dependency>
            <groupId>io.casehub</groupId>
            <artifactId>casehub-worker-api</artifactId>
        </dependency>
```

- [ ] **Step 4: Verify all POMs resolve**

Run: `mvn --batch-mode validate -N && mvn --batch-mode validate`
Expected: BUILD SUCCESS (all modules resolve the new dependency)

- [ ] **Step 5: Commit**

```
git add pom.xml workers-*/pom.xml
git commit -m "build(#14): add casehub-worker-api dependency to all modules"
```

---

### Task 2: Migrate workers-common production code

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerCorrelationContext.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerRetrySupport.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultHandler.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultPublisher.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/PendingCompletion.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/AsyncWorkerCompletionRegistry.java`
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerFaultEvent.java`

No changes to: `WorkerStatusPublisher.java` (uses `WorkResult` which stays in engine-api)

**Interfaces:**
- Consumes: casehub-worker-api on classpath (Task 1)
- Produces: workers-common production code compiles with new types

The migration pattern for each file is the same. Apply ALL of these transformations:

**Import swaps:**

| Find | Replace with |
|------|-------------|
| `import io.casehub.api.model.Worker;` | `import io.casehub.worker.api.Worker;` |
| `import io.casehub.api.model.Capability;` | `import io.casehub.worker.api.Capability;` |
| `import io.casehub.api.model.ExecutionPolicy;` | `import io.casehub.platform.api.governance.ExecutionPolicy;` |
| `import io.casehub.api.model.RetryPolicy;` | `import io.casehub.platform.api.governance.RetryPolicy;` |
| `import io.casehub.api.model.BackoffStrategy;` | `import io.casehub.platform.api.governance.BackoffStrategy;` |

Do NOT change: `import io.casehub.api.model.event.CaseHubEventType;` or `import io.casehub.api.model.event.EventStreamType;` — these stay.

**Getter renames:**

| Find | Replace with |
|------|-------------|
| `worker.getName()` | `worker.name()` |
| `capability.getName()` | `capability.name()` |
| `worker.getExecutionPolicy()` | `worker.executionPolicy()` |

- [ ] **Step 1: Migrate WorkerCorrelationContext.java**

Replace `import io.casehub.api.model.Worker;` with `import io.casehub.worker.api.Worker;`

This file has no getter calls — import-only change.

- [ ] **Step 2: Migrate WorkerRetrySupport.java**

Import swaps (5 imports — Worker, ExecutionPolicy, RetryPolicy, BackoffStrategy):
```
import io.casehub.api.model.Worker;          → import io.casehub.worker.api.Worker;
import io.casehub.api.model.ExecutionPolicy;  → import io.casehub.platform.api.governance.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;      → import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.api.model.BackoffStrategy;  → import io.casehub.platform.api.governance.BackoffStrategy;
```

Leave unchanged: `import io.casehub.api.model.event.CaseHubEventType;` and `import io.casehub.api.model.event.EventStreamType;`

Getter renames in this file:
- `worker.getName()` → `worker.name()` (multiple occurrences)
- `worker.getExecutionPolicy()` → `worker.executionPolicy()` (1 occurrence)

- [ ] **Step 3: Migrate WorkerFaultHandler.java**

Import swaps (2 imports):
```
import io.casehub.api.model.RetryPolicy; → import io.casehub.platform.api.governance.RetryPolicy;
import io.casehub.api.model.Worker;       → import io.casehub.worker.api.Worker;
```

Getter renames: `worker.getName()` → `worker.name()` (multiple occurrences)

- [ ] **Step 4: Migrate WorkerFaultPublisher.java**

Replace `import io.casehub.api.model.Capability;` with `import io.casehub.worker.api.Capability;`

- [ ] **Step 5: Migrate PendingCompletion.java**

Replace `import io.casehub.api.model.Capability;` with `import io.casehub.worker.api.Capability;`

- [ ] **Step 6: Migrate AsyncWorkerCompletionRegistry.java**

Replace `import io.casehub.api.model.Capability;` with `import io.casehub.worker.api.Capability;`

- [ ] **Step 7: Migrate WorkerFaultEvent.java**

Two import swaps:
```
import io.casehub.api.model.Capability; → import io.casehub.worker.api.Capability;
import io.casehub.api.model.Worker;     → import io.casehub.worker.api.Worker;
```

- [ ] **Step 8: Verify workers-common production compiles**

Run: `mvn --batch-mode compile -pl workers-common`
Expected: BUILD SUCCESS

- [ ] **Step 9: Commit**

```
git add workers-common/src/main/
git commit -m "refactor(#14): migrate workers-common production imports to worker-api + platform-api governance"
```

---

### Task 3: Migrate workers-testing + workers-common tests

**Files:**
- Modify: `workers-testing/src/main/java/io/casehub/workers/testing/WorkerTestSupport.java`
- Modify: 10 test files in `workers-common/src/test/java/io/casehub/workers/common/`

**Interfaces:**
- Consumes: workers-common production code compiles with new types (Task 2)
- Produces: `WorkerTestSupport.testWorker()` returns `io.casehub.worker.api.Worker`, `testCapability()` returns `io.casehub.worker.api.Capability`. workers-common tests pass.

- [ ] **Step 1: Migrate WorkerTestSupport.java**

Replace the entire file content with:

```java
package io.casehub.workers.testing;

import io.casehub.worker.api.Capability;
import io.casehub.worker.api.Worker;
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
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
            .map(tag -> Capability.of(tag, "", ""))
            .toList();
        return Worker.builder().name(name).capabilities(caps)
            .function(new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of())))
            .build();
    }

    public static Capability testCapability(String tag) {
        return Capability.of(tag, "", "");
    }
}
```

- [ ] **Step 2: Migrate workers-common test files — apply all four transformations**

For each of the 10 test files below, apply ALL applicable transformations:

**Import swaps** (same table as Task 2):
- `import io.casehub.api.model.Worker;` → `import io.casehub.worker.api.Worker;`
- `import io.casehub.api.model.Capability;` → `import io.casehub.worker.api.Capability;`
- `import io.casehub.api.model.ExecutionPolicy;` → `import io.casehub.platform.api.governance.ExecutionPolicy;`
- `import io.casehub.api.model.RetryPolicy;` → `import io.casehub.platform.api.governance.RetryPolicy;`
- `import io.casehub.api.model.BackoffStrategy;` → `import io.casehub.platform.api.governance.BackoffStrategy;`

Leave unchanged: `import io.casehub.api.model.event.CaseHubEventType;` (WorkerRetrySupportTest)

**Capability construction** — replace all occurrences:
- `new Capability("...", "", "")` → `Capability.of("...", "", "")`

**Worker.Builder.function()** — replace all occurrences:
- `.function(ctx -> null)` → `.function(new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of())))`

Add these two new imports to every file that has the function() fix:
```java
import io.casehub.worker.api.WorkerFunction;
import io.casehub.worker.api.WorkerResult;
```

Also add `import java.util.Map;` if not already present.

**Getter renames** (in test assertion/setup code):
- `worker.getName()` → `worker.name()`

**Files to migrate (apply all applicable patterns):**

1. `WorkerCorrelationContextTest.java` — Worker + Capability imports, Capability.of, function fix
2. `WorkerRetrySupportTest.java` — Worker + Capability + ExecutionPolicy + RetryPolicy + BackoffStrategy imports, Capability.of, function fix (3 sites), getter rename
3. `WorkerFaultHandlerTest.java` — Worker + Capability + ExecutionPolicy + RetryPolicy + BackoffStrategy imports, Capability.of, function fix
4. `WorkerCompletionExpiryObserverTest.java` — Worker + Capability imports, Capability.of, function fix
5. `WorkerFaultCallbackObserverTest.java` — Worker + Capability imports, Capability.of, function fix
6. `WorkerCallbackResourceTest.java` — Worker + Capability imports, Capability.of, function fix
7. `WorkerFaultPublisherTest.java` — Worker + Capability imports, Capability.of, function fix (2 sites)
8. `WorkflowCompletionPublisherTest.java` — Worker + Capability imports, Capability.of, function fix
9. `AsyncWorkerCompletionRegistryTest.java` — Worker + Capability imports, Capability.of, function fix, getter rename
10. `PendingCompletionTest.java` — Capability import only, Capability.of

- [ ] **Step 3: Verify workers-common compiles and tests pass**

Run: `mvn --batch-mode test -pl workers-common,workers-testing`
Expected: BUILD SUCCESS, all tests pass

- [ ] **Step 4: Commit**

```
git add workers-testing/ workers-common/src/test/
git commit -m "refactor(#14): migrate workers-testing + workers-common tests to worker-api types"
```

---

### Task 4: Migrate per-module production and test code

**Files:** 5 modules × (1 production + 2–3 test files) = 17 files total

Provisioner files (`*ReactiveWorkerProvisioner.java` and their tests) only import `ProvisionContext` which stays in engine-api — no changes needed.

**Interfaces:**
- Consumes: workers-common + workers-testing migrated (Tasks 2–3)
- Produces: full build passes with all modules on new types

The pattern is identical across all 5 modules. For each module:

**Production file** (`*WorkerExecutionManager.java`):
- Swap `import io.casehub.api.model.Worker;` → `import io.casehub.worker.api.Worker;`
- Swap `import io.casehub.api.model.Capability;` → `import io.casehub.worker.api.Capability;`
- Rename `worker.getName()` → `worker.name()` (all occurrences)
- Rename `capability.getName()` → `capability.name()` (all occurrences)

**Test files** (`*ExecutionManagerTest.java`, `*FaultEventHandlerTest.java`, and `CasehubProducerTest.java` for camel):
- Swap Worker + Capability imports (same as production)
- Replace `new Capability("...", "", "")` → `Capability.of("...", "", "")`
- Replace `.function(ctx -> null)` → `.function(new WorkerFunction.Sync(ctx -> WorkerResult.of(Map.of())))`
- Add `WorkerFunction`, `WorkerResult`, `Map` imports

- [ ] **Step 1: Migrate workers-http**

Production (1 file): `HttpWorkerExecutionManager.java` — 2 import swaps, getter renames
Test (2 files): `HttpWorkerExecutionManagerTest.java`, `HttpWorkerFaultEventHandlerTest.java` — import swaps, Capability.of, function fix

- [ ] **Step 2: Migrate workers-camel**

Production (1 file): `CamelWorkerExecutionManager.java` — 2 import swaps, getter renames
Test (3 files): `CamelWorkerExecutionManagerTest.java`, `CamelWorkerFaultEventHandlerTest.java`, `CasehubProducerTest.java` — import swaps, Capability.of, function fix

- [ ] **Step 3: Migrate workers-github-actions**

Production (1 file): `GitHubActionsWorkerExecutionManager.java` — 2 import swaps, getter renames
Test (2 files): `GitHubActionsWorkerExecutionManagerTest.java`, `GitHubActionsWorkerFaultEventHandlerTest.java` — import swaps, Capability.of, function fix

- [ ] **Step 4: Migrate workers-mcp**

Production (1 file): `McpWorkerExecutionManager.java` — 2 import swaps, getter renames
Test (2 files): `McpWorkerExecutionManagerTest.java`, `McpWorkerFaultEventHandlerTest.java` — import swaps, Capability.of, function fix

- [ ] **Step 5: Migrate workers-script**

Production (1 file): `ScriptWorkerExecutionManager.java` — 2 import swaps, getter renames
Test (2 files): `ScriptWorkerExecutionManagerTest.java`, `ScriptWorkerFaultEventHandlerTest.java` — import swaps, Capability.of, function fix

- [ ] **Step 6: Full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS — all modules compile, all tests pass

- [ ] **Step 7: Commit**

```
git add workers-http/ workers-camel/ workers-github-actions/ workers-mcp/ workers-script/
git commit -m "refactor(#14): migrate per-module imports to worker-api + platform-api governance"
```
