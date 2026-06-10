# workers-github-actions Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `workers-github-actions` module — a CaseHub worker that dispatches case steps to GitHub Actions workflows via `workflow_dispatch` and `repository_dispatch` REST APIs.

**Architecture:** Separate module following the established worker pattern (Constants, EventBusAddresses, FaultPublisher, FaultEventHandler, Provisioner, ExecutionManager, TokenResolver). Fire-and-forget completion — no async completion registry. Prerequisite extraction of `PermanentFaultException`, `RetryAfterException`, and `parseRetryAfter()` to `workers-common` before module creation.

**Tech Stack:** Java 21, Quarkus 3.32.2, Vert.x Mutiny WebClient, Mockito, AssertJ

**Spec:** `docs/superpowers/specs/2026-06-10-casehub-workers-github-actions-design.md` (rev 2)

---

## Task 1: Extract PermanentFaultException to workers-common

**Files:**
- Move: `workers-http/src/main/java/io/casehub/workers/http/PermanentFaultException.java` → `workers-common/src/main/java/io/casehub/workers/common/PermanentFaultException.java`
- Modify: all imports in `workers-http/src/main/java/` and `workers-http/src/test/java/`

- [ ] **Step 1: Create PermanentFaultException in workers-common**

```java
package io.casehub.workers.common;

public class PermanentFaultException extends RuntimeException {
    private final int statusCode;
    public PermanentFaultException(int statusCode, String message) {
        super(message);
        this.statusCode = statusCode;
    }
    public int statusCode() { return statusCode; }
}
```

Write to: `workers-common/src/main/java/io/casehub/workers/common/PermanentFaultException.java`

- [ ] **Step 2: Update imports in workers-http source files**

Use IntelliJ rename refactoring to move `PermanentFaultException` from `io.casehub.workers.http` to `io.casehub.workers.common`. This updates all source and test files in workers-http atomically.

Files affected (source):
- `HttpWorkerExecutionManager.java` — 4 references
- `HttpWorkerFaultEventHandler.java` — 1 reference

Files affected (test):
- `HttpWorkerExecutionManagerTest.java` — 7 references
- `HttpWorkerFaultEventHandlerTest.java` — 2 references

- [ ] **Step 3: Delete the original file from workers-http**

Delete: `workers-http/src/main/java/io/casehub/workers/http/PermanentFaultException.java`

- [ ] **Step 4: Build to verify**

Run: `mvn --batch-mode install -pl workers-common,workers-http -am`
Expected: BUILD SUCCESS — all existing tests pass with updated imports.

- [ ] **Step 5: Commit**

```
feat(workers-common): extract PermanentFaultException from workers-http

Worker-agnostic "don't retry" signal — applies to any worker that talks to
a fallible external API, not just HTTP. The HTTP worker becomes a consumer.

Refs #6
```

---

## Task 2: Extract RetryAfterException to workers-common

**Files:**
- Move: `workers-http/src/main/java/io/casehub/workers/http/RetryAfterException.java` → `workers-common/src/main/java/io/casehub/workers/common/RetryAfterException.java`
- Modify: all imports in `workers-http/src/main/java/` and `workers-http/src/test/java/`

- [ ] **Step 1: Create RetryAfterException in workers-common**

```java
package io.casehub.workers.common;

public class RetryAfterException extends RuntimeException {
    private final long retryAfterMs;
    public RetryAfterException(long retryAfterMs, String message) {
        super(message);
        this.retryAfterMs = retryAfterMs;
    }
    public long retryAfterMs() { return retryAfterMs; }
}
```

Write to: `workers-common/src/main/java/io/casehub/workers/common/RetryAfterException.java`

- [ ] **Step 2: Update imports in workers-http source files**

Use IntelliJ rename refactoring. Files affected:

Source:
- `HttpWorkerExecutionManager.java` — 5 references (submitAsync, parseRetryAfter)
- `HttpWorkerFaultEventHandler.java` — 1 reference

Test:
- `HttpWorkerExecutionManagerTest.java` — 12 references
- `HttpWorkerFaultEventHandlerTest.java` — 2 references

- [ ] **Step 3: Delete the original file from workers-http**

Delete: `workers-http/src/main/java/io/casehub/workers/http/RetryAfterException.java`

- [ ] **Step 4: Build to verify**

Run: `mvn --batch-mode install -pl workers-common,workers-http -am`
Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```
feat(workers-common): extract RetryAfterException from workers-http

Worker-agnostic "retry after delay" signal — applies to any worker that
talks to a rate-limited API. GitHub's API also returns Retry-After on 429.

Refs #6
```

---

## Task 3: Extract parseRetryAfter to WorkerRetrySupport

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerRetrySupport.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Create: `workers-common/src/test/java/io/casehub/workers/common/WorkerRetrySupportParseRetryAfterTest.java`
- Modify: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerExecutionManagerTest.java` (remove migrated tests)

- [ ] **Step 1: Write the failing tests in workers-common**

```java
package io.casehub.workers.common;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class WorkerRetrySupportParseRetryAfterTest {

    @Test
    void integerSeconds() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("60", 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isEqualTo(60000);
    }

    @Test
    void httpDate_inFuture() {
        ZonedDateTime future = ZonedDateTime.now(ZoneOffset.UTC).plusSeconds(120);
        String httpDate = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
            .format(future);
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(httpDate, 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isBetween(115_000L, 125_000L);
    }

    @Test
    void httpDate_inPast() {
        ZonedDateTime past = ZonedDateTime.now(ZoneOffset.UTC).minusSeconds(60);
        String httpDate = DateTimeFormatter
            .ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US)
            .format(past);
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(httpDate, 429, "Too Many Requests");
        assertThat(ex).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) ex).retryAfterMs()).isEqualTo(0);
    }

    @Test
    void unparseable() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("garbage", 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
        assertThat(ex).isInstanceOf(RuntimeException.class);
    }

    @Test
    void nullValue() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter(null, 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void blankValue() {
        RuntimeException ex = WorkerRetrySupport.parseRetryAfter("   ", 429, "Too Many Requests");
        assertThat(ex).isNotInstanceOf(RetryAfterException.class);
    }
}
```

Write to: `workers-common/src/test/java/io/casehub/workers/common/WorkerRetrySupportParseRetryAfterTest.java`

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerRetrySupportParseRetryAfterTest`
Expected: COMPILATION ERROR — `parseRetryAfter` method does not exist on `WorkerRetrySupport`.

- [ ] **Step 3: Add parseRetryAfter to WorkerRetrySupport**

Add to `WorkerRetrySupport.java` after the `computeBackoffDelayMs` method. Add the `HTTP_DATE` constant as a private static field:

```java
private static final DateTimeFormatter HTTP_DATE =
    DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss zzz", Locale.US);

public static RuntimeException parseRetryAfter(String retryAfter, int status, String statusMessage) {
    String message = status + " " + statusMessage;
    if (retryAfter == null || retryAfter.isBlank()) {
        return new RuntimeException(message);
    }
    try {
        long seconds = Long.parseLong(retryAfter.trim());
        return new RetryAfterException(seconds * 1000, message);
    } catch (NumberFormatException ignored) {
        // not an integer
    }
    try {
        ZonedDateTime retryDate = ZonedDateTime.parse(retryAfter.trim(), HTTP_DATE);
        long deltaMs = retryDate.toInstant().toEpochMilli() - System.currentTimeMillis();
        return new RetryAfterException(Math.max(0, deltaMs), message);
    } catch (DateTimeParseException ignored) {
        // unparseable
    }
    return new RuntimeException(message);
}
```

Add imports to `WorkerRetrySupport.java`:
```java
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Locale;
```

- [ ] **Step 4: Run tests — verify they pass**

Run: `mvn --batch-mode test -pl workers-common -Dtest=WorkerRetrySupportParseRetryAfterTest`
Expected: 6 tests PASS.

- [ ] **Step 5: Update HttpWorkerExecutionManager to delegate to WorkerRetrySupport**

In `HttpWorkerExecutionManager.java`:

1. Remove the `HTTP_DATE` field (line 44).
2. Remove the `parseRetryAfter` instance method (lines 226–248).
3. Replace all calls from `parseRetryAfter(...)` to `WorkerRetrySupport.parseRetryAfter(...)` (2 call sites: `handleResponse` and `submitAsync`).
4. Remove unused imports: `java.time.ZonedDateTime`, `java.time.format.DateTimeFormatter`, `java.time.format.DateTimeParseException`, `java.util.Locale`.

- [ ] **Step 6: Remove migrated tests from HttpWorkerExecutionManagerTest**

Remove these test methods from `HttpWorkerExecutionManagerTest.java`:
- `parseRetryAfter_integerSeconds`
- `parseRetryAfter_httpDate_inFuture`
- `parseRetryAfter_httpDate_inPast`
- `parseRetryAfter_unparseable`
- `parseRetryAfter_null`

These are now covered by `WorkerRetrySupportParseRetryAfterTest`.

- [ ] **Step 7: Build to verify everything still passes**

Run: `mvn --batch-mode install -pl workers-common,workers-http -am`
Expected: BUILD SUCCESS — all remaining HTTP tests pass.

- [ ] **Step 8: Commit**

```
feat(workers-common): extract parseRetryAfter to WorkerRetrySupport

Static utility for parsing Retry-After headers (integer seconds and
HTTP-date). GitHub's API also returns Retry-After on 429 — shared by
HTTP and GitHub Actions workers.

Refs #6
```

---

## Task 4: Create workers-github-actions module skeleton

**Files:**
- Create: `workers-github-actions/pom.xml`
- Modify: `pom.xml` (parent — add module + dependencyManagement entry)

- [ ] **Step 1: Create module POM**

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

    <artifactId>casehub-workers-github-actions</artifactId>

    <name>CaseHub Workers :: GitHub Actions</name>
    <description>WorkerProvisioner and WorkerExecutionManager for GitHub Actions — dispatch case steps via workflow_dispatch and repository_dispatch</description>

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
            <groupId>io.smallrye.reactive</groupId>
            <artifactId>smallrye-mutiny-vertx-web-client</artifactId>
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

Write to: `workers-github-actions/pom.xml`

- [ ] **Step 2: Add module to parent POM**

In `pom.xml`, add `<module>workers-github-actions</module>` after `workers-camel` in the `<modules>` section. Add the dependencyManagement entry:

```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-workers-github-actions</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 3: Create package directory with placeholder**

Create directory: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/`
Create directory: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/`

- [ ] **Step 4: Build to verify module compiles**

Run: `mvn --batch-mode compile -pl workers-github-actions -am`
Expected: BUILD SUCCESS (empty module compiles)

- [ ] **Step 5: Commit**

```
chore(workers-github-actions): module skeleton — POM, parent wiring, Jandex

Refs #6
```

---

## Task 5: GitHubActionsWorkerConstants and GitHubActionsWorkerEventBusAddresses

**Files:**
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerConstants.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerEventBusAddresses.java`

- [ ] **Step 1: Create constants**

```java
package io.casehub.workers.githubactions;

public final class GitHubActionsWorkerConstants {
    public static final String WORKER_TYPE = "github-actions";
    public static final String CAPABILITY_WORKFLOW_DISPATCH = "github-actions:workflow-dispatch";
    public static final String CAPABILITY_REPOSITORY_DISPATCH = "github-actions:repository-dispatch";
    private GitHubActionsWorkerConstants() {}
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerConstants.java`

- [ ] **Step 2: Create event bus addresses**

```java
package io.casehub.workers.githubactions;

public final class GitHubActionsWorkerEventBusAddresses {
    public static final String GITHUB_ACTIONS_WORKER_FAULT = "casehub.workers.github-actions.fault";
    private GitHubActionsWorkerEventBusAddresses() {}
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerEventBusAddresses.java`

- [ ] **Step 3: Build to verify**

Run: `mvn --batch-mode compile -pl workers-github-actions -am`
Expected: BUILD SUCCESS

- [ ] **Step 4: Commit**

```
feat(workers-github-actions): constants and event bus addresses

WORKER_TYPE="github-actions", two capability tags, dedicated fault address.

Refs #6
```

---

## Task 6: GitHubActionsTokenResolver — TDD

**Files:**
- Create: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsTokenResolverTest.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsTokenResolver.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.PermanentFaultException;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class GitHubActionsTokenResolverTest {

    @Test
    void globalToken_resolves() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.of("ghp_global");
        resolver.orgTokens = Map.of();

        assertThat(resolver.resolve("any-org")).isEqualTo("ghp_global");
    }

    @Test
    void perOrgToken_takesPrecedence() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.of("ghp_global");
        resolver.orgTokens = Map.of("casehubio", "ghp_casehub");

        assertThat(resolver.resolve("casehubio")).isEqualTo("ghp_casehub");
    }

    @Test
    void perOrgMiss_fallsBackToGlobal() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.of("ghp_global");
        resolver.orgTokens = Map.of("other-org", "ghp_other");

        assertThat(resolver.resolve("casehubio")).isEqualTo("ghp_global");
    }

    @Test
    void noToken_throwsPermanentFault() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.empty();
        resolver.orgTokens = Map.of();

        assertThatThrownBy(() -> resolver.resolve("casehubio"))
            .isInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("casehubio");
    }

    @Test
    void noGlobal_perOrgMiss_throwsPermanentFault() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.empty();
        resolver.orgTokens = Map.of("other-org", "ghp_other");

        assertThatThrownBy(() -> resolver.resolve("casehubio"))
            .isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void hasToken_globalExists() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.of("ghp_global");
        resolver.orgTokens = Map.of();

        assertThat(resolver.hasToken()).isTrue();
    }

    @Test
    void hasToken_noGlobal() {
        GitHubActionsTokenResolver resolver = new GitHubActionsTokenResolver();
        resolver.globalToken = Optional.empty();
        resolver.orgTokens = Map.of();

        assertThat(resolver.hasToken()).isFalse();
    }
}
```

Write to: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsTokenResolverTest.java`

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsTokenResolverTest`
Expected: COMPILATION ERROR — class does not exist.

- [ ] **Step 3: Write the implementation**

```java
package io.casehub.workers.githubactions;

import io.casehub.workers.common.PermanentFaultException;
import jakarta.enterprise.context.ApplicationScoped;
import java.util.Map;
import java.util.Optional;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class GitHubActionsTokenResolver {

    @ConfigProperty(name = "casehub.workers.github-actions.token")
    Optional<String> globalToken;

    @ConfigProperty(name = "casehub.workers.github-actions.tokens", defaultValue = "")
    Map<String, String> orgTokens;

    @ConfigProperty(name = "casehub.workers.github-actions.api-base-url",
                    defaultValue = "https://api.github.com")
    String apiBaseUrl;

    public String resolve(String owner) {
        String orgToken = orgTokens.get(owner);
        if (orgToken != null && !orgToken.isBlank()) {
            return orgToken;
        }
        return globalToken
            .filter(t -> !t.isBlank())
            .orElseThrow(() -> new PermanentFaultException(0,
                "No GitHub token configured for org '" + owner
                    + "' and no global fallback (casehub.workers.github-actions.token)"));
    }

    public boolean hasToken() {
        return globalToken.isPresent() && !globalToken.get().isBlank();
    }

    public String apiBaseUrl() {
        return apiBaseUrl;
    }
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsTokenResolver.java`

- [ ] **Step 4: Run tests — verify they pass**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsTokenResolverTest`
Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```
feat(workers-github-actions): GitHubActionsTokenResolver — per-org + global PAT resolution

TDD: 7 tests. Per-org override → global fallback → PermanentFaultException.

Refs #6
```

---

## Task 7: GitHubActionsWorkerFaultPublisher

**Files:**
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultPublisher.java`

- [ ] **Step 1: Write the fault publisher**

```java
package io.casehub.workers.githubactions;

import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class GitHubActionsWorkerFaultPublisher {

    @Inject
    EventBus eventBus;

    public void fault(WorkerCorrelationContext ctx, Capability capability,
                      Long eventLogId, Throwable cause) {
        eventBus.publish(GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT,
            new WorkflowExecutionFailed(
                ctx.caseInstance(), ctx.worker(), capability,
                ctx.idempotency(), eventLogId.toString(), cause));
    }
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultPublisher.java`

- [ ] **Step 2: Build to verify**

Run: `mvn --batch-mode compile -pl workers-github-actions -am`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```
feat(workers-github-actions): fault publisher — routes failures to dedicated address

Refs #6
```

---

## Task 8: GitHubActionsWorkerFaultEventHandler — TDD

**Files:**
- Create: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultEventHandlerTest.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultEventHandler.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.api.model.BackoffStrategy;
import io.casehub.api.model.Capability;
import io.casehub.api.model.ExecutionPolicy;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.testing.WorkerTestSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class GitHubActionsWorkerFaultEventHandlerTest {

    private GitHubActionsWorkerFaultEventHandler handler;
    private WorkerRetrySupport retrySupport;
    private WorkerExecutionManager workerExecutionManager;
    private Vertx vertx;
    private EventLogRepository eventLogRepository;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @BeforeEach
    void setUp() {
        retrySupport = mock(WorkerRetrySupport.class);
        workerExecutionManager = mock(WorkerExecutionManager.class);
        vertx = mock(Vertx.class);
        eventLogRepository = mock(EventLogRepository.class);

        handler = new GitHubActionsWorkerFaultEventHandler();
        handler.retrySupport = retrySupport;
        handler.workerExecutionManager = workerExecutionManager;
        handler.vertx = vertx;
        handler.eventLogRepository = eventLogRepository;

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());
    }

    @Test
    void persistFailureLog_calledForEveryFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("boom");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(3L));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).persistFailureLog(instance, worker, "hash-1", "boom", instance.tenancyId);
    }

    @Test
    void permanentFault_publishesExhaustedImmediately() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        PermanentFaultException cause = new PermanentFaultException(400, "Bad Request");

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(retrySupport, never()).countFailedAttempts(any(), any(), any(), any());
    }

    @Test
    void retryAfterException_usesRetryAfterDelay() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RetryAfterException cause = new RetryAfterException(60000, "422 workflow_dispatch cache");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("owner", "casehubio");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(vertx).setTimer(delayCaptor.capture(), any(Handler.class));
        assertThat(delayCaptor.getValue()).isEqualTo(60000L);
    }

    @Test
    void normalFault_usesConfiguredBackoff() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        worker.setExecutionPolicy(ep);
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("500 Internal Server Error");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("owner", "casehubio");
        eventLog.setPayload(payload);
        when(eventLogRepository.findById(42L, instance.tenancyId))
            .thenReturn(Uni.createFrom().item(eventLog));

        when(vertx.setTimer(anyLong(), any(Handler.class))).thenAnswer(invocation -> {
            Handler<Long> timerHandler = invocation.getArgument(1);
            timerHandler.handle(1L);
            return 1L;
        });

        when(workerExecutionManager.submit(anyLong(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().voidItem());

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        ArgumentCaptor<Long> delayCaptor = ArgumentCaptor.forClass(Long.class);
        verify(vertx).setTimer(delayCaptor.capture(), any(Handler.class));
        assertThat(delayCaptor.getValue()).isEqualTo(10000L);
    }

    @Test
    void exhausted_afterMaxAttempts() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        worker.setExecutionPolicy(ep);
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("500 error");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(3L));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport).publishRetriesExhausted(instance.getUuid(), "w1", "hash-1");
        verify(vertx, never()).setTimer(anyLong(), any(Handler.class));
    }

    @Test
    void faultHandlingFailure_logsAndRecovers() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w1", "cap");
        Capability cap = WorkerTestSupport.testCapability("cap");
        RuntimeException cause = new RuntimeException("error");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("DB down")));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any());
        verify(workerExecutionManager, never()).submit(anyLong(), any(), any(), any(), any());
    }
}
```

Write to: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultEventHandlerTest.java`

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsWorkerFaultEventHandlerTest`
Expected: COMPILATION ERROR — class does not exist.

- [ ] **Step 3: Write the implementation**

```java
package io.casehub.workers.githubactions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.RetryPolicy;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.spi.EventLogRepository;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerRetrySupport;
import io.quarkus.vertx.ConsumeEvent;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Vertx;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class GitHubActionsWorkerFaultEventHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(GitHubActionsWorkerFaultEventHandler.class);

    @Inject WorkerRetrySupport retrySupport;
    @Inject WorkerExecutionManager workerExecutionManager;
    @Inject Vertx vertx;
    @Inject EventLogRepository eventLogRepository;

    @ConsumeEvent(value = GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT, blocking = true)
    public Uni<Void> onFault(WorkflowExecutionFailed event) {
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
                    .flatMap(ignored -> workerExecutionManager.submit(
                        Long.parseLong(event.eventLogId()),
                        event.caseInstance(), event.worker(), event.capability(), inputData));
            });
    }
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerFaultEventHandler.java`

- [ ] **Step 4: Run tests — verify they pass**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsWorkerFaultEventHandlerTest`
Expected: 7 tests PASS.

- [ ] **Step 5: Commit**

```
feat(workers-github-actions): fault event handler — retry with RetryAfterException support

TDD: 7 tests. PermanentFault skips retry, RetryAfterException uses retryAfterMs,
standard backoff otherwise. No emitOn — WebClient is event-loop native.

Refs #6
```

---

## Task 9: GitHubActionsWorkerExecutionManager — TDD

**Files:**
- Create: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManagerTest.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManager.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.casehub.workers.testing.WorkerTestSupport;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class GitHubActionsWorkerExecutionManagerTest {

    private GitHubActionsWorkerExecutionManager manager;
    private GitHubActionsTokenResolver tokenResolver;
    private GitHubActionsWorkerFaultPublisher faultPublisher;
    private WorkflowCompletionPublisher completionPublisher;
    private WebClient webClient;
    private HttpRequest<Buffer> request;

    @BeforeEach
    void setUp() {
        tokenResolver = mock(GitHubActionsTokenResolver.class);
        faultPublisher = mock(GitHubActionsWorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        webClient = mock(WebClient.class);
        request = mock(HttpRequest.class);

        manager = new GitHubActionsWorkerExecutionManager();
        manager.tokenResolver = tokenResolver;
        manager.faultPublisher = faultPublisher;
        manager.completionPublisher = completionPublisher;
        manager.objectMapper = new ObjectMapper();
        manager.webClient = webClient;

        when(tokenResolver.resolve(anyString())).thenReturn("ghp_test_token");
        when(tokenResolver.apiBaseUrl()).thenReturn("https://api.github.com");
        when(webClient.requestAbs(any(HttpMethod.class), anyString())).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
    }

    // --- workflow-dispatch 204 ---

    @Test
    void workflowDispatch_204_completesWithOutput() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        Map<String, Object> inputData = Map.of(
            "owner", "casehubio", "repo", "devtown",
            "workflow_id", "ci.yml", "ref", "main");

        manager.submit(1L, instance, worker, cap, inputData).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue())
            .containsEntry("dispatched", true)
            .containsEntry("owner", "casehubio")
            .containsEntry("repo", "devtown");
        verify(faultPublisher, never()).fault(any(), any(), anyLong(), any());
    }

    @Test
    void workflowDispatch_correctUrl() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        verify(webClient).requestAbs(HttpMethod.POST,
            "https://api.github.com/repos/casehubio/devtown/actions/workflows/ci.yml/dispatches");
    }

    @Test
    void workflowDispatch_requestBody_withInputs() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        Map<String, Object> inputData = new HashMap<>();
        inputData.put("owner", "casehubio");
        inputData.put("repo", "devtown");
        inputData.put("workflow_id", "ci.yml");
        inputData.put("ref", "main");
        inputData.put("inputs", Map.of("environment", "staging"));

        manager.submit(1L, instance, worker, cap, inputData).await().indefinitely();

        ArgumentCaptor<Object> bodyCaptor = ArgumentCaptor.forClass(Object.class);
        verify(request).sendJson(bodyCaptor.capture());
        Map<String, Object> body = (Map<String, Object>) bodyCaptor.getValue();
        assertThat(body).containsEntry("ref", "main");
        assertThat(body).containsKey("inputs");
        assertThat((Map<String, Object>) body.get("inputs")).containsEntry("environment", "staging");
    }

    @Test
    void workflowDispatch_requestBody_withoutInputs() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Object> bodyCaptor = ArgumentCaptor.forClass(Object.class);
        verify(request).sendJson(bodyCaptor.capture());
        Map<String, Object> body = (Map<String, Object>) bodyCaptor.getValue();
        assertThat(body).containsEntry("ref", "main");
        assertThat(body).doesNotContainKey("inputs");
    }

    // --- repository-dispatch 204 ---

    @Test
    void repositoryDispatch_204_completesWithOutput() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"))
            .await().indefinitely();

        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), any());
    }

    @Test
    void repositoryDispatch_correctUrl() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"))
            .await().indefinitely();

        verify(webClient).requestAbs(HttpMethod.POST,
            "https://api.github.com/repos/casehubio/devtown/dispatches");
    }

    // --- Missing required fields ---

    @Test
    void workflowDispatch_missingOwner_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("repo", "devtown", "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void workflowDispatch_missingRef_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown", "workflow_id", "ci.yml"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void repositoryDispatch_missingEventType_permanentFault() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    // --- Error responses ---

    @Test
    void workflowDispatch_422_retryAfter60s() {
        HttpResponse<Buffer> response = mockResponse(422);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(60000L);
    }

    @Test
    void repositoryDispatch_422_permanentFault() {
        HttpResponse<Buffer> response = mockResponse(422);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "event_type", "upstream-published"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void response_429_withRetryAfter() {
        HttpResponse<Buffer> response = mockResponse(429);
        when(response.getHeader("Retry-After")).thenReturn("30");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(30000L);
    }

    @Test
    void response_401_permanentFault() {
        HttpResponse<Buffer> response = mockResponse(401);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void response_404_permanentFault() {
        HttpResponse<Buffer> response = mockResponse(404);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void response_500_retryableFault() {
        HttpResponse<Buffer> response = mockResponse(500);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isNotInstanceOf(PermanentFaultException.class)
            .isNotInstanceOf(RetryAfterException.class);
    }

    // --- Custom API base URL ---

    @Test
    void customApiBaseUrl_usedInUrl() {
        when(tokenResolver.apiBaseUrl()).thenReturn("https://github.example.com/api/v3");
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        verify(webClient).requestAbs(HttpMethod.POST,
            "https://github.example.com/api/v3/repos/casehubio/devtown/actions/workflows/ci.yml/dispatches");
    }

    // --- Headers ---

    @Test
    void noCasehubHeaders_onOutboundRequest() {
        HttpResponse<Buffer> response = mockResponse(204);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w",
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);
        Capability cap = WorkerTestSupport.testCapability(
            GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH);

        manager.submit(1L, instance, worker, cap,
            Map.of("owner", "casehubio", "repo", "devtown",
                   "workflow_id", "ci.yml", "ref", "main"))
            .await().indefinitely();

        verify(request).putHeader("Authorization", "Bearer ghp_test_token");
        verify(request).putHeader("Accept", "application/vnd.github+json");
        verify(request).putHeader("X-GitHub-Api-Version", "2022-11-28");
        verify(request, never()).putHeader(eq("X-CaseHub-Idempotency"), anyString());
        verify(request, never()).putHeader(eq("X-CaseHub-Case-Id"), anyString());
    }

    // --- SPI methods ---

    @Test
    void schedulePersistedEvent_returnsVoid() {
        assertThat(manager.schedulePersistedEvent(new EventLog()).await().indefinitely()).isNull();
    }

    @Test
    void getActiveWorkCount_returnsZero() {
        assertThat(manager.getActiveWorkCount("any")).isEqualTo(0);
    }

    // --- Helper ---

    private HttpResponse<Buffer> mockResponse(int status) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.statusMessage()).thenReturn("Status " + status);
        when(response.getHeader(anyString())).thenReturn(null);
        return response;
    }
}
```

Write to: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManagerTest.java`

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsWorkerExecutionManagerTest`
Expected: COMPILATION ERROR — class does not exist.

- [ ] **Step 3: Write the implementation**

```java
package io.casehub.workers.githubactions;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.RetryAfterException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class GitHubActionsWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(GitHubActionsWorkerExecutionManager.class);

    @Inject GitHubActionsTokenResolver tokenResolver;
    @Inject GitHubActionsWorkerFaultPublisher faultPublisher;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject io.vertx.mutiny.core.Vertx vertx;
    @Inject ObjectMapper objectMapper;

    WebClient webClient;

    @PostConstruct
    void init() {
        this.webClient = WebClient.create(vertx);
    }

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        String capTag = capability.getName();
        boolean isWorkflowDispatch = GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH.equals(capTag);

        String owner = stringField(inputData, "owner");
        String repo = stringField(inputData, "repo");
        if (owner == null || repo == null) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData),
                capability, eventLogId,
                new PermanentFaultException(0, "Missing required inputData: owner, repo"));
            return Uni.createFrom().voidItem();
        }

        String url;
        Map<String, Object> body;

        if (isWorkflowDispatch) {
            String workflowId = stringField(inputData, "workflow_id");
            String ref = stringField(inputData, "ref");
            if (workflowId == null || ref == null) {
                faultPublisher.fault(buildCtx(instance, worker, capability, inputData),
                    capability, eventLogId,
                    new PermanentFaultException(0,
                        "Missing required inputData for workflow-dispatch: workflow_id, ref"));
                return Uni.createFrom().voidItem();
            }
            url = tokenResolver.apiBaseUrl() + "/repos/" + owner + "/" + repo
                + "/actions/workflows/" + workflowId + "/dispatches";
            body = new LinkedHashMap<>();
            body.put("ref", ref);
            Object inputs = inputData.get("inputs");
            if (inputs != null) {
                body.put("inputs", inputs);
            }
        } else {
            String eventType = stringField(inputData, "event_type");
            if (eventType == null) {
                faultPublisher.fault(buildCtx(instance, worker, capability, inputData),
                    capability, eventLogId,
                    new PermanentFaultException(0,
                        "Missing required inputData for repository-dispatch: event_type"));
                return Uni.createFrom().voidItem();
            }
            url = tokenResolver.apiBaseUrl() + "/repos/" + owner + "/" + repo + "/dispatches";
            body = new LinkedHashMap<>();
            body.put("event_type", eventType);
            Object clientPayload = inputData.get("client_payload");
            if (clientPayload != null) {
                body.put("client_payload", clientPayload);
            }
        }

        String token;
        try {
            token = tokenResolver.resolve(owner);
        } catch (PermanentFaultException e) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData),
                capability, eventLogId, e);
            return Uni.createFrom().voidItem();
        }

        WorkerCorrelationContext ctx = buildCtx(instance, worker, capability, inputData);

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, url);
        request.putHeader("Authorization", "Bearer " + token);
        request.putHeader("Accept", "application/vnd.github+json");
        request.putHeader("X-GitHub-Api-Version", "2022-11-28");

        return request.sendJson(body)
            .flatMap(response -> {
                int status = response.statusCode();
                if (status >= 200 && status < 300) {
                    completionPublisher.complete(ctx, Map.of(
                        "dispatched", true, "owner", owner, "repo", repo));
                    return Uni.createFrom().<Void>voidItem();
                }
                if (status == 422) {
                    if (isWorkflowDispatch) {
                        throw new RetryAfterException(60_000,
                            "422 — workflow_dispatch trigger may be cached (GE-20260426-805acb)");
                    } else {
                        throw new PermanentFaultException(status,
                            status + " " + response.statusMessage());
                    }
                }
                if (status == 429) {
                    throw WorkerRetrySupport.parseRetryAfter(
                        response.getHeader("Retry-After"), status, response.statusMessage());
                }
                if (status >= 400 && status < 500) {
                    throw new PermanentFaultException(status,
                        status + " " + response.statusMessage());
                }
                throw new RuntimeException(status + " " + response.statusMessage());
            })
            .onFailure().recoverWithUni(t -> {
                faultPublisher.fault(ctx, capability, eventLogId, t);
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

    private WorkerCorrelationContext buildCtx(CaseInstance instance, Worker worker,
                                              Capability capability,
                                              Map<String, Object> inputData) {
        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        return new WorkerCorrelationContext(instance, worker, idempotency, instance.tenancyId);
    }

    private static String stringField(Map<String, Object> data, String key) {
        Object val = data.get(key);
        if (val == null) return null;
        String s = val.toString();
        return s.isBlank() ? null : s;
    }
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsWorkerExecutionManager.java`

- [ ] **Step 4: Run tests — verify they pass**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsWorkerExecutionManagerTest`
Expected: 20 tests PASS.

- [ ] **Step 5: Commit**

```
feat(workers-github-actions): execution manager — workflow_dispatch + repository_dispatch

TDD: 20 tests. Fire-and-forget on 204. 422 retryable (60s) for workflow-dispatch,
permanent for repository-dispatch. No CaseHub headers — GitHub API ignores them.

Refs #6
```

---

## Task 10: GitHubActionsReactiveWorkerProvisioner — TDD

**Files:**
- Create: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisionerTest.java`
- Create: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisioner.java`

- [ ] **Step 1: Write the failing tests**

```java
package io.casehub.workers.githubactions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GitHubActionsReactiveWorkerProvisionerTest {

    private GitHubActionsReactiveWorkerProvisioner provisioner;
    private GitHubActionsTokenResolver tokenResolver;

    @BeforeEach
    void setUp() {
        tokenResolver = mock(GitHubActionsTokenResolver.class);
        provisioner = new GitHubActionsReactiveWorkerProvisioner();
        provisioner.tokenResolver = tokenResolver;
    }

    @Test
    void getCapabilities_returnsBothTags() {
        assertThat(provisioner.getCapabilities().await().indefinitely())
            .containsExactlyInAnyOrder(
                GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH,
                GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);
    }

    @Test
    void provision_workflowDispatch_succeeds() {
        when(tokenResolver.hasToken()).thenReturn(true);
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        ProvisionResult result = provisioner.provision(
            Set.of(GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH), ctx)
            .await().indefinitely();
        assertThat(result).isNotNull();
    }

    @Test
    void provision_repositoryDispatch_succeeds() {
        when(tokenResolver.hasToken()).thenReturn(true);
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        ProvisionResult result = provisioner.provision(
            Set.of(GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH), ctx)
            .await().indefinitely();
        assertThat(result).isNotNull();
    }

    @Test
    void provision_unknownCapability_throws() {
        when(tokenResolver.hasToken()).thenReturn(true);
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        assertThatThrownBy(() -> provisioner.provision(
            Set.of("unknown-capability"), ctx).await().indefinitely())
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void provision_noToken_throws() {
        when(tokenResolver.hasToken()).thenReturn(false);
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        assertThatThrownBy(() -> provisioner.provision(
            Set.of(GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH), ctx)
            .await().indefinitely())
            .isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("token");
    }

    @Test
    void terminate_returnsVoid() {
        assertThat(provisioner.terminate("any").await().indefinitely()).isNull();
    }
}
```

Write to: `workers-github-actions/src/test/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisionerTest.java`

- [ ] **Step 2: Run tests — verify they fail**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsReactiveWorkerProvisionerTest`
Expected: COMPILATION ERROR — class does not exist.

- [ ] **Step 3: Write the implementation**

```java
package io.casehub.workers.githubactions;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.api.spi.ReactiveWorkerProvisioner;
import io.casehub.workers.common.WorkerProvisioningException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class GitHubActionsReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    private static final Set<String> CAPABILITIES = Set.of(
        GitHubActionsWorkerConstants.CAPABILITY_WORKFLOW_DISPATCH,
        GitHubActionsWorkerConstants.CAPABILITY_REPOSITORY_DISPATCH);

    @Inject
    GitHubActionsTokenResolver tokenResolver;

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        boolean match = capabilities.stream().anyMatch(CAPABILITIES::contains);
        if (!match) {
            throw new WorkerProvisioningException(
                "No matching GitHub Actions capability. Supported: " + CAPABILITIES);
        }
        if (!tokenResolver.hasToken()) {
            throw new WorkerProvisioningException(
                "GitHub Actions worker requires a configured token "
                    + "(casehub.workers.github-actions.token)");
        }
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        return Uni.createFrom().item(CAPABILITIES);
    }
}
```

Write to: `workers-github-actions/src/main/java/io/casehub/workers/githubactions/GitHubActionsReactiveWorkerProvisioner.java`

- [ ] **Step 4: Run tests — verify they pass**

Run: `mvn --batch-mode test -pl workers-github-actions -Dtest=GitHubActionsReactiveWorkerProvisionerTest`
Expected: 5 tests PASS.

- [ ] **Step 5: Commit**

```
feat(workers-github-actions): provisioner — static capabilities, token validation

TDD: 5 tests. Validates capability match and global token availability.
Per-org resolution deferred to dispatch time (ProvisionContext has no inputData).

Refs #6
```

---

## Task 11: Full build and CLAUDE.md update

**Files:**
- Modify: `CLAUDE.md` (add workers-github-actions to module table and key types)

- [ ] **Step 1: Full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS — all modules compile, all tests pass.

- [ ] **Step 2: Update CLAUDE.md module table**

Add row to the Module Structure table:

```
| `workers-github-actions` | `casehub-workers-github-actions` | `io.casehub.workers.githubactions` | GitHub Actions worker — workflow_dispatch + repository_dispatch |
```

- [ ] **Step 3: Add workers-github-actions key types to CLAUDE.md**

Add section after `## workers-http Key Types`:

```markdown
## workers-github-actions Key Types

| Type | Purpose |
|------|---------|
| `GitHubActionsWorkerConstants.WORKER_TYPE = "github-actions"` | workerType discriminator |
| `GitHubActionsWorkerEventBusAddresses.GITHUB_ACTIONS_WORKER_FAULT` | Separate fault address from HTTP and Camel |
| `GitHubActionsTokenResolver` | Per-org + global PAT resolution from config properties |
| `GitHubActionsWorkerExecutionManager` | Dispatches via Vert.x WebClient — fire-and-forget on 204 |
| `GitHubActionsWorkerFaultPublisher` | Fires `WorkflowExecutionFailed` on `GITHUB_ACTIONS_WORKER_FAULT` |
| `GitHubActionsWorkerFaultEventHandler` | `@ConsumeEvent(GITHUB_ACTIONS_WORKER_FAULT, blocking=true)` — 422 on workflow-dispatch retryable (60s), 422 on repository-dispatch permanent |
| `GitHubActionsReactiveWorkerProvisioner` | Capability probe — validates tags and token availability |
```

- [ ] **Step 4: Update Key Rules in CLAUDE.md**

Add bullet: `- HTTP 422 on workflow-dispatch throws `RetryAfterException(60_000)` — workflow_dispatch trigger caching (GE-20260426-805acb). HTTP 422 on repository-dispatch throws `PermanentFaultException` — malformed request.`

- [ ] **Step 5: Note extraction in workers-common Key Types**

Add to workers-common Key Types table:
```
| `PermanentFaultException` | Worker-agnostic "don't retry" signal — moved from workers-http |
| `RetryAfterException` | Worker-agnostic "retry after delay" signal — moved from workers-http |
```

- [ ] **Step 6: Commit**

```
docs: update CLAUDE.md — workers-github-actions module, extraction to workers-common

Refs #6
```

---

## Task 12: Commit untracked workers-camel/README.md

The handover noted this file sitting untracked on project main.

- [ ] **Step 1: Check if file exists on the branch**

Check `git status` for the file.

- [ ] **Step 2: Commit if present**

```
docs: workers-camel README

Refs #6
```
