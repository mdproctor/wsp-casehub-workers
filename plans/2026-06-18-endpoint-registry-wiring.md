# EndpointRegistry Wiring Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `EndpointRegistry` SPI from `casehub-platform-api` into workers-http and workers-mcp as Tier 3 endpoint resolution, making `WorkerCapabilityResolver` tenancy-aware across all four resolver implementations.

**Architecture:** Change `WorkerCapabilityResolver<T>` to accept `tenancyId` on `resolve()` and `firstMatch()`. HTTP and MCP resolvers inject `EndpointRegistry` and query it as Tier 3 (after SPI beans and config). Camel and Script pass tenancyId through without using it. The registry handles tenant → platform-global fallback internally — resolvers make a single call.

**Tech Stack:** Java 21, Quarkus CDI, casehub-platform-api (EndpointRegistry, EndpointDescriptor, Path, EndpointPropertyKeys, EndpointProtocol, TenancyConstants), Mutiny, JUnit 5, AssertJ, Mockito

**Spec:** `docs/superpowers/specs/2026-06-18-endpoint-registry-wiring-design.md`

---

### Task 1: WorkerCapabilityResolver interface change

**Files:**
- Modify: `workers-common/src/main/java/io/casehub/workers/common/WorkerCapabilityResolver.java`

- [ ] **Step 1: Update the interface**

```java
package io.casehub.workers.common;

import java.util.Optional;
import java.util.Set;

public interface WorkerCapabilityResolver<T> {
    T resolve(String capabilityTag, String tenancyId);
    Optional<String> firstMatch(Set<String> capabilities, String tenancyId);
    Set<String> capabilities();
}
```

- [ ] **Step 2: Verify compilation fails for all implementors**

Run: `mvn --batch-mode compile -pl workers-common 2>&1 | tail -5`
Expected: BUILD SUCCESS (interface-only change — no implementors in this module)

Run: `mvn --batch-mode compile 2>&1 | grep "error:" | head -20`
Expected: Compilation errors in workers-http, workers-mcp, workers-camel, workers-script (4 resolvers, 4 execution managers, 4 provisioners — 12 compile errors)

- [ ] **Step 3: Commit the interface change**

```
git add workers-common/src/main/java/io/casehub/workers/common/WorkerCapabilityResolver.java
git commit -m "feat(#12): add tenancyId to WorkerCapabilityResolver resolve() and firstMatch()

Breaking change — all four implementations must update.

Refs #12"
```

---

### Task 2: Camel resolver, execution manager, provisioner — tenancyId passthrough

**Files:**
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelCapabilityResolver.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelWorkerExecutionManager.java`
- Modify: `workers-camel/src/main/java/io/casehub/workers/camel/CamelReactiveWorkerProvisioner.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelCapabilityResolverTest.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelWorkerExecutionManagerTest.java`
- Test: `workers-camel/src/test/java/io/casehub/workers/camel/CamelReactiveWorkerProvisionerTest.java`

Camel doesn't use EndpointRegistry — routes are code-defined. The `tenancyId` parameter is accepted and ignored.

- [ ] **Step 1: Update CamelCapabilityResolver**

Change `resolve(String capabilityTag)` to `resolve(String capabilityTag, String tenancyId)`. Change `firstMatch(Set<String> capabilities)` to `firstMatch(Set<String> capabilities, String tenancyId)`. The method bodies are unchanged — tenancyId is not used.

```java
@Override
public String resolve(String capabilityTag, String tenancyId) {
    String uri = resolvedRoutes.get(capabilityTag);
    if (uri == null) {
        throw WorkerProvisioningException.noRouteFound(capabilityTag);
    }
    return uri;
}

@Override
public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
    return capabilities.stream()
        .filter(resolvedRoutes::containsKey)
        .findFirst();
}
```

- [ ] **Step 2: Update CamelWorkerExecutionManager**

Line 47: change `camelCapabilityResolver.resolve(capability.getName())` to `camelCapabilityResolver.resolve(capability.getName(), instance.tenancyId)`.

Line 63: change `camelCapabilityResolver.exchangePattern(capability.getName())` — this method is NOT on the interface, so it doesn't need tenancyId. No change to this line.

- [ ] **Step 3: Update CamelReactiveWorkerProvisioner**

```java
@Override
public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
    String tenancyId = TenancyConstants.PLATFORM_TENANT_ID; // engine#530: context.tenancyId()
    String capability = camelCapabilityResolver.firstMatch(capabilities, tenancyId)
        .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
    camelCapabilityResolver.resolve(capability, tenancyId);
    return Uni.createFrom().item(ProvisionResult.empty());
}
```

Add import: `import io.casehub.platform.api.identity.TenancyConstants;`

- [ ] **Step 4: Update CamelCapabilityResolverTest**

Every call to `resolver.resolve("tag")` becomes `resolver.resolve("tag", "tenant-1")`. Every call to `resolver.firstMatch(set)` becomes `resolver.firstMatch(set, "tenant-1")`. The tenancyId value is arbitrary — Camel ignores it.

- [ ] **Step 5: Update CamelWorkerExecutionManagerTest**

Line 49: change `when(resolver.resolve("missing")).thenThrow(...)` to `when(resolver.resolve(eq("missing"), anyString())).thenThrow(...)`.

- [ ] **Step 6: Update CamelReactiveWorkerProvisionerTest**

Update mock calls for `firstMatch()` and `resolve()` to include tenancyId parameter. Use `anyString()` for the tenancyId matcher.

- [ ] **Step 7: Run tests**

Run: `mvn --batch-mode test -pl workers-camel`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```
git add workers-camel/
git commit -m "feat(#12): update Camel resolver, execution manager, provisioner for tenancyId

Passthrough only — Camel routes are code-defined, tenancyId is not used.

Refs #12"
```

---

### Task 3: Script resolver, execution manager, provisioner — tenancyId passthrough

**Files:**
- Modify: `workers-script/src/main/java/io/casehub/workers/script/ScriptDefinitionResolver.java`
- Modify: `workers-script/src/main/java/io/casehub/workers/script/ScriptWorkerExecutionManager.java`
- Modify: `workers-script/src/main/java/io/casehub/workers/script/ScriptReactiveWorkerProvisioner.java`
- Test: `workers-script/src/test/java/io/casehub/workers/script/ScriptDefinitionResolverTest.java`
- Test: `workers-script/src/test/java/io/casehub/workers/script/ScriptWorkerExecutionManagerTest.java`
- Test: `workers-script/src/test/java/io/casehub/workers/script/ScriptReactiveWorkerProvisionerTest.java`

Script doesn't use EndpointRegistry — subprocesses are local. The `tenancyId` parameter is accepted and ignored.

- [ ] **Step 1: Update ScriptDefinitionResolver**

Change `resolve(String capabilityTag)` to `resolve(String capabilityTag, String tenancyId)`. Change `firstMatch(Set<String> capabilities)` to `firstMatch(Set<String> capabilities, String tenancyId)`. Method bodies unchanged.

```java
@Override
public ScriptDefinition resolve(String capabilityTag, String tenancyId) {
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
public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
    return capabilities.stream()
        .filter(cap -> {
            if (!cap.startsWith(TAG_PREFIX)) return false;
            return definitions.containsKey(cap.substring(TAG_PREFIX.length()));
        })
        .findFirst();
}
```

- [ ] **Step 2: Update ScriptWorkerExecutionManager**

Line 73: change `scriptDefinitionResolver.resolve(capability.getName())` to `scriptDefinitionResolver.resolve(capability.getName(), instance.tenancyId)`.

- [ ] **Step 3: Update ScriptReactiveWorkerProvisioner**

```java
@Override
public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
    String tenancyId = TenancyConstants.PLATFORM_TENANT_ID; // engine#530: context.tenancyId()
    String capability = scriptDefinitionResolver.firstMatch(capabilities, tenancyId)
        .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
    scriptDefinitionResolver.resolve(capability, tenancyId);
    return Uni.createFrom().item(ProvisionResult.empty());
}
```

Add import: `import io.casehub.platform.api.identity.TenancyConstants;`

- [ ] **Step 4: Update ScriptDefinitionResolverTest**

Every call to `resolver.resolve("script:tag")` becomes `resolver.resolve("script:tag", "tenant-1")`. Every `firstMatch(set)` becomes `firstMatch(set, "tenant-1")`.

- [ ] **Step 5: Update ScriptWorkerExecutionManagerTest**

The test uses a real `ScriptDefinitionResolver` (not mocked). The `resolve()` call is inside `submit()` which uses `instance.tenancyId`. No mock update needed — the real resolver ignores tenancyId. But verify the test still compiles and passes.

- [ ] **Step 6: Update ScriptReactiveWorkerProvisionerTest**

Update mock calls for `firstMatch()` and `resolve()` to include tenancyId parameter.

- [ ] **Step 7: Run tests**

Run: `mvn --batch-mode test -pl workers-script`
Expected: All tests pass.

- [ ] **Step 8: Commit**

```
git add workers-script/
git commit -m "feat(#12): update Script resolver, execution manager, provisioner for tenancyId

Passthrough only — scripts are local subprocesses, tenancyId is not used.

Refs #12"
```

---

### Task 4: HTTP EndpointResolver — Tier 3 wiring (TDD)

**Files:**
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java`
- Test: `workers-http/src/test/java/io/casehub/workers/http/HttpEndpointResolverTest.java`

- [ ] **Step 1: Write failing test — Tier 3 registry hit**

Add to `HttpEndpointResolverTest.java`:

```java
import io.casehub.platform.api.endpoints.EndpointCapability;
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.endpoints.EndpointType;
import io.casehub.platform.api.path.Path;
import java.util.Optional;

// ... in the test class:

private static EndpointRegistry stubRegistry(String capabilityTag, EndpointDescriptor descriptor) {
    return new EndpointRegistry() {
        @Override public void register(EndpointDescriptor endpoint) {}
        @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
            if (path.equals(Path.of("http", capabilityTag))) {
                return Optional.of(descriptor);
            }
            return Optional.empty();
        }
        @Override public java.util.List<EndpointDescriptor> discover(
                io.casehub.platform.api.endpoints.EndpointQuery query) { return java.util.List.of(); }
        @Override public void deregister(Path path, String tenancyId) {}
    };
}

private static EndpointRegistry emptyRegistry() {
    return new EndpointRegistry() {
        @Override public void register(EndpointDescriptor endpoint) {}
        @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
            return Optional.empty();
        }
        @Override public java.util.List<EndpointDescriptor> discover(
                io.casehub.platform.api.endpoints.EndpointQuery query) { return java.util.List.of(); }
        @Override public void deregister(Path path, String tenancyId) {}
    };
}

@Test
void tier3_registryHit_resolvesFromRegistry() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("http", "registry-endpoint"),
        "tenant-1",
        EndpointType.WORKER,
        EndpointProtocol.HTTP,
        Map.of(EndpointPropertyKeys.URL, "https://registry.example.com/dispatch",
               "method", "PUT",
               "mode", "ASYNC",
               "timeout-seconds", "45",
               "headers.X-Registry", "true"),
        null,
        Set.of(EndpointCapability.DISPATCH));

    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(), Map.of(), 30,
        stubRegistry("registry-endpoint", descriptor));

    ResolvedEndpoint endpoint = resolver.resolve("registry-endpoint", "tenant-1");
    assertThat(endpoint.url()).isEqualTo("https://registry.example.com/dispatch");
    assertThat(endpoint.method()).isEqualTo("PUT");
    assertThat(endpoint.mode()).isEqualTo(ExchangeMode.ASYNC);
    assertThat(endpoint.timeoutSeconds()).isEqualTo(45);
    assertThat(endpoint.headers()).containsEntry("X-Registry", "true");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpEndpointResolverTest#tier3_registryHit_resolvesFromRegistry`
Expected: Compilation error — `initialize()` doesn't accept `EndpointRegistry`, `resolve()` doesn't accept `tenancyId`.

- [ ] **Step 3: Write more failing tests before implementing**

```java
@Test
void tier3_registryMiss_throws() {
    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(), Map.of(), 30, emptyRegistry());

    assertThatThrownBy(() -> resolver.resolve("unknown", "tenant-1"))
        .isInstanceOf(WorkerProvisioningException.class);
}

@Test
void tier1_winsOverTier3() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("http", "send-email"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.HTTP,
        Map.of(EndpointPropertyKeys.URL, "https://registry.example.com/email"),
        null, Set.of(EndpointCapability.DISPATCH));

    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30,
        stubRegistry("send-email", descriptor));

    ResolvedEndpoint endpoint = resolver.resolve("send-email", "tenant-1");
    assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
}

@Test
void tier3_wrongProtocol_ignored() {
    EndpointDescriptor mcpDescriptor = new EndpointDescriptor(
        Path.of("http", "send-email"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.MCP,
        Map.of(EndpointPropertyKeys.URL, "https://mcp.example.com/server"),
        null, Set.of(EndpointCapability.DISPATCH));

    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(), Map.of(), 30,
        stubRegistry("send-email", mcpDescriptor));

    assertThatThrownBy(() -> resolver.resolve("send-email", "tenant-1"))
        .isInstanceOf(WorkerProvisioningException.class);
}

@Test
void tier3_noOpRegistry_tier3Invisible() {
    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30, emptyRegistry());

    assertThat(resolver.capabilities()).contains("send-email");
    ResolvedEndpoint endpoint = resolver.resolve("send-email", "tenant-1");
    assertThat(endpoint.url()).isEqualTo("https://spi.example.com/send");
}

@Test
void tier3_blankUrl_throws() {
    EndpointDescriptor noUrl = new EndpointDescriptor(
        Path.of("http", "bad-endpoint"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.HTTP, Map.of(), null, Set.of(EndpointCapability.DISPATCH));

    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(), Map.of(), 30,
        stubRegistry("bad-endpoint", noUrl));

    assertThatThrownBy(() -> resolver.resolve("bad-endpoint", "tenant-1"))
        .isInstanceOf(WorkerProvisioningException.class);
}

@Test
void firstMatch_checksRegistryAfterStaticSet() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("http", "registry-only"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.HTTP,
        Map.of(EndpointPropertyKeys.URL, "https://registry.example.com/only"),
        null, Set.of(EndpointCapability.DISPATCH));

    HttpEndpointResolver resolver = new HttpEndpointResolver();
    resolver.initialize(List.of(SEND_EMAIL_SPI), Map.of(), 30,
        stubRegistry("registry-only", descriptor));

    assertThat(resolver.firstMatch(Set.of("registry-only", "send-email"), "tenant-1"))
        .isPresent();
}
```

- [ ] **Step 4: Implement Tier 3 in HttpEndpointResolver**

Update imports:

```java
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
```

Add field:

```java
@Inject
EndpointRegistry endpointRegistry;
```

Update `initialize()` methods:

```java
void initialize() {
    Map<String, Map<String, String>> configEndpoints = loadConfigEndpoints();
    List<HttpWorkerRoute> routes = List.of();
    if (spiRoutes != null && !spiRoutes.isUnsatisfied()) {
        routes = spiRoutes.stream().toList();
    }
    initialize(routes, configEndpoints, defaultTimeoutSeconds, endpointRegistry);
}

void initialize(List<HttpWorkerRoute> spiRouteList,
                Map<String, Map<String, String>> configEndpoints,
                int defaultTimeout,
                EndpointRegistry registry) {
    resolvedEndpoints.clear();
    this.registry = registry;

    // Tier 1: SPI-registered HttpWorkerRoute beans (highest priority)
    for (HttpWorkerRoute route : spiRouteList) {
        int timeout = route.timeoutSeconds() == -1 ? defaultTimeout : route.timeoutSeconds();
        resolvedEndpoints.put(route.capabilityTag(), new ResolvedEndpoint(
            route.url(),
            route.method(),
            route.exchangeMode(),
            route.headers(),
            timeout
        ));
    }

    // Tier 2: Configuration properties (putIfAbsent — Tier 1 wins)
    if (configEndpoints != null) {
        configEndpoints.forEach((tag, props) -> {
            resolvedEndpoints.putIfAbsent(tag, buildFromConfig(props, defaultTimeout));
        });
    }
}
```

Add registry field: `private EndpointRegistry registry;`

Update `resolve()`:

```java
@Override
public ResolvedEndpoint resolve(String capabilityTag, String tenancyId) {
    ResolvedEndpoint endpoint = resolvedEndpoints.get(capabilityTag);
    if (endpoint != null) {
        return endpoint;
    }
    // Tier 3: EndpointRegistry
    if (registry != null) {
        endpoint = resolveFromRegistry(capabilityTag, tenancyId);
        if (endpoint != null) {
            return endpoint;
        }
    }
    throw WorkerProvisioningException.noRouteFound(capabilityTag);
}
```

Update `firstMatch()`:

```java
@Override
public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
    // Check static set first
    Optional<String> staticMatch = capabilities.stream()
        .filter(resolvedEndpoints::containsKey)
        .findFirst();
    if (staticMatch.isPresent()) {
        return staticMatch;
    }
    // Probe registry for unmatched tags
    if (registry != null) {
        for (String cap : capabilities) {
            if (resolveFromRegistry(cap, tenancyId) != null) {
                return Optional.of(cap);
            }
        }
    }
    return Optional.empty();
}
```

Add private registry resolution method:

```java
private ResolvedEndpoint resolveFromRegistry(String capabilityTag, String tenancyId) {
    return registry.resolve(Path.of("http", capabilityTag), tenancyId)
        .filter(d -> d.protocol() == EndpointProtocol.HTTP)
        .map(d -> buildFromDescriptor(d))
        .orElse(null);
}

private ResolvedEndpoint buildFromDescriptor(EndpointDescriptor descriptor) {
    Map<String, String> props = descriptor.properties();
    String url = props.get(EndpointPropertyKeys.URL);
    if (url == null || url.isBlank()) {
        throw new WorkerProvisioningException(
            "EndpointRegistry descriptor for " + descriptor.path().value()
            + " has blank URL");
    }
    String method = props.getOrDefault("method", "POST");
    ExchangeMode mode = parseMode(props.getOrDefault("mode", "SYNC"));
    int timeout = parseTimeout(props.get("timeout-seconds"), defaultTimeoutSeconds);
    Map<String, String> headers = extractHeaders(props);
    return new ResolvedEndpoint(url, method, mode, headers, timeout);
}
```

- [ ] **Step 5: Update existing tests for tenancyId parameter**

All existing test calls to `resolver.resolve("tag")` become `resolver.resolve("tag", "tenant-1")`. All `resolver.firstMatch(set)` become `resolver.firstMatch(set, "tenant-1")`. The existing `setUp()` calls `resolver.initialize(routes, config, 30)` — add `emptyRegistry()` as the fourth argument.

- [ ] **Step 6: Run all tests**

Run: `mvn --batch-mode test -pl workers-http -Dtest=HttpEndpointResolverTest`
Expected: All tests pass (existing + new).

- [ ] **Step 7: Commit**

```
git add workers-http/src/main/java/io/casehub/workers/http/HttpEndpointResolver.java
git add workers-http/src/test/java/io/casehub/workers/http/HttpEndpointResolverTest.java
git commit -m "feat(#12): wire EndpointRegistry as Tier 3 in HttpEndpointResolver

Tier 1 (SPI) > Tier 2 (config) > Tier 3 (registry).
Single registry call with tenancyId — registry handles tenant fallback.
Protocol check: only EndpointProtocol.HTTP descriptors accepted.

Refs #12"
```

---

### Task 5: HTTP execution manager and provisioner — tenancyId passthrough

**Files:**
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpWorkerExecutionManager.java`
- Modify: `workers-http/src/main/java/io/casehub/workers/http/HttpReactiveWorkerProvisioner.java`
- Test: `workers-http/src/test/java/io/casehub/workers/http/HttpWorkerExecutionManagerTest.java`
- Test: `workers-http/src/test/java/io/casehub/workers/http/HttpReactiveWorkerProvisionerTest.java`

- [ ] **Step 1: Update HttpWorkerExecutionManager**

In `submit()`, change line that calls `resolve()`:

```java
endpoint = httpEndpointResolver.resolve(capability.getName(), instance.tenancyId);
```

And the fault path also passes tenancyId (though it's not used by the resolver there, it keeps the call site consistent):

```java
} catch (WorkerProvisioningException e) {
    LOG.errorf("HTTP endpoint for capability %s missing at dispatch time", capability.getName());
    faultPublisher.fault(
        HttpWorkerEventBusAddresses.HTTP_WORKER_FAULT,
        new WorkerCorrelationContext(instance, worker,
            WorkerExecutionKeys.inputDataHash(instance.getUuid(), worker.getName(),
                capability.getName(), inputData), instance.tenancyId),
        capability, eventLogId, e);
    return Uni.createFrom().voidItem();
}
```

- [ ] **Step 2: Update HttpReactiveWorkerProvisioner**

```java
import io.casehub.platform.api.identity.TenancyConstants;

@Override
public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
    String tenancyId = TenancyConstants.PLATFORM_TENANT_ID; // engine#530: context.tenancyId()
    String capability = httpEndpointResolver.firstMatch(capabilities, tenancyId)
        .orElseThrow(() -> WorkerProvisioningException.noRouteFound(capabilities.toString()));
    httpEndpointResolver.resolve(capability, tenancyId);
    return Uni.createFrom().item(ProvisionResult.empty());
}
```

- [ ] **Step 3: Update HttpWorkerExecutionManagerTest**

Update mock setup. Change the mock pattern for `resolve()` to include tenancyId. Since the test uses `instance.tenancyId` (which comes from `testInstance()`), use `anyString()` for the tenancyId matcher:

All lines with `when(resolver.resolve("tag"))` become `when(resolver.resolve(eq("tag"), anyString()))`.

- [ ] **Step 4: Update HttpReactiveWorkerProvisionerTest**

Update mock calls for `firstMatch()` and `resolve()` to include tenancyId parameter.

- [ ] **Step 5: Run tests**

Run: `mvn --batch-mode test -pl workers-http`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```
git add workers-http/
git commit -m "feat(#12): pass tenancyId through HTTP execution manager and provisioner

Refs #12"
```

---

### Task 6: MCP pom.xml — add casehub-platform-api dependency

**Files:**
- Modify: `workers-mcp/pom.xml`

- [ ] **Step 1: Add dependency**

Add to `workers-mcp/pom.xml` in the `<dependencies>` section:

```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-platform-api</artifactId>
</dependency>
```

Version comes from the parent BOM — no version tag needed.

- [ ] **Step 2: Verify**

Run: `mvn --batch-mode dependency:resolve -pl workers-mcp 2>&1 | tail -5`
Expected: BUILD SUCCESS

- [ ] **Step 3: Commit**

```
git add workers-mcp/pom.xml
git commit -m "build(#12): add casehub-platform-api dependency to workers-mcp

Required for EndpointRegistry, Path, TenancyConstants.

Refs #12"
```

---

### Task 7: MCP ServerResolver — Tier 3 wiring (TDD)

**Files:**
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java`
- Test: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java`

- [ ] **Step 1: Write failing test — Tier 3 registry hit for MCP server**

Add to `McpServerResolverTest.java`:

```java
import io.casehub.platform.api.endpoints.EndpointCapability;
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.endpoints.EndpointType;
import io.casehub.platform.api.path.Path;

private static EndpointRegistry stubMcpRegistry(String serverName, EndpointDescriptor descriptor) {
    return new EndpointRegistry() {
        @Override public void register(EndpointDescriptor endpoint) {}
        @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
            if (path.equals(Path.of("mcp", serverName))) {
                return Optional.of(descriptor);
            }
            return Optional.empty();
        }
        @Override public java.util.List<EndpointDescriptor> discover(
                io.casehub.platform.api.endpoints.EndpointQuery query) { return java.util.List.of(); }
        @Override public void deregister(Path path, String tenancyId) {}
    };
}

private static EndpointRegistry emptyRegistry() {
    return new EndpointRegistry() {
        @Override public void register(EndpointDescriptor endpoint) {}
        @Override public Optional<EndpointDescriptor> resolve(Path path, String tenancyId) {
            return Optional.empty();
        }
        @Override public java.util.List<EndpointDescriptor> discover(
                io.casehub.platform.api.endpoints.EndpointQuery query) { return java.util.List.of(); }
        @Override public void deregister(Path path, String tenancyId) {}
    };
}

@Test
void tier3_registryHit_resolvesServerFromRegistry() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("mcp", "slack"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.MCP,
        Map.of(EndpointPropertyKeys.URL, "https://slack-registry.internal/mcp",
               "timeout-seconds", "60",
               "tools", "send-message,list-channels",
               "headers.Authorization", "Bearer registry-token"),
        null, Set.of(EndpointCapability.DISPATCH));

    McpServerResolver resolver = new McpServerResolver();
    resolver.initialize(List.of(), 30,
        stubMcpRegistry("slack", descriptor));

    ResolvedMcpServer server = resolver.resolve("mcp:slack:send-message", "tenant-1");
    assertThat(server.name()).isEqualTo("slack");
    assertThat(server.url()).isEqualTo("https://slack-registry.internal/mcp");
    assertThat(server.timeoutSeconds()).isEqualTo(60);
    assertThat(server.headers()).containsEntry("Authorization", "Bearer registry-token");
    assertThat(server.tools()).containsExactlyInAnyOrder("send-message", "list-channels");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest#tier3_registryHit_resolvesServerFromRegistry`
Expected: Compilation error — `initialize()` doesn't accept `EndpointRegistry`, `resolve()` doesn't accept `tenancyId`.

- [ ] **Step 3: Write more failing tests**

```java
@Test
void tier3_registryMiss_throws() {
    McpServerResolver resolver = new McpServerResolver();
    resolver.initialize(List.of(), 30, emptyRegistry());

    assertThatThrownBy(() -> resolver.resolve("mcp:unknown:tool", "tenant-1"))
        .isInstanceOf(WorkerProvisioningException.class);
}

@Test
void tier1_winsOverTier3() {
    EndpointDescriptor registryDescriptor = new EndpointDescriptor(
        Path.of("mcp", "slack"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.MCP,
        Map.of(EndpointPropertyKeys.URL, "https://registry-slack.internal/mcp",
               "tools", "send-message"),
        null, Set.of(EndpointCapability.DISPATCH));

    McpServerResolver resolver = new McpServerResolver();
    List<McpServerResolver.ServerConfig> servers = List.of(
        new McpServerResolver.ServerConfig("slack", "https://config-slack.internal/mcp",
            "send-message", 30, Map.of(), "auto")
    );
    resolver.initialize(servers, 30,
        stubMcpRegistry("slack", registryDescriptor));

    ResolvedMcpServer server = resolver.resolve("mcp:slack:send-message", "tenant-1");
    assertThat(server.url()).isEqualTo("https://config-slack.internal/mcp");
}

@Test
void tier3_wrongProtocol_ignored() {
    EndpointDescriptor httpDescriptor = new EndpointDescriptor(
        Path.of("mcp", "slack"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.HTTP,
        Map.of(EndpointPropertyKeys.URL, "https://wrong-protocol.internal/mcp",
               "tools", "send-message"),
        null, Set.of(EndpointCapability.DISPATCH));

    McpServerResolver resolver = new McpServerResolver();
    resolver.initialize(List.of(), 30,
        stubMcpRegistry("slack", httpDescriptor));

    assertThatThrownBy(() -> resolver.resolve("mcp:slack:send-message", "tenant-1"))
        .isInstanceOf(WorkerProvisioningException.class);
}

@Test
void firstMatch_serverExistenceIsMatch() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("mcp", "slack"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.MCP,
        Map.of(EndpointPropertyKeys.URL, "https://slack.internal/mcp"),
        null, Set.of(EndpointCapability.DISPATCH));

    McpServerResolver resolver = new McpServerResolver();
    resolver.initialize(List.of(), 30,
        stubMcpRegistry("slack", descriptor));

    Optional<String> match = resolver.firstMatch(
        Set.of("mcp:slack:any-tool"), "tenant-1");
    assertThat(match).hasValue("mcp:slack:any-tool");
}

@Test
void tier3_emptyTools_discoveryMode() {
    EndpointDescriptor descriptor = new EndpointDescriptor(
        Path.of("mcp", "slack"), "tenant-1", EndpointType.WORKER,
        EndpointProtocol.MCP,
        Map.of(EndpointPropertyKeys.URL, "https://slack.internal/mcp"),
        null, Set.of(EndpointCapability.DISPATCH));

    McpServerResolver resolver = new McpServerResolver();
    resolver.initialize(List.of(), 30,
        stubMcpRegistry("slack", descriptor));

    ResolvedMcpServer server = resolver.resolve("mcp:slack:send-message", "tenant-1");
    assertThat(server.tools()).isEmpty();
}
```

- [ ] **Step 4: Implement Tier 3 in McpServerResolver**

Add imports:

```java
import io.casehub.platform.api.endpoints.EndpointDescriptor;
import io.casehub.platform.api.endpoints.EndpointPropertyKeys;
import io.casehub.platform.api.endpoints.EndpointProtocol;
import io.casehub.platform.api.endpoints.EndpointRegistry;
import io.casehub.platform.api.path.Path;
```

Add field: `private EndpointRegistry registry;`

Update `initialize()` methods:

```java
void initialize(List<ServerConfig> servers, int defaultTimeout) {
    initialize(servers, defaultTimeout, null);
}

void initialize(List<ServerConfig> servers, int defaultTimeout, EndpointRegistry registry) {
    serversByName.clear();
    capabilityToServerName.clear();
    configByName.clear();
    this.registry = registry;

    // ... existing Tier 1/2 logic unchanged ...
}
```

Note: The two-arg `initialize()` is kept for backward compatibility with `initializeFromConfig()` which calls `initialize(servers, defaultTimeoutSeconds)`. That method doesn't have registry access at config-load time — the registry is injected via CDI. Update `initializeFromConfig()`:

```java
void initializeFromConfig() {
    List<ServerConfig> servers = loadFromConfig();
    initialize(servers, defaultTimeoutSeconds, endpointRegistry);
}
```

Add CDI field:

```java
@Inject
EndpointRegistry endpointRegistry;
```

Update `resolve()`:

```java
@Override
public ResolvedMcpServer resolve(String capabilityTag, String tenancyId) {
    String serverName = capabilityToServerName.get(capabilityTag);
    if (serverName != null) {
        return serversByName.get(serverName);
    }
    // Tier 3: EndpointRegistry
    if (registry != null) {
        String parsedServer = parseServerName(capabilityTag);
        if (!parsedServer.isEmpty()) {
            ResolvedMcpServer fromRegistry = resolveFromRegistry(parsedServer, tenancyId);
            if (fromRegistry != null) {
                return fromRegistry;
            }
        }
    }
    throw WorkerProvisioningException.noRouteFound(capabilityTag);
}
```

Update `firstMatch()`:

```java
@Override
public Optional<String> firstMatch(Set<String> capabilities, String tenancyId) {
    Optional<String> staticMatch = capabilities.stream()
        .filter(capabilityToServerName::containsKey)
        .findFirst();
    if (staticMatch.isPresent()) {
        return staticMatch;
    }
    // Probe registry — server existence is a match (tool validation deferred to dispatch)
    if (registry != null) {
        for (String cap : capabilities) {
            String parsedServer = parseServerName(cap);
            if (!parsedServer.isEmpty()) {
                Optional<EndpointDescriptor> descriptor =
                    registry.resolve(Path.of("mcp", parsedServer), tenancyId);
                if (descriptor.isPresent() && descriptor.get().protocol() == EndpointProtocol.MCP) {
                    return Optional.of(cap);
                }
            }
        }
    }
    return Optional.empty();
}
```

Add private registry resolution method:

```java
private ResolvedMcpServer resolveFromRegistry(String serverName, String tenancyId) {
    return registry.resolve(Path.of("mcp", serverName), tenancyId)
        .filter(d -> d.protocol() == EndpointProtocol.MCP)
        .map(d -> buildFromDescriptor(serverName, d))
        .orElse(null);
}

private ResolvedMcpServer buildFromDescriptor(String serverName, EndpointDescriptor descriptor) {
    Map<String, String> props = descriptor.properties();
    String url = props.get(EndpointPropertyKeys.URL);
    if (url == null || url.isBlank()) {
        throw new WorkerProvisioningException(
            "EndpointRegistry descriptor for MCP server '" + serverName + "' has blank URL");
    }
    int timeout = parseTimeout(props.get("timeout-seconds"));
    if (timeout == -1) timeout = defaultTimeoutSeconds;
    Map<String, String> headers = extractHeaders(props);
    Set<String> tools = parseTools(props.getOrDefault("tools", ""), serverName);
    return new ResolvedMcpServer(serverName, url, timeout, headers, tools);
}
```

- [ ] **Step 5: Update existing tests for tenancyId parameter**

All existing test calls to `resolver.resolve("mcp:slack:tool")` become `resolver.resolve("mcp:slack:tool", "tenant-1")`. All `resolver.firstMatch(set)` become `resolver.firstMatch(set, "tenant-1")`. Existing `initialize(servers, 30)` calls stay as-is (two-arg overload still works — passes null registry).

- [ ] **Step 6: Run all tests**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest`
Expected: All tests pass.

- [ ] **Step 7: Commit**

```
git add workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java
git add workers-mcp/src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java
git commit -m "feat(#12): wire EndpointRegistry as Tier 3 in McpServerResolver

Tier 1 (config) > Tier 3 (registry). Single registry call with tenancyId.
Protocol check: only EndpointProtocol.MCP descriptors accepted.
firstMatch() validates server existence — tool validation deferred to dispatch.

Refs #12"
```

---

### Task 8: MCP execution manager and provisioner — tenancyId passthrough

**Files:**
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerExecutionManager.java`
- Modify: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpReactiveWorkerProvisioner.java`
- Test: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpWorkerExecutionManagerTest.java`
- Test: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpReactiveWorkerProvisionerTest.java`

- [ ] **Step 1: Update McpWorkerExecutionManager**

In `submit()`, change the resolver call to pass tenancyId. Find the line with `serverResolver.resolve(capability.getName())` and change to:

```java
serverResolver.resolve(capability.getName(), instance.tenancyId)
```

- [ ] **Step 2: Update McpReactiveWorkerProvisioner**

Replace the entire `provision()` method:

```java
import io.casehub.platform.api.identity.TenancyConstants;

@Override
public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
    String tenancyId = TenancyConstants.PLATFORM_TENANT_ID; // engine#530: context.tenancyId()
    String capability = serverResolver.firstMatch(capabilities, tenancyId)
        .orElseThrow(() -> new WorkerProvisioningException(
            "No matching MCP capability. Supported: " + serverResolver.capabilities()));
    return Uni.createFrom().item(ProvisionResult.empty());
}
```

Note: the old implementation checked `capabilities.stream().anyMatch(serverResolver.capabilities()::contains)` — static-only check. The new one uses `firstMatch(capabilities, tenancyId)` which checks static then registry.

- [ ] **Step 3: Update McpWorkerExecutionManagerTest**

Line 63: change `when(serverResolver.resolve(CAP_TAG)).thenReturn(TEST_SERVER)` to:

```java
when(serverResolver.resolve(eq(CAP_TAG), anyString())).thenReturn(TEST_SERVER);
```

Check all other test methods that call `resolve()` on the mock and update them.

- [ ] **Step 4: Update McpReactiveWorkerProvisionerTest**

Update mock calls for `firstMatch()` to include tenancyId parameter. The provisioner now calls `firstMatch(capabilities, tenancyId)` instead of `capabilities()` directly.

- [ ] **Step 5: Run tests**

Run: `mvn --batch-mode test -pl workers-mcp`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```
git add workers-mcp/
git commit -m "feat(#12): pass tenancyId through MCP execution manager and provisioner

Provisioner now uses firstMatch() (registry-aware) instead of static capabilities() check.

Refs #12"
```

---

### Task 9: Full build verification

- [ ] **Step 1: Run full build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS across all modules.

- [ ] **Step 2: Verify no regressions**

Check test counts haven't decreased:

Run: `mvn --batch-mode test 2>&1 | grep "Tests run:" | tail -10`
Expected: All modules report test runs with 0 failures, 0 errors.

- [ ] **Step 3: Final commit (if any fixups needed)**

If any compilation or test issues were found in Step 1-2, fix and commit with:

```
git commit -m "fix(#12): address build issues from full verification

Refs #12"
```
