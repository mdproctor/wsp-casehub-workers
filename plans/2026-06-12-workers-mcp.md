# workers-mcp Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the `workers-mcp` module — an MCP tool dispatch worker that lets CaseHub case steps call tools on any MCP server via Streamable HTTP transport.

**Architecture:** 8 classes + 1 record + 1 mutable class following the established worker module pattern (same as `workers-github-actions`). New concept: `McpSessionManager` handles MCP protocol lifecycle (lazy initialization with concurrent dedup, session caching, cleanup on shutdown). Config-based server registry with explicit tool declaration. Request-response completion — hold the non-blocking Uni until the tool result arrives, fire `WorkflowExecutionCompleted`.

**Tech Stack:** Java 21, Quarkus 3.32, Vert.x WebClient (Mutiny), Smallrye Mutiny, CDI (`@ApplicationScoped`), MicroProfile Config. Tests: JUnit 5, Mockito, AssertJ.

**Spec:** `docs/superpowers/specs/2026-06-12-casehub-workers-mcp-design.md` (Approved rev 2, 5 review cycles)

---

## File Map

### New module: `workers-mcp/`

| File | Responsibility |
|------|---------------|
| `workers-mcp/pom.xml` | Module POM — same dependencies as workers-github-actions |
| `src/main/java/io/casehub/workers/mcp/McpWorkerConstants.java` | `WORKER_TYPE = "mcp"` |
| `src/main/java/io/casehub/workers/mcp/McpWorkerEventBusAddresses.java` | `MCP_WORKER_FAULT` address |
| `src/main/java/io/casehub/workers/mcp/ResolvedMcpServer.java` | Config record — `name`, `url`, `timeoutSeconds`, `headers`, `tools` |
| `src/main/java/io/casehub/workers/mcp/McpSession.java` | Runtime session state — `sessionId`, `protocolVersion`, `requestIdCounter` |
| `src/main/java/io/casehub/workers/mcp/McpServerResolver.java` | Config → `ResolvedMcpServer`, `WorkerCapabilityResolver` impl |
| `src/main/java/io/casehub/workers/mcp/McpSessionManager.java` | Session lifecycle — lazy init, cache, invalidate, shutdown cleanup |
| `src/main/java/io/casehub/workers/mcp/McpWorkerFaultPublisher.java` | Publishes `WorkflowExecutionFailed` to fault address |
| `src/main/java/io/casehub/workers/mcp/McpWorkerFaultEventHandler.java` | `@ConsumeEvent` — retry logic |
| `src/main/java/io/casehub/workers/mcp/McpReactiveWorkerProvisioner.java` | Capability probe |
| `src/main/java/io/casehub/workers/mcp/McpWorkerExecutionManager.java` | Dispatch `tools/call`, handle response, fire completion |
| `src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java` | Resolver unit tests |
| `src/test/java/io/casehub/workers/mcp/McpSessionManagerTest.java` | Session lifecycle unit tests |
| `src/test/java/io/casehub/workers/mcp/McpReactiveWorkerProvisionerTest.java` | Provisioner unit tests |
| `src/test/java/io/casehub/workers/mcp/McpWorkerExecutionManagerTest.java` | Execution manager unit tests |
| `src/test/java/io/casehub/workers/mcp/McpWorkerFaultEventHandlerTest.java` | Fault handler unit tests |

### Modified files

| File | Change |
|------|--------|
| `pom.xml` (parent) | Add `<module>workers-mcp</module>` and `casehub-workers-mcp` to `<dependencyManagement>` |

---

## Task 1: Module scaffold — POM + constants + record

**Files:**
- Create: `workers-mcp/pom.xml`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerConstants.java`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerEventBusAddresses.java`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/ResolvedMcpServer.java`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpSession.java`
- Modify: `pom.xml` (parent)

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

    <artifactId>casehub-workers-mcp</artifactId>

    <name>CaseHub Workers :: MCP</name>
    <description>WorkerProvisioner and WorkerExecutionManager for MCP — dispatch case steps to MCP server tools via Streamable HTTP</description>

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

- [ ] **Step 2: Add module to parent POM**

In `pom.xml` (parent), add `<module>workers-mcp</module>` after `workers-github-actions` in the `<modules>` section. Add dependency management entry:

```xml
<dependency>
    <groupId>io.casehub</groupId>
    <artifactId>casehub-workers-mcp</artifactId>
    <version>${project.version}</version>
</dependency>
```

- [ ] **Step 3: Create McpWorkerConstants**

```java
package io.casehub.workers.mcp;

public final class McpWorkerConstants {
    public static final String WORKER_TYPE = "mcp";
    public static final String PROTOCOL_VERSION = "2025-06-18";
    public static final String CLIENT_NAME = "CaseHub";
    public static final String CLIENT_VERSION = "0.2";
    private McpWorkerConstants() {}
}
```

- [ ] **Step 4: Create McpWorkerEventBusAddresses**

```java
package io.casehub.workers.mcp;

public final class McpWorkerEventBusAddresses {
    public static final String MCP_WORKER_FAULT = "casehub.workers.mcp.fault";
    private McpWorkerEventBusAddresses() {}
}
```

- [ ] **Step 5: Create ResolvedMcpServer record**

```java
package io.casehub.workers.mcp;

import java.util.Map;
import java.util.Set;

public record ResolvedMcpServer(
    String name,
    String url,
    int timeoutSeconds,
    Map<String, String> headers,
    Set<String> tools
) {}
```

- [ ] **Step 6: Create McpSession**

```java
package io.casehub.workers.mcp;

import java.util.concurrent.atomic.AtomicLong;

public class McpSession {
    private final String sessionId;
    private final String protocolVersion;
    private final AtomicLong requestIdCounter;

    public McpSession(String sessionId, String protocolVersion) {
        this.sessionId = sessionId;
        this.protocolVersion = protocolVersion;
        this.requestIdCounter = new AtomicLong(2);
    }

    public String sessionId() { return sessionId; }
    public String protocolVersion() { return protocolVersion; }
    public long nextRequestId() { return requestIdCounter.getAndIncrement(); }
    public boolean hasSessionId() { return sessionId != null && !sessionId.isBlank(); }
}
```

Note: counter starts at 2 because `id: 1` is used by the `initialize` request during session creation.

- [ ] **Step 7: Verify compilation**

Run: `mvn --batch-mode compile -pl workers-mcp -am`
Expected: BUILD SUCCESS

- [ ] **Step 8: Commit**

```
feat(workers-mcp): module scaffold — POM, constants, ResolvedMcpServer, McpSession
```

---

## Task 2: McpServerResolver — config parsing and capability resolution

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpServerResolver.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpServerResolverTest.java`

- [ ] **Step 1: Write failing tests**

```java
package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.workers.common.WorkerProvisioningException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class McpServerResolverTest {

    @Test
    void singleServer_twoTools_buildsTwoCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of())
        ), 30);

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
            "mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test
    void multipleServers_buildsCombinedCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of()),
            serverConfig("jira", "https://jira.internal/mcp", "create-issue", 60, Map.of())
        ), 30);

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
            "mcp:slack:send-message", "mcp:jira:create-issue");
    }

    @Test
    void resolve_validTag_returnsServer() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of("Authorization", "Bearer xxx"))
        ), 30);

        ResolvedMcpServer server = resolver.resolve("mcp:slack:send-message");
        assertThat(server.name()).isEqualTo("slack");
        assertThat(server.url()).isEqualTo("https://slack.internal/mcp");
        assertThat(server.timeoutSeconds()).isEqualTo(30);
        assertThat(server.headers()).containsEntry("Authorization", "Bearer xxx");
        assertThat(server.tools()).containsExactlyInAnyOrder("send-message", "list-channels");
    }

    @Test
    void resolve_multipleTagsSameServer_returnsSameInstance() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of())
        ), 30);

        ResolvedMcpServer s1 = resolver.resolve("mcp:slack:send-message");
        ResolvedMcpServer s2 = resolver.resolve("mcp:slack:list-channels");
        assertThat(s1).isSameAs(s2);
    }

    @Test
    void resolve_unknownServer_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of())
        ), 30);

        assertThatThrownBy(() -> resolver.resolve("mcp:unknown:send-message"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void resolve_knownServerUnlistedTool_throws() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of())
        ), 30);

        assertThatThrownBy(() -> resolver.resolve("mcp:slack:unknown-tool"))
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void timeoutFallsBackToGlobalDefault() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", -1, Map.of())
        ), 45);

        assertThat(resolver.resolve("mcp:slack:send-message").timeoutSeconds()).isEqualTo(45);
    }

    @Test
    void noHeaders_emptyMap() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("db", "https://db.internal/mcp", "query", 30, Map.of())
        ), 30);

        assertThat(resolver.resolve("mcp:db:query").headers()).isEmpty();
    }

    @Test
    void blankUrl_throwsAtStartup() {
        McpServerResolver resolver = new McpServerResolver();
        assertThatThrownBy(() -> resolver.initialize(List.of(
            serverConfig("slack", "  ", "send-message", 30, Map.of())
        ), 30)).isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("slack");
    }

    @Test
    void toolsParsing_trims_whitespace() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", " send-message , list-channels ", 30, Map.of())
        ), 30);

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
            "mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test
    void toolsParsing_ignoresEmptyEntries() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message,,list-channels,", 30, Map.of())
        ), 30);

        assertThat(resolver.capabilities()).containsExactlyInAnyOrder(
            "mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test
    void toolsParsing_duplicatesRejected() {
        McpServerResolver resolver = new McpServerResolver();
        assertThatThrownBy(() -> resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message,send-message", 30, Map.of())
        ), 30)).isInstanceOf(WorkerProvisioningException.class)
            .hasMessageContaining("duplicate");
    }

    @Test
    void emptyTools_noCapabilities() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "", 30, Map.of())
        ), 30);

        assertThat(resolver.capabilities()).isEmpty();
    }

    @Test
    void firstMatch_findsMatch() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of())
        ), 30);

        assertThat(resolver.firstMatch(java.util.Set.of("mcp:slack:send-message", "mcp:jira:create-issue")))
            .isPresent().hasValue("mcp:slack:send-message");
    }

    @Test
    void firstMatch_noMatch_returnsEmpty() {
        McpServerResolver resolver = new McpServerResolver();
        resolver.initialize(List.of(
            serverConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of())
        ), 30);

        assertThat(resolver.firstMatch(java.util.Set.of("mcp:jira:create-issue"))).isEmpty();
    }

    record ServerConfig(String name, String url, String tools, int timeoutSeconds, Map<String, String> headers) {}

    private static ServerConfig serverConfig(String name, String url, String tools, int timeout, Map<String, String> headers) {
        return new ServerConfig(name, url, tools, timeout, headers);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest`
Expected: FAIL — `McpServerResolver` class not found

- [ ] **Step 3: Implement McpServerResolver**

```java
package io.casehub.workers.mcp;

import io.casehub.workers.common.WorkerCapabilityResolver;
import io.casehub.workers.common.WorkerProvisioningException;
import io.quarkus.runtime.StartupEvent;
import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.inject.Inject;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.eclipse.microprofile.config.Config;
import org.eclipse.microprofile.config.inject.ConfigProperty;

import static jakarta.interceptor.Interceptor.Priority.APPLICATION;

@ApplicationScoped
public class McpServerResolver implements WorkerCapabilityResolver<ResolvedMcpServer> {

    @Inject
    Config config;

    @ConfigProperty(name = "casehub.workers.mcp.default-timeout-seconds", defaultValue = "30")
    int defaultTimeoutSeconds;

    private final Map<String, ResolvedMcpServer> servers = new HashMap<>();
    private final Map<String, String> tagToServer = new HashMap<>();

    void onStartup(@Observes @Priority(APPLICATION) StartupEvent ev) {
        List<ServerConfig> configs = loadFromConfig();
        initialize(configs, defaultTimeoutSeconds);
    }

    record ServerConfig(String name, String url, String tools, int timeoutSeconds, Map<String, String> headers) {}

    void initialize(List<ServerConfig> serverConfigs, int defaultTimeout) {
        servers.clear();
        tagToServer.clear();

        for (ServerConfig sc : serverConfigs) {
            if (sc.url() == null || sc.url().isBlank()) {
                throw new WorkerProvisioningException(
                    "MCP server '" + sc.name() + "' has no URL configured");
            }

            int timeout = sc.timeoutSeconds() > 0 ? sc.timeoutSeconds() : defaultTimeout;

            Set<String> toolSet = parseTools(sc.name(), sc.tools());
            ResolvedMcpServer server = new ResolvedMcpServer(
                sc.name(), sc.url(), timeout,
                sc.headers() != null ? Map.copyOf(sc.headers()) : Map.of(),
                Set.copyOf(toolSet));

            servers.put(sc.name(), server);
            for (String tool : toolSet) {
                tagToServer.put("mcp:" + sc.name() + ":" + tool, sc.name());
            }
        }
    }

    private Set<String> parseTools(String serverName, String toolsCsv) {
        if (toolsCsv == null || toolsCsv.isBlank()) {
            return Set.of();
        }
        Set<String> result = new HashSet<>();
        for (String raw : toolsCsv.split(",")) {
            String tool = raw.trim();
            if (tool.isEmpty()) continue;
            if (!result.add(tool)) {
                throw new WorkerProvisioningException(
                    "MCP server '" + serverName + "' has duplicate tool: " + tool);
            }
        }
        return result;
    }

    @Override
    public ResolvedMcpServer resolve(String capabilityTag) {
        String serverName = tagToServer.get(capabilityTag);
        if (serverName == null) {
            throw new WorkerProvisioningException(
                "No MCP server configured for capability: " + capabilityTag);
        }
        return servers.get(serverName);
    }

    @Override
    public Optional<String> firstMatch(Set<String> capabilities) {
        return capabilities.stream()
            .filter(tagToServer::containsKey)
            .findFirst();
    }

    @Override
    public Set<String> capabilities() {
        return Set.copyOf(tagToServer.keySet());
    }

    ResolvedMcpServer serverByName(String name) {
        return servers.get(name);
    }

    private List<ServerConfig> loadFromConfig() {
        if (config == null) return List.of();
        String prefix = "casehub.workers.mcp.servers.";
        Map<String, Map<String, String>> grouped = new LinkedHashMap<>();
        for (String key : config.getPropertyNames()) {
            if (key.startsWith(prefix)) {
                String remainder = key.substring(prefix.length());
                int dot = remainder.indexOf('.');
                if (dot > 0) {
                    String serverName = remainder.substring(0, dot);
                    String prop = remainder.substring(dot + 1);
                    config.getOptionalValue(key, String.class).ifPresent(value ->
                        grouped.computeIfAbsent(serverName, k -> new LinkedHashMap<>()).put(prop, value));
                }
            }
        }
        return grouped.entrySet().stream().map(e -> {
            Map<String, String> props = e.getValue();
            Map<String, String> headers = new LinkedHashMap<>();
            String headerPrefix = "headers.";
            props.forEach((k, v) -> {
                if (k.startsWith(headerPrefix)) {
                    headers.put(k.substring(headerPrefix.length()), v);
                }
            });
            int timeout = -1;
            String ts = props.get("timeout-seconds");
            if (ts != null && !ts.isBlank()) {
                try { timeout = Integer.parseInt(ts); } catch (NumberFormatException ignored) {}
            }
            return new ServerConfig(e.getKey(), props.get("url"), props.getOrDefault("tools", ""), timeout, headers);
        }).toList();
    }

    static String parseServerName(String capabilityTag) {
        int first = capabilityTag.indexOf(':');
        int second = capabilityTag.indexOf(':', first + 1);
        if (first < 0 || second < 0) return null;
        return capabilityTag.substring(first + 1, second);
    }

    static String parseToolName(String capabilityTag) {
        int first = capabilityTag.indexOf(':');
        int second = capabilityTag.indexOf(':', first + 1);
        if (first < 0 || second < 0) return null;
        return capabilityTag.substring(second + 1);
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpServerResolverTest`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```
feat(workers-mcp): McpServerResolver — config-based server registry with N:1 tag mapping
```

---

## Task 3: McpSessionManager — initialization, caching, concurrent dedup, cleanup

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpSessionManager.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpSessionManagerTest.java`

- [ ] **Step 1: Write failing tests**

```java
package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

import io.casehub.workers.common.PermanentFaultException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("unchecked")
class McpSessionManagerTest {

    private McpSessionManager sessionManager;
    private McpServerResolver serverResolver;
    private WebClient webClient;
    private HttpRequest<Buffer> request;

    @BeforeEach
    void setUp() {
        serverResolver = new McpServerResolver();
        serverResolver.initialize(java.util.List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of())
        ), 30);

        webClient = mock(WebClient.class);
        request = mock(HttpRequest.class);

        sessionManager = new McpSessionManager();
        sessionManager.serverResolver = serverResolver;
        sessionManager.webClient = webClient;

        when(webClient.requestAbs(any(HttpMethod.class), anyString())).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
    }

    @Test
    void firstDispatch_triggersInitialization() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn("session-123");

        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        McpSession session = sessionManager.getOrInitialize("slack").await().indefinitely();

        assertThat(session.sessionId()).isEqualTo("session-123");
        assertThat(session.protocolVersion()).isEqualTo("2025-06-18");
    }

    @Test
    void secondDispatch_reusesCachedSession() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn(null);

        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        McpSession s1 = sessionManager.getOrInitialize("slack").await().indefinitely();
        McpSession s2 = sessionManager.getOrInitialize("slack").await().indefinitely();

        assertThat(s1).isSameAs(s2);
        verify(request, times(2)).sendJson(any());
    }

    @Test
    void invalidate_nextDispatch_reinitializes() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn("session-1");

        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        sessionManager.getOrInitialize("slack").await().indefinitely();
        sessionManager.invalidate("slack");
        McpSession s2 = sessionManager.getOrInitialize("slack").await().indefinitely();

        assertThat(s2).isNotNull();
        verify(request, times(4)).sendJson(any());
    }

    @Test
    void versionMismatch_permanentFault() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2024-11-05\",\"capabilities\":{},\"serverInfo\":{\"name\":\"OldServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn(null);

        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(initResponse));

        assertThatThrownBy(() -> sessionManager.getOrInitialize("slack").await().indefinitely())
            .isInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("2024-11-05");
    }

    @Test
    void transientFailure_removesCache_nextDispatchRetries() {
        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("connection refused")));

        assertThatThrownBy(() -> sessionManager.getOrInitialize("slack").await().indefinitely())
            .isInstanceOf(RuntimeException.class);

        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn(null);
        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        McpSession session = sessionManager.getOrInitialize("slack").await().indefinitely();
        assertThat(session).isNotNull();
    }

    @Test
    void noSessionId_sessionHasNullId() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn(null);
        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        McpSession session = sessionManager.getOrInitialize("slack").await().indefinitely();
        assertThat(session.hasSessionId()).isFalse();
    }

    @Test
    void requestIdCounter_startsAt2() {
        HttpResponse<Buffer> initResponse = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"protocolVersion\":\"2025-06-18\",\"capabilities\":{},\"serverInfo\":{\"name\":\"TestServer\",\"version\":\"1.0\"}}}");
        when(initResponse.getHeader("Mcp-Session-Id")).thenReturn(null);
        HttpResponse<Buffer> initializedResponse = mockResponse(202);

        when(request.sendJson(any()))
            .thenReturn(Uni.createFrom().item(initResponse))
            .thenReturn(Uni.createFrom().item(initializedResponse));

        McpSession session = sessionManager.getOrInitialize("slack").await().indefinitely();
        assertThat(session.nextRequestId()).isEqualTo(2L);
        assertThat(session.nextRequestId()).isEqualTo(3L);
    }

    private HttpResponse<Buffer> mockResponse(int status) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.statusMessage()).thenReturn("Status " + status);
        when(response.getHeader(anyString())).thenReturn(null);
        return response;
    }

    private HttpResponse<Buffer> mockJsonResponse(int status, String body) {
        HttpResponse<Buffer> response = mockResponse(status);
        when(response.bodyAsString()).thenReturn(body);
        when(response.body()).thenReturn(Buffer.buffer(body));
        return response;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpSessionManagerTest`
Expected: FAIL — `McpSessionManager` class not found

- [ ] **Step 3: Implement McpSessionManager**

```java
package io.casehub.workers.mcp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.workers.common.PermanentFaultException;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.jboss.logging.Logger;

@ApplicationScoped
public class McpSessionManager {

    private static final Logger LOG = Logger.getLogger(McpSessionManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Inject McpServerResolver serverResolver;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    WebClient webClient;

    private final ConcurrentHashMap<String, Uni<McpSession>> sessions = new ConcurrentHashMap<>();

    @PostConstruct
    void init() {
        if (webClient == null) {
            webClient = WebClient.create(vertx);
        }
    }

    public Uni<McpSession> getOrInitialize(String serverName) {
        return sessions.computeIfAbsent(serverName,
            k -> performInitialization(k)
                .onFailure().invoke(t -> sessions.remove(k))
                .memoize().indefinitely());
    }

    public void invalidate(String serverName) {
        sessions.remove(serverName);
    }

    private Uni<McpSession> performInitialization(String serverName) {
        ResolvedMcpServer server = serverResolver.serverByName(serverName);
        if (server == null) {
            return Uni.createFrom().failure(
                new PermanentFaultException(0, "No MCP server configured: " + serverName));
        }

        ObjectNode initRequest = OBJECT_MAPPER.createObjectNode();
        initRequest.put("jsonrpc", "2.0");
        initRequest.put("id", 1);
        initRequest.put("method", "initialize");
        ObjectNode params = initRequest.putObject("params");
        params.put("protocolVersion", McpWorkerConstants.PROTOCOL_VERSION);
        params.putObject("capabilities");
        ObjectNode clientInfo = params.putObject("clientInfo");
        clientInfo.put("name", McpWorkerConstants.CLIENT_NAME);
        clientInfo.put("version", McpWorkerConstants.CLIENT_VERSION);

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        request.timeout(server.timeoutSeconds() * 1000L);
        server.headers().forEach(request::putHeader);

        return request.sendJson(initRequest)
            .flatMap(response -> {
                int status = response.statusCode();
                if (status >= 400 && status < 500) {
                    throw new PermanentFaultException(status,
                        "MCP initialization failed: " + status + " " + response.statusMessage());
                }
                if (status >= 500) {
                    throw new RuntimeException(
                        "MCP initialization failed: " + status + " " + response.statusMessage());
                }

                String body = response.bodyAsString();
                JsonNode json;
                try {
                    json = OBJECT_MAPPER.readTree(body);
                } catch (Exception e) {
                    throw new PermanentFaultException(0, "Malformed MCP initialize response");
                }

                if (json.has("error")) {
                    throw new PermanentFaultException(0,
                        "MCP initialization rejected: " + json.get("error"));
                }

                JsonNode result = json.get("result");
                String protocolVersion = result.path("protocolVersion").asText("");
                if (!McpWorkerConstants.PROTOCOL_VERSION.equals(protocolVersion)) {
                    throw new PermanentFaultException(0,
                        "Unsupported MCP protocol version: " + protocolVersion
                            + " (expected " + McpWorkerConstants.PROTOCOL_VERSION + ")");
                }

                String sessionId = response.getHeader("Mcp-Session-Id");
                McpSession session = new McpSession(sessionId, protocolVersion);

                return sendInitializedNotification(server, session);
            });
    }

    private Uni<McpSession> sendInitializedNotification(ResolvedMcpServer server, McpSession session) {
        ObjectNode notification = OBJECT_MAPPER.createObjectNode();
        notification.put("jsonrpc", "2.0");
        notification.put("method", "notifications/initialized");

        HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, server.url());
        request.putHeader("Content-Type", "application/json");
        request.putHeader("Accept", "application/json, text/event-stream");
        request.putHeader("MCP-Protocol-Version", session.protocolVersion());
        request.timeout(server.timeoutSeconds() * 1000L);
        if (session.hasSessionId()) {
            request.putHeader("Mcp-Session-Id", session.sessionId());
        }
        server.headers().forEach(request::putHeader);

        return request.sendJson(notification)
            .map(response -> {
                if (response.statusCode() != 202) {
                    LOG.warnf("MCP initialized notification returned %d (expected 202) for server %s",
                        response.statusCode(), server.name());
                }
                return session;
            });
    }

    @PreDestroy
    void cleanup() {
        sessions.forEach((name, sessionUni) -> {
            try {
                McpSession session = sessionUni.await().atMost(java.time.Duration.ofMillis(100));
                if (session != null && session.hasSessionId()) {
                    ResolvedMcpServer server = serverResolver.serverByName(name);
                    if (server != null) {
                        HttpRequest<Buffer> req = webClient.requestAbs(HttpMethod.DELETE, server.url());
                        req.putHeader("Mcp-Session-Id", session.sessionId());
                        server.headers().forEach(req::putHeader);
                        req.send().subscribe().with(
                            r -> LOG.debugf("MCP session cleanup for %s: %d", name, r.statusCode()),
                            t -> LOG.debugf("MCP session cleanup for %s failed: %s", name, t.getMessage()));
                    }
                }
            } catch (Exception ignored) {}
        });
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpSessionManagerTest`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```
feat(workers-mcp): McpSessionManager — lazy init, concurrent dedup, session caching, shutdown cleanup
```

---

## Task 4: McpWorkerFaultPublisher + McpWorkerFaultEventHandler

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerFaultPublisher.java`
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerFaultEventHandler.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpWorkerFaultEventHandlerTest.java`

- [ ] **Step 1: Create McpWorkerFaultPublisher** (no dedicated test — tested via execution manager)

```java
package io.casehub.workers.mcp;

import io.casehub.api.model.Capability;
import io.casehub.engine.common.internal.event.WorkflowExecutionFailed;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.vertx.mutiny.core.eventbus.EventBus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

@ApplicationScoped
public class McpWorkerFaultPublisher {

    @Inject
    EventBus eventBus;

    public void fault(WorkerCorrelationContext ctx, Capability capability,
                      Long eventLogId, Throwable cause) {
        eventBus.publish(McpWorkerEventBusAddresses.MCP_WORKER_FAULT,
            new WorkflowExecutionFailed(
                ctx.caseInstance(), ctx.worker(), capability,
                ctx.idempotency(), eventLogId.toString(), cause));
    }
}
```

- [ ] **Step 2: Write failing fault handler tests**

```java
package io.casehub.workers.mcp;

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
class McpWorkerFaultEventHandlerTest {

    private McpWorkerFaultEventHandler handler;
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

        handler = new McpWorkerFaultEventHandler();
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
        Worker worker = WorkerTestSupport.testWorker("w1", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");
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
        Worker worker = WorkerTestSupport.testWorker("w1", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");
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
        Worker worker = WorkerTestSupport.testWorker("w1", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");
        RetryAfterException cause = new RetryAfterException(30000, "429 rate limited");

        when(retrySupport.countFailedAttempts(any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().item(1L));

        EventLog eventLog = new EventLog();
        ObjectNode payload = OBJECT_MAPPER.createObjectNode().put("channel", "#general");
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
        assertThat(delayCaptor.getValue()).isEqualTo(30000L);
    }

    @Test
    void exhausted_afterMaxAttempts() {
        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        RetryPolicy retryPolicy = new RetryPolicy(3, 10000, BackoffStrategy.FIXED);
        ExecutionPolicy ep = new ExecutionPolicy(5000, retryPolicy);
        Worker worker = WorkerTestSupport.testWorker("w1", "mcp:slack:send-message");
        worker.setExecutionPolicy(ep);
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");
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
        Worker worker = WorkerTestSupport.testWorker("w1", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");
        RuntimeException cause = new RuntimeException("error");

        when(retrySupport.persistFailureLog(any(), any(), any(), any(), any()))
            .thenReturn(Uni.createFrom().failure(new RuntimeException("DB down")));

        WorkflowExecutionFailed event = new WorkflowExecutionFailed(
            instance, worker, cap, "hash-1", "42", cause);

        handler.onFault(event).await().indefinitely();

        verify(retrySupport, never()).publishRetriesExhausted(any(), any(), any());
    }
}
```

- [ ] **Step 3: Implement McpWorkerFaultEventHandler**

```java
package io.casehub.workers.mcp;

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
public class McpWorkerFaultEventHandler {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};
    private static final Logger LOG = Logger.getLogger(McpWorkerFaultEventHandler.class);

    @Inject WorkerRetrySupport retrySupport;
    @Inject WorkerExecutionManager workerExecutionManager;
    @Inject Vertx vertx;
    @Inject EventLogRepository eventLogRepository;

    @ConsumeEvent(value = McpWorkerEventBusAddresses.MCP_WORKER_FAULT, blocking = true)
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

- [ ] **Step 4: Run tests**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpWorkerFaultEventHandlerTest`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```
feat(workers-mcp): McpWorkerFaultPublisher + McpWorkerFaultEventHandler — fault pipeline
```

---

## Task 5: McpReactiveWorkerProvisioner

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpReactiveWorkerProvisioner.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpReactiveWorkerProvisionerTest.java`

- [ ] **Step 1: Write failing tests**

```java
package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.workers.common.WorkerProvisioningException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class McpReactiveWorkerProvisionerTest {

    private McpReactiveWorkerProvisioner provisioner;
    private McpServerResolver resolver;

    @BeforeEach
    void setUp() {
        resolver = new McpServerResolver();
        resolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message,list-channels", 30, Map.of())
        ), 30);

        provisioner = new McpReactiveWorkerProvisioner();
        provisioner.serverResolver = resolver;
    }

    @Test
    void getCapabilities_returnsAllTags() {
        assertThat(provisioner.getCapabilities().await().indefinitely())
            .containsExactlyInAnyOrder("mcp:slack:send-message", "mcp:slack:list-channels");
    }

    @Test
    void provision_matchingCapability_succeeds() {
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        ProvisionResult result = provisioner.provision(
            Set.of("mcp:slack:send-message"), ctx).await().indefinitely();
        assertThat(result).isNotNull();
    }

    @Test
    void provision_unknownCapability_throws() {
        ProvisionContext ctx = new ProvisionContext(UUID.randomUUID(), "task", null, null, null, null);
        assertThatThrownBy(() -> provisioner.provision(
            Set.of("mcp:unknown:tool"), ctx).await().indefinitely())
            .isInstanceOf(WorkerProvisioningException.class);
    }

    @Test
    void terminate_returnsVoid() {
        assertThat(provisioner.terminate("any").await().indefinitely()).isNull();
    }
}
```

- [ ] **Step 2: Implement McpReactiveWorkerProvisioner**

```java
package io.casehub.workers.mcp;

import io.casehub.api.model.ProvisionContext;
import io.casehub.api.spi.ProvisionResult;
import io.casehub.api.spi.ReactiveWorkerProvisioner;
import io.casehub.workers.common.WorkerProvisioningException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;

@ApplicationScoped
public class McpReactiveWorkerProvisioner implements ReactiveWorkerProvisioner {

    @Inject
    McpServerResolver serverResolver;

    @Override
    public Uni<ProvisionResult> provision(Set<String> capabilities, ProvisionContext context) {
        boolean match = capabilities.stream().anyMatch(serverResolver.capabilities()::contains);
        if (!match) {
            throw new WorkerProvisioningException(
                "No matching MCP capability. Supported: " + serverResolver.capabilities());
        }
        return Uni.createFrom().item(ProvisionResult.empty());
    }

    @Override
    public Uni<Void> terminate(String workerId) {
        return Uni.createFrom().voidItem();
    }

    @Override
    public Uni<Set<String>> getCapabilities() {
        return Uni.createFrom().item(serverResolver.capabilities());
    }
}
```

- [ ] **Step 3: Run tests**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpReactiveWorkerProvisionerTest`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```
feat(workers-mcp): McpReactiveWorkerProvisioner — capability probe
```

---

## Task 6: McpWorkerExecutionManager — dispatch, response parsing, completion

**Files:**
- Create: `workers-mcp/src/main/java/io/casehub/workers/mcp/McpWorkerExecutionManager.java`
- Create: `workers-mcp/src/test/java/io/casehub/workers/mcp/McpWorkerExecutionManagerTest.java`

- [ ] **Step 1: Write failing tests**

```java
package io.casehub.workers.mcp;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

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
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

@SuppressWarnings("unchecked")
class McpWorkerExecutionManagerTest {

    private McpWorkerExecutionManager manager;
    private McpServerResolver serverResolver;
    private McpSessionManager sessionManager;
    private McpWorkerFaultPublisher faultPublisher;
    private WorkflowCompletionPublisher completionPublisher;
    private WebClient webClient;
    private HttpRequest<Buffer> request;

    @BeforeEach
    void setUp() {
        serverResolver = new McpServerResolver();
        serverResolver.initialize(List.of(
            new McpServerResolver.ServerConfig("slack", "https://slack.internal/mcp", "send-message", 30, Map.of("Authorization", "Bearer xxx"))
        ), 30);

        sessionManager = mock(McpSessionManager.class);
        faultPublisher = mock(McpWorkerFaultPublisher.class);
        completionPublisher = mock(WorkflowCompletionPublisher.class);
        webClient = mock(WebClient.class);
        request = mock(HttpRequest.class);

        manager = new McpWorkerExecutionManager();
        manager.serverResolver = serverResolver;
        manager.sessionManager = sessionManager;
        manager.faultPublisher = faultPublisher;
        manager.completionPublisher = completionPublisher;
        manager.webClient = webClient;

        McpSession session = new McpSession("session-123", "2025-06-18");
        when(sessionManager.getOrInitialize(anyString()))
            .thenReturn(Uni.createFrom().item(session));

        when(webClient.requestAbs(any(HttpMethod.class), anyString())).thenReturn(request);
        when(request.putHeader(anyString(), anyString())).thenReturn(request);
        when(request.timeout(anyLong())).thenReturn(request);
    }

    @Test
    void successfulToolCall_json_completesWithContent() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"Done\"}],\"isError\":false}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsKey("content");
        verify(faultPublisher, never()).fault(any(), any(), anyLong(), any());
    }

    @Test
    void successfulToolCall_structuredContent_preferred() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"{}\"}],\"structuredContent\":{\"temperature\":22.5},\"isError\":false}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Map<String, Object>> outputCaptor = ArgumentCaptor.forClass(Map.class);
        verify(completionPublisher).complete(any(), outputCaptor.capture());
        assertThat(outputCaptor.getValue()).containsEntry("temperature", 22.5);
        assertThat(outputCaptor.getValue()).doesNotContainKey("content");
    }

    @Test
    void isError_true_retryableFault() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"Rate limit exceeded\"}],\"isError\":true}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isNotInstanceOf(PermanentFaultException.class)
            .hasMessageContaining("isError");
    }

    @Test
    void jsonRpcError_invalidParams_permanentFault() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-32602,\"message\":\"Invalid params\"}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void jsonRpcError_internalError_retryable() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"error\":{\"code\":-32603,\"message\":\"Internal error\"}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http404_withSession_retryable() {
        HttpResponse<Buffer> response = mockResponse(404);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        verify(sessionManager).invalidate("slack");
        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http404_withoutSession_permanentFault() {
        McpSession noSessionSession = new McpSession(null, "2025-06-18");
        when(sessionManager.getOrInitialize(anyString()))
            .thenReturn(Uni.createFrom().item(noSessionSession));

        HttpResponse<Buffer> response = mockResponse(404);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http429_retryAfter() {
        HttpResponse<Buffer> response = mockResponse(429);
        when(response.getHeader("Retry-After")).thenReturn("60");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(RetryAfterException.class);
        assertThat(((RetryAfterException) causeCaptor.getValue()).retryAfterMs()).isEqualTo(60000L);
    }

    @Test
    void http400_permanentFault() {
        HttpResponse<Buffer> response = mockResponse(400);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isInstanceOf(PermanentFaultException.class);
    }

    @Test
    void http500_retryable() {
        HttpResponse<Buffer> response = mockResponse(500);
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue())
            .isNotInstanceOf(PermanentFaultException.class)
            .isNotInstanceOf(RetryAfterException.class);
    }

    @Test
    void malformedResponse_retryable() {
        HttpResponse<Buffer> response = mockJsonResponse(200, "<html>Bad Gateway</html>");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        ArgumentCaptor<Throwable> causeCaptor = ArgumentCaptor.forClass(Throwable.class);
        verify(faultPublisher).fault(any(), eq(cap), eq(1L), causeCaptor.capture());
        assertThat(causeCaptor.getValue()).isNotInstanceOf(PermanentFaultException.class);
    }

    @Test
    void protocolHeaders_sentCorrectly() {
        HttpResponse<Buffer> response = mockJsonResponse(200,
            "{\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"OK\"}],\"isError\":false}}");
        when(response.getHeader("Content-Type")).thenReturn("application/json");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        verify(request).putHeader("Accept", "application/json, text/event-stream");
        verify(request).putHeader("MCP-Protocol-Version", "2025-06-18");
        verify(request).putHeader("Mcp-Session-Id", "session-123");
        verify(request).putHeader("Authorization", "Bearer xxx");
    }

    @Test
    void sseResponse_extractsJsonRpcResult() {
        String sseBody = "event: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":2,\"result\":{\"content\":[{\"type\":\"text\",\"text\":\"SSE result\"}],\"isError\":false}}\n\n";
        HttpResponse<Buffer> response = mockJsonResponse(200, sseBody);
        when(response.getHeader("Content-Type")).thenReturn("text/event-stream");
        when(request.sendJson(any())).thenReturn(Uni.createFrom().item(response));

        CaseInstance instance = WorkerTestSupport.testCaseInstance();
        Worker worker = WorkerTestSupport.testWorker("w", "mcp:slack:send-message");
        Capability cap = WorkerTestSupport.testCapability("mcp:slack:send-message");

        manager.submit(1L, instance, worker, cap, Map.of("channel", "#general")).await().indefinitely();

        verify(completionPublisher).complete(any(WorkerCorrelationContext.class), any());
        verify(faultPublisher, never()).fault(any(), any(), anyLong(), any());
    }

    @Test
    void schedulePersistedEvent_returnsVoid() {
        assertThat(manager.schedulePersistedEvent(new EventLog()).await().indefinitely()).isNull();
    }

    @Test
    void getActiveWorkCount_returnsZero() {
        assertThat(manager.getActiveWorkCount("any")).isEqualTo(0);
    }

    private HttpResponse<Buffer> mockResponse(int status) {
        HttpResponse<Buffer> response = mock(HttpResponse.class);
        when(response.statusCode()).thenReturn(status);
        when(response.statusMessage()).thenReturn("Status " + status);
        when(response.getHeader(anyString())).thenReturn(null);
        return response;
    }

    private HttpResponse<Buffer> mockJsonResponse(int status, String body) {
        HttpResponse<Buffer> response = mockResponse(status);
        when(response.bodyAsString()).thenReturn(body);
        when(response.body()).thenReturn(Buffer.buffer(body));
        return response;
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpWorkerExecutionManagerTest`
Expected: FAIL — `McpWorkerExecutionManager` class not found

- [ ] **Step 3: Implement McpWorkerExecutionManager**

```java
package io.casehub.workers.mcp;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.casehub.api.model.Capability;
import io.casehub.api.model.Worker;
import io.casehub.engine.common.internal.history.EventLog;
import io.casehub.engine.common.internal.model.CaseInstance;
import io.casehub.engine.common.internal.utils.WorkerExecutionKeys;
import io.casehub.engine.common.spi.scheduler.WorkerExecutionManager;
import io.casehub.workers.common.PermanentFaultException;
import io.casehub.workers.common.WorkerCorrelationContext;
import io.casehub.workers.common.WorkerRetrySupport;
import io.casehub.workers.common.WorkflowCompletionPublisher;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpMethod;
import io.vertx.mutiny.core.buffer.Buffer;
import io.vertx.mutiny.ext.web.client.HttpRequest;
import io.vertx.mutiny.ext.web.client.HttpResponse;
import io.vertx.mutiny.ext.web.client.WebClient;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.LinkedHashMap;
import java.util.Map;
import org.jboss.logging.Logger;

@ApplicationScoped
public class McpWorkerExecutionManager implements WorkerExecutionManager {

    private static final Logger LOG = Logger.getLogger(McpWorkerExecutionManager.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    @Inject McpServerResolver serverResolver;
    @Inject McpSessionManager sessionManager;
    @Inject McpWorkerFaultPublisher faultPublisher;
    @Inject WorkflowCompletionPublisher completionPublisher;
    @Inject io.vertx.mutiny.core.Vertx vertx;

    WebClient webClient;

    @PostConstruct
    void init() {
        if (webClient == null) {
            webClient = WebClient.create(vertx);
        }
    }

    @Override
    public Uni<Void> submit(Long eventLogId, CaseInstance instance, Worker worker,
                            Capability capability, Map<String, Object> inputData) {
        String capTag = capability.getName();
        String serverName = McpServerResolver.parseServerName(capTag);
        String toolName = McpServerResolver.parseToolName(capTag);

        ResolvedMcpServer server;
        try {
            server = serverResolver.resolve(capTag);
        } catch (Exception e) {
            faultPublisher.fault(buildCtx(instance, worker, capability, inputData),
                capability, eventLogId, new PermanentFaultException(0, e.getMessage()));
            return Uni.createFrom().voidItem();
        }

        String idempotency = WorkerExecutionKeys.inputDataHash(
            instance.getUuid(), worker.getName(), capability.getName(), inputData);
        WorkerCorrelationContext ctx = new WorkerCorrelationContext(
            instance, worker, idempotency, instance.tenancyId);

        return sessionManager.getOrInitialize(serverName)
            .flatMap(session -> {
                long requestId = session.nextRequestId();

                ObjectNode body = OBJECT_MAPPER.createObjectNode();
                body.put("jsonrpc", "2.0");
                body.put("method", "tools/call");
                body.put("id", requestId);
                ObjectNode params = body.putObject("params");
                params.put("name", toolName);
                params.set("arguments", OBJECT_MAPPER.valueToTree(inputData));

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
                    .flatMap(response -> handleHttpResponse(response, session, serverName, requestId, ctx));
            })
            .onFailure().recoverWithUni(t -> {
                faultPublisher.fault(ctx, capability, eventLogId, t);
                return Uni.createFrom().voidItem();
            });
    }

    private Uni<Void> handleHttpResponse(HttpResponse<Buffer> response, McpSession session,
                                          String serverName, long requestId,
                                          WorkerCorrelationContext ctx) {
        int status = response.statusCode();

        if (status == 404) {
            if (session.hasSessionId()) {
                sessionManager.invalidate(serverName);
                throw new RuntimeException("MCP session expired (404) — will re-initialize on retry");
            } else {
                throw new PermanentFaultException(404, "MCP endpoint not found");
            }
        }
        if (status == 429) {
            throw WorkerRetrySupport.parseRetryAfter(
                response.getHeader("Retry-After"), status, response.statusMessage());
        }
        if (status >= 400 && status < 500) {
            throw new PermanentFaultException(status, status + " " + response.statusMessage());
        }
        if (status >= 500) {
            throw new RuntimeException(status + " " + response.statusMessage());
        }

        String contentType = response.getHeader("Content-Type");
        JsonNode jsonRpcResponse;

        try {
            if (contentType != null && contentType.contains("text/event-stream")) {
                jsonRpcResponse = parseSSE(response.bodyAsString(), requestId);
            } else {
                jsonRpcResponse = OBJECT_MAPPER.readTree(response.bodyAsString());
            }
        } catch (Exception e) {
            throw new RuntimeException("Malformed MCP response: " + e.getMessage(), e);
        }

        if (jsonRpcResponse == null) {
            throw new RuntimeException("No matching JSON-RPC response found in SSE stream");
        }

        return handleJsonRpcResponse(jsonRpcResponse, ctx);
    }

    private Uni<Void> handleJsonRpcResponse(JsonNode jsonRpc, WorkerCorrelationContext ctx) {
        if (jsonRpc.has("error")) {
            JsonNode error = jsonRpc.get("error");
            int code = error.path("code").asInt(0);
            String message = error.path("message").asText("Unknown error");
            if (code == -32600 || code == -32601 || code == -32602 || code == -32700) {
                throw new PermanentFaultException(0, "JSON-RPC error " + code + ": " + message);
            }
            throw new RuntimeException("JSON-RPC error " + code + ": " + message);
        }

        JsonNode result = jsonRpc.get("result");
        if (result == null) {
            throw new RuntimeException("Malformed JSON-RPC response: no result or error");
        }

        boolean isError = result.path("isError").asBoolean(false);
        if (isError) {
            String errorText = "";
            JsonNode content = result.get("content");
            if (content != null && content.isArray() && !content.isEmpty()) {
                errorText = content.get(0).path("text").asText("");
            }
            throw new RuntimeException("MCP tool returned isError: " + errorText);
        }

        Map<String, Object> output;
        JsonNode structuredContent = result.get("structuredContent");
        if (structuredContent != null && structuredContent.isObject()) {
            output = OBJECT_MAPPER.convertValue(structuredContent, MAP_TYPE);
        } else {
            JsonNode content = result.get("content");
            if (content != null) {
                output = Map.of("content", OBJECT_MAPPER.convertValue(content,
                    new TypeReference<java.util.List<Map<String, Object>>>() {}));
            } else {
                output = Map.of();
            }
        }

        completionPublisher.complete(ctx, output);
        return Uni.createFrom().voidItem();
    }

    private JsonNode parseSSE(String body, long expectedId) {
        if (body == null || body.isBlank()) return null;
        String[] events = body.split("\n\n");
        for (String event : events) {
            StringBuilder data = new StringBuilder();
            for (String line : event.split("\n")) {
                if (line.startsWith("data: ")) {
                    data.append(line.substring(6));
                } else if (line.startsWith("data:")) {
                    data.append(line.substring(5));
                }
            }
            if (data.isEmpty()) continue;
            try {
                JsonNode json = OBJECT_MAPPER.readTree(data.toString());
                if ((json.has("result") || json.has("error"))
                        && json.has("id") && json.get("id").asLong() == expectedId) {
                    return json;
                }
            } catch (Exception ignored) {}
        }
        return null;
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
}
```

- [ ] **Step 4: Run tests**

Run: `mvn --batch-mode test -pl workers-mcp -Dtest=McpWorkerExecutionManagerTest`
Expected: ALL PASS

- [ ] **Step 5: Commit**

```
feat(workers-mcp): McpWorkerExecutionManager — tools/call dispatch, dual response parsing, fault classification
```

---

## Task 7: Full build and integration verification

- [ ] **Step 1: Run all tests in the module**

Run: `mvn --batch-mode test -pl workers-mcp`
Expected: ALL PASS, 0 failures

- [ ] **Step 2: Run full project build**

Run: `mvn --batch-mode install`
Expected: BUILD SUCCESS across all modules (workers-common, workers-http, workers-camel, workers-github-actions, workers-mcp, workers-testing)

- [ ] **Step 3: Verify test count**

Run: `mvn --batch-mode test -pl workers-mcp 2>&1 | grep "Tests run:"`
Expected: reasonable test count covering all 5 test classes

- [ ] **Step 4: Commit spec update**

```
docs: workers-mcp design spec — MCP tool dispatch worker (Approved rev 2, 5 review cycles)
```

---

## Task Summary

| Task | Description | Classes |
|------|-------------|---------|
| 1 | Module scaffold | POM, constants, record, McpSession |
| 2 | McpServerResolver | Config parsing, N:1 tag resolution |
| 3 | McpSessionManager | Session lifecycle, concurrent dedup |
| 4 | Fault pipeline | FaultPublisher + FaultEventHandler |
| 5 | Provisioner | Capability probe |
| 6 | Execution manager | Dispatch, response parsing, completion |
| 7 | Integration verify | Full build, all tests green |
