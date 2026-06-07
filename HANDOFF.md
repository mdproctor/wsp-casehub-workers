# Handoff — 2026-06-07 (Bootstrap)

**Head commit (project):** a40786f — chore: bootstrap casehub-workers repo
**Head commit (workspace):** (initial — this commit)

---

## Context — Why This Repo Exists

This repo was created at the end of a design session in `casehubio/parent`. The session explored:

1. A generalised **Desired State Management (DSM)** system (research doc in parent: `docs/superpowers/research/2026-06-07-desired-state-management-research.md`)
2. The need for a **workers collection** — different execution runtimes for CaseHub case steps, beyond claudony (Claude CLI) and casehub-openclaw (OpenClaw)
3. A **casehub-endpoints** module for platform — a named endpoint registry to decouple workers from hardcoded connection details

The session established that claudony and openclaw are *integration platforms* (own repos). Workers like HTTP, Camel, Script are *thin SPI implementations* that belong together in one multi-module repo.

---

## Immediate Next Step

**Brainstorm the Camel worker design.**

The Camel worker is priority 2 (HTTP is 1, but Camel is the one that justifies the repo's existence — 300+ connectors). The brainstorm should produce a spec for `workers-camel` covering: SPI implementation approach, route configuration model, result mapping, and how it integrates with the `casehub-endpoints` registry once that ships from platform.

Run `/brainstorm` or invoke `superpowers:brainstorming` at the start of the session. The spec lands in `docs/superpowers/specs/`.

After the Camel brainstorm, the endpoint registry spec (also from this session) goes to `casehubio/platform` for implementation.

---

## Worker Candidates — Full Table

Produced in the `casehubio/parent` design session. Use this as the starting point for the brainstorm.

| Priority | Worker | Enterprise Reach | Impl Complexity | CaseHub Fit | Gap Filled | Quarkus Native | Justification |
|----------|--------|-----------------|-----------------|-------------|------------|----------------|---------------|
| 1 | **HTTP/Webhook** | High — any HTTP service | Low | High | Yes — broadest compat | Yes (RESTEasy) | Prove the framework works; every system has an HTTP endpoint |
| 2 | **Camel** | Very High — 300+ connectors | Medium | High | Yes — enterprise integration | Yes (quarkus-camel) | Kafka, AWS, Salesforce, SAP, FTP, DB all become workers without custom code |
| 3 | **MCP** | High — any MCP server | Low-Med | Very High | Yes — extends AI surface | Yes (Quarkus MCP) | Any MCP tool becomes a dispatchable CaseHub worker; natural fit |
| 4 | **Script** | Medium | Low | High | Yes | Partial (subprocess) | Shell/Python/JS for automation, data processing, glue code |
| 5 | **GitHub Actions** | Medium | Low | High | Yes | No (REST API) | Trigger GH Actions as case steps — directly relevant to devtown |
| 6 | **K8s Job** | High — cloud-native | Medium | High | Yes | No (k8s client) | Any containerised workload as a case step |
| 7 | **Ansible** | Medium-High | Medium | High — strategic | Yes | No (Ansible Runner) | Execute Ansible playbooks; natural execution layer for DSM transition plans |
| 8 | **AWS Lambda** | Medium | Low | Medium | Partial (Camel covers) | No (AWS SDK) | FaaS dispatch for AWS-heavy deployments |
| 9 | **Temporal** | Low-Med | High | Medium | Partial | No | Durable sub-workflows via Temporal.io |
| 10 | **gRPC** | Medium | Medium | Medium | Partial | Yes (quarkus-grpc) | Microservices integration |

---

## Endpoint Registry Concept

Also designed in the parent session. A `casehub-endpoints` module is planned for `casehub-platform` (issue filed — check `casehubio/platform` issues).

**The problem:** Workers currently hardcode connection details (URLs, credentials, topic names). This makes configuration fragile and prevents the DSM system from managing worker topology declaratively.

**The solution:** A named endpoint registry. Workers reference endpoints by `Path`-based name; the registry resolves connection details at runtime.

**Endpoint levels:**

| Level | Example Path | Scope | Mutable? |
|-------|-------------|-------|---------|
| System | `external/salesforce/prod` | Tenant-global | Rarely |
| Service | `casehubio/qhorus/api` | Platform-global | No |
| Worker | `workers/camel/lead-enrichment` | Tenant | Yes |
| Agent | `agents/claude:analyst@v1` | Tenant | Yes — lifecycle |
| Case | `cases/{id}/work` | Case-instance | Yes — open/close |
| Human | `humans/alice@casehubio.com` | Tenant | No |

**Preliminary SPI (to be designed in platform session):**

```java
interface EndpointRegistry {
    void register(EndpointDescriptor endpoint);
    Optional<EndpointDescriptor> resolve(String path, String tenancyId);
    List<EndpointDescriptor> discover(EndpointQuery query);
    void deregister(String path, String tenancyId);
}
```

The `Path` type from `casehub-platform-api` is the addressing primitive.

**Workers + endpoints interplay:** The Camel worker brainstorm should assume the endpoint registry will exist. A Camel route worker config should reference `endpoint: external/kafka/prod-cluster` rather than embedding the broker URL. Design the worker to accept both (endpoint name OR inline config) so it works before platform ships the registry.

---

## Key Architecture Context for Brainstorming

### Where workers fit in the CaseHub tier structure

```
Foundation     platform, ledger, work, qhorus, connectors, iot, eidos, neural-text
Orchestration  casehub-engine          ← defines WorkerProvisioner SPI
Integration    claudony, openclaw, casehub-workers  ← implements WorkerProvisioner
Application    devtown, aml, clinical, life
```

### SPIs to implement (all in casehub-engine-api)

- `WorkerProvisioner` — provision/deprovision a worker. **`postToChannel` is 6-param** (engine#343).
- `CaseChannelProvider` — open channels for the worker.
- `WorkerStatusListener` — receive lifecycle events.
- `WorkerContextProvider` — supply context at invocation.

### How claudony does it (reference implementation)

claudony provisions Claude CLI tmux sessions. The `ClaudonyWorkerExecutionManager` watches for session exit via `WorkflowExecutionCompleted`. Workers report back via `CaseSignalSink` or channel dispatch.

For an HTTP worker: provision = configure the endpoint; execute = POST to the endpoint; complete = receive the webhook callback and fire `WorkflowExecutionCompleted`.

For a Camel worker: provision = register the route; execute = send a message to the route's entry endpoint; complete = receive the route's output exchange and map to `WorkflowExecutionCompleted`.

### Connection to Desired State Management

The DSM session (research doc in parent) established that workers are provisioned as nodes in a desired-state graph. The `casehub-deployment` domain (planned Integration-tier repo) will declare "I want a Camel worker provisioned at endpoint `workers/camel/lead-enrichment`" and the DSM system provisions it. Workers in this repo are the provisioning targets.

---

## What's Left (from parent session)

- `casehubio/platform` — implement `casehub-endpoints` module (issue filed)
- `casehubio/parent` — add casehub-workers to build dependency order in PLATFORM.md
- This repo — brainstorm workers-camel (immediate next step)
- This repo — brainstorm workers-http (after camel, simpler)
- Future — MCP worker, Script worker, GitHub Actions worker

---

## Key References

- Research doc: `casehubio/parent` `docs/superpowers/research/2026-06-07-desired-state-management-research.md`
- Engine SPI reference: `casehub-engine-api` — WorkerProvisioner, CaseChannelProvider
- Claudony reference impl: `casehubio/claudony` casehub/ module
- Platform endpoint issue: `casehubio/platform` (check issues for "endpoints" or "registry")
- PLATFORM.md: `casehubio/parent docs/PLATFORM.md` — tier structure, capability ownership
