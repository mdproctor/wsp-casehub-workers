# Handoff — 2026-06-18 (EndpointRegistry wiring)

**Head commit (project):** 7b4a5e1 — feat(#12): wire EndpointRegistry as Tier 3 in McpServerResolver
**Head commit (workspace):** f4a910e — archive(issue-12-endpoint-registry-wiring): move plans to attic

---

## What Happened

Wired EndpointRegistry SPI (platform#73) into workers-http and workers-mcp as Tier 3 endpoint resolution (#12). Changed `WorkerCapabilityResolver` to be tenancy-aware — `resolve()` and `firstMatch()` now take `tenancyId`. All four resolver implementations updated (HTTP and MCP gain registry integration; Camel and Script pass through). Also fixed upstream SNAPSHOT break — `WorkflowExecutionFailed` removed from engine-common, replaced with local `WorkerFaultEvent` record (fix #9). Filed engine#530 (tenancyId on ProvisionContext) and engine#531 (getCapabilities hard gate). Design spec went through 2 review cycles with 7 findings.

---

## Immediate Next Step

**Pick the next work item.** Options: K8s worker (#10), composite `WorkerExecutionManager` in engine (engine#461), or engine#530/engine#531 (prerequisite for full tenant-aware provisioning).

---

## Cross-Module

**Blocked by:**
- `casehub-engine` — engine#461 composite `WorkerExecutionManager` for co-deploying multiple workers · M · Med
- `casehub-engine` — engine#530 add tenancyId to ProvisionContext · XS · Low
- `casehub-engine` — engine#531 remove getCapabilities() hard gate · XS · Low

---

## What's Left

- Upstream SNAPSHOT break: `Worker` constructor went private — fixed in tests, `WorkerTestSupport` review pending · XS · Low

---

## What's Next

| # | Description | Scale | Complexity | Notes |
|---|-------------|-------|------------|-------|
| #10 | workers-k8s — Kubernetes Job dispatch worker | L | Med | Watch-based completion model |
| — | Composite WorkerExecutionManager in engine | M | Med | engine#461 — required for co-deployment |
| — | engine#530 + engine#531 | XS | Low | Prerequisite for full tenant-aware provisioning |

---

## Key References

- Spec: `docs/superpowers/specs/2026-06-18-endpoint-registry-wiring-design.md`
- Blog: workspace `blog/2026-06-18-mdp05-tenancy-aware-endpoint-resolution.md`
- Garden: GE-20260618-a50133 (upstream SNAPSHOT record deletion gotcha)
