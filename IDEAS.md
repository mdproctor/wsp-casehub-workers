# Idea Log

Undecided possibilities — things worth remembering but not yet decided.
Promote to an ADR when ready to decide; discard when no longer relevant.

---

## 2026-06-11 — MCP proxy/aggregator for LLM agents

**Priority:** medium
**Status:** active

CaseHub exposes itself as an MCP server, aggregating tools from multiple backend MCP servers behind a single surface. LLM agents connect to CaseHub as one MCP server and get access to all configured integrations. Inbound complement to the outbound MCP worker — the worker calls out to MCP servers, the proxy lets MCP clients call in.

**Context:** Arose during MCP worker design discussion. If CaseHub already manages connections to multiple MCP servers (for the worker), exposing those as a unified MCP surface for LLM agents is a natural extension. Separate module from the worker.

**Promoted to:**
