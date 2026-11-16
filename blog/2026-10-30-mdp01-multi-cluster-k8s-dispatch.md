---
layout: post
title: "Multi-Cluster K8s Dispatch and the Pattern That Made It Trivial"
date: 2026-10-30
entry_type: article
subtype: diary
projects: [casehubio/workers]
tags: [kubernetes, architecture, workers, multi-cluster, patterns]
---

# Multi-Cluster K8s Dispatch and the Pattern That Made It Trivial

A case engine that dispatches work to Kubernetes has a natural limitation: it talks to one cluster. The CDI-injected `KubernetesClient` is a singleton. Every Job definition resolves to the same API server, regardless of where the work should actually run. For a development environment or a single-region deployment, this is fine. For anything beyond that — production and staging clusters, regional isolation, GPU workloads on a dedicated cluster — it's a wall.

The interesting part isn't the multi-cluster feature itself. It's that we already had the answer, sitting in a different worker module, waiting to be recognised.

## The shape of the problem

CaseHub's worker architecture is a set of thin dispatch modules. Each one translates a case step into the target runtime's protocol — HTTP calls, Camel routes, MCP tool invocations, Kubernetes Jobs — and fires a completion event when done. The K8s worker, built in v1 around a single `KubernetesClient`, had three consumers of that singleton:

- **K8sWorkerRuntime** — checks cluster connectivity at startup, starts informers
- **K8sWorkerExecutionManager** — creates Jobs via `client.resource(job).create()`
- **K8sJobInformerManager** — watches for Job completion via `SharedIndexInformer<Job>`, reads Pod logs, deletes finished Jobs

Every one of those uses the same client. Extending to multiple clusters means each needs to resolve the *right* client for the *right* cluster at the *right* time.

![Single-cluster architecture — all paths through one KubernetesClient](images/single-cluster-k8s.svg)

## The MCP precedent

The MCP worker already solved this problem. MCP servers are inherently multi-endpoint — you configure named servers, each with its own URL, authentication headers, and session state. The MCP worker manages this through three coordinating types:

- `McpServerResolver` — maps capability tags to named servers
- `McpSessionManager` — manages per-server session lifecycle, with `ConcurrentHashMap` dedup and memoised initialisation
- `McpWorkerRuntime` — initialises servers in parallel via `Uni.join().all()`, isolates per-server failures via `ServerInitResult`, enters RUNNING if at least one server succeeds

The shape — resolver → per-endpoint client manager → parallel init with partial-failure tolerance → per-endpoint dispatch — is not MCP-specific. It's a general pattern for any worker that talks to multiple instances of the same kind of thing.

![The multi-endpoint pattern — same shape across MCP and K8s](images/multi-endpoint-pattern.svg)

## Applying the pattern to K8s

The K8s multi-cluster implementation follows the MCP shape almost exactly:

**`K8sClusterManager`** replaces the singleton `KubernetesClient` injection. It reads `casehub.workers.k8s.clusters.<name>.context` from config, creates a `KubernetesClient` per named kubeconfig context via `KubernetesClientBuilder`, and provides `clientForJob(definition)` — which reads the nullable `cluster` field from `JobDefinition` and returns the matching client, falling back to the default CDI-injected client when no cluster is specified.

**`JobDefinition.cluster()`** is the routing key. It's nullable. `null` means "use the default client" — the same client v1 always used. Existing configurations continue to work without changes.

**`K8sJobInformerManager.start()`** now accepts `Map<String, Set<String>>` — cluster name to namespaces — instead of a flat `Set<String>` of namespaces. Informers are keyed by `"cluster:namespace"` composite. The per-cluster `KubernetesClient` is captured in the `ResourceEventHandler` closure and threaded through every follow-up operation: `processTerminal()`, `findLastPod()`, log capture, Job deletion. The informer doesn't need to look up which client to use later — it already has it.

**`K8sWorkerRuntime.initialize()`** iterates over clusters, checks connectivity per-cluster, and enters RUNNING if at least one is reachable. A staging cluster going down doesn't take the runtime offline.

![Multi-cluster architecture — per-cluster clients, informers, and partial-failure tolerance](images/multi-cluster-k8s.svg)

## Why partial failure matters

The MCP worker taught us something worth generalising: a multi-endpoint worker should not gate all dispatch on all endpoints being healthy. If you configure three MCP servers and one is temporarily unreachable, the other two should still work. The same applies to K8s clusters.

`K8sWorkerRuntime` checks each cluster's connectivity individually. Unreachable clusters get logged and skipped. Informers start only for reachable clusters. If a Job targets an unreachable cluster at dispatch time, it faults through the normal retry pipeline — the same pipeline that handles API server errors, invalid specs, and OOM kills. No special case needed.

This is the operational property that matters most in production: a configuration-time decision (adding a cluster) cannot create a runtime-time failure (all dispatch stops). The blast radius is bounded to the cluster that's actually down.

## What the config looks like

```properties
# Named clusters — point to kubeconfig contexts
casehub.workers.k8s.clusters.prod.context=prod-eks-context
casehub.workers.k8s.clusters.staging.context=staging-context

# Jobs reference clusters
casehub.workers.k8s.jobs.ml-training.cluster=prod
casehub.workers.k8s.jobs.ml-training.image=ml-pipeline:latest
casehub.workers.k8s.jobs.ml-training.namespace=ml-jobs

# Jobs without cluster use the default — backwards compatible
casehub.workers.k8s.jobs.data-export.image=exporter:latest
casehub.workers.k8s.jobs.data-export.namespace=batch
```

The operational model is straightforward: add a cluster definition, reference it from job definitions. Jobs that don't reference a cluster work exactly as before.

## Patterns are infrastructure

The real value here isn't multi-cluster K8s dispatch. It's the recognition that the MCP worker's multi-server architecture was never MCP-specific — it was a general solution to "this worker type talks to N instances of the same kind of thing." Resolver, client manager, parallel init with partial failure, per-endpoint dispatch. The same four responsibilities, the same structural relationships, reused without modification.

When the next worker type needs multi-endpoint support — multiple Camel contexts, multiple GitHub organisations, multiple scenario engine instances — the pattern is already proven. The question isn't "how do we design this?" It's "which existing worker module do we copy the shape from?"

That's the difference between a pattern documented in a wiki and a pattern embedded in running code. The wiki version tells you what to do. The code version shows you that it works, handles the edge cases you haven't thought of yet, and comes with tests.
