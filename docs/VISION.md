# Vision — streaming-project

## What the platform is

**streaming-project ingests events from external APIs, normalizes them into a common
envelope, aggregates them into windowed analytics, stores them for search, and exposes
them for visualization and alerting.**

Its defining property is that **a source is data, not code**. Registering a new source,
declaring how its payload maps onto the common envelope, and declaring what to compute
over it are all authoring exercises against a contract the platform interprets. They are
not Python changes, and they are not a deploy.

The platform grows in Python only when it gains a genuinely new *capability* — a new
transport, a new transform primitive, a new sink. It does not grow when it gains a new
source.

---

## The stages

| Stage | What it does |
| --- | --- |
| **Ingestion** | Pulls or receives events from an external API and wraps each one in a common envelope, without interpreting its meaning |
| **Normalization** | Maps a source's own payload shape onto the platform's shared vocabulary, driven by a per-source contract |
| **Analytics** | Computes windowed aggregations and derived classifications over normalized events |
| **Storage** | Keeps normalized events searchable, so an individual observation can be retrieved and not only its aggregate |
| **Visualization** | Presents what the platform produces, and raises alerts on conditions worth reacting to |

Each stage is independently deployable and communicates with the next through a durable
log. No stage knows which source an event came from beyond what the envelope declares.

---

## The central principle

> **Adding a source is configuration. Adding a capability is code. The two must never
> be confused.**

Everything else in this document follows from that sentence, including the parts of the
stack that have nothing to do with data processing.

A platform that makes this claim only at the contract layer has made it halfway. If a
new source is a YAML file but shipping it still requires editing a shared deployment
file and restarting the stack, then the claim holds when writing the contract and
breaks when running it. Closing that gap is a requirement, not a refinement.

---

## Technology and its role

| Technology | Role in the platform | Status |
| --- | --- | --- |
| **Kafka** | Durable transport between every stage; the boundary each stage reads from and writes to | In use |
| **Flink** | Processing engine for both normalization and analytics | In use |
| **OpenSearch** | Searchable store for normalized events; makes an individual observation retrievable, not just its aggregate | Planned |
| **Grafana** | Visualization and alerting over what the platform produces | Planned |
| **Kubernetes** | Orchestration; makes "a source is configuration" true at deployment time, not only at authoring time | Planned |
| **ArgoCD** | Continuous delivery; reconciles that configuration from Git automatically, so declaring a source and running it are the same act | Planned |
| **Terraform** | Infrastructure provisioning | Opportunistic — adopted only if the project provisions real cloud resources |

Alongside these, one goal is not a technology: **the ability to plan an architecture and
choose its components**, including the judgment to decide when a component is not
warranted yet.

---

## What this platform is not

- **Not a managed service.** It runs under a single operator.
- **Not production-hardened.** Correctness is taken seriously; availability is not.
- **Not a general-purpose ETL tool.** It is opinionated about streaming, about the shape
  of its envelope, and about contracts being declarative.
- **Not a library.** Nothing here is meant to be imported by another project.

---

## When this document changes

This is a photograph of the intended shape of the project, not a status report.

It changes when the plan itself changes — a technology is replaced, a stage is added or
removed, or the central principle above stops being what the project is trying to prove.

It does **not** change when a milestone is reached, a bug is fixed, a feature ships, or a
component is finally built. Those are tracked elsewhere in the repository.
