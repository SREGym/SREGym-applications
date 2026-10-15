# Agentic Retry Platform

An autonomous-agent benchmark application for SREGym that models compounded, multi-layer retry amplification and metastable overload.

## Architecture

- **Agent Orchestrator** (1 replica): planner and supervisor with workflow metadata and retry budgets in Redis.
- **Tool Gateway** (1 replica): has a 1-second overall tool deadline, nested tool/transport retries, and cancellation forwarding.
- **Data API** (1 replica): fault-injectable query service with an explicitly labelled finite worker queue.
- **PgBouncer and PostgreSQL**: provide a real database path and a 25-connection physical pool.
- **Redis**: stores workflow metadata, retry budget state, and synchronized fault state; it is not described as a durable work queue.

Service readiness is dependency-aware: Data API checks Redis and PgBouncer; the gateway checks Data API and Redis; the orchestrator checks gateway and Redis. Runtime code lives only in `helm/files/` and is loaded into the chart ConfigMap from there.

## Overload dynamics

A transient backend latency increase can cause caller deadlines to expire. Without cancellation propagation, earlier physical work continues while speculative replanning and nested retries issue replacements. The accumulated Data API queue and orphaned work can sustain overload after the initial disturbance is removed. Mitigation enables cancellation, a retry budget, admission control, and single-attempt retry layers.
