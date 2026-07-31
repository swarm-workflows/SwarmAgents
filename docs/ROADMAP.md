# SwarmAgents Roadmap

Identified improvement areas based on codebase analysis, organized by priority.

## Recently Landed

- **Gossip consensus stack** — SWIM membership, epidemic dissemination, Snow/Snowball engine (`consensus.protocol: snow`) with per-peer batching and congestion control; scalability Phases 0-4 (broadcast fan-out, Redis batching/registry discovery, backlog drain, gossip-fed selection state) validated at scale
- **Contextual bandit delegation** — LinUCB/LinTS (`mab.algorithm: linucb`), deployment-validated (Scenarios A/B/C incl. outage/rejoin fixes)
- **Hybrid quantum-classical jobs (Phases 1-2)** — `QuantumSpec`/`QuantumBackend`, qubit-aware feasibility/cost, `--split-hybrid` producer/consumer co-scheduling over a Redis-streams measurement layer
- **Pegasus replay pipeline** — `pegasus_profile_extractor.py` + converter `--data-nodes per-file` / `--dtn-map` / `--dtn-names`; see `docs/PEGASUS_TO_SWARM.md`

## Next Up

- **Quantum Phase 3** — joint pair consensus for split jobs, VQE-style variational runtime, OpenQASM circuit payloads
- **Wall-time-faithful replay** — `Job.execute()` sleeps a flat 1s (`time.sleep(wt)` disabled); add a configurable `wall_time_scale` so replayed workloads exercise realistic execution times and load spreading (see item 7)
- **DAG dependency gating** — the Pegasus converter flattens workflows into an independent job pool; honor `jobDependencies` (e.g. via the data-predicate mechanism) so replayed makespans are comparable to Pegasus
- **Bandit tuning** — `ts_variance` sweep for LinTS; error bars over batched scenario runs

---

## Critical

### 1. Test Coverage Expansion
**Current state:** `tests/` covers consensus (Snow, PBFT pending/out-of-order, broadcast), SWIM, gossip dissemination, bandits/MAB manager, failure simulation, quantum (models + Phase 2), and the Redis repository.

**Remaining gaps (by priority):**
| Module | Why Critical |
|--------|-------------|
| `swarm/selection/engine.py` | Cost matrix computation, thresholded selection, cache invalidation |
| `swarm/agents/resource_agent.py` | Adapter pattern, cost computation, failure detection |
| `swarm/comm/grpc_client.py` | Channel pool, retry logic, health checks |
| `swarm/topology/topology.py` | Neighbor computation, routing for all topology types |
| `swarm/models/job.py` | State transitions, lifecycle timestamps, job classification |
| `swarm/queue/object_queue.py` | Thread-safe queue operations |

Also: `test_bandit.py::TestStepSize::test_greedy_uses_recency_estimate` is seed-dependent and flakes intermittently — needs a fixed seed or a tolerance.

### 2. TLS/SSL for gRPC
All gRPC channels use `grpc.insecure_channel()`. No encryption or authentication for inter-agent communication.

**Recommendation:** Add optional TLS support with configurable certificate paths in `config_swarm_multi.yml`.

### 3. Redis Retry Limits
`repository.py` uses an infinite `while True` retry loop on `WatchError` with no backoff or maximum retry count. Under high contention this could spin indefinitely.

**Location:** `swarm/database/repository.py:77-99`

**Recommendation:** Add exponential backoff and a configurable max retry limit.

---

## High Priority

### 4. Hardcoded Colmena Connection
Docker bridge IP and port are hardcoded in `colmena_agent.py:467-468`:
```python
host = "172.17.0.1"  # Docker bridge IP
port = 50055         # Hardcoded...
```

**Recommendation:** Move to configuration file.

### 5. Silent Exception Handling
Several `except Exception: pass` blocks silently swallow errors:
- `swarm/comm/grpc_server.py:97` — server shutdown health check failure
- `swarm/agents/resource_agent.py:1788` — job execution callback
- `baselines/scheduler.py:237-239` — baseline job execution

**Recommendation:** Add logging at `WARNING` level minimum in all catch blocks.

### 6. Temporary Hack in Selection
`resource_agent.py:1313` contains a `# TEMP HACK` that restricts hierarchical agents (level > 0) to only evaluate themselves in the cost matrix. This bypasses the normal multi-agent selection for parent nodes.

**Recommendation:** Either formalize this as the intended design for hierarchical agents and document it, or implement proper multi-agent evaluation for parent-level selection.

### 7. Data Transfer + Execution Simulation
`Job.execute()` sleeps a flat 1s — `time.sleep(wt)` is commented out — and the staged-in/staged-out transfer TODOs remain. Flat-1s execution also distorts load balance (one agent can win every election because utilization never rises) and makes replayed makespans meaningless.

**Recommendation:** Add a configurable `wall_time_scale` (1.0 = faithful, 0 = current stub) and implement transfer time simulation using `data_in`/`data_out` nodes — `DataNode.size_bytes` (populated by the Pegasus pipeline) gives real transfer volumes; `transfer_in_time`/`transfer_out_time` fields already exist on the Job model.

### 8. Remove Backup Code
`swarm/agents/bkp/` contains 3 old agent versions (`agent_grpc_v0.py`, `resource_agent_v0.py`, `resource_agent_v1.py`) that are no longer referenced.

**Recommendation:** Archive to a git tag or remove entirely — git history preserves them.

---

## Medium Priority

### 9. Event-Driven Threading
66 instances of `time.sleep()` for polling across the codebase. Key polling loops:
- `selection_main()` and `scheduling_main()` use `sleep(0.5)` between iterations
- `_do_inbound()` polls message queue every 0.5s
- `_do_periodic()` polls every 0.5s

**Recommendation:** Replace with `threading.Event.wait(timeout)` for lower latency and CPU usage. The `condition` variable already exists in `agent_grpc.py` but isn't used consistently.

### 10. Magic Numbers
Scattered numeric constants without named definitions:
- gRPC retry delays: `0.05`, `0.8` (`grpc_client.py:172-179`)
- Health check timeout: `0.7` (`grpc_client.py`)
- Health check interval: `2s` (`grpc_client.py`)
- Default executor workers: `3` (`resource_agent.py:139`)
- Various `sleep(0.5)` intervals

**Recommendation:** Extract to named constants or configuration parameters.

### 11. Job Lock Contention
Every property access on `Job` acquires `self.lock` (RLock) — 30+ locked property accessors. This may cause contention when multiple threads access the same job object.

**Recommendation:** Profile under load. Consider using a copy-on-read pattern or reducing lock granularity for read-only properties.

### 12. Print Statements in Production Code
`swarm/consensus/engine.py:287` uses `print()` in leader election path. Several other core modules have print statements that should use the logging framework.

**Recommendation:** Replace all `print()` in `swarm/` package with `logger.debug()` or `logger.info()`.

### 13. Peer Parent Feasibility Estimation
When evaluating peer parent agents in hierarchical topology, feasibility uses aggregate metrics (`max_child_capacity`, aggregated DTNs) which may not reflect actual child capabilities. This can cause cascading delegation failures.

**Location:** `resource_agent.py:1048-1073`

**Current mitigation:** Delegation monitoring + reassignment recovers from incorrect estimates.

**Recommendation:** Consider adding a "delegation confidence" score or a pre-delegation feasibility query to children.

### 14. Agent Recovery — DONE
Implemented via `RecoveryTracker` (`swarm/utils/recovery.py`) + wiring in `resource_agent.py`. Three rejoin signals: heartbeat progress (record written *after* the failure timestamp, fresh for `recovery_grace_seconds`), gRPC channel-up, and SWIM alive/joined — all funnel through `_on_agent_recovered`, and the agent re-enters neighbor maps/quorum via the normal refresh path. A `recovery_cooldown_seconds` window keeps stale channel-DOWN events from flapping a just-recovered agent while its reconnect settles. Unit tests in `tests/test_agent_recovery.py`; validated end-to-end (SIGKILL → detect 1s → 0 spurious recoveries while dead → rejoin ~7s after restart, one clean recovery per peer).

Remaining nuance: recovery trusts the restarted process; there is no state resync for jobs the agent held pre-crash (they are reassigned by the failure path).

---

## Low Priority

### 15. Configurable Selection Threshold in ColmenaAgent
`colmena_agent.py:510` hardcodes `threshold_pct=10.0` with a TODO comment to read from config.

### 16. JSON Schema Validation
All Redis I/O uses raw JSON without schema validation. External inputs (job definitions, agent profiles) are loaded with `json.load()` directly.

**Recommendation:** Add Pydantic validation at system boundaries (job ingestion, config loading).

### 17. `ast.literal_eval` Usage
`plotting/analyze_single_run.py:101` uses `ast.literal_eval()` instead of `json.loads()` for parsing. While safer than `eval()`, `json.loads()` is preferred.

### 18. Centralized Simulation Improvement
`swarm_job_selection_simulation.py:302` has a TODO to switch from centralized `distribute_jobs` to per-agent job checking with global state access.

---

## Feature Ideas

### Adaptive Cost Weights
Currently, cost weights (`cpu=0.4, ram=0.3, disk=0.2, gpu=0.1`) are static per config. Consider learning optimal weights based on cluster utilization patterns over time.

### Multi-Objective Selection
Current selection optimizes a single scalar cost. Consider Pareto-optimal selection for multi-objective scenarios (minimize latency + maximize utilization + balance load).

### Consensus Protocol Variants
The PBFT-like protocol works well for moderate agent counts, and the Snow/Avalanche gossip engine (implemented — see `docs/GOSSIP_CONSENSUS_DESIGN.md`) covers large deployments. Remaining idea:
- **Raft-based consensus** for stronger leader-based ordering

### Observability Dashboard
Metrics are exported to JSON/CSV files. Consider adding:
- Real-time Prometheus/Grafana integration
- Distributed tracing (OpenTelemetry) for end-to-end job lifecycle visibility

### Formal Python Packaging
No `setup.py` or `pyproject.toml` exists. Adding proper packaging would enable:
- `pip install -e .` for development
- Versioned releases
- Dependency pinning via `poetry` or `pip-tools`
