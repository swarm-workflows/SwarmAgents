# SwarmAgents Roadmap

Identified improvement areas based on codebase analysis, organized by priority.

---

## Critical

### 1. Test Coverage Expansion
**Current state:** Only `tests/test_bandit.py` exists (~2% module coverage).

**Missing tests (by priority):**
| Module | Why Critical |
|--------|-------------|
| `swarm/consensus/engine.py` | Core consensus logic — dominance checks, quorum, out-of-order messages |
| `swarm/selection/engine.py` | Cost matrix computation, thresholded selection, cache invalidation |
| `swarm/database/repository.py` | Redis transactions, state index consistency, optimistic locking |
| `swarm/agents/resource_agent.py` | Adapter pattern, cost computation, failure detection |
| `swarm/comm/grpc_client.py` | Channel pool, retry logic, health checks |
| `swarm/topology/topology.py` | Neighbor computation, routing for all topology types |
| `swarm/models/job.py` | State transitions, lifecycle timestamps, job classification |
| `swarm/queue/object_queue.py` | Thread-safe queue operations |

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

### 7. Data Transfer Simulation
`job.py:358,367` has TODO comments for staged-in/staged-out data transfers. The `execute()` method currently uses `time.sleep(1)` instead of the job's actual `wall_time`, and data transfer simulation is not implemented.

**Recommendation:** Implement data transfer time simulation using `data_in`/`data_out` nodes with `transfer_in_time`/`transfer_out_time` fields (already defined in the Job model).

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

### 14. Agent Recovery
`resource_agent.py:2256` has a TODO for agent recovery when `enable_agent_recovery` is True. Currently, failed agents are only detected and removed — there is no mechanism to rejoin after recovery.

**Recommendation:** Implement agent rejoin protocol: re-announce via heartbeat, re-enter neighbor maps, re-add to quorum.

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
The current PBFT-like protocol works well for moderate agent counts. Consider implementing:
- **Raft-based consensus** for stronger leader-based ordering
- **Gossip-based protocol** for very large deployments (1000+ agents)

### Observability Dashboard
Metrics are exported to JSON/CSV files. Consider adding:
- Real-time Prometheus/Grafana integration
- Distributed tracing (OpenTelemetry) for end-to-end job lifecycle visibility

### Formal Python Packaging
No `setup.py` or `pyproject.toml` exists. Adding proper packaging would enable:
- `pip install -e .` for development
- Versioned releases
- Dependency pinning via `poetry` or `pip-tools`
