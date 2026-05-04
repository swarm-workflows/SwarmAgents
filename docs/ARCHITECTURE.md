# SwarmAgents Architecture

## Overview

SwarmAgents is a distributed job scheduling system where autonomous agents reach consensus on job assignments using a PBFT-like protocol with cost-based selection. The system is designed around five decoupled layers connected by dependency injection and adapter patterns.

```
                    +-----------------------+
                    |    Agent Layer        |
                    | (ResourceAgent,       |
                    |  LLMAgent, Colmena)   |
                    +-----------+-----------+
                          |   adapters
               +----------+----------+
               |                     |
    +----------v--------+  +--------v-----------+
    | Consensus Layer   |  |  Selection Layer   |
    | (PBFT protocol)   |  |  (cost matrix +    |
    |                   |  |   LRU caching)     |
    +----------+--------+  +--------+-----------+
               |                     |
    +----------v---------------------v-----------+
    |          Communication Layer               |
    |          (gRPC + protobuf)                 |
    +--------------------+-----------------------+
                         |
    +--------------------v-----------------------+
    |          Data Layer                        |
    |          (Redis persistence)               |
    +--------------------------------------------+
```

## Layer Details

### 1. Agent Layer (`swarm/agents/`)

**Class Hierarchy:**
```
Agent (agent_grpc.py) — abstract base, implements Observer
 ├── ResourceAgent (resource_agent.py) — rule-based cost computation
 │    └── LlmAgent (llm/llm_agent.py) — LLM-enhanced, inherits ResourceAgent
 └── ColmenaAgent (colmena_agent.py) — Colmena workflow integration
```

**Base Agent** (`agent_grpc.py`) provides:
- gRPC transport lifecycle management
- Redis persistence via `Repository`
- Topology management and peer discovery via heartbeats
- Thread-safe data structures (`neighbor_map`, `children`, `parents`, `failed_agents`)
- Message queue management (`AgentQueues`)
- Abstract hooks: `_process()`, `on_periodic()`, `should_shutdown()`

**ResourceAgent** (`resource_agent.py`) adds:
- Cost-based job selection with configurable weights
- PBFT consensus integration via adapter classes
- Job lifecycle management (selection, scheduling, execution)
- Failure detection and job reassignment
- MAB-based hierarchical delegation

**LlmAgent** (`llm/llm_agent.py`) extends ResourceAgent:
- LLM-driven cost computation with peer context awareness
- Decentralized bidding (each agent evaluates only itself via LLM)
- Graceful fallback to analytical cost on LLM failure
- Audit trail of LLM scores persisted to Redis

**ColmenaAgent** (`colmena_agent.py`):
- External consensus trigger via gRPC RPC (`TriggerConsensus`)
- Simpler model without continuous selection loop

### 2. Consensus Layer (`swarm/consensus/`)

Framework-agnostic PBFT-like protocol using dependency injection via Protocol interfaces.

**State Machine:**
```
PENDING → PRE_PREPARE → PREPARE → COMMIT → COMPLETE
```

**Protocol Flow:**
1. **Propose** — Agent calls `engine.propose()` with job proposals; broadcasts `PROPOSAL` to peers
2. **Dominance Check** — Peers compare incoming proposals against existing ones. Lower cost wins; ties broken by lower `agent_id`
3. **Prepare** — Accepted proposals trigger `PREPARE` broadcast; engine accumulates votes
4. **Commit** — When `len(prepares) >= quorum`, engine broadcasts `COMMIT`
5. **Complete** — When `len(commits) >= quorum`:
   - **Leader** (proposer): `host.on_leader_elected()` triggers `select_job()`
   - **Participant**: `host.on_participant_commit()` tracks assignment

**Quorum:** `ceil((n+1)/2)` where `n` = live agent count (adjusts dynamically with failures).

**Key Data Structures:**
- `ProposalContainer` — Thread-safe storage with dual indexing (`by_pid` and `by_object_id`)
- `ProposalInfo` — Carries `p_id`, `object_id`, `agent_id`, `cost`, `prepares`, `commits`
- Merge logic: duplicate proposals safely union their prepare/commit vote lists

**Message Hierarchy:**
```
Message (base, JSONField)
 ├── HeartBeat — liveness + capacity updates
 ├── JobStatus — job completion notifications
 ├── Proposal — consensus proposal (object_id, cost, agent_id)
 │    ├── Prepare — prepare phase vote (inherits Proposal)
 │    └── Commit  — commit phase vote (inherits Proposal)
```

**Protocol Interfaces** (`interfaces.py`):
- `ConsensusHost` — callbacks for domain logic (`get_object`, `calculate_quorum`, `on_leader_elected`, etc.)
- `ConsensusTransport` — message sending abstraction (`send`, `broadcast`)
- `TopologyRouter` — topology-aware forwarding (`should_forward`)

### 3. Selection Layer (`swarm/selection/`)

Generic cost-based assignment engine with LRU caching and version-based invalidation.

**Core: `SelectionEngine`** (`engine.py`)
- Accepts pluggable `FeasibleFn` and `CostFn` callables via constructor
- `compute_cost_matrix(assignees, candidates)` — builds 2D `[agents x jobs]` matrix with caching
- `pick_agent_per_candidate(...)` — column-wise selection: for each job, picks the best agent
  - `threshold_pct`: tolerance around best cost (e.g., 10% = within 1.1x minimum)
  - `tie_break_key`: deterministic tie-breaker (typically by `agent_id`)
  - Handles `+inf` for infeasible pairs

**Cache Strategy:**
- Separate LRU caches for feasibility and cost (default 131,072 entries each)
- Cache key: `(agent_key, job_key, agent_version, job_version)`
- Invalidation: version increments on state changes; optional TTL expiration
- Expected hit rates: ~95% feasibility, ~80% cost

**Penalties** (`penalties.py`):
- `apply_multiplicative_penalty(cost_matrix, assignees, factor_fn)` — applies live (non-cached) penalties on top of the cached base matrix
- Separates stable costs (resource fit) from volatile signals (load, queue depth)
- Factor > 1.0 penalizes; < 1.0 boosts

### 4. Communication Layer (`swarm/comm/`)

**Protocol** (`consensus.proto`):
- Single gRPC service: `ConsensusService.SendMessage(ConsensusMessage) → Ack`
- `ConsensusMessage`: `sender_id`, `receiver_id`, `message_type`, `payload` (JSON), `timestamp`

**Server** (`grpc_server.py`):
- ThreadPoolExecutor with 32 workers
- Keepalive: 30s interval, 10s timeout
- Max message size: 50MB
- Health service for liveness/readiness probes

**Client** (`grpc_client.py`):
- Persistent channel pool: one gRPC channel per neighbor
- Connectivity monitoring with state callbacks (IDLE → READY → TRANSIENT_FAILURE)
- Background health checks every 2s
- Retry: up to 4 attempts with exponential backoff (50ms → 400ms)
- Timeout: 2s per RPC call

**Transport** (`grpc_transport.py`):
- Observer pattern: notifies registered agents on incoming messages
- Path tracking in messages to prevent routing loops
- Pre-serializes JSON once for broadcast (sends to all peers)

### 5. Data Layer (`swarm/database/`)

**Redis Repository** (`repository.py`):

**Key Schema:** `<prefix>:<level>:<group>:<id>`
- `job:<L>:<G>:<job_id>` — Job objects (JSON blobs)
- `agent:<L>:<G>:<agent_id>` — Agent metadata
- `state:<L>:<G>:<state_value>` — Secondary index: set of job keys by state
- `mab:<agent_id>` — MAB policy state

**Consistency:**
- Optimistic locking via Redis WATCH/MULTI/EXEC transactions
- Atomic state index maintenance: on job state change, atomically SREM from old state set + SADD to new
- Retry on WatchError (concurrent modification)

**Multi-Level Partitioning:**
- Keys include `level` + `group` coordinates for hierarchical workload isolation
- Each level/group has independent state indices

## Adapter Pattern (Key Integration)

The consensus and selection engines are framework-agnostic. Agents integrate via three adapter classes defined in each agent's module:

```python
class _HostAdapter(ConsensusHost):
    """Bridges consensus callbacks to agent methods."""
    def get_object(object_id)        # Local queue lookup → Redis fallback
    def on_leader_elected(obj, p_id) # → agent.select_job()
    def on_participant_commit(...)   # → tracks assignment in job_assignments map
    def calculate_quorum()           # → ceil((live_agents + 1) / 2)

class _TransportAdapter(ConsensusTransport):
    """Wraps agent's gRPC send/broadcast."""
    def send(dest, payload)          # → agent.send()
    def broadcast(payload)           # → agent.broadcast()

class _RouteAdapter(TopologyRouter):
    """Topology-specific forwarding."""
    def should_forward()             # Star/Ring: True, Mesh: False
```

This design ensures:
- ConsensusEngine has **zero** dependencies on Agent, Redis, or gRPC
- New agent types only need to implement the three adapters
- Engines are independently testable

## Threading Model

Each agent runs 4-5 daemon threads:

| Thread | Function | Interval | Purpose |
|--------|----------|----------|---------|
| `periodic` | `_do_periodic()` | 0.5s | Heartbeat, peer refresh, failure detection, shutdown check |
| `inbound` | `_do_inbound()` | 0.5s | Drains message queue, dispatches to consensus engine |
| `selection` | `selection_main()` | continuous | Cost computation, proposal creation |
| `scheduling` | `scheduling_main()` | continuous | Job execution or hierarchical delegation |
| `executor` | ThreadPoolExecutor | on-demand | Concurrent job execution (default 3 workers) |

**Synchronization:**
- `ProposalContainer.lock` (RLock) for proposal storage
- `ThreadSafeDict` for neighbor maps, job assignments, delegated jobs
- `ObjectQueue` for thread-safe job queues
- Selection engine caching is lock-free (accessed only from selection thread)

## Data Flow: End-to-End Job Lifecycle

### Job State Machine
```
PENDING → PRE_PREPARE → PREPARE → COMMIT → READY → RUNNING → COMPLETE
                                             ↓
                                          BLOCKED (infeasible, retried)
```

### Phase 1: Selection (`selection_main`)
1. Wait for quorum — block until `live_agent_count == configured_agent_count`
2. Fetch batch of PENDING jobs from local queue
3. Build agent list from `neighbor_map`
4. Compute cost matrix: `selector.compute_cost_matrix(agents, jobs)`
5. Apply live load penalties: `apply_multiplicative_penalty(matrix, agents, load_factor)`
6. Select best agent per job: `selector.pick_agent_per_candidate(...)`
7. Filter for self-assignments → create proposals; infeasible → BLOCKED; better for peers → back-off
8. Initiate consensus: `engine.propose(proposals)`

### Phase 2: Consensus (PBFT)
1. Leader broadcasts PROPOSAL
2. Peers perform dominance check, respond with PREPARE if accepted
3. Quorum of PREPAREs → broadcast COMMIT
4. Quorum of COMMITs → finalize: leader calls `select_job()`, participants track assignment

### Phase 3: Scheduling (`scheduling_main`)
- **Leaf agents**: Check capacity, submit to ThreadPoolExecutor
- **Parent agents** (hierarchical): Identify capable child groups (DTN-aware) → MAB-guided selection → delegate via Redis bucket writes → monitor completion

### Phase 4: Execution
- `Job.execute()` runs (simulated wall-time sleep)
- Exit status persisted to Redis
- Parent agents receive feedback, update MAB rewards

## Cost Computation

### ResourceAgent Formula
```
cost = (weighted_usage + bottleneck_penalty) x time_penalty x connectivity_penalty x 100
```

**Components:**
- **Weighted usage**: `w_cpu * (job.cores/total.cores) + w_ram * (job.ram/total.ram) + ...`
  - Default weights: CPU=0.4, RAM=0.3, Disk=0.2, GPU=0.1
  - Dynamically tuned per job type (e.g., `cpu_bound` scales CPU weight x1.5)
- **Bottleneck penalty**: `max(core_ratio, ram_ratio, disk_ratio, gpu_ratio)^2`
- **Time penalty**: long jobs (`> threshold`): `1.5 + (wt - threshold)/threshold`; short: `1 + (wt/threshold)^2`
- **Connectivity penalty**: `1 + factor * (1 - avg_dtn_connectivity_score)`

### LlmAgent
- LLM produces a score (0-100, higher = better fit) with explanation
- Converted to cost: `cost = 100 - score`
- Includes peer context for load-aware scoring
- Falls back to analytical cost on LLM failure

### Cache Signatures
- Job: `(job_id, cores, ram, disk, gpu, wall_time, job_type, required_dtns)`
- Agent: `(agent_id, version, capacities, dtn_pairs)`
- Any component change invalidates all cached entries for that entity

## Topology (`swarm/topology/topology.py`)

| Topology | Peers | Forwarding | Scale | Use Case |
|----------|-------|------------|-------|----------|
| **Ring** | N-1, N+1 (circular) | Yes (multi-hop) | 10-50 agents | Low overhead |
| **Mesh** | All agents | No (direct) | 5-30 agents | Benchmarking, full connectivity |
| **Star** | Hub ↔ all leaves | Yes (via hub) | Small | Simple coordination |
| **Hierarchical** | Parent/children + co-parents | Down the tree | 30-1000+ | Production scale |

### Hierarchical Topology Details
- **Level 0**: Leaf workers in groups of 5-10
- **Level 1+**: Coordinators (can be LLM agents)
- **Co-parents**: K parents per group for failover
- **Job flow**: Jobs enter at top level, delegated down via Redis bucket writes
- **DTN-aware routing**: Parent checks child group DTN capabilities before delegation
- **MAB learning**: Coordinators learn which child groups to prefer via Epsilon-Greedy or UCB1

## Multi-Armed Bandit (`swarm/rl/`)

Used by hierarchical coordinators to learn child group preferences:

**Policies:**
- **Epsilon-Greedy**: Explore with probability epsilon (decaying), exploit best Q-value otherwise
- **UCB1**: `UCB(a) = Q(a) + c * sqrt(log(N) / n_a)` — exploration decreases naturally

**Integration:**
- `MABManager` wraps policy, bridges with agent delegation logic
- Arms = child group IDs; rewards = +1.0 (success) / -1.0 (failure)
- State persisted to Redis (`mab:<agent_id>`) for recovery
- Feedback loop: job completion → `report_outcome()` → policy update

## Failure Handling

### Detection (Multi-Layer)
1. **Passive**: `_detect_failed_agents()` checks `last_updated` timestamps against `peer_expiry_seconds`
2. **Active**: gRPC channel connectivity callbacks (UP/DOWN events)
3. **Aggressive mode**: Immediate failure marking on gRPC channel down

### Response
1. Increment agent `version` → invalidates selection engine cache
2. Remove from `neighbor_map` → quorum recalculates
3. Clear stuck consensus proposals for failed agent
4. Reassign failed agent's jobs to PENDING state
5. Record failure metrics (timestamp, detection method, downtime)

### Safety Property
Quorum recalculates based on live agents. System survives up to `(N-1)/2` failures without deadlock. Minimum quorum of 1 prevents total stall.

## Key Invariants

1. Each job has at most one winning proposal per consensus round
2. Proposal dominance: `(cost1 < cost2) OR (cost1 == cost2 AND agent_id1 < agent_id2)`
3. Protocol tolerates out-of-order message delivery (PREPARE before PROPOSAL)
4. Duplicate messages safely merge via union of prepare/commit vote lists
5. Cache invalidation is version-monotonic: version increments on any state change
