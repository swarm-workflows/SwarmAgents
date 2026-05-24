# Gossip-Based Consensus Design for SWARM+

> **Status:** Proposal (pre-implementation design document)
> **Author:** SWARM+ Team
> **Related:** [ROADMAP.md](ROADMAP.md) (line 147-149), [COMPLEXITY.md](COMPLEXITY.md)

---

## Table of Contents

1. [Problem Statement & Motivation](#1-problem-statement--motivation)
2. [Background — Current Architecture](#2-background--current-architecture)
3. [Hybrid Three-Layer Architecture](#3-hybrid-three-layer-architecture)
4. [Snow-Adapted Job Assignment Protocol](#4-snow-adapted-job-assignment-protocol)
5. [Integration with Existing Architecture](#5-integration-with-existing-architecture)
6. [Failure Detection Improvements](#6-failure-detection-improvements)
7. [Performance Analysis](#7-performance-analysis)
8. [Migration Path](#8-migration-path)
9. [Risks & Mitigations](#9-risks--mitigations)

---

## 1. Problem Statement & Motivation

The current SWARM+ consensus protocol is a PBFT-like 3-phase protocol (propose → prepare → commit) that achieves deterministic agreement on job assignments. While effective, it has fundamental scalability limitations:

**Current message complexity per job (single group of n agents):**

| Phase    | Messages       |
|----------|----------------|
| Propose  | p(n - 1)       |
| Prepare  | n(n - 1)       |
| Commit   | n(n - 1)       |
| **Total** | **O(n²)** per group |

Where p is the number of competing proposers (typically 1-3 due to cost-based self-selection).

With mesh grouping across m groups (n = N/m agents per group), the effective per-job complexity is O(N²/m²). Batching b jobs per round reduces this to O(N²/(m²·b)) amortized. However, for deployments of 1000+ agents, even with grouping the quadratic intra-group cost becomes a bottleneck: a group of 100 agents generates ~10,000 messages per job.

**Design goals:**
- Support 1000+ agents with sub-linear per-agent message overhead
- Maintain exactly-once job assignment guarantees (no double-assignment)
- Preserve the existing adapter-based architecture for seamless integration
- Allow gradual migration — both protocols can coexist during transition

---

## 2. Background — Current Architecture

### 2.1 PBFT-like Consensus Flow

The `ConsensusEngine` class (`swarm/consensus/engine.py:34`) implements a framework-agnostic 3-phase protocol:

1. **Propose** — An agent that self-selects as the best candidate calls `engine.propose(proposals)`, which broadcasts a `Proposal` message to all peers. The proposer implicitly prepares its own proposal.

2. **Prepare** — On receiving a proposal, `engine.on_proposal()` performs a dominance check against existing proposals (both outgoing and incoming). If the new proposal is better (lower cost, or equal cost with lexicographically smaller agent ID), the agent broadcasts a `Prepare` message. Inferior proposals are suppressed.

3. **Commit** — Once an agent accumulates `quorum = ceil((n+1)/2)` prepare votes for a proposal, `engine.on_prepare()` transitions to the commit phase and broadcasts a `Commit` message. After quorum commits are reached, the leader executes `select_job()` and participants record the assignment.

### 2.2 Adapter Pattern

The consensus engine is decoupled from the agent framework via three protocol interfaces defined in `swarm/consensus/interfaces.py:33-58`:

- **`ConsensusHost`** — Domain introspection (get objects, check agreement, calculate quorum) and side effects (leader elected, participant commit). Implemented by `_HostAdapter` in `resource_agent.py:57-100`.
- **`ConsensusTransport`** — Message delivery (`send`, `broadcast`). Implemented by `_TransportAdapter` in `resource_agent.py:103-109`, which delegates to gRPC.
- **`TopologyRouter`** — Routing decisions (`should_forward`). Implemented by `_RouteAdapter` in `resource_agent.py:112-119`.

The engine is instantiated at `resource_agent.py:125-126`:

```python
self.engine = ConsensusEngine(agent_id, _HostAdapter(self), _TransportAdapter(self),
                              router=_RouteAdapter(self))
```

This adapter pattern is the key integration point — a new consensus engine only needs to satisfy the same three protocol interfaces.

### 2.3 Cost-Based Self-Selection

The `SelectionEngine` (`swarm/selection/engine.py`) computes a cost matrix across all candidates (jobs) and assignees (agents). An agent only proposes for a job if it computes itself as the minimum-cost agent. The `selection_threshold_pct` parameter further restricts proposals to agents within a configured percentage of the global minimum cost. This mechanism ensures that typically only 1-3 agents propose for the same job (p = O(1)), keeping the proposal phase lightweight.

---

## 3. Hybrid Three-Layer Architecture

Rather than replacing PBFT entirely with a pure gossip protocol, we propose a hybrid architecture with three distinct layers, each addressing a specific concern at the appropriate scale:

```
┌─────────────────────────────────────────────────┐
│  Layer 3: Snow/Avalanche Consensus              │
│  (Job assignment agreement — O(k·log n) rounds) │
├─────────────────────────────────────────────────┤
│  Layer 2: Gossip State Dissemination            │
│  (Agent load/capacity — epidemic broadcast)     │
├─────────────────────────────────────────────────┤
│  Layer 1: SWIM Membership                       │
│  (Failure detection — O(1) per agent per round) │
└─────────────────────────────────────────────────┘
```

### 3.1 Layer 1 — SWIM Membership Protocol

**Purpose:** Failure detection with O(1) per-agent message overhead per protocol round, replacing the current `peer_expiry_seconds` heartbeat polling.

**Protocol:**

Each protocol round (period T, e.g., 1 second), an agent:

1. **Ping** — Selects one random peer and sends a direct ping.
2. **Ping-req** — If no ack within timeout, selects k_req random peers and asks them to ping the target on its behalf (indirect probing).
3. **Suspect** — If neither direct nor indirect probes succeed, marks the target as *suspected*.
4. **Confirm/Failed** — If the target remains suspected for a configurable duration without refutation, it is declared *failed*. The target can refute suspicion by responding to any probe.

**Dissemination:** Membership changes (join, suspect, failed) are piggybacked on protocol messages (pings, acks, ping-reqs) as "infection-style" updates, achieving O(log n) propagation rounds without dedicated broadcast.

**Parameters:**
- `T` = protocol period (1s recommended)
- `k_req` = indirect probe fanout (3-5)
- `suspect_timeout` = duration before suspect → failed (5-10s, replaces `peer_expiry_seconds`)

**Messages per agent per round:** O(1) direct ping + O(k_req) indirect probes in the failure case = O(1) amortized.

### 3.2 Layer 2 — Gossip State Dissemination

**Purpose:** Propagate agent load, capacity, and availability information across the cluster using an epidemic protocol, replacing the current approach where agents share state via Redis queries or broadcast.

**Protocol:**

Each gossip round (period T_gossip, e.g., 500ms-2s):

1. Each agent maintains a local state vector: `{agent_id: (cpu_util, ram_util, disk_util, gpu_util, load, version)}`.
2. On each round, the agent selects `fanout` (e.g., 3) random peers and sends its known state.
3. On receiving gossip, merge using version numbers — higher version wins.
4. State entries expire after `state_ttl` (e.g., 30s) if not refreshed, indicating a stale/failed agent.

**Convergence:** With fanout f, full dissemination takes O(log_f(n)) rounds. For f=3 and n=1000, this is ~7 rounds (~3.5s at 500ms period).

**Parameters:**
- `T_gossip` = gossip period (500ms-2s)
- `fanout` = number of peers per round (3-5)
- `state_ttl` = expiry for stale entries (30s)

**Messages per agent per round:** O(fanout) = O(1).

**Total messages per round across all agents:** O(n · fanout) = O(n).

### 3.3 Layer 3 — Snow/Avalanche Consensus

**Purpose:** Achieve distributed agreement on job assignments with O(k · log n) total message rounds instead of O(n²) all-to-all broadcast.

**Background:** The Snowball/Avalanche family of consensus protocols (introduced by Team Rocket / Avalanche) achieves agreement through repeated random sub-sampling. Unlike PBFT, which requires all-to-all communication within a group, Snow queries only a small random subset of peers per round.

**Core mechanism:**

For each job requiring assignment:

1. An agent computes its local cost and preferred assignee.
2. It queries k random peers: "Who should execute this job?"
3. Each queried peer responds with its preferred assignee (the one with the lowest cost from its perspective).
4. If >= α (alpha threshold) out of k responses agree on the same assignee, confidence increases.
5. After β (beta) consecutive successful rounds, the agent commits to that decision.
6. A Redis CAS (Compare-And-Set) operation finalizes the assignment atomically.

**Parameters:**
- `k` = sample size per query round (e.g., 20)
- `α` = quorum threshold within the sample (e.g., 14 out of 20, i.e., αk = 0.7k)
- `β` = consecutive rounds required for commitment (e.g., 20)

---

## 4. Snow-Adapted Job Assignment Protocol

This section details how the Snow consensus mechanism is adapted specifically for SWARM+'s job assignment workflow.

### 4.1 Protocol Flow

```
Agent A (initiator)              Random Peers (k subset)          Redis
    │                                    │                          │
    │  1. Detect new job J               │                          │
    │  2. Compute local cost for J       │                          │
    │                                    │                          │
    │──── SnowQuery(J, preferred=A) ────>│                          │
    │     (to k random peers)            │                          │
    │                                    │  3. Each peer computes   │
    │                                    │     local cost, returns  │
    │                                    │     preferred assignee   │
    │<─── SnowResponse(preferred=X) ─────│                          │
    │                                    │                          │
    │  4. Tally: if ≥α agree on X,       │                          │
    │     increment confidence for X     │                          │
    │                                    │                          │
    │  5. Repeat steps 1-4 for β rounds  │                          │
    │                                    │                          │
    │  6. After β consecutive successes: │                          │
    │──── CAS(job:J:assignee, ∅ → X) ───────────────────────────>│
    │                                    │                          │
    │<─── CAS result ──────────────────────────────────────────────│
    │                                    │                          │
    │  7a. If CAS succeeded: commit      │                          │
    │  7b. If CAS failed: re-enter Snow  │                          │
    │      with updated state            │                          │
```

### 4.2 Job Announcement

Instead of broadcasting a proposal to all group members (O(n) messages), new jobs are disseminated via the gossip state layer (Layer 2):

1. When a new job appears in the Redis queue, agents that pull it add a gossip entry: `{job_id: J, status: "available", min_cost_seen: C, preferred_agent: A}`.
2. This propagates through epidemic gossip in O(log n) rounds.
3. Any agent that receives the job announcement and computes a feasible cost can initiate a Snow query.

### 4.3 Snow Query Phase

Each initiating agent runs the following loop:

```
function snow_decide(job_id, my_cost, my_agent_id):
    preferred = my_agent_id
    confidence = 0

    for round in 1..MAX_ROUNDS:
        peers = random_sample(live_agents, k)
        responses = query_peers(peers, job_id)

        # Tally responses
        counts = Counter(r.preferred for r in responses)
        top_choice, top_count = counts.most_common(1)[0]

        if top_count >= alpha_k:
            if top_choice == preferred:
                confidence += 1
            else:
                preferred = top_choice
                confidence = 1  # Reset: new candidate
        else:
            confidence = 0  # No supermajority — reset

        if confidence >= beta:
            return preferred  # Decided

    return UNDECIDED  # Fall back to PBFT or retry
```

### 4.4 Peer Response Logic

When a peer receives a Snow query for job J:

1. Look up J in local state (from gossip layer or direct Redis fetch).
2. Compute `my_cost = compute_job_cost(J)` using the existing `SelectionEngine`.
3. If the peer has already decided on J (via its own Snow process), return that decision.
4. Otherwise, return the agent with the lowest cost known: either itself (if feasible and lowest) or the best from gossip state.

This reuses the existing cost computation and feasibility checks without modification.

### 4.5 Exactly-Once Finalization via Redis CAS

After Snow decides on an assignee, the winning agent finalizes the assignment atomically:

```python
# Lua script executed atomically in Redis
KEYS[1] = "job:{job_id}:assignee"
ARGV[1] = agent_id

local current = redis.call('GET', KEYS[1])
if current == false then
    redis.call('SET', KEYS[1], ARGV[1])
    return 1  -- Success: assignment recorded
else
    return 0  -- Conflict: another agent already assigned
end
```

**Conflict resolution:** If the CAS fails (another agent committed first), the agent re-enters the Snow loop with updated state. Since the assigned agent is now visible via gossip, subsequent Snow rounds will converge on the already-committed assignment.

### 4.6 Consistency Guarantees

- **Safety (no double-assignment):** Guaranteed by Redis CAS — exactly one agent can win the SET operation per job.
- **Liveness (all jobs eventually assigned):** The gossip layer ensures all agents eventually learn about unassigned jobs. If Snow fails to converge within MAX_ROUNDS, the agent falls back to the existing PBFT protocol within its local group.
- **Consistency (all agents agree):** After CAS commits, the assignment propagates via gossip. Agents that query the committed job in subsequent Snow rounds receive the committed assignment. Stale Snow rounds for the same job are harmless — they either converge on the same answer or fail the CAS.

---

## 5. Integration with Existing Architecture

### 5.1 New Engine Class

A new `GossipConsensusEngine` class replaces `ConsensusEngine` when configured. It implements the same interaction pattern but with different internal mechanics:

```python
class GossipConsensusEngine:
    """
    Snow/Avalanche-based consensus engine.
    Implements the same host/transport/router callback model
    as ConsensusEngine for drop-in replacement.
    """
    def __init__(self, agent_id: int, host: ConsensusHost,
                 transport: GossipTransport, router: TopologyRouter,
                 k: int = 20, alpha: float = 0.7, beta: int = 20):
        self.agent_id = agent_id
        self.host = host
        self.transport = transport
        self.router = router
        self.k = k
        self.alpha_k = int(alpha * k)
        self.beta = beta

        # Snow state per job
        self.snow_state = {}  # job_id -> {preferred, confidence, round}

        # SWIM membership
        self.membership = SWIMMembership(agent_id, transport)

        # Gossip state
        self.gossip = GossipStateDisseminator(agent_id, transport)
```

### 5.2 Transport Adapter

A new `_GossipTransportAdapter` extends the transport interface to support gossip-specific operations:

```python
class GossipTransport(ConsensusTransport, Protocol):
    """Extended transport for gossip operations."""
    def send(self, dest: int, payload: object) -> None: ...
    def broadcast(self, payload: object) -> None: ...  # Retained for compatibility
    def send_to_random(self, payload: object, count: int) -> list[int]: ...
    def query_peers(self, peers: list[int], payload: object,
                    timeout: float) -> list[object]: ...

class _GossipTransportAdapter(GossipTransport):
    def __init__(self, agent: "ResourceAgent"):
        self.agent = agent

    def send(self, dest: int, payload: object) -> None:
        self.agent.send(dest, payload)

    def broadcast(self, payload: object) -> None:
        self.agent.broadcast(payload)

    def send_to_random(self, payload: object, count: int) -> list[int]:
        live = self.agent.membership.get_live_agents()
        targets = random.sample(live, min(count, len(live)))
        for t in targets:
            self.agent.send(t, payload)
        return targets

    def query_peers(self, peers: list[int], payload: object,
                    timeout: float) -> list[object]:
        # Async query with timeout — implementation uses gRPC bidirectional streaming
        # or a request-response pattern on the existing gRPC channel
        ...
```

### 5.3 Configuration-Driven Engine Selection

The consensus engine is selected via `config_swarm_multi.yml`:

```yaml
consensus:
  protocol: "pbft"         # "pbft" (default) or "snow"
  snow:
    k: 20                  # Sample size per Snow query round
    alpha: 0.7             # Supermajority threshold (fraction of k)
    beta: 20               # Consecutive rounds for commitment
    max_rounds: 100        # Max rounds before fallback to PBFT
  gossip:
    fanout: 3              # Peers per gossip round
    period_ms: 1000        # Gossip interval in milliseconds
    state_ttl_s: 30        # State entry expiry
  swim:
    period_ms: 1000        # SWIM protocol period
    k_req: 3               # Indirect probe fanout
    suspect_timeout_s: 8   # Suspect → failed timeout
```

Engine instantiation in `ResourceAgent.__init__`:

```python
consensus_cfg = self.config.get("consensus", {})
protocol = consensus_cfg.get("protocol", "pbft")

if protocol == "snow":
    snow_cfg = consensus_cfg.get("snow", {})
    self.engine = GossipConsensusEngine(
        agent_id,
        _HostAdapter(self),
        _GossipTransportAdapter(self),
        router=_RouteAdapter(self),
        k=snow_cfg.get("k", 20),
        alpha=snow_cfg.get("alpha", 0.7),
        beta=snow_cfg.get("beta", 20),
    )
else:
    self.engine = ConsensusEngine(
        agent_id,
        _HostAdapter(self),
        _TransportAdapter(self),
        router=_RouteAdapter(self),
    )
```

### 5.4 Hierarchical Hybrid Mode

For hierarchical topologies, the two protocols can coexist:

- **Intra-group (Level 0):** PBFT within small groups (10-50 agents) where O(n²) is manageable.
- **Inter-group (Level 1):** Snow consensus among group leaders, where the number of groups may be large (50-200+).

This maps directly to the existing hierarchical topology implementation. Level 0 agents use `ConsensusEngine`; Level 1 coordinators use `GossipConsensusEngine`.

### 5.5 New Message Types

Add to `consensus.proto`:

```protobuf
message SnowQuery {
    string source_agent = 1;
    string job_id = 2;
    string preferred_agent = 3;
    double preferred_cost = 4;
    int32 round = 5;
}

message SnowResponse {
    string source_agent = 1;
    string job_id = 2;
    string preferred_agent = 3;
    double cost = 4;
    bool already_decided = 5;
}

message GossipState {
    string source_agent = 1;
    repeated AgentStateEntry entries = 2;
}

message AgentStateEntry {
    string agent_id = 1;
    double cpu_util = 2;
    double ram_util = 3;
    double disk_util = 4;
    double gpu_util = 5;
    int32 load = 6;
    int64 version = 7;
}

message SwimPing {
    string source_agent = 1;
    repeated MembershipUpdate updates = 2;
}

message SwimAck {
    string source_agent = 1;
    repeated MembershipUpdate updates = 2;
}

message SwimPingReq {
    string source_agent = 1;
    string target_agent = 2;
}

message MembershipUpdate {
    string agent_id = 1;
    string status = 2;    // "alive", "suspect", "failed", "joined"
    int64 incarnation = 3;
}
```

---

## 6. Failure Detection Improvements

### 6.1 Current Approach

The existing failure detection relies on periodic polling:

- Each agent tracks `peer_expiry_seconds` (default 300s) — if no heartbeat is received within this window, a peer is marked stale.
- `_detect_failed_agents()` scans all known peers and checks timestamps.
- Failed agent detection triggers `_reassign_jobs_from_failed_agent()` to redistribute jobs.

**Limitations:**
- O(n) messages per agent per heartbeat round (broadcast heartbeat to all peers).
- 300s detection latency is too slow for reactive scheduling.
- No suspicion mechanism — a single missed heartbeat can cause false positives if the threshold is reduced.

### 6.2 SWIM-Based Improvement

The SWIM membership layer (Section 3.1) addresses all three limitations:

| Property | Current (Heartbeat) | SWIM |
|----------|---------------------|------|
| Messages per agent per round | O(n) | O(1) |
| Total messages per round | O(n²) | O(n) |
| Detection latency | 300s (configurable) | 5-10s (suspect_timeout) |
| False positive handling | None | Suspicion + refutation |

### 6.3 Integration with Existing Failure Handling

The SWIM layer replaces the heartbeat mechanism but preserves the existing failure response logic:

```python
# In ResourceAgent, replace heartbeat polling with SWIM callbacks:

class _SwimCallback:
    def __init__(self, agent: "ResourceAgent"):
        self.agent = agent

    def on_agent_failed(self, failed_agent_id: int):
        """Called by SWIM when an agent transitions suspect → failed."""
        self.agent.logger.warning(f"SWIM: Agent {failed_agent_id} detected as FAILED")
        # Reuse existing reassignment logic
        self.agent._reassign_jobs_from_failed_agent(failed_agent_id)
        # Update quorum calculation
        self.agent._update_live_agent_count()

    def on_agent_joined(self, new_agent_id: int):
        """Called by SWIM when a new agent joins or recovers."""
        self.agent.logger.info(f"SWIM: Agent {new_agent_id} joined/recovered")
        self.agent._update_live_agent_count()

    def on_agent_suspected(self, suspected_agent_id: int):
        """Called by SWIM when an agent is suspected but not yet confirmed failed."""
        self.agent.logger.info(f"SWIM: Agent {suspected_agent_id} suspected")
        # Optionally stop sending jobs to suspected agents
```

---

## 7. Performance Analysis

### 7.1 Message Complexity Comparison

| Protocol | Messages per Job | Per-Agent Overhead | Total (n agents) |
|----------|------------------|--------------------|-------------------|
| **PBFT (current, flat)** | O(n²) | O(n) | O(n²) |
| **PBFT (mesh, m groups)** | O(n²/m²) | O(n/m) | O(n²/m²) |
| **Snow (flat)** | O(k · β) | O(k) | O(k · β · n_initiators) |
| **Snow (with gossip announce)** | O(k · β + n) | O(k + fanout) | O(k·β + n·fanout) |
| **Hybrid (PBFT intra + Snow inter)** | O(g² + k·β) | O(g + k) | O(m·g² + k·β·m) |

Where:
- k = Snow sample size (20), β = consecutive rounds (20), g = group size, m = number of groups
- n_initiators = number of agents that independently initiate Snow for the same job (typically 1-3)

**Concrete example — 1000 agents:**

| Protocol | Group Size | Messages per Job |
|----------|-----------|------------------|
| PBFT (flat) | 1000 | ~1,000,000 |
| PBFT (10 mesh groups) | 100 | ~10,000 |
| PBFT (50 mesh groups) | 20 | ~400 |
| Snow (flat) | 1000 | ~400 (k=20, β=20) |
| Hybrid (PBFT/50 + Snow inter) | 20 intra, 50 inter | ~800 |

Snow achieves roughly constant per-job message cost regardless of total agent count, since k and β are fixed parameters.

### 7.2 Convergence Time

- **PBFT:** 3 message rounds (propose → prepare → commit), each requiring a full broadcast. Latency = 3 × network_RTT × topology_diameter.
- **Snow:** β rounds of k-peer queries. Each round requires one RTT to k peers (in parallel). Latency = β × RTT. With β=20 and RTT=1ms, convergence ≈ 20ms. Rounds can execute concurrently for different jobs.
- **Gossip state propagation:** O(log_f(n)) rounds. For f=3, n=1000: ~7 rounds. At 500ms period, full propagation ≈ 3.5s.
- **SWIM failure detection:** Expected detection time ≈ suspect_timeout + O(log n) protocol rounds. Typically 5-10s.

### 7.3 Trade-offs

| Property | PBFT (Current) | Snow (Proposed) |
|----------|----------------|-----------------|
| **Safety** | Deterministic (quorum-based) | Probabilistic (tunable via k, α, β) + Redis CAS |
| **Liveness** | Guaranteed with quorum | Guaranteed (termination after MAX_ROUNDS, fallback to PBFT) |
| **Message complexity** | O(n²) per group | O(k · β) per job |
| **Latency (small n)** | Lower (3 rounds) | Higher (β rounds, though each is faster) |
| **Latency (large n)** | Higher (broadcast to many peers) | Lower (only k peers per round) |
| **Failure tolerance** | Up to f < n/3 Byzantine | Probabilistic (depends on k, α) |
| **Implementation complexity** | Moderate (current) | Higher (three new subsystems) |

### 7.4 Recommended Parameter Ranges

| Deployment Size | Protocol | k | α | β | Gossip Fanout | Notes |
|----------------|----------|---|---|---|---------------|-------|
| < 50 agents | PBFT | — | — | — | — | PBFT is optimal for small clusters |
| 50-200 agents | Snow or Hybrid | 10-15 | 0.6-0.7 | 10-15 | 3 | Snow provides modest improvement |
| 200-1000 agents | Snow | 15-20 | 0.7 | 15-20 | 3-5 | Significant reduction in messages |
| 1000+ agents | Hybrid (PBFT intra + Snow inter) | 20-30 | 0.7-0.8 | 20-30 | 5 | Best of both: strong intra-group, scalable inter-group |

**Parameter tuning guidance:**
- **Increasing k** improves confidence per round but increases per-round message cost.
- **Increasing α** requires stronger supermajority, reducing probability of incorrect decisions but potentially increasing rounds to converge.
- **Increasing β** reduces the probability of incorrect commitment exponentially (failure probability ≈ (1-α)^β).

---

## 8. Migration Path

The migration is designed as four incremental phases, each independently useful and testable.

### Phase 1: SWIM Membership Layer

**Scope:** Add SWIM failure detection alongside the existing heartbeat mechanism.

**Changes:**
- New module: `swarm/membership/swim.py`
- New proto messages: `SwimPing`, `SwimAck`, `SwimPingReq`, `MembershipUpdate`
- Integrate with `ResourceAgent` via `_SwimCallback`
- Config toggle: `failure_detection.protocol: "heartbeat" | "swim"`
- Both mechanisms can run in parallel during transition — SWIM augments heartbeat, does not replace it yet

**Validation:**
- Compare false positive rates between heartbeat (300s) and SWIM (8s suspect timeout)
- Measure detection latency under simulated failures (`kill_agents.py`)
- Verify no impact on existing PBFT consensus

**Risk:** Low — purely additive, no changes to consensus path.

### Phase 2: Gossip State Dissemination

**Scope:** Replace broadcast-based state sharing with epidemic gossip for agent load/capacity.

**Changes:**
- New module: `swarm/gossip/disseminator.py`
- New proto message: `GossipState`, `AgentStateEntry`
- Agents periodically push local state to `fanout` random peers instead of (or in addition to) Redis writes
- `SelectionEngine` reads from local gossip state instead of querying Redis for peer capacities

**Validation:**
- Measure state convergence time across 100, 500, 1000 agents
- Compare cost matrix accuracy (gossip state vs Redis ground truth)
- Benchmark reduction in Redis read load

**Risk:** Low-medium — gossip state may be slightly stale compared to Redis. Staleness is bounded by `state_ttl`.

### Phase 3: Snow Consensus Engine

**Scope:** Implement `GossipConsensusEngine` as an alternative to `ConsensusEngine`, selectable via config.

**Changes:**
- New module: `swarm/consensus/gossip_engine.py`
- New proto messages: `SnowQuery`, `SnowResponse`
- New adapter: `_GossipTransportAdapter`
- Redis CAS Lua script for exactly-once finalization
- Config toggle: `consensus.protocol: "pbft" | "snow"`
- Fallback: if Snow fails to converge in MAX_ROUNDS, delegate to PBFT within local group

**Validation:**
- Correctness: verify no double-assignments under concurrent Snow queries (stress test with 100+ jobs, 100+ agents)
- Performance: compare job assignment throughput and latency vs PBFT at various scales
- Convergence: measure actual rounds-to-converge distribution

**Risk:** Medium — probabilistic safety requires thorough testing. Redis CAS provides a deterministic safety net.

### Phase 4: Hybrid Hierarchical Mode

**Scope:** Use PBFT within groups and Snow across groups in hierarchical topologies.

**Changes:**
- Modify `ResourceAgent` to instantiate different engines at different topology levels
- Level 0 agents use `ConsensusEngine` (PBFT) for intra-group consensus
- Level 1 coordinators use `GossipConsensusEngine` (Snow) for inter-group coordination
- Integrate MAB delegation (`swarm/rl/mab_manager.py`) with Snow-based inter-group consensus

**Validation:**
- End-to-end test: hierarchical topology with 500+ agents, 20+ groups
- Compare against pure PBFT hierarchical and pure Snow flat
- Measure inter-group consensus latency and message overhead

**Risk:** Medium-high — interaction between two consensus protocols at different levels needs careful testing.

---

## 9. Risks & Mitigations

### 9.1 Probabilistic Safety

**Risk:** Snow consensus is probabilistic — there is a non-zero (but exponentially small) probability of conflicting decisions.

**Mitigation:**
- Redis CAS provides a deterministic safety net. Even if two agents reach different Snow decisions, only one can win the CAS operation.
- The losing agent detects the conflict and converges on the committed assignment via gossip.
- Safety failure probability with k=20, α=0.7, β=20 is astronomically low (< 10^-30 per decision).

### 9.2 Network Partitions

**Risk:** During a network partition, agents in different partitions may attempt to assign the same job to different agents.

**Mitigation:**
- Redis CAS ensures exactly-one assignment as long as Redis is reachable.
- If Redis is partitioned, agents fall back to PBFT within their reachable partition (which already handles this scenario via quorum).
- SWIM membership detects partition-induced failures and adjusts the live agent set accordingly.

### 9.3 Increased Latency for Small Deployments

**Risk:** Snow requires β rounds to commit, which may be slower than PBFT's 3-round protocol for small clusters.

**Mitigation:**
- Configuration guidance: recommend PBFT for deployments < 50 agents.
- The `consensus.protocol` config toggle makes this a deployment-time decision, not a code change.
- Hybrid mode allows PBFT within small groups while using Snow only at scale.

### 9.4 Implementation Complexity

**Risk:** Three new subsystems (SWIM, gossip, Snow) increase codebase complexity and maintenance burden.

**Mitigation:**
- Phased migration — each phase is independently valuable and can be rolled back.
- SWIM and gossip dissemination are well-studied protocols with mature reference implementations (e.g., Hashicorp Memberlist, SWIM paper).
- The adapter pattern ensures new engines are isolated from agent logic.

### 9.5 Gossip State Staleness

**Risk:** Gossip-propagated agent state may be stale, leading to suboptimal cost computations.

**Mitigation:**
- Bounded staleness via `state_ttl` — entries expire and are refreshed within O(log n) gossip rounds.
- Cost-based self-selection already tolerates imperfect state — agents only propose if they believe they are the best, and dominance filtering (or Snow sampling) resolves disagreements.
- For latency-sensitive decisions, agents can query Redis directly as a fallback.

### 9.6 Redis as Single Point of Failure for CAS

**Risk:** The Redis CAS operation is a centralization point for the exactly-once guarantee.

**Mitigation:**
- Redis is already a dependency in the current architecture (data layer).
- For high-availability deployments, use Redis Sentinel or Redis Cluster.
- Long-term: the CAS check can be replaced with a distributed lock or Raft-based key-value store if Redis availability is insufficient.

---

## References

1. **SWIM Protocol:** Das, A., Gupta, I., & Motivala, A. (2002). "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol." DSN 2002.
2. **Avalanche Consensus:** Team Rocket (2018). "Scalable and Probabilistic Leaderless BFT Consensus through Metastability." arXiv:1906.08936.
3. **Epidemic Algorithms:** Demers, A., et al. (1987). "Epidemic Algorithms for Replicated Database Maintenance." PODC 1987.
4. **Current SWARM+ Complexity:** [COMPLEXITY.md](COMPLEXITY.md) — O(n²/m²) with mesh grouping.
5. **SWARM+ Roadmap:** [ROADMAP.md](ROADMAP.md) — Gossip-based protocol identified for 1000+ agent deployments.
