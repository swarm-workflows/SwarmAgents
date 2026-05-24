# Message Complexity Analysis: PBFT-like Consensus with Cost-Based Self-Selection

## Protocol Overview

In standard PBFT, a **designated leader** proposes and **all n replicas** participate in every round. In our protocol, agents **self-select** based on cost: an agent only proposes if it computes itself as the **best agent** for the job. Additionally, dominance filtering in `on_proposal`/`on_prepare`/`on_commit` suppresses messages for inferior proposals.

## Definitions

- **N** = total number of agents
- **m** = number of mesh groups (mesh topology) or Level-0 groups (hierarchical topology)
- **g** = agents per Level-0 group (g = N_L0 / m, where N_L0 is the number of Level-0 agents)
- **G** = number of Level-1 coordinator agents (hierarchical topology)
- **p** = number of agents that independently propose for the same job (competing proposers)
- **q** = quorum = floor(n/2) + 1
- **b** = jobs per proposal batch (`jobs_per_proposal`)
- **L** = number of hierarchy levels

## Per-Job Message Complexity (Single Group of n Agents)

| Phase              | Standard PBFT   | Our Protocol (best case) | Our Protocol (worst case) |
|--------------------|-----------------|--------------------------|---------------------------|
| Propose/Pre-prepare | n - 1           | p(n - 1)                 | p(n - 1)                  |
| Prepare            | n(n - 1)        | n(n - 1)                 | p * n(n - 1)              |
| Commit             | n(n - 1)        | n(n - 1)                 | n(n - 1)                  |
| **Total**          | **2n^2 - n - 1** | **(2n + p)(n - 1)**      | **(pn + n + p)(n - 1)**   |

### Best Case

When all agents receive the best (lowest-cost) proposal first, each agent sends exactly one prepare for the winning proposal. Total: (2n + p)(n - 1).

### Worst Case

When all p proposals arrive at every agent in worst-to-best cost order, each agent accepts and prepares for up to p successive proposals before converging on the best one. Total: (pn + n + p)(n - 1).

## Why p is Small in Practice

The self-selection mechanism ensures p << n:

1. **Cost-matrix filtering** -- Each agent computes costs for *all* peers in its group and only proposes if it is the minimum-cost agent from its own perspective (`pick_agent_per_candidate()`).

2. **`selection_threshold_pct`** -- The winning cost must be within a configured percentage (default 10%) of the global minimum; otherwise the job is deferred rather than proposed.

3. **Feasibility filtering** -- Agents that cannot run the job (insufficient capacity, missing DTN connectivity) are excluded entirely from the cost matrix (cost = infinity).

Since agents in a mesh group share similar resource visibility, they largely agree on who the best agent is. Typically **p = 1** (or a small constant, say 2-3, due to race conditions or slightly different load snapshots).

## Dominance Filtering

When an agent receives a proposal inferior to one already accepted, it suppresses the prepare message entirely (returns without broadcasting). This prevents cascading votes for suboptimal proposals and reduces the effective number of prepare messages exchanged, particularly when multiple agents propose concurrently.

A proposal is considered inferior if:
- It has higher cost than an existing proposal for the same job, OR
- It has equal cost but a lexicographically larger agent ID (deterministic tie-breaking)

## Effective Complexity

The per-job message complexity is **O(pn + n^2)**. With p = O(1) due to cost-based self-selection, this simplifies to **O(n^2)** -- matching PBFT's asymptotic bound but with a lower constant factor since the proposal phase generates p(n-1) messages rather than the full (n-1) broadcast.

## Batching

Proposals are batched with up to b jobs per `engine.propose()` call. The prepare and commit phases process the batch as a single message exchange, giving an amortized per-job complexity of:

**O((pn + n^2) / b)**

## Mesh Grouping (m Groups, Single Level)

Mesh grouping confines consensus to intra-group communication (`should_forward()` returns `False` for mesh topology in `_RouteAdapter`, `resource_agent.py:116-119`). This reduces per-job complexity from O(N^2) for a flat topology to **O(N^2 / m^2)**, a quadratic improvement in the number of groups. Groups operate in parallel on independent job subsets, with no inter-group coordination.

## Hierarchical Topology (Multi-Level)

Hierarchical topology introduces multiple consensus levels. Unlike mesh grouping where groups operate independently on pre-partitioned jobs, hierarchical topology adds **cross-group coordination** via Level-1 (or higher) coordinator agents.

### Two-Level Hierarchy

In a two-level hierarchy (the most common deployment):

- **Level-0 (ResourceAgents):** N_L0 agents organized into m groups of g = N_L0/m agents each. Each group runs independent intra-group PBFT consensus.
- **Level-1 (CoordinatorAgents):** G coordinator agents (typically G = m, one per group) form their own consensus group using the same PBFT protocol.

Each job undergoes **two sequential consensus rounds**:

| Step | Action | Messages | Code Reference |
|------|--------|----------|----------------|
| 1. Level-1 consensus | G coordinators run PBFT to decide which coordinator selects the job | **O(G^2)** | `engine.py:83-93` (propose), `engine.py:97-142` (prepare), `engine.py:144-230` (commit) |
| 2. Delegation | Winning coordinator writes job to child group's Redis queue | **O(1)** (Redis write, not consensus) | `resource_agent.py:1678-1684` (`repository.save()`) |
| 3. Level-0 consensus | g agents in the target group run PBFT to assign the job | **O(g^2)** | Same consensus engine, isolated peer set |

**Total per job: O(G^2 + g^2)**

Since N ≈ G·g (total agents = coordinators × group size, ignoring the coordinators themselves):

**O(G^2 + (N/G)^2)**

Minimizing over G: the optimal group count is G = N^(1/2), giving **g = N^(1/2)** and:

**O(N^(1/2))^2 + O(N^(1/2))^2 = O(N)**

This is an **order-of-magnitude improvement** over flat O(N^2).

### Key Implementation Detail: Delegation is NOT Consensus

The delegation from Level-1 to Level-0 is a **direct Redis write** (`resource_agent.py:1678-1684`), not a consensus round. The coordinator simply saves the job to the child group's Redis namespace with state=PENDING. Level-0 agents pick it up during their next job-pool poll and enter their own consensus. This keeps the two consensus stages independent and additive rather than multiplicative.

### Concrete Example: Hier-110

- N = 110 agents total (100 Level-0 + 10 Level-1)
- G = 10 coordinator agents
- m = 10 groups of g = 10 Level-0 agents each
- Per-job messages:
  - Level-1 consensus: O(10^2) = O(100) messages
  - Level-0 consensus: O(10^2) = O(100) messages
  - Total: ~200 messages per job
- Flat mesh-110 would require: O(110^2) = O(12,100) messages per job
- **Improvement: ~60x reduction**

### Three-Level Hierarchy

For three-level hierarchies (e.g., Hier-990: 88 groups × 10 L0 agents + 22 groups × 4 L1 coordinators + 1 group × 22 L2 coordinators), each job traverses three consensus rounds:

| Level | Group Size | Messages |
|-------|-----------|----------|
| Level-2 | G_2 coordinators | O(G_2^2) |
| Level-1 | G_1 coordinators per super-group | O(G_1^2) |
| Level-0 | g agents | O(g^2) |

**Total: O(G_2^2 + G_1^2 + g^2)**

### General L-Level Hierarchy

For an L-level hierarchy with constant group size g at each level:

- Total agents: N ≈ g^L, so L = log_g(N)
- Each job traverses L consensus rounds, each costing O(g^2)
- **Total: O(g^2 · L) = O(g^2 · log_g N)**

With fixed g, this is **O(log N)** -- logarithmic in the total number of agents. However, this requires L = log_g(N) hierarchy levels. In practice, current deployments use 2-3 levels, yielding O(N) or O(N^(2/3)) rather than O(log N).

## Summary Table

| Configuration                        | Messages per job             | Notes |
|--------------------------------------|------------------------------|-------|
| Standard PBFT (N agents, flat)       | O(N^2)                       | All agents in single group |
| Our protocol, flat mesh (1 group)    | O(pN + N^2), p=O(1) → O(N^2) | Self-selection reduces constant |
| Mesh grouping, m groups, n = N/m     | O(n^2) = **O(N^2 / m^2)**    | No inter-group coordination |
| With batching (b jobs/round)         | **O(N^2 / (m^2 · b))** amortized | |
| **Hierarchical, 2-level (G + g)**    | **O(G^2 + g^2) = O((N/G)^2 + G^2)** | Two sequential consensus rounds |
| Hier, 2-level, G = √N               | **O(N)**                      | Optimal 2-level grouping |
| **Hierarchical, L-level, group size g** | **O(g^2 · L)**            | L = log_g(N) for constant g |
| Hier, L-level, constant g            | **O(g^2 · log N) = O(log N)** | Requires deep hierarchy |

## Key Differentiators from Standard PBFT

1. **Self-selection** -- Agents self-filter based on cost, reducing proposal-phase messages (lower constant factor).
2. **Dominance filtering** -- Inferior proposals are suppressed early, preventing redundant prepare/commit exchanges.
3. **Batching** -- Multiple jobs are proposed in a single consensus round, amortizing the O(n^2) overhead.
4. **Mesh grouping** -- Consensus is confined to groups of size n = N/m, yielding a quadratic reduction O(N^2/m^2) vs O(N^2).
5. **Hierarchical topology** -- Two (or more) sequential consensus rounds at different hierarchy levels reduce per-job complexity from O(N^2) to O(N) (2-level) or O(log N) (deep hierarchy with constant group size).

## Comparison: Mesh Grouping vs. Hierarchical

| Property | Mesh Grouping | Hierarchical |
|----------|--------------|--------------|
| Per-job complexity (G=√N) | O(N) | O(N) |
| Inter-group coordination | None (jobs pre-partitioned) | Yes (Level-1 consensus routes jobs) |
| Job routing intelligence | Static partitioning | Cost-based delegation by coordinators |
| Additional overhead | None | G^2 for coordinator consensus |
| Scalability path | Add groups | Add hierarchy levels → O(log N) |

Both approaches achieve O(N) with √N groups, but hierarchical topology pays the G^2 coordinator consensus cost in exchange for **intelligent cross-group job routing** based on aggregated capacity and DTN connectivity.
