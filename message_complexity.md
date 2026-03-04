# Message Complexity Analysis: PBFT-like Consensus with Cost-Based Self-Selection

## Protocol Overview

In standard PBFT, a **designated leader** proposes and **all n replicas** participate in every round. In our protocol, agents **self-select** based on cost: an agent only proposes if it computes itself as the **best agent** for the job. Additionally, dominance filtering in `on_proposal`/`on_prepare`/`on_commit` suppresses messages for inferior proposals.

## Definitions

- **N** = total number of agents
- **m** = number of mesh groups
- **n** = agents per group (n = N/m)
- **p** = number of agents that independently propose for the same job (competing proposers)
- **q** = quorum = floor(n/2) + 1
- **b** = jobs per proposal batch (`jobs_per_proposal`)

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

## Mesh Grouping (m Groups)

Mesh grouping confines consensus to intra-group communication (`should_forward()` returns `False` for mesh topology). This reduces per-job complexity from O(N^2) for a flat topology to **O(N^2 / m^2)**, a quadratic improvement in the number of groups. Groups operate in parallel.

## Summary Table

| Configuration                        | Messages per job             |
|--------------------------------------|------------------------------|
| Standard PBFT (N agents, flat)       | O(N^2)                       |
| Our protocol, flat mesh (1 group)    | O(pN + N^2), p=O(1) -> O(N^2) |
| Our protocol, m groups, n = N/m      | O(pn + n^2) = **O(N^2 / m^2)** |
| With batching (b jobs/round)         | **O(N^2 / (m^2 * b))** amortized |

## Key Differentiators from Standard PBFT

1. **Self-selection** -- Agents self-filter based on cost, reducing proposal-phase messages (lower constant factor).
2. **Dominance filtering** -- Inferior proposals are suppressed early, preventing redundant prepare/commit exchanges.
3. **Batching** -- Multiple jobs are proposed in a single consensus round, amortizing the O(n^2) overhead.
4. **Mesh grouping** -- Consensus is confined to groups of size n = N/m, yielding a quadratic reduction O(N^2/m^2) vs O(N^2).
