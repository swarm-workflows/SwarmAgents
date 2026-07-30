# Updates – June 2026 (SwarmAgents)

## Accomplishments
- Designed and began implementing a **gossip-based consensus stack** (SWIM failure detection + epidemic gossip + Snow-adapted job assignment) to replace the O(n²) PBFT-like protocol, targeting 1000+ agents while preserving exactly-once assignment.
- **Extended and evaluated the Hierarchical Consensus Framework under failures** — dual-mode leaf-node detection, MAB-driven delegation at Coordinator Agents, and co-parent failover for coordinator redundancy; this work was **submitted to eScience 2026**.
- Scaled hierarchical topologies to **60/110/120 agents** and stabilized large multi-host (30-VM) runs.
- Completed a **MAB evaluation with Pegasus workloads** (including a `top_k` sweep) and added **distributed baseline schedulers** for fair comparison against SWARM+.
- Added **DTN-aware data-locality modeling** across mesh and hierarchical topologies, and hardened reliability (SIGTERM/SIGHUP handling, FD limits, bounded retries, failure-state cleanup).

## Technical Challenges
- Quadratic per-job message complexity limits PBFT-like scaling beyond a few hundred agents, motivating the gossip/Snow redesign.
- Distinguishing genuine failures from transient network/GC delays under load without false-positive reselections.
- Stabilizing large multi-host runs (SSH launch hangs, FD exhaustion, signal handling under load).

## Next Steps
- Complete and evaluate the gossip/Snow protocol vs. the PBFT-like baseline (message complexity, latency, correctness) at 100–1000+ agents.
- Deploy and evaluate SwarmAgents on **FABRIC** across distributed sites, including DTN-aware scheduling.
- Integrate diameter-guided ring optimization with the gossip topology layer and explore LLM-assisted topology adaptation under failures.
