# Updates – July/August 2026 (SwarmAgents)

> **"SWARM+: Scalable and Resilient Multi-Agent Consensus for Decentralized Data-Aware Workload Management" was accepted at IEEE eScience 2026** — the paper reported as *submitted* in the June update.

## Accomplishments
- **Completed the gossip/Snow migration promised in June.** Snow consensus + SWIM + epidemic gossip are now the shipped defaults, replacing PBFT/heartbeat. Re-architecting the Snow driver (per-peer batching, capped in-flight decisions, off-driver finalization, locality-weighted sampling) took the hierarchical coordinator tier from livelock to **0.96 s with real consensus**.
- **Closed the scalability review end to end** (Phases 0–4, verified on FABRIC): Redis load cut **13.5k → 1.1k ops/s (~12×)** and now linear in agent count; **Hier-250 selection time 0.32 s vs. the published 24.53 s (~77×)**.
- **Contextual-bandit delegation (LinUCB/LinTS) completed and deployment-validated** across three scenarios: **73.4% vs. 61.9%** success against epsilon-greedy, non-stationarity handled by discounting, and outage/rejoin re-adoption.
- **Deployed the evaluation substrate at scale on FABRIC** — a two-part slice of **~90 VMs across 17 sites** with a Redis node and Prometheus/Grafana monitor, plus a metric-extraction pipeline (`evaluation/collect.py`) so every figure is regenerable from run trees rather than hand-copied from logs.
- **Added hybrid quantum-classical job support** (Phases 1–2): qubit-aware feasibility and cost, and data-triggered split scheduling where a hybrid job becomes co-scheduled quantum-producer / classical-consumer sub-jobs over a streams-based measurement layer.
- **Chaos-engineering evaluation of the LLM decision plane** (30-agent FABRIC fleet, per-host local inference, Chaos Jungle fault injection). Three results:
  - *A partial LLM outage is far more damaging than a total one* — load fairness collapses to **0.331** at 25% of hosts faulted and is **best (0.843) under total failure**; 8 LLM-blind agents captured **280/300 jobs (19× the healthy rate)** because a failed agent bids in ~0 s and out-races the healthy majority still reasoning.
  - *Latency is absorbed; skipping inference is not* — +3 s per call caused zero fallbacks and perfectly even placement.
  - *Semantic corruption is invisible* — poisoned agents inverted their bid polarity (mean 44.1 vs. 70.4 control) at 0% fallback, and completion, latency, fairness and placement stayed flat.
  - Ten SwarmAgents bugs found (**5 fixed**), and four defects filed upstream against Chaos Jungle.
- Designed the removal of Redis from the control plane (submit-to-any-agent ingestion, epidemic pending-pool, rendezvous-hash referee for exactly-once assignment).

## Technical Challenges
- **Gray failure in the decision plane:** the fallback path is orders of magnitude cheaper than the LLM path, so an agent whose reasoning fails is *rewarded* with a scheduling advantage. The fix is a uniform bid deadline, not a penalty on slow agents — slowness proved harmless.
- **Semantic faults evade every health signal we have** — nothing raises, no KPI moves, and the bids are systematically wrong. No production oracle would catch this today.
- **Fault attribution is harder than fault injection.** One early result credited an injected 503 when the proxy had never deployed; scenarios now verify the fault they claim to inject, and a tie-break fix falsified our own first explanation of the semantic result — a wrong mechanism removed before it reached the paper.
- **Substrate and inference risk:** 17 sites is a large failure surface mid-campaign, and FABRIC GPU components (needed to serve inference at constant latency) are the scarcest resource and can gate the whole campaign.

## Next Steps
- Build the semantic decision plane the follow-on paper needs: LLM *group delegation* at coordinators (today an LLM agent only scores its own bid), bandit×LLM composition, decision caching under a per-coordinator inference budget, and per-agent message/byte instrumentation to *measure* the O(n²)→O(fanout) claim rather than assert it.
- Stand up on-slice GPU inference (vLLM) so model capability can be swept at constant latency, keeping one deliberately slow configuration as the case that breaks PBFT.
- Run the CCGrid campaign on the 17-site slice — consensus × decision-plane factorial to 270 agents, real-WAN sensitivity, cost-of-reasoning accounting, and exactly-once safety stress. Code freeze 27 Sep, data freeze 8 Nov, abstract 23 Nov.
- Finish the remaining chaos scenarios on both flat and hierarchical arms; close the open findings (unenforced LLM timeout, bid deadline / fallback penalty).
- Implement and evaluate the decentralized job pool against the Redis-backed control plane.
