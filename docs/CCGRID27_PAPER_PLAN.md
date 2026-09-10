# CCGrid 2027 — conference paper plan (bandit + Snow)

**Decided 2026-09-10.** The work splits into two papers. This one is the conference paper:
contextual-bandit delegation (C1) and gossip consensus (C3) on the 17-site FABRIC slice. The
LLM decision plane (C2) and the quantum-hybrid strand stay in the journal paper
(`FGCS_EVAL_PLAN.md`), which remains the master document for the campaign: **substrate rules,
P0 code work, metric definitions and the experiment matrix are defined there and only there.**
This file holds what is specific to the conference submission — claims, page and figure budget,
which cells it draws on, and the dates.

| | |
|---|---|
| Venue | IEEE/ACM CCGrid 2027, Dallas–Fort Worth |
| Abstract | **24 Nov 2026 (AoE)** |
| Full paper | **8 Dec 2026** |
| Format | 10 pages, IEEE conference template, **including references, figures and tables** |
| Review | **Double-blind** |
| Journal paper | FGCS, submit by 31 Jan 2027 — 7 weeks later, same frozen revision |

---

## 1. What this paper claims

Two of the three claims from the journal thesis, plus the link between them:

- **C1** — a learned delegation policy (LinUCB) beats static and context-blind delegation on
  schedule quality, and keeps beating it under non-stationarity and churn.
- **C3** — gossip (Snow) consensus keeps a hierarchical scheduler completing at scales and RTTs
  where PBFT livelocks, at a bounded latency price.
- **The link** — Snow finalizes assignments faster, so the group state a coordinator learns
  from is *younger*; the bandit's regret should therefore fall under Snow at equal scale, for a
  reason that has nothing to do with either mechanism alone.

### The interaction result does NOT come with them

The journal thesis says the novel result is that *expensive decision-making changes which
consensus protocol you should run*. **That claim needs the LLM, and the LLM is not in this
paper.** A LinUCB decision costs microseconds — a matrix update, not a network round trip — so
the bandit cannot play the role of the expensive plane. Whatever this paper says about the
interaction has to rest on the staleness mechanism above, which is a different and smaller
claim.

That is a deliberate trade, and it has a falsifiable consequence: **if the staleness effect is
null, this paper is two mechanisms sharing a scale story.** That is still a CCGrid paper — a
real 17-site evaluation of gossip consensus for hierarchical scheduling, with an adaptive
delegation policy on top — but it is a weaker one, and we should know which paper we are
writing by the end of the campaign, not in December. See §5 for the gate.

## 2. What is explicitly out of scope

Naming these keeps the 10 pages honest and keeps the journal paper's contribution intact
(CCGrid and FGCS both prohibit substantial overlap; the journal must extend, not repeat):

- Everything LLM: bidding, LLM delegation, bid pacing, elicitation, the cost of reasoning
  (E4), the semantic-inversion test (E8). C2(a) and C2(b) are the journal's spine.
- The quantum/hybrid split-scheduling strand.
- The three-level hierarchy and the 990/1000-agent presets.
- Anything requiring the GPU node or on-slice vLLM.

## 3. Evaluation — cells drawn from the master matrix

All from `FGCS_EVAL_PLAN.md` §5, with the decision-plane axis restricted to `{analytic, bandit}`:

| Cell | From | Purpose here |
|---|---|---|
| E0 | Substrate re-baseline | The within-campaign reference every number is compared against |
| E1′ | `{PBFT, Snow, Hybrid} × {analytic, bandit}` | C3's core figure. Half of E1 — the LLM columns are the journal's |
| E2 | Delegation quality + E2a non-stationarity + E2b churn | C1 in full. This is the paper's deepest experiment |
| E3a | Per-RTT-bin analysis over E1′ runs | C3 on the real WAN. Analysis only, no extra runs |
| E5 | Coordination overhead | The mechanism behind C3: messages/job, rounds to finalize |
| E6 | Safety and correctness | Defuses "did your gossip protocol double-assign anything" |
| E7 | External baselines | See §6 — not optional at this venue |

**Fleet (revised 2026-09-10 for a 83-VM slice).** PSC (`agent-10`–`18`) is down indefinitely,
leaving 83 of 92 VMs, and every rung above Hier-30 was sized for ~90 hosts:

| Rung | Agents/VM | Hosts | Role |
|---|---|---|---|
| Hier-30 | 1 | 30 | Cheap sweeps; matches the eScience'26 operating point |
| **Hier-80** | 1 | 80 | Primary. Replaces Hier-90: 8 groups of 9 + 8 coordinators, same group *shape*, so the ladder still scales group count |
| Mesh-180 | 2 | 90 → **needs density**, see below | Flat-PBFT livelock contrast |
| Hier-270 | 3 | 90 → **needs density** | The collapse point |

Hier-30 and Hier-80 run at one agent per VM with **site-interleaved placement**
(`make_agent_hosts.py`, default). This matters more than it sounds: placement follows the order
of `agent_hosts.txt`, and agent ids map to sites in contiguous blocks, so the numeric ordering
used by every run before 2026-09-10 put 65 of 79 adjacent agents at the *same site* — a
hierarchical group's consensus traffic never left the building. Interleaved is 0 of 79.
**Every WAN number in this paper must come from interleaved runs.**

The top two rungs cannot fit 83 VMs at their planned density and must be packed further
(Mesh-180 at 3/VM = 60 hosts, Hier-270 at 4/VM = 68). **Report agents-per-VM per rung in the
results table.** Co-locating agents converts inter-agent messages into loopback, which flatters
exactly the quantity C3 measures, so a scaling curve that mixes densities silently is not a
scaling curve.

### Framing that decides the reviews

**Jobs simulate their wall time; they do not compute.** `Job.execute()` sleeps
`clamp(wall_time × scale, min, max)` from the Pegasus trace, capped at 120 s. The consensus
messages, delegation decisions, failure detection and WAN RTTs are all real; the application
work is not. So:

- **Headline metrics are control-plane:** completion %, selection/decision latency
  (p50/p95, split into consensus | queueing), consensus messages per job, rounds to finalize,
  delegation regret vs oracle, time-to-re-adoption after churn.
- **Makespan, throughput and utilisation are secondary**, reported with the cap and the
  scaling factor stated in the caption every time they appear.
- Say this in §IV before the first results table, not in a limitations paragraph at the end.
  A reviewer who discovers it late reads it as concealment.

## 4. Ten pages, and the figures that fit

Ten pages *including references* is roughly 8.5 of text. Budget:

| § | Pages | Content |
|---|---|---|
| I Intro + contributions | 1.25 | The collapse-and-repair result up front |
| II Background / related | 1.0 | BFT vs gossip; bandits for scheduling; cite eScience'26 in third person |
| III Design | 2.0 | Coordinator tier, Snow engine, LinUCB delegation with the GroupSnapshot context |
| IV Setup | 0.75 | Slice, fleet table with agents/VM, workload, the simulated-execution statement |
| V Results | 3.5 | Figures below |
| VI Threats / limitations | 0.5 | Density, simulated execution, single-implementation baselines |
| VII Conclusion + refs | 1.0 | |

**Six figures, no more.** Each has to earn a third of a page:

1. **F1 — collapse and repair.** Completion % vs scale for PBFT / Snow / Hybrid. The paper's
   reason to exist; nothing else goes on page 1.
2. **F2 — mechanism.** Consensus messages per job and rounds-to-finalize vs scale, showing
   *why* F1 happens.
3. **F3 — per-RTT-bin latency** (E3a). Selection latency by measured RTT bin per engine. No
   prior SWARM paper could produce this.
4. **F4 — delegation quality** (C1). Regret vs oracle over time for
   `{static, ε-greedy, UCB1, LinUCB}`, with the non-stationarity flip marked.
5. **F5 — churn.** Time-to-re-adoption after a group outage and rejoin, fixes on/off.
6. **F6 — the link, or its absence.** Bandit regret under PBFT vs Snow at equal scale against
   context age. If the effect is null, this becomes a table in §VI and F1–F5 carry the paper.

Two tables at most: the fleet/workload table and the E7 baseline comparison.

### Double-blind

The slice is a shared facility, so describing it is not identifying. What is: citing
eScience'26 as "our prior work". Cite it in the third person throughout, and check the
acknowledgements and the artifact URL before submitting.

## 5. Dates, working backwards from 8 December

| By | Gate |
|---|---|
| **12 Oct** | Code freeze (unchanged). P0-4 instrumentation must include **context age at decision time**, or F6 cannot be produced at all — see §4 of the master plan |
| 13 Oct | Campaign starts. E0 first, then E1′, on the frozen revision, interleaved placement |
| **10 Nov** | All E1′/E2 cells collected. **Gate: is the staleness effect (F6) real?** The answer decides whether the paper is "two mechanisms + an interaction" or "two mechanisms + a scale story", and the intro is written differently in each case |
| 17 Nov | E3a/E5/E6 analysis complete; all six figures drafted from real data |
| **24 Nov** | **Abstract due.** Title and abstract must match the F6 outcome |
| 1 Dec | Full draft, internal review |
| **8 Dec** | **Submit** |
| 15 Dec | Journal paper restart: C2 cells (E4, E8) on the same frozen revision |

The gap between 8 Dec and the 31 Jan journal deadline is 7 weeks, and the journal reuses this
campaign's substrate — so **no re-measurement between the two papers** (see
`swarm-no-cross-substrate-result-reuse`: one substrate, one revision, both papers).

## 6. Reviewer pre-mortem specific to this venue

| Attack | Answer |
|---|---|
| "Jobs don't run, you sleep." | §IV states it before any result; headline claims are control-plane. Real wall times come from a 25k-job Pegasus trace, so the *distribution* is real even though the work is not |
| "You compare your Snow to your PBFT." | E7 external baselines are mandatory, not optional. The PBFT arm must be tuned and the tuning reported, or the comparison is worthless |
| "270 agents is not scale." | Lead with 17 sites and real RTT, not the agent count. Density is disclosed per rung |
| "Avalanche and LinUCB are both off the shelf." | True. The contribution is the composition and the WAN-scale empirical result — say so in §I rather than implying novelty in the mechanisms |
| "Where is the LLM work this group published on?" | Under double-blind, cite it in third person as related work. It is the journal paper's subject and is not needed here |
