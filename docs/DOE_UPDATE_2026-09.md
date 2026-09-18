# Updates – September 2026 (SwarmAgents)

## Accomplishments

- Executed a real Pegasus workflow on the decentralized scheduler end to end for the first
  time — jobs run in the workflow's own containers across the 92-VM FABRIC slice, in
  dependency order, producing outputs byte-identical to the original Pegasus run.
- Completed the measurement and decision-plane work the two papers depend on: full
  consensus/LLM instrumentation, a delegation regret oracle (routing accuracy 0.822), and
  fixes to several defects that had made prior learning and message-cost numbers unusable.
  Test suite 208 → 711 passing.

## Technical Challenges

- Measurement and infrastructure failures are silent and flattering — a bandit rewarded
  before jobs ran, a month of unsynchronised clocks reported as healthy, a run that
  "completed" while every LLM call failed. Detecting these now takes explicit checks rather
  than trust in the obvious signal.
- Real workflow execution is not yet a fair measurement: data staging is missing, the shared
  filesystem standing in for it flattens the data locality the scheduler exists to exploit,
  and workflows share a single file-name namespace that prevents running several together.

## Next Steps

- Close the remaining evaluation tooling before the 12 Oct code freeze and run the CCGrid
  campaign on the 17-site slice to 270 agents (abstract 24 Nov, paper 1 Dec).
- Replace the shared-filesystem scaffold with real data staging and per-workflow isolation,
  then take up the FGCS journal cells on the LLM decision plane.

---

## Retained detail (not submitted)

Longer draft, kept for the measured numbers behind the bullets above.

## Accomplishments

- **Ran a real Pegasus workflow through the decentralized scheduler for the first time.** The
  extractor now captures each job's executable, arguments and container; the converter emits a
  self-contained runnable bundle (code + inputs + sha256 manifest) and reconstructs the workflow
  DAG from the file names the profiles already carry; agents execute jobs in the workflow's own
  container. Verified on the FABRIC slice: the 4-job soilmoisture DAG scheduled in dependency
  order and its **outputs were byte-identical to the original Pegasus run**, with per-job times
  within ~15% (0.915 / 14.889 / 3.772 / 2.219 s vs Pegasus 1.029 / 14.863 / 4.487 / 2.459 s).
  Apptainer 1.5.3 was installed on all 92 slice VMs so the workflow's own .sif images run rather
  than a substituted rebuild.
- **Completed the campaign instrumentation the paper figures depend on (P0-4).** Per-agent
  consensus message counts and protocol bytes, finalization statistics on both consensus engines,
  LLM token/latency/failure accounting, and one decision record per delegation — including
  *context age at decision time*, the staleness axis no prior version of the system could measure.
- **Delegation regret oracle (P1-1)**, scoring every routing decision against the best group
  actually available. Validated on hardware: **488 decisions, 0 unscored, routing accuracy 0.822**,
  ground-truth validation error 0.041 over 436 jobs.
- **Found and fixed a defect that invalidated every delegation-learning number to date**: the
  bandit was rewarded when a job was *scheduled*, not when it finished, so injected failures were
  invisible — 83 injected, **19 recorded**. After the fix, 76 → 122, 56 → 55, 108 → 108 on three
  verification runs.
- **Completed the LLM decision plane (P0-5 through P0-8):** the model's verdict now enters
  consensus (it previously priced jobs only when proposing), bid elicitation that can actually
  order agents, bid pacing so a failed bid stops out-racing a real one, and designated bidding
  made measurable — **11.36 → 5.28 bidders per job** on a paired 30-agent run.
- **Hardened the fault paths the resilience claims rest on.** A failed agent's in-flight jobs were
  silently stranded for the rest of a run (fixed: Redis-sourced recovery, exactly-once claim
  release, one reassigner per failure); PBFT re-broadcast COMMIT past quorum, biasing
  messages-per-job *against* PBFT; the coordinator-tier cost matrix is now a named, measured
  option rather than a hidden hack. Test suite **208 → 711 passing**; 108 commits,
  +21.5k / −1.5k lines.
- **Repaired the evaluation substrate.** Slice clocks: **26/92 → 92/92 synchronised** (66 hosts
  had been free-running for 30 days, 0.4–1.1 s apart — two nodes on one LAN off by 930 ms);
  agent placement corrected from **65/79 adjacent pairs at the same site to 0/79**, so hierarchical
  consensus traffic actually crosses the WAN; PSC recovered, all 92 VMs in service.
- **Split the publication plan into two papers** and corrected a deadline both had wrong: CCGrid'27
  (consensus + contextual-bandit delegation, paper 1 Dec 2026) and FGCS (LLM decision plane,
  31 Jan 2027).

## Technical Challenges

- **Measurement defects bias in the flattering direction, so they do not announce themselves.**
  Four this month, each found only by deliberate checking: the reward-timing bug above; PBFT's
  inflated message count; three separate approximations in the regret oracle, every one of which
  lowered regret; and coverage logic that read a partially-reporting fleet as healthy. The working
  rule is now to refuse a measurement whose inputs are unknown rather than substitute a plausible
  value.
- **Infrastructure fails silently and looks healthy.** A month of unsynchronised clocks that the
  standard tool reported as fine; a run that completed 197/197 jobs while all 924 of its LLM calls
  returned 403; a reachability sweep that wrote off a live site because rebuilt hosts present a new
  SSH key. Each is now detected explicitly rather than assumed away.
- **Workflow file names are a shared namespace.** Combining workflows is not just a matter of
  concatenating them: the repository's own 7-run profile has **73 colliding output names and 62
  names one workflow produces and another consumes**. Renaming cannot fix it — the names are inside
  the jobs' command lines — so per-workflow isolation is required.
- **Real execution is not yet a measurement.** There is no data stage-in, the shared filesystem
  standing in for it flattens exactly the data locality the scheduler is supposed to exploit, and
  the substrate differs from the Pegasus run being compared against. Stated up front in the docs
  rather than as a closing limitation.
- **Schedule compression.** Verifying the CFP moved the conference deadline a week earlier than
  both plans recorded, cutting drafting and internal review from 13 days to 7.

## Next Steps

- Close the five items owed before the **12 Oct code freeze**: a Sparrow-style sampling baseline
  (the external comparison is otherwise three centralized strawmen), per-site RTT measurement and
  join, a network-partition driver, a static delegation arm, and the context-error column.
- Run the CCGrid campaign on the 17-site slice — consensus × delegation factorial to 270 agents,
  real-WAN sensitivity and exactly-once safety under partition. Framing gate 10 Nov, abstract
  24 Nov, paper 1 Dec.
- Replace the shared-filesystem scaffold with real data staging and stage-out, and give each
  workflow its own working directory so several can share one run.
- FGCS journal cells on the LLM plane: decision caching under a per-coordinator inference budget,
  a wall-clock deadline for LLM decisions, and the cost-of-reasoning accounting.
- Implement and evaluate the decentralized job pool that removes Redis from the control plane.
