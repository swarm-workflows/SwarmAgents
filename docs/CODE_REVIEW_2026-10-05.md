# Code review — 2026-10-05

Second critical read of the code base, against the same question as the 2026-09-18 review
(`CODE_REVIEW_2026-09-18.md`): **where can a number either paper reports be wrong without
anything in the run saying so?** Everything found there is excluded here. Ranked by that
question; within a rank, by how many cells it touches. Every finding names the file and line,
the failing scenario, the metric it moves and the direction, and whether a test in `tests/` would
catch it (none of the HIGH items has one). **Status (2026-10-06): every HIGH item and every MEDIUM item has been addressed; each heading carries its own FIXED / PARTLY FIXED note.** Regression tests are in `tests/test_review_2026_10_05_*.py` (13 files); where a file's tests could run against the pre-fix code, the commit message says how many failed there. **Still partly open:** §44 (a run with no metrics.json reads `metrics_complete`), §46 (validate()'s last-row-wins join; plotting Redis readers), §56 (`--pegasus-dag-gating` defaults off), §60 (a remote agent start is still backgrounded, so `ssh_check` cannot fail; `cleanup.py` exits 0 on a Redis error), §64 (names with a directory component are refused, not supported), §65 (`_retry_unpersisted_completions` re-saves a stale payload), §67 (an input already in the work dir wins over the registry), §72 (apptainer's `docker://` conversion is inside the job's time), §33/§38 (delegation and consensus durations on `time.time()`), and the §G config-drift table's remaining rows.

**Method.** Six independent read-only passes, one per subsystem (consensus + membership; agent
core + repository + selection; execution + staging; metrics + collection + plotting; run tooling
+ converters; LLM plane + bandit), each given the project's recurring failure patterns as lenses
("the guard asked the wrong container", "one key, one default", "liveness sets must agree",
"refuse, don't approximate", "two meanings of completed", "test the default, not your example",
wall clock vs monotonic). Every HIGH item was then re-verified against the code by a second
reader; items 2 (PBFT own vote), 6 (leader guard) and 8 (DTN dicts) were additionally
**reproduced by execution** — the first two by running several real `ConsensusEngine` instances
over an in-memory bus, the third in-process against `AgentInfo`. MEDIUM and LOW items are
verified by line reference only.

**Read directly** (~28k lines): everything under `swarm/` except `swarm/quantum/` and the
generated `_pb2` files; `evaluation/{collect,oracle}.py`; all of `plotting/`; `run_test.py`,
`batch_tests_v2.py`, `generate_configs.py`, `make_agent_hosts.py`, `job_generator.py`,
`job_distributor.py`, `cleanup.py`, `kill_agents.py`, `main.py`, `staging_site.py`,
`pegasus_profile_extractor.py`, `pegasus_to_swarm_converter.py`, `swarm-multi-start.sh`,
`stop_agents_v2.sh`, `fix_slice_clocks.sh`, `setup_apptainer.sh`, `setup_nfs_workflow.sh`.
**Not reviewed**: `swarm/quantum/`, `colmena_agent.py`, `baselines/`, the notebooks.

**Headline.** The shipped configuration — `consensus.protocol: snow`, `aggressive_failure_detection:
true`, `failure_detection.protocol: swim` — has four defects that each on its own makes the Snow
plane behave differently from what the design says (§1–§4), and the PBFT plane has an off-by-one in
quorum counting (§5) that has been there since the engine was written. Three hierarchy paths
run jobs twice (§7–§9) and nothing in a run reports a double execution. The collector's livelock
criterion cannot fire (§13) and its cell means silently exclude the runs that collapse (§14).
**No Snow number, no hierarchical-failure number, and no number through the current
`collect.py` is citable until §1–§9 and §13–§18 land.** The 2026-09-18 review ended with the same
sentence about a different list; this one is longer because the earlier passes read for the
items they already suspected, and this one read everything.

---

## A. Consensus — the shipped protocol does not do what the design says

### 1. Snow compares a load-penalised cost against an unpenalised one — **HIGH, every Snow cell — FIXED 2026-10-06**

*Status:* `wire_cost_for_job` — the one path both planes answer peers through — now builds the number exactly as `selection_main` builds a proposal: feasibility against the agent's own info (infeasible → +inf), the plane's base cost, × `_projected_load_factor`, rounded by `proposal_cost`. A peer's answer and the proposal it would have made are equal by construction (`tests/test_review_2026_10_05_snow.py::TestWireCostMatchesProposalCost`). The same method now refuses jobs outside the pending queue for both planes, which closes the LLM half of §26.

`swarm/agents/resource_agent.py:2554-2566` (proposer), `:3795-3806` (`native_cost_for_job`),
`:195-207` (`my_cost_for_job`); `swarm/consensus/gossip_engine.py:302-313`.

**Mechanism.** The proposer's advertised cost is a cell of `cost_matrix_with_penalities`, i.e. the
base cost multiplied by `_projected_load_factor` (`1 + ((load + proposed)/100)^1.5`). A peer
answering a Snow query computes `_cost_job_on_agent(obj, info)` — the base cost, with no load
factor and no `is_job_feasible` check — and votes for itself whenever that is below the
initiator's number. In a homogeneous fleet under any load the inequality holds for every peer, so
each peer casts one vote for itself, no candidate reaches α, the round conflicts, and after
`max_rounds` the engine logs `[SNOW_ABANDON]`. The job then sits in PRE_PREPARE until
`_restart_selection` fires at `reselection_timeout_s` — **300 s in the shipped config** (the code
default is 60; CLAUDE.md says 60). `_reset_orphaned_jobs`, which would catch it sooner, is dead:
its call is commented out at `:2641`. PBFT compares penalised against penalised at every peer.

**Paper impact.** Snow placement ignores load; Snow `conflict_rounds` and `abandoned` are
inflated; Snow selection latency carries a 300 s tail that is a unit mismatch, not a protocol
property. Every Snow-vs-PBFT load, fairness, makespan and latency comparison moves.
`GOSSIP_CONSENSUS_DESIGN.md` §4.4 promises "if feasible and lowest"; neither half is true.

**Tests.** None. `tests/test_snow.py` drives the engine with a fake host cost, so the two call
sites never meet.

### 2. Snow peers abstain on every job that needs a DTN — **HIGH, every `--dtns` and workflow cell — reproduced — FIXED 2026-10-06**

*Status:* the `AgentInfo.dtns` setter converts every element to a `DataNode` whatever shape arrives (config list, Redis dict, or `DataNode`s), and treats `None` as no DTNs rather than raising. In-process and round-tripped copies now agree (`TestOwnDtnsAreDataNodes`).

`swarm/agents/resource_agent.py:1721` (`dtns=self.config.get("dtns")`),
`swarm/models/agent_info.py:109-111`, `:2384-2385` (`dtn.name` in `compute_job_cost`),
`:3797-3805` (the `except Exception` in `native_cost_for_job`).

**Mechanism.** A leaf builds its own `last_agent_info` from the raw config list. `AgentInfo.dtns`'s
list branch stores each element as-is, so in-process the values are **dicts**, not `DataNode`s.
`compute_job_cost` reads `dtn.name` → `AttributeError: 'dict' object has no attribute 'name'`,
which `native_cost_for_job` swallows at DEBUG and turns into `None`. Reproduced: the in-process
`AgentInfo` raises; the same object after `to_dict`/`from_dict` prices normally — which is why
`neighbor_map` entries (Redis round-tripped) and the proposer's own matrix (built from
`neighbor_map`) are unaffected and nothing visible fails.

**Consequence.** Under Snow, `None` means "yield" (§3), so for any job whose `required_dtns()` is
non-empty every responder endorses the initiator and the proposer always wins. Snow's cost voting
is inert for every `--dtns` cell and every converted Pegasus workflow with real DTN names.

**Tests.** None exercises `native_cost_for_job` with a DTN-bearing job.

### 3. "Yield" is a vote for the initiator, not an abstention — **HIGH, design mismatch — FIXED 2026-10-06**

*Status:* a peer with no opinion answers `preferred_agent=None`; the initiator counts it toward no candidate and takes α over `max(voters, sampled − abstained)`. A peer that does not answer at all stays in the denominator — silence is not an abstention, or one fast responder would decide every round. An infeasible peer (+inf) never prefers itself, even on a tie with an initiator that sent no cost. The `llm.snow_cost_fallback: yield` key keeps its name and now means what CLAUDE.md always said. `tests/test_cost_scale.py` had pinned the endorsement; it now pins the abstention (`TestAbstention`).

`swarm/consensus/gossip_engine.py:297-300`.

```python
if my_cost is None:
    # Can't evaluate locally — yield.
    preferred = int(q_preferred) if q_preferred is not None else self.agent_id
```

The response names the initiator's candidate as `preferred`, which the initiator tallies toward
α. CLAUDE.md (`llm.snow_cost_fallback: yield` "abstains on a cache miss") and
`llm_agent.py:913` both call this abstaining. It is an endorsement. Combined with §1 and §2, the
first agent to query collects near-unanimous support, which is the race-to-propose regime the
engine was built to remove, reached through the vote instead of the proposal. For LLM agents a
miss is the common case (a peer prices only jobs in its own window; with `designate_bidder` every
non-designated agent misses), so `llm_wire_cost_misses` should be read as endorsements, not
abstentions.

**Tests.** None.

### 4. `aggressive_failure_detection` is shipped `true`, code default `False`, and re-executes a live peer's jobs — **HIGH, exactly-once broken under the shipped config — FIXED 2026-10-06**

*Status:* shipped `false`, matching the code default. `ChannelPool._set_up` emits on transitions only. The gRPC UP callback no longer readmits anyone: it turned out to be the ONLY path out of `failed_agents`, heartbeat verdicts included. Readmission now happens in the neighbor refresh, and only on a Redis record newer than the one that was judged failed — both stamps the peer's own clock, so inter-host offset does not enter (`_readmit_if_heartbeat_resumed`, `TestReadmissionIsByHeartbeat`). Aggressive mode still exists for an experiment that wants it; its DOWN path is unchanged.

`config_swarm_multi.yml:202`; `swarm/agents/resource_agent.py:924` (default), `:4348-4390`
(`on_peer_status`), `:3943-3944` → `:4176-4270` (`_reassign_jobs_from_failed_agent`);
`swarm/comm/grpc_client.py:109-139`.

**Mechanism.** One health RPC that exceeds its 0.7 s deadline, or a `TRANSIENT_FAILURE` on the
channel, delivers `on_peer_status(DOWN)`. That adds the peer to `failed_agents` and runs the full
failed-agent path: release its exactly-once claims, reset its READY/RUNNING jobs to PENDING. The
peer is alive and still executing those jobs. They are re-elected and **run twice**. The
2026-09-15 recovery fix made this path effective; this key makes it fire on a 700 ms stall.

**Second half.** `_set_up` fires `_on_status` on every 2 s probe, not only on a change. A peer
whose gRPC server still answers — including a zombie whose heartbeat has stopped — is removed
from `failed_agents` within 2 s of being added. It returns to the broadcast set but stays out of
`neighbor_map`, so **its votes count while it is excluded from the quorum denominator**:
"liveness sets must agree", violated from the other direction than SWIM was. Heartbeat is not
authoritative in either direction while this key is on. All of it runs on the gRPC callback
thread concurrently with the periodic thread. The key is not mentioned in CLAUDE.md.

**Paper impact.** E2b/E6 completion, duplicate-execution and recovery-time numbers; any cell in
which a WAN RTT spike exceeds 700 ms, which on a 17-site slice is every cell.

**Tests.** None for `on_peer_status`.

### 5. PBFT never counts its own vote, but the quorum denominator includes itself — **HIGH, reproduced — FIXED 2026-10-06**

*Status:* a vote is now counted when it is cast, by whoever casts it — peers' on arrival as before, our own PREPARE when `on_proposal` sends it and our own COMMIT when we send it. COMMIT sending and finalization are each in one helper (`_commit_where_prepared`, `_finalize_if_quorum`) that every phase calls, `propose()` included, so a quorum completed by the agent's own vote acts at once and the one-COMMIT-per-proposal rule cannot be bypassed by a new caller. A PREPARE that arrives before its PROPOSAL adds no own vote — we have not cast one yet. `tests/test_review_2026_10_05_pbft.py` runs N real engines over a bus that encodes and decodes every message as the transport does: majority-alive clusters at n = 2, 3, 4, 5, 7 now finalize, 2-of-5 still does not, a lone agent finalizes (its quorum is 1), and the controls — healthy cluster, competing proposals, one COMMIT per agent, no double-counted vote — pass before and after. Two harness traps worth knowing: sharing message objects between engines makes them mutate one vote list, and `Prepare`/`Commit` subclass `Proposal`, so dispatch must test them first.

`swarm/consensus/engine.py:216` (proposer self-prepares), `:348` (PREPARE appends
`msg.agents[0]` only), `:462` (COMMIT appends `msg.agents[0]` only); `agent_grpc.py:536`
(`live_agent_count // 2 + 1`, over a `neighbor_map` that contains self); `agent_grpc.py:513-521`
(`broadcast` sends to `topology.peers`, which excludes self).

**Mechanism.** A PREPARE or COMMIT is recorded only when it arrives from a peer. An agent's own
COMMIT is broadcast and never appended to `proposal.commits`; a participant's own PREPARE is
never appended to `proposal.prepares` (only the proposer self-prepares). `calculate_quorum` is a
majority of a set that includes the agent. So an agent needs `q` votes from the `n − 1` others,
not `q − 1`. Reproduced with real engines over an in-memory bus:

| Live agents | Result |
|---|---|
| 1 or 2 | never finalize |
| 3, one silent | stalls |
| 4, one silent | stalls |
| 5, two silent | stalls |

PBFT tolerates one fewer crash than a majority quorum should. Once heartbeat evicts a dead peer
from a 3-agent group, the group has 2 live members, quorum 2, and can never finalize again —
every job cycles through the reselection timeout for the rest of the run. CLAUDE.md's "a fully
isolated agent has quorum 1" is true and it still cannot finalize.

**Paper impact.** The PBFT arm's apparent fault tolerance in every E2/E6 cell; small-tier
hierarchical behaviour (group of 9 with 4 dead is dead, not degraded).

**Tests.** None. Every PBFT test uses a fixed-quorum fake host; none runs several engines together.

### 6. PBFT's leader guard asks the wrong container — **MEDIUM-HIGH, reproduced — FIXED 2026-10-06**

*Status:* leader iff `proposal.agent_id == self.agent_id`. Both removal paths are restart cleanups the peers are not told about; when late votes carry the old proposal to quorum every peer has already recorded this agent as the assignee, so acting as leader is what keeps the cluster consistent — refusing is what stranded the job.

`swarm/consensus/engine.py:474`:
`if proposal.agent_id == self.agent_id and self.outgoing.contains(object_id=..., p_id=...)`.

Two paths remove the agent's own proposal from `outgoing` while the election is still live:
`_restart_selection` at the reselection timeout, and `_clear_consensus_for_failed_agent`
(`resource_agent.py:3994-4003`), which drops any own proposal a dead peer had prepared. Late
PREPAREs/COMMITs then re-adopt the proposal into `incoming`, and when they reach quorum the
finalize takes the **participant** branch with leader = self: `select_job` is never called, every
peer records this agent as the assignee, `finalized_count` increments. Confirmed by execution.
Same shape as the 2026-09-18 straggler defect, one container over.

**Tests.** None.

### 7–12. Snow and SWIM, medium

- **7. [FIXED 2026-10-06 — α never falls below 2 when two or more peers voted; and a periodic sweep (`_adopt_unannounced_claims`, Snow only, 256 jobs/tick round-robin) reads the claims of this agent's undecided pending jobs: a claim naming this agent is an election it won unannounced and it acts as leader, one naming another stops it proposing]** Small samples. `round(0.7 · eff)` is 1 for eff ≤ 2 (`gossip_engine.py:195-201`), so one
  self-vote wins a round; ties between the initiator's candidate and a self-voter are broken by
  `Counter.most_common`, i.e. response arrival order (`:490`); the preference moves to that peer
  (`:512-515`) and the job is claimed for it (`:525`, `:584`). Only the agent that wins the CAS
  gets `on_leader_elected`; a preferred peer that never proposed is never told and the job is
  stranded. Applies to tiers of ≤ 3, any group shrunk to that, and truncated SWIM peer sets (§11).
- **8. [FIXED 2026-10-06 — the finalize worker checks the Redis claim: an existing claim wins whatever the hint said; no claim means the hint was stale, and the instance re-opens and ignores further hints (`stale_decided_hints` counts them). Closed further the same day after the stop-time review: a decided-hint is never tallied as a vote (several stale peers could otherwise converge the re-opened vote on the dead agent), and no finalize claims a job for a candidate heartbeat does not consider live — it re-opens instead (`dead_candidate_refusals`); and a verified existing claim is used read-only, never passed to the CAS, so a claim released between the read and the CAS cannot be re-claimed for the agent it named; a decision naming an agent not live here is never committed locally; and under Snow a PENDING record with no claim counts as reset evidence, because the timestamp rule misses a reset stamped just before the local commit]** `already_decided` fast path has no α check (`:284-289`, `:354-356`). One response
  finalizes from the responder's local `job_assignments`. Agent X dies; the first detector releases
  the claim and resets J; a peer that has not yet detected answers `already_decided = X`; the CAS
  on the freed key gives J to the dead X; `try_claim_reassignment`'s 300 s TTL is spent, so J is
  stranded.
- **9. [FIXED 2026-10-06 — the quorum check and `_note_finalized` are one locked step (host callbacks outside it); the stash and its replay pops share a lock. A test that widens the race window fails on the old engine]** The PBFT engine has no lock and is driven from the inbound thread (`_process`), the
  periodic thread (`_replay_pending_consensus` at `resource_agent.py:1031-1049`, `remove_object`
  at `:1053, 1114, 1127, 4255`, `_clear_consensus_for_failed_agent`) and the selection thread
  (`propose`, `:2624`). A replayed COMMIT and an inbound COMMIT for one job can both pass the
  quorum check → double `_record_finalize`, double `select_job`. `_stash_pending`'s
  setdefault-then-append (`:139-147`) racing `pop` (`:1035`) loses a message silently.
- **10. [FIXED 2026-10-06 — `is_agreement_achieved` and `_already_finalized` run first on the object id; `get_object` only when the message will be used]** PBFT does a Redis GET before its cheap straggler checks (`engine.py:227-242`,
  `:280-291`, `:412-424` call `host.get_object` before `is_agreement_achieved` /
  `_already_finalized`; `get_object` at `resource_agent.py:105-130` is a synchronous GET on the
  single inbound thread once the job has left the local queue). `n − q` WAN round trips per phase
  per job, biased against PBFT, in exactly the wide-tier cells where PBFT is already losing.
- **11. [FIXED 2026-10-06 — host-known agents are admitted on every probe pick (never overriding a verdict); a ping from a peer held SUSPECT/FAILED is answered with that rumour, so the victim refutes with a higher incarnation on its next round instead of by chance; an unknown pinger is admitted; a relayed ack carries its target. No separate push-pull anti-entropy — the ack path covers the falsely-failed case]** SWIM membership is incomplete and recovery from FAILED depends on chance. Members are
  seeded from `neighbor_map` only while the candidate set is empty (`swim.py:576-597`), and SWIM
  starts on the first periodic tick, so late registrants are learned only by piggyback. `on_ping`
  (`:247-259`) never adds or revives the sender — a ping from a FAILED peer is first-hand proof of
  life and is ignored. A relayed ack is forwarded with `target_agent=None` because `on_ack` already
  popped the probe (`:266`, `:405-407`), so a late relayed ack cannot clear a suspicion. No
  anti-entropy; a FAILED verdict is undone only if the victim hears its own rumour inside the
  piggyback window. `live_peer_ids` (`resource_agent.py:183-193`) uses any non-empty SWIM set, so
  Snow samples a truncated pool (feeding §7) and gossip skips the missing agents.
  `test_swim_advisory.py:166-188` hands the verdict to the victim directly; wire delivery is untested.
- **12. [FIXED 2026-10-06 — Snow samples SWIM-live peers that are also in `neighbor_map`, i.e. reachable by `send`]** Snow's sample set and its deliverable set differ. Snow samples SWIM-live peers;
  `agent.send` silently returns when the destination is not in `neighbor_map`
  (`agent_grpc.py:524-526`); `_safe_send` still reports the send dispatched and `state.queried`
  counts it (`gossip_engine.py:455-459`). A round that sampled a heartbeat-evicted peer runs to the
  full `round_timeout` and `queries_` is overstated.

---

## B. Hierarchy and failure recovery — three paths that run a job twice

### 13. A dead coordinator's delegated jobs are re-elected and run again — **HIGH, every hierarchical kill cell — FIXED 2026-10-06**

*Status:* the coordinator-tier record now carries `delegated_groups` (new `Job` field), written BEFORE any child copy. A peer coordinator reassigning a dead coordinator's job leaves it alone when any child copy has moved past PENDING, and only takes it back — withdrawing the unpicked copies — when none has (`_take_back_unpicked_delegation`). Unreadable child copies count as owned: an untracked job is the lesser error than a twice-run one. Open: nobody monitors the job afterwards, so its bandit outcome is lost with the coordinator (a co-parent adopting the dead coordinator's `delegated_jobs` would close that).

`swarm/agents/resource_agent.py:3312-3335` (`scheduling_main` saves the job only at
`level − 1`), `:3375-3383` (`select_job` persisted the coordinator-level record READY),
`:4199-4250` (`_reassign_jobs_from_failed_agent` resets any READY/RUNNING record whose
`leader_id` is the dead agent), `:1014-1015` (peers discard a PENDING job from
`completed_jobs_set`).

**Mechanism.** After delegation the coordinator-level record stays READY with `leader_id` = the
coordinator for the whole delegated life of the job; nothing touches it until the delegation
monitor writes COMPLETE. When the coordinator dies, a peer coordinator's reassignment finds every
such record, resets it to PENDING, releases the claim. The job is re-elected and re-delegated.
`repository.save` to the child key **overwrites the child's RUNNING record with PENDING**; the
leaf peers see PENDING, drop the job from `completed_jobs_set` (the 2026-09-15 "peers must forget
it" rule, doing its job on the wrong trigger), and run it a second time. Reported independently
by two of the six passes.

**Paper impact.** E2b/E6 on a hierarchy: completion %, duplicate execution, recovery time, and
the bandit (the dead coordinator's groups are credited twice or charged twice).

**Tests.** `tests/test_failed_agent_reassignment.py` (11) are all flat-topology.

### 14. `_reassign_delegated_job` does not release the Snow claim — **HIGH, Snow coordinator tier — FIXED 2026-10-06**

*Status:* the claim is released and `delegated_groups` cleared before the job is re-saved PENDING.

`swarm/agents/resource_agent.py:1346-1478`; re-save as PENDING at `:1468-1471`, no
`release_assignment`.

The claim still names coordinator C. C has put itself in `delegation_failed_agents`, so it is
infeasible for itself and never proposes. Every other coordinator's finalize gets C back from the
CAS and takes the participant path, so nobody ever calls `select_job`. Repeats every reselection
timeout. Under `coordinator_cost_matrix: self` C's own copy goes BLOCKED and is retired
**FAILED** after `max_infeasible_retries`. **A feasible job is lost on every delegation timeout.**
PBFT is unaffected (no claim). `test_reward_timing.py` mocks this method.

### 15. Pulling a delegated job back leaves live copies in the children's pending queues — **HIGH, the overload path — FIXED 2026-10-06**

*Status:* each tick, a local pending job absent from all four state indices on two consecutive scans is dropped with its consensus state (`_purge_vanished_jobs`; two scans because the index reads are not transactional). `SimpleQueue.move_to_end` no longer re-inserts a removed object. Side effect: a job one agent retires FAILED is now also dropped by its peers, which kept retrying it. **Two races closed the same day** (found by the stop-time review gate): every withdrawal now deletes a copy only while it is still PENDING (`Repository.delete_if_state`, under WATCH), so a copy a child picked up between the coordinator's read and its delete is left running and tracked, and a pull-back that loses that race is abandoned; and `select_job` saves with `require_existing`, so a child that wins a withdrawn copy in the purge window cannot re-create it.

`resource_agent.py:1451-1467` (parent `delete`s the child key), `:1909-1919` (children purge
only ids present in the READY/RUNNING/COMPLETE indices), `simple_queue.py` (`move_to_end`
re-inserts), `repository.py:236-246` (`delete` is GET / SREM / DEL, not atomic).

A deleted key is in no index, so every child keeps the job PENDING locally and keeps proposing it.
When the group's load drops, the winner's `select_job` re-creates the child record and runs it
while the parent has already re-delegated it elsewhere. **Concurrent duplicate execution**, on the
delegation-timeout path — exactly when hierarchical cells are measured. `move_to_end` can
re-insert an object another thread just removed; a concurrent READY save during the non-atomic
delete leaves a dangling index entry.

### 16. Fan-out > 1: one group's untaken copy reassigns a job another group is running — **MEDIUM-HIGH — FIXED 2026-10-06**

*Status:* the monitor reads every copy first: terminal beats in-progress beats unpicked. Once any group has the job, the other groups' unpicked copies are withdrawn; a timeout (and its bandit penalty) applies only when no copy was picked up.

`resource_agent.py:1229-1265` (`_monitor_delegated_jobs`): a copy still PENDING past the timeout
sets `job_still_pending` and `break`s, even when another group's copy is RUNNING (that branch does
not break). `:1334-1344` then charges a timeout failure to **every** group including the executing
one, and `_reassign_delegated_job` (`:1459-1470`) deletes the running group's record and
re-queues the job. Wrong bandit penalty plus duplicate execution. `test_reward_timing` is
single-group.

### 17–21. Recovery and state, medium

- **17. [FIXED 2026-10-06 — the child's exit status is propagated with COMPLETE; the grace drop leaves the record alone]** The execution-grace drop writes the parent record COMPLETE (`:1274-1282` →
  `:1305-1322`): a job dropped after `delegation_timeout_s + delegation_exec_grace_s` takes the
  same `jobs_processed` path as a finished one. If the whole child group died (a site outage with
  site-clustered groups) the job is never re-delegated and is lost. And **exit status never
  propagates up** (`:1316` sets `state` only), so in a 3-level fleet the top bandit is credited a
  success for every delegated job, failed or not. `test_reward_timing.py:246` checks that no
  outcome is reported, not this write.
- **18. Failed-agent reassignment runs once and never retries** (`repository.py:476-490`,
  `resource_agent.py:3883-3885`, `:4234-4272`). The reassignment claim has a 300 s TTL so a dead
  reassigner does not strand the job — but nobody retries after it expires. If the winner's
  release/save raises (caught at `:4271`) or the winner dies, the job stays READY/RUNNING forever.
  Snow variant: a leader that dies between winning the CAS and persisting READY leaves a PENDING
  record; reassignment scans READY/RUNNING only, the claim is never released, every re-proposal
  finalizes to the corpse, once per 300 s.
- **19. [FIXED 2026-10-06 — delegation: parent record, child copies, then local state; `select_job` and `schedule_job` write first (`require_existing`), and a failed `schedule_job` write returns the job to `selected_queue`]** Local state is mutated before the Redis write in `select_job` (`:3379-3383`),
  `schedule_job` (`:3360-3373`: `ready_queue` and the completed set before the RUNNING save) and
  the delegation save (`:3314-3326`: `selected_queue.remove` before the per-group save and
  `delegated_jobs.set`). A write failure leaves a job that holds capacity and is tracked by
  nothing — the same mistake `execute_job` fixed for the completion write on 2026-09-15.
- **20. [FIXED 2026-10-06 — the child copy is a separate object; the coordinator's own stays READY]** A coordinator can re-propose a job it just delegated. `scheduling_main` sets the shared
  queue object back to PENDING and `job.level = level − 1` (`:3316`) before the next periodic READY
  scan removes it from `pending_queue`; `selection_main` has no `is_job_completed` check
  (`:2527`). Within one tick a `self`-matrix coordinator proposes it again; under Snow peers
  answer `already_decided` with winner = self, it finalizes, `select_job` runs again and the child
  record is overwritten. Under PBFT it inflates `proposers_per_job_l1` and `reproposals`.
- **21. `_update_pending_jobs` accepts reset evidence without checking the record's state**
  (`:998-1032`, `_reset_evidence` ~`:3040-3060`): the PENDING id list is a snapshot and the batch
  fetch is later, so a record that moved to READY/RUNNING in between with
  `last_transition_at > decided_at + 1` reads as a reset — decision forgotten (including the
  engine's `_finalized`), the READY record added to `pending_queue`, `completed_jobs_set.discard`
  run unconditionally. Reopens the straggler window for a tick.

---

## C. Bandit and LLM plane

### 22. The pending-context TTL is still sized for the pre-P0-9 reward timing — **HIGH for LinUCB/LinTS — FIXED 2026-10-06**

*Status:* `MABManager` takes `outcome_horizon_s` (the agent passes `delegation_timeout_s + delegation_exec_grace_s`, the point past which the monitor gives up) and defaults the TTL to `max(2 × timeout, horizon + timeout)`; an explicit `mab.pending_ttl_s` still wins.

`swarm/rl/mab_manager.py:91` (`pending_ttl_s = 2 × delegation_timeout_s`), `:267`
(`_sweep_pending` discards the stored context), `:360-381` (`report_outcome`);
`resource_agent.py:602-618` (`delegation_exec_grace_s`).

P0-9 moved the bandit reward from scheduling time to completion. Outcomes now arrive up to
`delegation_timeout_s + delegation_exec_grace_s` later — 240 s shipped, 1920 s with the wall-time
cap off or under real execution — while the context is swept at 240 s. When that happens: the
model update is skipped (`context=None`) while the arm's pull and reward counters still climb;
the per-type failure window is skipped because `job_type` came from the dropped entry, so
`grp_type_failure_rate` — the C1 feature — trains on short jobs only; in-flight is understated.
`test_mab_manager::test_pending_ttl_sweep` tests the sweep alone, never against the grace period.
**No LinUCB learning-curve number from a run with jobs longer than ~2 min is citable.**

### 23. `--delegation-policy llm` silently runs the bandit on resource-agent coordinators — **HIGH, arm mislabelled — FIXED 2026-10-06**

*Status:* refused at both layers — `run_test.py` before launch (`check_delegation_policy_is_honoured`, including a mixed or unknown coordinator tier) and a `ResourceAgent` coordinator at startup (`SUPPORTS_LLM_DELEGATION`, true only on `LlmAgent`). Leaves and `bandit` are unaffected.

`_init_delegation` exists only in `llm_agent.py:394`; `resource_agent.py:712` reads
`getattr(self, "delegation_policy", "bandit")`; `run_test.py:1408, 1934` record
`--delegation-policy` in `run_meta.json` and `collect.py` labels the arm from it. Level-1 agents
default to `resource` since 2026-09-18, so `run_test.py --delegation-policy llm` on a hierarchy
has the bandit (or random) choose every group, with no warning, while the run is recorded as the
LLM arm of E4. Only the per-row `policy` column in `decisions.csv` tells the truth. Same shape as
`designate_bidder` on a resource agent, documented 2026-09-20; needs the same treatment
(a startup refusal or a `run_meta` field that records what actually ran).

### 24–33. LLM plane, medium and low

- **24. [FIXED 2026-10-06 — parsed as float]** `llm.timeout_seconds` is truncated to int (`llm_config.py:32`): `0.5` → `0`, which means
  "disabled" in `llm_bidder.py:196` and `llm_delegator.py`; model calls on the selection and
  scheduling threads become unbounded, and pacing's bootstrap (`llm_agent.py:738`) becomes 0 so
  pacing does nothing.
- **25. [FIXED 2026-10-06 — an LLM agent refuses `enabled: false`, `use_for_selection: false` (both were ignored) and `provider: none` at startup, with a message naming `--agent-type resource`]** Three LLM keys do not do what they say. `llm.enabled` (default `False`) and
  `llm.use_for_selection` are parsed and never read (`enabled: false` still calls the model).
  `provider: none`, documented as the off switch (`config_swarm_multi.yml:492`, CLAUDE.md), makes
  `build_model` raise (`llm_bidder.py:85`) and the bidder is built unguarded (`llm_agent.py:97`),
  so the LlmAgent dies at startup.
- **26. [FIXED 2026-10-06 — answers go through `wire_cost_for_job` (§1), which requires the job pending and feasible; a reset drops the cached verdict (`LlmAgent._forget_decided`)]** The LLM verdict cache drops a guard the base class has. `native_cost_for_job`
  (`llm_agent.py:933-961`) is keyed by `job_id` only, 300 s TTL, never cleared on reset,
  `_forget_decided` or delegation failure; the base version answers only for jobs in
  `pending_queue` (`resource_agent.py:3793`). A coordinator barred by `delegation_failed_agents`
  still answers with its earlier low cost for 300 s > the 120 s delegation timeout — it votes for
  itself on a job it would refuse to propose.
- **27. [FIXED 2026-10-06 — a fallback verdict is cached after its pacing wait, as a real one is]** Uniform pacing holds real LLM verdicts from consensus but not fallbacks
  (`llm_agent.py:213-221` vs `:287-295`): on the success path `_pace_bid` runs before
  `_remember_cost`; on the fallback path the cost is cached first. Under Snow a peer querying
  during the hold gets a miss (an endorsement, §3) for a genuine bid and a cost for an analytic
  fallback — timing information in the one arm meant to remove it.
- **28. [FIXED 2026-10-06 by §19 — `_delegate_to_children` writes the parent record and child copies before touching local state]** A failed delegation write loses the job (`:3315` removes from `selected_queue` before
  the per-group save at `:3320` and `delegated_jobs.set` at `:3332`): a Redis error leaves the job
  untracked, the parent record READY under a live leader, the decision record already written.
- **29. [FIXED 2026-10-06 — shaped rewards are normalised over the outcome horizon (timeout + execution grace) when the agent supplies it]** Shaped reward turns successful long jobs into failures (`mab_manager.py:338` divides
  `time_since_delegation` — execution-inclusive since P0-9 — by `delegation_timeout_s`, a
  selection bound; `bandit.py:126` counts reward ≤ 0 as failure). Any successful job ≥ ~120 s
  scores 0. Default `shaped: false`.
- **30. [FIXED 2026-10-06 — refused; matched case-insensitively]** Unknown `mab.algorithm` becomes epsilon-greedy and reports the configured name**
  (`mab_manager.py:133-162`, `:441`) — including `"LinUCB"`, the spelling CLAUDE.md uses.
  `consensus.protocol` and `bid_pacing` raise on an unknown value; this key should too.
- **31. [FIXED 2026-10-06 — unwon jobs rotate to the back of the queue; the base loop's backlog-aware wait replaces the fixed 0.5 s sleep; `coordinator_cost_matrix: peers` is refused on an LLM coordinator rather than ignored]** `LlmAgent.selection_main` still lacks parts of the base loop beyond the DAG gate fixed
  2026-09-20: it scores `[self]` (`llm_agent.py:1276`) instead of `_selection_assignees()`
  (`resource_agent.py:2440`), so `coordinator_cost_matrix: peers` is a no-op on LLM coordinators;
  it never rotates unwon jobs to the back of the queue (head-of-line blocking with
  `designate_bidder` off); it always sleeps 0.5 s where the base skips the wait on a full backlog —
  LLM-vs-analytic throughput is confounded by loop cadence.
- **32. [FIXED 2026-10-06 — calls with unknown usage are counted (`usage_unknown_calls` → `llm_usage_unknown_calls`), marking the token totals as a lower bound; the `reasoning_time` schema field remains]** Token accounting misses failed calls (`LlmUsage.record(..., failed=True)` without usage
  at `llm_bidder.py:~248` and `llm_delegator.rank`): timeouts and pydantic-ai validation retries
  record 0 tokens, so the cost reported for P0-3/E4 is lowest in the runs with the most failures.
  `Bid` and `GroupRanking` also ask the model to fill `reasoning_time`.
- **33. [FIXED 2026-10-06 for `top_k` — refused below 1; wall-clock delegation timing still OPEN]** `mab.top_k: 0` silently drops jobs (`random.sample(..., 0)`, `select_top_k(..., 0)`,
  `ranked[:0]` → `[]`; monitor discards on `any([])`). Delegation timing uses `time.time()`
  throughout (`delegated_at` at `:3333`, the monitor, the pending TTL, the timeout decay).

---

## D. Measurement — the collector's numbers

### 34. Neither livelock criterion can fire — **HIGH, F1 undetectable — FIXED 2026-10-06**

*Status:* `reselection_multiplier` is gone; the same ratio is reported honestly as `tier_copies_per_job`. Churn comes from the agents' own counters: `jobs_restarted` (distinct jobs any agent reset) and `restarts_total`. `selection_*` still measures the last round — `sched_latency` spans every round and is the column a livelock criterion should use. **The plans' livelock rule must be rewritten against these columns**; that text is not in the repo.

`evaluation/collect.py:883` (`reselection_multiplier = len(raw)/n_unique`); `plotting/data.py`
(`all_jobs.csv` has one record per Redis key `job:<level>:<group>:<id>`, which a reselection
**overwrites**); `swarm/models/job.py:492-497` (`selection_started_at` re-stamped on every
PENDING→PRE_PREPARE); `resource_agent.py:1126` (`_restart_selection` resets to PENDING).

`reselection_multiplier` is always 1.0 on a flat run and always ≥ 2 on a hierarchical one (the
coordinator copy — `dedup_jobs`' own docstring says so). The plan's rule "multiplier ≥ 2 ⇒
livelock" is true for every hierarchical run and false for every flat one. `selection_*` measures
the **last** round only, so "p50 ≥ `reselection_timeout_s`" cannot fire on an assigned job, and
never-assigned jobs are dropped. "Restarts" exists only inside `metrics.json`; `sel_reproposals_l*`
is the only real churn signal on the row. `test_collect.py:145` passes by fabricating two records
for one job, which the export cannot produce. **Direction:** PBFT livelock undetected flat,
"detected" everywhere in a hierarchy.

### 35. Failed runs drop out of cell means — **HIGH, flatters the collapse cell — FIXED 2026-10-06**

*Status:* `aggregate()` emits `<metric>_n` beside every mean, and a run that finished nothing reports throughput 0.0 (measured) rather than NaN; makespan stays NaN (undefined) and its `_n` shows the survivors.

`collect.py:943-945` (`makespan_s`, `throughput_jobs_per_s` NaN on zero completions),
`:1006` (`proposers_per_job_l1` absent when `l1_jobs == 0`), `l1_selection_*` absent when
`level1_jobs.csv` is header-only; `aggregate()` at `:1036` does `dropna()` per metric and emits
`n_runs` for the whole group, never per-metric `n`. A Hier-270 PBFT cell in which 2 of 5 repeats
collapse reports throughput and coordinator-tier latency over the 3 survivors, labelled `n_runs=5`.
`proposers_per_job_l1` disappears in the very cell E5 exists to explain. This is the
"aggregate() averages the bad runs alone" corollary from the memory, in the other direction.

### 36. Fan-out ratios divide by assigned jobs, not offered jobs — **HIGH, inflated where it matters — FIXED 2026-10-06**

*Status:* new `jobs_offered` / `l<N>_jobs_offered` (assigned ∪ the pending files); `proposers_per_job_l*`, `proposals_per_job_l*` and `bidders_per_job` divide by them. Still open: `bidders_per_job` sums both tiers' LLM bids over one job count.

`collect.py:983-1010`; `plotting/data.py:300` (PENDING and leaderless jobs go to
`pending_*.csv`, so `n_unique`/`l{n}_jobs` exclude them). `proposers_per_job_l*`,
`proposals_per_job_l*` and `bidders_per_job` have numerators over every job proposed or bid on and
denominators over jobs that reached READY+. 27 coordinators proposing all 5400 jobs of which 200
finalize reads **729**, where the true value is 27. `bidders_per_job` also sums coordinator and
leaf LLM bids over one job count. `test_coordinator_matrix.py:237-290` assumes every proposed job
is assigned.

### 37. There is no consensus-messages-per-job column, and `msgs_sent` is not consensus traffic — **HIGH, F2 — FIXED 2026-10-06**

*Status:* `msgs_consensus_sent`, `msg_bytes_consensus_sent` (Proposal/Prepare/Commit and the four Snow types, from `sent_by_type`) and `consensus_msgs_per_job` over offered jobs. `msgs_sent` keeps its meaning (all traffic).

`swarm/comm/grpc_transport.py:112-138` (`_send_raw` is the one funnel and counts every type:
SwimPing/Ack/PingReq, GossipState, JobStatus, Snow batches); `collect.py:593-598` emits fleet
totals only and ignores `sent_by_type`. With the shipped `swim` + `gossip.enabled`, background
traffic scales with run duration × fleet size, not with job count, so the longest (livelocking)
cells carry the most background messages and F2's log-log slope is distorted. The by-type data is
in `metrics.json`; it is simply not surfaced.

### 38. `consensus_finalized` and `finalize_s_*` mean different things under PBFT and Snow — **MEDIUM — FIXED 2026-10-06**

*Status:* both engines report `won`; Snow's rounds/queries/time distributions now describe won decisions only (PBFT's always did). Collector: `consensus_won`; the over-agents median distribution is renamed `finalize_s_agent_median_*` / `rounds_agent_median_*`, and `finalize_s_p95_worst_agent` is the nearest honest tail. Durations still on `time.time()`.

PBFT counts once per job at the winning proposer (`engine.py:115-123`); Snow creates a
`_SnowState` per proposing agent (`gossip_engine.py:250`) and counts every instance that converges
(`:630-636`) — losing bidders via the participant path, single-node self-claims at 0 rounds
(`:423-427`), peer-decided shortcuts. Snow's fleet sum ≈ proposers/job × jobs; PBFT's ≈ jobs; the
Snow `rounds_`/`queries_`/`finalize_s_` distributions are pulled low. `collect.py:542-547, 607`
build `finalize_s_p95`/`p99` from per-agent **p50s** — percentiles of medians carrying the names
of latency tails. Both engines time with `time.time()` (`engine.py:123, 213`; Snow's `time_fn`
defaults to `time.time` at `:132` and the agent does not override it); Snow round deadlines run on
that clock too.

### 39. Jain's fairness ignores idle agents — **MEDIUM-HIGH, flattering — FIXED 2026-10-06**

*Status:* over the executing fleet — level-0 agents from `all_agents.csv` plus every agent that led a completed job, idle ones as zeros; `fairness_basis` says which source supplied the set and `fairness_jain_active` keeps the old number. `comparison.py` (E7) fixed the same way (§42).

`collect.py:948-951` and `comparison.py:53` take `value_counts()` of `leader_id` over completed
jobs; agents that ran nothing are absent. 30 agents with 10 sharing all the work evenly → 1.0; the
true index over the fleet is 0.33. The plan defines fairness "over per-agent load".

### 40–46. Collector and plotting, medium and low

- **40. [FIXED 2026-10-06 — the row carries `regret_decisions_unscored`, `regret_scored_share` and the with/without-a-choice counts; `regret_mean` is over decisions with a choice (the old mean is `regret_mean_all_scored`); the context-age correlation excludes no-choice rows. `--validate` is still not run by the collector]** The regret row hides what was refused. `regret_metrics` (`collect.py:797-803`) copies
  `regret_total`, `regret_mean`, `routing_accuracy` and drops `decisions_unscored`,
  `decisions_with_a_choice` and any `--validate` result. In E2b a SIGKILLed member has no
  `failure_sim_start` and a dead group's expired keys are absent from `all_agents.csv`; either way
  every decision with that group as a candidate is unscored (`oracle.py:188-190, 253`) and
  `regret_total` covers only the easy decisions with nothing on the row saying so. `regret_mean`
  (`oracle.py:329`) divides by all scored decisions including the zero-regret no-choice ones.
- **41. [FIXED 2026-10-06 in collect.py — first submission of any offered job; `comparison.py` already did]** Makespan starts at the earliest *completed* job's submission (`collect.py:936`,
  `submitted[complete_rows].min()`): if early jobs livelock and later ones finish, makespan
  shrinks and throughput rises. `comparison.py:46` uses the minimum over all jobs, so SWARM and the
  E7 baselines are measured differently.
- **42. [FIXED 2026-10-06 — `load_jobs_csv` dedups with the collector's `dedup_jobs`; fairness counts idle level-0 agents when the run has `all_agents.csv` (baselines fall back to leaders)]** `comparison.py` reads hierarchical `all_jobs.csv` without dedup. Coordinator-tier
  copies, whose `scheduling_latency` covers the coordinator tier only, enter SWARM's latency CDF
  and boxplot; `jobs_total` is doubled; `leader_counts` includes coordinators. SWARM latency
  biased low against the baselines.
- **43. [FIXED 2026-10-06 — `safe_mean/median/quantile` return NaN on no data; completion is `completed_at > 0`, success also requires exit 0; runs load deduplicated. The guessed coordinator ids remain]** `multi_run.py` reports failures as fast and successful. `safe_mean`/`safe_median`/
  `safe_quantile` (`plotting/stats.py:21-50`) return 0.0 when every value is NaN, so a livelocked
  run reports 0 s selection time; `completed_jobs`/`success_rate` (`multi_run.py:222-224`) test
  `exit_status == 0` but `save_jobs` writes None as 0 (`data.py:308, 322`), so READY and RUNNING
  jobs count as successes; no hierarchical dedup; coordinators guessed as the top 10 % of ids.
- **44. [PARTLY FIXED 2026-10-06 — the shortfall file carries `accounted`, and a kill run whose silent agents are exactly the declared ones reads complete; a run with no metrics.json still reads True]** `metrics_complete` is three-state data stored as a boolean. `collect.py:864` computes
  `not shortfall.exists()`, so a run with **no** `metrics.json` reads True (covers a plotting
  failure under `run_blocking(check=False)` and every pre-gate run); `run_test.py:1610` writes the
  shortfall file even when every silent agent was declared, so every correctly measured E2b/E6
  kill run reads False; `unexpectedly_silent` is never read.
- **45. [FIXED 2026-10-06 — `jobs_retired_infeasible` (from the agents' `infeasible_retired`, minus jobs already exported), `jobs_failed_total`, and both `*_of_seen` denominators include retired jobs. `ctx_skew_max_s` None-when-clean remains]** Infeasible-retired FAILED jobs are invisible (`resource_agent.py:4073` → `data.py:300`
  routes them to `pending_jobs.csv`), so `exit_failures`, `jobs_seen` and both `*_of_seen`
  percentages exclude them. `ctx_skew_max_s` is None on a measured-clean run
  (`instrumentation.py:374` → `collect.py:763-764`), so `aggregate()` averages skew over the
  skewed runs alone — the exact corollary bug the memory note records; `test_collect.py:903` covers
  the unmeasured case only.
- **46. [PARTLY FIXED 2026-10-06 — a decision within `PHASE_BOUNDARY_MARGIN_S` (2 s) of a phase boundary is unscoreable, since its phase cannot be resolved across hosts; the collector runs the profile validation and reports `regret_profile_validated` / `_mae` / `regret_types_beyond_noise`. validate()'s last-row-wins join and the plotting Redis readers remain]** Oracle and plotting details. Phase elapsed is the coordinator's wall-clock `ts` minus
  the member's wall-clock `failure_sim_start` (`oracle.py:191`) — cross-host, wrong by the offset
  near phase boundaries. `validate()` builds outcomes last-row-wins across tier and fan-out copies
  (`:358-368`) while the predicted rate is the min over selected groups (`:283`); mismatch under
  `top_k > 1`; `collect.py` never runs validation. `load_jobs_from_redis` (`data.py:167-168`, used
  by `mab.py`) scans groups 0–9 only — Hier-270 has 27 level-0 groups; `mab.py`'s
  `--metrics-run-id` defaults to None; its `--from-csv` reads `jobs.csv` not `all_jobs.csv`;
  `engine.conflicts` is capped at 4096 so `single_run.py`'s conflict totals undercount long runs.

---

## E. Run validity — ways a run exits 0 for a fleet or workload it did not measure

### 47. `run_test.py --debug` publishes zero jobs and exits 0 — **HIGH — FIXED 2026-10-06**

*Status:* `--debug` is no longer forwarded (the distributor has no logging at all); the producer's exit status is recorded and a non-zero one ends the wait and the run (exit 4). A test checks every flag `produce_jobs` can pass exists in `job_distributor.py`, which is the defect class.

`run_test.py:1186-1189` appends `--debug` to the `job_distributor.py` command;
`job_distributor.py:173-184` has no such argument. argparse exits 2 inside a `check=False` daemon
thread. Nothing is in Redis; the drain loop sees bucket 0 (below threshold), waits the 90 s
stability window, stops the agents; every agent flushes, the completeness gate passes, exit 0.
Both usage examples in `run_test.py`'s own docstring pass `--debug`.

### 48. The drain condition reads a Redis failure as "drained" and ignores in-flight jobs — **HIGH — FIXED 2026-10-06**

*Status:* `parse_bucket_set_count` returns None on `dump_db.py`'s connection-error text or a traceback; too many misses end the wait as `redis_unreadable` and the run exits 4. After PENDING drains the runner waits for READY/RUNNING (`--inflight-drain-max-s`, 600 s). `drain.json` records how the wait ended and what was in flight. Not yet read by `collect.py`.

`run_test.py:90-107` (`parse_bucket_set_count` always returns an int, so the `max_misses` branch
at `:1273-1298` is dead), and it watches bucket 1 (PENDING) only. If `dump_db.py` cannot connect
the count reads 0 → "LOW" → "Condition stable → proceed": a wrong `--db-host` or a firewall ends
the run after 90 s. Separately, the last wave still RUNNING when PENDING drops below the threshold
is cut off ~120 s later (90 s stable + 30 s grace, 20 s drain) — simulated jobs run to 120 s and
real execution has no cap — and nothing flags the truncation.

### 49. Local `--dynamic-agents` kills the whole initial fleet, and the gate passes — **HIGH — FIXED 2026-10-06**

*Status:* dynamic launches pass `--add`; the starter skips `pkill` and `rm -f shutdown` under it. Remote dynamic launches pass it too — every host but the first gets a non-zero `--start-offset` on the initial launch, so the phase decides, not the offset.

`run_test.py:570` → `swarm-multi-start.sh:89`: `pkill -f "python3\.11 .*main\.py" || true`,
unconditionally. In local mode every dynamic launch is "on an occupied host", so every documented
`--mode local --dynamic-agents` example SIGTERMs agents 1..N at the trigger; they flush metrics
stamped with this run_id and exit; the run continues with the dynamic agents only and reports
"Metrics complete". CLAUDE.md documents this as a remote wrap-around limitation; locally it is
every time.

### 50. The pre-flush reap covers only the current hosts file — **HIGH, ladder steps — FIXED 2026-10-06**

*Status:* after the flush the runner deletes every `agent:*` record, waits 5 s, and refuses to launch if any reappears, naming the hosts. Deleting first makes it a liveness test — a crashed agent's record inside its TTL is not mistaken for a live one.

`run_test.py:1907`, `:1476-1499`; `stop_agents_v2.sh:111-115`; `resource_agent.py:1570-1576`
(agents take whatever agent records Redis holds at their (level, group) as neighbours — they do not
filter by configured `peer_agents`). Stepping a ladder from 90 to 30 hosts, or a previous run that
crashed or got an unconfirmed stop: agents on hosts outside the new file survive the flush,
re-register at level 0 / group 0, are counted in quorum, sampled by Snow, and claim and execute
jobs from the new run. Their metrics carry the old run_id, so they surface only as a "NOTE: foreign
payloads" line and the gate passes.

### 51. `--use-config-dir` never checks a config's advertised address against where the agent is started — **HIGH — FIXED 2026-10-06**

*Status:* `check_launch_matches_configs` compares each launched config's `grpc.host` with its placement (resolving names) and refuses wildcards in remote mode. **It found a live defect on its first run:** remote dynamic agents were placed from the top of the host list while their configs numbered on, so every one advertised a host it was not on. The launcher now uses the generator's block arithmetic for both phases.

`run_test.py:484-555` places agent *i* on `host_list[(i−1)//agents_per_host]` from the *current*
hosts file; `grpc.host`/`port` were fixed at generation (`generate_configs.py:1056-1067`). Nothing
compares them. A hosts file regenerated after a node went down shifts the interleave; a different
`--agents-per-host` than at generation gives port clashes; and `generate_configs` without
`--agent-hosts-file` writes the base config's `0.0.0.0` with no warning (the standalone example in
CLAUDE.md does exactly this) — the bind-vs-advertised defect of the memory note, back through a
different door: consensus silently runs single-node or to the wrong peers.

### 52–60. Tooling, medium and low

- **52. [FIXED 2026-10-06 — a local run with `--config-dir` other than `./configs` is refused]** Local mode ignores `--config-dir` (`run_test.py:337` generates into it;
  `swarm-multi-start.sh:97` and `main.py:72` read `./configs` literally; `run_meta` and the
  fleet-fit checks also read `./configs`, so everything is internally consistent about the wrong
  fleet).
- **53. [FIXED 2026-10-06 — any declared agent that reported fails the run (`kills_not_taken` in the shortfall file), including the all-reported case that only warned]** `--expect-silent-agents` passes when a subset of the kills took (`run_test.py:1588-1621`:
  "accounted" means missing ⊆ declared; the "did not take effect" warning fires only when nothing
  is missing). `kill_agents.py --agent-ids` warns about ids it cannot find and kills the rest with
  exit 0; its remote ssh has no BatchMode or host-key options and a 10 s timeout, so a host that
  times out drops its agents from the kill list. Declare 3,7; only 3 dies; the run passes as a
  2-failure cell. `TestDeclaredSilentAgents` covers "none took" and "wrong agents", not "a subset".
- **[FIXED 2026-10-06 — a synthetic `jobs/` whose record count differs from `--jobs` is refused under `--use-config-dir`] 54. Under `--use-config-dir`, `jobs/` is neither cleaned, regenerated nor checked against
  `--jobs`** (`run_test.py:410-431`: `rm -rf jobs` is inside `if not args.use_config_dir`;
  `generate_configs.py:1384` reuses an existing `jobs/` silently). A previous run's 600 jobs are
  published into a run declaring 200; an empty `jobs/` leads straight into §47.
- **[FIXED 2026-10-06 — the job generator has its own RNG derived from `--seed`; coordinators are never job targets; `--job-target-agents K` (generate_configs, run_test, job_generator) models jobs on agents 1..K only, so with `--master-fleet-size` and K = the smallest rung's LEAF count every rung gets the identical workload; a K that reaches a coordinator on the rung being generated is refused, since its targets would differ from a rung where that id is a leaf (stop-time review)] 55. The synthetic workload differs from rung to rung even with `--seed` and
  `--master-fleet-size`** (`generate_configs.py:1384-1388` runs `JobGenerator` on the global RNG
  after the per-agent draws; `job_generator.py:181-186` samples targets from the current fleet).
  `--master-fleet-size` fixes the flavours, not the jobs. Hierarchical coordinators, which never
  execute, are valid job targets, so a job sized to a coordinator's unique top flavour is
  infeasible for every leaf.
- **[FIXED 2026-10-06 — `--job-interval` forwarded (default 1.0, the cadence runs actually had); `batch_tests_v2.py` exits 1 when any run fails and forwards the Pegasus/quantum/textfile/job-target flags; `--generate-plots`/`--log-dir` say they have no effect and warn; the converter defaults are per-file / job like run_test. `--pegasus-dag-gating` still defaults off] 56. Dead and disagreeing flags.** `--job-interval` (default 0.5) is never forwarded (the
  distributor uses its own 1.0; `batch_tests_v2` forwards the dead flag); `--generate-plots` and
  `--log-dir` are dead; `--pegasus-data-nodes` defaults to per-file in `run_test.py:1820` but
  per-site in the converter CLI; `dtn_scope` is `job` in `run_test.py:261` but `file` in the
  converter; `--pegasus-dag-gating` defaults off though its help says "Required for any real
  workflow"; `batch_tests_v2.py` cannot forward `--pegasus-dag-gating`, `-data-nodes`,
  `-dtn-names`, `-jobs-dir`, `-bundle-source-root`, `--textfile-dir`, `--quantum-*` or
  `--split-hybrid`, and **exits 0 when every run exits 3** (`:345-358`).
- **57. [FIXED 2026-10-06 — arguments are `None` (unknown) when nothing declared or recorded them (`resolve_arguments`); a dangling container name is an imageless container (refused), and an unparseable catalog is reported (`catalog_error_db`) and refuses execution in the converter; the catalog entry is the one for the site the job ran on (`catalog_entry_for`); unknown sizes stay absent and an unknown exit code is `None` and warned. Corpora converted before this keep the old values until re-extracted]** Extractor fail-opens, three ways. With no `workflow.yml` (or no PyYAML)
  `load_workflow_uses` returns `{}` and `argv_list = shlex.split("")` is `[]` — *empty*, which is
  runnable — not `None`, which is unknown (`pegasus_profile_extractor.py:100-104`, `:506-552`); the
  None/[] rule the converter enforces is defeated upstream, and the job runs with no arguments. A
  malformed transformation catalog returns `{}, {}` under a bare `except`, and a transformation
  site naming a container absent from `containers:` gives `None` (`:213-246`, `:512`;
  converter `:402`) — either way `container_db` is None, the imageless-container refusal never
  fires, and the job runs on the host. The catalog site is chosen as "first non-local", not the
  site the job ran on (`job_instance.site`). Unknown sizes become `0` (`:412-417`,
  `lfn_sizes.get(lfn, 0)`) and an unknown exit code becomes `0` (`:600`), so the converter's
  absent-is-not-zero rule receives an explicit zero and keeps it. `test_a_transformation_with_no_
  container_has_none` and `test_a_malformed_catalog_does_not_abort_the_run` **pin** the fail-open.
- **[FIXED 2026-10-06 — the distributor publishes in job-number order; the converter reads Redis in key order and refuses duplicate job ids] 58. Job order and identity are not deterministic.** `job_distributor.py:78-89` publishes in
  `st_ctime` order (after `rsync`, copy order — lexicographic `job_1`, `job_10`, …); the converter
  iterates Redis `scan_iter` unsorted (`:62-69`), so `job_{i}` numbering changes per conversion;
  `run_name` is `dax_label + basename(run_dir)` (`extractor:403`), so two submit trees ending in
  `run0001` produce identical job ids and Redis keeps one while `total_jobs_written` says N.
- **59. [FIXED 2026-10-06 — `fix_slice_clocks.sh` defaults to every agent-N in /etc/hosts; `setup_nfs_workflow.sh` proves a write round-trips from EVERY host in both `--check` and setup, and stages images by sha256, failing the step on any host]** Slice setup scripts can report success over a partial fleet. `fix_slice_clocks.sh:33-39`
  defaults to the repo-root `agent_hosts.txt` with no `^agent-N$` filter, which `run_test.py:343-347`
  overwrites with the last run's subset (or `localhost` in local mode) — a clock repair covers 30
  nodes or the database node and prints "All nodes synchronised". `setup_nfs_workflow.sh --check`
  relies on `findmnt` alone (which CLAUDE.md itself says is not a valid check); the setup write
  probe tests `HOSTS[0]` only; `--stage-image` failures `continue`/WARN and exit 0, skipping a host
  whenever the size matches, without a checksum.
- **60. Agent start is not observable.** The remote start command ends `a && b && … &`, so the
  whole AND-list is backgrounded and `ssh_check` cannot fail; a host whose profile, `cd` or
  starter fails is discovered at the metrics gate after the full runtime. The local start passes
  `>`, the log file, `2>&1` and `&` as **literal arguments** through `run_blocking`'s list form
  (`run_test.py:472`; `shell=False` at `:76`), so `local_agents_*_start.log` is never written. **[FIXED 2026-10-06 — written through `run_blocking(log_file=…)`]**
  `cleanup.py` exits 0 on a Redis connection error (run under `check=False`), so a failed flush is
  invisible; nothing asserts the keyspace is empty before launch.

---

## F. Real execution and staging

### 61. A refusal is permanent and indistinguishable from a workflow failure — **HIGH, every real-workflow run — FIXED 2026-10-06 (retry added the same day)**

*Status:* half fixed. A refusal's reason is now recorded on the job (`Job.refusal_reason`, through Redis), exported as a `refused` column, and counted by `collect.py` as `exec_refusals` beside `exit_failures` (absent for older exports). **Retry (added 2026-10-06, split agreed with the user):** the runner tags a refusal `transient` (`TransientRefusal`) when an input's producer or staging site could not be reached, a fetch passed its deadline, or a registry lookup failed; everything else — unrunnable spec, no work dir or runtime, missing executable or root input, a produced name with no location — is a configuration refusal and fails at once. A transient refusal returns the job to PENDING with its claim released (`_retry_refused`), up to `runtime.execution.refusal_retries` (3; the count rides on the job record so the cap holds across agents). A refused start is not counted in `executed_jobs`, and `exec_refusal_retries` is reported beside `jobs_executed_twice`.

`swarm/models/job.py:575-589`, `:620-623`; `resource_agent.py:3458-3460`.

A refused job (fetch failed, producer unreachable, store down, fetch deadline exceeded, image
pull failure) returns `exit_status = 1`, is persisted COMPLETE, and nothing re-runs it.
`ExecutionResult.refused`, `.reason`, `.duration_s` and `.command` are logged and never stored —
not on the Job, not in Redis, not in metrics. `STAGING_DESIGN.md` §5 says "refusing is loud and
retryable"; nothing retries. A momentary UNAVAILABLE from a producer, with the store briefly
unreachable, kills the child; every descendant stays gated for the run; `collect.py` counts it as
a real workflow failure. `test_a_refusal_is_not_a_silent_fallback_to_simulation` checks the exit
status only.

### 62. A timeout or agent shutdown does not stop a docker container — **HIGH, orphans across runs — FIXED 2026-10-06**

*Status:* `docker run` gets a unique `--name` and `--init`; a timeout `docker kill`s the container as well as the client's group. Running jobs are registered, and when the agent's drain times out `runner.terminate_all()` kills every one still running, containers included.

`swarm/execution/runner.py:416-421` (docker command: no `--init`, `--name` or `--cidfile`),
`:708-715`, `:737-758` (`_kill_group` SIGKILLs the process group of the `docker run` **client**).
The container belongs to dockerd and keeps running; `--rm` removes it only when it exits on its
own. A docker job that hits `timeout_s` is recorded 124 while its container keeps burning CPU and
writing into the run's work dir; orphans from run N compete with run N+1's measured jobs on the
same VM. Separately, `start_new_session=True` means no job dies with the agent; after the 20 s
`_drain_executor`, `os._exit` leaves every bare and apptainer job running too, and
`stop_agents_v2.sh` pkills `main.py` only. The timeout tests use a bare script, not a container.

### 63. The stale-input guard checks the location hash, not the readiness set — **HIGH, "the guard asked the wrong container" — FIXED 2026-10-06**

*Status:* the staging context carries `produced` (`Repository.produced_names`, one `SMISMEMBER` on the readiness set); a produced name with no location is refused before the inputs root is consulted, and a failed lookup refuses too. Staging off is unchanged.

`runner.py:567-596`; `repository.py:399-410`. `stage_inputs` treats "produced by this run" as
"has an entry in `data_loc`". A name present in `data_ready` with no location falls through to
`roots.inputs` — the stale-collision read the source order exists to prevent. The
`data_locations` docstring says "the caller distinguishes those two cases"; the caller never
consults readiness. Two real producers of such names: `_retry_unpublished_data`, which writes
readiness with no location, and a producer in a mixed fleet with staging off (the code's own
comment cites "staging code was on 5 of 92 agents"). `test_staging.py:648` asserts the repository
returns `{}` and says the caller must tell the cases apart; no test checks that `stage_inputs` does.

### 64–73. Staging and execution, medium and low

- **64. [FIXED 2026-10-06 — one rule, `staging.plain_name`: under staging a name with a directory component gets no location from the producer and is refused by the consumer (no silent fall through to the inputs root). Supporting such names is open]** LFNs containing `/` break the registry and the paths. The producer writes `data_ready`
  and `data_loc` under the raw `d.file` (`resource_agent.py:3458, 3499, 3508, 3547`); the consumer
  looks up by `basename` (`runner.py:534-536, 559, 567`), misses, and falls to `roots.inputs` — a
  stale read if the basename exists there, else a refusal. The producer also checks and serves
  `work/out.csv` while the job may have written `work/runA/out.csv`. The converter emits raw LFNs
  (`:225, 257`) and itself acknowledges slash-bearing names (`:481-485`).
- **65. [FIXED 2026-10-06 — the store REFUSES a different body under an existing (run, name); every location carries the producer's sha256 and `fetch_any` rejects a served copy that does not match it, trying the next. `_retry_unpersisted_completions` re-saving a stale payload is still open]** One name can be served as up to three different copies and every check passes. The
  store keeps the first copy per `(run, name)` but acks `ok=True` for a *different* body, logging
  ERROR only (`staging.py:362-388`); `HSET` on the location is last-writer-wins
  (`repository.py:128-132`); the registry carries no content digest, so the sha256 proves the
  stream was not corrupted, not that it is the producer's file; `_retry_unpersisted_completions`
  re-saves a stale payload without comparing state. A pushes `x`, dies before `_persist_completion`;
  the job is reassigned to B, a non-deterministic producer pushes `x'`; the store keeps A's `x` from
  an attempt never recorded complete and returns ok; B registers `[B, store]`; consumers read `x'`
  until B dies, then `x`, and both verify. `test_a_within_run_collision_is_reported_not_silently_
  discarded` **pins** the `ok` ack. The memory note records the non-deterministic re-run case as
  unguarded; this is its full shape.
- **66. [FIXED 2026-10-06 — a failed push WITHHOLDS the name (the completion is written without it); `_retry_stage_out` re-pushes each tick and publishes name and location in one MULTI (`Repository.publish_data`). The push deadline is `store_timeout_s + size / store_min_rate_bps`. The push is still synchronous on the completion path]** A failed stage-out still publishes the name (`resource_agent.py:3523-3547`), with a
  peer-only location. CLAUDE.md and `config_swarm_multi.yml:166` say "a name is never visible
  without a durable copy"; `STAGING_DESIGN.md` §6 documents the opposite, and
  `test_a_failed_stage_out_loses_durability_not_the_run` pins it. `store_timeout_s = 120` is a
  deadline for the whole stream, so any output taking > 120 s to upload *always* loses durability —
  exactly the large intermediates — and the synchronous wait adds to latency and makespan.
- **67. [PARTLY FIXED 2026-10-06 — declared outputs are removed from the work dir before a real job runs; an input already in the work dir still wins over the registry]** A file already in the work dir wins over the registry, and outputs are never cleared
  before a run (`runner.py:562-564`; `resource_agent.py:3499-3508`). A job that exits 0 without
  rewriting an output gets the old file published; a false-failed agent's copy of a reassigned
  producer still writes into the shared dir; a container orphaned by §62 writes after its timeout.
  `test_a_file_already_in_the_work_dir_is_never_fetched` pins the first half.
- **68. Two keys define the execution budget.** `delegation_exec_grace_s` defaults to
  `Job._WALL_TIME_MAX_S` (120 s; `resource_agent.py:602-617`); a real job is bounded by
  `runtime.execution.timeout_s` (3600 s) and never by `wall_time_max_s`. A coordinator drops any
  real job longer than `delegation_timeout_s + 120 s` with no bandit outcome — the bandit never
  sees long jobs.
- **69. [FIXED 2026-10-06 — `--containall` by default (no $HOME or host /tmp bind, clean environment, as docker); staged code bound `:ro`. Escape hatch `runtime.execution.apptainer_containall: false`]** The apptainer invocation is not isolated (`runner.py:455-458`): no `--containall`,
  `--no-home` or `--cleanenv`, so `$HOME` and `/tmp` are bound — and if agents run as root (the
  remote launch suggests so; unconfirmed) `/root/.ssh` and the root-mesh key are readable by
  workflow code, which makes `job_environment` scrubbing moot. The staged-code bind is read-write
  under apptainer but `:ro` under docker, so a job can modify the code bundle every agent runs.
- **70. [FIXED 2026-10-06 — shared token in `SWARM_STAGING_TOKEN`, sent as gRPC metadata and checked with `hmac.compare_digest` on Fetch and Put; `staging_site.py --token`; `run_test.py` forwards it to remote agents over ssh stdin, so it never appears in the run log or `ps`. Unset means open, with a startup warning on agents and the site. Transport is still plaintext gRPC and the site has no quota]** The data service has no authentication (`staging.py:288-391, 431, 498, 609`;
  `staging_site.py:32`). `Put` accepts any `(run, name)` from any host; combined with first-wins, a
  pre-seeded or rogue upload becomes the durable copy and the real producer's push is discarded.
  The site binds `0.0.0.0` and serves every run to anyone with a guessable run id. No quota.
- **71. [FIXED 2026-10-06 — a verifying receiver refuses a stream with no digest; a fetch or upload that ends without its final chunk is refused, never linked in]** Verification is optional at the far end. A receiver with `verify=True` accepts a stream
  carrying no digest (`staging.py:517-525`); a store started `--no-verify` serves and accepts
  unverified data; `Put` has no size field and does not require the `last` chunk (`:307-355`), so a
  stream that ends cleanly without it is linked in as the stored file.
- **72. [FIXED 2026-10-06 for docker — the image is inspected and pulled BEFORE the job's clock (`pull_s`), a failed pull is a transient refusal; `auto` stays in the declared runtime family unless `allow_runtime_substitution: true`. Apptainer's `docker://` conversion on first exec is still inside the job's time]** Images are fetched inside the timed job and the runtime is substituted silently. Docker
  pulls a missing image during `docker run`, apptainer converts `docker://` on first exec — both
  inside `duration_s`; a pull failure (125/255) is a job failure, not a refusal. Under `auto` a
  docker-kind container on a docker-less host silently runs under apptainer; the comment says the
  other runtime is accepted "only if the image reference is plausibly for it" and the code accepts
  any runtime in `KNOWN_RUNTIMES` (`runner.py:328-331, 421, 453`).
- **73. [FIXED 2026-10-06 — publish errors no longer skip the completion write; an empty work_dir publishes nothing; the env denylist adds PASS, AUTH, SSH_AUTH_SOCK and URL-embedded credentials; job logs are per attempt and hash-suffixed; a wildcard grpc.host with staging on is refused at startup. `_publish_produced`'s idle retry tick remains]** Smaller. `_publish_locations` (`resource_agent.py:3460`) sits outside any `try`, so an
  exception there skips `_persist_completion` entirely and the job stays RUNNING with no retry
  queue; with staging on and `work_dir` empty (simulate mode) outputs resolve against the agent's
  cwd and a same-named file there is served. The env denylist (`runner.py:69-90`) misses
  `SSH_AUTH_SOCK`, `*_PASS`, `*_AUTH` and URL-embedded credentials; docker passes *none* of the host
  environment while apptainer passes all of it. `.swarm-logs/<job>.out` is opened `wb`, so a
  re-execution truncates the previous attempt's logs; the id sanitisation maps `a/b` and `a_b` to
  one file. A wildcard `grpc.host` is logged, not refused (`agent_grpc.py:209-216`), so every
  published location is undialable. `_publish_produced` has no callers but its retry tick still
  writes readiness with no location, feeding §63.

---

## G. Config drift — one key, two defaults

| Key | Code default | Shipped `config_swarm_multi.yml` | Documented |
|---|---|---|---|
| `runtime.aggressive_failure_detection` (`resource_agent.py:924`) | `False` | `true` (`:202`) | not in CLAUDE.md |
| `runtime.reselection_timeout_s` (`:577`) | 60 | 300 (`:57`) | CLAUDE.md says 60; `delegation_timeout_s` derives from it, so 600 s shipped |
| `consensus.snow.{k, beta, round_timeout_ms, send_workers, max_inflight}` | 20 / 20 / 500 / 32 / 32 | 10 / 6 / 300 / 16 / 16 | documented, but a config without the block gets 20/20 silently |
| SWIM probe / suspect timeout | agent 1 s / 20 s; `SwimMembership` ctor 0.3 s / 8 s | suspect 60 s | — |
| `llm.timeout_seconds` | `int()` of value | 6 | fractional values become 0 = disabled (§24) |
| `mab.pending_ttl_s` | `2 × delegation_timeout_s` | — | should be ≥ `delegation_timeout_s + delegation_exec_grace_s` (§22) |
| UCB1 `exploration_weight` | √2 in ctor | 1.41 in manager and config | — |
| `--pegasus-data-nodes` | per-file (`run_test.py`) | per-site (converter CLI) | CLAUDE.md says per-site is the default and is wrong |
| `dtn_scope` | `job` (`run_test.py:261`) | `file` (converter CLI) | — |

Also: `GossipConsensusEngine.alpha_k` (`:140`) is never used; `_reset_orphaned_jobs` is dead
(`:2641`); the `_init_decision_state` comment says monotonic and `_note_decided` stores
`time.time()`; `_absorb_response` calls `_finalize` while holding `_lock` and when `_send_pool`
is None (after `stop()`) the Redis CAS and host callbacks run inside it; claim keys `assignee:`
and `reassign:` are not run-scoped (`repository.py:354, 457, 489`) while `data_ready` is; the
agent-key TTL is not floored at the failure detector's threshold (`resource_agent.py:1806-1810`;
shipped 300/60 is safe, nothing enforces it); the out-of-order message stash
(`:136-156`, `:1034-1049`) is drained only when the job id appears in the PENDING scan, so entries
for jobs that never become PENDING at this level/group (child records deleted by
`_reassign_delegated_job`) live forever and after 2048 objects every later out-of-order vote is
dropped; gossip entries evicted at one agent are re-accepted from a peer's younger copy with a
fresh TTL (`disseminator.py:144-155, 172-182`); the periodic tick SMEMBERS the whole COMPLETE index
and does three container operations per id (`:1909-1919`, `:1108-1116`), O(completed) per agent
per 0.5 s on the thread that also does READY removal and failure detection; advertised load
counts `ready_queue` only (`:625`) and `compute_proposed_load` excludes selected jobs
(`:2162-2170`), so a deep selected-but-unscheduled backlog looks like "merely full"; the selection
cache's job signature ignores `delegation_failed_agents` (harmless only while the 60 s cache TTL is
shorter than the delegation timeout).

---

## H. What is missing

**The one thing that would make most of §B measurable: a double-execution evidence metric. — BUILT 2026-10-06**: agents export `executed_jobs` (one entry per start of `execute_job`); `collect.py` reports `jobs_executed`, `jobs_executed_twice` and `executions_extra` — absent for payloads that predate it, 0 when measured clean, a lower bound when `metrics_complete` is false.
Items 4, 13, 15, 16 and 20 all produce a job that executes on two agents, and nothing in a run
reports it. Per-agent executed job ids in `metrics.json`, joined in `collect.py` into
`jobs_executed_twice`, turns five argued defects into a measured column — and gives the PBFT
double-assignment column, which today reads "not measured", a value.

**Metrics the plans name that do not exist** (beyond transfer accounting and the RTT matrix,
already known): consensus-only messages and bytes per job (data is in `metrics.json` by type);
a livelock column that works (§34); restarts and conflicts on the wide row; per-metric `n` in
`aggregate()`; context error (T-5 — `DecisionRecord` has no per-candidate inflight/headroom and
there is no join); F5 time-to-re-adoption and failure-detection latency; DTN/data-locality hit
rate; CPU-seconds; a per-run clock-sync validity column (`job_latency`, `sched_latency` and
makespan are all cross-host differences); the oracle validation verdict on the wide row.

**For an apples-to-apples Pegasus comparison**: a per-job execution record (command, refused
flag and reason, real `duration_s`, stdout/stderr paths, timeout/signal status — a negative
return code from a signal kill is stored as a bare int); timing comparable to kickstart
(`mark_started` precedes stage-in and image pull, `mark_completed` precedes stage-out; kickstart
times the executable alone); output size and digest at production time; retry semantics for
failures and refusals; final-output collection to a destination; cpu/memory limits on jobs
matching the scheduler's allocated capacities, and `--user`; a retention policy for work dirs,
`.swarm-logs` and the store's per-run directories.

**`run_meta.json` lacks**: the git SHA (controller, and the file-pushed code on the agents —
their git log lies); a hash of the base and per-agent configs; the hosts and sites files in launch
order; seed and master fleet size as fields (argv only today); a count and hash of the jobs dir or
bundle manifest; the kill log and kill seed (`kill_agents --random` is unseeded and not linked to
`--expect-silent-agents`).

**Tests that do not exist and would have caught the HIGH items**: several PBFT engines running
together over a bus (§5, §6); Snow with real `ResourceAgent` cost functions and a DTN-bearing job
(§1, §2, §7); `on_peer_status` with the shipped `aggressive_failure_detection: true` (§4);
coordinator-tier failed-agent reassignment (§13); children forgetting a job the parent pulled back
(§15); delegated-job reassignment under Snow (§14); persistence failure in `select_job`,
`schedule_job` and the delegation save (§19, §28); one decision record per delegation across all
five LLM fallback paths; the parent-level COMPLETE write on the grace drop (§17); `stage_inputs`
with a name in `data_ready` but not `data_loc` (§63); a timed-out *container* (§62); SWIM
membership seeding and on-the-wire refutation (§11); a run started with `--debug` (§47); a
hierarchical config cross-check that the observed leader distribution matches
`--groups-per-coordinator` when `--co-parents > 1` at G > 1 (the primary parent is the
lowest-id co-parent, not the owner — `generate_configs.py:867-875`).

**Design-doc drift, both directions** (`GOSSIP_CONSENSUS_DESIGN.md`): promises not delivered —
PBFT fallback when Snow does not converge (`:285`, table `:583`; `:669` contradicts it, and the
Redis-partition fallback at `:713` is also absent), job announcement over gossip (§4.2),
feasibility and gossip-best in the peer response (§4.4), SWIM anti-entropy. Reverse: the doc says
Phase 4 hybrid is "not implemented" and the engine is "single globally" (`:3`, `:11`, `:675`);
the code does per-level hybrid (`resource_agent.py:369-382`).

**Persisted bandit settings override the config** when state is reloaded from Redis: alpha,
discount, exploration weight, `ts_variance`, `step_size` are restored from the saved state
(`bandit.py:141, 244, 392-393, 445`) — the bug already fixed for epsilon, in five more places.
Only with `persist_to_redis: true` (off by default).

---

## Checked and found correct

PBFT straggler and COMMIT accounting after the 2026-09-18 fix; `SelectionCounters` distinct-job
counting; the `RunningStats` reservoir; context age on the monotonic clock with the two-clock
split; `submitted_at` as the top-tier stamp; the dedup progress tuple; run-id filtering in
`single_run.py`; the oracle's every-candidate refusal inside `score_decision`; the
Poisson-binomial SE; the completion write and the readiness/location registry as one MULTI; the
staging server's serve-only-published-names rule; the `ExecutionSpec`/converter refusal-rule
agreement test; the `None`/`[]` argument distinction at the converter boundary (defeated one layer
up, §57, but correct where it was built).

---

## Recommended order

1. **§1–§4** — Snow cost units (answer with the penalised cost, or strip the factor from the
   proposal; one unit), DTN dicts (normalise in the `AgentInfo` setter), yield as a true
   abstention (a response that counts toward neither candidate, with the α denominator adjusted),
   `aggressive_failure_detection: false` shipped and `on_status` on change only. Small diffs;
   every Snow number depends on them. Then re-measure the Snow smoke cells.
2. **§5, §6** — count the own vote (or exclude self from the denominator; pick one and write it
   down), key the leader guard on `proposal.agent_id == self` plus `_commits_sent` rather than
   `outgoing.contains`. Land with a multi-engine PBFT test.
3. **§13–§16** plus the **double-execution metric** (§H), so the fix is measurable rather than
   argued. Exclude delegated jobs from coordinator-tier reassignment (or carry a `delegated`
   state); release the claim in `_reassign_delegated_job`; have children purge on key deletion
   (a tombstone in the index, or a PENDING-with-generation record instead of a delete); break only
   when no copy is RUNNING.
4. **§34–§39** in `collect.py` — no run needed; re-collect existing data. Reselection and churn
   from the agents' own counters, not from Redis key counts; per-metric `n` and a NaN-as-failure
   policy in `aggregate()`; offered-jobs denominators; `msgs_consensus_*` from `sent_by_type`;
   fairness over the whole fleet; makespan from first submission over all jobs.
5. **§47–§51** in `run_test.py` — `--debug` forwarding, drain on Redis failure and on in-flight
   states, no `pkill` in the dynamic path, a fleet-wide reap before flush (sweep every host in
   `/etc/hosts`, not the current file), and an advertised-address check at launch.
6. **§22, §23** before any E4/C1 run; **§61–§63** before any real-workflow run.

Items 1–5 together are perhaps three days of work plus the tests named in §H. Per the standing
rules (`swarm-no-cross-substrate-result-reuse`), everything measured before they land is a smoke
run, not a result.
