# SwarmAgents — bugs found during the Chaos Jungle evaluation

Defects surfaced while standing up the LLM fault-injection experiments on a 30-node FABRIC
testbed (see [`CHAOS_JUNGLE_LLM_TEST_PLAN.md`](CHAOS_JUNGLE_LLM_TEST_PLAN.md)). Companion to
[`CHAOS_JUNGLE_FINDINGS.md`](CHAOS_JUNGLE_FINDINGS.md), which covers the framework side.

Most of these are **silent**: the system keeps running and produces plausible-looking results
while a scheduling term is inert, the fleet differs between runs, or the LLM is never actually
consulted. That makes them worth recording even though several are now fixed — the failure mode
is "quietly invalid experiment", not "crash".

| # | Status | Severity | Summary |
|---|--------|----------|---------|
| 1 | Fixed `c51b6af9` | **Blocker** | Ollama provider unusable — resolved endpoint was computed then discarded |
| 2 | Fixed `c51b6af9` | High | Structured output fails on small models; wrong pydantic-ai output mode |
| 3 | Fixed `bf5e2e44` | **High (silent)** | Job DTN names never matched agent DTNs, so the connectivity term was dead |
| 4 | Fixed `bf5e2e44` | **High (silent)** | Agent fleets were randomly regenerated per run, making runs incomparable |
| 5 | Fixed `fb60a3ef` | High | Per-file DTN spreading made 25% of jobs unschedulable and stalled runs |
| 6 | **Open** | Medium | `run_test.py` deletes `agent_hosts.txt`, then crashes reading it |
| 7 | **Open** | Medium (silent) | `llm.timeout_seconds` is parsed but never enforced — dead config |
| 8 | **Open** | Low | Converter mutates its module-level flavour table through aliased dicts |
| 9 | **Open** | Low (caveat) | `Job.execute()` ignores `wall_time`, so makespan is not meaningful |
| 10 | Fixed (uncommitted) | Medium (silent) | Selection and consensus both tie-break on agent id, and the proposal cost carries the id |
| 11 | **Open** | **High (silent)** | Under Snow, peers vote with the analytic cost — an LLM agent's bid is never consulted |

---

## Fixed

### 1. Ollama provider was unusable — endpoint computed then thrown away

`swarm/agents/llm/llm_bidder.py` resolved the endpoint and then ignored it:

```python
base_url = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434/v1")   # never used
api_key  = os.getenv("OLLAMA_API_KEY")                                  # never used
model = OpenAIChatModel(model_name, provider="ollama")                  # ignores both
```

`provider="ollama"` makes pydantic-ai construct its own `OllamaProvider`, which *requires*
`OLLAMA_BASE_URL` and raises `UserError` otherwise — so the whole provider was dead on arrival.

**Fix** — resolve **env → config `base_url` → default** and pass it explicitly via
`OllamaProvider(base_url=…, api_key=…)`. Env deliberately wins so a fault proxy can be interposed
in front of one host's agent without rewriting per-agent configs. Added `llm.base_url` to the
config schema.

### 2. Structured output fails on small local models

`qwen2.5:3b` advertises tool support but emits tool-call arguments that ignore the schema —
`{"bid": {"agent": 2, "job": 1.5, "scoreExplanation": …}}` instead of `{"score", "explanation"}` —
so every bid failed with *"Exceeded maximum output retries"*.

Measured on `qwen2.5:3b`:

| pydantic-ai output mode | success |
|---|---|
| `ToolOutput` (default) | 0/3 |
| `PromptedOutput` | 0/3 |
| **`NativeOutput`** (Ollama json_schema) | **3/3** |

**Fix** — use `NativeOutput(Bid)` for the `ollama` provider only; hosted providers keep the
default tool output, which is proven at 3062/3062 calls.

### 3. Job DTN names never matched agent DTN names (silent)

`run_test.py` called `convert_pegasus_profiles()` without `data_nodes_mode` or `dtn_names`, so
every converted job kept the raw Pegasus site name **`local`**, while `generate_configs.py` gives
agents **`dtn1…dtn10`**. Zero overlap, and `local` is *explicitly excluded* from a job's required
DTNs (it means local filesystem, not a transfer node) — so the DTN connectivity term could never
be satisfied and **dropped out of the cost model entirely**.

Nothing failed. Two complete baselines were produced before this was noticed, both with a
scheduling term silently inert.

**Fix** — default the conversion to per-file data nodes spread across the *same* pool
`generate_configs.py` draws from, so jobs and agents match by construction; `--pegasus-dtn-names`
overrides. After the fix every data-carrying job matches ≥1 agent DTN and the median job is
feasible on 9 of 30 agents — differentiating, not dead.

### 4. Agent fleets were randomly regenerated on every run (silent)

`generate_configs.py` drew capacities, flavours and DTN assignments from an **unseeded** RNG, and
`cleanup_between_runs` deletes `agent_profiles.json`/`agent_dtns.json` before each run — so every
run scheduled on a *different* fleet. Any baseline-vs-fault comparison was confounded.

**Fix** — added `--seed` (threaded through `run_test.py`). Verified: regenerating with the same
seed reproduces byte-identical profiles, DTN map and all 30 per-agent configs.

> **Sub-gotcha:** seeding alone is not sufficient. `generate_configs.py` **reuses an existing
> `agent_dtns.json`** if present, which takes a different code path and consumes randomness
> differently, so a "reproducibility check" that does not start from a clean state will
> disagree with itself. Delete `configs/`, `agent_profiles.json` and `agent_dtns.json` first.

### 5. Per-file DTN spreading made jobs unschedulable and stalled runs

`is_job_feasible` is **all-or-nothing** over a job's DTNs — an agent missing any one of them is
rejected — while agents receive only 1–4 of the 10. Spreading a job's files across the pool by
file-name hash therefore left **76 of 300 jobs with no feasible agent anywhere**. With a 10-job
proposal window those jobs head-of-line blocked the pending queue and the run stalled: agents
spinning on cached costs, no LLM calls, 181 jobs never draining.

**Fix** — added `--dtn-scope {file,job}` to the converter; `job` hashes over the job id so all of
a job's files land on one DTN. That is also the realistic model, since a workflow job stages from
one or two sites rather than eight. `run_test.py` passes `job`.

**Guidance that follows:** verify feasibility offline — intersect each job's required DTNs and
capacities against `agent_profiles.json` — *before* launching. A stalled run and a slow run look
identical for the first hour.

---

## Open

### 6. `run_test.py` deletes `agent_hosts.txt` and then crashes reading it

`cleanup_between_runs()` unconditionally removes `agent_hosts.txt`, but when
`--agent-hosts-file agent_hosts.txt` is passed, `generate_configs.py` then tries to read that same
path:

```
FileNotFoundError: [Errno 2] No such file or directory: 'agent_hosts.txt'
subprocess.CalledProcessError: Command '['python3.11', 'generate_configs.py', …]' returned exit status 1
```

The host list is already in memory (`read_hosts()` ran earlier); only the child process re-reads
the file. It fails for the most natural invocation — passing the file the repo itself documents.

**Workaround** — pass any other filename (we use `agent_hosts_cj.txt`).
**Suggested fix** — do not delete the file when `--agent-hosts-file` names it, or have
`generate_configs()` rewrite it from the in-memory list before invoking the child.

### 7. `llm.timeout_seconds` is never enforced (silent)

The key is parsed into `LlmConfig` (`llm_config.py`) and **used nowhere** — `llm_bidder.py`
contains no timeout handling at all. Observed effect: with `timeout_seconds: 6`, individual bids
took **14.9 s and 19.2 s** without ever tripping a fallback, and during one misconfiguration a
single call blocked for **20 minutes**.

This matters beyond tidiness: a reader configuring `timeout_seconds` reasonably expects a bounded
bid and a fallback to the analytic model on breach — which is exactly the resilience behaviour
the LLM agent advertises. Latency also propagates: an agent blocked in inference answers SWIM
probes late, which showed up as false membership failures.

**Suggested fix** — pass the deadline into the model call (pydantic-ai accepts per-request
timeouts / an `httpx` client timeout) and let the existing `except` path fall back, or remove the
key so it does not imply a guarantee.

### 8. Converter mutates its module-level flavour table through aliased dicts

In `pegasus_to_swarm_converter.py`:

```python
flavors.extend([flavor] * count)   # N references to the SAME dict from INSTANCE_FLAVORS
...
for flavor in flavors:             # mutates those dicts in place
    if flavor["core"] < max_core:
        flavor["core"] = int(math.ceil(max_core))
```

`[flavor] * count` stores repeated references, so the "grow every flavour to fit the largest job"
loop rewrites the module-level `INSTANCE_FLAVORS` entries. Harmless in a one-shot CLI run, but a
second call in the same process starts from already-inflated flavours.

**Suggested fix** — `flavors.extend(copy.deepcopy(flavor) for _ in range(count))`.

### 9. `Job.execute()` ignores `wall_time` (caveat, likely deliberate)

```python
wt = self.wall_time or 0.0
if wt > 0:
    #time.sleep(wt)
    time.sleep(1)
```

Every job occupies its agent for a flat second regardless of its real duration. Since the Pegasus
traces carry true wall times spanning seconds to ~25 hours, **makespan and utilisation from these
runs are not comparable to the source workflows** — though scheduling-decision metrics
(completion, fallback rate, fairness, conflicts) are unaffected, since `wall_time` still feeds the
cost model and the LLM prompt.

Worth either restoring a scaled sleep (`wall_time * scale`) or stating the limitation wherever
makespan is reported.

### 10. Tie-breaking on agent id, and a proposal cost that carries it (fixed)

Three places composed into one bias toward low agent ids:

```python
# swarm/selection/engine.py — exact ties went to the lowest agent id
tied = [i for i in finite_idx if col[i] == best_val]
best_idx = min(tied, key=lambda i: tie_break_key(assignees[i], float(col[i])))

# swarm/agents/llm/llm_agent.py:325 (and resource_agent.py:2064)
cost=round((cost + self.agent_id), 2)

# swarm/consensus/engine.py and gossip_engine.py — equal cost, lower id wins
existing.cost == incoming.cost and (existing.agent_id or "") > (incoming.agent_id or "")
```

`tie_break_key=agent_id` is documented as a *deterministic tie-break*, which assumes ties are
rare. They are not: in the fault-free reference run **626 of 1064 bids (59%) are the identical
value `Score=75.00`**, because a 3B model asked for a 0-100 score answers in round steps. The
proposal cost is worse than a tie-break — adding the raw agent id is a **±30 swing on a 0-100
scale** for a 30-agent fleet, larger than most real cost differences, and always in the same
direction. Observed directly in the logs: agent 25 advertised `Cost=25.00 FinalCost=50.00`
while agent 7 advertised `Cost=25.00 FinalCost=32.00` for the same bid.

**Fix** — `swarm/utils/tiebreak.py`: `tiebreak_rank(object_id, agent_id)` is a per-object
pseudorandom permutation of agents (blake2b, not `hash()` which is salted per process, and not
crc32 whose linearity left a measurable 4x bias — see `tests/test_tiebreak.py`). Selection, the
PBFT engine, the Snow engine and `ProposalContainer` all now break exact ties on it, so the
three layers still agree on a winner without any of them favouring low ids. The `+ agent_id`
term is gone; proposals advertise their real cost (confirmed in a live run: `FinalCost` now
equals `Cost`).

> **What the fix did *not* do.** It was expected to spread placement, and it did not:
>
> | run | code | agents 1-10 take |
> |---|---|---|
> | `cj-baseline-ref` | id tie-break | 153 of 300 |
> | `cj-baseline-fixedtb` | fixed | 146 of 300 |
> | `cj-baseline-fixedtb2` | fixed | 158 of 300 |
> | `cj-baseline-cloud` | fixed, uniform bid latency | **112 of 300** |
>
> Two runs under the fix bracket the pre-fix run; only removing the *latency* heterogeneity
> (moving inference off-host) moved the distribution. The
> earlier conclusion that "placement is decided by agent id" was wrong: id was standing in for
> **bid latency**, which is what actually decides placement (see the test plan's S09 section).
> Low-numbered hosts on this slice happen to infer faster — `corr(agent id, bid latency)` is
> +0.13 to +0.35 across runs — and it is the speed, not the number, that wins the race to
> propose. The tie-break is still worth fixing: a ±30 id term on a 0-100 cost cannot be
> defended, and it silently confounds any per-group analysis split by id. It is simply not the
> cause of the concentration it appeared to explain.

**Still open, and unaffected by this fix** — a cost that ties 59% of the time is worth
addressing at the source.

**A bigger model does not fix it — it makes it worse.** Comparing the two arms already run:

| model | bids | distinct score values | modal value | top-2 share |
|---|---|---|---|---|
| `qwen2.5:3b` (local Ollama) | 1064 | 46 | 75.00 (58.8%) | 73.2% |
| `gpt-oss-20b` (FABRIC gateway) | 3062 | **24** | 95.00 (69.6%) | **92.1%** |

The 6x larger model emits *half* as many distinct values and puts 92% of its bids on two of
them, both at the top of the range (95 and 90) — i.e. it rates nearly every agent an excellent
fit and discriminates between them barely at all. Degenerate cost signals are a property of
asking an LLM for a 0-100 rating, not of model size, so scaling the model is not the lever.
(Caveat: the gateway run used the older random fleet and 6-workflow trace, so the job mix
differed; the concentration comparison is indicative, not controlled.)

What is likely to work is changing the *elicitation*: ask for a finer scale, ask for a pairwise
or rank judgement instead of an absolute score, or break score ties with the analytic cost so a
tie falls back to a signal that is actually continuous. Note the sequencing, though — while
placement is decided by bid latency rather than bid value (see the test plan's S09/S09b), a
better cost signal cannot change placement on its own.

### 11. Under Snow, peer votes never see the LLM bid (silent)

`LlmAgent` replaces the cost function *in the selection engine only*:

```python
# swarm/agents/llm/llm_agent.py — the LLM cost reaches the selector
self.selector = SelectionEngine(cost=self._llm_or_analytic_cost, ...)
```

The Snow engine asks its host a different question, through an adapter `LlmAgent` does not
override:

```python
# swarm/agents/resource_agent.py — _HostAdapter.my_cost_for_job, used by every SnowQuery
return float(self.agent._cost_job_on_agent(obj, info))   # the ANALYTIC model
```

`_cost_job_on_agent` is defined once, in `ResourceAgent`; `LlmAgent` calls it only as its
fallback. So with `consensus.protocol: snow` — the shipped default — **an agent answering a
query prices the job analytically**, and the LLM's opinion enters the protocol only through
whoever initiated the proposal. The reasoning the fleet spends ~10 s per bid producing is
consulted once and then out-voted by a model it was meant to replace.

There is a second, sharper edge: the two costs are not on the same scale. The analytic cost is
roughly 0-1 (weighted utilisations plus penalties); the LLM cost is `100 - score`, so 25-75.
A peer comparing `my_cost` against the initiator's advertised cost is therefore comparing 0.5
against 45 and concluding it dominates, essentially always — the dominance rule degenerates.

This is not a tie-break problem and finding 10's fix does not touch it. It needs either the
LLM cost made available to the inbound query path (a cache, not a call — `_answer_query` runs
on the single inbound consumer thread and must not block), or the two cost models normalised
onto one scale before they are ever compared.
