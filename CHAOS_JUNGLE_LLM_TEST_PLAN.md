# Evaluating Chaos Jungle on SwarmAgents' LLM Scheduling Agents

**Purpose.** Use the [Chaos Jungle](https://swarmourr.github.io/CJ/index.html) (CJ) chaos-engineering
framework to evaluate the resilience of **SwarmAgents' LLM-driven scheduling agents** under
LLM-layer faults, producing figures and hypothesis tests for the Chaos Jungle paper. The focus is
the LLM plane; infrastructure faults appear only as a single composite scenario.

**Status (2026-08-16):** environment fully provisioned and validated — **Phase 0 complete**. Both
LLM backends have fault-free baselines at 30-agent scale, CJ is installed and proven to intercept
SwarmAgents' LLM path. Ready to begin Phase 1 (fault sweeps).

| | |
|---|---|
| **Code** | `SwarmAgents` @ `dabe53f0`, branch `chaos` |
| **Testbed** | FABRIC slice: 1 orchestrator (`database`) + 30 agent hosts, 8-core CPU / 7 GB RAM each, no GPU |
| **Chaos Jungle** | v0.1.0, pinned commit `5044939` (see [§6](#6-findings-for-the-cj-maintainers)) |
| **Workload** | 1413 job profiles extracted from real Pegasus workflow runs |

---

## 1. How CJ injects faults into SwarmAgents

SwarmAgents' `LlmAgent` scores every `<job, agent>` pair through **pydantic-ai**, converting the
model's 0–100 score into a scheduling cost (`cost = 100 − score`). On **any** exception it falls
back to the analytical cost model — logged as `[LLM_COST_FALLBACK]`.

CJ injects LLM faults by **spawning a local proxy and redirecting the client's base URL to it**,
then forwarding to the real upstream. Since pydantic-ai honours `OPENAI_BASE_URL` /
`OLLAMA_BASE_URL`, injection needs **no SwarmAgents code changes**:

```
agent (pydantic-ai) --[OLLAMA_BASE_URL]--> CJ fault proxy --> real LLM endpoint
```

The endpoint is resolved as **env var → YAML `llm.base_url` → default**. Env deliberately wins, so
setting `OLLAMA_BASE_URL` on one host redirects *that host's* agent into a fault proxy without
touching any per-agent config. **This is what makes partial-fault studies possible** — fault 10% of
the fleet by starting the proxy on 3 of 30 hosts.

Because faults are injected per host, the natural blast-radius knob is *which hosts* run a proxy.

---

## 2. Environment

### 2.1 Two LLM backends ("arms")

| Arm | Endpoint | Model | Role |
|-----|----------|-------|------|
| **Ollama** (primary) | `127.0.0.1:11434`, per host | `qwen2.5:3b` | All fault sweeps — plain HTTP, no shared contention |
| **Gateway** | FABRIC LiteLLM proxy | `gpt-oss-20b` | Realism spot-check, fault-free only |

**Why fault-inject on Ollama, not the gateway:** CJ's proxy is built for plain-HTTP upstreams.
Against the authenticated-HTTPS gateway it injects the delay but then fails to forward the
call — turning a *latency* fault into an *outage*, which would confound every measurement. Ollama
also avoids shared-endpoint contention that would otherwise pollute baseline-vs-fault deltas.

**Gateway access note:** the slice has **no IPv4 default route** (IPv6-only public egress), and the
gateway publishes only an A record — so its public address is unreachable from the slice. It *is*
reachable over the **FABNet dataplane** at `10.141.1.2`; mapping the hostname to that IP in
`/etc/hosts` keeps TLS/SNI valid. The gateway serves 4 reasoning+tool_use models; of these only
`gpt-oss-20b` is fast enough for a per-job scoring loop (1.3 s vs 37–43 s for the others).

### 2.2 Workload — a frozen, reproducible mixed Pegasus trace

Experiments run on a **fixed 300-job trace merged from two extractions**, built by
`build_mixed_trace.py` (seeded, stratified by workflow, so a rebuild is byte-identical):

| Source | Jobs | Contributes |
|--------|------|-------------|
| `cpegasus` | 200 | Per-file **data/DTN nodes** (real filenames + byte sizes) — 6 workflows |
| `pegasus` | 100 | **Workflow & resource diversity** — 10 workflows, heavier tail (up to 8 cores / 46 GB) |

**16 distinct workflows**, 199/300 jobs (66%) carrying data nodes. The `pegasus` extraction predates
the extractor's file-resolution feature, so its jobs have no data nodes — a legitimate second
population (jobs with no data movement), and its source hosts are no longer reachable to re-extract.
`pegasus2` (13,908 jobs) was evaluated and **excluded**: 97% is a single workflow of near-identical
2-second jobs, so it adds volume but almost no signal for LLM scoring.

**DTN names must match the agents' pool.** Convert with
`--dtn-names dtn1,…,dtn10`; otherwise job data-nodes keep the raw Pegasus site name (`local`) and
share *zero* names with agent DTNs, silently removing the connectivity term from the cost model.
With the frozen trace, every data-job matches ≥1 agent DTN and on average 50% of agents share a
DTN with a given job — real differentiation.

> **Fixed in `run_test.py`.** `--pegasus-profiles` previously converted with per-site naming and no
> DTN spread, leaving every job on site `local` — no overlap with the agents' `dtn1..dtn10`, so the
> connectivity term silently dropped out of the cost model (this affected the first two baselines).
> It now defaults to `--pegasus-data-nodes per-file` and spreads files across the *same* pool
> `generate_configs.py` gives agents, so the two match by construction. Both are overridable
> (`--pegasus-dtn-names`).

---

## 3. Validated baselines (fault-free)

Both arms: 30 agents, mesh topology, Snow consensus, 300 Pegasus jobs.

| Arm | Model | LLM calls | Fallbacks | Failed jobs | Latency (mean) |
|-----|-------|-----------|-----------|-------------|----------------|
| Gateway | `gpt-oss-20b` | 3062 | **0** | **0** | 5.85 s (under 30-agent contention) |
| Ollama | `qwen2.5:3b` | 3331 | **0** | **0** | 9.61 s (p50 9.72 / p95 11.46) |

Every scoring call succeeded on every agent, and all jobs reached a terminal state. These are the
reference points every fault scenario is measured against.

> **Runtime planning:** at ~9.6 s per bid, one 300-job Ollama run takes roughly 25–30 min. A full
> Tier-1 sweep with repeats is many hours — consider a 100-job trace for wide sweeps.

---

## 4. Metrics, oracles, and what we are actually measuring

The unit of measurement is **the scheduler**, not the model. CJ's
`ChaosRunner.measure(workload, n_baseline, n_fault)` runs a workload under both conditions and
computes the delta; our workload is one full SwarmAgents run.

**Primary (scheduling health)**
- Job **completion rate** and failed/orphaned count
- **Makespan**, P50/P95 job latency
- **Consensus conflicts** and reselections/restarts
- **Load fairness** (Jain's index)
- **Fallback rate** — `[LLM_COST_FALLBACK]` vs `[LLM_COST_COMPLETE]`; *the* key LLM-resilience signal

**Secondary (LLM plane)**
- Per-call latency (`ReasoningTime`), HTTP error mix (429/402/503/504/401)
- Score-distribution shift under semantic faults
- Token cost (gateway arm)

**Correctness invariant (must hold under every fault):** **zero double-assignments** — enforced by
Redis `SET NX` in the Snow engine. Safety must never degrade, only performance.

**Oracles / quality gates**
- `result.passed("completion_rate", threshold=…)` as a CI-style gate per scenario
- CJ's `LLMJudge` on the bidder's `explanation` field — does a corrupted score arrive with a
  confidently-wrong justification? (CJ Layer 10)
- "Graceful degradation" oracle: completion rate stays above threshold *because* fallback engaged

---

## 5. Experiment matrix

Each row is one CJ `Scenario`, run baseline-vs-fault with n≥5 repeats on a fixed job trace, across
**two topology arms**: flat mesh (30 agents) and hierarchical (LLM agents as Level-1 coordinators,
enabling the *targeted-fault-on-coordinators* story).

### Tier 1 — LLM API faults (CJ Layer 1) — core of the paper
| # | Fault | Sweep | Hypothesis |
|---|-------|-------|------------|
| L1 | `LLMLatency` | 1/3/6/10 s | Latency degrades throughput; fallback absorbs the worst case |
| L2 | `LLMTimeout` | hang 8/15 s | Cancellation + fallback keeps scheduling live |
| L3 | `LLMUnavailable` (503) | 25/50/100% of agents | Full outage ⇒ degrades to pure analytic scheduler, no correctness loss |
| L4 | `LLMRateLimit` (429 after n) | n = 5/20 | Back-off vs fallback; graceful or collapse? |
| L5 | `LLMResponseCorrupt` | truncate/empty/invalid_json | Parse errors ⇒ fallback, no crash, no bad proposals |
| L6 | `LLMBudgetExceeded` (402) | cap mid-run | Cost-cap outage ⇒ fallback |
| L7 | `LLMTokenStarvation` | tiny `max_tokens` | Real truncated output — does score degrade *silently*? |
| L8 | `LLMUnauthorized`/`AuthExpiry` | expiry mid-run | Credential failure ⇒ fallback |

### Tier 2 — Semantic / RAG faults (CJ Layer 4) — the "silent-wrong" story
The HTTP call succeeds and the JSON is valid, but the *content* is wrong — so **fallback never
fires** and consensus itself must absorb a corrupted cost signal. This is the most interesting tier.

| # | Fault | Hypothesis |
|---|-------|------------|
| S1 | `SemanticCorrupt(entity_swap)` | Skewed capacities distort scores; consensus tolerates a minority of poisoned bidders |
| S2 | `SemanticCorrupt(rag_poison)` | Poisoning peer-load context defeats load-aware scoring ⇒ dog-piling, fairness drops |
| S3 | `SemanticCorrupt(inject_distractor)` | Resilience to indirect prompt injection via gossiped peer state |
| S4 | `SemanticCorrupt(context_truncate)` | Degraded but not incorrect scheduling |

**Headline experiment:** sweep the poisoned-agent fraction 0→50% to find the tolerance threshold —
*"SwarmAgents tolerates up to K% semantically-corrupted LLM agents before completion and fairness
degrade."*

### Tier 3 — Composite "bad day" (the single infra touchpoint)
| # | Fault |
|---|-------|
| X1 | `LLMLatency` + `SemanticCorrupt(rag_poison)` on a minority + node loss, simultaneously. Does completion hold, and does double-assignment stay 0? |

### Tier 4 — Dosing strategies
Wrap Tier 1–2 faults in CJ strategies: percentage-based, ramp-up (cleanest degradation curves), and
targeted (coordinators only, hierarchical arm).

### Baselines & ablations
1. **Clean baseline** — done (§3)
2. **Analytic-only** (`resource` agents) — isolates what the LLM adds
3. **LLM with fallback disabled** — exposes true LLM dependence; expect sharp cliffs where the
   default config stays flat. *This is what proves the safety net's value.*
4. **Consensus ablation** — Snow vs PBFT under the same faults

---

## 6. Findings for the CJ maintainers

Issues found while deploying Chaos Jungle 0.1.0 — all reproducible, and relevant to the paper.

1. **`pip install chaos-jungle` does not work** — the package is not on PyPI, despite the docs
   instructing this. Install from GitHub.
2. **The repo HEAD / v1.5.0 cannot be imported at all.** `chaos_jungle/__init__.py` re-exports
   `InjectResult`, `ChaosFuzzer`, and other names that are **defined nowhere in the repository**
   (a repo-wide code search returns no definition). Introduced by the 2026-07-06 "v1.5.0" commit;
   every commit after it is docs-only, so `main` has been unimportable since.
   **Workaround:** pin the last good pre-v1.5.0 commit —
   ```bash
   pip install "git+https://github.com/swarmourr/CJ.git@5044939950f3ac020c6fe02073642295865cbeae"
   ```
   Verified at that commit: imports clean, all LLM/semantic fault classes instantiate, and
   `ChaosRunner` exposes `.measure()` / `.start()` / `.stop()`.
3. **The LLM proxy does not handle authenticated-HTTPS upstreams.** Against a Bearer-auth TLS
   endpoint, `LLMLatency` applied the delay but the forwarded request failed, silently converting a
   latency fault into an outage. Works correctly against plain-HTTP upstreams (e.g. Ollama).
4. **Docs/API drift:** `LLMRateLimit` takes `n` (docs suggest `after`); `LLMTimeout` takes
   `timeout_s`.

**CJ works as advertised once pinned.** Validated end-to-end: `ChaosRunner.measure()` around a real
`LlmBidder.score()` call produced a clean delta — **baseline 1.02 s → fault 3.08 s (Δ +2.06 s)**
with `LLMLatency(delay_s=3)`.

---

## 7. SwarmAgents bugs found and fixed

Both in `swarm/agents/llm/llm_bidder.py`; needed before the Ollama arm could run at all.

1. **Dead endpoint configuration.** The `ollama` branch computed `base_url`/`api_key` from the
   environment and then **discarded them**, calling `OpenAIChatModel(model, provider="ollama")` —
   which demands the `OLLAMA_BASE_URL` env var and raises otherwise. Now resolves
   **env → config `base_url` → default** and passes it explicitly via `OllamaProvider(...)`.
2. **Wrong structured-output mode for small models.** `qwen2.5:3b` supports tool calling but
   invents its own schema in the arguments — emitting `{"bid":{"agent":2,"job":1.5,...}}` instead
   of `{"score":…,"explanation":…}` — so pydantic-ai failed with *"Exceeded maximum output
   retries"*. Measured across modes:

   | Output mode | Success |
   |---|---|
   | `ToolOutput` (pydantic-ai default) | 0/3 |
   | `PromptedOutput` | 0/3 |
   | **`NativeOutput`** (Ollama json_schema) | **3/3** |

   The bidder now uses `NativeOutput(Bid)` **for the ollama provider only**; the gateway path keeps
   default tool output (proven 3062/3062).

New config keys in `config_swarm_multi.yml`: `llm.base_url`, plus `provider: ollama`,
`model: "qwen2.5:3b"`.

3. **DTN names never matched between jobs and agents.** `run_test.py --pegasus-profiles` called the
   converter without `data_nodes_mode`/`dtn_names`, so every job data-node kept the raw Pegasus site
   name `local` while agents held `dtn1..dtn10` — zero overlap, so the connectivity term could never
   be satisfied and dropped out of the cost model. Now defaults to `per-file` granularity and the
   agent DTN pool, with `--pegasus-data-nodes` / `--pegasus-dtn-names` to override.
4. **Agent fleets were unreproducible.** `generate_configs.py` used unseeded `random` for
   capacities, flavors and DTN assignment, and `cleanup_between_runs` deletes the profiles before
   every run — so each run built a different fleet. Added `--seed` (threaded through `run_test.py`);
   verified identical profiles/DTNs/configs across regenerations from a clean state.

---

## 8. Runbook

`ssh chaos` lands on `database` as `ubuntu`; the repo is at `/root/SwarmAgents` (use `sudo`).
`database` has passwordless root SSH to `agent-1 … agent-30`. Python is `python3.11`.

### 8.1 Mandatory pre-run cleanup — before EVERY run
Leftover agents on *any* host register into the shared Redis and stall the next run at
`[SEL_WAIT] … live≠configured`. Stale per-host logs also inflate the result counters, since they
append across runs.

```bash
ssh chaos 'sudo bash -c '"'"'
for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do
  ssh -o StrictHostKeyChecking=no $h "pkill -9 -f main[.]py; rm -f /root/SwarmAgents/swarm-multi/agent-*.log" 2>/dev/null &
done; wait
pkill -9 -f main[.]py; rm -f /root/SwarmAgents/swarm-multi/agent-*.log
docker exec redis redis-cli flushall
echo "dbsize=$(docker exec redis redis-cli dbsize)"
'"'"''
```

### 8.2 Select the LLM arm
```bash
# Ollama (default; used for all fault work)
ssh chaos 'sudo sed -i "s/^  provider:.*/  provider: ollama/; s/^  model:.*/  model: \"qwen2.5:3b\"/" /root/SwarmAgents/config_swarm_multi.yml'

# Gateway (fault-free spot-checks)
ssh chaos 'sudo sed -i "s/^  provider:.*/  provider: openai/; s/^  model:.*/  model: \"gpt-oss-20b\"/" /root/SwarmAgents/config_swarm_multi.yml'
```
Pre-warm Ollama before launching agents, or the first score per agent hits an ~11.5 s cold model
load and falls back once:
```bash
ssh chaos 'sudo bash -c '"'"'for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do ssh -o StrictHostKeyChecking=no $h "bash /root/cj_infer_check.sh" & done; wait'"'"''
```

### 8.2b Frozen fleet + trace (do this ONCE, then reuse for every run)

Agent profiles are otherwise **randomly regenerated on every run** — `cleanup_between_runs` deletes
`agent_profiles.json`/`agent_dtns.json` and `generate_configs.py` recreates them with unseeded
`random`. Baseline and fault runs would then use different agent fleets, confounding every delta.
Fix: generate once with `--seed`, then reuse via `--use-config-dir`.

```bash
# (a) Build the mixed trace (byte-identical for a given seed) and convert with matching DTN names
python3 build_mixed_trace.py \
  --cpegasus <cpegasus>/all_runs_jobs_profile.json \
  --pegasus  <pegasus>/profiles/all_runs_jobs_profile.json \
  --n-cpegasus 200 --n-pegasus 100 --seed 42 --output mixed_profile_300.json
python3 pegasus_to_swarm_converter.py --input mixed_profile_300.json --input-type json \
  --output-dir mixed_jobs_300/ --data-nodes per-file \
  --dtn-names dtn1,dtn2,dtn3,dtn4,dtn5,dtn6,dtn7,dtn8,dtn9,dtn10

# (b) Generate the frozen fleet ON THE SLICE. Start from a clean state — generate_configs REUSES
#     an existing agent_dtns.json, which silently breaks reproducibility.
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && rm -rf configs agent_profiles.json agent_dtns.json && \
  python3.11 generate_configs.py 30 10 ./config_swarm_multi.yml /root/SwarmAgents/configs mesh database 300 \
    --dtns --seed 42 --agent-hosts-file agent_hosts_cj.txt --agents-per-host 1 --agent-type llm"'

# (c) Stage the jobs and snapshot everything
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && rm -rf jobs && mkdir -p jobs && \
  cp mixed_jobs_300/job_*.json jobs/ && \
  tar czf /root/frozen_fleet_seed42.tgz configs jobs agent_profiles.json agent_dtns.json"'
```
Verified: regenerating from a clean state with `--seed 42` reproduces identical `agent_profiles.json`,
`agent_dtns.json`, and all 30 per-agent configs. Restore the snapshot any time with
`tar xzf /root/frozen_fleet_seed42.tgz -C /root/SwarmAgents`.

> The generated artifacts (`mixed_profile_300.json`, `mixed_jobs_300/`, `jobs/`, `configs/`) are
> **not in git** — the repo ignores `*.json` and `jobs/`. They are fully rebuildable from the
> commands above, since both the trace sampling (`--seed 42`) and the fleet (`--seed 42`) are
> deterministic. A copy of the trace is kept outside the repo at
> `pegasus-profiles/mixed-trace/`, and the slice snapshot at `/root/frozen_fleet_seed42.tgz`.

### 8.3 Run
```bash
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && nohup python3.11 run_test.py \
  --mode remote --agent-type llm --agents 30 --agents-per-host 1 \
  --topology mesh --jobs 300 --db-host database --agent-hosts-file agent_hosts_cj.txt \
  --use-config-dir \
  --jobs-per-interval 30 --stable-seconds 120 --runtime 3000 \
  --generate-plots --run-dir runs/<SCENARIO> > runs_<SCENARIO>.log 2>&1 &"'
```
- **`--use-config-dir` is what makes runs comparable** — it reuses the frozen fleet and staged
  `jobs/` instead of regenerating a fresh random fleet and re-converting. Redis is still flushed.
- Use a **unique `--run-dir` per run**; together with §8.1 this guarantees clean per-run counters.
- Pass `--agent-hosts-file agent_hosts_cj.txt` — **not** `agent_hosts.txt`. run_test deletes the
  literal `agent_hosts.txt` during cleanup and then crashes trying to read it.
- Hierarchical arm: `--topology hierarchical --hierarchical-level1-agent-type llm --groups G --group-size S`.

### 8.4 Read the results
```bash
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents/runs/<SCENARIO> && \
  echo COMPLETE=\$(grep -rho LLM_COST_COMPLETE . | wc -l) \
       FALLBACK=\$(grep -rho LLM_COST_FALLBACK . | wc -l) \
       BIDWON=\$(grep -rho LLM_BID_WON . | wc -l)"'
```
Healthy baseline ⇒ `COMPLETE > 0`, `FALLBACK ≈ 0`. Plots are written into the run dir.

### 8.5 Inject a fault
```bash
# Choose the blast radius: which hosts get a fault proxy (subset ⇒ poisoned-fraction curves)
FAULTED="agent-1 agent-2 agent-3"

ssh chaos 'sudo bash -c '"'"'
for h in '"$FAULTED"'; do
  scp -o StrictHostKeyChecking=no /root/SwarmAgents/cj_proxy.py $h:/root/SwarmAgents/ >/dev/null 2>&1
  ssh -o StrictHostKeyChecking=no $h "cd /root/SwarmAgents && nohup python3.11 cj_proxy.py \
      --fault latency --delay 3.0 --port 18011 --upstream http://127.0.0.1:11434/v1 \
      --base-url-env OLLAMA_BASE_URL > /var/log/cj_proxy.log 2>&1 &
    grep -q OLLAMA_BASE_URL /root/.profile || echo export OLLAMA_BASE_URL=http://127.0.0.1:18011/v1 >> /root/.profile"
done'"'"''
```
Available faults (`cj_proxy.py`, in this repo):

| `--fault` | Options |
|---|---|
| `latency` | `--delay S` |
| `unavailable` | — (503) |
| `timeout` | `--timeout-s S` |
| `ratelimit` | `--after N` |
| `corrupt` | `--mode truncate\|empty\|invalid_json` |
| `semantic` | `--mode entity_swap\|context_truncate\|inject_distractor\|rag_poison` |

Then run §8.1 → §8.3 into a fault-labelled run dir and compare with §8.4.

**Tear down before the next scenario** — otherwise the fault leaks into later runs:
```bash
ssh chaos 'sudo bash -c '"'"'
for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do
  ssh -o StrictHostKeyChecking=no $h "pkill -f cj_proxy.py; sed -i /OLLAMA_BASE_URL/d /root/.profile" 2>/dev/null &
done; wait; echo faults-cleared'"'"''
```

### 8.6 Regenerating the Pegasus workload (optional)
```bash
# On the Pegasus submit host (successful runs only):
ssh cpegasus 'python3 pegasus_profile_extractor.py --root /home/cc --output ~/all_runs_jobs_profile.json'
# Relay via laptop (submit host and slice are on different networks), then convert:
ssh chaos 'sudo bash -lc "cd /root/SwarmAgents && python3.11 pegasus_to_swarm_converter.py \
  --input all_runs_jobs_profile.json --input-type json --output-dir pegasus_jobs/ \
  --data-nodes per-file --dtn-names dtn1,dtn2,dtn3,dtn4,dtn5,dtn6,dtn7,dtn8"'
```

### 8.7 Teardown / secret hygiene
```bash
# Remove the gateway API key from all hosts when finished:
ssh chaos 'sudo bash -c '"'"'for h in $(cat /root/SwarmAgents/agent_hosts_cj.txt); do ssh -o StrictHostKeyChecking=no $h "sed -i /OPENAI_API_KEY/d /root/.profile" & done; wait; sed -i "/OPENAI_API_KEY/d" /root/.profile'"'"''
```

### 8.8 Helper scripts on the slice
| Script | Purpose |
|---|---|
| `/root/cj_install.sh` | Install the pinned CJ commit |
| `/root/cj_ollama_setup.sh` | Install Ollama, pull model, pre-warm |
| `/root/cj_ollama_fastfix.sh` | Repair a broken Ollama install from a local tarball |
| `/root/cj_infer_check.sh` | **Real** inference health check |
| `/root/SwarmAgents/cj_proxy.py` | Per-host CJ fault proxy driver |

---

## 9. Operational gotchas

Each of these cost real debugging time; all are guarded against above.

- **`/api/tags` is not an Ollama health check.** It only proves a model was *pulled*. It reported
  "30/30 ready" while **15 of 30 hosts could not infer at all** — their installs had been truncated
  by a dropped SSH, leaving no `llama-server` binary, so every call returned HTTP 500 and agents
  silently fell back to analytic cost. This invalidated a full baseline run before it was caught.
  **Always health-check with a real `/api/generate` call.**
- **Don't re-download from the internet when a healthy peer has the bits.** 15 concurrent Ollama
  downloads reached 47% in 35 minutes over the shared IPv6 egress. Packaging the CPU runtime from a
  healthy host (2.1 GB → 42 MB after excluding unused CUDA libs) and copying it internally took
  ~2 minutes.
- **`run_test.py` deletes `agent_hosts.txt`** during cleanup, then crashes reading it. Use a
  different filename.
- **Local-mode runs overwrite `agent_hosts.txt`** with `localhost`. Restore from `/etc/hosts`.
- **`pgrep -f <pattern>` matches its own command line** — a check for "no agents running" can report
  phantom processes. Use a bracket pattern like `mai[n].py`.
- **`timeout_seconds` in the LLM config is not a hard client timeout** — calls of 14.9 s were
  observed without triggering fallback.
- **Agent profiles are random per run unless seeded and reused.** `generate_configs.py` used
  unseeded `random` for capacities, flavors and DTN assignment; a `--seed` flag was added. Seeding
  alone is not enough — `generate_configs` **reuses an existing `agent_dtns.json`** if present,
  taking a different code path and producing different capacities, so always regenerate from a
  clean state.
- **DTN names must match between jobs and agents**, or the connectivity term silently disappears.
  Fixed in `run_test.py` (§2.2); if you call `pegasus_to_swarm_converter.py` directly, pass
  `--dtn-names dtn1,…,dtn10` yourself — its own default is still the historical per-site naming.

---

## 10. Next steps

1. **Phase 1 — Tier 1 sweep** on the Ollama arm. Suggested start: **L1 `LLMLatency`** (clean
   dose-response curve) or **L3 `LLMUnavailable`** (sharpest graceful-degradation signal — should
   drive fallback from 0% to 100% and prove the analytic safety net).
2. **Phase 2 — Tier 2 semantic sweep**, including the poisoned-fraction tolerance curve.
3. **Phase 3 — ablations**, especially fallback-disabled.
4. **Phase 4 — composite X1** and hierarchical/targeted-coordinator scenarios.
5. **Report the CJ issues in §6 upstream.**

**Open question for the team:** which figures are must-haves for the paper?

- **A** — Degradation curves: completion rate & P95 latency vs fault severity
- **B** — Fallback rate by fault type (the graceful-degradation headline)
- **C** — Poisoned-agent tolerance threshold (fairness & conflicts)
- **D** — Default vs fallback-disabled (value of the analytic safety net)
- **E** — Composite "bad day", annotated with the zero-double-assignment invariant
