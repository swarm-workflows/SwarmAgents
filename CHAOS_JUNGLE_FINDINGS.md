# Chaos Jungle — findings from an external evaluation

Issues found while deploying **Chaos Jungle v0.1.0** across a 30-node FABRIC testbed to
fault-inject the LLM scheduling agents of [SwarmAgents](https://github.com/swarm-workflows/SwarmAgents).
Written to be filed upstream (`swarmourr/CJ`); each item lists a reproduction, the evidence,
and the workaround we used.

**Context:** ~30 agent VMs (8-core, no GPU), each running a local Ollama (`qwen2.5:3b`) with a
per-host CJ fault proxy in front of it, plus a shared LiteLLM gateway over HTTPS.
Checked against `main` as of **2026-08-17**.

**Bottom line: CJ works as advertised once installed from the right commit.** We reproduced a
clean baseline-vs-fault delta with `ChaosRunner.measure()` (1.02 s → 3.08 s under
`LLMLatency(delay_s=3)`) and ran a full-fleet `LLMUnavailable` scenario end to end. The issues
below are about *getting to* that point.

| # | Severity | Summary |
|---|----------|---------|
| 1 | **Blocker** | `main` / v1.5.0 cannot be imported at all — two undefined re-exports |
| 2 | High | Documented `pip install chaos-jungle` cannot work — package is not on PyPI |
| 3 | High | The LLM proxy does not forward to authenticated-HTTPS upstreams; a latency fault silently becomes an outage |

---

## 1. `import chaos_jungle` fails on `main` (blocker)

`chaos_jungle/__init__.py` imports and re-exports two names that are **defined nowhere in the
repository**, so the package cannot be imported at all:

| Name | Imported from | Status in that module |
|------|---------------|-----------------------|
| `InjectResult` | `chaos_jungle.intercept` (line ~120, re-exported ~260) | not defined — module defines `inject`, `door`, `Behavior`, … |
| `ChaosFuzzer` | `chaos_jungle.fuzzing` (line ~136) | not defined — module defines only `fuzz_scenarios`, `summarise_fuzz` |

**Reproduce**
```bash
pip install git+https://github.com/swarmourr/CJ.git
python -c "import chaos_jungle"
# ImportError: cannot import name 'InjectResult' from 'chaos_jungle.intercept'
# after shimming that: cannot import name 'ChaosFuzzer' from 'chaos_jungle.fuzzing'
```

**Scope** — we checked every name `__init__.py` imports across all 19 modules; exactly these
two are undefined. So the fix is small.

**When it broke** — introduced by `21765afb` (2026-07-06, *"feat: add Hypothesis, ChaosScheduler,
and observability exporters (v1.5.0)"*). Every commit after it is docs-only, so `main` has been
unimportable since that date.

**Workaround** — pin the commit immediately before it:
```bash
pip install "git+https://github.com/swarmourr/CJ.git@5044939950f3ac020c6fe02073642295865cbeae"
```
Verified at that commit: imports cleanly, `LLMLatency`/`LLMUnavailable`/`LLMRateLimit`/
`LLMResponseCorrupt`/`LLMTimeout`/`SemanticCorrupt` all instantiate, and `ChaosRunner` exposes
`.measure()` / `.start()` / `.stop()`.

**Suggested fix** — either restore the two symbols or drop them from `__init__.py`'s import and
`__all__`. A smoke test that merely does `import chaos_jungle` would have caught this.

---

## 2. Documented install command cannot work

The docs site instructs, as the **first** option on both the landing page and Quickstart:

```bash
pip install chaos-jungle          # also: "chaos-jungle[docs]", "chaos-jungle[dev]"
```

but the package is not published: `https://pypi.org/pypi/chaos-jungle/json` → **404**, and pip
reports `Could not find a version that satisfies the requirement chaos-jungle (from versions: none)`.

The repository `README.md` is correct — it lists only the `git+https://…` form. The mismatch is
confined to the documentation site.

**Suggested fix** — either publish to PyPI or drop the `pip install chaos-jungle` lines from
`index` and `quickstart` (and the `[docs]`/`[dev]` extras, which have the same problem).

---

## 3. LLM proxy does not forward to authenticated-HTTPS upstreams

Against an HTTPS upstream requiring a `Authorization: Bearer …` header (a LiteLLM gateway),
`LLMLatency` **applied its delay but the forwarded request failed**, so the workload saw an
outage rather than added latency.

**Evidence** — `ChaosRunner.measure()` around a real client call:

| | latency | success |
|---|---|---|
| baseline | 1.02 s | 1.0 |
| fault (`LLMLatency(delay_s=3)`) | 3.08 s | **0.0** |

The +2.06 s delta shows the proxy is intercepting correctly; the `success` collapse shows the
upstream leg failing. The same fault against a **plain-HTTP** upstream (local Ollama) behaves
correctly, which is consistent with the documented examples all using Ollama.

**Why it matters** — this silently changes what is being measured. A latency experiment becomes
an availability experiment, and any conclusion drawn about timeout/retry behaviour would be
wrong. It is also invisible unless the workload reports success separately from latency.

**Suggested fix** — forward the inbound `Authorization` header (and any other client headers) and
support TLS to the upstream; failing that, document the plain-HTTP-only limitation prominently,
since "point CJ at your provider" is the natural reading of the current docs.

**Workaround** — we run all fault injection against local plain-HTTP Ollama endpoints and use the
HTTPS gateway only for fault-free runs.

---

## Corrections to earlier drafts of this list

Two items we initially recorded turned out to be **wrong on checking the source**, and are
withdrawn — noted here so they are not re-filed:

- *"`LLMRateLimit`/`LLMTimeout` parameter names drift from the docs."* False. The library takes
  `LLMRateLimit(n=…)` and `LLMTimeout(timeout_s=…)`, and `LLM_SCENARIOS.md` uses exactly those.
  The confusion was ours: `chaos_jungle.intercept.RateLimit` is a *different* class taking
  `after_n`, and the guide uses it correctly too. At most this is a naming-similarity nit
  (`RateLimit` vs `LLMRateLimit`), not a defect.
- *"The Quickstart docs link is broken."* False. `quickstart.html` is at the site root and
  returns 200; we had guessed a `guides/` prefix.

---

## Suggestion, not a defect: `LLM_SCENARIOS.md` is the most useful artefact

The scenario catalogue (S01–S11, R01–R10) with its **What / Why / How / Results** shape and the
*healthy vs problem* signal tables is excellent, and we adopted both the structure and the
numbering for our own experiments so results cross-reference directly.

The signal tables in particular have diagnostic value the API alone does not: S01's
*"delta near 0 → proxy not intercepting"* is exactly the check that would have surfaced issue 3
above on the first run. Promoting that convention into the API — for example, having
`measure()` warn when a fault produces no measurable delta — would make silent
non-injection much harder to miss.

One adaptation worth flagging for anyone applying the catalogue to a distributed system: CJ's
scenarios use a single in-process LLM call as the workload, which takes seconds. Ours is a
30-agent scheduling run over a fixed job trace, so each scenario takes ~11 minutes and
`measure(n_baseline=5, n_fault=5)` is a multi-hour job. The framework handles this fine; it
just changes `run_all.py` from an interactive tool into a batch one.
