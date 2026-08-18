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
| 3 | Medium | `upstream` must be an **origin**; a `/v1` base path silently yields 404s, turning a forwarding fault into an outage |
| 4 | **High** | `chaos-jungle stop` always crashes — `ChaosRunner.attach()` skips `__init__`, so a session can never be reverted from the CLI |

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

## 3. `upstream` must be an origin — a base path silently breaks forwarding faults

The proxy **appends the incoming request path** to `upstream`. Passing a base path that already
ends in `/v1` therefore produces `/v1/v1/chat/completions` upstream, and every forwarded call
404s.

**Reproduce** (local Ollama, `LLMLatency(delay_s=3)`):

| `upstream` | result through the proxy |
|---|---|
| `http://127.0.0.1:11434/v1` | **HTTP 404** after 3192 ms |
| `http://127.0.0.1:11434` | **HTTP 200** after 3866 ms ✓ |

The delay is applied in both cases, so the fault *looks* installed; only the response body
reveals that nothing was forwarded.

**Why it matters** — this is silent and it changes what the experiment measures. A latency
experiment becomes an availability experiment, and any conclusion about timeout or retry
behaviour drawn from it would be wrong. It is invisible unless the workload distinguishes
"slow success" from "fast failure": we lost a full 30-agent run to it. It is also easy to hit,
since `/v1` is exactly the base URL an OpenAI-compatible client is configured with, so copying
that value into `upstream` is the natural mistake.

Note the asymmetry that hides it: **`LLMUnavailable` never forwards** (it answers 503 directly),
so outage scenarios pass happily with a misconfigured upstream and only forwarding faults break.

**Suggested fix** — validate `upstream` and reject (or strip) a path component, or document
explicitly that it is an origin. A warning when an upstream response is 404 while the fault
expects a proxied success would also surface it immediately.

## 4. `chaos-jungle stop` always crashes, so a session can never be reverted from the CLI

**Reproduce** — start any fault, then stop it from another process (the documented "separate
mode" that `attach()` exists for):

```bash
chaos-jungle stop
# AttributeError: 'ChaosRunner' object has no attribute '_timer'
#   chaos_jungle/cli.py:137 in stop  ->  runner.stop()
#   chaos_jungle/runner.py:706       ->  if self._timer is not None:
```

**Root cause** — `ChaosRunner.attach()` (runner.py ~1493) reconstructs the runner with
`cls.__new__(cls)`, deliberately bypassing `__init__`, and then sets only six attributes:

```python
runner = cls.__new__(cls)
runner.scenario = Scenario(session["name"], faults=[])
runner.target = target or LocalTarget()
runner.db = db
runner.auto_preflight = False
runner._session_id = session["id"]
runner._fault_ids = []
return runner
```

`__init__` also initialises `_timer`, `_resource_thread` and `_resource_stop` (runner.py ~484),
and `stop()` dereferences `_timer` unconditionally. Every CLI stop therefore raises.

**Impact** — the CLI has no working way to end a chaos session. Faults started with
`chaos-jungle start` must be cleared by killing processes by hand, which is how we ended up
here. Because the session row is only flipped to `reverted` inside `stop()`, killing the process
instead leaves it permanently `running`:

```
  ID  NAME              STATUS      STARTED
   3  swarm-latency     running     2026-08-18T14:35:13Z     <- process long dead
   1  smoke-latency     running     2026-08-16T08:11:54Z     <- two days dead
```

We confirmed nothing was actually active: no listener on the proxy port on any of the 30 hosts.
Note the tell — sessions we stopped with **SIGTERM** (letting the driver's handler call
`runner.stop()`) show `reverted` correctly; only the **SIGKILL**ed ones are stuck, which is
consistent with the revert living solely in `stop()`.

**Secondary issue** — `chaos-jungle list` and `status` report a session's stored status without
checking whether anything is alive, so they confidently show an active fault that is not running.
For an audit trail of destructive experiments, "believed running" and "verified running" are worth
distinguishing.

**Suggested fix** — initialise the missing attributes in `attach()` (or factor the state setup
into a helper both paths call), and reconcile liveness in `list`/`status` — a session whose port
is unbound and whose process is gone should not be displayed as `running`.

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
- *"The LLM proxy cannot forward to authenticated-HTTPS upstreams."* **Withdrawn.** The original
  evidence — `LLMLatency` against the HTTPS gateway delaying correctly but returning no successful
  response — was caused by the `/v1` upstream suffix in issue 3, not by TLS or auth. Retested with
  an origin-only upstream, the proxy forwards to the HTTPS gateway correctly and the upstream's own
  auth error comes back intact; a control call bypassing the proxy produced the identical error,
  confirming our test harness, not CJ, was at fault.

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
