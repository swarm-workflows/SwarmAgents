"""E2 by-cell aggregate: python agg.py <dir holding runs_wide.csv, events.csv, workday.csv>.

Groups by CELL (the run directory's parent), never by collect.py's engine/policy factors, which
merge cells that differ in profile phase, outage, workload or guard. `.attempt<k>` runs are
dropped: they are failed attempts the campaign re-ran. Writes cells_agg.csv, events_agg.csv,
workday_agg.csv and prints tables.md to stdout."""
import sys

import numpy as np
import pandas as pd
import scipy.stats as ss

D = sys.argv[1]
w = pd.read_csv(f"{D}/runs_wide.csv")
w["cell"] = w["run_dir"].str.rstrip("/").str.split("/").str[-2]
w["run"] = w["run_dir"].str.rstrip("/").str.split("/").str[-1]
w = w[~w["run"].str.contains("attempt")]
# collect.py cannot see the planned job count in these paths (completion_basis "unknown"), so
# completion is executed / planned here: 8000 for the work day, 1800 for every other cell.
# E2b is a lower bound: a killed agent's first-life executions die with it.
w["planned"] = np.where(w["cell"].str.startswith("e2c-workday"), 8000, 1800)
w["completion_pct"] = 100 * w["jobs_executed"] / w["planned"]
w["success_pct"] = w["success_pct_of_seen"]
assert len(w) == 200 and (w.groupby("cell").size() == 5).all(), w.groupby("cell").size()

ENG = ["pbft", "snow"]
ARMS = ["linucb-shared", "linucb-pergroup", "lints", "ucb1", "eps", "random", "linucb-noguard"]
ORDER = ([f"e2-hier90-{e}-{a}" for a in ARMS for e in ENG]
         + [f"e2a-hier90-{e}-{a}" for a in ["linucb-d100", "linucb-d098", "eps-s0", "eps-s002"] for e in ENG]
         + [f"e2b-hier90-{e}-{a}" for a in ["linucb-shared", "linucb-nodecay", "eps"] for e in ENG]
         + [f"e2c-{k}-hier90-{e}-{a}" for k in ["workday", "mixshift"]
            for a in ["linucb-shared", "linucb-pergroup", "eps"] for e in ENG])
assert set(ORDER) == set(w.cell), set(ORDER) ^ set(w.cell)


def ci(v):
    v = pd.to_numeric(v, errors="coerce").dropna()
    if len(v) == 0:
        return np.nan, np.nan, 0
    h = ss.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / np.sqrt(len(v)) if len(v) > 1 else np.nan
    return v.mean(), h, len(v)


def agg(df, key, metrics, order):
    rows = []
    for k, g in df.groupby(key):
        r = {key: k, "n": len(g)}
        for m in metrics:
            if m in g.columns:
                r[m], r[m + "_ci"], r[m + "_n"] = ci(g[m])
        rows.append(r)
    out = pd.DataFrame(rows)
    out[key] = pd.Categorical(out[key], [c for c in order if c in set(out[key])], ordered=True)
    return out.sort_values(key).reset_index(drop=True)


def table(a, key, cols, fmt="{:.3f}", short=lambda c: c):
    head = "| " + " | ".join([key] + cols) + " |"
    lines = [head, "|" + "---|" * (len(cols) + 1)]
    for _, r in a.iterrows():
        cells = []
        for c in cols:
            m, h = r.get(c), r.get(c + "_ci")
            if m is None or pd.isna(m):
                cells.append("–")
            else:
                cells.append(fmt.format(m) + (f" ± {fmt.format(h)}" if pd.notna(h) and h > 0 else ""))
        lines.append("| " + " | ".join([short(str(r[key]))] + cells) + " |")
    return "\n".join(lines)


VALID = ["completion_pct", "success_pct", "jobs_executed_twice", "restarts_total",
         "regret_scored_share", "regret_profile_validated", "regret_profile_mae",
         "regret_types_beyond_noise", "metrics_complete"]
QUALITY = ["regret_mean", "routing_accuracy", "regret_guard_mean", "regret_policy_mean",
           "decisions_narrowed", "decisions_guard_excluded_best", "regret_decisions_with_a_choice"]
COST = ["job_latency_p50", "job_latency_p95", "l1_selection_p50", "throughput_jobs_per_s",
        "makespan_s", "ctx_err_mean"]
cells = agg(w, "cell", VALID + QUALITY + COST, ORDER)
cells.to_csv(f"{D}/cells_agg.csv", index=False)
strip = lambda c: c.replace("-hier90-", " ").replace("e2c-", "e2c ")

ev = pd.read_csv(f"{D}/events.csv")
EVM = ["pre_acc", "post_acc", "pre_regret", "post_regret", "recover_s", "event_at_s",
       "to_group_before", "dec_before", "to_group_down", "dec_down", "to_group_after", "dec_after",
       "readopt_s", "held_s"]
evs = agg(ev, "cell", EVM, ORDER)
# recover_s is absent when a run never recovered within the run: count those beside the mean.
evs["never_recovered"] = [int(ev[ev.cell == c]["recover_s"].isna().sum()) if "recover_s" in ev else 0
                          for c in evs["cell"]]
evs.to_csv(f"{D}/events_agg.csv", index=False)

wd = pd.read_csv(f"{D}/workday.csv")
wd["key"] = wd["cell"] + "#" + wd["segment"].astype(str)
wda = agg(wd, "key", ["accuracy", "regret_mean", "decisions"],
          [f"{c}#{i}" for c in ORDER for i in range(8)])
wda.to_csv(f"{D}/workday_agg.csv", index=False)

print("## Validity\n")
print(table(cells, "cell", ["completion_pct", "success_pct", "jobs_executed_twice", "restarts_total",
                            "regret_scored_share", "regret_profile_validated", "regret_profile_mae"],
            short=strip))
print("\n## Delegation quality (decisions with a choice)\n")
print(table(cells, "cell", ["regret_mean", "routing_accuracy", "regret_guard_mean",
                            "regret_policy_mean", "decisions_narrowed",
                            "decisions_guard_excluded_best"], short=strip))
print("\n## Cost\n")
print(table(cells, "cell", ["job_latency_p50", "job_latency_p95", "l1_selection_p50",
                            "throughput_jobs_per_s", "ctx_err_mean"],
            fmt="{:.2f}", short=strip))
print("\n## E2a flip and E2c job-mix shift (120 s before, 300 s after)\n")
fl = evs[evs["cell"].astype(str).str.startswith(("e2a", "e2c-mixshift"))]
print(table(fl, "cell", ["pre_acc", "post_acc", "pre_regret", "post_regret", "recover_s"],
            short=strip))
print("\n" + ", ".join(f"{strip(str(c))}: {n}/5 never recovered" for c, n in
                       zip(fl["cell"], fl["never_recovered"]) if n))
print("\n## E2b outage (group 3, coordinator 89)\n")
ob = evs[evs["cell"].astype(str).str.startswith("e2b")]
print(table(ob, "cell", ["to_group_before", "dec_before", "to_group_down", "dec_down",
                         "to_group_after", "dec_after", "readopt_s", "held_s"], fmt="{:.1f}",
            short=strip))
print("\n## E2c work day, accuracy by arrival segment\n")
wda["cell"] = wda["key"].astype(str).str.split("#").str[0]
wda["seg"] = wda["key"].astype(str).str.split("#").str[1].astype(int)
rates = wd.drop_duplicates("segment").set_index("segment")["rate"]
piv = wda.pivot(index="cell", columns="seg", values="accuracy")
piv = piv.loc[[c for c in ORDER if c in piv.index]]
print("| cell | " + " | ".join(f"seg{i} ({rates[i]:.2f}/s)" for i in piv.columns) + " |")
print("|" + "---|" * (len(piv.columns) + 1))
for c, r in piv.iterrows():
    print(f"| {strip(c)} | " + " | ".join(f"{v:.3f}" for v in r) + " |")
