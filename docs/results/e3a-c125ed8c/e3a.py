"""E3a: selection time by measured RTT bin, per cell (conference plan §4.2 E3a).

    python3.11 e3a.py <job_rtt.csv> <out-dir>

Input is collect.py's job_rtt.csv: one row per job with the RTT between the host of the agent
that executed it and the host of the coordinator that delegated it (evaluation/rtt.py; bins
lan < 5 <= regional < 40 <= continental < 90 <= transatlantic, ms). Per the plan the unit is the
job and the analysis is per run: each quantile is taken within one (cell, run, bin), then
averaged over the cell's runs with a 95 % t-interval. The coordinator tier (`l1_selection_s`)
and the leaf tier (`selection_s`) are reported separately, never summed. Unattributed jobs (no
RTT: unknown executor or coordinator, or an unmeasured pair) are counted, never binned.

Writes per_run.csv (cell, run, bin) and cells.csv (cell, bin) and prints tables.md.
"""
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import scipy.stats as ss

BINS = ["lan", "regional", "continental", "transatlantic"]

src, out = sys.argv[1], Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
df = pd.read_csv(src, low_memory=False)
df["cell"] = df["run_dir"].str.split("/").str[0]
df["runname"] = df["run_dir"].str.split("/").str[1]
df = df[~df["runname"].str.contains("attempt")]
assert set(df["rtt_kind"].dropna()) <= {"coordinator"}, "flat runs are a different quantity"

rows = []
for (cell, run), g in df.groupby(["cell", "runname"]):
    attributed = g[g["rtt_bin"].notna()]
    base = {"cell": cell, "run": run, "jobs": len(g), "unattributed": len(g) - len(attributed)}
    for b in BINS:
        x = attributed[attributed["rtt_bin"] == b]
        r = {**base, "bin": b, "n": len(x), "share": len(x) / len(attributed) if len(attributed) else np.nan,
             "rtt_ms_median": x["rtt_ms"].median() if len(x) else np.nan,
             "completed_share": x["completed"].astype(bool).mean() if len(x) else np.nan}
        for col, name in (("l1_selection_s", "l1"), ("selection_s", "leaf")):
            v = pd.to_numeric(x[col], errors="coerce").dropna()
            r[f"{name}_p50"] = v.quantile(0.5) if len(v) else np.nan
            r[f"{name}_p95"] = v.quantile(0.95) if len(v) else np.nan
        rows.append(r)
per_run = pd.DataFrame(rows)
per_run.to_csv(out / "per_run.csv", index=False)

METRICS = ["n", "share", "rtt_ms_median", "completed_share", "l1_p50", "l1_p95", "leaf_p50", "leaf_p95"]
agg = []
for (cell, b), g in per_run.groupby(["cell", "bin"]):
    r = {"cell": cell, "bin": b, "runs": len(g), "runs_with_jobs": int((g["n"] > 0).sum()),
         "unattributed_total": int(g["unattributed"].sum())}
    for m in METRICS:
        v = g[m].dropna()
        r[m] = v.mean() if len(v) else np.nan
        r[m + "_ci"] = (ss.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / np.sqrt(len(v))
                        if len(v) > 1 else np.nan)
    agg.append(r)
cells = pd.DataFrame(agg)
cells["bin"] = pd.Categorical(cells["bin"], BINS, ordered=True)
cells = cells.sort_values(["cell", "bin"])
cells.to_csv(out / "cells.csv", index=False)


def f(m, h, d=2):
    if pd.isna(m):
        return "–"
    return f"{m:.{d}f}" + (f" ± {h:.{d}f}" if pd.notna(h) and h > 0 else "")


print("| cell | bin | jobs/run | share | RTT ms | coord p50 (s) | coord p95 (s) | leaf p50 (s) | leaf p95 (s) | completed |")
print("|---|---|---|---|---|---|---|---|---|---|")
for _, r in cells.iterrows():
    if r["runs_with_jobs"] == 0:
        continue
    print(f"| {r['cell']} | {r['bin']} | {r['n']:.0f} | {r['share']:.2f} | {r['rtt_ms_median']:.0f} | "
          f"{f(r['l1_p50'], r['l1_p50_ci'])} | {f(r['l1_p95'], r['l1_p95_ci'])} | "
          f"{f(r['leaf_p50'], r['leaf_p50_ci'], 3)} | {f(r['leaf_p95'], r['leaf_p95_ci'], 3)} | "
          f"{r['completed_share']:.3f} |")
print(f"\nunattributed jobs, all runs: {int(per_run.drop_duplicates(['cell','run'])['unattributed'].sum())} "
      f"of {int(per_run.drop_duplicates(['cell','run'])['jobs'].sum())}")
