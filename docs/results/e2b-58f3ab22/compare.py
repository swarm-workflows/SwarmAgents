"""E2b at 58f3ab22 (gate on the detector window) against E2b at c9c980b8 (gate on peer expiry).

    python compare.py   # run from this directory; prints the tables in README.md
"""
import numpy as np
import pandas as pd
import scipy.stats as ss

OLD, NEW = "../e2-c9c980b8", "."
ARMS = [f"e2b-hier90-{e}-{a}" for a in ["linucb-shared", "linucb-nodecay", "eps"] for e in ["pbft", "snow"]]


def load(d):
    w = pd.read_csv(f"{d}/runs_wide.csv")
    w["cell"] = w["run_dir"].str.rstrip("/").str.split("/").str[-2]
    w["run"] = w["run_dir"].str.rstrip("/").str.split("/").str[-1]
    w = w[w.cell.isin(ARMS) & ~w.run.str.contains("attempt")].copy()
    w["completion_pct"] = 100 * w["jobs_executed"] / 1800
    e = pd.read_csv(f"{d}/events.csv")
    e = e[e.cell.isin(ARMS)]
    df = w.merge(e, on=["cell", "run"], how="left")
    assert len(df) == 30 and (df.groupby("cell").size() == 5).all()
    return df


def fmt(v):
    v = pd.to_numeric(v, errors="coerce").dropna()
    if len(v) == 0:
        return "–"
    m = v.mean()
    if len(v) < 2 or v.std(ddof=1) == 0:
        return f"{m:.3g}" + ("" if len(v) == 5 else f" (n={len(v)})")
    h = ss.t.ppf(0.975, len(v) - 1) * v.std(ddof=1) / np.sqrt(len(v))
    return f"{m:.3g} ± {h:.2g}" + ("" if len(v) == 5 else f" (n={len(v)})")


old, new = load(OLD), load(NEW)
COLS = [("to_group_down", "to dead group while down"), ("dec_down", "coord. 89 decisions while down"),
        ("readopt_s", "first delegation after rejoin (s)"), ("to_group_after", "to group 3 after rejoin"),
        ("dec_after", "decisions after rejoin"), ("regret_mean", "regret"),
        ("routing_accuracy", "accuracy"), ("regret_scored_share", "scored share"),
        ("success_pct_of_seen", "success %"), ("completion_pct", "completion % (lower bound)"),
        ("job_latency_p95", "job latency p95 (s)"), ("jobs_executed_twice", "jobs twice"),
        ("restarts_total", "restarts")]
for col, label in COLS:
    print(f"\n### {label}\n\n| cell | c9c980b8 | 58f3ab22 |\n|---|---|---|")
    for c in ARMS:
        print(f"| {c.replace('e2b-hier90-', '')} | {fmt(old[old.cell == c][col])} | "
              f"{fmt(new[new.cell == c][col])} |")
