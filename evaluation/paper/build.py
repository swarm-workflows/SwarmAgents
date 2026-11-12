#!/usr/bin/env python3
"""Paper tables and figures from collected campaign trees, in one pass.

    .venv/bin/python evaluation/paper/build.py --manifest evaluation/paper/manifest_frozen.yml

The manifest names every input; nothing is discovered by globbing, so a figure can only use a
run tree someone listed. Keys (all paths relative to the manifest's directory unless absolute):

    out: <dir>                       where cells.csv, validity.md, tables.md and fig/ are written
    sources:                         one entry per collect.py output (runs_wide.csv)
      - name: e1-hier270
        runs_wide: <path>/runs_wide.csv
        campaign: <campaign .yml>    planned jobs per cell come from its `args.jobs`
    e2_events: <timecourse.py events.csv>        (F5)
    e3a_cells: <e3a.py cells.csv>               (F3; a list is allowed)
    t6_per_workflow: <t6_compare.py csv>        (T-6 table; a list is allowed)
    events_summary: [<failure_summary.py csv>, ...]   (E7 under failure, E6)

Rules, the same as every docs/results/*/agg.py:
- a cell is the run directory's parent name; `.attempt<k>` runs are dropped (failed attempts the
  campaign re-ran);
- completion is jobs_completed / the cell's PLANNED jobs from its campaign file, never the jobs a
  run happened to see; a cell with no planned count gets no completion, not a guessed one;
- mean and a 95 % t-interval over repeats; n is printed beside every cell;
- validity is checked per run and printed first (validity.md): run twice, missing metrics,
  barrier-short agents, dropped sends, stranded jobs. A figure is drawn from every run anyway
  and the validity table says which to distrust; nothing is silently filtered.

Figures follow the conference plan §7 (F1 flat ladder + hierarchy since no hierarchical rung
collapsed; F2 mechanism; F3 RTT bins; F4 delegation regret; F5 re-adoption; F6 context error vs
regret) and are written as PDF (for LaTeX) and PNG (for a look). Colour follows the entity, in a
fixed order, with a distinct marker per entity so print and colour-blind readers keep identity.
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import yaml

# Entities in a fixed order: the colour and marker follow the entity, never its rank.
# Palette validated with the dataviz validator (light surface): CVD worst adjacent ΔE 9.1.
ENTITY_STYLE = {
    "pbft": ("#2a78d6", "o", "PBFT"),
    "snow": ("#eb6834", "s", "Snow"),
    "hybrid": ("#1baf7a", "^", "Hybrid"),
    "greedy": ("#eda100", "D", "Greedy"),
    "round-robin": ("#e87ba4", "v", "Round-robin"),
    "random": ("#008300", "P", "Random"),
    "sparrow": ("#6250d6", "X", "Sparrow"),
}
POLICY_ORDER = ["random", "eps", "ucb1", "linucb-shared", "linucb-pergroup", "lints", "linucb-noguard"]
POLICY_LABEL = {"random": "random", "eps": "ε-greedy", "ucb1": "UCB1", "linucb-shared": "LinUCB (shared)",
                "linucb-pergroup": "LinUCB (per-group)", "lints": "LinTS",
                "linucb-noguard": "LinUCB, no guard", "linucb-nodecay": "LinUCB, no decay"}
ENGINES = "pbft|snow|hybrid|greedy|round-robin|random|sparrow"
CELL_RE = re.compile(rf"^(?P<exp>e\d[a-z]*)(?:-(?P<variant>mixshift|workday))?-(?P<topo>hier|mesh)"
                     rf"(?P<agents>\d+)-(?P<engine>{ENGINES})(?:-(?P<rest>.*))?$")

METRICS = [  # (column, label, decimals) for cells.csv / tables.md
    ("completion_pct", "completed %", 1),
    ("jobs_executed_twice", "run twice", 0),
    ("l1_selection_p50", "coordinator decision p50 (s)", 2),
    ("l1_selection_p95", "coordinator decision p95 (s)", 2),
    ("selection_p50", "leaf decision p50 (s)", 2),
    ("sched_latency_p50", "arrival→assigned p50 (s)", 1),
    ("sched_latency_p95", "arrival→assigned p95 (s)", 1),
    ("job_latency_p50", "job latency p50 (s)", 1),
    ("job_latency_p95", "job latency p95 (s)", 1),
    ("makespan_s", "makespan (s)", 0),
    ("throughput_jobs_per_s", "throughput (jobs/s)", 2),
    ("consensus_msgs_per_job", "consensus msgs / job", 0),
    ("jobs_restarted", "jobs restarted", 0),
    ("msgs_dropped", "sends dropped", 0),
    ("regret_mean", "regret (mean)", 3),
    ("regret_scored_share", "regret scored share", 2),
    ("ctx_err_mean", "context error (mean)", 2),
    ("fairness_jain", "Jain (agents)", 3),
]
VALIDITY = [  # (column, rule, why) — a run breaking any rule is listed in validity.md
    ("jobs_executed_twice", lambda v: v > 0, "a job ran twice"),
    ("agents_missing_metrics", lambda v: v > 0, "agents missing metrics"),
    ("barrier_short_agents", lambda v: v > 0, "selection barrier timed out"),
    ("msgs_dropped", lambda v: v > 0, "sends dropped"),
    ("pending_count", lambda v: v > 0, "jobs still pending at stop"),
]


# ---------------------------------------------------------------- loading
def _path(base: Path, p) -> Path:
    p = Path(p)
    return p if p.is_absolute() else (base / p)


def planned_jobs(campaign: Path) -> dict:
    doc = yaml.safe_load(campaign.read_text())
    default = ((doc.get("defaults") or {}).get("args") or {}).get("jobs") or doc.get("jobs")
    out = {}
    for c in doc.get("cells") or []:
        out[c["name"]] = (c.get("args") or {}).get("jobs") or c.get("jobs") or default
    return out


def parse_cell(cell: str) -> dict:
    m = CELL_RE.match(cell)
    if not m:
        return {"exp": "", "topo": "", "agents": np.nan, "engine": "", "plane": "", "arrival": "",
                "variant": ""}
    rest = m["rest"] or ""
    arrival = "burst"
    am = re.search(r"(?:^|-)(poisson\d+)$", rest)
    if am:
        arrival, rest = am[1], rest[: am.start()]
    elif rest.endswith("burst"):
        rest = rest[: -len("burst")].rstrip("-")
    return {"exp": m["exp"], "topo": m["topo"], "agents": int(m["agents"]), "engine": m["engine"],
            "plane": rest or ("" if m["engine"] in ("pbft", "snow", "hybrid") else "baseline"),
            "arrival": arrival, "variant": m["variant"] or ""}


def load_runs(manifest: dict, base: Path) -> pd.DataFrame:
    frames = []
    for src in manifest.get("sources") or []:
        p = _path(base, src["runs_wide"])
        if not p.exists():
            print(f"[build] source {src['name']}: {p} missing, skipped", file=sys.stderr)
            continue
        w = pd.read_csv(p, low_memory=False)
        parts = w["run_dir"].astype(str).str.rstrip("/").str.split("/")
        w["cell"], w["run"] = parts.str[-2], parts.str[-1]
        w = w[~w["run"].str.contains("attempt")].copy()
        w["source"] = src["name"]
        plan = planned_jobs(_path(base, src["campaign"])) if src.get("campaign") else {}
        w["planned_jobs"] = w["cell"].map(plan).astype(float)
        done = pd.to_numeric(w.get("jobs_completed"), errors="coerce")
        # completion against the plan; blank where the plan is unknown (never jobs_seen)
        w["completion_pct"] = np.where(w["planned_jobs"] > 0, 100 * done / w["planned_jobs"], np.nan)
        frames.append(w)
    if not frames:
        raise SystemExit("no source could be read")
    runs = pd.concat(frames, ignore_index=True)
    meta = runs["cell"].map(parse_cell).apply(pd.Series)
    meta["agents"] = pd.to_numeric(meta["agents"], errors="coerce").astype("Int64")
    # the cell name is the truth for its factors; collect.py's path-derived guesses give way
    return pd.concat([runs.drop(columns=[c for c in meta.columns if c in runs]), meta], axis=1)


# ---------------------------------------------------------------- statistics
def ci95(x: pd.Series) -> float:
    x = pd.to_numeric(x, errors="coerce").dropna()
    if len(x) < 2:
        return 0.0
    from scipy import stats  # local: scipy only needed here
    return float(stats.t.ppf(0.975, len(x) - 1) * x.std(ddof=1) / math.sqrt(len(x)))


def cells_table(runs: pd.DataFrame) -> pd.DataFrame:
    rows = []
    keys = ["source", "cell", "exp", "variant", "topo", "agents", "engine", "plane", "arrival"]
    for k, g in runs.groupby(keys, dropna=False, sort=False):
        r = dict(zip(keys, k))
        r["n"] = len(g)
        for col, _, _ in METRICS:
            if col in g:
                v = pd.to_numeric(g[col], errors="coerce").dropna()
                r[col] = v.mean() if len(v) else np.nan
                r[col + "_ci"] = ci95(v) if len(v) else np.nan
                r[col + "_n"] = len(v)
        rows.append(r)
    return pd.DataFrame(rows)


def validity(runs: pd.DataFrame) -> list[str]:
    out = []
    for _, r in runs.iterrows():
        why = []
        for col, bad, text in VALIDITY:
            v = pd.to_numeric(pd.Series([r.get(col)]), errors="coerce").iloc[0]
            if not pd.isna(v) and bad(v):
                why.append(f"{text} ({v:,.0f})")
        if not pd.isna(r.get("completion_pct")) and r["completion_pct"] < 100:
            why.append(f"completed {r['completion_pct']:.1f} %")
        if why:
            out.append(f"| {r['source']} | {r['cell']} | {r['run']} | {'; '.join(why)} |")
    return out


def fmt(m, ci, nd):
    if pd.isna(m):
        return "–"
    return f"{m:.{nd}f}" if not ci or pd.isna(ci) else f"{m:.{nd}f} ± {ci:.{nd}f}"


# ---------------------------------------------------------------- figures
def _plt():
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    plt.rcParams.update({"font.size": 8, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.grid": True, "grid.color": "#e4e3df", "grid.linewidth": 0.6,
                         "axes.edgecolor": "#8a897f", "axes.labelcolor": "#2b2b28",
                         "xtick.color": "#5c5b53", "ytick.color": "#5c5b53",
                         "legend.frameon": False, "lines.linewidth": 2, "lines.markersize": 5,
                         "pdf.fonttype": 42, "ps.fonttype": 42})
    return plt


def _save(fig, out: Path, name: str, written: list):
    (out / "fig").mkdir(parents=True, exist_ok=True)
    for ext in ("pdf", "png"):
        fig.savefig(out / "fig" / f"{name}.{ext}", bbox_inches="tight", dpi=200)
    written.append(name)


def _ladder(ax, cells, metric, ylabel, engines, label_last=False):
    """One line per engine over agent count, mean ± 95 % CI, direct label at the right end."""
    drawn = False
    for e in engines:
        d = cells[cells.engine == e].sort_values("agents")
        d = d[d[metric].notna()] if metric in d else d.iloc[0:0]
        if d.empty:
            continue
        color, marker, label = ENTITY_STYLE[e]
        ax.errorbar(d.agents, d[metric], yerr=d.get(metric + "_ci"), color=color, marker=marker,
                    capsize=2, label=label)
        if label_last:
            ax.annotate(label, (d.agents.iloc[-1], d[metric].iloc[-1]), xytext=(4, 0),
                        textcoords="offset points", va="center", color="#2b2b28", fontsize=7)
        drawn = True
    ax.set_xlabel("agents")
    ax.set_ylabel(ylabel)
    return drawn


def figures(runs, cells, manifest, base, out) -> list[str]:
    plt = _plt()
    written = []
    burst = cells[(cells.arrival == "burst") & (cells.variant == "")]
    flat = burst[(burst.topo == "mesh") & burst.plane.isin(["analytic"])]
    hier = burst[(burst.topo == "hier") & (burst.plane == "analytic") & burst.exp.isin(["e0", "e1"])]

    # F1 — completion: flat ladder and the hierarchy, same y-scale
    if not flat.empty or not hier.empty:
        fig, axes = plt.subplots(1, 2, figsize=(7, 2.6), sharey=True, layout="constrained")
        _ladder(axes[0], flat, "completion_pct", "jobs completed (%)", ["pbft", "snow"])
        axes[0].set_title("flat mesh", fontsize=8)
        _ladder(axes[1], hier, "completion_pct", "", ["pbft", "snow", "hybrid"])
        axes[1].set_title("hierarchy (analytic, burst)", fontsize=8)
        axes[0].set_ylim(0, 105)
        _fig_legend(fig, axes)
        _save(fig, out, "F1_completion", written)
        plt.close(fig)

    # F2 — mechanism: consensus messages per job, and the coordinator tier's decision time
    if not flat.empty or not hier.empty:
        fig, axes = plt.subplots(1, 3, figsize=(9, 2.7), layout="constrained")
        _ladder(axes[0], flat, "consensus_msgs_per_job", "consensus msgs / job", ["pbft", "snow"])
        axes[0].set_title("flat mesh", fontsize=8)
        _ladder(axes[1], hier, "consensus_msgs_per_job", "consensus msgs / job", ["pbft", "snow", "hybrid"])
        axes[1].set_title("hierarchy", fontsize=8)
        _ladder(axes[2], hier, "l1_selection_p95", "coordinator decision p95 (s)", ["pbft", "snow", "hybrid"])
        axes[2].set_title("hierarchy, coordinator tier", fontsize=8)
        for a in axes[1:]:
            a.set_ylim(bottom=0)
        axes[0].set_yscale("log")   # flat PBFT is orders of magnitude above Snow
        _fig_legend(fig, axes)
        _save(fig, out, "F2_mechanism", written)
        plt.close(fig)

    # F3 — coordinator decision time by measured RTT bin (E3a), Hier-90 analytic burst
    e3 = [p for p in _as_list(manifest.get("e3a_cells")) if _path(base, p).exists()]
    if e3:
        c = pd.concat([pd.read_csv(_path(base, p)) for p in e3], ignore_index=True)
        m = c["cell"].map(parse_cell).apply(pd.Series)
        c = pd.concat([c.drop(columns=[x for x in m.columns if x in c]), m], axis=1)
        c = c[(c.plane == "analytic") & (c.arrival == "burst")]
        bins = ["lan", "regional", "continental", "transatlantic"]
        sizes = sorted(c.agents.dropna().unique())
        if sizes:
            fig, axes = plt.subplots(1, len(sizes), figsize=(3.4 * len(sizes), 2.6), squeeze=False,
                                     layout="constrained")
            for ax, n in zip(axes[0], sizes):
                d = c[c.agents == n]
                engines = [e for e in ENTITY_STYLE if e in set(d.engine)]
                width = 0.8 / max(1, len(engines))
                for i, e in enumerate(engines):
                    de = d[d.engine == e].set_index("bin").reindex(bins)
                    color, _, label = ENTITY_STYLE[e]
                    lo = np.minimum(de["l1_p50_ci"], de["l1_p50"])   # an interval never crosses 0
                    ax.bar(np.arange(len(bins)) + i * width, de["l1_p50"], width * 0.92,
                           yerr=[lo, de["l1_p50_ci"]], color=color, label=label, capsize=2)
                ax.set_xticks(np.arange(len(bins)) + width * (len(engines) - 1) / 2,
                              ["LAN", "regional", "contin.", "transatl."], fontsize=7)
                ax.set_ylim(bottom=0)
                ax.set_title(f"Hier-{int(n)}", fontsize=8)
                ax.set_ylabel("coordinator decision p50 (s)")
            _fig_legend(fig, axes[0])
            _save(fig, out, "F3_rtt_bins", written)
            plt.close(fig)

    # F4 — delegation regret by policy, per engine (E2 base cells)
    e2 = cells[(cells.exp == "e2") & (cells.variant == "") & cells.regret_mean.notna()] \
        if "regret_mean" in cells else cells.iloc[0:0]
    if not e2.empty:
        fig, ax = plt.subplots(figsize=(4.6, 2.4))
        pols = [p for p in POLICY_ORDER if p in set(e2.plane)]
        engines = [e for e in ("pbft", "snow", "hybrid") if e in set(e2.engine)]
        width = 0.8 / max(1, len(engines))
        for i, e in enumerate(engines):
            d = e2[e2.engine == e].set_index("plane").reindex(pols)
            color, _, label = ENTITY_STYLE[e]
            ax.bar(np.arange(len(pols)) + i * width, d.regret_mean, width * 0.92,
                   yerr=d.regret_mean_ci, color=color, label=label, capsize=2)
        ax.set_xticks(np.arange(len(pols)) + width * (len(engines) - 1) / 2,
                      [POLICY_LABEL.get(p, p) for p in pols], rotation=25, ha="right", fontsize=7)
        ax.set_ylabel("regret vs oracle (mean)")
        ax.set_ylim(bottom=0)
        ax.legend(loc="upper right")
        _save(fig, out, "F4_regret", written)
        plt.close(fig)

    # F5 — re-adoption of a group after its outage and rejoin (E2b events)
    ev = [p for p in _as_list(manifest.get("e2_events")) if _path(base, p).exists()]
    if ev:
        e = pd.concat([pd.read_csv(_path(base, p)) for p in ev], ignore_index=True)
        e = e[e.event == "outage"].copy()
        if not e.empty:
            m = e["cell"].map(parse_cell).apply(pd.Series)
            e = pd.concat([e.drop(columns=[x for x in m.columns if x in e]), m], axis=1)
            fig, ax = plt.subplots(figsize=(4.6, 2.4))
            arms = [p for p in POLICY_ORDER + ["linucb-nodecay"] if p in set(e.plane)]
            engines = [x for x in ("pbft", "snow", "hybrid") if x in set(e.engine)]
            width = 0.8 / max(1, len(engines))
            for i, en in enumerate(engines):
                color, marker, label = ENTITY_STYLE[en]
                for j, arm in enumerate(arms):
                    v = pd.to_numeric(e[(e.engine == en) & (e.plane == arm)].readopt_s, errors="coerce")
                    x = j + i * width
                    ax.scatter(np.full(v.notna().sum(), x), v.dropna(), color=color, marker=marker,
                               s=18, label=label if j == 0 else None, zorder=3)
                    never = int(v.isna().sum())
                    if never:  # a run that never re-adopted is shown as a count, not a value
                        ax.annotate(f"{never}×never", (x, 0), xytext=(0, -18), textcoords="offset points",
                                    ha="center", fontsize=6, color="#5c5b53")
            ax.set_xticks(np.arange(len(arms)) + width * (len(engines) - 1) / 2,
                          [POLICY_LABEL.get(a, a) for a in arms], fontsize=7)
            ax.set_ylabel("re-adoption after rejoin (s)")
            ax.set_ylim(bottom=0)
            ax.legend(loc="upper right")
            _save(fig, out, "F5_readoption", written)
            plt.close(fig)

    # F6 — context error against regret, per run, by engine (E2 base cells)
    if {"ctx_err_mean", "regret_mean"} <= set(runs.columns):
        r = runs[(runs.exp == "e2") & (runs.variant == "")]
        r = r[pd.to_numeric(r.ctx_err_mean, errors="coerce").notna()
              & pd.to_numeric(r.regret_mean, errors="coerce").notna()]
        if not r.empty:
            fig, ax = plt.subplots(figsize=(3.6, 2.8), layout="constrained")
            for e in ("pbft", "snow", "hybrid"):
                d = r[r.engine == e]
                if d.empty:
                    continue
                color, marker, label = ENTITY_STYLE[e]
                ax.scatter(pd.to_numeric(d.ctx_err_mean), pd.to_numeric(d.regret_mean), color=color,
                           marker=marker, s=20, label=label, edgecolors="white", linewidths=0.8)
            ax.set_xlabel("context error, mean |seen − true| in-flight")
            ax.set_ylabel("regret vs oracle (mean)")
            _fig_legend(fig, [ax])
            _save(fig, out, "F6_context_error", written)
            plt.close(fig)
    return written


def _fig_legend(fig, axes):
    """One legend above the panels, entities in their fixed order, de-duplicated."""
    seen = {}
    for ax in axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            seen.setdefault(l, h)
    order = [lbl for _, _, lbl in ENTITY_STYLE.values() if lbl in seen]
    fig.legend([seen[l] for l in order], order, loc="outside upper center", ncol=len(order))


def _as_list(v):
    return [] if v is None else (v if isinstance(v, list) else [v])


# ---------------------------------------------------------------- tables
def tables_md(runs, cells, manifest, base) -> str:
    L = ["# Paper tables (generated by evaluation/paper/build.py; do not edit)\n"]
    for (src, exp), g in cells.groupby(["source", "exp"], sort=False):
        L.append(f"\n## {src} — {exp or 'other'}\n")
        L.append("| cell | n | " + " | ".join(lbl for c, lbl, _ in METRICS if c in g) + " |")
        L.append("|---" * (2 + sum(1 for c, _, _ in METRICS if c in g)) + "|")
        for _, r in g.iterrows():
            vals = [fmt(r.get(c), r.get(c + "_ci"), nd) for c, _, nd in METRICS if c in g]
            L.append(f"| {r['cell']} | {r['n']} | " + " | ".join(vals) + " |")

    es = [p for p in _as_list(manifest.get("events_summary")) if _path(base, p).exists()]
    if es:
        e = pd.concat([pd.read_csv(_path(base, p)) for p in es], ignore_index=True)
        L.append("\n## Injected events (outage / partition) and the response\n")
        num = ["held_s", "workers_failed", "jobs_requeued", "workers_readmitted", "first_requeue_after_kill_s"]
        L.append("| cell | runs | event | outcomes | " + " | ".join(num) + " |")
        L.append("|---" * (4 + len(num)) + "|")
        for cell, g in e.groupby("cell", sort=False):
            oc = ", ".join(f"{k}×{v}" for k, v in g.outcome.fillna("").value_counts().items() if k)
            vals = []
            for c in num:
                v = pd.to_numeric(g[c], errors="coerce").dropna()
                vals.append("–" if v.empty else fmt(v.mean(), ci95(v), 1))
            L.append(f"| {cell} | {len(g)} | {g.event.iloc[0]} | {oc or '–'} | " + " | ".join(vals) + " |")

    t6 = [p for p in _as_list(manifest.get("t6_per_workflow")) if _path(base, p).exists()]
    if t6:
        t = pd.concat([pd.read_csv(_path(base, p)).assign(set=Path(p).parent.name) for p in t6],
                      ignore_index=True)
        # one row per CELL: PBFT and Snow runs are different engines, never repeats of one another.
        # t6_compare labels a run "cell/run"; the E6 concat carries an explicit cell column.
        lab = t["run"].astype(str).str.split("/").str[0]
        t["cell"] = t["cell"].fillna(lab) if "cell" in t else lab
        L.append("\n## Real workflows against Pegasus (execution excludes staging on both sides)\n")
        L.append("| set | cell | workflow | runs | jobs exit 0 | Pegasus exec critical path (s) | SWARM exec critical path (s) "
                 "| median exec ratio, adjusted |")
        L.append("|---|---|---|---|---|---|---|---|")
        for (st, cell, wf), g in t.groupby(["set", "cell", "workflow"], sort=False):
            cp = pd.to_numeric(g.swarm_exec_critical_path_s, errors="coerce").dropna()
            ra = pd.to_numeric(g.get("exec_ratio_adj_median"), errors="coerce").dropna()
            L.append(f"| {st} | {cell} | {wf} | {len(g)} | {int(g.swarm_exit0.sum())}/{int(g.jobs.sum())} "
                     f"| {g.peg_exec_critical_path_s.iloc[0]} | {fmt(cp.mean(), ci95(cp), 1)} "
                     f"| {fmt(ra.mean(), ci95(ra), 2)} |")
    return "\n".join(L) + "\n"


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--manifest", required=True, type=Path)
    ap.add_argument("--no-figures", action="store_true")
    a = ap.parse_args(argv)
    manifest = yaml.safe_load(a.manifest.read_text())
    base = a.manifest.resolve().parent
    out = _path(base, manifest["out"])
    out.mkdir(parents=True, exist_ok=True)

    runs = load_runs(manifest, base)
    cells = cells_table(runs)
    cells.to_csv(out / "cells.csv", index=False)
    bad = validity(runs)
    (out / "validity.md").write_text(
        "# Validity, per run (any listed run needs reading before its cell is quoted)\n\n"
        f"{len(runs)} runs in {cells.shape[0]} cells; {len(bad)} runs flagged.\n\n"
        "| source | cell | run | flags |\n|---|---|---|---|\n" + "\n".join(bad) + "\n")
    (out / "tables.md").write_text(tables_md(runs, cells, manifest, base))
    written = [] if a.no_figures else figures(runs, cells, manifest, base, out)
    unplanned = sorted(set(runs.loc[runs.planned_jobs.isna(), "cell"]))
    print(f"[build] {len(runs)} runs, {cells.shape[0]} cells, {len(bad)} runs flagged; "
          f"figures: {', '.join(written) or 'none'} -> {out}")
    if unplanned:
        print(f"[build] no planned job count (completion left blank): {', '.join(unplanned)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
