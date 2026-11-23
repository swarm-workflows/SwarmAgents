"""F1 and F2 with the review-ablation points overlaid (hollow markers).

    .venv/bin/python evaluation/paper/fig_ablation.py runs/paper-frozen <out-dir>

Reads runs/paper-frozen/cells.csv (the frozen campaign, as build.py wrote it) and
runs/paper-frozen/ablation/{capA,poolB}_runs_wide.csv (collect.py output for
campaign-review-ablations-d3755e2b). The frozen lines are drawn exactly as build.py draws them.
Added points: Snow with consensus.snow.max_inflight 64 at Hier-270, and PBFT with the 256/8192
send pool at Mesh-180. Both are 3 runs, mean +- sd.
"""
import sys
from pathlib import Path
import pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build as B

base, out = Path(sys.argv[1]), Path(sys.argv[2])
cells = pd.read_csv(base / "cells.csv").fillna({"variant": ""})
A = pd.read_csv(base / "ablation" / "capA_runs_wide.csv")
P = pd.read_csv(base / "ablation" / "poolB_runs_wide.csv")
snow64 = A[A.run_dir.str.startswith("revA-hier270-snow-analytic-mi64/")]
assert len(snow64) == 3 and len(P) == 3
ms = lambda d, c: (d[c].mean(), d[c].std(ddof=1))

plt = B._plt()
burst = cells[(cells.arrival == "burst") & (cells.variant == "")]
flat = burst[(burst.topo == "mesh") & burst.plane.isin(["analytic"])]
hier = burst[(burst.topo == "hier") & (burst.plane == "analytic") & burst.exp.isin(["e0", "e1"])]
SNOW, PBFT = B.ENTITY_STYLE["snow"][0], B.ENTITY_STYLE["pbft"][0]

def hollow(ax, x, d, col, color, marker, label):
    m, s = ms(d, col)
    return ax.errorbar([x], [m], yerr=[s], color=color, marker=marker, mfc="white", ls="none",
                       capsize=2, label=label, zorder=5)

def legend(fig, axes):
    seen = {}
    for ax in axes:
        for h, l in zip(*ax.get_legend_handles_labels()):
            seen.setdefault(l, h)
    order = [l for _, _, l in B.ENTITY_STYLE.values() if l in seen] + \
            [l for l in seen if l not in {x[2] for x in B.ENTITY_STYLE.values()}]
    fig.legend([seen[l] for l in order], order, loc="outside upper center", ncol=len(order),
               columnspacing=1.0, handletextpad=0.4)

L64, LPOOL = "Snow, limit 64", "PBFT, 4× send pool"
written = []
fig, axes = plt.subplots(1, 2, figsize=(7, 2.6), layout="constrained")
B._ladder(axes[0], flat, "completion_pct", "jobs completed (%)", ["pbft", "snow"])
hollow(axes[0], 180, P, "completion_pct_of_seen", PBFT, "o", LPOOL)
axes[0].set_title("flat mesh: completion", fontsize=8); axes[0].set_ylim(0, 105)
B._ladder(axes[1], hier, "sched_latency_p95", "arrival→assigned p95 (s)", ["pbft", "snow", "hybrid"])
hollow(axes[1], 270, snow64, "sched_latency_p95", SNOW, "s", L64)
axes[1].set_title("hierarchy: scheduling latency (all 35 runs complete)", fontsize=8)   # as build.py prints it for the frozen runs
axes[1].set_ylim(bottom=0)
legend(fig, axes); B._save(fig, out, "F1_completion", written); plt.close(fig)

fig, axes = plt.subplots(1, 3, figsize=(9, 2.7), layout="constrained")
B._ladder(axes[0], flat, "consensus_msgs_per_job", "consensus msgs / job", ["pbft", "snow"])
axes[0].set_title("flat mesh", fontsize=8)
B._ladder(axes[1], hier, "consensus_msgs_per_job", "consensus msgs / job", ["pbft", "snow", "hybrid"])
hollow(axes[1], 270, snow64, "consensus_msgs_per_job", SNOW, "s", L64)
axes[1].set_title("hierarchy", fontsize=8)
B._ladder(axes[2], hier, "l1_selection_p95", "coordinator decision p95 (s)", ["pbft", "snow", "hybrid"])
hollow(axes[2], 270, snow64, "l1_selection_p95", SNOW, "s", L64)
axes[2].set_title("hierarchy, coordinator tier", fontsize=8)
for a in axes[1:]:
    a.set_ylim(bottom=0)
axes[0].set_yscale("log")
legend(fig, axes); B._save(fig, out, "F2_mechanism", written); plt.close(fig)
for n, (m, s) in {"snow64 sched p95": ms(snow64, "sched_latency_p95"), "snow64 msgs/job": ms(snow64, "consensus_msgs_per_job"),
                  "snow64 l1 p95": ms(snow64, "l1_selection_p95"), "pbft pool completion": ms(P, "completion_pct_of_seen")}.items():
    print(f"{n}: {m:.2f} ± {s:.2f}")
print("wrote", written)
