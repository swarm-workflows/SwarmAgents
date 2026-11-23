"""F3 (coordinator decision time by RTT bin) as a single-column figure: Hier-90 and Hier-270 only.

    .venv/bin/python evaluation/paper/fig_f3_compact.py runs/paper-frozen <out-dir>

Same data and aggregation as build.py's F3 (e3a.py per_run.csv, mean ± sd over runs, analytic
delegation, burst arrivals). The Hier-30 panel (PBFT only) is left out to fit one column.
"""
import sys
from pathlib import Path
import numpy as np, pandas as pd
sys.path.insert(0, str(Path(__file__).resolve().parent))
import build as B

base, out = Path(sys.argv[1]), Path(sys.argv[2])
pr = pd.concat([pd.read_csv(p) for p in sorted((base / "e3a").glob("*.per_run.csv"))], ignore_index=True)
c = pr.groupby(["cell", "bin"])["l1_p50"].agg(l1_p50="mean", l1_p50_sd=lambda v: B.sd(v)).reset_index()
m = c["cell"].map(B.parse_cell).apply(pd.Series)
c = pd.concat([c.drop(columns=[x for x in m.columns if x in c]), m], axis=1)
c = c[(c.plane == "analytic") & (c.arrival == "burst") & c.agents.isin([90, 270])]
bins = ["lan", "regional", "continental", "transatlantic"]
plt = B._plt()
fig, axes = plt.subplots(1, 2, figsize=(4.6, 2.0), layout="constrained")
for ax, n in zip(axes, [90, 270]):
    d = c[c.agents == n]
    engines = [e for e in B.ENTITY_STYLE if e in set(d.engine)]
    width = 0.8 / len(engines)
    for i, e in enumerate(engines):
        de = d[d.engine == e].set_index("bin").reindex(bins)
        color, _, label = B.ENTITY_STYLE[e]
        lo = np.minimum(de["l1_p50_sd"], de["l1_p50"])
        ax.bar(np.arange(4) + i * width, de["l1_p50"], width * 0.92, yerr=[lo, de["l1_p50_sd"]],
               color=color, label=label, capsize=1.5)
        print(n, e, [round(float(x), 2) for x in de["l1_p50"]])
    ax.set_xticks(np.arange(4) + width * (len(engines) - 1) / 2, ["LAN", "region.", "contin.", "transatl."], fontsize=6)
    ax.set_ylim(bottom=0); ax.set_title(f"Hier-{n}", fontsize=8)
axes[0].set_ylabel("coord. decision p50 (s)")
B._fig_legend(fig, axes)
written = []; B._save(fig, out, "F3_rtt_bins_compact", written); plt.close(fig)
