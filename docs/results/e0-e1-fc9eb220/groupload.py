"""Reconstruct each Hier-90 G=1 group's load over time as its coordinator would aggregate it:
sum of children's allocations over sum of capacities, approximated per leaf as load_i x core_i."""
import json, sys, collections
d = sys.argv[1]
m = json.load(open(d + "/metrics.json")); p = json.load(open(d + "/agent_profiles.json"))
for c in range(82, 91):
    leaves = range(1 + 9 * (c - 82), 10 + 9 * (c - 82))
    caps = sum(p[str(a)]["core"] for a in leaves)
    bins = collections.defaultdict(float)
    for a in leaves:
        for t, load in (m[str(a)].get("load_trace") or []):
            bins[int(t)] = max(bins[int(t)], 0)  # ensure key
    # per-second: take each leaf's last sample in that second
    per = collections.defaultdict(dict)
    for a in leaves:
        for t, load in (m[str(a)].get("load_trace") or []):
            per[int(t)][a] = load
    series, last = [], {}
    for t in sorted(per):
        last.update(per[t])
        series.append(sum(last.get(a, 0) * p[str(a)]["core"] for a in leaves) / caps)
    mx = max(series) if series else 0; mean = sum(series) / len(series) if series else 0
    busy = sorted(series)[int(0.95 * (len(series) - 1))] if series else 0
    print(f"coord {c}: group load mean {mean:5.1f}%  p95 {busy:5.1f}%  max {mx:5.1f}%  -> load factor at max {1 + (mx/100)**1.5:.2f}")
