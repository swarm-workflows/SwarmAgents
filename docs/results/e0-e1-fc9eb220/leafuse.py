import json, sys, collections
R = "/root/SwarmAgents/runs/campaign-e0-e1-fc9eb220/"
for cell, nleaf in [("e0-hier90-pbft-analytic", 81), ("e1-hier90-snow-analytic", 81), ("e1-hier90-hybrid-analytic", 81),
                    ("e0-hier90-pbft-analytic-poisson3", 81), ("e1-hier90-snow-analytic-poisson3", 81), ("e1-hier90-hybrid-analytic-poisson3", 81),
                    ("e1-hier90-pbft-linucb-shared-poisson3", 87), ("e1-hier90-snow-linucb-shared-poisson3", 87)]:
    idle_runs, idle_cores = [], []
    for r in range(1, 6):
        d = R + cell + "/run0%d/" % r
        m = json.load(open(d + "metrics.json")); p = json.load(open(d + "agent_profiles.json"))
        ex = {a: len(m.get(str(a), {}).get("executed_jobs") or []) for a in range(1, nleaf + 1)}
        idle = [a for a, n in ex.items() if n == 0]
        idle_runs.append(len(idle))
        cores = collections.Counter(p[str(a)]["core"] for a in idle)
        idle_cores.append(dict(sorted(cores.items())))
    allcores = collections.Counter(p[str(a)]["core"] for a in range(1, nleaf + 1))
    print(f"{cell:40s} idle leaves per run {idle_runs}  fleet cores {dict(sorted(allcores.items()))}  idle-by-cores run1 {idle_cores[0]}")
