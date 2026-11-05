"""Price every job on every Hier-90 G=1 coordinator exactly as wire_cost_for_job does at zero load,
using ResourceAgent.compute_job_cost itself on the aggregate AgentInfo a coordinator advertises."""
import json, glob, sys, collections, statistics as st
sys.path.insert(0, "/root/SwarmAgents")
from types import SimpleNamespace
from swarm.agents.resource_agent import ResourceAgent
from swarm.models.job import Job

run = sys.argv[1]
prof = json.load(open(run + "/agent_profiles.json"))
fake = ResourceAgent.__new__(ResourceAgent)
fake.cpu_weight, fake.ram_weight, fake.disk_weight, fake.gpu_weight, fake.qpu_weight = 0.4, 0.3, 0.2, 0.1, 0.0
fake.long_job_threshold, fake.connectivity_penalty_factor = 20.0, 1.0
fake.quantum_penalty_factor, fake.split_comm_penalty_factor = 1.0, 1.0
fake.measurement_layer = None

if len(sys.argv) > 2 and sys.argv[2] == "g3":
    coords = [88, 89, 90]
    groups = {88: [str(i) for i in range(1, 31)], 89: [str(i) for i in range(31, 61)], 90: [str(i) for i in range(61, 88)]}
else:
    coords = list(range(82, 91))
    groups = {c: [str(i) for i in range(1 + 9 * (c - 82), 10 + 9 * (c - 82))] for c in coords}
agg = {}
for c, leaves in groups.items():
    tot = collections.Counter(); dtn = collections.defaultdict(list)
    for l in leaves:
        p = prof[l]
        for k in ("core", "ram", "disk", "gpu"):
            tot[k] += p[k]
        for d in p["dtns"]:
            dtn[d["name"]].append(d["connectivity_score"])
    dtns = {n: SimpleNamespace(name=n, connectivity_score=round(sum(v) / len(v), 2)) for n, v in dtn.items()}
    caps = SimpleNamespace(core=tot["core"], ram=tot["ram"], disk=tot["disk"], gpu=tot["gpu"], qubits=0)
    agg[c] = (caps, dtns)
    print(c, "group leaves", leaves[0], "-", leaves[-1], "core", tot["core"], "ram", tot["ram"], "disk", tot["disk"], "gpu", tot["gpu"],
          "mean dtn conn", round(st.mean(d.connectivity_score for d in dtns.values()), 3))

jobs = []
for f in glob.glob("/root/workloads/jobs_1800/*.json"):
    j = Job(); j.from_dict(json.load(open(f))); jobs.append(j)
cost = {c: [] for c in coords}
rank_cheapest = collections.Counter(); in_top3 = collections.Counter(); ratio_to_best = {c: [] for c in coords}
for j in jobs:
    row = {c: ResourceAgent.compute_job_cost(fake, j, agg[c][0], agg[c][1]) for c in coords}
    for c in coords: cost[c].append(row[c])
    order = sorted(coords, key=lambda c: row[c])
    rank_cheapest[order[0]] += 1
    for c in order[:3 if len(coords) > 3 else 1]: in_top3[c] += 1
    best = row[order[0]]
    for c in coords: ratio_to_best[c].append(row[c] / best if best > 0 else float("nan"))
print("\ncoord  median_cost  cheapest_for  in_top3_for  median_cost/best  p10_cost/best")
for c in coords:
    r = sorted(ratio_to_best[c])
    print(f"{c}  {st.median(cost[c]):10.3f}  {rank_cheapest[c]:12d}  {in_top3[c]:11d}  {st.median(r):15.2f}  {r[len(r)//10]:12.2f}")
