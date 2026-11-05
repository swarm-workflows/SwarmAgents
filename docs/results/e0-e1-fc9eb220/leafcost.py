import json, glob, sys, statistics as st
sys.path.insert(0, "/root/SwarmAgents")
from types import SimpleNamespace
from swarm.agents.resource_agent import ResourceAgent
from swarm.models.job import Job
prof = json.load(open(sys.argv[1] + "/agent_profiles.json"))
fake = ResourceAgent.__new__(ResourceAgent)
fake.cpu_weight, fake.ram_weight, fake.disk_weight, fake.gpu_weight, fake.qpu_weight = 0.4, 0.3, 0.2, 0.1, 0.0
fake.long_job_threshold, fake.connectivity_penalty_factor, fake.quantum_penalty_factor, fake.split_comm_penalty_factor = 20.0, 1.0, 1.0, 1.0
fake.measurement_layer = None
jobs = []
for f in glob.glob("/root/workloads/jobs_1800/*.json"):
    j = Job(); j.from_dict(json.load(open(f))); jobs.append(j)
print("job demand: core", sorted(set(j.capacities.core for j in jobs)), "ram", sorted(set(j.capacities.ram for j in jobs))[:8], "gpu>0:", sum(1 for j in jobs if j.capacities.gpu))
# one representative leaf per core size, in group 2 (leaves 19-27) and fleet-wide
by_core = {}
for a in range(1, 82):
    p = prof[str(a)]; by_core.setdefault(p["core"], a)
rows = {}
for core, a in sorted(by_core.items()):
    p = prof[str(a)]
    caps = SimpleNamespace(core=p["core"], ram=p["ram"], disk=p["disk"], gpu=p["gpu"], qubits=0)
    dtns = {d["name"]: SimpleNamespace(name=d["name"], connectivity_score=d["connectivity_score"]) for d in p["dtns"]}
    rows[core] = [ResourceAgent.compute_job_cost(fake, j, caps, dtns) for j in jobs]
big = rows[max(rows)]
print("\nleaf cores | median cost | median ratio to 32-core | projected load needed for parity (load factor 1+(L/100)^1.5)")
for core, cs in rows.items():
    ratio = st.median(c / b for c, b in zip(cs, big))
    need = 100 * (ratio - 1) ** (2 / 3) if ratio > 1 else 0
    print(f"{core:4d} | {st.median(cs):8.2f} | {ratio:6.2f} | {need:7.0f} %")
