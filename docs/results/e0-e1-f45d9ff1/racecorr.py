import json, csv, collections, glob, os
R = "/root/SwarmAgents/runs/campaign-e0-e1-f45d9ff1/"
cells = ["e0-hier30-pbft-analytic", "e0-hier90-pbft-analytic", "e0-hier90-pbft-analytic-poisson3",
         "e1-hier90-pbft-linucb-shared", "e1-hier90-pbft-linucb-pergroup", "e1-hier90-pbft-linucb-shared-poisson3"]
for cell in cells:
    print("==", cell)
    for rd in sorted(glob.glob(R + cell + "/run0?")):
        if ".attempt" in rd: continue
        meta = json.load(open(rd + "/run_meta.json")); p = json.load(open(rd + "/agent_profiles.json"))
        r = json.load(open(rd + "/rtt_matrix.json"))
        n, g = meta["agents"], meta.get("groups_per_coordinator", 1)
        k = {(90, 1): 9, (90, 3): 3, (30, 1): 3}[(n, g)]
        coords = [str(i) for i in range(n - k + 1, n + 1)]
        won = collections.Counter(x["leader_id"] for x in csv.DictReader(open(rd + "/level1_jobs.csv")))
        tot = sum(won.get(c, 0) for c in coords)
        rows = sorted(((r["database_rtt_ms"].get(p[c]["grpc"]["host"]), c, p[c]["grpc"]["host"], won.get(c, 0)) for c in coords))
        print("  " + os.path.basename(rd) + ": " + "  ".join(f"{c}@{h}({rtt:.1f}ms)={w/tot:.0%}" for rtt, c, h, w in rows))
