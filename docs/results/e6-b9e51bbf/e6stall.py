import csv, json, datetime
rd = "/root/SwarmAgents/runs/campaign-e6-b9e51bbf/e6-hier30-pbft/run01"
p = json.load(open(rd + ".partition.json")); cut, heal = p["applied_at"], p["healed_at"]
f = lambda t: datetime.datetime.utcfromtimestamp(float(t)).strftime("%H:%M:%S.%f")[:-3]
print("cut", f(cut), "heal", f(heal))
l1 = {j["job_id"]: j for j in csv.DictReader(open(rd + "/level1_jobs.csv"))}
for j in csv.DictReader(open(rd + "/level0_jobs.csv")):
    w = float(j["started_at"]) - float(j["submitted_at"])
    if w > 30:
        c = l1.get(j["job_id"], {})
        print(j["job_id"], "wait %.0f" % w, "sub", f(j["submitted_at"]), "| L1 sel", f(c.get("selection_started_at") or 0),
              "assigned", f(c.get("assigned_at") or 0), "coord", c.get("leader_id"), "| L0 sel", f(j["selection_started_at"]),
              "assigned", f(j["assigned_at"]), "leaf", j["leader_id"], "group", j.get("group"))
