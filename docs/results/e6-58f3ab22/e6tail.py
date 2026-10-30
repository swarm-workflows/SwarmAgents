import csv, glob, json
for rd in sorted(glob.glob("/root/SwarmAgents/runs/campaign-e6-58f3ab22/*/run0[0-9]")):
    p = json.load(open(rd + ".partition.json")); cut, heal = p["applied_at"], p["healed_at"]
    w = [(float(j["started_at"]) - float(j["submitted_at"]), float(j["submitted_at"]))
         for j in csv.DictReader(open(rd + "/level0_jobs.csv")) if j.get("started_at") not in ("", None, "0")]
    over = lambda t: sum(1 for x, _ in w if x > t)
    print(rd.split("/")[-2], rd[-5:], "jobs", len(w), ">30s", over(30), ">60s", over(60), ">250s", over(250),
          "stalled jobs submitted before/during/after cut:",
          sum(1 for x, s in w if x > 30 and s < cut), sum(1 for x, s in w if x > 30 and cut <= s < heal),
          sum(1 for x, s in w if x > 30 and s >= heal))
