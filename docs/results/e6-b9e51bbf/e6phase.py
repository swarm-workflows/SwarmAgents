"""E6: group split across the cut, and leaf/coordinator timing by submission phase."""
import csv, glob, json, statistics as st
from collections import defaultdict
R = "/root/SwarmAgents/runs/campaign-e6-b9e51bbf"
hosts = [l.strip() for l in open("/root/SwarmAgents/agent_hosts.txt") if l.strip()]
rows = []
for pj in sorted(glob.glob(f"{R}/*/run0[0-9].partition.json")):
    rd = pj[:-len(".partition.json")]; cell = rd.split("/")[-2]; run = rd.split("/")[-1]
    p = json.load(open(pj))
    side = {h: "A" for h in p["side_a"]} | {h: "B" for h in p["side_b"]}
    agents = json.load(open(rd + "/all_agents.csv"))
    split = defaultdict(lambda: defaultdict(int))
    for a in agents:
        h = hosts[int(a["agent_id"]) - 1]
        split[(a.get("level") or 0, a.get("group"))][side.get(h, "?")] += 1
    if run == "run01":
        print(cell, "split by (level, group):", {f"L{k[0]}g{k[1]}": dict(v) for k, v in sorted(split.items(), key=lambda x: (x[0][0], x[0][1] or 0))})
    cut, heal = p["applied_at"], p["healed_at"]
    for fn, tier in (("level0_jobs.csv", "leaf"), ("level1_jobs.csv", "coord")):
        try:
            jobs = list(csv.DictReader(open(f"{rd}/{fn}")))
        except OSError:
            continue
        ph, wt, bad = defaultdict(list), defaultdict(list), 0
        for j in jobs:
            try:
                s, a = float(j["selection_started_at"] or 0), float(j["assigned_at"] or 0)
                sub = float(j["submitted_at"]); st_ = float(j["started_at"] or 0)
            except (KeyError, ValueError):
                bad += 1; continue
            phase = "before" if sub < cut else ("during" if sub < heal else "after")
            if s > 0 and a >= s:
                ph[phase].append(a - s)
            else:
                bad += 1
            if tier == "leaf" and st_ > 0:
                wt[phase].append(st_ - sub)
        q = lambda v, f: v[int(f * (len(v) - 1))] if v else float("nan")
        for phase in ("before", "during", "after"):
            v, w = sorted(ph[phase]), sorted(wt[phase])
            rows.append((cell, run, tier, phase, len(v), q(v, .5), q(v, .95), v[-1] if v else float("nan"),
                         q(w, .5), q(w, .95), w[-1] if w else float("nan"), bad))
print("cell run tier phase n sel_p50 sel_p95 sel_max wait_p50 wait_p95 wait_max no_sel_stamp")
for r in rows:
    print(*r[:5], *(f"{x:.2f}" for x in r[5:11]), r[11])
