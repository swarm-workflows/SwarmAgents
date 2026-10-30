import json, glob
for pj in sorted(glob.glob("/root/SwarmAgents/runs/campaign-e6-b9e51bbf/*/run0[0-9].partition.json")):
    d = json.load(open(pj)); rd = pj[:-len(".partition.json")]
    try:
        m = json.load(open(rd + "/metrics.json"))
    except OSError:
        continue
    lost = sum(int(p.get("lost_leader_races") or 0) for p in m.values() if isinstance(p, dict))
    print(pj.split("/")[-2], pj.split("/")[-1][:5], "sides", len(d["side_a"]), len(d["side_b"]),
          "sites", len(d["sites_a"]), len(d["sites_b"]), "held", d.get("held_s"), d.get("outcome"),
          "apply_failed", d["apply_failed"], "heal_failed", d["heal_failed"], "lost_leader_races", lost)
