#!/usr/bin/env python3.11
"""Snapshot Scenario A results from Redis before the next run flushes it.
Usage: extract_scenario_a.py <out.json>"""
import json, sys
from collections import Counter
import redis
out = sys.argv[1]
r = redis.StrictRedis(host="localhost", port=6379, decode_responses=True)
data = {"jobs": [], "metrics": {}}
for k in r.scan_iter(match="job:0:*", count=1000):
    raw = r.get(k)
    if not raw:
        continue
    d = json.loads(raw)
    parts = k.split(":")
    data["jobs"].append({"key": k, "group": int(parts[2]), "job_id": parts[3],
                         "job_type": d.get("job_type"), "exit_status": d.get("exit_status"),
                         "state": d.get("state")})
for k in list(r.scan_iter(match="metrics*", count=1000)) + list(r.scan_iter(match="mab:*", count=1000)) + list(r.scan_iter(match="job:1:*", count=1000)):
    raw = r.get(k)
    if raw:
        try:
            data["metrics"][k] = json.loads(raw)
        except Exception:
            pass
json.dump(data, open(out, "w"), indent=1)
jobs = data["jobs"]
comp = [j for j in jobs if j["state"] == 8]
succ = [j for j in comp if int(j.get("exit_status") or 0) == 0]
print(f"L0 records: {len(jobs)}, complete: {len(comp)}, success: {len(succ)}"
      f" ({100*len(succ)/max(1,len(comp)):.1f}%)")
by = Counter()
for j in comp:
    rc = (j["job_type"] or "?").split("_")[0]
    ok = int(j.get("exit_status") or 0) == 0
    parity = "even(ram-fail)" if j["group"] % 2 == 0 else "odd(cpu-fail)"
    by[(rc, parity, ok)] += 1
for kk in sorted(by):
    print(kk, by[kk])
