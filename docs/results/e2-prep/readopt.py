import json, sys, collections
rd = sys.argv[1]
m = json.load(open(rd + "/metrics.json")); o = json.load(open(rd + "/outage.json"))
k, r = o["killed_at"], o["rejoined_at"]
dec = m["89"].get("delegation_decisions") or []
print("decision keys:", sorted(dec[0].keys())[:20] if dec else None)
win = collections.defaultdict(collections.Counter)
first_after = None
for d in dec:
    t = d.get("ts") or d.get("decided_at") or d.get("t")
    chosen = d.get("selected")
    if isinstance(chosen, list): chosen = chosen[0] if chosen else None
    phase = "before" if t < k else ("down" if t < r else "after")
    win[phase][str(chosen)] += 1
    if phase == "after" and str(chosen) == "4" and first_after is None:
        first_after = t - r
print({p: dict(c) for p, c in win.items()})
print("first delegation to group 4 after rejoin: %s s" % (None if first_after is None else round(first_after, 1)))
ex = collections.Counter()
for p in m.values():
    for j in p.get("executed_jobs") or []: ex[j] += 1
print("executed", len(ex), "twice", sum(1 for c in ex.values() if c > 1))
