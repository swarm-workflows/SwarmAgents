"""Per-run supplementary metrics not in collect.py's wide row. Reads metrics.json, run_meta.json,
level1_jobs.csv and arrivals.json only (never agent logs: run dirs hold stale logs)."""
import csv, json, os, sys, statistics as st, collections

CONS_CONSENSUS_TYPES = ("Proposal", "Prepare", "Commit", "SnowQueryBatch", "SnowResponseBatch",
                        "SnowQuery", "SnowResponse", "CommitCertificate")

def coord_ids(meta):
    n, g = meta["agents"], meta.get("groups_per_coordinator", 1)
    k = {(90, 1): 9, (90, 3): 3, (30, 1): 3}[(n, g)]
    return set(str(i) for i in range(n - k + 1, n + 1))

def run(d):
    meta = json.load(open(os.path.join(d, "run_meta.json")))
    m = json.load(open(os.path.join(d, "metrics.json")))
    coords = coord_ids(meta)
    out = {"run_dir": d, "cell": d.rstrip("/").split("/")[-2], "run": d.rstrip("/").split("/")[-1]}
    for tier, ids in (("l1", coords), ("l0", set(m) - coords)):
        agg = collections.Counter(); rounds = []; fin = []
        for a in ids:
            c = ((m.get(a) or {}).get("instrumentation") or {}).get("consensus") or {}
            for k in ("won", "finalized", "abandoned", "cancelled", "certificate_finalizes",
                      "certificates_resent", "commit_lock_holds", "commit_splits_resolved",
                      "late_decided_hints", "stale_decided_hints", "sends_dropped",
                      "finalize_errors", "finalize_lost", "conflict_rounds", "superseded_skips"):
                agg[k] += int(c.get(k) or 0)
            if c.get("rounds_mean") is not None and c.get("rounds_n"):
                rounds += [c["rounds_mean"]] * int(c["rounds_n"])
            if c.get("finalize_s_p50") is not None:
                fin.append(c["finalize_s_p50"])
        for k, v in agg.items():
            out[f"{tier}_{k}"] = v
        out[f"{tier}_rounds_mean"] = round(st.mean(rounds), 3) if rounds else None
        out[f"{tier}_finalize_p50_agent_median"] = round(st.median(fin), 3) if fin else None
    # message mix
    sent = collections.Counter(); dropped = collections.Counter()
    fetch_def = fd_stale = fd_skip = 0
    for a, p in m.items():
        msg = (p.get("instrumentation") or {}).get("messages") or {}
        for t, v in (msg.get("sent_by_type") or {}).items():
            sent[t] += v["msgs"]
        for t, v in (msg.get("dropped_by_type") or {}).items():
            dropped[t] += v["msgs"] if isinstance(v, dict) else int(v)
        fetch_def += int(p.get("inbound_fetch_deferred") or 0)
        fd_stale += int(p.get("fd_refreshed_stale_view") or 0)
        fd_skip += int(p.get("fd_skipped_stale_view") or 0)
    jobs = meta["jobs"]
    cons = sum(v for t, v in sent.items() if t in CONS_CONSENSUS_TYPES)
    out["msgs_per_job_consensus"] = round(cons / jobs, 1)
    out["msgs_per_job_overlay"] = round((sum(sent.values()) - cons) / jobs, 1)
    for t in ("Proposal", "Prepare", "Commit", "SnowQueryBatch", "SnowResponseBatch"):
        out[f"msgs_per_job_{t}"] = round(sent.get(t, 0) / jobs, 1)
    out["msgs_dropped"] = sum(dropped.values())
    out["inbound_fetch_deferred"] = fetch_def
    out["fd_refreshed_stale_view"] = fd_stale
    out["fd_skipped_stale_view"] = fd_skip
    # how jobs split across coordinators (level-1 winners)
    lead = collections.Counter()
    p1 = os.path.join(d, "level1_jobs.csv")
    if os.path.exists(p1):
        for r in csv.DictReader(open(p1)):
            l = r.get("leader_id") or r.get("leader")
            if l not in (None, "", "None"):
                lead[str(l)] += 1
    tot = sum(lead.values())
    if tot:
        shares = [lead.get(c, 0) / tot for c in coords]
        out["coord_share_max"] = round(max(shares), 3)
        out["coord_share_min"] = round(min(shares), 3)
        out["coord_jobs"] = " ".join(f"{c}:{lead.get(c,0)}" for c in sorted(coords, key=int))
    # how each coordinator spread its delegations over its groups (bandit cells)
    busiest = []
    for c in coords:
        arms = ((m.get(c) or {}).get("mab_stats") or {}).get("arms") or {}
        pulls = [int(v.get("pull_count") or 0) for v in arms.values()]
        if len(pulls) > 1 and sum(pulls):
            busiest.append(max(pulls) / sum(pulls))
        ms = (m.get(c) or {}).get("mab_stats") or {}
        out["load_guard_decisions"] = out.get("load_guard_decisions", 0) + int(ms.get("load_guard_decisions") or 0)
        out["load_guard_narrowed"] = out.get("load_guard_narrowed", 0) + int(ms.get("load_guard_narrowed") or 0)
    if busiest:
        out["group_busiest_share_mean"] = round(st.mean(busiest), 3)
        out["group_busiest_share_worst"] = round(max(busiest), 3)
    # arrivals
    ap = os.path.join(d, "arrivals.json")
    if os.path.exists(ap):
        a = json.load(open(ap))
        subs = sorted(j["submitted_s"] for j in a["jobs"] if j.get("submitted_s") is not None)
        out["arrival"] = a.get("arrival")
        out["arrival_span_s"] = round(subs[-1] - subs[0], 1) if subs else None
        out["arrival_rate_realised"] = round((len(subs) - 1) / (subs[-1] - subs[0]), 3) if len(subs) > 1 else None
        lag = [j["submitted_s"] - j["planned_s"] for j in a["jobs"] if j.get("planned_s") is not None and j.get("submitted_s") is not None]
        out["arrival_lag_max_s"] = round(max(lag), 3) if lag else None
    return out

rows = []
for root in sys.argv[1:]:
    for cell in sorted(os.listdir(root)):
        cd = os.path.join(root, cell)
        if not os.path.isdir(cd): continue
        for r in sorted(os.listdir(cd)):
            if ".attempt" in r or not r.startswith("run"): continue
            if os.path.exists(os.path.join(cd, r, "metrics.json")):
                rows.append(run(os.path.join(cd, r)))
keys = []
for r in rows:
    for k in r:
        if k not in keys: keys.append(k)
w = csv.DictWriter(sys.stdout, fieldnames=keys); w.writeheader(); [w.writerow(r) for r in rows]
