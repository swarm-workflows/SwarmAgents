import json, csv, collections, sys
for rd in sys.argv[1:]:
    a = json.load(open(rd + "/arrivals.json"))
    sub = {j["job_id"]: j["submitted_s"] for j in a["jobs"]}
    lead = {r["job_id"]: r["leader_id"] for r in csv.DictReader(open(rd + "/level1_jobs.csv"))}
    batches = collections.defaultdict(list)
    for jid, t in sub.items():
        if jid in lead:
            batches[int(t)].append(lead[jid])
    single = sum(1 for b in batches.values() if len(set(b)) == 1)
    top_share = sum(collections.Counter(b).most_common(1)[0][1] for b in batches.values()) / sum(len(b) for b in batches.values())
    seq = [collections.Counter(b).most_common(1)[0][0] for _, b in sorted(batches.items())]
    cell, run = rd.split("/")[-2], rd.split("/")[-1]
    print(cell[:34], run, "batches", len(batches), "single-winner", single,
          "top-winner share %.0f%%" % (100 * top_share), "| first 24 batch winners:", " ".join(seq[:24]))
