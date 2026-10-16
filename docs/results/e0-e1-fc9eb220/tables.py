import pandas as pd, numpy as np, sys
D = sys.argv[1]; a = pd.read_csv(f"{D}/cells_agg.csv").set_index("cell")
def f(c, m, p=2, ci=True, scale=1.0):
    v = a.loc[c, m]
    if pd.isna(v): return "–"
    s = f"{v*scale:.{p}f}"
    e = a.loc[c, m+"_ci"]
    if ci and not pd.isna(e) and e*scale >= 10**(-p): s += f" ± {e*scale:.{p}f}"
    return s
NAMES = {"pbft-analytic":"PBFT · analytic","snow-analytic":"Snow · analytic","hybrid-analytic":"Hybrid · analytic",
 "pbft-linucb-shared":"PBFT · LinUCB shared","pbft-linucb-pergroup":"PBFT · LinUCB per-group",
 "snow-linucb-shared":"Snow · LinUCB shared","snow-linucb-pergroup":"Snow · LinUCB per-group",
 "hybrid-linucb-shared":"Hybrid · LinUCB shared","hybrid-linucb-pergroup":"Hybrid · LinUCB per-group"}
def cells(sfx):
    out=[]
    for k in NAMES:
        c = ("e0-hier90-" if k=="pbft-analytic" else "e1-hier90-") + k + sfx
        out.append((NAMES[k], c))
    return out
def table(title, rows, cols):
    print(f"\n{title}\n")
    print("| Cell | " + " | ".join(h for h,_ in cols) + " |")
    print("|---|" + "---|"*len(cols))
    for name, c in rows:
        print(f"| {name} | " + " | ".join(fn(c) for _,fn in cols) + " |")
for sfx, lab in (("", "burst (20 jobs/s for 90 s)"), ("-poisson3", "steady Poisson 3 jobs/s for ~610 s")):
    R = cells(sfx)
    table(f"**Latency, Hier-90, {lab}.** Seconds, mean ± 95% CI over 5 runs.", R, [
        ("Coordinator sel. p50", lambda c: f(c,"l1_selection_p50")),
        ("Coordinator sel. p95", lambda c: f(c,"l1_selection_p95")),
        ("Leaf sel. p50", lambda c: f(c,"l0_selection_p50")),
        ("Job latency p50", lambda c: f(c,"job_latency_p50",1)),
        ("Job latency p95", lambda c: f(c,"job_latency_p95",1)),
        ("Wait p95", lambda c: f(c,"wait_p95",1)),
        ("Makespan", lambda c: f(c,"makespan_s",0))])
    table(f"**Cost and view quality, Hier-90, {lab}.**", R, [
        ("Consensus msgs/job", lambda c: f(c,"consensus_msgs_per_job",0)),
        ("Consensus KB/job", lambda c: f(c,"msg_bytes_consensus_sent",0,scale=1/1800/1000)),
        ("Overlay msgs/job", lambda c: f(c,"msgs_per_job_overlay",0)),
        ("Proposers/job (coord.)", lambda c: f(c,"proposers_per_job_l1",1)),
        ("Busiest coordinator share", lambda c: f(c,"coord_share_max",2,ci=False)),
        ("Least-used coordinator share", lambda c: f(c,"coord_share_min",3,ci=False)),
        ("Busiest group share (bandit)", lambda c: f(c,"group_busiest_share_mean",3,ci=False)),
        ("Context error (mean)", lambda c: f(c,"ctx_err_mean")),
        ("Jain fairness", lambda c: f(c,"fairness_jain",2,ci=False))])
    table(f"**Consensus internals, Hier-90, {lab}.** Per run, mean.", R, [
        ("Coord. finalize p50 (agent median)", lambda c: f(c,"l1_finalize_p50_agent_median",2,ci=False)),
        ("Coord. rounds/decision", lambda c: f(c,"l1_rounds_mean",1,ci=False)),
        ("Leaf rounds/decision", lambda c: f(c,"l0_rounds_mean",1,ci=False)),
        ("Coord. certificate finalizes", lambda c: f(c,"l1_certificate_finalizes",0,ci=False)),
        ("Coord. commit-lock holds", lambda c: f(c,"l1_commit_lock_holds",0,ci=False)),
        ("Leaf certificate finalizes", lambda c: f(c,"l0_certificate_finalizes",0,ci=False)),
        ("Snow abandons (all tiers)", lambda c: f"{a.loc[c,'l1_abandoned']+a.loc[c,'l0_abandoned']:.1f}")])
    table(f"**Selection p50 by measured RTT bin, Hier-90, {lab}.** Coordinator tier binned by leaf–coordinator RTT; leaf tier by the executor's median RTT to its peers. Seconds.", R, [
        ("Coord. LAN", lambda c: f(c,"rtt_lan_l1_selection_p50",2,ci=False)),
        ("Coord. regional", lambda c: f(c,"rtt_regional_l1_selection_p50",2,ci=False)),
        ("Coord. continental", lambda c: f(c,"rtt_continental_l1_selection_p50",2,ci=False)),
        ("Coord. transatlantic", lambda c: f(c,"rtt_transatlantic_l1_selection_p50",2,ci=False)),
        ("Leaf regional", lambda c: f(c,"rtt_regional_selection_p50",2,ci=False)),
        ("Leaf transatlantic", lambda c: f(c,"rtt_transatlantic_selection_p50",2,ci=False))])
c30="e0-hier30-pbft-analytic"; c90="e0-hier90-pbft-analytic"; c90p="e0-hier90-pbft-analytic-poisson3"
print("\n**E0, PBFT · analytic.** Seconds, mean ± 95% CI over 5 runs.\n")
print("| Metric | Hier-30 burst | Hier-90 burst | Hier-90 at 3 jobs/s |\n|---|---|---|---|")
for lab, m, p in [("Leaf selection p50","l0_selection_p50",2),("Leaf selection p95","l0_selection_p95",2),("Coordinator selection p50","l1_selection_p50",2),
                  ("Coordinator selection p95","l1_selection_p95",2),("Job latency p50","job_latency_p50",1),("Job latency p95","job_latency_p95",1),
                  ("Wait p95","wait_p95",1),("Execution p95","exec_p95",1),("Makespan","makespan_s",0),("Throughput (jobs/s)","throughput_jobs_per_s",2),
                  ("Consensus msgs/job","consensus_msgs_per_job",0),("Busiest coordinator share","coord_share_max",2),("Least-used coordinator share","coord_share_min",3)]:
    print(f"| {lab} | {f(c30,m,p)} | {f(c90,m,p)} | {f(c90p,m,p)} |")
