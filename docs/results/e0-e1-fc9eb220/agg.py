import pandas as pd, numpy as np, scipy.stats as ss, sys
D = sys.argv[1]
w = pd.read_csv(f"{D}/runs_wide.csv")
s = pd.read_csv(f"{D}/supp.csv")
w["cell"] = w["run_dir"].str.rstrip("/").str.split("/").str[-2]
w["run"] = w["run_dir"].str.rstrip("/").str.split("/").str[-1]
assert not w["run"].str.contains("attempt").any()
s = s.drop(columns=["run_dir"])
df = w.merge(s, on=["cell", "run"], how="left", suffixes=("", "_supp"))
assert len(df) == 95, len(df)
ORDER = ["e0-hier30-pbft-analytic", "e0-hier90-pbft-analytic", "e1-hier90-snow-analytic", "e1-hier90-hybrid-analytic",
 "e1-hier90-pbft-linucb-shared", "e1-hier90-pbft-linucb-pergroup", "e1-hier90-snow-linucb-shared", "e1-hier90-snow-linucb-pergroup",
 "e1-hier90-hybrid-linucb-shared", "e1-hier90-hybrid-linucb-pergroup"]
ORDER += [c + "-poisson3" for c in ORDER[1:]]
assert set(ORDER) == set(df.cell), set(df.cell) ^ set(ORDER)
df["cell"] = pd.Categorical(df["cell"], ORDER, ordered=True)
df.to_csv(f"{D}/runs_merged.csv", index=False)

METRICS = ["completion_pct","success_pct","jobs_executed_twice","pending_count","restarts_total","decided_unpersisted_resets",
 "msgs_dropped","l0_selection_p50","l0_selection_p95","l1_selection_p50","l1_selection_p95",
 "job_latency_p50","job_latency_p95","job_latency_p99","wait_p50","wait_p95","exec_p50","exec_p95","makespan_s","throughput_jobs_per_s",
 "consensus_msgs_per_job","msgs_per_job_consensus","msgs_per_job_overlay","msg_bytes_consensus_sent",
 "proposers_per_job_l1","proposals_per_job_l1","proposers_per_job_l0","tier_copies_per_job",
 "l1_won","l1_abandoned","l0_abandoned","l1_certificate_finalizes","l0_certificate_finalizes","l1_certificates_resent",
 "l1_commit_lock_holds","l0_commit_lock_holds","l1_commit_splits_resolved","l0_commit_splits_resolved",
 "l1_rounds_mean","l0_rounds_mean","l1_finalize_p50_agent_median","l0_finalize_p50_agent_median",
 "finalize_s_agent_median_p50","rounds_agent_median_p50",
 "coord_share_max","coord_share_min","group_busiest_share_mean","group_busiest_share_worst","load_guard_decisions","load_guard_narrowed",
 "ctx_age_p50","ctx_age_p95","ctx_err_mean","ctx_err_p95","ctx_err_chosen_mean","ctx_err_signed_mean","ctx_err_decisions_scored","ctx_err_decisions_unscored",
 "fairness_jain","fairness_jain_active","executing_agents",
 "rtt_lan_jobs","rtt_regional_jobs","rtt_continental_jobs","rtt_transatlantic_jobs",
 "rtt_lan_l1_selection_p50","rtt_regional_l1_selection_p50","rtt_continental_l1_selection_p50","rtt_transatlantic_l1_selection_p50",
 "rtt_lan_selection_p50","rtt_regional_selection_p50","rtt_continental_selection_p50","rtt_transatlantic_selection_p50",
 "arrival_rate_realised","arrival_span_s","arrival_lag_max_s","inbound_fetch_deferred","fd_refreshed_stale_view","fd_skipped_stale_view",
 "barrier_short_agents","metrics_complete"]
METRICS = [m for m in METRICS if m in df.columns]
missing = [m for m in ["l1_selection_p50"] if m not in df.columns]
rows = []
for cell, g in df.groupby("cell", observed=True):
    r = {"cell": cell, "n": len(g)}
    for m in METRICS:
        v = pd.to_numeric(g[m], errors="coerce").dropna()
        if len(v) == 0: r[m] = np.nan; r[m+"_ci"] = np.nan; continue
        r[m] = v.mean()
        r[m+"_ci"] = ss.t.ppf(0.975, len(v)-1) * v.std(ddof=1) / np.sqrt(len(v)) if len(v) > 1 else np.nan
        r[m+"_min"] = v.min(); r[m+"_max"] = v.max(); r[m+"_n"] = len(v)
    rows.append(r)
agg = pd.DataFrame(rows)
agg.to_csv(f"{D}/cells_agg.csv", index=False)
pd.set_option("display.width", 250); pd.set_option("display.max_columns", 30); pd.set_option("display.max_rows", 400)
short = {c: c.replace("e1-hier90-","").replace("e0-hier90-","E0-90 ").replace("e0-hier30-","E0-30 ") for c in ORDER}
def show(cols, fmt="{:.2f}"):
    t = agg[["cell"] + cols].copy(); t["cell"] = t["cell"].map(short)
    for c in cols:
        t[c] = [ (fmt.format(a) + (f"±{fmt.format(b)}" if not np.isnan(b) and b>0 else "")) if not np.isnan(a) else "-" for a,b in zip(agg[c], agg[c+"_ci"])]
    print(t.to_string(index=False)); print()
show(["completion_pct","jobs_executed_twice","pending_count","restarts_total","msgs_dropped","decided_unpersisted_resets"])
show(["l0_selection_p50","l0_selection_p95","l1_selection_p50","l1_selection_p95"])
show(["job_latency_p50","job_latency_p95","wait_p95","exec_p95","makespan_s","throughput_jobs_per_s"],"{:.1f}")
show(["consensus_msgs_per_job","msgs_per_job_consensus","msgs_per_job_overlay","proposers_per_job_l1","tier_copies_per_job"],"{:.1f}")
show(["l1_won","l1_abandoned","l0_abandoned","l1_certificate_finalizes","l0_certificate_finalizes","l1_commit_lock_holds","l1_rounds_mean","l0_rounds_mean"],"{:.1f}")
show(["l1_finalize_p50_agent_median","l0_finalize_p50_agent_median","coord_share_max","coord_share_min","group_busiest_share_mean","group_busiest_share_worst"],"{:.3f}")
show(["ctx_age_p50","ctx_err_mean","ctx_err_chosen_mean","ctx_err_signed_mean","ctx_err_decisions_unscored","fairness_jain","executing_agents"],"{:.2f}")
show(["arrival_rate_realised","arrival_span_s","arrival_lag_max_s","inbound_fetch_deferred","fd_refreshed_stale_view","barrier_short_agents"],"{:.2f}")
show(["rtt_lan_jobs","rtt_regional_jobs","rtt_continental_jobs","rtt_transatlantic_jobs"],"{:.0f}")
show(["rtt_lan_l1_selection_p50","rtt_regional_l1_selection_p50","rtt_continental_l1_selection_p50","rtt_transatlantic_l1_selection_p50"],"{:.2f}")
show(["rtt_lan_selection_p50","rtt_regional_selection_p50","rtt_continental_selection_p50","rtt_transatlantic_selection_p50"],"{:.2f}")
print("not in data:", [m for m in ["l1_selection_p50","finalize_s_agent_median_p50"] if m not in df.columns])
