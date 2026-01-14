import csv
import json
import os
import argparse
import glob
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union  # Added Union for typing

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import redis

from swarm.database.repository import Repository
from swarm.models.job import Job


# -------------------------------------------------------------------------
# PART 1: DATA COLLECTOR (Modified)
# -------------------------------------------------------------------------

class SwarmDataCollector:
    """
    Fetches raw data from the Repository (Redis) and normalizes it into
    standard CSV/JSON files in the output directory.
    """

    def __init__(self, output_dir: str, repo: Optional[Repository] = None):
        self.output_dir = output_dir
        self.repo = repo
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

    def _load_metrics_from_repo(self) -> Dict[int, Any]:
        """Loads metrics from Redis using the repository (no file fallback)."""
        metrics_by_agent = {}
        if self.repo:
            try:
                raw = self.repo.get_all_objects(key_prefix="metrics", level=None)
                for entry in raw:
                    if isinstance(entry, str):
                        try:
                            entry = json.loads(entry)
                        except:
                            continue
                    agent_id = entry.get("id")
                    if agent_id is None and "value" in entry:
                        agent_id = entry["value"].get("id")
                        entry = entry["value"]
                    if agent_id is not None:
                        metrics_by_agent[int(agent_id)] = entry
            except Exception as e:
                print(f"[Collector] Warning: Redis metrics fetch failed: {e}")
        return metrics_by_agent

    def _get_job_info_from_repo(self, job_id: Union[int, str]) -> Dict[str, Any]:
        """Safely fetches job info from the repository."""
        if self.repo:
            try:
                # Assuming the key format is 'job:<job_id>'
                job_key = f"{Repository.KEY_JOB}:{job_id}"
                job_obj = self.repo.get(job_key)
                if isinstance(job_obj, str):
                    return json.loads(job_obj)
                return job_obj if isinstance(job_obj, dict) else {}
            except Exception:
                pass
        return {}

    def collect_all(self, args: argparse.Namespace):
        """Main entry point to collect all data and save to disk."""
        print(f"--- Collecting Data to {self.output_dir} ---")

        metrics = self._fetch_raw_metrics()
        self._save_json("metrics.json", metrics)
        self._save_load_traces(metrics)
        self._save_stats_summaries(metrics)

        failed_agent_list = args.failed_agents.split(',') if args.failed_agents else []
        self._save_failures(metrics, failed_agent_list)
        self._save_agent_info()
        self._save_reassignments()

        self._save_jobs_hierarchical()

        # Compute fault tolerance evaluation metrics
        self._compute_fault_tolerance_metrics(failed_agent_list)

    def _save_agent_info(self):
        """Fetches all agent objects and saves their core and capacity information to agent_info.csv."""
        if not self.repo:
            print("[Collector] Skipping agent info: Repository not available.")
            return

        try:
            # Assuming Repository.KEY_AGENT exists and returns a list of agent objects/dicts
            agents = self.repo.get_all_objects(key_prefix="agent", level=None)
        except Exception as e:
            print(f"[Collector] Error fetching agent info: {e}")
            return

        if not agents:
            print("[Collector] No agent data found.")
            return

        agent_rows = []
        for agent_data in agents:
            # Normalize agent data structure to a dictionary
            if isinstance(agent_data, str):
                try:
                    agent_data = json.loads(agent_data)
                except:
                    continue
            if not isinstance(agent_data, dict): continue

            # --- Base and Capacities Extraction ---
            row = {
                "agent_id": agent_data.get("agent_id"),
                "host": agent_data.get("host"),
                "port": agent_data.get("port"),
                "group": agent_data.get("group"),
                "level": agent_data.get("level"),
                "shutting_down": agent_data.get("shutting_down"),
                "last_updated": agent_data.get("last_updated"),
            }

            # Flatten Capacities
            capacities = agent_data.get("capacities", {})
            for key, val in capacities.items():
                row[f"capacity_{key}"] = val

            # --- DTN Information Extraction ---
            dtns = agent_data.get("dtns", {})

            # Find the best DTN (highest connectivity_score) for primary stats
            best_dtn_score = 0
            best_dtn_name = None

            for dtn_name, dtn_info in dtns.items():
                score = dtn_info.get("connectivity_score", 0)
                if score > best_dtn_score:
                    best_dtn_score = score
                    best_dtn_name = dtn_name

            row["dtn_count"] = len(dtns)
            row["dtn_best_name"] = best_dtn_name
            row["dtn_best_score"] = best_dtn_score

            # Optionally save all DTN scores as a single JSON string for future deep dive
            row["dtns_raw"] = json.dumps(dtns)

            agent_rows.append(row)

        df = pd.DataFrame(agent_rows).dropna(subset=["agent_id"])
        if not df.empty:
            path = os.path.join(self.output_dir, "agent_info.csv")
            df.to_csv(path, index=False)
            print(f"[Saved] agent_info.csv ({len(df)} agents)")

    def _fetch_raw_metrics(self) -> Dict[int, Any]:
        """Fetch from Redis if available, else try loading local JSON backup."""
        # ... (Implementation unchanged) ...
        metrics_by_agent = {}
        if self.repo:
            try:
                raw = self.repo.get_all_objects(key_prefix="metrics", level=None)
                for entry in raw:
                    if isinstance(entry, str):
                        try:
                            entry = json.loads(entry)
                        except:
                            continue
                    agent_id = entry.get("id")
                    if agent_id is None and "value" in entry:
                        agent_id = entry["value"].get("id")
                        entry = entry["value"]
                    if agent_id is not None:
                        metrics_by_agent[int(agent_id)] = entry
            except Exception as e:
                print(f"[Collector] Warning: Redis fetch failed or repo invalid: {e}")

        if not metrics_by_agent:
            path = os.path.join(self.output_dir, "metrics.json")
            if os.path.exists(path):
                print("[Collector] Loaded metrics from existing metrics.json")
                with open(path, 'r') as f:
                    data = json.load(f)
                    metrics_by_agent = {int(k): v for k, v in data.items()}
        return metrics_by_agent

    def _save_load_traces(self, metrics: Dict[int, Any]):
        # ... (Implementation unchanged) ...
        rows = []
        for agent_id, data in metrics.items():
            trace = data.get("load_trace", [])
            for pair in trace:
                if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                    rows.append({
                        "agent_id": agent_id,
                        "timestamp": float(pair[0]),
                        "load": float(pair[1])
                    })

        df = pd.DataFrame(rows)
        if not df.empty:
            df["relative_time"] = df["timestamp"] - df["timestamp"].min()
            df.sort_values(["agent_id", "timestamp"], inplace=True)
            df.to_csv(os.path.join(self.output_dir, "agent_loads.csv"), index=False)
            print(f"[Saved] agent_loads.csv ({len(df)} rows)")

    def _save_stats_summaries(self, metrics: Dict[int, Any]):
        # ... (Implementation unchanged) ...
        agent_rows = []
        job_conflicts = defaultdict(int)
        job_restarts = defaultdict(int)

        for agent_id, data in metrics.items():
            c_map = data.get("conflicts", {}) or {}
            r_map = data.get("restarts", {}) or {}

            agent_rows.append({
                "agent_id": agent_id,
                "total_conflicts": sum(int(x) for x in c_map.values()),
                "total_restarts": sum(int(x) for x in r_map.values())
            })

            for jid, val in c_map.items(): job_conflicts[int(jid)] += int(val)
            for jid, val in r_map.items(): job_restarts[int(jid)] += int(val)

        pd.DataFrame(agent_rows).to_csv(os.path.join(self.output_dir, "stats_agents.csv"), index=False)
        all_jids = set(job_conflicts.keys()) | set(job_restarts.keys())
        job_rows = [{"job_id": j, "conflicts": job_conflicts[j], "restarts": job_restarts[j]} for j in all_jids]
        pd.DataFrame(job_rows).to_csv(os.path.join(self.output_dir, "stats_jobs.csv"), index=False)
        print("[Saved] stats_agents.csv and stats_jobs.csv")

    def _save_failures(self, metrics: Dict[int, Any], failed_agent_list):
        # ... (Implementation unchanged) ...
        failures = []
        for agent_id, data in metrics.items():
            for fid, ts in (data.get("failed_agents", {}) or {}).items():
                # Note: fid is a string from Redis, need to check against string list
                if str(fid) not in failed_agent_list:
                    continue
                failures.append({"reporter": agent_id, "failed_agent_id": int(fid), "timestamp": float(ts)})

        if failures:
            df = pd.DataFrame(failures).sort_values("timestamp").drop_duplicates(subset=["failed_agent_id"])
            df.to_csv(os.path.join(self.output_dir, "failures.csv"), index=False)
            print(f"[Saved] failures.csv ({len(df)} events)")

    def _save_json(self, filename, data):
        # ... (Implementation unchanged) ...
        with open(os.path.join(self.output_dir, filename), 'w') as f:
            json.dump(data, f, indent=2)

    def _save_reassignments(self):
        """
        Collects job reassignment data from Redis metrics and saves it to a CSV.
        """
        if self.repo is None:
            print("[Collector] Skipping reassignments: Repository not available.")
            return

        reassignments: Dict[int, Dict[str, Any]] = {}
        metrics: Dict[int, Any] = self._load_metrics_from_repo()

        for agent_id, m in metrics.items():

            # --- Regular Reassignments (e.g., due to failure) ---
            regular_reassignments = m.get("reassignments", {}) or {}
            for job_id_str, reassignment_info in regular_reassignments.items():
                job_id = int(job_id_str)
                if isinstance(reassignment_info, dict) and job_id not in reassignments:
                    # Get current agent from the job object for "to_agent"
                    job_obj = self._get_job_info_from_repo(job_id)
                    to_agent = job_obj.get("leader_id", -1)

                    reassignments[job_id] = {
                        "job_id": job_id,
                        "from_agent": reassignment_info.get("failed_agent", -1),
                        "to_agent": to_agent,
                        "timestamp": reassignment_info.get("timestamp", 0),
                        "reason": reassignment_info.get("reason", "failure"),
                        "type": "regular",
                        "reporter_agent": agent_id
                    }

            # --- Delegation Reassignments (e.g., due to overload) ---
            delegation_reassignments = m.get("delegation_reassignments", {}) or {}
            for job_id_str, reassignment_info in delegation_reassignments.items():
                job_id = int(job_id_str)
                if isinstance(reassignment_info, dict) and job_id not in reassignments:
                    # Get current agent from the job object for "to_agent"
                    job_obj = self._get_job_info_from_repo(job_id)
                    to_agent = job_obj.get("leader_id", -1)

                    reassignments[job_id] = {
                        "job_id": job_id,
                        "from_agent": reassignment_info.get("from_agent", -1),
                        "to_agent": to_agent,
                        "timestamp": reassignment_info.get("timestamp", 0),
                        "reason": reassignment_info.get("reason", "delegation"),
                        "type": "delegation",
                        "reporter_agent": agent_id
                    }

        if reassignments:
            df = pd.DataFrame(list(reassignments.values()))
            path = os.path.join(self.output_dir, "reassignments.csv")
            df.to_csv(path, index=False)
            print(f"[Saved] reassignments.csv ({len(df)} reassignment events)")
        else:
            print("[Collector] No reassignment data found.")

    def _compute_fault_tolerance_metrics(self, failed_agent_list):
        """
        Computes fault tolerance evaluation metrics:
        1. Failure detection latency
        2. Job reassignment time
        3. Throughput degradation during recovery
        4. System availability
        """
        print("[Computing] Fault tolerance metrics...")

        failures_path = os.path.join(self.output_dir, "failures.csv")
        reassignments_path = os.path.join(self.output_dir, "reassignments.csv")
        jobs_files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))

        if not os.path.exists(failures_path) or not jobs_files:
            print("[Collector] Insufficient data for fault tolerance metrics")
            return

        # Load data
        failures_df = pd.read_csv(failures_path)
        reassignments_df = pd.read_csv(reassignments_path) if os.path.exists(reassignments_path) else pd.DataFrame()
        all_jobs_df = pd.concat([pd.read_csv(f) for f in jobs_files], ignore_index=True)

        metrics = {}

        # =====================================================================
        # 1. FAILURE DETECTION LATENCY
        # =====================================================================
        # This requires knowing actual failure time vs detection time
        # For now, we assume failures are detected when first reported in metrics
        # Detection latency = time between consecutive heartbeats (e.g., 30s interval)
        # More accurate: compare agent shutdown time with first detection report

        # Simplified: use reassignment timestamps as proxy for detection
        if not reassignments_df.empty and not failures_df.empty:
            # Join failures with reassignments to get detection latency
            detection_latencies = []
            for _, failure in failures_df.iterrows():
                failed_agent_id = failure['failed_agent_id']
                failure_time = failure['timestamp']

                # Find jobs reassigned from this agent
                reassigned = reassignments_df[reassignments_df['from_agent'] == failed_agent_id]
                if not reassigned.empty:
                    # Detection latency = earliest reassignment time - failure time
                    earliest_reassignment = reassigned['timestamp'].min()
                    detection_latency = earliest_reassignment - failure_time
                    detection_latencies.append({
                        'failed_agent_id': failed_agent_id,
                        'failure_time': failure_time,
                        'detection_time': earliest_reassignment,
                        'detection_latency_s': detection_latency
                    })

            if detection_latencies:
                detection_df = pd.DataFrame(detection_latencies)
                detection_df.to_csv(os.path.join(self.output_dir, "metric_failure_detection_latency.csv"), index=False)
                metrics['avg_failure_detection_latency_s'] = detection_df['detection_latency_s'].mean()
                metrics['max_failure_detection_latency_s'] = detection_df['detection_latency_s'].max()
                print(f"[Saved] metric_failure_detection_latency.csv (avg: {metrics['avg_failure_detection_latency_s']:.2f}s)")

        # =====================================================================
        # 2. JOB REASSIGNMENT TIME
        # =====================================================================
        # Time from job failure (leader failed) to job being reassigned to new agent
        if not reassignments_df.empty:
            reassignment_times = []
            for _, row in reassignments_df.iterrows():
                job_id = row['job_id']
                from_agent = row['from_agent']
                to_agent = row['to_agent']
                reassignment_timestamp = row['timestamp']

                # Find when the job was originally assigned to from_agent
                job_data = all_jobs_df[all_jobs_df['job_id'] == job_id]
                if not job_data.empty:
                    original_assignment = job_data['assigned_at'].iloc[0]
                    reassignment_time = reassignment_timestamp - original_assignment
                    reassignment_times.append({
                        'job_id': job_id,
                        'from_agent': from_agent,
                        'to_agent': to_agent,
                        'original_assignment_time': original_assignment,
                        'reassignment_time': reassignment_timestamp,
                        'reassignment_duration_s': reassignment_time
                    })

            if reassignment_times:
                reassignment_df = pd.DataFrame(reassignment_times)
                reassignment_df.to_csv(os.path.join(self.output_dir, "metric_job_reassignment_time.csv"), index=False)
                metrics['avg_job_reassignment_time_s'] = reassignment_df['reassignment_duration_s'].mean()
                metrics['max_job_reassignment_time_s'] = reassignment_df['reassignment_duration_s'].max()
                print(f"[Saved] metric_job_reassignment_time.csv (avg: {metrics['avg_job_reassignment_time_s']:.2f}s)")

        # =====================================================================
        # 3. THROUGHPUT DEGRADATION DURING RECOVERY
        # =====================================================================
        if not failures_df.empty and not all_jobs_df.empty:
            completed_jobs = all_jobs_df[(all_jobs_df['completed_at'] > 0) & (all_jobs_df['submitted_at'] > 0)]

            if not completed_jobs.empty:
                start_time = completed_jobs['submitted_at'].min()
                first_failure = failures_df['timestamp'].min()

                # Define time windows
                baseline_window = (0, first_failure - start_time)  # Before failure
                failure_window = (first_failure - start_time, first_failure - start_time + 60)  # 0-60s after failure
                recovery_window = (first_failure - start_time + 60, first_failure - start_time + 120)  # 60-120s after failure

                completed_jobs['relative_completion'] = completed_jobs['completed_at'] - start_time

                # Calculate throughput for each window
                def calc_throughput(df, window_start, window_end):
                    window_df = df[(df['relative_completion'] >= window_start) &
                                 (df['relative_completion'] < window_end)]
                    duration = window_end - window_start
                    return len(window_df) / duration if duration > 0 else 0

                baseline_throughput = calc_throughput(completed_jobs, *baseline_window)
                failure_throughput = calc_throughput(completed_jobs, *failure_window)
                recovery_throughput = calc_throughput(completed_jobs, *recovery_window)

                degradation_pct = ((baseline_throughput - failure_throughput) / baseline_throughput * 100) if baseline_throughput > 0 else 0
                recovery_pct = ((recovery_throughput - failure_throughput) / (baseline_throughput - failure_throughput) * 100) if (baseline_throughput - failure_throughput) > 0 else 0

                throughput_metrics = [{
                    'window': 'baseline',
                    'start_time_s': baseline_window[0],
                    'end_time_s': baseline_window[1],
                    'throughput_jobs_per_s': baseline_throughput,
                    'degradation_pct': 0.0
                }, {
                    'window': 'failure',
                    'start_time_s': failure_window[0],
                    'end_time_s': failure_window[1],
                    'throughput_jobs_per_s': failure_throughput,
                    'degradation_pct': degradation_pct
                }, {
                    'window': 'recovery',
                    'start_time_s': recovery_window[0],
                    'end_time_s': recovery_window[1],
                    'throughput_jobs_per_s': recovery_throughput,
                    'degradation_pct': ((baseline_throughput - recovery_throughput) / baseline_throughput * 100) if baseline_throughput > 0 else 0
                }]

                throughput_df = pd.DataFrame(throughput_metrics)
                throughput_df.to_csv(os.path.join(self.output_dir, "metric_throughput_degradation.csv"), index=False)
                metrics['baseline_throughput_jobs_per_s'] = baseline_throughput
                metrics['failure_throughput_jobs_per_s'] = failure_throughput
                metrics['recovery_throughput_jobs_per_s'] = recovery_throughput
                metrics['max_degradation_pct'] = degradation_pct
                metrics['recovery_pct'] = recovery_pct
                print(f"[Saved] metric_throughput_degradation.csv (max degradation: {degradation_pct:.1f}%)")

        # =====================================================================
        # 4. SYSTEM AVAILABILITY
        # =====================================================================
        if not all_jobs_df.empty:
            total_jobs = len(all_jobs_df)
            completed_jobs = len(all_jobs_df[all_jobs_df['completed_at'] > 0])
            successful_jobs = len(all_jobs_df[(all_jobs_df['completed_at'] > 0) &
                                             (all_jobs_df['exit_status'] == 0)])

            availability_pct = (successful_jobs / total_jobs * 100) if total_jobs > 0 else 0
            completion_rate_pct = (completed_jobs / total_jobs * 100) if total_jobs > 0 else 0

            availability_metrics = [{
                'total_jobs': total_jobs,
                'completed_jobs': completed_jobs,
                'successful_jobs': successful_jobs,
                'failed_jobs': completed_jobs - successful_jobs,
                'pending_jobs': total_jobs - completed_jobs,
                'completion_rate_pct': completion_rate_pct,
                'availability_pct': availability_pct
            }]

            availability_df = pd.DataFrame(availability_metrics)
            availability_df.to_csv(os.path.join(self.output_dir, "metric_system_availability.csv"), index=False)
            metrics['system_availability_pct'] = availability_pct
            metrics['completion_rate_pct'] = completion_rate_pct
            print(f"[Saved] metric_system_availability.csv (availability: {availability_pct:.1f}%)")

        # Save summary metrics
        if metrics:
            summary_df = pd.DataFrame([metrics])
            summary_df.to_csv(os.path.join(self.output_dir, "fault_tolerance_metrics_summary.csv"), index=False)
            print(f"[Saved] fault_tolerance_metrics_summary.csv")

    def _save_jobs_hierarchical(self):
        """
        Processes jobs fetched from the repository keyed by level and saves
        this data to level-specific CSV files.
        """
        if self.repo is None:
            print("[Collector] Skipping hierarchical jobs: Repository not available.")
            return

        jobs = {
            "0": self.repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=0),
            "1": self.repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=1),
            "2": self.repo.get_all_objects(key_prefix=Repository.KEY_JOB, level=2)
        }

        # Intermediate structure: {level_int: [job_data_dict, ...]}
        jobs_by_level: Dict[int, List[Dict]] = defaultdict(list)
        pending_jobs_by_level: Dict[int, List[Dict]] = defaultdict(list)

        # 1. Iterate through the input dictionary: key is the level string, value is the list of jobs
        for lvl_str, job_list in jobs.items():
            lvl = int(lvl_str)
            for job_data in job_list:
                # Type checking/conversion for the job object
                if isinstance(job_data, dict):
                    job = Job()
                    job.from_dict(job_data)
                else:
                    job = job_data  # Assume it's a Job instance if not a dict

                # Helper function to safely get attributes from job_data (dict) or job (object)
                def get_ts(obj, key):
                    # Check if obj is a dict and has the key, else check if it's an object with getattr
                    val = obj.get(key) if isinstance(obj, dict) else getattr(obj, key, 0)
                    return val if isinstance(val, dict) else {"0": val}.get(lvl_str, 0)  # Handle potential nested dict

                # Use getattr for object or .get for dictionary
                def get_simple_attr(obj, key):
                    return obj.get(key) if isinstance(obj, dict) else getattr(obj, key, 0)

                submitted_at = get_ts(job_data, "submitted_at").get(lvl_str, 0)
                assigned_at = get_ts(job_data, "assigned_at").get(lvl_str, 0)

                # Calculate latency
                lat = (assigned_at - submitted_at) if (assigned_at and submitted_at) else 0

                job_data_row = {
                    "job_id": get_simple_attr(job, "job_id"),
                    "leader_id": get_simple_attr(job, "leader_id"),
                    "level": lvl,
                    "submitted_at": submitted_at,
                    "selection_started_at": get_ts(job_data, "selection_started_at").get(lvl_str, 0),
                    "assigned_at": assigned_at,
                    "started_at": get_ts(job_data, "started_at").get(lvl_str, 0),
                    "completed_at": get_ts(job_data, "completed_at").get(lvl_str, 0),
                    "exit_status": get_simple_attr(job, "exit_status"),
                    "reasoning_time": get_simple_attr(job, "reasoning_time") or 0,
                    "scheduling_latency": lat,
                }

                # Use job.leader_id for classification
                if get_simple_attr(job, "leader_id") is None:
                    pending_jobs_by_level[lvl].append(job_data_row)
                else:
                    jobs_by_level[lvl].append(job_data_row)

        # 2. Save CSV per level from the constructed dictionary
        for lvl, rows in jobs_by_level.items():
            if rows:
                p = os.path.join(self.output_dir, f"jobs_level_{lvl}.csv")
                pd.DataFrame(rows).to_csv(p, index=False)
                print(f"[Saved] {p}")

        for lvl, rows in pending_jobs_by_level.items():
            if rows:
                p = os.path.join(self.output_dir, f"pending_jobs_level_{lvl}.csv")
                pd.DataFrame(rows).to_csv(p, index=False)
                print(f"[Saved] {p}")


# -------------------------------------------------------------------------
# PART 2: VISUALIZER (Added Reassignment Plotting)
# -------------------------------------------------------------------------

class SwarmVisualizer:
    def __init__(self, output_dir: str):
        self.output_dir = output_dir

    def plot_all(self):
        print(f"--- Generating Plots from {self.output_dir} ---")
        if not os.path.exists(self.output_dir):
            print(f"Error: Directory {self.output_dir} does not exist.")
            return

        self.plot_agent_loads()
        self.plot_stats_overview()
        self.plot_hierarchical_latency()
        self.plot_reasoning_time()
        self.plot_failures()
        self.plot_reassignment_summary()
        self.plot_jobs_per_agent()
        self.plot_timeline_with_failures()
        self.plot_throughput_degradation()
        self.plot_latency_details()

    def plot_agent_loads(self):
        path = os.path.join(self.output_dir, "agent_loads.csv")
        if not os.path.exists(path): return
        df = pd.read_csv(path)
        if df.empty: return

        # 1. Timeline
        plt.figure(figsize=(12, 6))
        for agent_id in sorted(df["agent_id"].unique()):
            sub = df[df["agent_id"] == agent_id]
            plt.plot(sub["relative_time"], sub["load"], label=f"Agent {agent_id}")
        plt.xlabel("Time (s)")
        plt.ylabel("Load")
        plt.title("Agent Load Over Time")
        plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_load_time.png"))
        plt.close()

        # 2. Summary
        agg = df.groupby("agent_id")["load"].agg(["mean", "max"]).reset_index()
        agg.plot(x="agent_id", y=["mean", "max"], kind="bar", figsize=(10, 5))
        plt.title("Load Statistics per Agent")
        plt.ylabel("Load")
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_load_summary.png"))
        plt.close()
        print("Generated: Load plots")

    def plot_stats_overview(self):
        path_agents = os.path.join(self.output_dir, "stats_agents.csv")
        if os.path.exists(path_agents):
            df = pd.read_csv(path_agents)
            if not df.empty and (df["total_conflicts"].sum() > 0 or df["total_restarts"].sum() > 0):
                df.set_index("agent_id")[["total_conflicts", "total_restarts"]].plot(kind="bar", figsize=(10, 6))
                plt.title("Conflicts & Restarts per Agent")
                plt.tight_layout()
                plt.savefig(os.path.join(self.output_dir, "plot_stats_agents.png"))
                plt.close()

        path_jobs = os.path.join(self.output_dir, "stats_jobs.csv")
        if os.path.exists(path_jobs):
            df = pd.read_csv(path_jobs)
            if not df.empty:
                bad_jobs = df[(df["conflicts"] > 0) | (df["restarts"] > 0)].sort_values("conflicts",
                                                                                        ascending=False).head(50)
                if not bad_jobs.empty:
                    bad_jobs.plot(x="job_id", y=["conflicts", "restarts"], kind="bar", figsize=(12, 6), stacked=True)
                    plt.title("Top 50 Jobs with Issues")
                    plt.tight_layout()
                    plt.savefig(os.path.join(self.output_dir, "plot_stats_jobs_problematic.png"))
                    plt.close()
        print("Generated: Conflict/Restart plots")

    def plot_hierarchical_latency(self):
        files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        for f in files:
            try:
                level = f.split("jobs_level_")[-1].replace(".csv", "")
                df = pd.read_csv(f)
                if df.empty: continue

                plt.figure(figsize=(8, 5))
                plt.hist(df["scheduling_latency"], bins=30, alpha=0.7, edgecolor='black')
                plt.title(f"Scheduling Latency Distribution (Level {level})")
                plt.xlabel("Latency (s)")
                plt.savefig(os.path.join(self.output_dir, f"plot_latency_hist_level_{level}.png"))
                plt.close()

                plt.figure(figsize=(10, 6))
                plt.scatter(df["leader_id"], df["scheduling_latency"], alpha=0.6)
                plt.title(f"Latency vs Agent (Level {level})")
                plt.xlabel("Agent ID")
                plt.ylabel("Latency (s)")
                plt.savefig(os.path.join(self.output_dir, f"plot_latency_scatter_level_{level}.png"))
                plt.close()
            except Exception as e:
                print(f"Skipping corrupt file {f}: {e}")
        if files:
            print("Generated: Hierarchical latency plots")

    def plot_reasoning_time(self):
        files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        plotted = False
        for f in files:
            df = pd.read_csv(f)
            if "reasoning_time" in df.columns and df["reasoning_time"].sum() > 0:
                plt.figure(figsize=(8, 6))
                plt.scatter(df["reasoning_time"], df["scheduling_latency"], alpha=0.5)
                plt.xlabel("Reasoning Time (s)")
                plt.ylabel("Scheduling Latency (s)")
                plt.title("Reasoning Time vs Scheduling Overhead")
                plt.grid(True)
                plt.savefig(os.path.join(self.output_dir, "plot_reasoning_vs_latency.png"))
                plt.close()
                plotted = True
                break
        if plotted:
            print("Generated: Reasoning time plots")

    def plot_failures(self):
        path = os.path.join(self.output_dir, "failures.csv")
        if not os.path.exists(path): return

        df = pd.read_csv(path)
        if df.empty: return

        plt.figure(figsize=(10, 3))
        plt.scatter(df["timestamp"], [1] * len(df), c='red', s=100, marker='x')
        for _, row in df.iterrows():
            plt.text(row["timestamp"], 1.05, f"Agent {int(row['failed_agent_id'])}", rotation=45)

        plt.yticks([])
        plt.xlabel("Timestamp")
        plt.title("Timeline of Agent Failures")
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_failures_timeline.png"))
        plt.close()
        print("Generated: Failure timeline")

    def plot_reassignment_summary(self):
        """Plots the count and types of job reassignments."""
        path = os.path.join(self.output_dir, "reassignments.csv")
        if not os.path.exists(path): return

        df = pd.read_csv(path)
        if df.empty: return

        # Bar chart: Reassignment counts by type (regular vs. delegation)
        type_counts = df["type"].value_counts()

        plt.figure(figsize=(8, 5))
        type_counts.plot(kind='bar', color=['skyblue', 'coral'])
        plt.title("Job Reassignment Breakdown")
        plt.ylabel("Number of Reassignments")
        plt.xlabel("Reassignment Type")
        plt.xticks(rotation=0)
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_reassignments_summary.png"))
        plt.close()

        # Scatter plot: Reassignments over time
        if "timestamp" in df.columns:
            plt.figure(figsize=(12, 4))
            df["relative_time"] = df["timestamp"] - df["timestamp"].min()
            plt.scatter(df["relative_time"], df["type"], c=df["type"].astype('category').cat.codes, cmap='viridis')
            plt.title("Reassignment Timeline")
            plt.xlabel("Relative Time (s)")
            plt.ylabel("Reassignment Type")
            plt.tight_layout()
            plt.savefig(os.path.join(self.output_dir, "plot_reassignments_timeline.png"))
            plt.close()

        print("Generated: Reassignment plots")

    def plot_jobs_per_agent(self):
        """Bar chart showing number of jobs completed by each agent."""
        files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        if not files:
            return

        # Combine all levels
        all_jobs = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        if all_jobs.empty or "leader_id" not in all_jobs.columns:
            return

        jobs_per_agent = all_jobs["leader_id"].value_counts().sort_index()

        plt.figure(figsize=(12, 6))
        plt.bar(jobs_per_agent.index, jobs_per_agent.values, color='steelblue', edgecolor='black')
        plt.xlabel("Agent ID")
        plt.ylabel("Number of Jobs Completed")
        plt.title("Job Distribution Across Agents")
        plt.grid(axis='y', alpha=0.3)
        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_jobs_per_agent.png"))
        plt.close()
        print("Generated: Jobs per agent plot")

    def plot_timeline_with_failures(self):
        """Timeline showing throughput, latency, and active agents with failure markers."""
        jobs_files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        failures_path = os.path.join(self.output_dir, "failures.csv")

        if not jobs_files:
            return

        df = pd.concat([pd.read_csv(f) for f in jobs_files], ignore_index=True)
        df = df[(df["completed_at"] > 0) & (df["submitted_at"] > 0)]

        if df.empty:
            return

        # Load failures if available
        failures = pd.read_csv(failures_path) if os.path.exists(failures_path) else pd.DataFrame()

        # Calculate metrics over time
        start_time = df["submitted_at"].min()
        df["relative_completion"] = df["completed_at"] - start_time
        df["latency"] = df["completed_at"] - df["submitted_at"]

        # Create time windows
        window_size = 10  # seconds
        max_time = df["relative_completion"].max()
        time_bins = np.arange(0, max_time + window_size, window_size)

        throughput = []
        avg_latency = []
        active_agents = []

        for i in range(len(time_bins) - 1):
            window_start = time_bins[i]
            window_end = time_bins[i + 1]
            window_df = df[(df["relative_completion"] >= window_start) & (df["relative_completion"] < window_end)]

            throughput.append(len(window_df) / window_size)
            avg_latency.append(window_df["latency"].mean() if not window_df.empty else 0)
            active_agents.append(window_df["leader_id"].nunique() if not window_df.empty else 0)

        time_centers = (time_bins[:-1] + time_bins[1:]) / 2

        # Create figure with 3 subplots
        fig, axes = plt.subplots(3, 1, figsize=(14, 10))

        # Subplot 1: Throughput
        axes[0].plot(time_centers, throughput, 'b-', linewidth=2, label='Throughput')
        axes[0].set_ylabel("Jobs/sec", fontsize=11)
        axes[0].set_title("System Performance Timeline with Failure Events", fontsize=13, fontweight='bold')
        axes[0].grid(alpha=0.3)
        axes[0].legend()

        # Subplot 2: Latency
        axes[1].plot(time_centers, avg_latency, 'g-', linewidth=2, label='Avg Latency')
        axes[1].set_ylabel("Latency (s)", fontsize=11)
        axes[1].grid(alpha=0.3)
        axes[1].legend()

        # Subplot 3: Active Agents
        axes[2].plot(time_centers, active_agents, 'orange', linewidth=2, label='Active Agents')
        axes[2].set_ylabel("Agent Count", fontsize=11)
        axes[2].set_xlabel("Time (s)", fontsize=11)
        axes[2].grid(alpha=0.3)
        axes[2].legend()

        # Add failure markers
        if not failures.empty:
            failure_times = failures["timestamp"] - start_time
            for ft in failure_times:
                for ax in axes:
                    ax.axvline(ft, color='red', linestyle='--', alpha=0.7, linewidth=1)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_timeline_failures.png"), dpi=300, bbox_inches="tight")
        plt.close()
        print("Generated: Timeline with failures plot")

    def plot_throughput_degradation(self):
        """Analyze throughput degradation due to failures."""
        jobs_files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        failures_path = os.path.join(self.output_dir, "failures.csv")

        if not jobs_files or not os.path.exists(failures_path):
            return

        df = pd.concat([pd.read_csv(f) for f in jobs_files], ignore_index=True)
        df = df[(df["completed_at"] > 0) & (df["submitted_at"] > 0)]
        failures = pd.read_csv(failures_path)

        if df.empty or failures.empty:
            return

        start_time = df["submitted_at"].min()
        first_failure = failures["timestamp"].min()

        # Baseline throughput (before first failure)
        baseline_duration = min(30, first_failure - start_time)
        baseline_df = df[df["completed_at"] <= first_failure]
        baseline_throughput = len(baseline_df) / baseline_duration if baseline_duration > 0 else 0

        # Failure period throughput (60s after first failure)
        failure_period_end = first_failure + 60
        failure_df = df[(df["completed_at"] >= first_failure) & (df["completed_at"] <= failure_period_end)]
        failure_throughput = len(failure_df) / 60 if len(failure_df) > 0 else 0

        # Degradation
        degradation_pct = ((baseline_throughput - failure_throughput) / baseline_throughput * 100) if baseline_throughput > 0 else 0

        # Create bar chart
        fig, ax = plt.subplots(1, 1, figsize=(10, 6))

        categories = ['Baseline\n(pre-failure)', 'During Failure\n(+60s)']
        values = [baseline_throughput, failure_throughput]
        colors = ['green', 'orange']

        bars = ax.bar(categories, values, color=colors, alpha=0.7, edgecolor='black')
        ax.set_ylabel("Throughput (jobs/sec)", fontsize=11)
        ax.set_title(f"Throughput Degradation Analysis ({degradation_pct:.1f}% degradation)", fontsize=13, fontweight='bold')
        ax.grid(axis='y', alpha=0.3)

        # Add value labels
        for bar, val in zip(bars, values):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + 0.02 * max(values),
                   f'{val:.3f}', ha='center', va='bottom', fontsize=10, fontweight='bold')

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_throughput_degradation.png"), dpi=300, bbox_inches="tight")
        plt.close()
        print("Generated: Throughput degradation plot")

    def plot_latency_details(self):
        """Detailed latency analysis with box plots and CDFs."""
        files = glob.glob(os.path.join(self.output_dir, "jobs_level_*.csv"))
        if not files:
            return

        all_jobs = pd.concat([pd.read_csv(f) for f in files], ignore_index=True)
        if all_jobs.empty or "scheduling_latency" not in all_jobs.columns:
            return

        latencies = all_jobs["scheduling_latency"][all_jobs["scheduling_latency"] > 0]
        if latencies.empty:
            return

        # Create figure with 2 subplots
        fig, axes = plt.subplots(1, 2, figsize=(14, 6))

        # Subplot 1: Box plot by agent
        if "leader_id" in all_jobs.columns:
            data_by_agent = [all_jobs[all_jobs["leader_id"] == aid]["scheduling_latency"].dropna()
                            for aid in sorted(all_jobs["leader_id"].unique())]
            axes[0].boxplot(data_by_agent, labels=sorted(all_jobs["leader_id"].unique()))
            axes[0].set_xlabel("Agent ID")
            axes[0].set_ylabel("Scheduling Latency (s)")
            axes[0].set_title("Latency Distribution by Agent")
            axes[0].grid(axis='y', alpha=0.3)

        # Subplot 2: CDF
        sorted_latencies = np.sort(latencies)
        cdf = np.arange(1, len(sorted_latencies) + 1) / len(sorted_latencies)

        axes[1].plot(sorted_latencies, cdf, linewidth=2, color='steelblue')
        axes[1].set_xlabel("Scheduling Latency (s)")
        axes[1].set_ylabel("CDF")
        axes[1].set_title("Cumulative Distribution of Latency")
        axes[1].grid(alpha=0.3)

        # Add percentile markers
        percentiles = [50, 90, 95, 99]
        for p in percentiles:
            val = np.percentile(sorted_latencies, p)
            axes[1].axhline(p/100, color='red', linestyle='--', alpha=0.5)
            axes[1].text(sorted_latencies.max() * 0.7, p/100, f'p{p}={val:.2f}s', fontsize=9)

        plt.tight_layout()
        plt.savefig(os.path.join(self.output_dir, "plot_latency_details.png"), dpi=300, bbox_inches="tight")
        plt.close()
        print("Generated: Detailed latency analysis")


# -------------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Swarm Analytics")
    parser.add_argument("--dir", type=str, required=True, help="Output directory for CSVs and Plots")
    parser.add_argument("--collect", action="store_true", help="Fetch data from Redis/Repo and save CSVs")
    parser.add_argument("--plot", action="store_true", help="Generate plots from existing CSVs")
    parser.add_argument("--host", type=str, required=False, default="localhost",
                        help="Database Host (required with --collect)")
    parser.add_argument("--failed-agents", type=str, default=None,
                        help="Comma-separated list of agent IDs that actually failed (e.g., '1,3,5'). Only these will be counted in failure metrics. Useful to exclude intentionally shut down agents.")

    args = parser.parse_args()

    if args.collect:
        try:
            redis_client = redis.StrictRedis(
                host=args.host,
                port=6379,
                decode_responses=True
            )
            # Test connection
            redis_client.ping()
        except redis.exceptions.ConnectionError:
            print(f"Error: Could not connect to Redis at {args.host}:6379.")
            return

        repo = Repository(redis_client=redis_client)
        collector = SwarmDataCollector(args.dir, repo=repo)
        collector.collect_all(args)

    if args.plot:
        viz = SwarmVisualizer(args.dir)
        viz.plot_all()


if __name__ == "__main__":
    main()