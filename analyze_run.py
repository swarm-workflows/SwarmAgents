#!/usr/bin/env python3.11
"""
analyze_run.py — Standalone diagnostic script for SwarmAgents runs.

Reads Redis state and agent logs to produce a comprehensive report of:
  1. Redis key overview
  2. Job state breakdown (per level+group)
  3. Agent liveness (heartbeat staleness)
  4. Co-parent leadership (for hierarchical topologies)
  5. Delegation activity (from agent logs)
  6. Infeasible job report
  7. Summary table

Usage:
  python analyze_run.py --db-host localhost --log-dir swarm-multi --config-dir configs --agents 30
  python analyze_run.py --db-host localhost --agents 30 --output report.json
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import re
import time
from collections import defaultdict

import redis
import yaml


# ObjectState enum values (from swarm/models/object.py)
STATE_NAMES = {
    1: "PENDING",
    2: "PRE_PREPARE",
    3: "PREPARE",
    4: "COMMIT",
    5: "READY",
    6: "RUNNING",
    7: "IDLE",
    8: "COMPLETE",
    9: "FAILED",
    10: "BLOCKED",
}


def get_redis_client(host: str, port: int = 6379) -> redis.StrictRedis:
    return redis.StrictRedis(host=host, port=port, decode_responses=True)


# ── Section 1: Redis overview ────────────────────────────────────────────────

def redis_overview(r: redis.StrictRedis) -> dict:
    """Count keys by prefix."""
    prefix_counts = defaultdict(int)
    for key in r.scan_iter("*"):
        prefix = key.split(":")[0]
        prefix_counts[prefix] += 1

    print("\n" + "=" * 60)
    print("1. REDIS KEY OVERVIEW")
    print("=" * 60)
    for prefix in sorted(prefix_counts):
        print(f"  {prefix}: {prefix_counts[prefix]} keys")
    total = sum(prefix_counts.values())
    print(f"  TOTAL: {total} keys")
    return dict(prefix_counts)


# ── Section 2: Job state breakdown ───────────────────────────────────────────

def job_state_breakdown(r: redis.StrictRedis, hierarchical: bool = False) -> dict:
    """For each level+group, count jobs by state."""
    print("\n" + "=" * 60)
    print("2. JOB STATE BREAKDOWN")
    print("=" * 60)

    levels = [0, 1, 2] if hierarchical else [0]
    report = {}

    for level in levels:
        # Scan for all job keys at this level
        pattern = f"job:{level}:*"
        jobs_by_group = defaultdict(lambda: defaultdict(list))

        for key in r.scan_iter(pattern):
            val = r.get(key)
            if not val:
                continue
            try:
                job_data = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                continue

            state_int = job_data.get("state", 0)
            state_name = STATE_NAMES.get(state_int, f"UNKNOWN({state_int})")
            group = job_data.get("group", 0)
            job_id = job_data.get("job_id", key)

            jobs_by_group[group][state_name].append({
                "job_id": job_id,
                "core": job_data.get("capacities", {}).get("core", 0),
                "ram": job_data.get("capacities", {}).get("ram", 0),
                "disk": job_data.get("capacities", {}).get("disk", 0),
                "gpu": job_data.get("capacities", {}).get("gpu", 0),
            })

        level_report = {}
        for group in sorted(jobs_by_group):
            group_data = jobs_by_group[group]
            print(f"\n  Level {level}, Group {group}:")
            group_report = {}
            for state_name in sorted(group_data):
                job_list = group_data[state_name]
                count = len(job_list)
                print(f"    {state_name}: {count}")
                group_report[state_name] = {
                    "count": count,
                    "job_ids": [j["job_id"] for j in job_list],
                }
                # Show details for non-complete jobs
                if state_name != "COMPLETE" and count > 0:
                    for j in job_list[:5]:  # Show first 5
                        print(f"      Job {j['job_id']}: core={j['core']}, ram={j['ram']}, "
                              f"disk={j['disk']}, gpu={j['gpu']}")
                    if count > 5:
                        print(f"      ... and {count - 5} more")
            level_report[str(group)] = group_report
        report[str(level)] = level_report

    return report


# ── Section 3: Agent liveness ────────────────────────────────────────────────

def agent_liveness(r: redis.StrictRedis, stale_threshold: float = 60.0) -> dict:
    """Show agent heartbeat staleness."""
    print("\n" + "=" * 60)
    print("3. AGENT LIVENESS")
    print("=" * 60)

    now = time.time()
    agents = {}

    # Scan for agent keys
    for key in r.scan_iter("agent:*"):
        val = r.get(key)
        if not val:
            continue
        try:
            agent_data = json.loads(val)
        except (json.JSONDecodeError, TypeError):
            continue

        agent_id = agent_data.get("agent_id", key)
        last_updated = agent_data.get("last_updated", 0)
        level = agent_data.get("level", 0)

        try:
            staleness = now - float(last_updated) if last_updated else float('inf')
        except (ValueError, TypeError):
            staleness = float('inf')

        is_alive = staleness < stale_threshold
        status = "ALIVE" if is_alive else "DEAD"
        agents[str(agent_id)] = {
            "level": level,
            "last_updated": last_updated,
            "staleness_s": round(staleness, 1),
            "status": status,
        }
        marker = "" if is_alive else " *** DEAD ***"
        print(f"  Agent {agent_id} (Level {level}): {status} "
              f"(staleness={staleness:.1f}s){marker}")

    alive_count = sum(1 for a in agents.values() if a["status"] == "ALIVE")
    dead_count = sum(1 for a in agents.values() if a["status"] == "DEAD")
    print(f"\n  Summary: {alive_count} alive, {dead_count} dead (threshold={stale_threshold}s)")

    return agents


# ── Section 4: Co-parent leadership ──────────────────────────────────────────

def co_parent_leadership(r: redis.StrictRedis, config_dir: str, agents_info: dict) -> dict:
    """Check co-parent leadership for hierarchical topologies."""
    print("\n" + "=" * 60)
    print("4. CO-PARENT LEADERSHIP")
    print("=" * 60)

    report = {}
    # Find Level-1 agent configs
    config_files = sorted(glob.glob(os.path.join(config_dir, "*.yml")))
    if not config_files:
        print("  No config files found in", config_dir)
        return report

    for cfg_path in config_files:
        try:
            with open(cfg_path) as f:
                cfg = yaml.safe_load(f)
        except Exception:
            continue

        topo = cfg.get("topology", {})
        if topo.get("level", 0) != 1:
            continue

        co_parent_groups = topo.get("co_parent_groups")
        if not co_parent_groups:
            continue

        agent_id = cfg_path.split("_")[-1].replace(".yml", "")
        print(f"\n  Agent {agent_id} co-parent groups:")

        for group_id, co_parents in sorted(co_parent_groups.items(), key=lambda x: str(x[0])):
            # Determine which co-parents are alive using agents_info
            alive_co_parents = []
            for cp_id in sorted(co_parents):
                status = agents_info.get(str(cp_id), {}).get("status", "UNKNOWN")
                alive_co_parents.append((cp_id, status == "ALIVE"))

            # Leader = lowest alive co-parent
            leader = None
            for cp_id, is_alive in alive_co_parents:
                if is_alive:
                    leader = cp_id
                    break

            status_str = ", ".join(
                f"{cp_id}({'OK' if alive else 'DEAD'})"
                for cp_id, alive in alive_co_parents
            )
            leader_str = f"Leader: {leader}" if leader else "NO LEADER"
            print(f"    Group {group_id}: [{status_str}] -> {leader_str}")

            report[str(group_id)] = {
                "co_parents": [{"id": cp_id, "alive": alive} for cp_id, alive in alive_co_parents],
                "leader": leader,
            }

    if not report:
        print("  No co-parent configurations found.")

    return report


# ── Section 5: Delegation activity ───────────────────────────────────────────

def delegation_activity(log_dir: str) -> dict:
    """Grep Level-1 agent logs for [SELECTED] messages."""
    print("\n" + "=" * 60)
    print("5. DELEGATION ACTIVITY")
    print("=" * 60)

    selected_re = re.compile(r"\[SELECTED\]")
    agent_re = re.compile(r"agent[-_]?(\d+)", re.IGNORECASE)

    per_agent = defaultdict(int)
    log_files = sorted(glob.glob(os.path.join(log_dir, "agent*.log")))

    if not log_files:
        print(f"  No agent log files found in {log_dir}")
        return {}

    for log_path in log_files:
        # Extract agent ID from filename
        basename = os.path.basename(log_path)
        m = agent_re.search(basename)
        agent_id = m.group(1) if m else basename

        count = 0
        try:
            with open(log_path) as f:
                for line in f:
                    if selected_re.search(line):
                        count += 1
        except Exception:
            continue
        per_agent[agent_id] = count
        if count > 0:
            print(f"  Agent {agent_id}: {count} jobs selected")

    total = sum(per_agent.values())
    print(f"\n  Total [SELECTED] events: {total}")
    return dict(per_agent)


# ── Section 6: Infeasible job report ─────────────────────────────────────────

def infeasible_job_report(r: redis.StrictRedis, hierarchical: bool = False) -> dict:
    """List PENDING/BLOCKED/FAILED jobs with their resource requirements."""
    print("\n" + "=" * 60)
    print("6. INFEASIBLE JOB REPORT")
    print("=" * 60)

    levels = [0, 1, 2] if hierarchical else [0]
    categories = {"PENDING": [], "BLOCKED": [], "FAILED": []}
    state_map = {"PENDING": 1, "BLOCKED": 10, "FAILED": 9}

    for level in levels:
        for state_name, state_val in state_map.items():
            pattern = f"state:{level}:*:{state_val}"
            for key in r.scan_iter(pattern):
                job_ids = r.smembers(key)
                for job_key in job_ids:
                    val = r.get(job_key)
                    if not val:
                        continue
                    try:
                        job_data = json.loads(val)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    caps = job_data.get("capacities", {})
                    categories[state_name].append({
                        "job_id": job_data.get("job_id", job_key),
                        "level": level,
                        "core": caps.get("core", 0),
                        "ram": caps.get("ram", 0),
                        "disk": caps.get("disk", 0),
                        "gpu": caps.get("gpu", 0),
                    })

    for state_name, jobs in categories.items():
        if jobs:
            print(f"\n  {state_name}: {len(jobs)} jobs")
            for j in jobs[:10]:
                print(f"    Job {j['job_id']} (Level {j['level']}): "
                      f"core={j['core']}, ram={j['ram']}, disk={j['disk']}, gpu={j['gpu']}")
            if len(jobs) > 10:
                print(f"    ... and {len(jobs) - 10} more")
        else:
            print(f"\n  {state_name}: 0 jobs")

    retired = len(categories["FAILED"])
    still_cycling = len(categories["PENDING"]) + len(categories["BLOCKED"])
    print(f"\n  Summary: {retired} infeasible retired (FAILED), {still_cycling} still cycling (PENDING+BLOCKED)")

    return {k: v for k, v in categories.items()}


# ── Section 7: Summary table ────────────────────────────────────────────────

def summary_table(r: redis.StrictRedis, hierarchical: bool = False) -> dict:
    """Per-group summary: total, complete, pending, failed."""
    print("\n" + "=" * 60)
    print("7. SUMMARY TABLE")
    print("=" * 60)

    levels = [0, 1, 2] if hierarchical else [0]
    totals = {"total": 0, "complete": 0, "pending": 0, "failed": 0, "blocked": 0, "other": 0}
    group_summaries = {}

    for level in levels:
        for key in r.scan_iter(f"job:{level}:*"):
            val = r.get(key)
            if not val:
                continue
            try:
                job_data = json.loads(val)
            except (json.JSONDecodeError, TypeError):
                continue

            state_int = job_data.get("state", 0)
            group = job_data.get("group", 0)
            group_key = f"L{level}G{group}"

            if group_key not in group_summaries:
                group_summaries[group_key] = {"total": 0, "complete": 0, "pending": 0, "failed": 0, "blocked": 0, "other": 0}

            group_summaries[group_key]["total"] += 1
            totals["total"] += 1

            if state_int == 8:  # COMPLETE
                group_summaries[group_key]["complete"] += 1
                totals["complete"] += 1
            elif state_int == 1:  # PENDING
                group_summaries[group_key]["pending"] += 1
                totals["pending"] += 1
            elif state_int == 9:  # FAILED
                group_summaries[group_key]["failed"] += 1
                totals["failed"] += 1
            elif state_int == 10:  # BLOCKED
                group_summaries[group_key]["blocked"] += 1
                totals["blocked"] += 1
            else:
                group_summaries[group_key]["other"] += 1
                totals["other"] += 1

    # Print table
    header = f"  {'Group':<12} {'Total':>6} {'Complete':>9} {'Pending':>8} {'Failed':>7} {'Blocked':>8} {'Other':>6}"
    print(header)
    print("  " + "-" * (len(header) - 2))

    for group_key in sorted(group_summaries):
        s = group_summaries[group_key]
        print(f"  {group_key:<12} {s['total']:>6} {s['complete']:>9} {s['pending']:>8} "
              f"{s['failed']:>7} {s['blocked']:>8} {s['other']:>6}")

    print("  " + "-" * (len(header) - 2))
    print(f"  {'TOTAL':<12} {totals['total']:>6} {totals['complete']:>9} {totals['pending']:>8} "
          f"{totals['failed']:>7} {totals['blocked']:>8} {totals['other']:>6}")

    return {"groups": group_summaries, "totals": totals}


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Analyze a SwarmAgents run from Redis + logs")
    ap.add_argument("--db-host", required=True, help="Redis host")
    ap.add_argument("--db-port", type=int, default=6379, help="Redis port")
    ap.add_argument("--log-dir", default="swarm-multi", help="Directory containing agent log files")
    ap.add_argument("--config-dir", default="configs", help="Directory containing agent config YAML files")
    ap.add_argument("--agents", type=int, required=True, help="Number of agents in the run")
    ap.add_argument("--stale-threshold", type=float, default=60.0,
                    help="Seconds before declaring an agent dead (default: 60)")
    ap.add_argument("--hierarchical", action="store_true",
                    help="Analyze hierarchical topology (checks levels 0, 1, 2)")
    ap.add_argument("--output", type=str, default=None,
                    help="Write structured JSON report to this file")
    args = ap.parse_args()

    r = get_redis_client(args.db_host, args.db_port)

    # Try to detect hierarchical from Redis keys
    hierarchical = args.hierarchical
    if not hierarchical:
        # Auto-detect: check for level-1 job keys
        for _ in r.scan_iter("job:1:*", count=1):
            hierarchical = True
            print("(Auto-detected hierarchical topology)")
            break

    print(f"\nAnalyzing run: db={args.db_host}:{args.db_port}, "
          f"agents={args.agents}, hierarchical={hierarchical}")

    report = {}
    report["redis_overview"] = redis_overview(r)
    report["job_state_breakdown"] = job_state_breakdown(r, hierarchical=hierarchical)
    report["agent_liveness"] = agent_liveness(r, stale_threshold=args.stale_threshold)
    report["co_parent_leadership"] = co_parent_leadership(r, args.config_dir, report["agent_liveness"])
    report["delegation_activity"] = delegation_activity(args.log_dir)
    report["infeasible_jobs"] = infeasible_job_report(r, hierarchical=hierarchical)
    report["summary"] = summary_table(r, hierarchical=hierarchical)

    if args.output:
        with open(args.output, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print(f"\nJSON report written to: {args.output}")

    print("\nAnalysis complete.")


if __name__ == "__main__":
    main()
