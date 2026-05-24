"""Shared data loading and saving helpers used across plotting modules."""

import csv
import json
import os
import sys
from typing import Any

import pandas as pd
import redis

from swarm.database.repository import Repository
from swarm.models.job import Job, ObjectState


def load_metrics_from_repo(repo: Repository) -> dict[int, dict]:
    """
    Load metrics objects from Redis and normalize their shape.
    Expected schema per agent:
      {
        "id": <int>,
        "restarts": {job_id: count, ...} or {},
        "conflicts": {job_id: count, ...} or {},
        "idle_time": [floats] or [],
        "load_trace": [[timestamp, load], ...] or []
      }
    Returns: {agent_id: metrics_dict}
    """
    metrics_by_agent: dict[int, dict] = {}
    try:
        raw = repo.get_all_objects(key_prefix="metrics", level=None)
    except Exception:
        raw = []

    if not raw:
        return {}

    for entry in raw:
        # entry can be dict or a serialized json string
        if isinstance(entry, str):
            try:
                entry = json.loads(entry)
            except Exception:
                continue
        if not isinstance(entry, dict):
            continue
        agent_id = entry.get("id")
        if agent_id is None:
            # Some repos return keys; try to sniff common patterns
            # e.g., {"key":"metrics:1","value":{"id":1,...}}
            if "value" in entry and isinstance(entry["value"], dict):
                agent_id = entry["value"].get("id")
                entry = entry["value"]
        if agent_id is None:
            continue

        # normalize fields
        entry.setdefault("restarts", {})
        entry.setdefault("conflicts", {})
        entry.setdefault("idle_time", [])
        entry.setdefault("load_trace", [])
        metrics_by_agent[int(agent_id)] = entry

    return metrics_by_agent


def load_metrics_from_file(path: str) -> dict[int, dict]:
    """
    Load metrics from a saved JSON file.
    Returns: {agent_id: metrics_dict}
    """
    if not os.path.exists(path):
        return {}

    try:
        with open(path, 'r') as f:
            metrics = json.load(f)

        # Convert string keys to int keys if needed
        return {int(k): v for k, v in metrics.items()}
    except Exception as e:
        print(f"Error loading metrics from {path}: {e}")
        return {}


def load_jobs_csv(csv_path: str) -> pd.DataFrame:
    """Load an all_jobs.csv file and return a DataFrame with numeric columns."""
    df = pd.read_csv(csv_path)
    for col in ["submitted_at", "selection_started_at", "assigned_at",
                 "started_at", "completed_at", "scheduling_latency", "reasoning_time"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def load_jobs_from_csv(output_dir: str) -> pd.DataFrame:
    """Load job data from CSV file."""
    jobs_path = os.path.join(output_dir, "jobs.csv")
    if os.path.exists(jobs_path):
        return pd.read_csv(jobs_path)
    return pd.DataFrame()


def load_jobs_from_redis(db_host: str, db_port: int = 6379) -> pd.DataFrame:
    """Load job data from Redis."""
    if Repository is None:
        return pd.DataFrame()

    redis_client = redis.StrictRedis(
        host=db_host,
        port=db_port,
        decode_responses=True
    )
    repo = Repository(redis_client=redis_client)
    jobs = []

    try:
        # Try multiple levels for hierarchical topology
        for level in range(3):
            for group in range(10):
                raw = repo.get_all_objects(
                    key_prefix=Repository.KEY_JOB,
                    level=level,
                    group=group
                )
                for data in raw:
                    if isinstance(data, dict):
                        jobs.append(data)
    except Exception as e:
        print(f"Warning: Error loading jobs from Redis: {e}")

    if jobs:
        return pd.DataFrame(jobs)
    return pd.DataFrame()


def load_metrics_from_redis(db_host: str, db_port: int = 6379) -> dict[int, dict]:
    """Load metrics from Redis."""
    if Repository is None:
        print("Error: swarm package not available. Use --from-csv mode.")
        sys.exit(1)

    redis_client = redis.StrictRedis(
        host=db_host,
        port=db_port,
        decode_responses=True
    )
    repo = Repository(redis_client=redis_client)
    metrics_by_agent = {}

    try:
        raw = repo.get_all_objects(key_prefix="metrics", level=None)
        for data in raw:
            if isinstance(data, dict) and "id" in data:
                agent_id = int(data["id"])
                metrics_by_agent[agent_id] = data
    except Exception as e:
        print(f"Error loading from Redis: {e}")

    return metrics_by_agent


def save_metrics(metrics: dict[int, dict], path: str):
    """Save metrics dict to JSON file."""
    with open(path, 'w', newline='') as f:
        json.dump(metrics, f, indent=2)


def save_agents(agents: list[Any], path: str):
    """Save agents list to JSON file."""
    with open(path, 'w', newline='') as f:
        json.dump(agents, f, indent=2)


def save_jobs(jobs: list[Any], path: str, level: int | None = None):
    """
    Save jobs to CSV with level-specific timestamps.

    Args:
        jobs: List of job objects or dicts
        path: Output CSV path
        level: Hierarchy level (0, 1, etc.). If None, uses last timestamp (backward compatible)
    """
    detailed_latency = []
    pending_jobs = []
    for job_data in jobs:
        if isinstance(job_data, dict):
            job = Job()
            job.from_dict(job_data)
        else:
            job = job_data

        job_id = job.job_id
        leader_id = getattr(job, "leader_id", None)

        # Ensure numeric/float for reasoning_time (0.0 if missing)
        reasoning_time = getattr(job, "reasoning_time", None)
        try:
            reasoning_time = float(reasoning_time) if reasoning_time is not None else 0.0
        except Exception:
            reasoning_time = 0.0

        # Extract timestamps for the specified level
        submitted_at_dict = job.submitted_at_dict
        selection_started_at_dict = job.selection_started_at_dict
        assigned_at_dict = job.assigned_at_dict
        started_at_dict = job.started_at_dict
        completed_at_dict = job.completed_at_dict

        # Choose timestamp based on level
        if level is not None:
            # Use timestamps at the specified level (if available)
            submitted_at = submitted_at_dict.get(level, submitted_at_dict.get(0, 0))
            selection_started_at = selection_started_at_dict.get(level, 0)
            assigned_at = assigned_at_dict.get(level, 0)
            started_at = started_at_dict.get(level, started_at_dict.get(0, 0))
            completed_at = completed_at_dict.get(level, completed_at_dict.get(0, 0))
        else:
            # Backward compatible: use last timestamps (max values)
            submitted_at = submitted_at_dict.get(0, 0) if submitted_at_dict else 0
            selection_started_at = max(selection_started_at_dict.values()) if selection_started_at_dict else 0
            assigned_at = max(assigned_at_dict.values()) if assigned_at_dict else 0
            started_at = max(started_at_dict.values()) if started_at_dict else 0
            completed_at = max(completed_at_dict.values()) if completed_at_dict else 0

        # Calculate scheduling latency for this level
        scheduling_latency = assigned_at - submitted_at if assigned_at and submitted_at else 0

        # IMPORTANT: leader_id before reasoning_time to match header
        if leader_id is None or job.state not in [ObjectState.READY, ObjectState.RUNNING, ObjectState.COMPLETE]:
            pending_jobs.append([
            job_id,
            submitted_at,
            selection_started_at,
            assigned_at,
            started_at,
            completed_at,
            job.exit_status if job.exit_status is not None else 0,
            leader_id,
            reasoning_time,
            scheduling_latency,
            ])
        else:
            detailed_latency.append([
                job_id,
                submitted_at,
                selection_started_at,
                assigned_at,
                started_at,
                completed_at,
                job.exit_status if job.exit_status is not None else 0,
                leader_id,
                reasoning_time,
                scheduling_latency,
            ])

    file_name = f"{path}/all_jobs.csv" if level is None else f"{path}/level{level}_jobs.csv"
    pending_file_name = f"{path}/pending_jobs.csv" if level is None else f"{path}/pending_level{level}_jobs.csv"
    with open(file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'job_id', 'submitted_at', 'selection_started_at', 'assigned_at',
            'started_at', 'completed_at', 'exit_status', 'leader_id', 'reasoning_time', 'scheduling_latency',
        ])
        writer.writerows(detailed_latency)

    with open(pending_file_name, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([
            'job_id', 'submitted_at', 'selection_started_at', 'assigned_at',
            'started_at', 'completed_at', 'exit_status', 'leader_id', 'reasoning_time', 'scheduling_latency',
        ])
        writer.writerows(pending_jobs)
