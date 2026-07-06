#!/usr/bin/env python3.11
"""Scenario A (contextual bandit eval): write mab config into config_swarm_multi.yml.
Groups 0/2/4 (agents 1-5,11-15,21-25) fail ram_bound jobs at 0.8;
groups 1/3 (agents 6-10,16-20) fail cpu_bound jobs at 0.8; 0.05 otherwise.
Usage: scenario_a_config.py [linucb|epsilon_greedy]"""
import sys, yaml
algo = sys.argv[1] if len(sys.argv) > 1 else "linucb"
path = "/root/SwarmAgents/config_swarm_multi.yml"
cfg = yaml.safe_load(open(path))
ram = ['ram_bound_short_dtn_light', 'ram_bound_short_dtn_heavy', 'ram_bound_medium_dtn_light', 'ram_bound_medium_dtn_heavy', 'ram_bound_long_dtn_light', 'ram_bound_long_dtn_heavy']
cpu = ['cpu_bound_short_dtn_light', 'cpu_bound_short_dtn_heavy', 'cpu_bound_medium_dtn_light', 'cpu_bound_medium_dtn_heavy', 'cpu_bound_long_dtn_light', 'cpu_bound_long_dtn_heavy']
ram_fail = {t: 0.8 for t in ram}; ram_fail["default"] = 0.05
cpu_fail = {t: 0.8 for t in cpu}; cpu_fail["default"] = 0.05
per_agent = {}
for g in range(5):
    prof = ram_fail if g % 2 == 0 else cpu_fail
    for aid in range(5*g+1, 5*g+6):
        per_agent[str(aid)] = dict(prof)
cfg["mab"] = {
  "enabled": True, "algorithm": algo,
  "epsilon": 0.1, "epsilon_decay": 0.995, "epsilon_min": 0.01,
  "exploration_weight": 1.41, "top_k": 1,
  "linucb": {"alpha": 1.0, "discount": 0.995},
  "context": {"job_types": ram+cpu, "failure_window": 20, "max_group_size": 10,
              "max_inflight": 32, "max_dtns": 4, "long_job_threshold": 20.0,
              "max_caps": {"core": 16, "ram": 64, "disk": 500, "gpu": 4}},
  "reward": {"shaped": True, "exit_failure": -0.5, "timeout": -1.0, "non_winner": None},
  "persist_to_redis": True, "persist_interval_s": 10.0,
  "failure_simulation": {"enabled": True, "failure_probability": 0.05,
      "per_agent_failure_rates": per_agent, "per_job_type_failure_rates": {}},
}
yaml.safe_dump(cfg, open(path, "w"), sort_keys=False)
print(f"mab.algorithm={algo}; {len(per_agent)} leaf failure profiles")
