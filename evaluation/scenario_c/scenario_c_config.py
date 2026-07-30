#!/usr/bin/env python3.11
"""Scenario C (dynamic agent addition / cold start): groups 0-3 fail all
jobs at 0.30; group 4 (agents 21-25) is the good resource at 0.05. Group 4
leaves are killed just after startup and restarted mid-run by the driver.
delegation_timeout_s=60 so delegations to the dead group fail fast.
Usage: scenario_c_config.py [linucb|epsilon_greedy]"""
import sys, yaml
algo = sys.argv[1] if len(sys.argv) > 1 else "linucb"
path = "/root/SwarmAgents/config_swarm_multi.yml"
cfg = yaml.safe_load(open(path))
ram = [f"ram_bound_{t}_dtn_{io}" for t in ("short","medium","long") for io in ("light","heavy")]
cpu = [f"cpu_bound_{t}_dtn_{io}" for t in ("short","medium","long") for io in ("light","heavy")]
per_agent = {str(a): 0.30 for a in range(1, 21)}
per_agent.update({str(a): 0.05 for a in range(21, 26)})
cfg["runtime"]["delegation_timeout_s"] = 60
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
      "per_agent_failure_rates": per_agent,
      "per_job_type_failure_rates": {}},
}
yaml.safe_dump(cfg, open(path, "w"), sort_keys=False)
print(f"scenario C: algorithm={algo}, groups 0-3 @0.30, group 4 @0.05, delegation_timeout=60s")
