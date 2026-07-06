#!/usr/bin/env python3.11
"""Scenario B (non-stationarity): Scenario A failure profiles that FLIP
mid-run. Phase 0: groups 0/2/4 fail ram_bound 0.8, groups 1/3 fail
cpu_bound 0.8. Phase 1 (after_s=160, ~mid-workload at 1 job/s): parity
inverted. Usage: scenario_b_config.py [discount]"""
import sys, yaml
discount = float(sys.argv[1]) if len(sys.argv) > 1 else 0.995
path = "/root/SwarmAgents/config_swarm_multi.yml"
cfg = yaml.safe_load(open(path))
ram = [f"ram_bound_{t}_dtn_{io}" for t in ("short","medium","long") for io in ("light","heavy")]
cpu = [f"cpu_bound_{t}_dtn_{io}" for t in ("short","medium","long") for io in ("light","heavy")]
ram_fail = {t: 0.8 for t in ram}; ram_fail["default"] = 0.05
cpu_fail = {t: 0.8 for t in cpu}; cpu_fail["default"] = 0.05

def profile(even_fails_ram):
    per_agent = {}
    for g in range(5):
        even = g % 2 == 0
        prof = (ram_fail if even else cpu_fail) if even_fails_ram else (cpu_fail if even else ram_fail)
        for aid in range(5*g+1, 5*g+6):
            per_agent[str(aid)] = dict(prof)
    return per_agent

cfg["mab"] = {
  "enabled": True, "algorithm": "linucb",
  "epsilon": 0.1, "epsilon_decay": 0.995, "epsilon_min": 0.01,
  "exploration_weight": 1.41, "top_k": 1,
  "linucb": {"alpha": 1.0, "discount": discount},
  "context": {"job_types": ram+cpu, "failure_window": 20, "max_group_size": 10,
              "max_inflight": 32, "max_dtns": 4, "long_job_threshold": 20.0,
              "max_caps": {"core": 16, "ram": 64, "disk": 500, "gpu": 4}},
  "reward": {"shaped": True, "exit_failure": -0.5, "timeout": -1.0, "non_winner": None},
  "persist_to_redis": True, "persist_interval_s": 10.0,
  "failure_simulation": {"enabled": True, "failure_probability": 0.05,
      "per_agent_failure_rates": profile(True),
      "per_job_type_failure_rates": {},
      "phases": [{"after_s": 160, "per_agent_failure_rates": profile(False)}]},
}
yaml.safe_dump(cfg, open(path, "w"), sort_keys=False)
print(f"scenario B: linucb discount={discount}, flip after_s=160")
