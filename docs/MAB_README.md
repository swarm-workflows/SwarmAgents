# Multi-Armed Bandit (MAB) for Hierarchical Job Delegation

This document describes the Multi-Armed Bandit reinforcement learning layer added to coordinator agents for intelligent job delegation in hierarchical topologies.

## Overview

In hierarchical topologies, coordinator agents (Level 1+) delegate jobs to child groups. Previously, jobs were delegated to ALL capable child groups. With MAB enabled, coordinators **learn from job outcomes** which child groups to prefer, reducing delegation to unreliable or overloaded groups.

```
Level 2 coordinator ─── MAB selects which Level 1 group(s) to delegate to
                         ↑ success/failure signal from Level 1 outcomes
Level 1 coordinator ─── MAB selects which Level 0 group(s) to delegate to
                         ↑ success/failure signal from Level 0 execution
Level 0 leaf agent  ─── Executes job, reports exit_status (0=success, non-zero=failure)
```

## Configuration

Add the `mab` section to your config file (e.g., `config_swarm_multi.yml`):

```yaml
mab:
  enabled: true                    # Enable MAB for hierarchical delegation
  algorithm: "epsilon_greedy"      # "epsilon_greedy", "ucb1", or "linucb" (contextual)

  # Epsilon-Greedy parameters
  epsilon: 0.1                     # Initial exploration rate (probability of random selection)
  epsilon_decay: 0.995             # Decay multiplier applied after each update
  epsilon_min: 0.01                # Minimum epsilon floor

  # UCB1 parameters
  exploration_weight: 1.41         # sqrt(2) by default; higher = more exploration

  # Delegation control
  top_k: 1                         # Number of child groups to delegate to per job
                                   # Set to large number to recover original behavior

  # Non-stationarity (epsilon_greedy / ucb1)
  step_size: null                  # Recency-weighted Q update; null = lifetime average

  # Contextual bandit (algorithm: "linucb") — see CONTEXTUAL_BANDIT_DESIGN.md
  linucb:
    alpha: 1.0                     # Exploration width
    discount: 0.995                # Forgetting factor; 1.0 = standard LinUCB
  context:
    job_types: []                  # One-hot vocabulary, e.g. ["compute", "transfer"]
    failure_window: 20             # Window length for per-group/per-type failure rates
    # max_group_size / max_inflight / max_dtns / long_job_threshold / max_caps: normalization caps
  reward:
    shaped: false                  # Latency-shaped rewards instead of binary +/-1
    exit_failure: -0.5             # Shaped reward for non-zero exit status
    timeout: -1.0                  # Shaped reward for delegation timeout
    non_winner: null               # Reward for co-selected groups that lost the job

  # State persistence
  persist_to_redis: false          # Save/load bandit state across restarts
  persist_interval_s: 30.0         # Seconds between persistence writes

  # Failure simulation (for testing)
  failure_simulation:
    enabled: false                 # Enable simulated job failures
    failure_probability: 0.1       # Base failure rate (0.0-1.0)
    per_agent_failure_rates:       # Override for specific agents
      "3": 0.5                     # Agent 3 fails 50% of jobs
      "7": 0.3                     # Agent 7 fails 30% of jobs
    per_job_type_failure_rates:    # Override for specific job types
      "compute": 0.2
      "transfer": 0.05
```

## Algorithms

### Epsilon-Greedy

- With probability `epsilon`, selects a random child group (exploration)
- Otherwise, selects the group with highest Q-value (exploitation)
- Epsilon decays over time: `epsilon = max(epsilon_min, epsilon * epsilon_decay)`
- Good for: Simple scenarios, when you want predictable exploration rate

### UCB1 (Upper Confidence Bound)

- Balances exploitation and exploration using confidence intervals
- Score = Q-value + exploration_weight * sqrt(ln(total_pulls) / arm_pulls)
- Arms with zero pulls are selected first
- Good for: Optimizing regret, adaptive exploration based on uncertainty

### LinUCB (Contextual)

- One shared linear model scores each candidate group from a per-(job, group)
  feature vector: score = theta . x + alpha * sqrt(x . A^-1 . x)
- Learns job-dependent routing (e.g. which group succeeds at which job type)
  via job-x-group interaction features; non-contextual policies can only
  learn a single per-group average
- Newly added child groups are scored from their features immediately (no
  cold start); `discount` < 1 forgets stale observations under shifting load
- Full design, feature schema, and evaluation plan:
  [CONTEXTUAL_BANDIT_DESIGN.md](CONTEXTUAL_BANDIT_DESIGN.md)

## How It Works

1. **Job Selection**: When a coordinator selects a job, it identifies capable child groups via `_get_child_groups_for_job()`

2. **MAB Selection**: Instead of delegating to all capable groups, MAB picks `top_k` groups:
   ```python
   selected_groups = mab_manager.select_groups(capable_groups, job, top_k=1)
   ```

3. **Delegation Tracking**: MABManager stashes each selected (job, group)
   pair — with its selection-time feature vector in contextual mode — in a
   TTL-swept pending map for reward attribution (works for any `top_k`)

4. **Outcome Monitoring**: `_monitor_delegated_jobs()` checks job completion status at child level:
   - Reads `exit_status` from completed jobs in Redis
   - Credits the group where completion was observed, passing the
     delegation-to-completion latency
   - On delegation timeout, reports failure for every group the job was
     delegated to

5. **Reward Update**: MAB updates Q-values (and the LinUCB model in
   contextual mode):
   - Default: success = +1.0, failure/timeout = -1.0
   - With `reward.shaped: true`: success = 1 − latency/delegation_timeout,
     non-zero exit = `reward.exit_failure`, timeout = `reward.timeout`

## Running Tests

### Prerequisites

```bash
# Start Redis
docker run -d -p 6379:6379 redis

# Install dependencies
cd SwarmAgents
pip install -r requirements.txt
```

### Basic MAB Test

```bash
# Edit config_swarm_multi.yml to enable MAB:
#   mab.enabled: true
#   mab.failure_simulation.enabled: true

python run_test.py \
  --mode local \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical --hierarchical-level1-agent-type resource \
  --jobs 200 \
  --db-host localhost \
  --jobs-per-interval 20 \
  --run-dir runs/mab-test-001
```

### Test with Pre-Generated Failure Jobs

Generate jobs where a percentage are pre-marked to fail using `job_generator.py`:

```bash
# Generate 300 jobs with 20% base failure rate
python job_generator.py \
  --job-count 300 \
  --agent-profile-path configs/agent_profiles.json \
  --output-dir jobs/ \
  --failure-rate 0.2

# Generate jobs with per-agent failure rates (agent 3 fails 50%, agent 4 fails 30%)
python job_generator.py \
  --job-count 300 \
  --agent-profile-path configs/agent_profiles.json \
  --output-dir jobs/ \
  --failure-rate 0.1 \
  --failure-agents '{"3":0.5,"4":0.3}'
```

Then run the test (MAB will learn from the pre-determined failures):

```bash
python run_test.py \
  --mode local \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical --hierarchical-level1-agent-type resource \
  --jobs 300 \
  --db-host localhost \
  --jobs-per-interval 20 \
  --run-dir runs/mab-prefail-test
```

### Test with Runtime Failure Simulation

Alternatively, use runtime failure injection via config (failures injected during execution):

```yaml
mab:
  enabled: true
  top_k: 1
  failure_simulation:
    enabled: true
    failure_probability: 0.1
    per_agent_failure_rates:
      "3": 0.5    # Agent 3 in group X fails 50%
      "4": 0.5    # Agent 4 in group X fails 50%
```

Run and observe MAB learning to avoid the high-failure group:

```bash
python run_test.py \
  --mode local \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical --hierarchical-level1-agent-type resource \
  --jobs 300 \
  --db-host localhost \
  --jobs-per-interval 20 \
  --run-dir runs/mab-heterogeneous
```

### Batch Testing for Statistical Comparison

```bash
# Baseline (MAB disabled)
python batch_tests_v2.py \
  --runs 10 \
  --base-out runs/baseline-batch \
  --mode local \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical --hierarchical-level1-agent-type resource \
  --jobs 200 \
  --db-host localhost

# MAB enabled
python batch_tests_v2.py \
  --runs 10 \
  --base-out runs/mab-batch \
  --mode local \
  --agent-type resource \
  --agents 30 \
  --topology hierarchical --hierarchical-level1-agent-type resource \
  --jobs 200 \
  --db-host localhost
```

## Analyzing Results

### Plotting MAB Results

Use `plot_mab_results.py` to visualize MAB performance:

```bash
# Plot from Redis (immediately after a run)
python plot_mab_results.py --db-host localhost --output-dir runs/mab-test-001

# Plot from saved CSV/JSON files
python plot_mab_results.py --from-csv --output-dir runs/mab-test-001

# Compare multiple runs (baseline vs MAB)
python plot_mab_results.py --compare runs/baseline-001 runs/mab-test-001 --output-dir runs/comparison

# Print summary only (no plots)
python plot_mab_results.py --from-csv --output-dir runs/mab-test-001 --no-plots
```

Generated plots:
- `mab_q_values.png` - Learned Q-values per child group
- `mab_selections.png` - Selection distribution (bar + pie)
- `mab_success_rates.png` - Success/failure counts and rates per group
- `mab_rewards_over_time.png` - Cumulative reward and trend per group
- `job_exit_status.png` - Overall job success/failure distribution
- `mab_comparison.png` - Success rate comparison across runs (when using --compare)

### Check MAB Statistics (Manual)

```bash
# View MAB stats in Redis
python dump_db.py --host localhost --type redis | grep -A 50 mab_stats

# Search agent logs for MAB activity
grep -i "MAB" runs/mab-test-001/agent-*.log
```

### Key Metrics to Compare

| Metric | Description |
|--------|-------------|
| `mab_stats.arms[X].q_value` | Learned value for group X (higher = better) |
| `mab_stats.arms[X].successes` | Count of successful job completions |
| `mab_stats.arms[X].failures` | Count of failed job completions |
| `mab_selections[X]` | How many times group X was selected |
| Job success rate | Overall % of jobs with exit_status=0 |

### Expected Behavior

1. **Q-values diverge**: Groups with high-failure agents should have lower Q-values
2. **Selection shifts**: `mab_selections` should show more delegations to better groups
3. **Improved success rate**: With MAB, overall job success rate should improve vs baseline

## Architecture

### Files

| File | Description |
|------|-------------|
| `swarm/rl/__init__.py` | Package init |
| `swarm/rl/bandit.py` | `ArmStats`, `BanditPolicy`, `EpsilonGreedyPolicy`, `UCB1Policy` |
| `swarm/rl/mab_manager.py` | `MABManager` - bridges bandit with ResourceAgent |

### Integration Points in `resource_agent.py`

| Location | Change |
|----------|--------|
| `__init__` | Reads MAB config, initializes tracking structures |
| `_init_mab()` | Creates MABManager once children are known |
| `on_periodic()` | Calls `_init_mab()` after refreshing children |
| `scheduling_main()` | Uses MAB to select groups instead of all capable |
| `_monitor_delegated_jobs()` | Feeds success/failure rewards to MAB |
| `execute_job()` | Injects simulated failures for testing |
| `save_results()` | Exports MAB stats to metrics |

## Tuning Guide

### Exploration vs Exploitation

**Too much exploration** (high epsilon, high UCB exploration_weight):
- Delegates to suboptimal groups frequently
- Slower convergence to best groups
- Useful early in learning or in changing environments

**Too much exploitation** (low epsilon, low exploration_weight):
- May get stuck on initially-lucky groups
- Misses better groups discovered later
- Useful when environment is stable

### Recommended Starting Points

| Scenario | Algorithm | Parameters |
|----------|-----------|------------|
| Quick learning, stable environment | epsilon_greedy | epsilon=0.1, decay=0.99, min=0.01 |
| Noisy outcomes, need robustness | ucb1 | exploration_weight=1.41 |
| Very long runs | epsilon_greedy | epsilon=0.2, decay=0.999, min=0.01 |
| Few child groups (2-3) | ucb1 | exploration_weight=2.0 |

### top_k Setting

- `top_k=1`: Most aggressive learning, single group per job
- `top_k=2`: Hedged delegation, slower learning but more robust
- `top_k >= num_groups`: Reverts to original behavior (delegate to all)

## Troubleshooting

### MAB not activating

- Verify `mab.enabled: true` in config
- MAB only works with hierarchical topology (agents need children)
- Check logs for "MAB initialised for agent X"

### Q-values not diverging

- Ensure `failure_simulation.enabled: true` for testing
- Increase job count (need enough samples)
- Check that `top_k=1` so reward attribution is unambiguous

### State not persisting

- Set `persist_to_redis: true`
- Verify Redis connectivity
- Check logs for "MAB state persisted" / "MAB state loaded"

## Future Enhancements

- [ ] Contextual bandits (consider job features in selection) — designed in
      [CONTEXTUAL_BANDIT_DESIGN.md](CONTEXTUAL_BANDIT_DESIGN.md) (shared-model
      LinUCB); also covers Thompson Sampling (`lin_ts`), forgetting/discounting
      for non-stationary environments, and reward shaping.
      **Phases 1–3 done:** `LinUCBPolicy` + `ContextExtractor` implemented,
      selectable via `mab.algorithm: linucb`, and fully wired into
      ResourceAgent (live group snapshots, latency-shaped rewards, any-top_k
      attribution); at-scale evaluation (Phase 4) pending
- [x] Sliding window for non-stationary environments — optional `step_size`
      on Epsilon-Greedy/UCB1 (exponential recency-weighted Q) and `discount`
      on LinUCB
- [ ] Per-job-type bandits (separate learners for different job types) —
      subsumed by the contextual design (job type is a context feature)
