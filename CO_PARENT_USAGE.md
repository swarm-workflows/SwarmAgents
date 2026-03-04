# Multi-Parent Shared Parenting for Hierarchical Topology

This guide explains how to use the co-parent (shared parenting) feature in hierarchical topology mode. When enabled, each Level-0 child group has multiple Level-1 co-parents instead of a single parent. If the active parent dies, a co-parent takes over automatically.

## Overview

In the default hierarchical topology (K=1), each Level-0 child group has exactly one Level-1 parent agent. If that parent dies, the group is orphaned and no new jobs are delegated to it.

With shared parenting (K>1), each child group is assigned K co-parents from the Level-1 peer pool using a circular round-robin scheme. Only one co-parent is the active leader at any time; the others are warm standbys that continuously refresh child state so they can take over quickly on failure.

### Key Properties

- **Deterministic leader election**: Each co-parent independently determines the leader by checking which co-parents have fresh heartbeats. The lowest live agent ID wins. No distributed locks or extra Redis keys needed.
- **Backward compatible**: K=1 (the default) produces identical behavior to the existing 1:1 parent-child scheme.
- **Reduced orphan probability**: The probability of a group being orphaned drops from p to p^K (where p is the single-agent failure probability).

## Quick Start

### Prerequisites

- Minimum 30 agents for hierarchical topology
- Redis running (`docker run -d -p 6379:6379 redis`)

### Run with co-parenting enabled

```bash
cd SwarmAgents

python run_test_v2.py \
    --mode local \
    --agent-type resource \
    --agents 30 \
    --topology hierarchical \
    --hierarchical-level1-agent-type resource \
    --jobs 200 \
    --db-host localhost \
    --jobs-per-proposal 10 \
    --run-dir runs/co-parent-test \
    --co-parents 2
```

This creates 25 Level-0 agents in 5 groups of 5, plus 5 Level-1 agents — each child group gets 2 co-parents via circular assignment.

## How Co-Parent Assignment Works

Co-parents are assigned using **circular round-robin** across the Level-1 agent pool.

### Example: 30 agents, K=2

Level-1 agents: 26, 27, 28, 29, 30

| Child Group | Co-Parents |
|-------------|------------|
| 0           | [26, 27]   |
| 1           | [27, 28]   |
| 2           | [28, 29]   |
| 3           | [29, 30]   |
| 4           | [26, 30]   |

Each Level-1 agent co-parents 2 groups (its primary group and one neighbor's group). Agent 26 leads groups 0 and 4; agent 27 leads group 1 and is standby for group 0; etc.

### Three-level hierarchies

For three-level hierarchies (100, 990, 1000 agents), the circular assignment is applied within each super-group among the Level-1 agents in that super-group.

## Config Generation

### Direct config generation

```bash
python generate_configs.py 30 20 config_swarm_multi.yml configs/ hierarchical localhost 100 --co-parents 2
```

### Verify generated configs

**Check a Level-0 agent (should have `co_parents`):**
```bash
python3.11 -c "
import yaml
c = yaml.safe_load(open('configs/config_swarm_multi_1.yml'))
t = c['topology']
print(f'Agent 1: level={t[\"level\"]}, parent={t[\"parent\"]}, co_parents={t.get(\"co_parents\")}')
"
# Output: Agent 1: level=0, parent=26, co_parents=[26, 27]
```

**Check a Level-1 agent (should have `co_parent_groups` and expanded `children`):**
```bash
python3.11 -c "
import yaml
c = yaml.safe_load(open('configs/config_swarm_multi_26.yml'))
t = c['topology']
print(f'Agent 26: level={t[\"level\"]}, children={t[\"children\"]}')
print(f'  co_parent_groups={t.get(\"co_parent_groups\")}')
print(f'  primary_group={t.get(\"primary_group\")}')
"
# Output: Agent 26: level=1, children=[0, 4]
#   co_parent_groups={0: [26, 27], 4: [26, 30]}
#   primary_group=0
```

## Batch Testing

```bash
python batch_tests_v2.py \
    --runs 10 \
    --base-out runs/batch-co-parent \
    --mode local \
    --agent-type resource \
    --agents 30 \
    --topology hierarchical \
    --hierarchical-level1-agent-type resource \
    --jobs 200 \
    --db-host localhost \
    --co-parents 2
```

## Leader Election Details

Leader election is **local and deterministic** — no distributed locks required.

Each co-parent for a group independently evaluates:

1. Get the sorted list of co-parent agent IDs for the group
2. For each candidate (lowest ID first):
   - If the candidate is myself → I am the leader
   - If the candidate has a fresh heartbeat in `neighbor_map` → that candidate is the leader (not me)
   - If the candidate has no heartbeat → skip (assumed dead)
3. If all lower-ID candidates are dead → I am the leader

The `neighbor_map` is refreshed from Redis every periodic cycle (same mechanism used for peer expiry detection), so leader transitions happen within one refresh interval (~20s based on `peer_expiry_seconds`).

## Failover Behavior

### Normal operation
- Only the active leader delegates jobs to the child group
- Standby co-parents refresh child state but do not delegate
- Non-leader co-parents report zero aggregated capacity, so they don't win Level-1 consensus for jobs they can't delegate

### When the leader dies
1. Leader's heartbeat expires in Redis (within `peer_expiry_seconds`, ~20s)
2. Standby co-parents detect the stale heartbeat during their next `neighbor_map` refresh
3. The lowest-ID surviving co-parent becomes the new leader
4. New leader immediately starts delegating jobs to the group
5. Jobs queued during the transition gap are picked up by the new leader

### Edge cases

| Scenario | Behavior |
|----------|----------|
| **Leader transition gap** | ~20s window where no one delegates. Jobs queue in Redis and are delegated once new leader takes over. Level-0 children continue executing already-delegated jobs. |
| **Brief duplicate delegation** | Possible during heartbeat staleness window. Mitigated by Redis job_id idempotency and Level-0 consensus preventing double execution. |
| **All co-parents die** | Group orphaned (same as today with K=1). Probability drops from p to p^K. |
| **K=1 (default)** | Identical behavior to the original 1:1 parent-child scheme. |

## Monitoring and Debugging

### Check which agent is leading each group

Look for delegation log messages in Level-1 agent logs:

```bash
# Only the active leader should show delegation messages for a group
grep "Delegated job" runs/co-parent-test/agent-26.log | head
grep "Delegated job" runs/co-parent-test/agent-27.log | head
```

### Verify failover

1. Start a test with `--co-parents 2`
2. Kill one Level-1 agent during the run (e.g., `kill <pid>`)
3. Check that the co-parent picks up delegation:

```bash
# Should see delegation messages appear after the leader dies
grep "Delegated job" runs/co-parent-test/agent-27.log | tail -20
```

4. Verify no duplicate job execution at Level-0:

```bash
# Check for any job ID appearing more than once in SCHEDULED logs
grep "\[SCHEDULED\]" runs/co-parent-test/agent-*.log | \
    awk '{print $NF}' | sort | uniq -c | sort -rn | head
```

## Configuration Reference

### New CLI arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `--co-parents K` | 1 | Number of co-parents per child group in hierarchical topology |

### New config fields

**Level-0 agent topology section:**
```yaml
topology:
  parent: 26           # Primary parent (lowest-ID co-parent)
  co_parents: [26, 27] # All co-parent IDs (only present when K > 1)
```

**Level-1 agent topology section:**
```yaml
topology:
  children: [0, 4]                          # All groups this agent co-parents
  co_parent_groups:                         # Map of group → co-parent list (only when K > 1)
    0: [26, 27]
    4: [26, 30]
  primary_group: 0                          # Original 1:1 group (for debugging)
```
