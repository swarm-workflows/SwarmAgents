#!/usr/bin/env python3.11
"""Pick active agents by querying Redis for recent job assignments.

Redis key format: job:<level>:<group>:<id> -> JSON with leader_id field
Agent key format: agent:<level>:<group>:<id> -> JSON with agent_id field

Usage:
    python3.11 pick_active_agents.py --db-host HOST --count N [--level 0]
    python3.11 pick_active_agents.py --db-host HOST --count N --site-outage --group 0

Prints comma-separated agent IDs to stdout.
"""
import argparse, random, sys, json, redis


def get_active_l0_agents(r):
    """Find L0 agents that have been assigned jobs."""
    active = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="job:0:*", count=1000)
        for key in keys:
            try:
                val = r.get(key)
                if val:
                    data = json.loads(val)
                    lid = data.get("leader_id")
                    if lid is not None and 1 <= int(lid) <= 100:
                        active.add(int(lid))
            except Exception as e:
                print(f"Warning: failed to parse job data for {key}: {e}", file=sys.stderr)
        if cursor == 0:
            break
    return sorted(active)


def get_active_l1_agents(r):
    """Find L1 coordinators that have delegated jobs."""
    active = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match="job:1:*", count=1000)
        for key in keys:
            try:
                val = r.get(key)
                if val:
                    data = json.loads(val)
                    lid = data.get("leader_id")
                    if lid is not None and 101 <= int(lid) <= 110:
                        active.add(int(lid))
            except Exception as e:
                print(f"Warning: failed to parse job data for {key}: {e}", file=sys.stderr)
        if cursor == 0:
            break
    return sorted(active)


def get_agents_in_group(r, group, level=0):
    """Find all registered agents in a specific group."""
    agents = set()
    cursor = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match=f"agent:{level}:{group}:*", count=100)
        for key in keys:
            try:
                val = r.get(key)
                if val:
                    data = json.loads(val)
                    aid = data.get("agent_id")
                    if aid is not None:
                        agents.add(int(aid))
            except Exception as e:
                print(f"Warning: failed to parse agent data for {key}: {e}", file=sys.stderr)
        if cursor == 0:
            break
    return sorted(agents)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-host", default="database")
    parser.add_argument("--db-port", type=int, default=6379)
    parser.add_argument("--count", type=int, required=True,
                        help="Number of L0 agents to pick")
    parser.add_argument("--distributed", action="store_true",
                        help="Pick agents spread across groups (1-2 per group)")
    parser.add_argument("--site-outage", action="store_true",
                        help="Pick all agents from one group + its coordinator")
    parser.add_argument("--group", type=int, default=-1,
                        help="Target group for site-outage (-1 = auto-pick active group)")
    args = parser.parse_args()

    r = redis.Redis(host=args.db_host, port=args.db_port, decode_responses=False)

    active_l0 = get_active_l0_agents(r)
    active_l1 = get_active_l1_agents(r)
    print(f"Active L0 agents ({len(active_l0)}): {active_l0}", file=sys.stderr)
    print(f"Active L1 agents ({len(active_l1)}): {active_l1}", file=sys.stderr)

    if args.site_outage:
        # Find all agents in the target group + its coordinator
        if args.group >= 0:
            target_group = args.group
        else:
            # Pick a group that has active agents
            group_activity = {}
            for aid in active_l0:
                g = (aid - 1) // 10  # group = (agent_id - 1) // 10
                group_activity.setdefault(g, []).append(aid)
            if group_activity:
                # Pick the group with the most active agents
                target_group = max(group_activity, key=lambda g: len(group_activity[g]))
            else:
                target_group = random.randint(0, 9)

        # Get all agents in this group from Redis
        group_agents = get_agents_in_group(r, target_group, level=0)
        if not group_agents:
            # Fallback: compute from ID range
            start = target_group * 10 + 1
            group_agents = list(range(start, start + 10))

        # Add the coordinator for this group (101 + group)
        coordinator = 101 + target_group
        picked = sorted(group_agents + [coordinator])
        print(f"Site outage: group {target_group}, agents {group_agents}, "
              f"coordinator {coordinator}", file=sys.stderr)
        print(",".join(str(a) for a in picked))
        return

    if args.distributed:
        # Pick agents spread across groups, preferring active ones
        picked = []
        per_group = max(1, args.count // 10)
        remainder = args.count - per_group * 10

        for g in range(10):
            group_active = [a for a in active_l0 if (a - 1) // 10 == g]
            start = g * 10 + 1
            group_all = list(range(start, start + 10))

            if group_active:
                n = min(per_group, len(group_active))
                picked.extend(random.sample(group_active, n))
            else:
                # No active agents in this group, pick random
                picked.extend(random.sample(group_all, per_group))

        # Distribute remainder across groups with most active agents
        if remainder > 0:
            remaining_active = [a for a in active_l0 if a not in picked]
            if remaining_active:
                extra = min(remainder, len(remaining_active))
                picked.extend(random.sample(remaining_active, extra))

        picked = sorted(picked[:args.count])
    else:
        # Pick N random active agents
        if len(active_l0) >= args.count:
            picked = sorted(random.sample(active_l0, args.count))
        else:
            # Not enough active, supplement with random
            picked = list(active_l0)
            all_l0 = [i for i in range(1, 101) if i not in picked]
            need = args.count - len(picked)
            picked.extend(random.sample(all_l0, min(need, len(all_l0))))
            picked = sorted(picked)

    print(f"Picked {len(picked)} agents: {picked}", file=sys.stderr)
    print(",".join(str(a) for a in picked))


if __name__ == "__main__":
    main()
