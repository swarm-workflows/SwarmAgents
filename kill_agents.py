#!/usr/bin/env python3
"""
Script to kill agents for failure testing.

Supports:
- Local and remote agent killing
- Kill all at once or gradually (with interval)
- Kill specific agents or random selection
- Configurable kill signal (SIGTERM, SIGKILL, SIGINT)
"""

import argparse
import subprocess
import time
import random
import sys
from typing import List, Optional


class AgentKiller:
    def __init__(self, mode: str = 'local', hosts_file: Optional[str] = None,
                 ssh_user: Optional[str] = None, ssh_key: Optional[str] = None):
        self.mode = mode
        self.hosts_file = hosts_file
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.hosts = []

        if mode == 'remote' and hosts_file:
            self._load_hosts()

    def _load_hosts(self):
        """Load remote host list from file."""
        with open(self.hosts_file, 'r') as f:
            self.hosts = [line.strip() for line in f if line.strip() and not line.startswith('#')]
        print(f"Loaded {len(self.hosts)} remote hosts")

    def _find_local_agents(self, pattern: str = 'main.py') -> List[dict]:
        """Find agent processes running locally."""
        try:
            # Find processes matching the pattern
            result = subprocess.run(
                ['ps', 'aux'],
                capture_output=True,
                text=True,
                check=True
            )

            agents = []
            for line in result.stdout.split('\n'):
                if pattern in line and 'grep' not in line and 'kill_agents.py' not in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        pid = parts[1]
                        # Try to extract agent ID from command line
                        agent_id = None

                        # Method 1: --agent-id flag
                        if '--agent-id' in line:
                            try:
                                idx = parts.index('--agent-id')
                                if idx + 1 < len(parts):
                                    agent_id = parts[idx + 1]
                            except (ValueError, IndexError):
                                pass

                        # Method 2: positional arg after main.py (e.g., "python main.py 5")
                        if agent_id is None and 'main.py' in parts:
                            try:
                                main_idx = parts.index('main.py')
                                if main_idx + 1 < len(parts):
                                    potential_id = parts[main_idx + 1]
                                    if potential_id.isdigit():
                                        agent_id = potential_id
                            except (ValueError, IndexError):
                                pass

                        agents.append({
                            'pid': pid,
                            'agent_id': agent_id,
                            'host': 'localhost',
                            'cmdline': ' '.join(parts[10:])[:100]  # First 100 chars of command
                        })

            return agents
        except subprocess.CalledProcessError as e:
            print(f"Error finding local agents: {e}")
            return []

    def _find_remote_agents(self, host: str, pattern: str = 'main.py') -> List[dict]:
        """Find agent processes running on a remote host."""
        try:
            ssh_cmd = ['ssh']
            if self.ssh_key:
                ssh_cmd.extend(['-i', self.ssh_key])

            target = f"{self.ssh_user}@{host}" if self.ssh_user else host
            ssh_cmd.extend([target, 'ps aux'])

            result = subprocess.run(
                ssh_cmd,
                capture_output=True,
                text=True,
                check=True,
                timeout=10
            )

            agents = []
            for line in result.stdout.split('\n'):
                if pattern in line and 'grep' not in line:
                    parts = line.split()
                    if len(parts) >= 2:
                        pid = parts[1]
                        agent_id = None
                        if '--agent-id' in line:
                            idx = line.split().index('--agent-id')
                            if idx + 1 < len(line.split()):
                                agent_id = line.split()[idx + 1]

                        # Method 2: positional arg after main.py (e.g., "python main.py 5")
                        if agent_id is None and 'main.py' in parts:
                            try:
                                main_idx = parts.index('main.py')
                                if main_idx + 1 < len(parts):
                                    potential_id = parts[main_idx + 1]
                                    if potential_id.isdigit():
                                        agent_id = potential_id
                            except (ValueError, IndexError):
                                pass

                        agents.append({
                            'pid': pid,
                            'agent_id': agent_id,
                            'host': host,
                            'cmdline': ' '.join(parts[10:])[:100]
                        })

            return agents
        except subprocess.CalledProcessError as e:
            print(f"Error finding agents on {host}: {e}")
            return []
        except subprocess.TimeoutExpired:
            print(f"Timeout connecting to {host}")
            return []

    def find_all_agents(self, pattern: str = 'main.py') -> List[dict]:
        """Find all agent processes (local or remote)."""
        all_agents = []

        if self.mode == 'local':
            all_agents = self._find_local_agents(pattern)
        elif self.mode == 'remote':
            for host in self.hosts:
                print(f"Scanning {host}...")
                agents = self._find_remote_agents(host, pattern)
                all_agents.extend(agents)

        return all_agents

    def kill_agent(self, agent: dict, signal: str = 'SIGTERM') -> bool:
        """Kill a single agent."""
        host = agent['host']
        pid = agent['pid']
        agent_id = agent.get('agent_id', 'unknown')

        try:
            if host == 'localhost':
                # Local kill
                result = subprocess.run(
                    ['kill', f'-{signal}', pid],
                    capture_output=True,
                    text=True,
                    timeout=5
                )
                success = result.returncode == 0
            else:
                # Remote kill
                ssh_cmd = ['ssh']
                if self.ssh_key:
                    ssh_cmd.extend(['-i', self.ssh_key])

                target = f"{self.ssh_user}@{host}" if self.ssh_user else host
                ssh_cmd.extend([target, f'kill -{signal} {pid}'])

                result = subprocess.run(
                    ssh_cmd,
                    capture_output=True,
                    text=True,
                    timeout=10
                )
                success = result.returncode == 0

            if success:
                print(f"✓ Killed agent {agent_id} (PID {pid}) on {host}")
            else:
                print(f"✗ Failed to kill agent {agent_id} (PID {pid}) on {host}: {result.stderr}")

            return success
        except subprocess.TimeoutExpired:
            print(f"✗ Timeout killing agent {agent_id} (PID {pid}) on {host}")
            return False
        except Exception as e:
            print(f"✗ Error killing agent {agent_id} (PID {pid}) on {host}: {e}")
            return False

    def kill_agents(self, agents: List[dict], interval: float = 0,
                    signal: str = 'SIGTERM', random_order: bool = False):
        """
        Kill multiple agents.

        :param agents: List of agent dicts to kill
        :param interval: Seconds to wait between kills (0 = all at once)
        :param signal: Signal to send (SIGTERM, SIGKILL, SIGINT)
        :param random_order: Randomize kill order
        """
        if random_order:
            agents = agents.copy()
            random.shuffle(agents)

        total = len(agents)
        killed = 0

        print(f"\n{'='*60}")
        print(f"Killing {total} agents with signal {signal}")
        if interval > 0:
            print(f"Interval: {interval}s between kills")
        else:
            print(f"Mode: Kill all at once")
        print(f"{'='*60}\n")

        for i, agent in enumerate(agents, 1):
            print(f"[{i}/{total}] ", end='')
            if self.kill_agent(agent, signal):
                killed += 1

            # Wait between kills (except for last one)
            if interval > 0 and i < total:
                print(f"Waiting {interval}s before next kill...")
                time.sleep(interval)

        print(f"\n{'='*60}")
        print(f"Summary: Killed {killed}/{total} agents")
        print(f"{'='*60}\n")


def main():
    parser = argparse.ArgumentParser(
        description='Kill agents for failure testing',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Kill all local agents at once
  python kill_agents.py --mode local --all

  # Kill agents one every 30 seconds
  python kill_agents.py --mode local --all --interval 30

  # Kill 5 random agents
  python kill_agents.py --mode local --count 5 --random

  # Kill specific agent IDs
  python kill_agents.py --mode local --agent-ids 5 9 12

  # Kill remote agents
  python kill_agents.py --mode remote --hosts-file hosts.txt --all --ssh-user ubuntu

  # Use SIGKILL instead of SIGTERM
  python kill_agents.py --mode local --all --signal SIGKILL
        """
    )

    # Mode selection
    parser.add_argument('--mode', choices=['local', 'remote'], default='local',
                        help='Kill local or remote agents')

    # Agent selection
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--all', action='store_true',
                       help='Kill all agents found')
    group.add_argument('--count', type=int,
                       help='Kill N agents (randomly selected if --random)')
    group.add_argument('--agent-ids', type=int, nargs='+',
                       help='Kill specific agent IDs')

    # Kill options
    parser.add_argument('--interval', type=float, default=0,
                        help='Seconds to wait between kills (default: 0 = all at once)')
    parser.add_argument('--signal', default='SIGTERM',
                        choices=['SIGTERM', 'SIGKILL', 'SIGINT'],
                        help='Signal to send to agents (default: SIGTERM)')
    parser.add_argument('--random', action='store_true',
                        help='Randomize kill order')
    parser.add_argument('--pattern', default='main.py',
                        help='Process pattern to match (default: main.py)')

    # Remote options
    parser.add_argument('--hosts-file', type=str,
                        help='File containing list of remote hosts (one per line)')
    parser.add_argument('--ssh-user', type=str,
                        help='SSH username for remote hosts')
    parser.add_argument('--ssh-key', type=str,
                        help='SSH private key path')

    # Dry run
    parser.add_argument('--dry-run', action='store_true',
                        help='Show what would be killed without actually killing')

    args = parser.parse_args()

    # Validate remote mode
    if args.mode == 'remote' and not args.hosts_file:
        parser.error('--hosts-file required for remote mode')

    # Create killer
    killer = AgentKiller(
        mode=args.mode,
        hosts_file=args.hosts_file,
        ssh_user=args.ssh_user,
        ssh_key=args.ssh_key
    )

    # Find agents
    print("Searching for agents...")
    all_agents = killer.find_all_agents(args.pattern)

    if not all_agents:
        print("No agents found!")
        sys.exit(1)

    print(f"\nFound {len(all_agents)} agents:")
    for agent in all_agents:
        agent_id = agent.get('agent_id', 'unknown')
        print(f"  - Agent {agent_id} (PID {agent['pid']}) on {agent['host']}")

    # Select agents to kill
    agents_to_kill = []

    if args.all:
        agents_to_kill = all_agents
    elif args.count:
        if args.count > len(all_agents):
            print(f"\nWarning: Requested {args.count} agents but only {len(all_agents)} available")
            agents_to_kill = all_agents
        else:
            if args.random:
                agents_to_kill = random.sample(all_agents, args.count)
            else:
                agents_to_kill = all_agents[:args.count]
    elif args.agent_ids:
        # Filter by agent IDs
        requested_ids = set(str(aid) for aid in args.agent_ids)
        for agent in all_agents:
            if agent.get('agent_id') in requested_ids:
                agents_to_kill.append(agent)

        if len(agents_to_kill) < len(args.agent_ids):
            found_ids = {agent.get('agent_id') for agent in agents_to_kill}
            missing = requested_ids - found_ids
            print(f"\nWarning: Could not find agents with IDs: {missing}")

    if not agents_to_kill:
        print("\nNo agents selected to kill!")
        sys.exit(1)

    # Dry run
    if args.dry_run:
        print(f"\n{'='*60}")
        print("DRY RUN - Would kill the following agents:")
        print(f"{'='*60}")
        for i, agent in enumerate(agents_to_kill, 1):
            agent_id = agent.get('agent_id', 'unknown')
            print(f"  {i}. Agent {agent_id} (PID {agent['pid']}) on {agent['host']}")
        print(f"\nSignal: {args.signal}")
        print(f"Interval: {args.interval}s")
        print(f"Random order: {args.random}")
        sys.exit(0)

    # Confirm before killing
    print(f"\n{'='*60}")
    print(f"About to kill {len(agents_to_kill)} agents")
    print(f"Signal: {args.signal}")
    if args.interval > 0:
        print(f"Interval: {args.interval}s between kills")
        print(f"Total time: ~{args.interval * (len(agents_to_kill) - 1):.1f}s")
    print(f"{'='*60}")

    response = input("\nContinue? [y/N]: ")
    if response.lower() != 'y':
        print("Cancelled.")
        sys.exit(0)

    # Kill agents
    killer.kill_agents(
        agents_to_kill,
        interval=args.interval,
        signal=args.signal,
        random_order=args.random
    )


if __name__ == '__main__':
    main()
