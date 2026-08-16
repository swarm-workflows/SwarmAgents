# DB-node-driven cluster setup

Replaces the per-node configuration cells of `SWARM-2slice.ipynb` (cells
15–28) when direct SSH from your laptop to the VMs times out. Only the
**database node** needs to be reachable (your working `ssh swarm` alias);
everything else is configured *from* the database node over the FABRIC
management network.

**No passwordless access between nodes is required up front.** Bootstrap SSH
uses your FABRIC sliver key (the one already injected into every VM as the
`ubuntu` user); step 2 then builds the root SSH mesh that the SWARM tooling
(`push_swarmagents.sh`, `run_test.py --mode remote`) relies on.

## Prerequisites

- Both slice parts up (`StableOK`): run the notebook through
  `post_boot_config()` for part 1 and part 2. Slice creation talks only to
  the FABRIC orchestrator, so it is unaffected by the SSH timeouts.
- A working `ssh swarm` alias to the database node.
- fablib configured locally (same environment the notebook runs in).

## Workflow

### 1. Generate the plan (laptop, control-plane only — no node SSH)

```bash
cd notebooks/db_node_setup
python3 gen_inventory.py                  # defaults match SWARM-2slice.ipynb
# options: --total-agents 100 --branch swarm-multi-deploy --slice1/--slice2 ...
```

Writes `plan/`: management IPs + users, NIC MACs with their assigned
FABNetv4 IPs/gateways (same deterministic assignment as the notebook),
`/etc/hosts` block, `prometheus.yml`, bastion info.

### 2. Upload the bundle (laptop)

```bash
./upload_to_db.sh swarm
# key paths are auto-detected from fablib; override with
#   SLICE_KEY=~/.ssh/slice_key BASTION_KEY=~/.ssh/fabric_bastion_key ./upload_to_db.sh swarm
```

Ships these scripts, `plan/`, `node_tools/`, `push_swarmagents.sh`, the
monitoring tooling, and your sliver key to `~/swarm-setup/` on the database
node. The bastion key is optional — upload it if some sites are IPv6-only
and the database node can't reach their management IPs directly; SSH then
falls back to a ProxyJump through the FABRIC bastion automatically.

### 3. Run the setup (database node)

```bash
ssh swarm
tmux new -s setup            # recommended; the full run takes a while
cd ~/swarm-setup/db_node_setup
./setup_all.sh               # or ./setup_all.sh 3 to resume from step 3
```

| Step | Script | Notebook equivalent |
|------|--------|---------------------|
| 0 | `00_check_access.sh` | — (probes SSH to every node, caches direct/bastion transport) |
| 1 | `01_netplan.sh` | cells 15–16 (upload node_tools, netplan per NIC by MAC, ping check) |
| 2 | `02_ssh_mesh.sh` | cells 17–18 (root keypairs + cross-distributed authorized_keys) |
| 3 | `03_etc_hosts.sh` | cell 19 (hosts block, idempotent via markers) |
| 4 | `04_push_swarm.sh` | cell 20 (clone + push SwarmAgents to all agents) |
| 5 | `05_deps.sh` | cell 28 (pip requirements + protobuf pin) |
| 6 | `06_monitoring.sh` | cells 23–25 (node_exporter, Prometheus/Grafana, target check) |

All steps are idempotent and safe to re-run. Per-node logs land in
`logs/<step-func>-<node>.log`; a step fails loudly listing the failed nodes.
Parallelism is 16 nodes at a time (`PARALLEL=32 ./setup_all.sh` to change).

### 4. Run the experiment (database node, as before)

```bash
sudo bash -c "cd /root/SwarmAgents && docker compose up -d redis"
sudo bash -c "cd /root/SwarmAgents && ./batch_tests_v2.py --runs 1 --base-out run-h-30-100 \
    --mode remote --agent-type resource --agents 30 --topology hierarchical \
    --hierarchical-level1-agent-type resource --jobs 100 --db-host database \
    --job-interval 120 --jobs-per-interval 1"
```

Fetch results: `scp -r swarm:/root/SwarmAgents/run-h-30-100 .`

Grafana/Prometheus from your laptop (monitor VM is only on FABNetv4, so
tunnel via the db node): `ssh -L 3000:<monitor-ip>:3000 -L 9090:<monitor-ip>:9090 swarm`
(step 6 prints the monitor IP).

## Where to run the steps

Steps 0–3, 5 and 6 work identically from the database node *or* from your
laptop — they only drive other nodes over SSH. Step 4 always executes on the
database node itself (it is dispatched there over SSH), so either location is
fine.

Running from the **database node** is still preferable for long runs: it
keeps the bastion hop out of the picture for same-site nodes and survives
your laptop sleeping. Use `tmux`.

## SSH transport notes

- Most FABRIC management addresses are **IPv6** (84 of 94 in this slice).
  Bastion `ProxyCommand` hops therefore must bracket the target as
  `-W [%h]:%p`; an unbracketed IPv6 literal fails with *"Bad stdio
  forwarding specification"*. This is handled by `_wspec` in `lib.sh`.
- The FABRIC bastion account has **no shell** — connecting to it directly
  prints *"This account is currently not available."* That is normal; only
  TCP forwarding is permitted, so the multiplexing master is opened with
  `ssh -fN` and verified with `ssh -O check`.
- **FABRIC runs several bastions and they do not all reach every site.**
  HAWI, for example, is reachable only via `bastion-star-1`, not via the
  default `bastion.fabric-testbed.net`. `lib.sh` therefore tries each
  bastion in `BASTION_HOSTS` per node and remembers the one that worked
  (`state/transport/<node>` holds `bastion:<host>`). Override the list with
  `BASTION_HOSTS="host-a host-b" ./00_check_access.sh`. Bastions that fail
  to prime are pruned so they don't burn timeouts on every node.
- Bastion hops are multiplexed (`ControlMaster`/`ControlPersist`), which
  keeps the bastion from rate-limiting a 94-node fan-out. Control sockets
  live in `/tmp/swm-cm-<uid>/` because Unix socket paths are capped at ~104
  bytes — a socket under `state/` fails with *"ControlPath too long"*.
- *"Connection timed out during banner exchange"* means the bastion reached
  the VM but its `sshd` never answered. Try the other bastions first; if all
  fail, the VM or site is genuinely sick — check the slice in the portal.

## Notes

- The sliver key sits at `~/swarm-setup/db_node_setup/keys/slice_key` on the
  database node; it only grants access to your own slice VMs, but you can
  delete it after setup — the root mesh from step 2 is what the SWARM
  tooling uses from then on.
- `plan/` must be regenerated (steps 1–2 of the workflow) after any slice
  modification that adds/removes nodes or NICs.
- The notebook's `install.sh` cell (cell 21) references a file that does not
  exist in the repo; it is intentionally not part of this pipeline — the pip
  installs of cell 28 are covered by step 5.
