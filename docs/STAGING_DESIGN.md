# Data staging — moving a workflow's files between agents

**Status 2026-09-21: slice 1 implemented** (location registry, transfer service, stage-in).
Stage-out and producer-durability are specified here and deliberately not built — see §6.

Until now "staging" meant a local copy from a root the agent could already see. On the slice that
root is one NFS export mounted at the same path everywhere, which works and is what validated the
real-execution path — but it makes every agent equidistant from every file, which is exactly the
variable the scheduler's DTN penalties price. Nothing moved data between agents and nothing staged
out. This is the piece that closes that.

## 1. What the tree already had, and why the design follows from it

| Piece | Where | What it already does |
|---|---|---|
| `Job.data_in` / `data_out` | `swarm/models/data_node.py` | Logical file names per job, with `size_bytes`. Already the single declaration of what a job reads and writes — **no second list is introduced here.** |
| Readiness registry | `repository.py`, `data_ready:<run>:<level>:names` | A Redis SET of names that exist. Answers *whether*, never *where*. |
| Completion + publication as one write | `repository.save(..., produced_data=[...])` | The names land in the same MULTI as COMPLETE, so an agent dying between the two cannot gate a subtree forever. |
| `stage_inputs` | `swarm/execution/runner.py` | Never overwrites, copies atomically (`os.link`, not `os.replace`), refuses a missing input. |
| DAG gating | `_data_predicate_ready` | A job is not selectable until its parents' names are in the registry. |

So the gap is exactly one thing: **the registry records existence, not location.** Everything else
— when a file is ready, who declared it, how to put it in place without corrupting a concurrent
writer — is already solved and is reused unchanged.

## 2. Transport: a second gRPC port on the same address as consensus

Two candidates were considered on the live slice, and the addressing decided it.

- **SSH/scp.** `notebooks/db_node_setup/02_ssh_mesh.sh` gives root SSH between *all* nodes, so this
  needs no new code. Rejected: `ssh agent-N` resolves through `/etc/hosts` to the management path,
  **not** the FABNet data plane that consensus uses and that E3a bins by RTT. Data would move over
  a different network than the one being measured, and every data-movement number would describe
  the wrong link. This project has been bitten three times by exactly that shape of error (NFS
  flattening locality; sequential placement putting a group's consensus on one LAN; clocks).
- **A second gRPC service on the agent's own `grpc.host`.** Chosen. `grpc.host`/`port` is what the
  agent binds and advertises in `AgentInfo`, and what `transport.send` dials — the data-plane
  address. A second **port** on that same host therefore traverses the same measured paths.

**A separate server and port, not a second RPC on `ConsensusService`.** A 65 MB transfer sharing a
channel with consensus would queue behind, and ahead of, the very messages whose latency the paper
measures. Separate ports keep bulk data off the consensus path, and keep the transport's message
counters (P0-4) counting consensus only.

`runtime.execution.staging.port_offset` (default 1000) sets the data port as `grpc.port +
offset`, so a fleet that already allocates one port per agent gets the second for free with no new
config per agent. The firewall on the slice restricts only the uplink, so intra-slice ports are
open.

## 3. The location registry

The readiness SET is unchanged and stays the scheduling path's one round trip (`smismember` for
every gated job on every pass). A **HASH alongside it**, `data_loc:<run>:<level>:names`, maps
logical name → `{agent_id, host, port, produced_at}`, written **inside the same MULTI** as the
completion and the readiness names. The invariant that made publication safe is preserved: a
descendant can never observe a name as ready without also being able to find it.

What is deliberately *not* in a location record:

- **No path.** The serving agent resolves the name against its own published map (§4); publishing
  a path invites a consumer to read it directly, which works only under the shared mount the whole
  exercise is trying to stop depending on.
- **No checksum.** Hashing every output at publish time costs wall clock proportional to output
  size, on the critical path of a run whose makespan is a headline number. The transfer computes a
  digest *while streaming* and the receiver verifies it, which catches the failure that actually
  matters — a truncated or corrupted transfer — and costs nothing when nothing is transferred.

## 4. Serving: a published map, never an arbitrary path

The transfer server answers only for names **this agent has published in this run**. It keeps
`name → absolute path` in memory, populated as the agent publishes its outputs, and a request for
anything else is refused by name. The server never joins a request onto a directory, so there is no
traversal surface: an unknown name has no path at all.

A request also carries the run id and is refused if it does not match, which is the same guard the
readiness registry's run-scoped key provides.

## 5. Staging in

`stage_inputs` keeps its three rules and gains a source, in this order — and **the order is
load-bearing**, not a preference:

1. **Already in the working directory** → nothing to do (a parent that ran on this agent, or
   another agent's copy under a shared mount).
2. **Produced by this run, on another agent** → look the name up in the location registry and
   fetch it from the producer into a temp file, verify the digest, then `os.link` it into place —
   the same never-overwrite, atomic rule the local path uses, for the same reason.
3. **Resolvable under `roots.inputs`** → the existing local copy. **This is the shared-mount path
   and is otherwise unchanged**, so a run configured as today behaves exactly as today.
4. Otherwise refuse, naming which of the three it was.

**Step 2 deliberately precedes step 3**, which is the reverse of the first draft of this document.
Workflow file names are a flat namespace — 62 colliding names measured in the shipped profile — so
a name in the location registry was produced *by this run*, and a file of the same name sitting in
the inputs root is a collision, not a copy. Taking the root would feed a child last week's file and
look entirely healthy. A produced file that cannot be fetched is therefore a **refusal naming the
producer**, never a fall back to step 3.

**A lookup that is unavailable or fails is a refusal, not a fall back to step 3.** The first
version of this warned and resolved locally — a brief Redis outage should not fail a job whose
input is sitting on disk. That reasoning does not survive staging being on: a name a parent
produced lives on another agent, the lookup is the only thing that knows which, and resolving it
from a same-named file in the inputs root is precisely the stale-collision read this ordering
exists to prevent, reached through the error path instead of the happy one. The fallback was more
permissive than the path it stood in for, which is the shape of at least four defects already in
`CODE_REVIEW_2026-09-18.md`. Refusing is loud and retryable; a stale input is silent and produces
plausible numbers. Staging *off* keeps the old, harmless behaviour, because with no
produced-elsewhere names there is nothing to confuse a local resolve with.

**The refusal is per name, not per job.** Its first version returned before any input was
examined, which rejected jobs that could not possibly read a stale file: one with no declared
inputs at all, and one whose inputs a parent on this very agent had already written into the
working directory. Neither consults the inputs root, so neither is ambiguous. The guard now sits
exactly where a name has fallen past the working directory and is about to be resolved from the
root — the one place the ambiguity is real. The residual conservatism is narrow and deliberate: a
genuine DAG-root input, present only in `roots.inputs`, is refused while the registry is
unreachable, because nothing available at that moment distinguishes it from a produced name.

A fetch failure is a refusal, not a job failure: it is a configuration or fleet problem that will
repeat, and the existing refusal path already reports it that way.

## 6. What is NOT built, and must be decided before a makespan is quoted

- **Stage-out.** A run's final outputs live wherever they were produced. There is no collection to
  a destination. Cheap to add once a destination is chosen (another agent, the database node, an
  external endpoint), and it is the second half of any Pegasus comparison.
- **Producer durability — the one that changes correctness.** Under the shared mount a dead agent's
  outputs survive. With per-agent storage they do not: the producing job is COMPLETE, so nothing
  re-runs it, its name is in the readiness registry, so every descendant is released — and then
  refuses to stage. The system would deadlock on a dead producer in a way that looks like a staging
  bug. Three options, in increasing cost: **(a)** treat an unreachable producer as a reason to
  reset the producing job to PENDING and let the existing reassignment machinery re-run it (needs
  the readiness name retracted in the same transaction, and is only correct for a deterministic
  job); **(b)** replicate each output to *k* peers at publish time, which costs bandwidth on the
  critical path; **(c)** write outputs to the shared export and use staging only for locality
  measurement, which is honest but gives up the durability argument. **Until one is chosen, do not
  run a failure-injection cell with staging enabled** — E2b and E6 kill agents, and a killed
  producer is exactly this case.
- **Transfer accounting.** Bytes moved per job, per link, are not yet in `collect.py`. The figure
  that would make staging a *result* rather than a capability needs it.

## 6a. A dependency the regeneration moved

Adding `DataTransferService` regenerated `consensus_pb2.py`, which stamps the toolchain version
into `ValidateProtobufRuntimeVersion` — and that **raises** when the installed runtime is older
than the gencode. The floor went from 5.29.0 to 6.31.1 as a side effect, and that module is
imported by the consensus transport, so too old a runtime stops every agent rather than just
staging. `requirements.txt` states `protobuf>=6.31.1` instead of leaving it implied. Verified on
the slice 2026-09-21: agents 7.35.1, database node 7.36.1, module imports cleanly on both.
**Re-check the pin after any protoc run.**

## 7. Configuration

```yaml
runtime:
  execution:
    mode: real
    work_dir: /var/tmp/swarm-wf/work     # LOCAL per agent when staging is on
    staging:
      enabled: false                     # off by default: the shared-mount path is unchanged
      port_offset: 1000                  # data port = grpc.port + this
      chunk_bytes: 1048576               # 1 MiB
      timeout_s: 300.0                   # per fetch
      verify: true                       # digest the stream and check it at the receiver
```

`enabled: false` is the shipped default, so nothing about an existing run changes until a run asks
for staging. With `enabled: true` the work dir must be **local** to each agent — a shared work dir
would make every fetch a no-op and measure nothing.
