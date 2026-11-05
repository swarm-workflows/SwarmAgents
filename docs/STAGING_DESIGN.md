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

**A staging site's namespace is `(run, name)`, not `name`.** Files are stored under
`<store_dir>/<run_id>/<name>` and looked up the same way. The first version was flat, and
because placement is never-overwrite, a second run producing the same file name hit the
existing copy, kept the *older* one, reported a clean store of bytes it had discarded, and then
served run 1's content to run 2's consumers. The registry keys are run-scoped
(`data_ready:<run>`, `data_loc:<run>`) but the store directory was not, and workflow file names
are a flat namespace — 62 colliding names measured in the shipped profile — so two runs of the
same workflow is the expected case, not a corner. An upload carrying no run id is refused,
because there is nowhere to file it; an agent, which serves one run's outputs and already
checks the run for equality, still serves bare names. Within a single run, a second upload of
the same name with *different* bytes keeps the first copy and logs at ERROR, rather than
reporting success for bytes that were dropped.

The transfer server answers only for names **this agent has published in this run**. It keeps
`name → absolute path` in memory, populated as the agent publishes its outputs, and a request for
anything else is refused by name. The server never joins a request onto a directory, so there is no
traversal surface: an unknown name has no path at all.

A request also carries the run id and is refused if it does not match, which is the same guard the
readiness registry's run-scoped key provides. **An agent fails closed on an unknown run**: the
first version compared the two ids only when both were non-empty, so an agent whose
`SWARM_RUN_ID` was empty skipped the check and served its names to any run — and agents do
outlive their runs here, so run 1's agent would answer a run-2 consumer with run 1's file of the
same name. Staging therefore refuses to start without `SWARM_RUN_ID`. A **store** needs no equality
check to be *safe*, because its namespace is already `(run, name)`: it files each upload under
the requesting run and looks it up the same way, which is what lets one site back a whole
campaign. But `--run-id` on the site is an operator saying "this one is for one run", and that
binds **downloads as well as uploads** — exempting the store from the check outright dropped it
on the download side, so a site restarted with a restriction still served every run it had
already accumulated, because startup re-publishes them all.

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
- ~~**Producer durability**~~ — **BUILT 2026-09-21 as eager stage-out, and measured on the
  slice.** The problem it solved: under the shared mount a dead agent's outputs survive; with
  per-agent storage they do not, and since the producing job is COMPLETE nothing re-runs it,
  its name is in the readiness registry, so every descendant is released and then refuses.
  Three options were weighed. **(a) Re-run the dead producer** — only correct for a
  deterministic job, needs the readiness name retracted in the same transaction, and
  *cascades*: the producer's own inputs may be gone too, so a deep DAG re-runs a subtree.
  **(b) Replicate to k peers** — k uploads for a guarantee one gives. **(c) Eager stage-out to
  a staging site** — chosen. One upload, no cascade, and it is what Pegasus itself does, which
  matters for a comparison against Pegasus.

  As built: each output is pushed to the site **before its name is published**, so a name is
  never visible without a durable copy behind it — the same one-write rule the completion
  follows, extended one step. A consumer tries the **producer first** and the store only on
  failure, so the common case stays one hop; store-first would make every DAG edge pay two WAN
  hops for a guarantee it does not need while the producer is alive. The location registry
  therefore holds a preference-ordered list. A failed push costs durability for that file, not
  the run, and is logged at ERROR.

  **Measured end to end on the slice, 2026-09-21** (`runs/soil-stageout-20260921`, soilmoisture,
  5 agents, local work dirs, site on the database node):

  | | |
  |---|---|
  | Run | 4/4 jobs exit 0, makespan 36.3 s, metrics complete |
  | Stage-out | 5 outputs, 831 B – 228 KB, **0.035–0.097 s each, 0.244 s total** |
  | Share of makespan | **~0.7 %** |
  | Store fallback | producer unreachable → 228 KB served from the site in **0.19 s** |

  So the upload cost is noise at this workflow's output sizes. It would **not** be noise for
  multi-hundred-megabyte outputs, and the push is synchronous on the completion path, so a
  workflow with large intermediates should measure it again before quoting a makespan.
  **Failure-injection cells are no longer blocked on this** — but see the accounting gap below
  before quoting a data-movement number from one.

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
