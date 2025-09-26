# SwarmAgents

This repository provides a framework for implementing greedy distributed consensus and selection algorithms. It is designed to support scalable, resilient, and efficient decision-making across multiple agents in a distributed system.

The core logic enables agents to reach consensus and make selections (such as job assignments) using PBFT-like protocols and cost-based selection strategies. The framework is modular, allowing for flexible integration of custom consensus, selection, and penalty mechanisms.

## Table of Contents

- [Key Features](#key-features)
- [Core Modules](#core-modules)
- [Example: Job Selection](#example-job-selection)
- [Testing](#testing)
- [Results](#results)
- [Utilities](#utilities)

## Key Features
- Greedy distributed selection and consensus algorithms
- Modular design for consensus, selection, and penalty logic
- Example application for job selection
- Extensible for other distributed resource allocation problems


## Core Modules

### Consensus Engine (`consensus/engine.py`)
This module implements a generic PBFT-like consensus engine for distributed agreement among agents. It is framework-agnostic and uses host, transport, and router adapters for I/O and side effects. The engine manages proposals, prepares, and commits, allowing agents to reach agreement on object states (such as job assignments) through quorum-based rounds.

**How it works:**
- Agents initiate proposals for objects (e.g., jobs) and broadcast them to peers.
- Peers respond with prepare and commit messages, tracked by the engine.
- The engine checks for quorum and triggers selection or commit actions when consensus is reached.
- The engine is designed to be integrated with custom agent logic via adapter classes.

**How to use:**
- Instantiate `ConsensusEngine` with your agent's ID and adapter implementations for host, transport, and router.
- Use the `propose`, `on_proposal`, `on_prepare`, and `on_commit` methods to drive the consensus protocol.
- See `resource_agent.py` for a concrete example of integration.

### Selection Engine (`selection/engine.py`)
This module provides a generic, cache-enabled engine for assigning candidates (e.g., jobs) to assignees (e.g., agents/resources) based on feasibility and cost functions. It supports cost matrix computation, threshold-based selection, and memoization for performance.

**How it works:**
- Define feasibility and cost functions for your assignment problem.
- The engine computes a cost matrix for all candidate-assignee pairs, marking infeasible assignments with infinity.
- It selects the best assignee for each candidate using greedy or thresholded strategies, with optional tie-breaking and acceptance criteria.
- Internal LRU caches speed up repeated feasibility and cost checks.

**How to use:**
- Instantiate `SelectionEngine` with your feasibility and cost functions, plus key and versioning functions for candidates and assignees.
- Use `compute_cost_matrix` to build the assignment matrix, and `pick_agent_per_candidate` to select assignments.
- The engine is highly configurable for different selection policies and caching needs.

## Example: Job Selection

The file `swarm/agents/resource_agent.py` provides a comprehensive example of distributed job selection using both the consensus and selection engines:

### Cost Computation Equation

The cost for assigning a job to an agent is computed as a weighted sum of resource utilizations, plus penalties for long jobs and connectivity. Importantly, the weights can be tailored based on the job type, allowing the system to prioritize different resources for different workloads (e.g., compute-heavy, memory-heavy, or data-transfer jobs):
$$
	ext{cost} = w_{cpu} \cdot \text{CPU}_{util} + w_{ram} \cdot \text{RAM}_{util} + w_{disk} \cdot \text{Disk}_{util} + w_{gpu} \cdot \text{GPU}_{util} + \text{penalties}
$$

Where:
- $w_{cpu}$, $w_{ram}$, $w_{disk}$, $w_{gpu}$ are the weights for each resource (see `__init__` and `compute_job_cost` in `resource_agent.py`).
- Resource utilization terms are computed as the fraction of required over available capacity.
- Penalties include:
	- Long job penalty: applied if job execution time exceeds `long_job_threshold`.
	- Connectivity penalty: scaled by `connectivity_penalty_factor` for DTN jobs.

**Job Type-Specific Weights:**
- The cost computation can dynamically adjust the weights based on the job type (e.g., CPU-intensive, memory-intensive, I/O-bound, or DTN/data-transfer jobs).
- For example, a data-transfer (DTN) job may use a higher weight for network or disk utilization, while a compute-heavy job may prioritize CPU and GPU weights.
- This logic is implemented in `compute_job_cost`, where the job's type or attributes are checked and the weights are set accordingly before computing the final cost.

This approach allows the system to flexibly prioritize resources and penalties for different job classes, improving scheduling efficiency and resource utilization.

The final cost is used by the selection engine to compare candidate assignments and select the best agent for each job.

### Job Feasibility

Job feasibility determines whether an agent can execute a given job, based on its current resource capacities and the job's requirements. The logic is implemented in the `is_job_feasible` method of `resource_agent.py`.

**Feasibility criteria include:**
- The agent must have enough available CPU, RAM, Disk, and GPU resources to meet the job's requirements.
- The agent must satisfy any additional constraints, such as data dependencies or network connectivity (for DTN jobs).
- The method checks for resource overcommitment and ensures that jobs are only assigned to agents that can complete them successfully.

This feasibility check is performed before cost computation and candidate selection, ensuring that only valid agent-job pairs are considered in the assignment process.

### Cost Matrix and Candidate Selection

After feasibility is determined, the selection engine computes a cost matrix representing the assignment cost for each agent-job pair. This is handled by the `compute_cost_matrix` method of the `SelectionEngine` (see its instantiation and usage in `resource_agent.py`).

- **Cost Matrix:**
	- Each row corresponds to an agent, each column to a job.
	- Entries are the computed costs (see cost equation above), or infinity for infeasible assignments.
	- The matrix is built using the agent's current capacities, job requirements, and configured weights/penalties.

- **Candidate Selection:**
	- The selection engine uses the cost matrix to identify the best agent for each job, typically minimizing cost.
	- Selection can be thresholded using `selection_threshold_pct` to allow more flexible candidate pools.
	- Tie-breaking and acceptance criteria can be configured for custom selection policies.

This process ensures that jobs are assigned to agents in a way that balances resource usage, cost, and system constraints, as implemented in the selection logic of `resource_agent.py`.

### Distributed Consensus

Once candidate assignments are selected, agents use the consensus engine to coordinate agreement on job selection across the distributed system. This is implemented using a PBFT-like protocol in the `ConsensusEngine` (see its instantiation and usage in `resource_agent.py`).

- **Consensus Protocol:**
	- Agents broadcast proposals for job assignments to their peers.
	- Peers respond with prepare and commit messages, tracked by the consensus engine.
	- The engine checks for quorum (majority agreement) before finalizing job assignments.
	- Methods such as `__on_proposal`, `__on_prepare`, and `__on_commit` in `resource_agent.py` handle the protocol steps.

- **Fault Tolerance:**
	- The protocol ensures that job selection is robust to agent failures and network partitions.
	- Only when a quorum is reached does the system commit to a job assignment, maintaining consistency.

This distributed consensus mechanism allows agents to make autonomous decisions while ensuring global agreement and reliability in job scheduling.

### Job Execution
- **Execution & State Management:**
	- After consensus, jobs are scheduled and executed by the selected agents.
	- Job states and metrics are managed via methods like `schedule_job`, `execute_job`, and the `Metrics` class.


Additional implementation details:
- **Job Queue:** The job queue is managed using a Redis database, allowing agents to efficiently share and update job states in a distributed manner.
- **Message Exchange:** Agents communicate and exchange consensus/selection messages using gRPC, enabling fast and reliable messaging across the network.
- **Supported Topologies:** The framework supports various network topologies, including Ring, Mesh, and Hierarchical, which can be configured to match different deployment scenarios. Topology logic is handled via the `Topology` and `TopologyType` classes and used in agent initialization and routing decisions.

For implementation details, see:
- `resource_agent.py`: Integration of selection and consensus engines, job feasibility/cost logic, and scheduling workflow.
- `is_job_feasible`, `compute_job_cost`, and `SelectionEngine` usage for assignment logic.
- The weights and thresholds in the agent's `__init__` for tuning selection behavior.

### Testing

You can simulate all agents on a single host for testing and benchmarking. For example, to run 30 agents in a ring topology with 100 jobs and a local Redis database, use:

```bash
python run_test.py --agents 30 --topology ring --jobs 100 --db-host localhost --jobs-per-interval 10 --run-dir swarm-multi
```

This will launch the specified number of agents, distribute jobs, and store results in the `swarm-multi` directory. Adjust parameters as needed for your experiments.

### Results

Simulation results for different topologies (Ring, Mesh, Hierarchical, etc.) are stored in the `runs/simulation` directory. Each run contains logs, metrics, and output files summarizing agent performance, job completion rates, consensus rounds, and resource utilization.

- **How to interpret results:**
	- Compare job completion times, consensus latency, and resource usage across topologies.
	- Use the provided plotting scripts (see `plot_latency_jobs.py`) to visualize performance metrics.
	- Analyze logs for details on agent decisions, consensus events, and system behavior under different configurations.

For example, after running a simulation with `run_test.py`, inspect the corresponding results directory for summary files and plots. This helps evaluate the scalability, efficiency, and fault tolerance of the framework under various network structures.
### Example Results Visualizations

Below are example images showing results for different topologies:

**Mesh Topology:**
![Scheduling Latency](runs/simulation/mesh/30/scheduling_latency_histogram.png)

![Jobs Per Agent](runs/simulation/mesh/30/jobs_per_agent.png)

![Agent Load](runs/simulation/mesh/30/agent_loads_summary.png)

**Ring Topology:**
![Scheduling Latency](runs/simulation/ring/30/scheduling_latency_histogram.png)

![Jobs Per Agent](runs/simulation/ring/30/jobs_per_agent.png)

![Agent Load](runs/simulation/ring/30/agent_loads_summary.png)

**Hierarchical Topology:**
![Scheduling Latency](runs/simulation/hierarchical/30/scheduling_latency_histogram.png)

![Jobs Per Agent](runs/simulation/hierarchical/30/jobs_per_agent.png)

![Agent Load](runs/simulation/hierarchical/30/agent_loads_summary.png)

## Utilities

The repository includes several utility scripts to support simulation setup and management:

- **job_generator.py**: Generates synthetic job descriptions for simulation runs. You can customize the number, type, and resource requirements of jobs to match your experimental needs.
```
python job_generator.py  --help
usage: job_generator.py [-h] --job-count JOB_COUNT --agent-profile-path AGENT_PROFILE_PATH
                        [--output-dir OUTPUT_DIR] [--enable-dtns]

Generate jobs matching agent profiles.

options:
  -h, --help            show this help message and exit
  --job-count JOB_COUNT
                        Number of jobs to generate
  --agent-profile-path AGENT_PROFILE_PATH
                        Path to agent profiles JSON
  --output-dir OUTPUT_DIR
                        Directory to save job files
  --enable-dtns         Assign DTNs to jobs based on agent profiles
```

- **generate_configs.py**: Creates agent configuration files for different topologies and agent counts. This helps automate the setup of large-scale experiments with consistent parameters.
```
python generate_configs.py  --help
usage: generate_configs.py [-h] [--dtns] [--flavor-percentages [PERCENT ...]]
                           [--agent-hosts-file AGENT_HOSTS_FILE] [--agents-per-host AGENTS_PER_HOST]
                           num_agents jobs_per_proposal base_config_file output_dir topology database
                           job_cnt

Generate agent configuration files.

positional arguments:
  num_agents            Number of agents to generate configurations for.
  jobs_per_proposal     Number of Jobs per proposal.
  base_config_file      Path to the base configuration YAML file.
  output_dir            Directory where generated configs should be saved.
  topology              Topology: mesh | ring | star | hierarchical
  database              Database host
  job_cnt               Job Count

options:
  -h, --help            show this help message and exit
  --dtns                Enable DTNs
  --flavor-percentages [PERCENT ...]
                        Percentages for small, medium, large, xtralarge, xxtralarge (e.g. 0.4 0.25 0.15
                        0.15 0.05)
  --agent-hosts-file AGENT_HOSTS_FILE
                        Path to file with agent hosts (one per line)
  --agents-per-host AGENTS_PER_HOST
                        Number of agents per host (for grpc.host assignment)
```
- **job_distributor.py**: Distributes jobs to agents or queues in the system, supporting various distribution strategies and load patterns.
```
python job_distributor.py --help
usage: job_distributor.py [-h] --redis-host REDIS_HOST [--redis-port REDIS_PORT] --jobs-dir JOBS_DIR
                          --jobs-per-interval JOBS_PER_INTERVAL [--interval INTERVAL] [--level LEVEL]
                          [--group GROUP]

Distribute job JSON files to Redis at a controlled rate.

options:
  -h, --help            show this help message and exit
  --redis-host REDIS_HOST
                        Redis host address
  --redis-port REDIS_PORT
                        Redis port (default: 6379)
  --jobs-dir JOBS_DIR   Directory containing job JSON files
  --jobs-per-interval JOBS_PER_INTERVAL
                        Number of jobs to push per interval
  --interval INTERVAL   Interval duration in seconds (default: 1.0)
  --level LEVEL         Topology level for which to add Jobs
  --group GROUP         Topology group at a level for which to add Jobs
```
- **dump_db.py**: Exports the contents of the Redis database used by SwarmAgents. This script is useful for debugging, analysis, and archiving the state of jobs, agents, and other system data during or after simulation runs. It can output the database contents in a human-readable or machine-readable format for further inspection.
```
usage: dump_db.py [-h] [--host HOST] [--key KEY] [--count] [--type {redis,etcd}]

Display Redis queue or count.

options:
  -h, --help           show this help message and exit
  --host HOST          Host (default: localhost)
  --key KEY            Key prefix to match (default: *)
  --count              Only display the count of entries
  --type {redis,etcd}  Type of data store to query (default: etcd)
```
These utilities streamline the process of preparing, configuring, and running distributed simulations with SwarmAgents.
