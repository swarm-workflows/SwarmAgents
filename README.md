# SwarmAgents-Consensus

This repository hosts code for emulating SWARM agents and investigating different consensus algorithms concerning task scheduling among the agents.

## Practical Byzantine Fault Tolerance (PBFT)
PBFT algorithm is explored and implemented in `pbft_agentv2.py`. This agent works as follows:

- Each agent has a copy of the task Queue
- Collaborative decision-making by agents to determine tasks for scheduling taking into account the following factors:
  - Dependencies of tasks on data
  - Resource requirements specified by a task
  - Agent’s own resources
  - Neighbor’s load status
- Inter Agent messaging is done over kafka using broadcast messaging
- Agents exchange their load information via Heartbeat messages
- Each agent attempts to be a leader to execute a task based on following conditions:
  - Has enough resources for the task
  - Has access to any of the data dependencies of the task
  - Has load lesser than all its neighbors
- If all the above conditions are met, agent sends Proposal to be a leader to every other agent in the network.
- Each Proposal also has a seed value which is a random value generated between 0 and 1
- Proposal collision is resolved by choosing an agent with the lowest
- After a Proposal is sent, agent waits to receive Prepare messages indicating the acceptance of the proposal.
- If quorum count of Prepares are received, agent sends Commit messages to everyone in the network indicating leader election.
- If quorum count of Commits are received, agent finalizes leader election, at this point the task is scheduled and executed.

### Performance
PBFT consensus works with smaller number of agents but struggles to scale efficiently as the number of agents increases, due to its communication complexity.
However, it's resilient and fault tolerant.

![Scheduling latency - 3 agents](./runs/pbft/3/scheduling-latency.png)
![Scheduling latency - 5 agents](./runs/pbft/5/scheduling-latency.png)
![Scheduling latency - 10 agents](./runs/pbft/10/scheduling-latency.png)

![Scheduling latency comparison - Number of agents](./runs/pbft/overall-scheduling-latency.png)
### Usage
- Set up the python environment by installing all the dependencies:
```
pip install -r requirements.txt
```
- Bring up the kafka cluster using `docker-compose up -d`
- Generates `tasks.json` via `python task_generator.py`
- Start Agent 0 is started via following command:
```
   python3.11 main-pbft.py 0 100
```
- Rest of the agents can be started via following command:
```
    sh start.sh
```
NOTE: Remember to clean up the logs and kafka topic between runs via
```
rm -rf *.log*
python3.11 kafka_cleanup.py
```

## Raft Consensus Algorithm
RAFT algorithm is explored and implemented in `raft_agentv.py`. This agent works as follows:
### Single Leader Election:
A single leader is chosen based on the agent's load.
### Shared Task Queue
Task queue is maintained in Redis
### Job Scheduling:
The leader agent assigns the job to other agents based on their loads.
The leader agent schedules the job on itself if it can accommodate the job and its load is below a defined threshold.
### Leader Down:
If the leader fails, one of the followers assumes leadership and job scheduling continues
### Agent Termination Handling:
If an agent abruptly terminates while executing a job, the leader monitors the job's status.
If a job remains in a particular state beyond a specified threshold time, it is assumed to be terminated. The leader then moves the job to a Pending state for rescheduling.

### Usage
- Set up the python environment by installing all the dependencies:
```
pip install -r requirements.txt
```
- Bring up the kafka cluster using `docker-compose up -d`
- Generates `tasks.json` via `python task_generator.py`
- Start Agent 0 is started via following command:
```
   python3.11 main-raft.py 0 100
```
- Rest of the agents can be started via following command:
```
    sh raft-start.sh
```
NOTE: Remember to clean up the logs and kafka topic between runs via
```
rm -rf *.log*
python3.11 kafka_cleanup.py
```

### Performance
Scales well, handling large numbers of agents effectively.

Number of Agents: 10

![Scheduling latency](./runs/raft/raft-by-time.png)
