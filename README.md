# SwarmAgents-Consensus

This repository hosts code for emulating SWARM agents and investigating different consensus algorithms concerning task scheduling among the agents.

## Practical Byzantine Fault Tolerance (PBFT)
PBFT algorithm is explored and implemented in `pbft_agent.py`. This agent works as follows:

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

### Task State Machine
- The task's state transitions are influenced by message exchanges. 
- Agents are vying to become leaders for tasks. 
- Once a consensus is established and a leader is elected, the leading agent can proceed to execute the task. 
- Agents may concurrently participate in leader elections for multiple tasks.

NOTE: Only tasks in Pending state are picked by an agent for scheduling.

#### Initiator Agent
![Initiator Agent](./images/pbft-state-initiator.png)

#### Participant Agent
![Participant Agent](./images/pbft-state-participant.png)

### Agent Architecture
![Agent Architecture](./images/pbft-agent.png)

### Performance
PBFT consensus works with smaller number of agents but struggles to scale efficiently as the number of agents increases, due to its communication complexity.
However, it's resilient and fault tolerant.

### Usage
1. Setup the python environment by installing all the dependencies:
```
pip install -r requirements.txt
```
2. Bring up the kafka cluster using `docker-compose up -d`
3. Generates `tasks.json` via `python task_generator.py`
4. Launch the agents via `sh start.sh`. User can increase the number of agents within `start.sh`

## Raft Consensus Algorithm
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
1. Setup the python environment by installing all the dependencies:
```
pip install -r requirements.txt
```
2. Bring up the kafka cluster and redis using `docker-compose up -d`
3. Generates `tasks.json` via `python task_generator.py`
4. Launch the agents:
   ```
   python main-raft.py <agent id> <number of agents> <number of tasks>
   ```
