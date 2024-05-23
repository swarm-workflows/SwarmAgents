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

- **Scenario 1: Number of tasks: 100, Number of Agents: 10**
  - Agent-1 
```
2024-05-21 17:51:06,686 - agent-1 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 12
2024-05-21 17:51:06,686 - agent-1 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:51:06,686 - agent-1 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2001.13
2024-05-21 17:51:06,686 - agent-1 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 31.5
2024-05-21 17:51:06,686 - agent-1 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1660.9166666666667
```
 - Agent-2
```
2024-05-21 17:51:04,990 - agent-2 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 6
2024-05-21 17:51:04,990 - agent-2 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 54.7
2024-05-21 17:51:04,991 - agent-2 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 1942.69
2024-05-21 17:51:04,991 - agent-2 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 38.5
2024-05-21 17:51:04,991 - agent-2 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 863.0
```
 - Agent-3
```
2024-05-21 17:52:41,970 - agent-3 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 9
2024-05-21 17:52:41,970 - agent-3 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:52:41,970 - agent-3 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2078.78
2024-05-21 17:52:41,970 - agent-3 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 42.125
2024-05-21 17:52:41,970 - agent-3 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1704.888888888889
```
 - Agent-4
```
2024-05-21 17:50:33,643 - agent-4 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 15
2024-05-21 17:50:33,643 - agent-4 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:50:33,643 - agent-4 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 1966.01
2024-05-21 17:50:33,644 - agent-4 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 31.533333333333335
2024-05-21 17:50:33,644 - agent-4 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1414.6
```
 - Agent-5
```
2024-05-21 17:51:57,683 - agent-5 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 7
2024-05-21 17:51:57,683 - agent-5 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:51:57,683 - agent-5 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2005.56
2024-05-21 17:51:57,683 - agent-5 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 26.285714285714285
2024-05-21 17:51:57,683 - agent-5 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 2258.285714285714
```
 - Agent-6
```
2024-05-21 17:54:20,116 - agent-6 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 13
2024-05-21 17:54:20,117 - agent-6 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:54:20,117 - agent-6 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2136.97
2024-05-21 17:54:20,117 - agent-6 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 35.07692307692308
2024-05-21 17:54:20,117 - agent-6 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1329.5384615384614
```
 - Agent-7
```
2024-05-21 17:51:51,615 - agent-7 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 9
2024-05-21 17:51:51,615 - agent-7 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:51:51,615 - agent-7 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2044.93
2024-05-21 17:51:51,615 - agent-7 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 45.125
2024-05-21 17:51:51,616 - agent-7 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 892.7777777777778
```
 - Agent-8
```
2024-05-21 17:53:57,424 - agent-8 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 7
2024-05-21 17:53:57,424 - agent-8 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:53:57,424 - agent-8 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2134.13
2024-05-21 17:53:57,424 - agent-8 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 33.285714285714285
2024-05-21 17:53:57,424 - agent-8 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1621.7142857142858
```
 - Agent-9
```
2024-05-21 17:49:43,181 - agent-9 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 5
2024-05-21 17:49:43,181 - agent-9 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 70.2
2024-05-21 17:49:43,181 - agent-9 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 1891.92
2024-05-21 17:49:43,181 - agent-9 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 28.6
2024-05-21 17:49:43,181 - agent-9 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 2445.8
```
 - Agent-10
```
2024-05-21 17:53:17,443 - agent-10 - {pbft_agent.py:458} - [MainThread]- INFO - Total Tasks = 100, Completed Tasks = 14
2024-05-21 17:53:17,443 - agent-10 - {pbft_agent.py:459} - [MainThread]- INFO -   Average Waiting Time = 30.7
2024-05-21 17:53:17,443 - agent-10 - {pbft_agent.py:460} - [MainThread]- INFO -   Average Consensus Time = 2079.01
2024-05-21 17:53:17,443 - agent-10 - {pbft_agent.py:461} - [MainThread]- INFO -   Average Execution Time = 33.5
2024-05-21 17:53:17,443 - agent-10 - {pbft_agent.py:462} - [MainThread]- INFO -   Average Completion Time = 1742.0
```

 - **Scenario 2: Number of tasks: 500, Number of Agents: 5**
![Wait Time and Leader Election Time distribution across agents](./runs/5/agent-1.png)

### Usage
1. Setup the python environment by installing all the dependencies:
```
pip install -r requirements.txt
```
2. Bring up the kafka cluster using `docker-compose up -d`
3. Generates `tasks.json` via `python task_generator.py`
4. Launch the agents via `sh start.sh`. User can increase the number of agents within `start.sh`

## Raft Consensus Algorithm
**TODO** - work in progress
