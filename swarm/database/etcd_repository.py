import json
from typing import Optional, Dict, Tuple, List

import pyetcd


class EtcdRepository:
    """
    Repository class to handle Etcd-based storage for jobs, agents, and multi-phase consensus state
    (Pre-Prepare, Prepare, Commit) for decentralized job scheduling.
    """

    KEY_JOB = "job"
    KEY_AGENT = "agent"
    KEY_PRE_PREPARE = "pre_prepare"
    KEY_PREPARE = "prepare"
    KEY_COMMIT = "commit"

    def __init__(self, host: str = 'localhost', port: int = 2379):
        self.client = pyetcd.client(host=host, port=port)

    ##########################
    # GENERIC JOB OPERATIONS #
    ##########################

    def save(self, obj: dict, key_prefix: str = KEY_JOB, key: Optional[str] = None):
        """
        Saves an object (job, agent, or consensus state) to etcd.
        If 'key' is not provided, it constructs the key from key_prefix and obj_id.
        """
        if not key:
            obj_id = obj.get("id") or obj.get(f"{key_prefix}_id")
            if obj_id is None:
                raise ValueError("obj_id must be set to save an object")
            key = f"{key_prefix}:{obj_id}"

        # Ensure key is bytes for pyetcd
        key_bytes = key.encode()
        # Ensure value is JSON string and then bytes
        value_bytes = json.dumps(obj).encode()

        self.client.put(key_bytes, value_bytes)

    def get(self, obj_id: str, key_prefix: str = KEY_JOB) -> Optional[dict]:
        """Retrieves an object from etcd by its ID and key prefix."""
        key = f"{key_prefix}/{obj_id}".encode() # Key must be bytes for pyetcd
        value, _ = self.client.get(key)
        if value:
            return json.loads(value.decode())
        return None # Return None if key not found

    def delete(self, obj_id: str, key_prefix: str = KEY_JOB):
        """Deletes an object from etcd by its ID and key prefix."""
        key = f"{key_prefix}/{obj_id}".encode() # Key must be bytes
        self.client.delete(key)

    def get_all_ids(self, key_prefix: str = KEY_JOB) -> List[str]:
        """
        Retrieves all IDs for a given key_prefix.
        For simple prefixes like 'job' or 'agent', it gets the last segment.
        For consensus prefixes (pre_prepare, prepare, commit), it extracts unique job IDs.
        """
        prefix_bytes = f"{key_prefix}/".encode()
        ids = set() # Use a set to store unique IDs

        for kv in self.client.get_prefix(prefix_bytes):
            key_str = kv[1].key.decode() # etcd key is bytes, decode to string

            if key_prefix in [self.KEY_PRE_PREPARE, self.KEY_PREPARE, self.KEY_COMMIT]:
                # For consensus keys like pre_prepare/job_ABC/agent_123, extract job_ABC
                parts = key_str.split('/')
                if len(parts) >= 3: # Expecting at least [prefix, job_id, agent_id]
                    ids.add(parts[1]) # The job_id is the second part
            else:
                # For simple keys like job/job_XYZ or agent/agent_PQR, extract job_XYZ or agent_PQR
                ids.add(key_str.split('/')[-1]) # The ID is the last part

        return sorted(list(ids)) # Return sorted list of unique IDs

    def get_all_objects(self, key_prefix: str = KEY_JOB) -> List[dict]:
        """Retrieves all objects for a given key_prefix."""
        prefix_bytes = f"{key_prefix}".encode()
        objects = []
        for kv in self.client.get_prefix(prefix_bytes):
            value = kv[0] # The value is the first element of the tuple
            if value:
                try:
                    objects.append(json.loads(value.decode()))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    print(f"Warning: Could not decode JSON for key {kv[1].key.decode()}")
        return objects

    def delete_all(self, key_prefix: str = KEY_JOB):
        """Deletes all entries under a given key_prefix.
        Note: pyetcd's delete_prefix is safe to use here.
        """
        prefix_bytes = f"{key_prefix}/".encode()
        self.client.delete_prefix(prefix_bytes)

    ################################
    # PRE-PREPARE PHASE OPERATIONS #
    ################################

    def push_pre_prepare(self, job_id: str, cost: float, agent_id: int):
        """Records a pre-prepare message for a job from an agent."""
        key = f"{self.KEY_PRE_PREPARE}/{job_id}/{agent_id}".encode()
        self.client.put(key, str(round(float(cost), 2)).encode()) # Store cost as string bytes

    def get_pre_prepare(self, job_id: str) -> Dict[str, float]:
        """Retrieves all pre-prepare messages for a specific job."""
        prefix_bytes = f"{self.KEY_PRE_PREPARE}/{job_id}/".encode()
        result = {}
        for value, meta in self.client.get_prefix(prefix_bytes):
            agent_id = meta.key.decode().split("/")[-1]
            result[agent_id] = float(value.decode())
        return result

    def get_min_cost_agent_for_job(self, job_id: str) -> Tuple[Optional[int], float]:
        """Finds the agent with the minimum cost for a given job from pre-prepare messages."""
        job_costs = self.get_pre_prepare(job_id)
        if not job_costs:
            return None, float('inf')
        min_agent = min(job_costs, key=lambda k: job_costs[k])
        return int(min_agent), job_costs[min_agent]

    ###########################
    # PREPARE PHASE OPERATIONS #
    ###########################

    def push_prepare_vote(self, job_id: str, leader_agent_id: int, agent_id: int):
        """Records a prepare vote for a job by an agent."""
        key = f"{self.KEY_PREPARE}/{job_id}/{agent_id}".encode()
        self.client.put(key, str(leader_agent_id).encode()) # Store leader ID as string bytes

    def get_prepare(self, job_id: str) -> Dict[str, int]:
        """Retrieves all prepare votes for a specific job."""
        prefix_bytes = f"{self.KEY_PREPARE}/{job_id}/".encode()
        result = {}
        for value, meta in self.client.get_prefix(prefix_bytes):
            agent_id = meta.key.decode().split("/")[-1]
            result[agent_id] = int(value.decode())
        return result

    #########################
    # COMMIT PHASE OPERATIONS #
    #########################

    def push_commit_vote(self, job_id: str, leader_agent_id: int, agent_id: int):
        """Records a commit vote for a job by an agent."""
        key = f"{self.KEY_COMMIT}/{job_id}/{agent_id}".encode()
        self.client.put(key, str(leader_agent_id).encode()) # Store leader ID as string bytes

    def get_commit(self, job_id: str) -> Dict[str, int]:
        """Retrieves all commit votes for a specific job."""
        prefix_bytes = f"{self.KEY_COMMIT}/{job_id}/".encode()
        result = {}
        for value, meta in self.client.get_prefix(prefix_bytes):
            agent_id = meta.key.decode().split("/")[-1]
            result[agent_id] = int(value.decode())
        return result

    #####################
    # INTERNAL UTILITIES #
    #####################

    def count_pre_prepare_votes(self, job_id: str) -> int:
        """Counts the number of pre-prepare votes for a specific job."""
        prefix_bytes = f"{self.KEY_PRE_PREPARE}/{job_id}/".encode()
        count = sum(1 for _ in self.client.get_prefix(prefix_bytes))
        return count


if __name__ == '__main__':
    # --- Example Usage ---
    repo = EtcdRepository()

    # Clear existing data for a clean test
    print("--- Cleaning up existing data ---")
    repo.delete_all(repo.KEY_JOB)
    repo.delete_all(repo.KEY_AGENT)
    repo.delete_all(repo.KEY_PRE_PREPARE)
    repo.delete_all(repo.KEY_PREPARE)
    repo.delete_all(repo.KEY_COMMIT)
    print("--- Cleanup complete ---")

    print("\n--- Testing save and get_all_ids ---")

    # Save some example jobs
    job1_data = {"job_id": "job_ABC", "status": "pending", "description": "Process A"}
    job2_data = {"job_id": "job_XYZ", "status": "running", "description": "Process B"}
    repo.save(job1_data, key_prefix=repo.KEY_JOB, key=f"{repo.KEY_JOB}/job_ABC")
    repo.save(job2_data, key_prefix=repo.KEY_JOB, key=f"{repo.KEY_JOB}/job_XYZ")
    # Using 'id' if 'job_id' is not present
    job3_data = {"job_id": "job_PQR", "status": "completed", "description": "Process C"}
    repo.save(job3_data, key_prefix=repo.KEY_JOB) # This should automatically construct the key

    # Save some example agents
    agent1_data = {"agent_id": "agent_1", "status": "idle"}
    agent2_data = {"agent_id": "agent_2", "status": "busy"}
    repo.save(agent1_data, key_prefix=repo.KEY_AGENT)
    repo.save(agent2_data, key_prefix=repo.KEY_AGENT)

    # Save some pre_prepare consensus entries
    repo.push_pre_prepare("job_ABC", 10.5, 1)
    repo.push_pre_prepare("job_ABC", 8.2, 2)
    repo.push_pre_prepare("job_XYZ", 5.0, 1)

    print("\n--- get_all_ids for 'job' prefix ---")
    job_ids = repo.get_all_ids(key_prefix=repo.KEY_JOB)
    print(f"Job IDs: {job_ids}") # Expected: ['job_ABC', 'job_PQR', 'job_XYZ'] (order may vary)

    print("\n--- get_all_ids for 'agent' prefix ---")
    agent_ids = repo.get_all_ids(key_prefix=repo.KEY_AGENT)
    print(f"Agent IDs: {agent_ids}") # Expected: ['agent_1', 'agent_2'] (order may vary)

    print("\n--- get_all_ids for 'pre_prepare' prefix (should get unique job IDs) ---")
    pre_prepare_job_ids = repo.get_all_ids(key_prefix=repo.KEY_PRE_PREPARE)
    print(f"Pre-Prepare Job IDs: {pre_prepare_job_ids}") # Expected: ['job_ABC', 'job_XYZ'] (order may vary)

    print("\n--- Testing get and get_all_objects ---")
    retrieved_job = repo.get("job_ABC", key_prefix=repo.KEY_JOB)
    print(f"Retrieved Job 'job_ABC': {retrieved_job}")

    all_jobs = repo.get_all_objects(key_prefix=repo.KEY_JOB)
    print("All Job Objects:")
    for job in all_jobs:
        print(f"  {job}")

    print("\n--- Testing consensus phase operations ---")
    print(f"Pre-prepare for job_ABC: {repo.get_pre_prepare('job_ABC')}")
    min_agent, min_cost = repo.get_min_cost_agent_for_job('job_ABC')
    print(f"Min cost agent for job_ABC: Agent {min_agent}, Cost {min_cost}")
    print(f"Count pre-prepare votes for job_ABC: {repo.count_pre_prepare_votes('job_ABC')}")

    repo.push_prepare_vote('job_ABC', 2, 1) # Agent 1 votes for leader 2
    repo.push_prepare_vote('job_ABC', 2, 2) # Agent 2 votes for leader 2
    print(f"Prepare votes for job_ABC: {repo.get_prepare('job_ABC')}")

    repo.push_commit_vote('job_ABC', 2, 1) # Agent 1 commits to leader 2
    print(f"Commit votes for job_ABC: {repo.get_commit('job_ABC')}")

    print("\n--- Deleting some specific keys ---")
    repo.delete("job_PQR", key_prefix=repo.KEY_JOB)
    print(f"Job_PQR deleted. Remaining job IDs: {repo.get_all_ids(repo.KEY_JOB)}")

    print("\n--- Cleaning up all test data ---")
    repo.delete_all(repo.KEY_JOB)
    repo.delete_all(repo.KEY_AGENT)
    repo.delete_all(repo.KEY_PRE_PREPARE)
    repo.delete_all(repo.KEY_PREPARE)
    repo.delete_all(repo.KEY_COMMIT)
    print("--- All test data deleted ---")
