from collections import defaultdict
import random


class PBFTNode:
    def __init__(self, node_id, all_nodes):
        self.node_id = node_id
        self.all_nodes = all_nodes
        self.sequence_number = 0
        self.request_queue = defaultdict(dict)
        self.pre_prepare_msgs = defaultdict(dict)
        self.prepare_msgs = defaultdict(dict)
        self.commit_msgs = defaultdict(dict)
        self.responses = []
        self.leader = None  # Initially, no leader

    def handle_task(self, task):
        # Add task to the queue and initiate the PBFT process
        self.sequence_number += 1
        self.request_queue[self.sequence_number] = {
            'task': task
        }
        self.pre_prepare()

    def pre_prepare(self):
        # Pre-prepare phase: leader proposes a task
        leader_id = self._determine_leader()
        for node_id in self.all_nodes:
            self.pre_prepare_msgs[node_id][self.sequence_number] = {
                'leader': leader_id,
                'sequence_number': self.sequence_number,
                'task': self.request_queue[self.sequence_number]['task']
            }
            self.send_message('pre_prepare', node_id, self.pre_prepare_msgs[node_id][self.sequence_number])

    def _determine_leader(self):
        # Determine the leader using a round-robin strategy
        all_ids = list(self.all_nodes.keys())
        index = all_ids.index(self.node_id)
        leader_index = (self.sequence_number + index) % len(self.all_nodes)
        return all_ids[leader_index]

    def prepare(self, leader, sequence_number, node_id):
        # Prepare phase: nodes prepare the proposed task
        self.prepare_msgs[node_id][sequence_number] = {
            'leader': leader,
            'sequence_number': sequence_number
        }
        self.send_message('prepare', node_id, self.prepare_msgs[node_id][sequence_number])

    def commit(self, leader, sequence_number, node_id):
        # Commit phase: nodes commit to the proposed task
        self.commit_msgs[node_id][sequence_number] = {
            'leader': leader,
            'sequence_number': sequence_number
        }
        self.send_message('commit', node_id, self.commit_msgs[node_id][sequence_number])

    def receive_message(self, msg_type, sender, msg):
        if msg_type == 'pre_prepare':
            self.prepare(msg['leader'], msg['sequence_number'], sender)
        elif msg_type == 'prepare':
            self.commit(msg['leader'], msg['sequence_number'], sender)
        elif msg_type == 'commit':
            self.responses.append((sender, msg['sequence_number']))
            if len(self.responses) > (len(self.all_nodes) // 3):
                self.finalize_task(msg['sequence_number'])

    def finalize_task(self, sequence_number):
        print(f"KOMAL -- {self.request_queue}")
        try:
            task = self.request_queue[sequence_number]['task']
            print(f"Node: {self.node_id} Task finalized: {task}")
            # Execute task
        except Exception as e:
            print(e)

    def send_message(self, msg_type, recipient, msg):
        self.all_nodes[recipient].receive_message(msg_type, self.node_id, msg)


# Example usage
if __name__ == "__main__":
    nodes = {
        'node1': PBFTNode('node1', {}),
        'node2': PBFTNode('node2', {}),
        'node3': PBFTNode('node3', {}),
    }
    nodes['node1'].all_nodes = nodes
    nodes['node2'].all_nodes = nodes
    nodes['node3'].all_nodes = nodes

    # Simulate tasks being added to the queue of each node
    nodes['node1'].handle_task("Task 1")
    nodes['node2'].handle_task("Task 1")
    nodes['node3'].handle_task("Task 1")
