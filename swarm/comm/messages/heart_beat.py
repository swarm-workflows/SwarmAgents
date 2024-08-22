from swarm.comm.messages.message import Message, MessageType
from swarm.models.agent_info import AgentInfo


class HeartBeat(Message):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.message_type = MessageType.HeartBeat


if __name__ == '__main__':
    from swarm.models.capacities import Capacities

    agent = AgentInfo(agent_id="agent-1", load=0.1, capacities=Capacities(core=1.0, disk=1.0, ram=1.0),)
    print(agent)
    print(agent.to_dict())

    heartbeat = HeartBeat(agent=agent)
    print(heartbeat)
    print(heartbeat.to_dict())
    print(heartbeat.to_json())

    h = HeartBeat.from_dict(heartbeat.to_dict())
    print(f"Heartbeat object: {h}")
