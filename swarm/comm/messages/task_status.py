from typing import List, Tuple

from swarm.comm.messages.message import MessageType, Message
from swarm.comm.messages.task_info import TaskInfo


class TaskStatus(Message):
    def __init__(self, **kwargs):
        self._tasks = []
        super().__init__(**kwargs)
        self._message_type = MessageType.TaskStatus

    @property
    def tasks(self) -> List[TaskInfo]:
        return self._tasks

    @tasks.setter
    def tasks(self, values: List[Tuple[TaskInfo, dict]]):
        if isinstance(values, list):
            for v in values:
                if isinstance(v, TaskInfo):
                    self._tasks.append(v)
                elif isinstance(v, dict):
                    self._tasks.append(TaskInfo.from_dict(v))
                else:
                    raise ValueError("Unsupported value type for proposals")
        else:
            raise ValueError("Unsupported value type for proposals")

    def __str__(self):
        return f"[tasks: {self.tasks}]"


if __name__ == '__main__':
    from swarm.models.task import TaskState
    t_info = TaskInfo(task_id="t1", state=TaskState.FAILED)
    print(t_info)
    print(t_info.to_dict())
    task_status = TaskStatus(tasks=[t_info])
    print(task_status)
    print(task_status.to_dict())
    print(task_status.to_json())

    new_t = TaskStatus.from_dict(task_status.to_dict())
    print(new_t)