from typing import List, Tuple

from swarm.comm.messages.message import MessageType, Message
from swarm.comm.messages.job_info import JobInfo


class JobStatus(Message):
    def __init__(self, **kwargs):
        self._jobs = []
        super().__init__(**kwargs)
        self._message_type = MessageType.JobStatus

    @property
    def jobs(self) -> List[JobInfo]:
        return self._jobs

    @jobs.setter
    def jobs(self, values: List[Tuple[JobInfo, dict]]):
        if isinstance(values, list):
            for v in values:
                if isinstance(v, JobInfo):
                    self._jobs.append(v)
                elif isinstance(v, dict):
                    self._jobs.append(JobInfo.from_dict(v))
                else:
                    raise ValueError("Unsupported value type for proposals")
        else:
            raise ValueError("Unsupported value type for proposals")

    def __str__(self):
        return f"[jobs: {self.jobs}]"


if __name__ == '__main__':
    from swarm.models.job import JobState
    t_info = JobInfo(job_id="t1", state=JobState.FAILED)
    print(t_info)
    print(t_info.to_dict())
    job_status = JobStatus(jobs=[t_info])
    print(job_status)
    print(job_status.to_dict())
    print(job_status.to_json())

    new_t = JobStatus.from_dict(job_status.to_dict())
    print(new_t)