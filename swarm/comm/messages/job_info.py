from swarm.models.json_field import JSONField


class JobInfo(JSONField):
    def __init__(self, **kwargs):
        self.job_id = None
        self.state = None
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        """
        Set fields
        :param kwargs:
        :return: self to support call chaining
        """
        for k, v in kwargs.items():
            try:
                # will toss an exception if field is not defined
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = f"Unable to set field {k} of message, no such field available "\
                       f"{[k for k in self.__dict__.keys()]}"
                if forgiving:
                    print(report)
                else:
                    raise Exception(report)
        return self

    def __str__(self):
        return f"job_id: {self.job_id}, state: {self.state}"

