# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""
Execution models: what it takes to actually *run* a job rather than simulate it.

Until this existed a `Job` carried only its resource shape and a wall time, and
`Job.execute()` slept for that wall time. That is enough to study scheduling — ordering,
placement, consensus — and every number the project has measured so far comes from it. It is
not enough to run a real workflow, which is what an apples-to-apples comparison against
Pegasus needs: the same code, in the same container, over the same inputs.

Two paths, and both are needed:

* ``path`` is what Pegasus *invoked*, and it is a path **inside the container**
  (``/srv/analyze_moisture``). It is what `invocation.executable` records.
* ``pfn`` is the **host** path of the code that was staged to that location
  (``/home/.../bin/analyze_moisture.py``), and it lives only in the transformation catalog.

Neither alone can re-run the job: the pfn says what code to ship, the in-container path says
what to invoke once it has been shipped. A job that has one but not the other is not
runnable, and `ExecutionSpec.runnable()` is the single place that decides so — callers must
ask rather than test a field, because "has an executable" and "can be executed" differ by the
container and by whether the arguments could be parsed at all.

Everything here is optional on a `Job`. A job with no `ExecutionSpec` simulates exactly as
before, so the default measurement path is untouched and no existing result moves.
"""
from typing import List, Optional

from swarm.models.json_field import JSONField


class ExecutionModelException(Exception):
    def __init__(self, msg: str):
        super().__init__(f"Execution model exception: {msg}")


class ContainerSpec(JSONField):
    """The container a job's code runs inside.

    ``image`` is kept as the catalog's URI, scheme and all
    (``file:///...sif``, ``docker://...``), rather than being normalised to a local path
    here. The scheme is the only thing that says where the image came from, and a runtime
    that cannot honour it must be able to say so precisely instead of failing on a path that
    was silently rewritten. ``kind`` is the runtime the catalog asked for
    (``singularity``/``docker``); a host may only have the other one, which is a deployment
    question and deliberately not resolved in the model.
    """

    def __init__(self, **kwargs):
        self.name = ""
        self.kind = ""          # singularity | docker | shifter | ...
        self.image = ""         # URI as written in the catalog, scheme preserved
        self.image_site = ""
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                self.__setattr__(k, v)
            except AttributeError:
                report = (f"Unable to set field {k} of container spec, no such field "
                          f"available {[f for f in self.__dict__.keys()]}")
                if forgiving:
                    print(report)
                else:
                    raise ExecutionModelException(report)
        return self

    def __str__(self):
        return f"{self.kind}:{self.image}" if self.image else ""


class ExecutionSpec(JSONField):
    """How to run one job for real.

    ``arguments`` distinguishes **empty from unknown**, and the distinction is load-bearing.
    ``[]`` means the recorded command line genuinely had no arguments — most Pegasus jobs,
    which pass data by file rather than by flag. ``None`` means the recorded ``argv`` could
    not be parsed (an unbalanced quote, say). Running a job with ``[]`` when the truth is
    ``None`` silently executes a *different command* than the one being compared against,
    which is the one failure this whole comparison cannot tolerate, so ``runnable()`` refuses
    ``None`` rather than treating it as "no arguments".
    """

    def __init__(self, **kwargs):
        self.transformation = ""            # logical name, e.g. "analyze_moisture"
        self.path = ""                      # in-container path actually invoked
        self.pfn = ""                       # host path of the code staged to `path`
        self.pfn_type = ""                  # stageable | installed
        self.arguments: Optional[List[str]] = []
        # How many tasks Pegasus clustered into this one Condor job, when it did. `None` or 1
        # means an ordinary single-task job. It is carried because it is the DIFFERENCE
        # between two jobs that otherwise look identical: both arrive with `arguments = None`
        # and both are refused, but one is a designed refusal (a cluster has no single
        # command line — see docs/WORKFLOW_EXECUTION.md 1.3) and the other is a parsing
        # failure worth investigating. Without it the artifact cannot explain its own
        # refusal, and the correct behaviour reads as a bug to be fixed.
        self.clustered_tasks: Optional[int] = None
        # NOTE: there is deliberately no `inputs` field here. The files a job needs are
        # already `Job.data_in`, which the converter populates from the workflow; a second
        # list would be a second source of truth for the same fact, and the two would drift.
        # The runner stages from `data_in` — see `swarm/execution/runner.py`.
        self.container: Optional[ContainerSpec] = None
        self._set_fields(**kwargs)

    def _set_fields(self, forgiving=False, **kwargs):
        for k, v in kwargs.items():
            try:
                self.__getattribute__(k)
                # The container arrives as a plain dict from JSON and as a ContainerSpec from
                # code; normalise here so callers never have to check which they hold.
                if k == "container" and isinstance(v, dict):
                    v = ContainerSpec.from_dict(v)
                self.__setattr__(k, v)
            except AttributeError:
                report = (f"Unable to set field {k} of execution spec, no such field "
                          f"available {[f for f in self.__dict__.keys()]}")
                if forgiving:
                    print(report)
                else:
                    raise ExecutionModelException(report)
        return self

    def refusal_reason(self) -> Optional[str]:
        """Why this job cannot be executed, or ``None`` when it can.

        Deliberately strict, and every clause is a way a run could otherwise produce numbers
        that look like a comparison but are not one:

        * no ``path`` — nothing to invoke;
        * ``arguments is None`` — the command line could not be parsed, so any invocation
          would be of a different command than Pegasus ran (see the class docstring);
        * a container named but with no image — the code would run against whatever the host
          happens to have installed, which is precisely the variable the container removes.

        ``pfn`` is *not* required: an ``installed`` transformation lives in the image already
        and has nothing to stage.

        The *reason* is produced here rather than at the call site so there is one wording of
        each refusal, and so the clustered case can say what it is. A caller that composes its
        own message from a bare boolean cannot distinguish a cluster (correct, permanent, not
        a defect) from an unparseable argv (a real extraction problem), and both then read as
        the same bug.
        """
        if not self.path:
            return "no in-container path to invoke"
        if self.arguments is None:
            if (self.clustered_tasks or 1) > 1:
                return (f"clustered job wrapping {self.clustered_tasks} tasks: Pegasus ran "
                        f"them as separate sequential invocations, so no single command line "
                        f"describes it (docs/WORKFLOW_EXECUTION.md 1.3). Not a defect")
            return "arguments could not be parsed, and the job is not clustered"
        if self.container is not None and not self.container.image:
            return f"container {self.container.name!r} is named but has no image"
        return None

    def runnable(self) -> bool:
        """True when this job can actually be executed, as opposed to merely described.

        One definition, in `refusal_reason` — a second copy of the clauses here would drift
        from the wording that explains them.
        """
        return self.refusal_reason() is None

    def to_dict(self) -> Optional[dict]:
        """Serialise, nesting the container rather than flattening it.

        `JSONField.to_dict` drops empty values and would emit the `ContainerSpec` object
        itself, which is not JSON-serialisable, so the nesting is done here.
        """
        out = {}
        for key in ("transformation", "path", "pfn", "pfn_type"):
            value = getattr(self, key)
            if value:
                out[key] = value
        # Emitted whenever it is not the "no arguments" default, so `None` (unparseable)
        # survives the round trip and keeps refusing to run at the far end.
        if self.arguments is None or self.arguments:
            out["arguments"] = self.arguments
        # Only when it says something: 1 is what an ordinary job is, and emitting it on every
        # record would add a field to thousands of jobs to state the default.
        if (self.clustered_tasks or 1) > 1:
            out["clustered_tasks"] = self.clustered_tasks
        if self.container is not None:
            container = self.container.to_dict()
            if container:
                out["container"] = container
        return out or None

    def __str__(self):
        args = " ".join(self.arguments) if self.arguments else ""
        return f"{self.path} {args}".strip()
