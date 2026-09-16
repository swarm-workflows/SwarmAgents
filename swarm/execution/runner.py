# MIT License
#
# Copyright (c) 2024 swarm-workflows
#
# Author: Komal Thareja(kthare10@renci.org)
"""Running a workflow job for real, instead of sleeping for its wall time.

`Job.execute()` sleeps. That is the right default and is where every measurement so far
comes from; this module is what happens instead when a run is explicitly told to execute.
It is deliberately a separate module so the simulation path stays readable and so the two
cannot drift into one another's error handling.

The governing rule everywhere here is **refuse rather than approximate**. Running *something*
when the declared thing is unavailable produces a number that looks like a result and is not
one, and the whole reason this exists is a like-for-like comparison against a Pegasus run.
So: a missing container runtime is a refusal, not a fallback to the host; an unresolvable
executable is a refusal, not a guess at a basename; an image the catalog named and the host
does not have is a refusal. Each refusal reports *why*, because a silent one is
indistinguishable from a job that simply failed.

**Where the job runs.** All jobs of a run share one working directory, which is Pegasus's
scratch-directory semantics and is what makes a DAG work at all: `data_in`/`data_out` carry
bare logical file names (`soil_moisture_map.json`), so job B finds job A's output only if
both ran in the same directory. Per-job directories would silently break every edge.

**Paths are rewritten, not guessed.** A catalog's `pfn` is a path on the *Pegasus submit
host* (`/home/ubuntu/soilmoisture-workflow/bin/analyze.py`), which does not exist on the
fleet. `path_rewrites` maps prefixes onto wherever the code was actually staged. It is an
explicit prefix map rather than a basename search because two workflows can have a
`process.py` and picking the wrong one would run silently and produce plausible output.
"""
import logging
import os
import shlex
import shutil
import signal
import subprocess
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from swarm.models.execution import ExecutionSpec

#: Container runtimes understood here, in the order `auto` prefers them.
KNOWN_RUNTIMES = ("apptainer", "singularity", "docker")

#: Catalog container kinds that mean "a .sif run by apptainer/singularity".
_SIF_KINDS = ("singularity", "apptainer")


@dataclass
class ExecutionResult:
    """Outcome of one real execution.

    `exit_status` follows the shell convention and is the process's own — never a replayed
    one. `refused` separates "we did not run this" from "it ran and failed", which matters
    because the two call for opposite responses: a refusal is a configuration bug that will
    repeat for every job, a failure is the workflow's own business.
    """
    exit_status: int
    duration_s: float = 0.0
    refused: bool = False
    reason: str = ""
    stdout_path: Optional[str] = None
    stderr_path: Optional[str] = None
    command: List[str] = field(default_factory=list)


@dataclass
class ExecutionPolicy:
    """Process-wide execution policy, set once at agent startup from `runtime.execution`.

    Class-level rather than per-job for the same reason the simulation policy is: a `Job` is
    reconstructed from Redis on every read and has no route back to the agent's config.
    """
    mode: str = "simulate"                  # simulate | real
    work_dir: str = ""                      # shared scratch; all jobs of a run cd into it
    timeout_s: float = 3600.0
    container_runtime: str = "auto"         # auto | docker | apptainer | singularity | none
    path_rewrites: Tuple[Tuple[str, str], ...] = ()
    image_overrides: Dict[str, str] = field(default_factory=dict)
    capture_output: bool = True

    def enabled(self) -> bool:
        return str(self.mode).lower() == "real"


#: The live policy. Replaced wholesale by `configure()` so a half-applied config is not
#: reachable from another thread mid-update.
_POLICY = ExecutionPolicy()
logger = logging.getLogger(__name__)


def configure(**kwargs) -> ExecutionPolicy:
    """Install a new process-wide policy. Call once, at agent startup."""
    global _POLICY
    rewrites = kwargs.pop("path_rewrites", ()) or ()
    normalised: List[Tuple[str, str]] = []
    for entry in rewrites:
        if isinstance(entry, dict):
            src, dst = entry.get("from"), entry.get("to")
        else:
            src, dst = entry
        if src and dst:
            normalised.append((str(src), str(dst)))
    # Longest prefix first, so a specific rewrite is not pre-empted by a general one that
    # happens to have been listed earlier. Config order must not decide which path wins.
    normalised.sort(key=lambda p: len(p[0]), reverse=True)
    _POLICY = ExecutionPolicy(path_rewrites=tuple(normalised), **kwargs)
    return _POLICY


def policy() -> ExecutionPolicy:
    return _POLICY


def rewrite_path(path: str, pol: Optional[ExecutionPolicy] = None) -> str:
    """Map a submit-host path onto where the code actually lives on this fleet."""
    if not path:
        return path
    pol = pol or _POLICY
    for src, dst in pol.path_rewrites:
        if path == src or path.startswith(src.rstrip("/") + "/"):
            return dst.rstrip("/") + path[len(src.rstrip("/")):]
    return path


def resolve_image(spec: ExecutionSpec, pol: Optional[ExecutionPolicy] = None) -> str:
    """The image reference to hand the runtime, with the catalog's URI scheme honoured.

    An override keyed by container *name* exists because the catalogs here declare
    singularity `.sif` images while the agent VMs have only Docker. Substituting one for the
    other is a deployment decision with real consequences for what is being measured, so it
    is made explicitly in config and never inferred from what the host happens to have.
    """
    pol = pol or _POLICY
    container = spec.container
    if container is None:
        return ""
    override = pol.image_overrides.get(container.name)
    if override:
        return override
    image = container.image or ""
    if image.startswith("file://"):
        return rewrite_path(image[len("file://"):], pol)
    if image.startswith("docker://"):
        return image[len("docker://"):]
    return image


def _available_runtime(kind: str, pol: ExecutionPolicy) -> Optional[str]:
    """Which runtime binary to use for a container of this declared `kind`.

    Returns None when nothing suitable is installed, which is a refusal rather than a
    fallback: running the code outside its container would silently change what is being
    measured into "whatever this host has installed".
    """
    requested = (pol.container_runtime or "auto").lower()
    if requested == "none":
        return None
    if requested != "auto":
        return requested if shutil.which(requested) else None
    # `auto`: honour what the catalog declared, then accept the other family only if the
    # image reference is plausibly for it (an override will have rewritten a .sif already).
    order = _SIF_KINDS if (kind or "").lower() in _SIF_KINDS else ("docker",)
    for candidate in tuple(order) + KNOWN_RUNTIMES:
        if shutil.which(candidate):
            return candidate
    return None


def build_command(spec: ExecutionSpec, work_dir: str,
                  pol: Optional[ExecutionPolicy] = None) -> Tuple[List[str], str]:
    """Build the argv to run, or return ([], reason) explaining the refusal.

    Three shapes, and the bind mount in the container cases is the interesting part. A
    `stageable` transformation's code is *not* in the image — Pegasus stages it in at run
    time, which is why `spec.path` (`/srv/analyze_moisture`) exists inside the container and
    nowhere else. Binding the resolved pfn onto that exact path reproduces what Pegasus did,
    and keeps the in-container path in the job record meaningful rather than decorative.
    """
    pol = pol or _POLICY
    args = list(spec.arguments or [])
    container = spec.container

    if container is None:
        # No container: run the staged code directly. `spec.path` is an in-container path and
        # is meaningless here, so the pfn is the only thing that can be executed.
        target = rewrite_path(spec.pfn or "", pol)
        if not target:
            return [], "no container and no pfn: nothing to execute"
        if not os.path.exists(target):
            return [], f"executable not found at {target} (after path_rewrites)"
        return [target] + args, ""

    image = resolve_image(spec, pol)
    if not image:
        return [], f"container {container.name!r} has no resolvable image"
    runtime = _available_runtime(container.kind, pol)
    if runtime is None:
        return [], (f"no container runtime for kind {container.kind!r} "
                    f"(container_runtime={pol.container_runtime}); refusing to run outside "
                    f"the container")

    # Docker cannot run a singularity .sif, and handing it one produces a confusing runtime
    # error rather than an answer. This is the *default* situation on this fleet -- the agent
    # VMs have docker only and every catalog here declares a .sif -- so it is refused up
    # front, naming the config key that fixes it. An image that came from `image_overrides`
    # is trusted: supplying a docker image for a .sif container is exactly what that key is
    # for, and second-guessing it would make the escape hatch unusable.
    overridden = container.name in pol.image_overrides
    if runtime == "docker" and not overridden and (
            (container.kind or "").lower() in _SIF_KINDS or image.endswith(".sif")):
        return [], (f"container {container.name!r} is a {container.kind} image ({image}) and "
                    f"the only runtime available is docker, which cannot run it. Supply a "
                    f"docker image via runtime.execution.image_overrides[{container.name!r}], "
                    f"or install apptainer on the agents.")

    # Stageable code is bound onto the in-container path Pegasus used. `installed` code is
    # already in the image and must not be shadowed by a bind.
    binds: List[Tuple[str, str]] = []
    if (spec.pfn_type or "").lower() != "installed" and spec.pfn:
        host_pfn = rewrite_path(spec.pfn, pol)
        if not os.path.exists(host_pfn):
            return [], f"staged code not found at {host_pfn} (after path_rewrites)"
        binds.append((host_pfn, spec.path))

    if runtime == "docker":
        cmd = ["docker", "run", "--rm", "-v", f"{work_dir}:{work_dir}", "-w", work_dir]
        for host, inside in binds:
            cmd += ["-v", f"{host}:{inside}:ro"]
        # --entrypoint, because the image's own entrypoint would otherwise swallow the
        # command and run whatever the image was built to run.
        cmd += ["--entrypoint", spec.path, image] + args
        return cmd, ""

    # apptainer / singularity
    cmd = [runtime, "exec", "--bind", f"{work_dir}:{work_dir}", "--pwd", work_dir]
    for host, inside in binds:
        cmd += ["--bind", f"{host}:{inside}"]
    cmd += [image, spec.path] + args
    return cmd, ""


def run(spec: ExecutionSpec, job_id: str,
        work_dir: Optional[str] = None,
        timeout_s: Optional[float] = None,
        pol: Optional[ExecutionPolicy] = None) -> ExecutionResult:
    """Execute one job and return its real outcome.

    Never raises: a refusal and a crash are both reported as an `ExecutionResult`, because
    the caller is `Job.execute()`, whose own exception handler would otherwise record a
    configuration problem as a failed job and hide it in the workflow's own failure count.
    """
    pol = pol or _POLICY
    if not spec.runnable():
        return ExecutionResult(exit_status=1, refused=True,
                               reason="execution spec is not runnable "
                                      "(missing path, unknown arguments, or imageless container)")
    work_dir = work_dir or pol.work_dir
    if not work_dir:
        return ExecutionResult(exit_status=1, refused=True,
                               reason="no working directory configured "
                                      "(runtime.execution.work_dir)")
    try:
        os.makedirs(work_dir, exist_ok=True)
    except OSError as exc:
        return ExecutionResult(exit_status=1, refused=True,
                               reason=f"working directory {work_dir} unusable: {exc}")

    cmd, refusal = build_command(spec, work_dir, pol)
    if not cmd:
        return ExecutionResult(exit_status=1, refused=True, reason=refusal)

    # Output goes to files, never to pipes. A chatty job piped into memory would grow without
    # bound in the agent's own process, and the executor runs several jobs at once.
    out_path = err_path = None
    stdout = stderr = subprocess.DEVNULL
    if pol.capture_output:
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(job_id))
        log_dir = os.path.join(work_dir, ".swarm-logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
            out_path = os.path.join(log_dir, f"{safe}.out")
            err_path = os.path.join(log_dir, f"{safe}.err")
            stdout = open(out_path, "wb")
            stderr = open(err_path, "wb")
        except OSError as exc:
            # Losing the logs must not lose the run; fall back to discarding output.
            logger.warning("[EXEC] %s: cannot capture output (%s)", job_id, exc)
            out_path = err_path = None
            stdout = stderr = subprocess.DEVNULL

    started = time.monotonic()
    proc = None
    try:
        # New process group, so a timeout can kill the whole tree. A container launcher is a
        # parent of the real work: killing only the child leaves the container running and
        # the job's resources held for the rest of the run.
        proc = subprocess.Popen(cmd, cwd=work_dir, stdout=stdout, stderr=stderr,
                                stdin=subprocess.DEVNULL, start_new_session=True)
        rc = proc.wait(timeout=timeout_s if timeout_s is not None else pol.timeout_s)
        return ExecutionResult(exit_status=int(rc), duration_s=time.monotonic() - started,
                               stdout_path=out_path, stderr_path=err_path, command=cmd)
    except subprocess.TimeoutExpired:
        _kill_group(proc)
        limit = timeout_s if timeout_s is not None else pol.timeout_s
        return ExecutionResult(exit_status=124,  # conventional timeout status
                               duration_s=time.monotonic() - started,
                               reason=f"timed out after {limit}s",
                               stdout_path=out_path, stderr_path=err_path, command=cmd)
    except (OSError, ValueError) as exc:
        _kill_group(proc)
        # Could not even start it — a configuration problem, not a job failure.
        return ExecutionResult(exit_status=1, refused=True,
                               duration_s=time.monotonic() - started,
                               reason=f"could not start {cmd[0]!r}: {exc}",
                               stdout_path=out_path, stderr_path=err_path, command=cmd)
    finally:
        for handle in (stdout, stderr):
            if handle not in (subprocess.DEVNULL, None):
                try:
                    handle.close()
                except OSError:
                    pass


def _kill_group(proc) -> None:
    """SIGKILL the process group, tolerating a process that already exited."""
    if proc is None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass


def describe(cmd: List[str]) -> str:
    """A copy-pasteable rendering of a command, for logs."""
    return " ".join(shlex.quote(c) for c in cmd)
