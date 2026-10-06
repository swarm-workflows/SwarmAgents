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
import hashlib
import logging
import re
import os
import shlex
import shutil
import signal
import subprocess
import threading
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

from swarm.models.execution import ExecutionSpec

#: Container runtimes understood here, in the order `auto` prefers them.
KNOWN_RUNTIMES = ("apptainer", "singularity", "docker")

#: Catalog container kinds that mean "a .sif run by apptainer/singularity". These are the
#: words a transformation catalog may use, not binaries to invoke.
_SIF_KINDS = ("singularity", "apptainer")

#: Extensions that make an image reference unambiguously a FILE rather than a registry
#: reference, whatever the catalog called the container's kind.
_IMAGE_FILE_SUFFIXES = (".sif", ".simg", ".img", ".sqsh")

#: Binaries that can run a .sif, in preference order — which is a different list from the
#: kinds above, though the words overlap. `apptainer` comes first because it is the real
#: binary: current packages ship `singularity` only as a compatibility symlink to it, so
#: preferring that name depends on an alias a future package could drop, and logs a command
#: naming a program that is not what ran.
_SIF_RUNTIMES = ("apptainer", "singularity")

#: Substrings marking an environment variable as the agent's business, not the job's. A job
#: inherits the agent's environment, which on this fleet holds `OPENAI_API_KEY` among other
#: things; handing that to arbitrary workflow code -- inside a container built by someone
#: else, whose stdout is captured to a shared filesystem -- is not something a scheduler
#: should do quietly. Matched case-insensitively as substrings, so a new `..._TOKEN` is
#: covered without anyone remembering to add it.
_SECRET_MARKERS = ("KEY", "TOKEN", "SECRET", "PASSWORD", "PASSWD", "CREDENTIAL",
                   # Added 2026-10-06 (code review §73): `*_PASS`, `*_AUTH*`, and the ssh agent
                   # socket, through which a job could use the agent's ssh identity.
                   "PASS", "AUTH", "SSH_AUTH_SOCK")

#: A URL with credentials in it (`redis://user:pw@host`, `http_proxy=http://u:p@proxy`): the
#: variable NAME says nothing, the value is the secret.
_URL_CREDENTIALS = re.compile(r"://[^/@\s:]+:[^/@\s]+@")

#: Agent-internal variables that are not secrets but mean nothing to a job and would be
#: actively misleading inside one.
_AGENT_ONLY_VARS = ("SWARM_RUN_ID", "LLM_BASE_URL")


def job_environment(base: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """The environment a job runs with: the agent's, minus the agent's own secrets.

    A denylist rather than an allowlist, deliberately. Real workflow code needs an
    unremarkable amount of ambient environment (PATH, HOME, LANG, proxy settings), and an
    allowlist would break workflows in ways that look like workflow bugs. The denylist errs
    the other way -- it can drop a variable a job legitimately wanted -- which fails loudly
    in that job rather than silently leaking a credential.
    """
    env = dict(os.environ if base is None else base)
    for name in list(env):
        upper = name.upper()
        if (name in _AGENT_ONLY_VARS or any(m in upper for m in _SECRET_MARKERS)
                or _URL_CREDENTIALS.search(str(env.get(name, "")))):
            env.pop(name, None)
    return env


class TransientRefusal(str):
    """A refusal reason worth retrying. Still a `str`, so a caller that only reads the text
    — every one that predates retry — is unaffected; `run()` checks the type."""


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
    # Seconds spent pulling the container image before the job started; outside `duration_s`.
    pull_s: float = 0.0
    # A refusal that may not happen again: an input could not be fetched (producer or staging
    # site unreachable, fetch deadline) or the registry could not be read. The agent returns
    # such a job to the pool, up to `runtime.execution.refusal_retries`. Every other refusal is
    # a configuration fault that would repeat on any agent, and fails at once.
    transient: bool = False
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
    # Run apptainer with `--containall`: no $HOME or host /tmp bind, a clean environment and
    # private IPC/PID namespaces. Without it apptainer bound $HOME and /tmp — on a root-run agent
    # that exposed /root/.ssh (the root mesh key) to workflow code and made `job_environment`'s
    # scrubbing moot, and the job saw the whole host environment where docker gave it none
    # (code review 2026-10-05 §69). Escape hatch for a workflow that needs the host home.
    apptainer_containall: bool = True
    # Under `container_runtime: auto`, whether a container may run under the OTHER runtime family
    # when its declared one is not installed (a docker image under apptainer). Off: that silently
    # changed the runtime being compared against Pegasus (code review 2026-10-05 §72).
    allow_runtime_substitution: bool = False
    # How long a docker image pull may take before the job is refused (transient).
    pull_timeout_s: float = 1800.0
    # Where to find the three kinds of thing a job needs, for jobs that name them RELATIVELY.
    # They map onto fields that already exist rather than introducing new ones:
    #   code   <- ExecutionSpec.pfn          (the executable)
    #   inputs <- Job.data_in                (the files it reads)
    #   images <- ContainerSpec.image        (the container)
    # An ABSOLUTE path is used as written, so a Pegasus-derived job -- whose catalog records
    # absolute submit-host paths -- keeps working through `path_rewrites` exactly as before.
    # Relative is for jobs authored directly, where a root is the natural way to say it.
    roots: Dict[str, str] = field(default_factory=dict)

    def enabled(self) -> bool:
        return str(self.mode).lower() == "real"


#: The live policy. Replaced wholesale by `configure()` so a half-applied config is not
#: reachable from another thread mid-update.
_POLICY = ExecutionPolicy()

DEFAULT_MODE = "simulate"


def resolve_mode(execution_cfg: Optional[dict]) -> str:
    """`runtime.execution.mode`, resolved in ONE place.

    The default lives here and nowhere else. It was briefly resolved twice — the agent
    defaulting an absent key to `simulate` (correct) and `run_test.py`'s bundle validation
    treating an absent key as "cannot tell, assume it executes" — so a perfectly ordinary
    simulated replay, read through a config with no execution block, was refused for a
    file-name collision that could never be acted on.

    An unknown value raises rather than defaulting, for the same reason `consensus.protocol`
    does: the two modes produce results that cannot be compared, and nothing in the run would
    say which one had been used.
    """
    mode = str((execution_cfg or {}).get("mode", DEFAULT_MODE)).lower()
    if mode not in ("simulate", "real"):
        raise ValueError(
            f"runtime.execution.mode {mode!r} is not 'simulate' or 'real'. Refused "
            "rather than defaulted: the two produce results that cannot be compared.")
    return mode
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


def resolve_under_root(path: str, kind: str, pol: Optional[ExecutionPolicy] = None) -> str:
    """Resolve one path: absolute as written (after rewrites), relative under its root.

    The split is what lets one mechanism serve two quite different callers. A job converted
    from a Pegasus run carries absolute submit-host paths, which `path_rewrites` maps onto
    this fleet; a job authored by hand says `bin/analyze.py` and means "in the code root".
    Neither has to know about the other.

    Returns "" when the path cannot be resolved — no root configured for its kind, or a
    relative path that climbs out of that root. Call `unresolved_reason` for which. Guessing
    a base directory would run whatever happened to sit at that relative path from the
    process's own working directory.

    **Containment is enforced here**, in the single place every relative path passes through.
    A job record is workflow-supplied data: `../../usr/bin/something` joined onto a root
    escapes it, and the whole point of naming a root is that everything resolves inside it.
    The check is lexical (`normpath`), which is what defeats `..`; a symlink *inside* the
    root is placed by whoever administers the root and is deliberately still followed.
    """
    if not path:
        return ""
    if os.path.isabs(path):
        return rewrite_path(path, pol)
    pol = pol or _POLICY
    root = (pol.roots or {}).get(kind, "")
    if not root:
        return ""
    root_norm = os.path.normpath(root)
    candidate = os.path.normpath(os.path.join(root_norm, path))
    # `root_norm + os.sep` is wrong for a root that already ends in the separator: a root of
    # "/" made the prefix "//", which nothing matches, so every path under it was refused.
    prefix = root_norm if root_norm.endswith(os.sep) else root_norm + os.sep
    if candidate != root_norm and not candidate.startswith(prefix):
        return ""
    return candidate


def unresolved_reason(path: str, kind: str, pol: Optional[ExecutionPolicy] = None) -> str:
    """Why `resolve_under_root` returned nothing, so a refusal can say which it was.

    "No root configured" and "climbs out of the root" are different mistakes with different
    fixes, and a message covering both vaguely sends a reader to the wrong one.
    """
    pol = pol or _POLICY
    root = (pol.roots or {}).get(kind, "")
    if not root:
        return f"runtime.execution.roots.{kind} is not set, so {path!r} cannot be resolved"
    return (f"{path!r} resolves outside runtime.execution.roots.{kind} ({root}); a relative "
            f"path may not climb out of its root")


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
    # An override REPLACES the catalog's image, so it goes through the same resolution as
    # any other value. Returning it verbatim was the last remaining way a relative path
    # reached the runtime unresolved -- and so got resolved against the agent's own working
    # directory -- which is the fall-back refused everywhere else here. It matters because
    # `image_overrides` is the documented answer to "the catalog names a .sif and this host
    # runs docker", so writing a path there is the natural thing to do.
    #
    # What the override does NOT inherit is the catalog's `kind`. Substituting a docker
    # image for a singularity one is the entire purpose of the key, so classifying the
    # override by the kind it is replacing would refuse exactly the case it exists for. The
    # override is classified by its own shape instead: a recognisable image-file suffix, or
    # something that actually exists under the images root, is a file; anything else is a
    # registry reference.
    override = pol.image_overrides.get(container.name)
    image = override or (container.image or "")
    treat_as_file_kind = (container.kind or "").lower() in _SIF_KINDS and not override
    if image.startswith("file://"):
        # A file:// image is a local path under every runtime, so it resolves the same way
        # for all of them and the rewrite applies here.
        return rewrite_path(image[len("file://"):], pol)
    if image and not os.path.isabs(image) and "://" not in image:
        # A bare reference with no scheme is ambiguous, and the two container kinds resolve
        # it in opposite ways: `ubuntu:22.04` is a REGISTRY reference that docker looks up
        # itself, while `Soil.sif` is a FILE that lives in the images root. Treating every
        # bare name as a path broke every catalog that names a plain docker tag; treating
        # every bare name as a registry reference would send a hand-authored .sif to a
        # registry. The catalog's own `kind` is what distinguishes them, with a filename
        # suffix as the tiebreak when the kind is missing or wrong.
        looks_like_file = image.lower().endswith(_IMAGE_FILE_SUFFIXES)
        # NOTE: no filesystem check here. "A local file wins" is an APPTAINER rule -- only
        # apptainer can run an image from a path. Applied here, where the runtime is not yet
        # known, it also fired for docker, turning a valid registry reference that happened
        # to collide with a name in the images root into `docker run /imgs/ubuntu`, which
        # docker cannot execute. The check belongs in the apptainer branch of
        # `build_command`, and that is where it is.
        if looks_like_file or treat_as_file_kind:
            # Returning the bare name when no root is configured would hand the runtime a
            # relative path, which it resolves against the process's own working directory:
            # the exact fall-back this is documented not to do. Return "" so `build_command`
            # refuses and names the key.
            return resolve_under_root(image, "images", pol)
        return image                       # registry reference; the runtime resolves it
    # `docker://` is deliberately NOT stripped here: whether it belongs depends on the
    # runtime, so it is decided in `build_command`. Docker wants a bare `repo:tag`;
    # apptainer *requires* the scheme (`apptainer exec docker://repo:tag`) and, given a bare
    # reference, looks for a local file of that name and fails with a confusing error.
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
    # `auto`: the family the catalog declared. The other family only when explicitly allowed —
    # the comment here used to promise "only if the image reference is plausibly for it" while
    # the code accepted any installed runtime, so a docker container on a docker-less host ran
    # under apptainer with nothing saying so (§72).
    order = _SIF_RUNTIMES if (kind or "").lower() in _SIF_KINDS else ("docker",)
    candidates = tuple(order) + (KNOWN_RUNTIMES if pol.allow_runtime_substitution else ())
    for candidate in candidates:
        if shutil.which(candidate):
            if candidate not in order:
                logger.warning("[EXEC] %s container running under %s (substitution allowed by "
                               "runtime.execution.allow_runtime_substitution)", kind, candidate)
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
        if not spec.pfn:
            return [], "no container and no pfn: nothing to execute"
        target = resolve_under_root(spec.pfn, "code", pol)
        if not target:
            # Distinguished from "no pfn at all": there IS one, and it cannot be resolved.
            # The reason says which — an unset root and an escaping path are different
            # mistakes with different fixes.
            return [], f"pfn: {unresolved_reason(spec.pfn, 'code', pol)}"
        if not os.path.exists(target):
            return [], f"executable not found at {target} (after path_rewrites)"
        return [target] + args, ""

    image = resolve_image(spec, pol)
    if not image:
        declared = container.image or ""
        if (declared and not os.path.isabs(declared) and "://" not in declared
                and (declared.lower().endswith(_IMAGE_FILE_SUFFIXES)
                     or (container.kind or "").lower() in _SIF_KINDS)):
            return [], f"container image: {unresolved_reason(declared, 'images', pol)}"
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
    if runtime == "docker" and not overridden and not image.startswith("docker://") and (
            (container.kind or "").lower() in _SIF_KINDS or image.endswith(".sif")):
        return [], (f"container {container.name!r} is a {container.kind} image ({image}) and "
                    f"the only runtime available is docker, which cannot run it. Supply a "
                    f"docker image via runtime.execution.image_overrides[{container.name!r}], "
                    f"or install apptainer on the agents.")

    # Stageable code is bound onto the in-container path Pegasus used. `installed` code is
    # already in the image and must not be shadowed by a bind.
    binds: List[Tuple[str, str]] = []
    if (spec.pfn_type or "").lower() != "installed" and spec.pfn:
        host_pfn = resolve_under_root(spec.pfn, "code", pol)
        if not host_pfn:
            return [], f"pfn: {unresolved_reason(spec.pfn, 'code', pol)}"
        if not os.path.exists(host_pfn):
            return [], f"staged code not found at {host_pfn}"
        binds.append((host_pfn, spec.path))

    if runtime == "docker":
        # Docker takes a bare reference; the scheme, if the catalog carried one, is ours to
        # remove here rather than in `resolve_image` (see there).
        if image.startswith("docker://"):
            image = image[len("docker://"):]
        if os.path.isabs(image):
            # Docker cannot run an image from a filesystem path -- it resolves references
            # against a registry or its local image store, never a file. Refusing here says
            # so plainly instead of letting docker fail with "invalid reference format".
            return [], (f"{image} is a filesystem path and the runtime is docker, which can "
                        f"only run a registry reference. Use apptainer for a local image, or "
                        f"set runtime.execution.image_overrides[{container.name!r}] to a "
                        f"docker image.")
        cmd = ["docker", "run", "--rm", "-v", f"{work_dir}:{work_dir}", "-w", work_dir]
        for host, inside in binds:
            cmd += ["-v", f"{host}:{inside}:ro"]
        # --entrypoint, because the image's own entrypoint would otherwise swallow the
        # command and run whatever the image was built to run.
        cmd += ["--entrypoint", spec.path, image] + args
        return cmd, ""

    # apptainer / singularity
    #
    # The mirror of the docker case above, and it has to be done explicitly. Apptainer reads a
    # bare reference as a LOCAL FILE NAME: handed `ubuntu:22.04` it looks for a file of that
    # name in the working directory and fails with an error naming a path nobody wrote. It
    # needs the scheme to pull from a registry. So docker has its scheme removed and apptainer
    # has one added, from the same resolved reference.
    #
    # This is not a guess about an ambiguous string: it only applies to what `resolve_image`
    # already classified as a registry reference (by the catalog's `kind`, or by the absence
    # of an image-file suffix). A resolved .sif is an absolute path by this point and is left
    # exactly as it is.
    if (image and "://" not in image and not os.path.isabs(image)
            and not image.lower().endswith(_IMAGE_FILE_SUFFIXES)):
        # Before deciding this is a registry reference, LOOK. A suffix is a hint, not proof:
        # an apptainer **sandbox** is a directory with no extension at all
        # (`apptainer build --sandbox mybox/ …` then `apptainer exec mybox/ …`), and an image
        # file in the images root need not be named `.sif`. Prefixing either turns a local
        # image into a registry pull for something that does not exist, which fails with a
        # network error about a repository nobody published.
        #
        # Existence is checkable, so it is checked rather than inferred from the string. Only
        # a reference that resolves to nothing on disk is treated as a registry reference.
        # Only the configured images root counts. An earlier version also tried
        # `os.path.exists(image)` as a fall-back, which tests the path against the AGENT'S
        # OWN working directory — so an unrelated file that happens to share the name gets
        # run instead of the image. That is the cwd fall-back this module refuses everywhere
        # else, reintroduced by accident while fixing the sandbox case.
        local = resolve_under_root(image, "images", pol)
        image = local if (local and os.path.exists(local)) else "docker://" + image
        # (apptainer only -- see the note in `resolve_image`.)
    cmd = [runtime, "exec"]
    if pol.apptainer_containall:
        cmd.append("--containall")
    cmd += ["--bind", f"{work_dir}:{work_dir}", "--pwd", work_dir]
    for host, inside in binds:
        # Read-only, as under docker: the staged code bundle is shared by every agent of the
        # run, and a job must not be able to change the code the next job runs (§69).
        cmd += ["--bind", f"{host}:{inside}:ro"]
    cmd += [image, spec.path] + args
    return cmd, ""


def stage_inputs(data_in, work_dir: str,
                 pol: Optional[ExecutionPolicy] = None,
                 locator=None, run_id: str = "", requester: str = "") -> Tuple[List[str], str]:
    """Put the job's declared inputs in the working directory. Returns (staged, refusal).

    Driven by `Job.data_in`, which already records what a job reads — there is no second list
    to keep in step with it.

    Four sources are tried per input, **in this order**, and the order is load-bearing:

    1. **Already in the working directory** — a parent job that ran on this agent, or another
       agent's copy under a shared mount.
    2. **Produced by this run, on another agent** — resolved through *locator* and fetched
       (`swarm/execution/staging.py`). This is deliberately ahead of the inputs root: a name in
       the location registry was produced *by this run*, and a file of the same name sitting in
       the inputs root is a collision, not a copy. Workflow file names are a flat namespace —
       62 colliding names were measured in the shipped profile — so preferring the run's own
       output is the difference between a child reading its parent's result and reading last
       week's file of the same name.
    3. **The inputs root** — the run's curated root inputs, which by definition no job produces.
       This is the shared-mount path and is unchanged.
    4. Otherwise refuse, naming which of the three it was.

    *locator* is `repository.data_locations`, injected rather than imported so this module keeps
    no dependency on the data layer; passing None (the default, and what every simulated run
    does) collapses this back to exactly the previous behaviour.

    Three rules, and each is a way this would otherwise go quietly wrong:

    * **Never overwrite.** A file already in the working directory is either a parent job's
      output or another agent's copy of the same input. Overwriting the first with a stale
      replica would corrupt a DAG in the most confusing way available: the parent ran, the
      child read something else.
    * **Copy atomically.** The working directory is shared, and several agents stage
      concurrently; a half-written file is readable and looks complete. Write to a unique
      temporary name in the same directory, then `os.replace`, which is atomic on POSIX.
    * **Refuse a missing input**, rather than letting the job start without it. A job whose
      input is absent does not usually fail — it produces empty or default output, which is
      indistinguishable from a real result until someone checks the numbers.
    """
    pol = pol or _POLICY
    staged: List[str] = []

    # One lookup for the whole job's inputs rather than one per name: staging runs on the
    # execution path of every job in a workflow, and a round trip per DAG edge would put the
    # WAN into it. Absent locator (simulated runs, staging off) costs nothing at all.
    locations: dict = {}
    staging_on = False
    try:
        from swarm.execution import staging
        staging_on = staging.policy().enabled
        if staging_on and (locator is None or not run_id):
            # The process-wide context is how a `Job` — rebuilt from Redis with no route back
            # to the agent — reaches the lookup at all.
            ctx = staging.context()
            locator = locator or ctx.locator
            run_id = run_id or ctx.run_id
            requester = requester or ctx.agent_id
    except Exception:                       # noqa: BLE001 - staging is optional
        staging_on = False

    # Why the registry could not be consulted, or "" when it could. **Recorded here and acted on
    # per name, inside the loop** — not returned from here. Refusing up front rejected jobs that
    # never needed the registry at all: one with no declared inputs, and one whose inputs a
    # parent on this very agent had already written into the working directory. Neither can read
    # a stale file, because neither reads the inputs root. The ambiguity this guards against is
    # narrower than the whole function, so the guard has to be too.
    lookup_unavailable = ""
    lookup_transient = False      # a failed lookup may succeed later; an absent one never will
    if staging_on and locator is None:
        lookup_unavailable = "no location lookup is configured"
    # Names this run produced, from the READINESS set. The guard below used to treat "produced
    # by this run" as "has a location" — the wrong container: a name in the readiness set with
    # no location (re-published by `_retry_unpublished_data`, which writes none, or produced by
    # an agent with staging off) fell through to the inputs root and read last week's file of
    # the same name (code review 2026-10-05 §63).
    produced_here: set = set()
    produced = None
    if staging_on:
        try:
            from swarm.execution import staging as _staging
            produced = _staging.context().produced
        except Exception:                    # noqa: BLE001
            produced = None

    if locator is not None:
        wanted = [os.path.basename(str(getattr(n, "file", "") or ""))
                  for n in (data_in or []) if getattr(n, "file", None)]
        wanted = [w for w in wanted if w and w not in (".", "..")]
        if wanted:
            try:
                locations = locator(wanted) or {}
            except Exception as exc:       # noqa: BLE001
                if staging_on:
                    lookup_unavailable = f"the location lookup failed ({exc})"
                    lookup_transient = True
                else:
                    logger.warning("[STAGE] location lookup failed (%s); staging is off, so "
                                   "resolving inputs locally as usual", exc)
            if produced is not None:
                try:
                    produced_here = set(produced(wanted) or ())
                except Exception as exc:   # noqa: BLE001
                    lookup_unavailable = lookup_unavailable or (
                        f"the produced-names lookup failed ({exc})")
                    lookup_transient = True

    for node in data_in or []:
        # `DataNode.name` is the SITE (`local`, `dtn3`); `file` is the logical file name.
        # Only `file` may be used here — falling back to `name` would try to stage a file
        # called "dtn3". A per-site conversion carries no `file` at all, so such a node
        # simply describes where data lives and has nothing to stage; skip it rather than
        # invent a name. (Per-file conversion, which `--dag-gating` already forces, is what
        # produces stageable nodes.)
        name = getattr(node, "file", None)
        if not name:
            continue
        if staging_on and "/" in str(name):
            from swarm.execution import staging as _st
            if _st.plain_name(name) is None:
                return staged, (f"input {name!r} has a directory component, which staging does "
                                f"not support: the producer and this consumer would key it "
                                f"differently")
        # basename is the traversal guard: a declared name may not escape the working
        # directory, whatever the workflow says.
        name = os.path.basename(str(name))
        if not name or name in (".", ".."):
            continue
        dest = os.path.join(work_dir, name)
        if os.path.exists(dest):
            continue                        # parent output, or already staged

        # Produced by this run on another agent: fetch it before considering the inputs root.
        locs = locations.get(name)
        if locs:
            from swarm.execution import staging          # local import: keeps the data path optional
            # Peer first, staging site last — the producer sets that order (see
            # `Repository.data_locations`), so the common case is one hop and the store is
            # reached only when the producer cannot answer.
            result = staging.fetch_any(name, locs, work_dir, run_id=run_id, requester=requester)
            if result.ok:
                staged.append(name)
                continue
            # A produced file we cannot fetch is a refusal, not a reason to fall back to a
            # same-named file elsewhere: that is precisely how a child would silently read the
            # wrong input. The reason names the producer, because the usual cause is that it died.
            producers = ", ".join(str(l.get("agent_id", l.get("host", "?"))) for l in locs)
            return staged, TransientRefusal(
                f"input {name!r} was produced by agent {producers} but could not be staged: "
                f"{result.reason}")

        if lookup_unavailable:
            # This name is not in the working directory, so the next step would resolve it from
            # the inputs root — and with staging on, a name a parent produced lives on another
            # agent while a same-named file in the root is a collision, not a copy. Without the
            # registry the two are indistinguishable, so this is the one place the ambiguity is
            # real. Note the refusal is reached only for a name that would otherwise have been
            # read from the root: a job whose inputs are all already local never gets here.
            reason = (f"input {name!r} is not in the working directory and staging is "
                      f"enabled but {lookup_unavailable}, so a name produced by another "
                      f"agent cannot be told apart from a same-named file in the inputs "
                      f"root; refusing rather than risking a stale input")
            return staged, (TransientRefusal(reason) if lookup_transient else reason)

        if name in produced_here:
            # Produced by THIS run, and nobody recorded where. A same-named file in the inputs
            # root is a collision, not this run's output.
            return staged, (f"input {name!r} was produced by this run but has no recorded "
                            f"location, so it cannot be fetched; refusing rather than read a "
                            f"same-named file from the inputs root")

        src = resolve_under_root(name, "inputs", pol)
        if not src:
            return staged, (f"input {name!r} is not in the working directory and "
                            f"{unresolved_reason(name, 'inputs', pol)}")
        if not os.path.isfile(src):
            return staged, (f"input {name!r} is not in the working directory and was not "
                            f"found in the inputs root ({(pol.roots or {})['inputs']})")
        tmp = f"{dest}.staging.{os.getpid()}.{threading.get_ident()}"
        try:
            shutil.copy2(src, tmp)
            # Exclusive link, NOT os.replace. The existence check above is check-then-act:
            # a parent job can finish and write its output during the copy, and a replace
            # would then clobber a fresh result with a stale replica -- precisely the
            # corruption the never-overwrite rule exists to prevent, just through a window
            # instead of directly. os.link fails if the destination exists, which makes the
            # rule atomic rather than merely intended.
            try:
                os.link(tmp, dest)
                staged.append(name)
            except FileExistsError:
                pass                        # someone won the race; their copy stands
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        except OSError as exc:
            try:
                os.unlink(tmp)
            except OSError:
                pass
            return staged, f"could not stage input {name!r}: {exc}"
    return staged, ""


def run(spec: ExecutionSpec, job_id: str,
        work_dir: Optional[str] = None,
        timeout_s: Optional[float] = None,
        pol: Optional[ExecutionPolicy] = None,
        data_in=None,
        locator=None, run_id: str = "", requester: str = "",
        data_out=None) -> ExecutionResult:
    """Execute one job and return its real outcome.

    Never raises: a refusal and a crash are both reported as an `ExecutionResult`, because
    the caller is `Job.execute()`, whose own exception handler would otherwise record a
    configuration problem as a failed job and hide it in the workflow's own failure count.
    """
    pol = pol or _POLICY
    spec_refusal = spec.refusal_reason()
    if spec_refusal:
        # The precise reason, from the model that decided it. The old message listed every
        # clause at once, so a clustered job (a designed refusal) and an unparseable argv (a
        # real extraction defect) produced identical log lines and were investigated as the
        # same problem.
        return ExecutionResult(exit_status=1, refused=True,
                               reason=f"execution spec is not runnable: {spec_refusal}")
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

    # Inputs first: a missing one is a configuration problem, and finding that out before
    # starting a container is both faster and a clearer error than after.
    _staged, staging_refusal = stage_inputs(data_in, work_dir, pol,
                                            locator=locator, run_id=run_id,
                                            requester=requester)
    if staging_refusal:
        return ExecutionResult(exit_status=1, refused=True, reason=str(staging_refusal),
                               transient=isinstance(staging_refusal, TransientRefusal))

    cmd, refusal = build_command(spec, work_dir, pol)
    if not cmd:
        return ExecutionResult(exit_status=1, refused=True, reason=refusal)

    _clear_declared_outputs(data_out, work_dir, job_id)

    # Output goes to files, never to pipes. A chatty job piped into memory would grow without
    # bound in the agent's own process, and the executor runs several jobs at once.
    out_path = err_path = None
    stdout = stderr = subprocess.DEVNULL
    if pol.capture_output:
        # One file per ATTEMPT, and the id's hash in the name: opening `<id>.out` with "wb"
        # truncated a re-run's predecessor, and sanitising `a/b` and `a_b` to one name made two
        # jobs share a log (§73).
        safe = "".join(c if c.isalnum() or c in "-_." else "_" for c in str(job_id))
        safe = (f"{safe}-{hashlib.sha1(str(job_id).encode()).hexdigest()[:8]}"
                f"-{time.time_ns()}")
        log_dir = os.path.join(work_dir, ".swarm-logs")
        try:
            os.makedirs(log_dir, exist_ok=True)
            out_path = os.path.join(log_dir, f"{safe}.out")
            err_path = os.path.join(log_dir, f"{safe}.err")
            stdout = open(out_path, "wb")
            stderr = open(err_path, "wb")
        except OSError as exc:
            # Losing the logs must not lose the run; fall back to discarding output. Close
            # whichever handle did open first -- the fallback rebinds both names to DEVNULL,
            # so without this the `finally` would close DEVNULL and leak the real file.
            for handle in (stdout, stderr):
                if handle not in (subprocess.DEVNULL, None):
                    try:
                        handle.close()
                    except OSError:
                        pass
            logger.warning("[EXEC] %s: cannot capture output (%s)", job_id, exc)
            out_path = err_path = None
            stdout = stderr = subprocess.DEVNULL

    cmd, container = _name_container(cmd, job_id, run_id)
    # Pull before the clock starts. A missing docker image used to be pulled by `docker run`
    # inside the job's timed duration, and a pull failure (125) was recorded as the JOB failing
    # (§72). Apptainer's conversion of a `docker://` reference on first exec is still inside it.
    pull_s, pull_refusal = _ensure_docker_image(cmd, pol)
    if pull_refusal:
        return ExecutionResult(exit_status=1, refused=True, transient=True,
                               reason=pull_refusal, pull_s=pull_s)
    started = time.monotonic()
    proc = None
    try:
        # New process group, so a timeout can kill the whole tree. A container launcher is a
        # parent of the real work: killing only the child leaves the container running and
        # the job's resources held for the rest of the run.
        proc = subprocess.Popen(cmd, cwd=work_dir, stdout=stdout, stderr=stderr,
                                stdin=subprocess.DEVNULL, start_new_session=True,
                                env=job_environment())
        _register(proc, container)
        rc = proc.wait(timeout=timeout_s if timeout_s is not None else pol.timeout_s)
        return ExecutionResult(exit_status=int(rc), duration_s=time.monotonic() - started,
                               stdout_path=out_path, stderr_path=err_path, command=cmd)
    except subprocess.TimeoutExpired:
        _kill_group(proc, container)
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
        _unregister(proc)
        for handle in (stdout, stderr):
            if handle not in (subprocess.DEVNULL, None):
                try:
                    handle.close()
                except OSError:
                    pass


# Jobs this process has started and not yet reaped: pid -> (Popen, container name or None).
# `terminate_all` uses it at agent shutdown, because `start_new_session` deliberately detaches
# every job from the agent's process group — so when the agent exits after its drain timeout,
# nothing else would stop them, and run N's jobs went on competing with run N+1's.
_LIVE: Dict[int, tuple] = {}
_LIVE_LOCK = threading.Lock()


def _register(proc, container: Optional[str]) -> None:
    with _LIVE_LOCK:
        _LIVE[proc.pid] = (proc, container)


def _unregister(proc) -> None:
    if proc is None:
        return
    with _LIVE_LOCK:
        _LIVE.pop(proc.pid, None)


def _name_container(cmd: List[str], job_id: str, run_id: str) -> Tuple[List[str], Optional[str]]:
    """Give a `docker run` a unique name and an init process, so it can be killed.

    A docker container belongs to dockerd, not to the `docker run` client this module starts:
    SIGKILLing the client's process group on a timeout left the container running — burning
    CPU and writing into the run's working directory after the job was recorded 124 — and
    `--rm` removes it only when it exits on its own (code review 2026-10-05 §62). A name makes
    `docker kill` possible; `--init` makes the container's own PID 1 forward signals.
    Apptainer and bare commands run inside the process group and need neither.
    """
    if len(cmd) < 2 or os.path.basename(cmd[0]) != "docker" or cmd[1] != "run":
        return cmd, None
    safe = lambda s: "".join(c if c.isalnum() or c in "-_." else "_" for c in str(s))[:40]
    name = f"swarm-{safe(run_id or 'run')}-{safe(job_id)}-{os.getpid()}-{int(time.time()*1000)}"
    return [cmd[0], "run", "--name", name, "--init"] + cmd[2:], name


def _ensure_docker_image(cmd: List[str], pol: "ExecutionPolicy") -> Tuple[float, str]:
    """Make sure a `docker run`'s image is local. Returns (seconds spent, refusal or "")."""
    if len(cmd) < 2 or os.path.basename(cmd[0]) != "docker" or cmd[1] != "run":
        return 0.0, ""
    try:
        image = cmd[cmd.index("--entrypoint") + 2]
    except (ValueError, IndexError):
        return 0.0, ""
    started = time.monotonic()
    try:
        have = subprocess.run(["docker", "image", "inspect", image], stdout=subprocess.DEVNULL,
                              stderr=subprocess.DEVNULL, timeout=60).returncode == 0
        if have:
            return time.monotonic() - started, ""
        logger.info("[EXEC] pulling %s before the job starts", image)
        pulled = subprocess.run(["docker", "pull", image], stdout=subprocess.DEVNULL,
                                stderr=subprocess.PIPE, timeout=pol.pull_timeout_s, text=True)
    except (OSError, subprocess.TimeoutExpired) as exc:
        return time.monotonic() - started, f"could not pull image {image}: {exc}"
    elapsed = time.monotonic() - started
    if pulled.returncode != 0:
        return elapsed, (f"could not pull image {image}: "
                         f"{(pulled.stderr or '').strip()[:300]}")
    logger.info("[EXEC] pulled %s in %.1fs (not counted in the job's duration)", image, elapsed)
    return elapsed, ""


def terminate_all() -> int:
    """Kill every job this process started that is still running. Returns how many."""
    with _LIVE_LOCK:
        live = list(_LIVE.values())
    for proc, container in live:
        _kill_group(proc, container)
    return len(live)


def _clear_declared_outputs(data_out, work_dir: str, job_id: str) -> None:
    """Remove this job's declared outputs from the working directory before it runs.

    The working directory is shared and outlives jobs, so a declared output can already be
    there — this job's earlier attempt, or a copy left by an agent that was judged failed while
    still running. A job that then exits 0 WITHOUT rewriting it had the stale file published as
    its output, and every descendant read it (code review 2026-10-05 §67). With the old file
    gone, a missing output is the visible "exited 0 but did not write it" error instead.
    """
    for node in data_out or []:
        name = getattr(node, "file", None)
        if not name:
            continue
        local = os.path.basename(str(name))
        if not local or local in (".", ".."):
            continue
        path = os.path.join(work_dir, local)
        if os.path.isfile(path):
            try:
                os.unlink(path)
                logger.info("[EXEC] %s: removed pre-existing declared output %s before running",
                            job_id, local)
            except OSError as exc:
                logger.warning("[EXEC] %s: could not remove stale output %s (%s)",
                               job_id, local, exc)


def _kill_group(proc, container: Optional[str] = None) -> None:
    """SIGKILL the process group and reap it, tolerating a process that already exited.

    The reap is not optional. Killing without waiting leaves a zombie for every timeout, and
    an agent is a long-lived process running many jobs, so they accumulate until it runs out
    of PIDs. The wait is bounded because the group has just been SIGKILLed: if it somehow
    still has not exited we would rather leak one entry than block the executor thread for
    the rest of the run.
    """
    if container:
        try:
            subprocess.run(["docker", "kill", container], stdout=subprocess.DEVNULL,
                           stderr=subprocess.DEVNULL, timeout=15, check=False)
        except (OSError, subprocess.TimeoutExpired) as exc:
            logger.warning("[EXEC] could not kill container %s: %s", container, exc)
    if proc is None:
        return
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except (ProcessLookupError, PermissionError, OSError):
        try:
            proc.kill()
        except OSError:
            pass
    try:
        proc.wait(timeout=10)
    except (subprocess.TimeoutExpired, OSError, ValueError):
        logger.warning("[EXEC] pid %s did not reap after SIGKILL", getattr(proc, "pid", "?"))


def describe(cmd: List[str]) -> str:
    """A copy-pasteable rendering of a command, for logs."""
    return " ".join(shlex.quote(c) for c in cmd)
