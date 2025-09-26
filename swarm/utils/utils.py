import hashlib
import string
import random
import time
from contextlib import contextmanager

from swarm.models.capacities import Capacities
from swarm.models.job import Job


def generate_id() -> str:
    return hashlib.sha256(''.join(random.choices(string.ascii_lowercase, k=8)).encode()).hexdigest()[:8]

@contextmanager
def timed(logger, section: str, **fields):
    """
    Usage:
        with self._timed("proposal.loop", job_id=p.job_id, p_id=p.p_id):
            ...code...
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt_ms = (time.perf_counter() - t0) * 1000.0
        meta = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
        logger.info(f"[TIMING] section={section} duration_ms={dt_ms:.3f} {meta}")


def job_capacities(jobs: list[Job]):
    allocated_caps = Capacities()
    for t in jobs:
        allocated_caps += t.get_capacities()
    return allocated_caps


def normalize_host(host: str, port: int = None) -> str:
    # Always dial IPv4 loopback if given localhost/::1 to avoid cache split
    host = "127.0.0.1" if host in ("localhost", "::1") else host
    if port is not None:
        return f"{host}:{int(port)}"
    return host