import hashlib
import string
import random
import time
from contextlib import contextmanager


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