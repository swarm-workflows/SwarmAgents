# MIT License
#
# Copyright (c) 2024 swarm-workflows

# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.

# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# Author: Komal Thareja(kthare10@renci.org)
"""Run instrumentation shared by the transport, the consensus engines and the agents (P0-4).

Everything here is written from many threads and read once at teardown, so each container
owns a lock and hands back a plain dict. `metrics.json` and the node_exporter textfile
export are both rendered from those dicts, which is what keeps the two from drifting.

The one metric that did not exist before this module is **context age at decision time**:
how old the coordinator's view of a child group was at the instant it delegated. It is the
only quantity that can test the claim that faster finalization leaves a coordinator deciding
on younger context, and no pre-existing counter can be made to yield it.
"""
from __future__ import annotations

import math
import os
import random
import threading
import time
from collections import deque
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple


# --------------------------------------------------------------------------- distributions

class RunningStats:
    """Exact count/sum/min/max plus quantiles from a bounded **uniform** sample.

    The sample is a reservoir (Algorithm R), not the first or last N values. A head-truncated
    sample would report the warm-up as the whole run and a tail-truncated one would report only
    the converged regime; either reads as a real distribution and neither is one. `sampled` and
    `observations` differ exactly when the cap bound, so a reader can tell.
    """

    __slots__ = ("_lock", "_cap", "_sample", "_seen", "_rng",
                 "count", "total", "min", "max")

    def __init__(self, sample_cap: int = 20000, seed: Optional[int] = None):
        self._lock = threading.Lock()
        self._cap = max(1, int(sample_cap))
        self._sample: List[float] = []
        self._seen = 0
        self._rng = random.Random(seed)
        self.count = 0
        self.total = 0.0
        self.min: Optional[float] = None
        self.max: Optional[float] = None

    def add(self, value: float) -> None:
        try:
            v = float(value)
        except (TypeError, ValueError):
            return
        if math.isnan(v) or math.isinf(v):
            return
        with self._lock:
            self.count += 1
            self.total += v
            if self.min is None or v < self.min:
                self.min = v
            if self.max is None or v > self.max:
                self.max = v
            self._seen += 1
            if len(self._sample) < self._cap:
                self._sample.append(v)
            else:
                j = self._rng.randrange(self._seen)
                if j < self._cap:
                    self._sample[j] = v

    def summary(self, prefix: str = "") -> Dict[str, Any]:
        with self._lock:
            n = self.count
            sample = sorted(self._sample)
            total, lo, hi = self.total, self.min, self.max
        out: Dict[str, Any] = {
            f"{prefix}n": n,
            f"{prefix}mean": round(total / n, 6) if n else None,
            f"{prefix}min": round(lo, 6) if lo is not None else None,
            f"{prefix}max": round(hi, 6) if hi is not None else None,
            f"{prefix}sampled": len(sample),
        }
        for q in (0.50, 0.95, 0.99):
            out[f"{prefix}p{int(q * 100)}"] = _quantile(sample, q)
        return out


def _quantile(sorted_values: Sequence[float], q: float) -> Optional[float]:
    """Linear-interpolated quantile of an already-sorted sequence."""
    n = len(sorted_values)
    if n == 0:
        return None
    if n == 1:
        return round(float(sorted_values[0]), 6)
    pos = q * (n - 1)
    lo = int(math.floor(pos))
    hi = min(lo + 1, n - 1)
    frac = pos - lo
    return round(float(sorted_values[lo]) * (1 - frac) + float(sorted_values[hi]) * frac, 6)


# ----------------------------------------------------------------------- message accounting

class MessageCounters:
    """Consensus messages and serialized bytes, by direction and message type.

    `bytes` is the protobuf `ByteSize()` of the request — the payload actually put on the
    wire, excluding HTTP/2 framing and gRPC's 5-byte length prefix. Report it as "protocol
    bytes", not as link bytes; the framing overhead is roughly constant per message, so the
    *count* column carries it.

    Drops are counted separately and are **not** in `sent`: a fan-out send abandoned because
    the broadcast pool was saturated never reached the network, and folding it into the sent
    total would understate a saturated coordinator's message cost per delivered message.
    """

    __slots__ = ("_lock", "_sent", "_recv", "_dropped")

    def __init__(self):
        self._lock = threading.Lock()
        self._sent: Dict[str, List[int]] = {}      # type -> [msgs, bytes]
        self._recv: Dict[str, List[int]] = {}
        self._dropped: Dict[str, int] = {}

    def record_sent(self, msg_type: Any, nbytes: int = 0) -> None:
        self._bump(self._sent, str(msg_type), int(nbytes or 0))

    def record_received(self, msg_type: Any, nbytes: int = 0) -> None:
        self._bump(self._recv, str(msg_type), int(nbytes or 0))

    def record_dropped(self, msg_type: Any) -> None:
        key = str(msg_type)
        with self._lock:
            self._dropped[key] = self._dropped.get(key, 0) + 1

    def _bump(self, table: Dict[str, List[int]], key: str, nbytes: int) -> None:
        # `d[k] = d.get(k, 0) + 1` is not atomic across threads even under the GIL, and the
        # broadcast pool bumps these from 16 workers at once, so the lock is load-bearing:
        # without it the message counts silently undercount exactly in the saturated runs
        # the message-cost figure exists to show.
        with self._lock:
            entry = table.get(key)
            if entry is None:
                table[key] = [1, nbytes]
            else:
                entry[0] += 1
                entry[1] += nbytes

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            sent = {k: {"msgs": v[0], "bytes": v[1]} for k, v in self._sent.items()}
            recv = {k: {"msgs": v[0], "bytes": v[1]} for k, v in self._recv.items()}
            dropped = dict(self._dropped)
        return {
            "sent_msgs": sum(v["msgs"] for v in sent.values()),
            "sent_bytes": sum(v["bytes"] for v in sent.values()),
            "recv_msgs": sum(v["msgs"] for v in recv.values()),
            "recv_bytes": sum(v["bytes"] for v in recv.values()),
            "dropped_msgs": sum(dropped.values()),
            "sent_by_type": sent,
            "recv_by_type": recv,
            "dropped_by_type": dropped,
        }


# ------------------------------------------------------------------ context age at decision

#: Returned when a candidate group has no observation behind it at all (no live child, or a
#: child record that never carried a timestamp). Counted, never folded into the age stats:
#: an unknown age is not a large age, and substituting one would put the staleness figure's
#: tail exactly where the dead groups are.
UNKNOWN = None


@dataclass
class ContextAge:
    """How old the view behind a delegation decision was, in seconds, at the decision."""
    mean: Optional[float] = None
    min: Optional[float] = None
    max: Optional[float] = None
    chosen: Optional[float] = None      # mean over the groups actually delegated to
    oldest_max: Optional[float] = None  # worst *child* observation in any candidate group
    unknown: int = 0                    # candidate groups with no observation behind them
    skewed: int = 0                     # ages that came back negative and were clamped to 0

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)


def context_age(snapshots: Dict[int, Any], selected: Sequence[int],
                now: Optional[float] = None) -> ContextAge:
    """Age of *snapshots* at wall-clock *now*, from their `observed_at` timestamps.

    `now` must be the moment the decision was made, not the moment the snapshots were built.
    Under `delegation.policy: llm` those are seconds apart, and that gap is the effect the
    inference budget item (P0-3) is about — it would vanish if the age were frozen at build.

    Clock skew: `observed_at` is stamped on the *child's* clock and read on the coordinator's.
    A negative age is therefore skew, not a measurement, and is clamped to 0 and counted in
    `skewed` rather than dropped — a run whose `skewed` count is not ~0 has an NTP problem and
    its age distribution should not be reported.
    """
    ts = time.time() if now is None else float(now)
    ages: List[float] = []
    oldest: List[float] = []
    chosen: List[float] = []
    unknown = 0
    skewed = 0
    chosen_set = set(selected or ())

    for group, snap in (snapshots or {}).items():
        observed = getattr(snap, "observed_at", None)
        if not observed:
            unknown += 1
            continue
        age = ts - float(observed)
        if age < 0:
            skewed += 1
            age = 0.0
        ages.append(age)
        if group in chosen_set:
            chosen.append(age)
        oldest_observed = getattr(snap, "oldest_observed_at", None) or observed
        oldest_age = ts - float(oldest_observed)
        oldest.append(max(0.0, oldest_age))

    if not ages:
        return ContextAge(unknown=unknown, skewed=skewed)
    return ContextAge(
        mean=round(sum(ages) / len(ages), 6),
        min=round(min(ages), 6),
        max=round(max(ages), 6),
        chosen=round(sum(chosen) / len(chosen), 6) if chosen else None,
        oldest_max=round(max(oldest), 6) if oldest else None,
        unknown=unknown,
        skewed=skewed,
    )


@dataclass
class DecisionRecord:
    """One delegation decision, with everything the oracle (P1-1) needs to label it offline."""
    ts: float
    job_id: str
    job_type: Optional[str]
    policy: str                      # what actually decided: bandit | llm | random | all
    n_candidates: int
    candidates: List[int]
    selected: List[int]
    decide_s: float
    age: ContextAge = field(default_factory=ContextAge)

    def as_dict(self) -> Dict[str, Any]:
        row = {
            "ts": round(self.ts, 6),
            "job_id": self.job_id,
            "job_type": self.job_type,
            "policy": self.policy,
            "n_candidates": self.n_candidates,
            "candidates": list(self.candidates),
            "selected": list(self.selected),
            "decide_s": round(self.decide_s, 6),
        }
        row.update({f"ctx_age_{k}": v for k, v in self.age.as_dict().items()})
        return row


class DecisionLog:
    """Per-decision records (bounded) plus aggregates that stay exact when the bound binds.

    The bound is a ring: when it binds the *oldest* records go. A delegation run's interesting
    regime is the converged one, and dropping the tail instead would leave a log of nothing but
    cold-start exploration.
    """

    def __init__(self, max_records: int = 20000):
        self._lock = threading.Lock()
        self._records: deque = deque(maxlen=max(1, int(max_records)))
        self._added = 0
        self._by_policy: Dict[str, int] = {}
        self._age = RunningStats()
        self._age_chosen = RunningStats()
        self._decide = RunningStats()
        self._unknown = 0
        self._skewed = 0

    def add(self, record: DecisionRecord) -> None:
        with self._lock:
            self._records.append(record)
            self._added += 1
            self._by_policy[record.policy] = self._by_policy.get(record.policy, 0) + 1
            self._unknown += record.age.unknown
            self._skewed += record.age.skewed
        # RunningStats take their own lock; keep them out of the one above.
        if record.age.mean is not None:
            self._age.add(record.age.mean)
        if record.age.chosen is not None:
            self._age_chosen.add(record.age.chosen)
        self._decide.add(record.decide_s)

    def records(self) -> List[Dict[str, Any]]:
        with self._lock:
            return [r.as_dict() for r in self._records]

    def summary(self) -> Dict[str, Any]:
        with self._lock:
            added = self._added
            kept = len(self._records)
            by_policy = dict(self._by_policy)
            unknown, skewed = self._unknown, self._skewed
        out: Dict[str, Any] = {
            "decisions": added,
            "records_kept": kept,
            "records_dropped": max(0, added - kept),
            "by_policy": by_policy,
            "ctx_unknown_groups": unknown,
            "ctx_skewed_ages": skewed,
        }
        out.update(self._age.summary("ctx_age_"))
        out.update(self._age_chosen.summary("ctx_age_chosen_"))
        out.update(self._decide.summary("decide_s_"))
        return out


# ------------------------------------------------------------------------ LLM call accounting

class LlmUsage:
    """Calls, latency and tokens for one LLM call site (bidding or delegation).

    Failures are counted **and timed**. A timeout burned the whole `llm.timeout_seconds`
    before it raised, so charging only the successes would make inference look cheapest in
    precisely the runs that are drowning in fallbacks — the same accounting trap the
    delegation stats already avoid.
    """

    def __init__(self, site: str):
        self.site = site
        self._lock = threading.Lock()
        self.calls = 0
        self.failures = 0
        self.input_tokens = 0
        self.output_tokens = 0
        self.requests = 0          # provider requests, which retries make > calls
        self._latency = RunningStats()

    def record(self, elapsed_s: float, usage: Any = None, failed: bool = False) -> None:
        with self._lock:
            self.calls += 1
            if failed:
                self.failures += 1
            if usage is not None:
                self.input_tokens += int(getattr(usage, "input_tokens", 0) or 0)
                self.output_tokens += int(getattr(usage, "output_tokens", 0) or 0)
                self.requests += int(getattr(usage, "requests", 0) or 0)
        self._latency.add(elapsed_s)

    def snapshot(self) -> Dict[str, Any]:
        with self._lock:
            out = {
                "site": self.site,
                "calls": self.calls,
                "failures": self.failures,
                "input_tokens": self.input_tokens,
                "output_tokens": self.output_tokens,
                "provider_requests": self.requests,
            }
        out.update(self._latency.summary("latency_"))
        return out


def extract_usage(result: Any) -> Any:
    """Best-effort token usage off a pydantic-ai run result.

    `usage()` is a method on the result object and its field names have moved between
    pydantic-ai majors, so this never raises and never blocks a bid on an accounting detail.
    """
    try:
        usage = result.usage() if callable(getattr(result, "usage", None)) else None
    except Exception:
        return None
    if usage is None:
        return None
    # Older pydantic-ai spelled these request_tokens/response_tokens.
    if not hasattr(usage, "input_tokens") and hasattr(usage, "request_tokens"):
        class _Shim:
            input_tokens = getattr(usage, "request_tokens", 0)
            output_tokens = getattr(usage, "response_tokens", 0)
            requests = getattr(usage, "requests", 0)
        return _Shim()
    return usage


# ------------------------------------------------------------- node_exporter textfile export

def render_prom(samples: Iterable[Tuple[str, Dict[str, Any], Any]],
                help_text: Optional[Dict[str, str]] = None) -> str:
    """Render (name, labels, value) triples as Prometheus text format.

    Two rules the Prometheus text format enforces and a naive writer breaks:

    * Only finite numbers are emitted. node_exporter rejects the *entire file* on one
      malformed line, so a `None` quantile from an empty distribution must be dropped rather
      than written as NaN and taking every other metric in the file down with it.
    * Every sample of a metric family is written contiguously, after that family's one TYPE
      line. Samples arrive here interleaved (one message type at a time, each carrying both
      `_msgs` and `_bytes`), so they are grouped by name first; a second TYPE line for a
      family already seen is a parse error, not a warning.
    """
    help_text = help_text or {}
    families: Dict[str, List[str]] = {}
    for name, labels, value in samples:
        if value is None:
            continue
        try:
            number = float(value)
        except (TypeError, ValueError):
            continue
        if math.isnan(number) or math.isinf(number):
            continue
        if labels:
            rendered = ",".join(f'{k}="{_escape(str(v))}"' for k, v in sorted(labels.items()))
            sample = f"{name}{{{rendered}}} {number!r}"
        else:
            sample = f"{name} {number!r}"
        families.setdefault(name, []).append(sample)

    lines: List[str] = []
    for name, rows in families.items():
        if name in help_text:
            lines.append(f"# HELP {name} {help_text[name]}")
        lines.append(f"# TYPE {name} gauge")
        lines.extend(rows)
    return "\n".join(lines) + ("\n" if lines else "")


def _escape(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def write_textfile(path: str, text: str) -> None:
    """Write *path* atomically, the way node_exporter's textfile collector requires.

    The temporary file must live in the **same directory** as the target: node_exporter scans
    the directory and `os.replace` is only atomic within a filesystem, so a tmp file in /tmp
    would give a cross-device rename and a collector that can read a half-written file.
    """
    directory = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(directory, exist_ok=True)
    tmp = os.path.join(directory, f".{os.path.basename(path)}.{os.getpid()}.tmp")
    try:
        with open(tmp, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


#: Sub-dicts under a key with this suffix are keyed by a *value* (a message type), not by a
#: metric name. Their keys become a `type` label instead of part of the metric name, because
#: one metric name per message type is a cardinality explosion in the name space Prometheus
#: cannot aggregate over — `sum by (type)` needs the type to be a label.
_LABELLED_SUFFIX = "_by_type"


def flatten_for_prom(prefix: str, payload: Any, labels: Optional[Dict[str, Any]] = None):
    """Yield (name, labels, value) triples for the numeric leaves of a metrics dict.

    Strings are skipped rather than rendered: `consensus.protocol` is an identity, and the
    only honest Prometheus encoding of an identity is a label on another series, not a
    gauge. It travels in `metrics.json`, which is where the analysis reads it from.
    """
    labels = dict(labels or {})
    if isinstance(payload, dict):
        for key, value in payload.items():
            if isinstance(value, dict) and key.endswith(_LABELLED_SUFFIX):
                # `{"sent_by_type": {"PREPARE": {"msgs": 3}}}` ->
                # swarm_..._sent_by_type_msgs{type="PREPARE"} 3
                for type_name, sub in value.items():
                    for item in flatten_for_prom(f"{prefix}_{key}", sub,
                                                 {**labels, "type": type_name}):
                        yield item
            elif isinstance(value, dict):
                for item in flatten_for_prom(f"{prefix}_{key}", value, labels):
                    yield item
            elif isinstance(value, bool):
                yield (f"{prefix}_{key}", labels, 1 if value else 0)
            elif isinstance(value, (int, float)):
                yield (f"{prefix}_{key}", labels, value)
    elif isinstance(payload, (int, float)) and not isinstance(payload, bool):
        yield (prefix, labels, payload)
