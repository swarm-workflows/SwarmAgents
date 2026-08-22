"""Every reference to the chaos test plan must resolve to a section that exists.

The plan's sections were renumbered on 2026-08-22 (they had grown by insertion into two colliding
schemes). The renumbering itself was scripted and verified, but the references to it were rewritten
by matching two *forms* — `§4d.2` and `test plan 4d.2` — and three live ones were written in other
forms and silently left pointing at labels that no longer existed:

    `CHAOS_JUNGLE_LLM_TEST_PLAN.md` 4d.2      (bare label after the filename)
    See the test plan, section 2.1b.          ("section", not "§")
    (see the test plan, 4f.2)                 (comma, no keyword)

A dangling section reference is the cheapest possible defect to introduce and one of the more
annoying to be on the receiving end of, so it is worth a test rather than a careful reading. This
parses the labels the plan actually defines and checks every reference in the repo against them.
"""
from __future__ import annotations

import os
import re

import pytest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PLAN = os.path.join(REPO_ROOT, "CHAOS_JUNGLE_LLM_TEST_PLAN.md")

# Where a bare "§N" means "the test plan's section N". Excluded on purpose:
#   docs/            — those documents number their own sections, so §7.4 there is theirs
#   cc-usage-log.md  — an append-only dated record; its old labels are annotated in place and
#                      resolved by the plan's old->new table, and rewriting history is wrong
SCANNED = ["CHAOS_JUNGLE_LLM_TEST_PLAN.md", "SWARMAGENTS_FINDINGS.md", "CHAOS_JUNGLE_FINDINGS.md",
           "scenarios", "tests", "swarm/agents/llm"]

# `§7.2`, `test plan 7.2`, `test plan, §7.2`, `TEST_PLAN.md 7.2`, `section 7.2`.
# The label pattern deliberately admits the OLD lettered forms (4d.2, 2.1b, 4.0b) — a detector
# that only understood the new purely-numeric labels would be blind to exactly the references the
# renumbering left behind, which is the thing this file exists to catch.
REF = re.compile(r"(?:§\s?|(?:test plan|TEST_PLAN\.md)[`,]?\s+§?|section\s+)"
                 # The tail guard rejects a following alphanumeric, and a "." only when a longer
                 # label follows it. `(?![0-9a-z.])` would reject a reference ENDING A SENTENCE
                 # ("§7.2.") — the same trap that first made the renumbering miss four references.
                 r"(\d[0-9a-z]*(?:\.[0-9a-z]+)*)(?![0-9a-z])(?!\.[0-9a-z])")


def plan_labels() -> set[str]:
    """Every section label the plan defines, from its headings, outside code fences."""
    labels, fence = set(), False
    for line in open(PLAN).read().splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
            continue
        if fence:
            continue
        m = re.match(r"^#{2,6} (\d+(?:\.\d+)*)\.? ", line)
        if m:
            labels.add(m.group(1))
    return labels


def scanned_files() -> list[str]:
    """Everything scanned, except this file — it quotes the dangling forms on purpose, as the
    regression cases for the detector, so including it would make the check permanently red."""
    out = []
    for entry in SCANNED:
        path = os.path.join(REPO_ROOT, entry)
        if os.path.isfile(path):
            out.append(path)
        for base, _, names in os.walk(path):
            out += [os.path.join(base, n) for n in names if n.endswith((".md", ".py"))]
    return sorted(set(out) - {os.path.abspath(__file__)})


def references(path: str) -> list[tuple[int, str]]:
    """(line number, label) for each reference, skipping the plan's own old->new mapping table."""
    text = open(path, errors="ignore").read()
    if path == PLAN:
        # The table exists to list the labels that no longer resolve.
        text = text.partition("<details>")[0] + text.partition("</details>")[2]
    hits = []
    for i, line in enumerate(text.splitlines(), 1):
        hits += [(i, m.group(1)) for m in REF.finditer(line)]
    return hits


def test_the_plan_defines_a_contiguous_ascending_section_numbering():
    """The point of the renumbering: labels run 1..N in order, with no letters and no gaps at the
    top level. A gap means a section was dropped or a label was mistyped."""
    labels = plan_labels()
    tops = sorted({int(l.split(".")[0]) for l in labels})
    assert tops == list(range(1, max(tops) + 1)), f"gap in top-level numbering: {tops}"
    assert not [l for l in labels if re.search(r"[a-z]", l)]


def test_every_reference_to_the_test_plan_resolves():
    labels = plan_labels()
    dangling = []
    for path in scanned_files():
        for line_no, label in references(path):
            if label not in labels:
                rel = os.path.relpath(path, REPO_ROOT)
                dangling.append(f"{rel}:{line_no} -> §{label}")
    assert not dangling, "references to sections the plan does not define:\n  " + \
                         "\n  ".join(dangling)


@pytest.mark.parametrize("form", [
    "see `CHAOS_JUNGLE_LLM_TEST_PLAN.md` 4d.2.",     # bare label after the filename
    "See the test plan, section 2.1b.",              # "section", not "§"
    "(see the test plan, 4f.2)",                     # comma, no keyword
    "measured in the test plan §4.0b",               # the form that WAS covered
])
def test_the_detector_catches_the_forms_that_slipped_through(form):
    """Pins the parser against the exact strings that were missed. A checker that only recognised
    `§label` would pass this file while the repo still dangled."""
    found = [m.group(1) for m in REF.finditer(form)]
    assert found, f"reference form not recognised: {form!r}"
    # None of these old labels exist any more, so each must be reported as dangling.
    assert all(label not in plan_labels() for label in found)
