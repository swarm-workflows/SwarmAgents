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


# Headings that carry no section number by design: the parts, and the contents list.
STRUCTURAL = re.compile(r"^(?:Part [IVX]+\b|Contents$)")


def plan_headings() -> list[tuple[int, str]]:
    """(level, title) for EVERY heading outside a code fence, in document order.

    Nothing is filtered here. A collector that skips what its label pattern does not recognise
    cannot report a heading that is unlabelled (`## Appendix`), labelled in the old scheme
    (`## 4b.`), or labelled in a way the pattern did not anticipate (`## 4B.`, `## 7-2`) — it just
    drops it, and every invariant below then holds over a set that excludes the problem. That is
    the same "check data you already filtered" mistake this file has now made twice, so the
    classification is done in the assertions, not in the collector.
    """
    out, fence = [], False
    for line in open(PLAN).read().splitlines():
        if line.lstrip().startswith("```"):
            fence = not fence
            continue
        if fence:
            continue
        m = re.match(r"^(#{1,6}) (.+?)\s*$", line)
        if m and not m.group(2).startswith("Evaluating Chaos Jungle"):
            out.append((len(m.group(1)), m.group(2)))
    return out


LABEL = re.compile(r"^(\d+(?:\.\d+)*)\.? \S")


def plan_sections() -> list[tuple[int, str]]:
    """(level, label) for content sections — every heading that is not a part or the contents.

    Raises through test_every_content_section_carries_a_wellformed_label if any of them fails to
    parse, so this can only ever return a complete list.
    """
    out = []
    for lvl, title in plan_headings():
        if lvl == 1 or STRUCTURAL.match(title):
            continue
        m = LABEL.match(title)
        if m:
            out.append((lvl, m.group(1)))
    return out


def plan_labels() -> set[str]:
    return {label for _, label in plan_sections()}


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


def test_every_content_section_carries_a_wellformed_label():
    """The check that makes the rest meaningful: every heading under a part must carry a purely
    numeric label. This is what catches the old scheme (`4b`, `2.1b`), a case the pattern did not
    anticipate (`4B.`, `7-2`), and — the one a label-shaped pattern can never catch — a section
    with no number at all, which simply would not have been collected."""
    unlabelled = [(lvl, title) for lvl, title in plan_headings()
                  if lvl > 1 and not STRUCTURAL.match(title) and not LABEL.match(title)]
    assert not unlabelled, ("headings under a part with no well-formed numeric label:\n  "
                            + "\n  ".join(f"{'#' * l} {t}" for l, t in unlabelled))


def test_labels_ascend_in_document_order():
    """The actual claim of "sequential in reading order", and the one a set-based check cannot
    make: §12 must not appear before §7. Sorting the labels before comparing them — which is what
    the first version of this test did — proves only that they sort, which is vacuous."""
    labels = [label for _, label in plan_sections()]
    keyed = [tuple(int(p) for p in l.split(".")) for l in labels]
    out_of_order = [(labels[i - 1], labels[i]) for i in range(1, len(keyed))
                    if keyed[i] <= keyed[i - 1]]
    assert not out_of_order, f"labels not ascending in document order: {out_of_order}"


def test_numbering_is_contiguous_at_every_depth():
    """A gap means a section was dropped or mistyped: §7.1, §7.2, §7.4 is a missing §7.3, and a
    top level of 1..19 missing 13 is a section that fell out of a reorganisation."""
    labels = plan_labels()
    children: dict[tuple, list[int]] = {}
    for label in labels:
        parts = tuple(int(p) for p in label.split("."))
        children.setdefault(parts[:-1], []).append(parts[-1])
    for parent, kids in sorted(children.items()):
        expected = list(range(1, len(kids) + 1))
        got = sorted(kids)
        where = "top level" if not parent else "§" + ".".join(str(p) for p in parent)
        assert got == expected, f"{where}: expected {expected}, got {got}"
        # ...and a subsection cannot exist without the section it belongs to.
        if parent:
            assert ".".join(str(p) for p in parent) in labels, f"{where} is referenced but missing"


def test_heading_depth_matches_label_depth():
    """§7.2 must be a level deeper than §7. A mismatch renders the contents list wrong even when
    every label is correct, which is how the pre-restructure document read as a flat wall."""
    base = min(lvl for lvl, label in plan_sections() if "." not in label)
    wrong = [(lvl, label) for lvl, label in plan_sections()
             if lvl != base + label.count(".")]
    assert not wrong, f"heading level does not match label depth: {wrong}"


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
