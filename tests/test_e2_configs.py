"""campaigns/make_e2_configs.py: every E2 arm differs from its engine's base only in its own keys."""
import os
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)
sys.path.insert(0, os.path.join(REPO, "campaigns"))

import make_e2_configs as m  # noqa: E402
from swarm.utils.yaml_strict import safe_load  # noqa: E402


def _flat(d, p=""):
    out = {}
    for k, v in d.items():
        if isinstance(v, dict) and k != "failure_simulation":
            out.update(_flat(v, p + k + "."))
        else:
            out[p + k] = v
    return out


@pytest.mark.parametrize("engine", sorted(m.ENGINES))
@pytest.mark.parametrize("arm", sorted(m.ARMS))
def test_an_arm_changes_only_its_keys(engine, arm):
    base = _flat(m.build(engine, "linucb-shared"))
    cfg = _flat(m.build(engine, arm))
    declared = {k for k in m.ARMS[arm] if not k.startswith("_")}
    diff = {k for k in set(base) | set(cfg) if base.get(k) != cfg.get(k)}
    if m.ARMS[arm].get("_flip"):
        assert cfg["mab.failure_simulation"]["phases"][0]["after_s"] == m.FLIP_AFTER_S
        diff.discard("mab.failure_simulation")
    assert diff <= declared and declared - {k for k in declared if base.get(k) == cfg.get(k)} == diff


def test_engines_differ_only_in_protocol():
    for arm in m.ARMS:
        a, b = _flat(m.build("pbft", arm)), _flat(m.build("snow", arm))
        assert {k for k in set(a) | set(b) if a.get(k) != b.get(k)} == {"consensus.protocol"}


def test_the_validated_pilot_config_is_the_pbft_base():
    pilot = safe_load(open(os.path.join(REPO, "campaigns", "config_e2_pbft_linucb_shared.yml")))
    assert m.build("pbft", "linucb-shared") == pilot


def test_an_unknown_key_is_refused():
    with pytest.raises(SystemExit):
        m._set({"mab": {}}, "mab.no_such_key", 1)


def test_written_files_match_the_generator():
    for engine in m.ENGINES:
        for arm in m.ARMS:
            path = os.path.join(REPO, "campaigns", "e2", f"config_e2_{engine}_{arm}.yml")
            assert safe_load(open(path)) == m.build(engine, arm), path
