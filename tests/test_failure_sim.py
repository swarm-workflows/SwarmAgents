# Tests for MAB failure-simulation rate resolution (Scenario A/B evaluation)
import pytest

from swarm.agents.resource_agent import ResourceAgent


class TestResolveFailureRate:
    def test_flat_per_agent_rate_wins(self):
        r = ResourceAgent._resolve_failure_rate(
            "3", "cpu_bound_short_dtn_light", {"3": 0.5}, {}, 0.1)
        assert r == 0.5

    def test_per_agent_per_type_dict(self):
        pa = {"3": {"cpu_bound_short_dtn_light": 0.8, "default": 0.05}}
        assert ResourceAgent._resolve_failure_rate(
            "3", "cpu_bound_short_dtn_light", pa, {}, 0.1) == 0.8
        assert ResourceAgent._resolve_failure_rate(
            "3", "ram_bound_long_dtn_heavy", pa, {}, 0.1) == 0.05

    def test_dict_without_default_falls_through(self):
        pa = {"3": {"cpu_bound_short_dtn_light": 0.8}}
        assert ResourceAgent._resolve_failure_rate(
            "3", "x", pa, {"x": 0.3}, 0.1) == 0.3
        assert ResourceAgent._resolve_failure_rate(
            "3", "y", pa, {}, 0.1) == 0.1

    def test_unknown_agent_uses_type_then_base(self):
        assert ResourceAgent._resolve_failure_rate(
            "7", "x", {"3": 0.9}, {"x": 0.3}, 0.1) == 0.3
        assert ResourceAgent._resolve_failure_rate(
            "7", "y", {"3": 0.9}, {}, 0.1) == 0.1


class TestSelectFailurePhase:
    BASE_PA = {"1": 0.8}
    BASE_PT = {"compute": 0.2}
    PHASES = [
        {"after_s": 100, "per_agent_failure_rates": {"1": 0.1}},
        {"after_s": 200, "per_agent_failure_rates": {"1": 0.5},
         "per_job_type_failure_rates": {"compute": 0.9}},
    ]

    def test_before_any_phase_uses_base(self):
        pa, pt, idx = ResourceAgent._select_failure_phase(
            50, self.PHASES, self.BASE_PA, self.BASE_PT)
        assert (pa, pt, idx) == (self.BASE_PA, self.BASE_PT, -1)

    def test_first_phase_active(self):
        pa, pt, idx = ResourceAgent._select_failure_phase(
            150, self.PHASES, self.BASE_PA, self.BASE_PT)
        assert pa == {"1": 0.1}
        assert pt == self.BASE_PT  # phase 0 doesn't override per-type
        assert idx == 0

    def test_last_matching_phase_wins(self):
        pa, pt, idx = ResourceAgent._select_failure_phase(
            250, self.PHASES, self.BASE_PA, self.BASE_PT)
        assert pa == {"1": 0.5}
        assert pt == {"compute": 0.9}
        assert idx == 1

    def test_no_phases(self):
        pa, pt, idx = ResourceAgent._select_failure_phase(
            1e9, [], self.BASE_PA, self.BASE_PT)
        assert (pa, pt, idx) == (self.BASE_PA, self.BASE_PT, -1)


class TestLivenessGate:
    def test_live_groups_from_children(self):
        class Child:
            def __init__(self, group):
                self.group = group

        class Stub:
            class children:
                @staticmethod
                def values():
                    return [Child(0), Child(0), Child(3), Child(None)]

        live = ResourceAgent._get_live_child_groups(Stub())
        assert live == {0, 3}  # None group folds into 0; group with no
        # heartbeats (e.g. 1, 2, 4) is absent — gated out of delegation


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
