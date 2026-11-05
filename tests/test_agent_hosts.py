"""Tests for the hosts-file builder (make_agent_hosts.py).

Two properties, both of which were got wrong by hand:

  * a node returning from a rebuild has a new host key, and plain ssh under BatchMode reports
    that as a failure indistinguishable from a timeout — AMST was written off as down for a day
    that way while all seven nodes were up;
  * agents are assigned to hosts in the order this file lists them, and agent ids map to sites
    in contiguous blocks, so the obvious numeric ordering clusters each hierarchical group at
    one site. Measured on the live slice: 82% of adjacent pairs same-site sequentially, 0%
    interleaved.
"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import make_agent_hosts as mah  # noqa: E402


class TestSiteInference:
    def test_a_site_is_the_slash_24(self):
        assert mah.site_of("10.145.7.2") == "10.145.7.0"
        assert mah.site_of("10.145.7.254") == "10.145.7.0"
        assert mah.site_of("10.140.130.8") != mah.site_of("10.145.7.2")

    def test_an_unresolvable_host_is_not_silently_grouped(self):
        assert mah.site_of(None) == "unknown"
        assert mah.site_of("not-an-address") == "unknown"


class TestInterleave:
    def test_balanced_sites_never_place_neighbours_together(self):
        by_site = {f"site{s}": [f"agent-{s}-{i}" for i in range(4)] for s in range(5)}
        order = mah.interleave(by_site)
        assert len(order) == 20
        sites = [h.rsplit("-", 1)[0] for h in order]
        assert all(a != b for a, b in zip(sites, sites[1:])), sites

    def test_every_host_is_emitted_exactly_once(self):
        by_site = {"a": ["h1", "h2", "h3"], "b": ["h4"], "c": ["h5", "h6"]}
        order = mah.interleave(by_site)
        assert sorted(order) == ["h1", "h2", "h3", "h4", "h5", "h6"]

    def test_a_dominant_site_is_drained_first_not_last(self):
        """Largest-first is what keeps neighbours apart for as long as possible; taking the
        big site last would leave a solid tail of it."""
        by_site = {"big": [f"b{i}" for i in range(6)], "small": ["s1", "s2"]}
        order = mah.interleave(by_site)
        head = [h[0] for h in order[:4]]
        assert head.count("s") == 2, f"small site should be spread through the head: {order}"

    def test_a_single_site_still_returns_everything(self):
        by_site = {"only": ["h1", "h2", "h3"]}
        assert mah.interleave(by_site) == ["h1", "h2", "h3"]


class TestOrderingsDiffer:
    def test_sequential_clusters_what_interleaved_spreads(self):
        """The regression this file exists for: both orderings are valid, and which one a run
        used has to be a deliberate choice, because they measure different networks."""
        by_site = {"s1": ["a1", "a2", "a3"], "s2": ["b1", "b2", "b3"]}
        sequential = [h for hosts in by_site.values() for h in hosts]
        interleaved = mah.interleave(by_site)

        def same_site_pairs(order):
            sites = [h[0] for h in order]
            return sum(1 for a, b in zip(sites, sites[1:]) if a == b)

        assert same_site_pairs(sequential) > same_site_pairs(interleaved)
        assert same_site_pairs(interleaved) == 0


class TestCli:
    def test_asking_for_more_hosts_than_exist_fails_loudly(self, tmp_path, monkeypatch, capsys):
        """Sizing a run above the fleet must stop here with a readable reason, not at agent
        start-up several minutes later."""
        monkeypatch.setattr(mah, "probe", lambda h: "UP" if h.endswith(("1", "2")) else "DOWN")
        monkeypatch.setattr(mah, "resolve", lambda h: "10.0.0.1")
        monkeypatch.setattr(sys, "argv", [
            "make_agent_hosts.py", "--last", "9", "--count", "5",
            "--out", str(tmp_path / "hosts.txt")])
        assert mah.main() == 1
        assert "only 2 are reachable" in capsys.readouterr().err

    def test_it_writes_hosts_and_parallel_site_labels(self, tmp_path, monkeypatch):
        ips = {"agent-1": "10.1.1.5", "agent-2": "10.2.2.5", "agent-3": "10.1.1.6"}
        monkeypatch.setattr(mah, "probe", lambda h: "UP" if h in ips else "DOWN")
        monkeypatch.setattr(mah, "resolve", lambda h: ips.get(h))
        hosts_file, sites_file = tmp_path / "h.txt", tmp_path / "s.txt"
        monkeypatch.setattr(sys, "argv", [
            "make_agent_hosts.py", "--last", "3",
            "--out", str(hosts_file), "--sites-out", str(sites_file)])
        assert mah.main() == 0
        hosts = hosts_file.read_text().split()
        sites = sites_file.read_text().split()
        assert sorted(hosts) == ["agent-1", "agent-2", "agent-3"]
        # Line i of the sites file must be the site of line i of the hosts file: run_test.py
        # pairs them positionally, so a mismatch mislabels every agent's locality.
        assert sites == [mah.site_of(ips[h]) for h in hosts]

    def test_a_key_changed_host_is_reported_and_excluded(self, tmp_path, monkeypatch, capsys):
        """KEY is reachable-but-unverified: too risky to run on unattended, too important to
        report as down (that is how a whole site got written off)."""
        monkeypatch.setattr(mah, "probe",
                            lambda h: {"agent-1": "UP", "agent-2": "KEY"}.get(h, "DOWN"))
        monkeypatch.setattr(mah, "resolve", lambda h: "10.0.0.1")
        out = tmp_path / "h.txt"
        monkeypatch.setattr(sys, "argv", ["make_agent_hosts.py", "--last", "2", "--out", str(out)])
        assert mah.main() == 0
        err = capsys.readouterr().err
        assert "key-changed: 1" in err
        assert "agent-2" in err
        assert out.read_text().split() == ["agent-1"]
