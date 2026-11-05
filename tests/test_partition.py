"""T-3: the site partition driver — what it cuts, that it verifies, and that it always heals."""
import json
import os
import sys
from pathlib import Path

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)

import partition  # noqa: E402
from partition import (Partition, apply_script, cell_hosts, chain, heal_script,  # noqa: E402
                       split_by_site)

PAIRS = [("h1", "s1"), ("h2", "s2"), ("h3", "s1"), ("h4", "s3"), ("h5", "s2"), ("h6", "s4")]


def test_the_cell_is_the_first_hosts_of_the_file():
    assert cell_hosts(["a", "b", "c", "d"], ["x", "y", "z", "w"], 5, 2) == \
        [("a", "x"), ("b", "y"), ("c", "z")]
    with pytest.raises(SystemExit):
        cell_hosts(["a"], ["x"], 3, 1)


def test_a_split_never_divides_a_site_and_is_balanced():
    a, b = split_by_site(PAIRS)
    site = dict(PAIRS)
    assert not ({site[h] for h in a} & {site[h] for h in b})
    assert abs(len(a) - len(b)) <= 2 and sorted(a + b) == sorted(h for h, _ in PAIRS)


def test_explicit_side_a_and_its_refusals():
    a, b = split_by_site(PAIRS, ["s1"])
    assert sorted(a) == ["h1", "h3"]
    with pytest.raises(SystemExit):
        split_by_site(PAIRS, ["nope"])
    with pytest.raises(SystemExit):
        split_by_site([("h1", "s1"), ("h2", "s1")])


def test_the_rules_drop_both_directions_in_one_tokened_chain_with_a_deadman():
    script = apply_script("abcd1234", ["10.0.0.2", "10.0.0.3"], deadman_s=300)
    c = chain("abcd1234")
    assert len(c) <= 28
    for ip in ("10.0.0.2", "10.0.0.3"):
        assert f"iptables -A {c} -s {ip} -j DROP" in script
        assert f"iptables -A {c} -d {ip} -j DROP" in script
    assert f"iptables -I INPUT 1 -j {c}" in script and f"iptables -I OUTPUT 1 -j {c}" in script
    assert "systemd-run" in script and "--on-active=300" in script and "swp-abcd1234" in script
    assert "database" not in script


def test_heal_removes_only_its_own_chain_and_cancels_the_timer():
    script = heal_script("abcd1234")
    assert "systemctl stop swp-abcd1234.timer" in script
    assert f"iptables -X {chain('abcd1234')}" in script
    assert "SWP_" in script and script.count("SWP_") == script.count(chain("abcd1234"))


class FakeFleet:
    """Executes the driver's scripts symbolically: tracks which hosts hold the chain and
    answers pings according to it."""

    def __init__(self, addresses, database_ip="10.9.9.9", short_rules=(), leaky=()):
        self.short_rules, self.leaky = set(short_rules), set(leaky)
        self.addr = addresses
        self.ip_host = {ip: h for h, ip in addresses.items()}
        self.blocked = {}            # host -> set of blocked ips
        self.calls = []
        self.db = database_ip

    def __call__(self, host, script):
        self.calls.append((host, script))
        if "iptables -N" in script:
            ips = {w.split()[0] for w in script.split("-d ")[1:]}
            if host not in self.leaky:
                self.blocked[host] = ips
            n = 2 * len(ips) - (2 if host in self.short_rules else 0)
            return 0, f"applied {n}"
        if "iptables -X" in script:
            self.blocked.pop(host, None)
            return 0, "healed"
        if script.startswith("ping"):
            target = script.split()[-5] if False else script.split("ping -n -c 2 -W 1 ")[1].split()[0]
            if target == self.db:
                return 0, "up"
            dst_host = self.ip_host.get(target)
            cut = target in self.blocked.get(host, set()) or \
                self.addr.get(host) in self.blocked.get(dst_host, set())
            return 0, "down" if cut else "up"
        return 1, "?"


def _part(fleet):
    addr = fleet.addr
    return Partition(["h1", "h3"], ["h2", "h4"], addr, fleet.db, duration_s=1,
                     runner=fleet, token="t0k3n")


def test_each_side_blocks_only_the_other_sides_addresses_and_the_cut_verifies():
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4"}
    fleet = FakeFleet(addr)
    p = _part(fleet)
    assert p.apply()
    assert fleet.blocked["h1"] == {"10.0.0.2", "10.0.0.4"}
    assert fleet.blocked["h2"] == {"10.0.0.1", "10.0.0.3"}
    v = p.verify(expect_cut=True)
    assert v["ok"], v
    assert p.heal()
    assert not fleet.blocked
    assert p.verify(expect_cut=False)["ok"]


def test_a_cut_that_did_not_take_is_reported_not_assumed():
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4"}
    fleet = FakeFleet(addr)
    p = _part(fleet)
    # rules never applied: the cross-site ping still answers
    assert not p.verify(expect_cut=True)["ok"]


def test_every_host_must_install_every_rule():
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4"}
    fleet = FakeFleet(addr, short_rules={"h3"})
    p = _part(fleet)
    assert not p.apply()
    assert "h3" in p.record["apply_failed"]


def test_one_leaky_host_fails_verification():
    """One probe per side would miss it: h3 holds no rules, h1 (the old probe host) does."""
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4"}
    fleet = FakeFleet(addr, leaky={"h3", "h4"})
    p = _part(fleet)
    p.apply()
    v = p.verify(expect_cut=True)
    assert not v["ok"] and set(v["bad"]) & {"h3", "h4"}


def test_the_driver_heals_when_stopped_mid_partition(tmp_path, monkeypatch):
    (tmp_path / "hosts").write_text("h1\nh2\nh3\nh4\n")
    (tmp_path / "sites").write_text("s1\ns2\ns1\ns2\n")
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4",
            "database": "10.9.9.9"}
    fleet = FakeFleet({k: v for k, v in addr.items() if k != "database"})
    monkeypatch.setattr(partition, "ssh_run", fleet)
    monkeypatch.setattr(partition.socket, "gethostbyname", lambda h: addr[h])

    def stop(_):
        raise partition._Stop()

    monkeypatch.setattr(partition.time, "sleep", stop)
    out = tmp_path / "p.json"
    rc = partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                         str(tmp_path / "sites"), "--agents", "4", "--duration", "60",
                         "--out", str(out)])
    rec = json.loads(out.read_text())
    assert rec["stopped_early"] is True
    assert rec["healed_at"] and not fleet.blocked, "a stopped driver left the cut in place"
    assert rec["verify_cut"]["ok"] and rec["verify_heal"]["ok"]
    assert rc == 3 and rec["outcome"] == "held_short", "a short partition is not a success"


def test_a_partition_stopped_before_it_applied_is_not_a_success(tmp_path, monkeypatch):
    (tmp_path / "hosts").write_text("h1\nh2\nh3\nh4\n")
    (tmp_path / "sites").write_text("s1\ns2\ns1\ns2\n")
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4",
            "database": "10.9.9.9"}
    fleet = FakeFleet({k: v for k, v in addr.items() if k != "database"})
    monkeypatch.setattr(partition, "ssh_run", fleet)
    monkeypatch.setattr(partition.socket, "gethostbyname", lambda h: addr[h])

    def stop(_):
        raise partition._Stop()

    monkeypatch.setattr(partition.time, "sleep", stop)       # stopped during --delay
    out = tmp_path / "p.json"
    rc = partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                         str(tmp_path / "sites"), "--agents", "4", "--duration", "60",
                         "--delay", "30", "--out", str(out)])
    rec = json.loads(out.read_text())
    assert rc == 3 and rec["outcome"] == "not_applied" and rec["held_s"] == 0.0
    assert not fleet.calls, "nothing was applied, so nothing touched a host"


def test_a_full_partition_exits_zero(tmp_path, monkeypatch):
    (tmp_path / "hosts").write_text("h1\nh2\nh3\nh4\n")
    (tmp_path / "sites").write_text("s1\ns2\ns1\ns2\n")
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4",
            "database": "10.9.9.9"}
    fleet = FakeFleet({k: v for k, v in addr.items() if k != "database"})
    monkeypatch.setattr(partition, "ssh_run", fleet)
    monkeypatch.setattr(partition.socket, "gethostbyname", lambda h: addr[h])
    clock = [1000.0]
    monkeypatch.setattr(partition.time, "time", lambda: clock[0])
    monkeypatch.setattr(partition.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    out = tmp_path / "p.json"
    rc = partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                         str(tmp_path / "sites"), "--agents", "4", "--duration", "60",
                         "--out", str(out)])
    rec = json.loads(out.read_text())
    assert rc == 0 and rec["outcome"] == "ok" and rec["held_s"] >= 60


def test_a_cut_that_would_take_redis_with_it_is_refused(tmp_path, monkeypatch):
    (tmp_path / "hosts").write_text("h1\nh2\n")
    (tmp_path / "sites").write_text("s1\ns2\n")
    monkeypatch.setattr(partition.socket, "gethostbyname",
                        lambda h: {"h1": "10.0.0.1", "h2": "10.0.0.2", "database": "10.0.0.2"}[h])
    with pytest.raises(SystemExit):
        partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                        str(tmp_path / "sites"), "--agents", "2", "--duration", "1",
                        "--out", str(tmp_path / "p.json")])


def test_the_hold_is_measured_from_the_cut_being_in_place_not_from_applying(tmp_path, monkeypatch):
    """A slow apply must not count toward the hold: stopped 10 s short of a 60 s hold after a
    20 s apply, the partition is short, even though 70 s passed since applying began."""
    (tmp_path / "hosts").write_text("h1\nh2\nh3\nh4\n")
    (tmp_path / "sites").write_text("s1\ns2\ns1\ns2\n")
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4",
            "database": "10.9.9.9"}
    fleet = FakeFleet({k: v for k, v in addr.items() if k != "database"})
    clock = [1000.0]

    def slow_fleet(host, script):
        if "iptables -N" in script:
            clock[0] += 5.0            # 4 hosts x 5 s = a 20 s apply
        return fleet(host, script)

    monkeypatch.setattr(partition, "ssh_run", slow_fleet)
    monkeypatch.setattr(partition.socket, "gethostbyname", lambda h: addr[h])
    monkeypatch.setattr(partition.time, "time", lambda: clock[0])

    def sleep(s):
        clock[0] += 50.0               # 10 s short of the hold, then stopped
        raise partition._Stop()

    monkeypatch.setattr(partition.time, "sleep", sleep)
    out = tmp_path / "p.json"
    rc = partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                         str(tmp_path / "sites"), "--agents", "4", "--duration", "60",
                         "--out", str(out)])
    rec = json.loads(out.read_text())
    assert rc == 3 and rec["outcome"] == "held_short" and rec["held_s"] < 60


def test_a_stop_while_rules_are_going_on_is_not_a_success(tmp_path, monkeypatch):
    """The wait and the delay must never count as hold: stopped mid-apply after a long delay,
    the partition held for nothing."""
    (tmp_path / "hosts").write_text("h1\nh2\nh3\nh4\n")
    (tmp_path / "sites").write_text("s1\ns2\ns1\ns2\n")
    addr = {"h1": "10.0.0.1", "h2": "10.0.0.2", "h3": "10.0.0.3", "h4": "10.0.0.4",
            "database": "10.9.9.9"}
    fleet = FakeFleet({k: v for k, v in addr.items() if k != "database"})
    clock = [1000.0]

    def stopping_fleet(host, script):
        if "iptables -N" in script and len(fleet.blocked) >= 1:
            raise partition._Stop()            # SIGTERM lands mid-apply
        return fleet(host, script)

    monkeypatch.setattr(partition, "ssh_run", stopping_fleet)
    monkeypatch.setattr(partition.socket, "gethostbyname", lambda h: addr[h])
    monkeypatch.setattr(partition.time, "time", lambda: clock[0])
    monkeypatch.setattr(partition.time, "sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    monkeypatch.setattr(partition.Partition, "_fan",
                        lambda self, fn, hosts: {h: fn(h) for h in hosts})
    out = tmp_path / "p.json"
    rc = partition.main(["run", "--hosts-file", str(tmp_path / "hosts"), "--sites-file",
                         str(tmp_path / "sites"), "--agents", "4", "--duration", "60",
                         "--delay", "300", "--out", str(out)])
    rec = json.loads(out.read_text())
    assert rc == 3 and rec["outcome"] == "interrupted_during_apply" and rec["held_s"] == 0.0
    assert not fleet.blocked, "the hosts that were cut must be healed"
