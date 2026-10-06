"""Code review 2026-10-05 §56, §58, §59 leftovers: flags, converter defaults, setup scripts."""
import argparse
import os
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO)


# --------------------------------------------------------------------------- §56
def test_converter_defaults_match_the_runner():
    """per-site loses DAG edges and inputs, and run_test.py always passed per-file / job: one
    concept, one default."""
    import pegasus_to_swarm_converter as conv
    src = open(os.path.join(REPO, "pegasus_to_swarm_converter.py")).read()
    assert '"--data-nodes", choices=["per-site", "per-file"], default="per-file",' in src
    assert '"--dtn-scope", choices=["file", "job"], default="job",' in src
    import inspect
    assert inspect.signature(conv.map_profile).parameters["data_nodes_mode"].default == "per-file"
    assert inspect.signature(conv.make_dtn_resolver).parameters["dtn_scope"].default == "job"


def test_a_batch_forwards_run_flags():
    from batch_tests_v2 import forwarded_run_flags
    args = argparse.Namespace(pegasus_jobs_dir="/b", pegasus_data_nodes="per-file",
                              pegasus_dtn_names=None, pegasus_bundle_source_root=None,
                              textfile_dir="/t", quantum_agents_pct=None, quantum_fraction=0.2,
                              hybrid_fraction=None, job_target_agents=27,
                              pegasus_dag_gating=True, split_hybrid=False)
    flags = forwarded_run_flags(args)
    assert flags == ["--pegasus-jobs-dir", "/b", "--pegasus-data-nodes", "per-file",
                     "--textfile-dir", "/t", "--quantum-fraction", "0.2",
                     "--job-target-agents", "27", "--pegasus-dag-gating"]


def test_every_forwarded_flag_exists_on_the_runner():
    from batch_tests_v2 import _FORWARD_SWITCHES, _FORWARD_VALUED
    src = open(os.path.join(REPO, "run_test.py")).read()
    for name in _FORWARD_VALUED + _FORWARD_SWITCHES:
        assert f'"--{name.replace("_", "-")}"' in src, name


def test_dead_flags_say_so():
    src = open(os.path.join(REPO, "run_test.py")).read()
    assert 'help="NO EFFECT: the plotting step always runs in full.' in src
    assert 'help="NO EFFECT: agent logs are written to --run-dir' in src


# --------------------------------------------------------------------------- §58
def _mapped(*ids):
    return [(i, {"id": j}, {}, []) for i, j in enumerate(ids, 1)]


def test_duplicate_job_ids_are_refused():
    from pegasus_to_swarm_converter import refuse_duplicate_job_ids
    with pytest.raises(ValueError, match="occur more than once"):
        refuse_duplicate_job_ids(_mapped("wf_run0001_a", "wf_run0001_b", "wf_run0001_a"))
    refuse_duplicate_job_ids(_mapped("a", "b"))


def test_redis_profiles_are_read_in_key_order():
    src = open(os.path.join(REPO, "pegasus_to_swarm_converter.py")).read()
    assert "for key in sorted(r.scan_iter(match=pattern)):" in src


# --------------------------------------------------------------------------- §59
def test_the_clock_script_defaults_to_the_whole_slice():
    src = open(os.path.join(REPO, "fix_slice_clocks.sh")).read()
    assert "/etc/hosts" in src
    assert 'mapfile -t HOSTS < "$HERE/agent_hosts.txt"' not in src


def test_the_hosts_parse_keeps_agents_and_drops_monitoring_aliases(tmp_path):
    hosts = tmp_path / "hosts"
    hosts.write_text("127.0.0.1 localhost\n10.0.0.5 database\n10.0.1.10 agent-10\n"
                     "10.0.1.1 agent-1\n10.9.1.1 agent-1-mon\n")
    out = subprocess.run(
        ["bash", "-c", "awk '{for (i = 2; i <= NF; i++) if ($i ~ /^agent-[0-9]+$/) print $i}' "
                       f"{hosts} | sort -t- -k2,2n -u"], capture_output=True, text=True).stdout
    assert out.split() == ["agent-1", "agent-10"]


def test_nfs_setup_probes_every_host_and_checksums_images():
    src = open(os.path.join(REPO, "setup_nfs_workflow.sh")).read()
    assert 'FIRST="${HOSTS[0]}"' not in src
    assert "unwritable=$(probe_writes)" in src
    assert src.count("unwritable=$(probe_writes)") == 2          # --check and setup
    assert "sha256sum" in src and 'stat -c%s "$STAGE_IMAGE"' not in src


def test_the_scripts_parse():
    for script in ("fix_slice_clocks.sh", "setup_nfs_workflow.sh"):
        assert subprocess.run(["bash", "-n", os.path.join(REPO, script)]).returncode == 0
