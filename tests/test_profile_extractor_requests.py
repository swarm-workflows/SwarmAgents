"""A Pegasus-planned job's resource request must survive into its profile.

Pegasus writes `request_cpus = $(pegasus_cores)` and defines `pegasus_cores` further down the
same submit file. The extractor read only literal numbers, so every Pegasus-planned job
recorded 0 cores and no memory, and the converter gave each job 1 core. The EHT SMILI jobs
had asked HTCondor for 2 cores and 6 GB, so SWARM ran them single-threaded and their images
differed from the reference (T-6 pilot, 11 Nov 2026).
"""
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pegasus_profile_extractor as ext  # noqa: E402

#: The shape of a real Pegasus 5/6 submit file (eht-m87 run0001, run_smili).
PEGASUS_SUB = """\
universe = vanilla
request_cpus = $(pegasus_cores)
request_disk = $(pegasus_diskspace_mb) MB
request_memory = $(pegasus_memory_mb) MB
+pegasus_wf_xformation = "run_smili"
pegasus_cores = 2
pegasus_gpus = 0
pegasus_memory_mb = 6144
pegasus_diskspace_mb = 5120
queue
"""


class TestSubRequests(unittest.TestCase):
    def _load(self, files):
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp, "00", "00")
            d.mkdir(parents=True)
            for name, body in files.items():
                (d / name).write_text(body)
            return ext.load_sub_requests(tmp)

    def test_pegasus_macros_are_expanded(self):
        req = self._load({"run_smili_smili_095_lo.sub": PEGASUS_SUB})["run_smili_smili_095_lo"]
        self.assertEqual(req["cpus"], 2)
        self.assertEqual(req["memory"], 6144)          # MB
        self.assertEqual(req["disk"], 5120 * 1024)     # KB

    def test_literal_values_and_condor_default_units(self):
        req = self._load({"j.sub": "request_cpus = 4\nrequest_memory = 2048\n"
                                   "request_disk = 1000\n"})["j"]
        self.assertEqual(req, {"cpus": 4, "memory": 2048, "disk": 1000})

    def test_units_are_honoured(self):
        req = self._load({"j.sub": "request_memory = 2 GB\nrequest_disk = 3G\n"})["j"]
        self.assertEqual(req["memory"], 2048)
        self.assertEqual(req["disk"], 3 * 1024 ** 2)

    def test_an_unresolvable_request_is_absent_not_zero(self):
        """Absent lets the converter's default apply visibly; a guessed number would not."""
        req = self._load({"j.sub": "request_cpus = $(undefined)\nrequest_memory = 100\n"})
        self.assertNotIn("cpus", req["j"])
        self.assertEqual(req["j"]["memory"], 100)

    def test_macro_names_are_case_insensitive_as_in_condor(self):
        req = self._load({"j.sub": "request_cpus = $(Pegasus_Cores)\npegasus_cores = 3\n"})
        self.assertEqual(req["j"]["cpus"], 3)


if __name__ == "__main__":
    unittest.main()
