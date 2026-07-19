#!/usr/bin/env python3
"""Regression tests for the machine-readable CoreMeta performance gate."""

from __future__ import annotations

import copy
import hashlib
import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parent.parent
CHECKER = REPO_ROOT / "scripts" / "check-coremeta-perf-report.py"
SCENARIOS = (
    "point_get_small",
    "point_get_large",
    "prefix_page_early_large",
    "prefix_page_deep_small",
    "prefix_page_deep_large",
    "bounded_list_scaled_page_large",
    "durable_single_row",
    "transactional_head_read_and_batch",
)
COMPLEXITY_GATES = (
    "point_get_work_is_table_size_independent",
    "deep_page_work_is_table_size_independent",
    "deep_page_work_is_comparable_to_early_page",
    "bounded_list_work_scales_with_page_size",
)


def valid_report() -> dict:
    profile_spec = {
        "small_rows": 4096,
        "large_rows": 65536,
        "page_size": 64,
    }
    scenarios = [
        {
            "name": name,
            "sample_count": 2,
            "latency_ns": [1000, 1100],
            "p95_ms": 0.0011,
            "logical_work_per_operation": 2.0,
            "item_count_mismatches": 0,
        }
        for name in SCENARIOS
    ]
    gates = [
        {"name": name, "category": "complexity", "pass": True}
        for name in COMPLEXITY_GATES
    ]
    gates.extend(
        [
            {"name": "bounded_results", "category": "correctness", "pass": True},
            {"name": "point_latency", "category": "latency", "pass": True},
        ]
    )
    return {
        "schema": "anvil.perf.coremeta_gate_report.v1",
        "profile": "quick",
        "pass": True,
        "git_commit": "test-commit",
        "gate_manifest_hash": "blake3:test",
        "dataset_id": "test-dataset",
        "seed": "test-seed",
        "profile_spec": profile_spec,
        "scenarios": scenarios,
        "gates": gates,
    }


def valid_manifest(report: dict) -> dict:
    return {
        "schema": "anvil.perf.coremeta_gate_manifest.v1",
        "dataset_id": report["dataset_id"],
        "seed": report["seed"],
        "profiles": {"quick": report["profile_spec"]},
    }


class CoreMetaReportCheckerTests(unittest.TestCase):
    def run_checker(
        self, report: dict, manifest: dict | None = None
    ) -> subprocess.CompletedProcess[str]:
        with tempfile.TemporaryDirectory(prefix="anvil-coremeta-report-test-") as directory:
            path = Path(directory) / "report.json"
            report_to_write = copy.deepcopy(report)
            manifest_bytes = json.dumps(manifest or valid_manifest(report)).encode()
            report_to_write["gate_manifest_sha256"] = (
                f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}"
            )
            path.write_text(json.dumps(report_to_write), encoding="utf-8")
            (path.parent / "gate-manifest.json").write_bytes(manifest_bytes)
            environment = os.environ.copy()
            environment.pop("GITHUB_ACTIONS", None)
            environment.pop("GITHUB_SHA", None)
            return subprocess.run(
                [str(CHECKER), "--report", str(path), "--profile", "quick"],
                cwd=REPO_ROOT,
                check=False,
                capture_output=True,
                text=True,
                env=environment,
            )

    def test_complete_passing_report_is_accepted(self) -> None:
        result = self.run_checker(valid_report())
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_report_level_failure_is_rejected(self) -> None:
        report = valid_report()
        report["pass"] = False
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_missing_raw_samples_are_rejected(self) -> None:
        report = valid_report()
        del report["scenarios"][0]["latency_ns"]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_missing_complexity_gate_is_rejected(self) -> None:
        report = valid_report()
        missing = COMPLEXITY_GATES[0]
        report["gates"] = [gate for gate in report["gates"] if gate["name"] != missing]
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_bounded_result_mismatch_is_rejected(self) -> None:
        report = valid_report()
        report["scenarios"][0]["item_count_mismatches"] = 1
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)

    def test_manifest_profile_mismatch_is_rejected(self) -> None:
        report = valid_report()
        manifest = valid_manifest(report)
        manifest["profiles"]["quick"] = {"small_rows": 1}
        result = self.run_checker(report, manifest)
        self.assertNotEqual(result.returncode, 0)

    def test_failed_individual_gate_is_rejected(self) -> None:
        report = copy.deepcopy(valid_report())
        report["gates"][0]["pass"] = False
        result = self.run_checker(report)
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
