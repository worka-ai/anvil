#!/usr/bin/env python3
"""Validate CoreMeta performance-gate evidence after the benchmark exits."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
from pathlib import Path
from typing import Any


REPORT_SCHEMA = "anvil.perf.coremeta_gate_report.v1"
REQUIRED_SCENARIOS = {
    "point_get_small",
    "point_get_large",
    "prefix_page_early_large",
    "prefix_page_deep_small",
    "prefix_page_deep_large",
    "bounded_list_scaled_page_large",
    "durable_single_row",
    "transactional_head_read_and_batch",
}
REQUIRED_COMPLEXITY_GATES = {
    "point_get_work_is_table_size_independent",
    "deep_page_work_is_table_size_independent",
    "deep_page_work_is_comparable_to_early_page",
    "bounded_list_work_scales_with_page_size",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--profile", choices=("quick", "release"), required=True)
    return parser.parse_args()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(f"CoreMeta performance report rejected: {message}")


def finite_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def validate(
    report: dict[str, Any],
    manifest: dict[str, Any],
    manifest_bytes: bytes,
    expected_profile: str,
) -> None:
    require(report.get("schema") == REPORT_SCHEMA, "unexpected report schema")
    require(report.get("profile") == expected_profile, "profile mismatch")
    require(report.get("pass") is True, "benchmark reported a failed gate")
    require(bool(report.get("git_commit")), "git commit is absent")
    require(bool(report.get("gate_manifest_hash")), "manifest hash is absent")
    require(
        report.get("gate_manifest_sha256")
        == f"sha256:{hashlib.sha256(manifest_bytes).hexdigest()}",
        "copied manifest SHA-256 does not match the report",
    )
    require(
        manifest.get("schema") == "anvil.perf.coremeta_gate_manifest.v1",
        "unexpected gate manifest schema",
    )
    require(
        manifest.get("dataset_id") == report.get("dataset_id")
        and manifest.get("seed") == report.get("seed"),
        "report dataset identity does not match copied manifest",
    )
    require(
        manifest.get("profiles", {}).get(expected_profile) == report.get("profile_spec"),
        "report profile does not match copied manifest",
    )
    if os.environ.get("GITHUB_ACTIONS") == "true":
        require(
            report.get("git_commit") == os.environ.get("GITHUB_SHA"),
            "CI report commit does not match GITHUB_SHA",
        )

    scenarios = report.get("scenarios")
    require(isinstance(scenarios, list), "scenarios must be an array")
    by_name = {scenario.get("name"): scenario for scenario in scenarios}
    require(REQUIRED_SCENARIOS <= set(by_name), "one or more required scenarios are absent")
    for name in REQUIRED_SCENARIOS:
        scenario = by_name[name]
        require(scenario.get("sample_count", 0) > 0, f"{name} has no samples")
        samples = scenario.get("latency_ns")
        require(isinstance(samples, list), f"{name} has no raw latency evidence")
        require(
            len(samples) == scenario["sample_count"],
            f"{name} raw sample count does not match summary",
        )
        require(all(finite_number(value) and value >= 0 for value in samples), f"{name} has invalid latency samples")
        require(finite_number(scenario.get("p95_ms")), f"{name} p95 is invalid")
        require(
            scenario.get("item_count_mismatches") == 0,
            f"{name} contains bounded-result mismatches",
        )
        require(
            finite_number(scenario.get("logical_work_per_operation")),
            f"{name} work evidence is invalid",
        )

    gates = report.get("gates")
    require(isinstance(gates, list) and gates, "gates must be a non-empty array")
    gate_names = {gate.get("name") for gate in gates}
    require(REQUIRED_COMPLEXITY_GATES <= gate_names, "required complexity gates are absent")
    require(all(gate.get("pass") is True for gate in gates), "at least one gate failed")
    require(
        {gate.get("category") for gate in gates} >= {"correctness", "complexity", "latency"},
        "correctness, complexity, and latency categories are all required",
    )


def write_step_summary(report: dict[str, Any], report_path: Path) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    scenarios = report["scenarios"]
    with Path(summary_path).open("a", encoding="utf-8") as summary:
        summary.write("## CoreMeta performance gate\n\n")
        summary.write(f"Profile: `{report['profile']}`  \n")
        summary.write(f"Evidence: `{report_path}`  \n")
        summary.write(f"Manifest: `{report['gate_manifest_hash']}`\n\n")
        summary.write("| Scenario | p95 ms | work/op | samples |\n")
        summary.write("|---|---:|---:|---:|\n")
        for scenario in scenarios:
            summary.write(
                f"| {scenario['name']} | {scenario['p95_ms']:.3f} | "
                f"{scenario['logical_work_per_operation']:.2f} | "
                f"{scenario['sample_count']} |\n"
            )


def main() -> None:
    args = parse_args()
    require(args.report.is_file(), f"report does not exist: {args.report}")
    manifest_path = args.report.parent / "gate-manifest.json"
    require(manifest_path.is_file(), f"copied manifest does not exist: {manifest_path}")
    report = json.loads(args.report.read_text(encoding="utf-8"))
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    require(isinstance(report, dict), "report root must be an object")
    require(isinstance(manifest, dict), "manifest root must be an object")
    validate(report, manifest, manifest_bytes, args.profile)
    write_step_summary(report, args.report)
    print(f"CoreMeta {args.profile} performance gate passed: {args.report}")


if __name__ == "__main__":
    main()
