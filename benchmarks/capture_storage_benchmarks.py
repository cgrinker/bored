#!/usr/bin/env python3
"""Run storage benchmarks and archive JSON snapshots.

This script executes the `bored_benchmarks` binary with the JSON output option,
optionally enforces baseline thresholds, and writes the captured measurements to
`benchmarks/results/` (or a caller-provided directory). It is meant to provide a
repeatable way to track performance over time by checking the generated JSON
artifacts into an external tracking system or simply storing them locally.
"""

from __future__ import annotations

import argparse
import datetime as _dt
import json
import os
import pathlib
import subprocess
import sys
from typing import Any, Dict


def _default_binary(build_dir: pathlib.Path, config: str) -> pathlib.Path:
    """Compute the default benchmark binary path for the current platform."""
    platform_suffix = ".exe" if sys.platform.startswith("win") else ""
    candidate = build_dir / config / f"bored_benchmarks{platform_suffix}"
    return candidate


def _run_benchmark(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Run the benchmark command, returning the completed process object."""
    return subprocess.run(command, text=True, capture_output=True, check=False)


def _extract_json(stdout: str) -> Dict[str, Any]:
    """Extract the first JSON document emitted by the benchmark."""
    for line in stdout.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            continue
    raise RuntimeError("Benchmark output did not contain JSON payload")


def _write_snapshot(data: Dict[str, Any], output_dir: pathlib.Path, tag: str | None) -> pathlib.Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    suffix = f"_{tag}" if tag else ""
    path = output_dir / f"storage_benchmarks_{timestamp}{suffix}.json"
    with path.open("w", encoding="utf-8") as stream:
        json.dump(data, stream, indent=2, sort_keys=True)
        stream.write("\n")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Capture storage benchmark snapshots")
    parser.add_argument(
        "--build-dir",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parents[1] / "build",
        help="CMake build directory containing the benchmark executable",
    )
    parser.add_argument(
        "--config",
        default="Release",
        help="Build configuration subdirectory to search (default: %(default)s)",
    )
    parser.add_argument(
        "--binary",
        type=pathlib.Path,
        help="Explicit path to bored_benchmarks executable (overrides build-dir/config)",
    )
    parser.add_argument(
        "--samples",
        type=int,
        default=10,
        help="Number of samples to collect per benchmark (default: %(default)s)",
    )
    parser.add_argument(
        "--baseline",
        type=pathlib.Path,
        help="Optional baseline JSON file to enforce regression thresholds",
    )
    parser.add_argument(
        "--tolerance",
        type=float,
        default=0.15,
        help="Allowable fractional regression when --baseline is provided (default: %(default)s)",
    )
    parser.add_argument(
        "--output-dir",
        type=pathlib.Path,
        default=pathlib.Path(__file__).resolve().parent / "results",
        help="Directory to store JSON snapshots (default: %(default)s)",
    )
    parser.add_argument(
        "--tag",
        help="Optional suffix to append to the output filename",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Echo benchmark stdout/stderr to the console",
    )

    args = parser.parse_args(argv)

    binary = args.binary or _default_binary(args.build_dir, args.config)
    if not binary.exists():
        parser.error(f"Benchmark binary not found at {binary}")

    command = [str(binary), f"--json", f"--samples={args.samples}"]
    if args.baseline:
        command.append(f"--baseline={args.baseline}")
        command.append(f"--tolerance={args.tolerance}")

    process = _run_benchmark(command)
    if args.verbose:
        if process.stdout:
            sys.stdout.write(process.stdout)
        if process.stderr:
            sys.stderr.write(process.stderr)

    data = _extract_json(process.stdout)
    snapshot_path = _write_snapshot(data, args.output_dir, args.tag)

    if process.returncode != 0:
        message_lines = ["Benchmark run reported regressions:"]
        trailing_output = process.stdout.strip().splitlines()[1:]
        trailing_output += process.stderr.strip().splitlines()
        message_lines.extend(line for line in trailing_output if line.strip())
        message_lines.append(f"JSON snapshot retained at {snapshot_path}")
        raise SystemExit("\n".join(message_lines))

    print(f"Captured storage benchmark snapshot -> {snapshot_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
