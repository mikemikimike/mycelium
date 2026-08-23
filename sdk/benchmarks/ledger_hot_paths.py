"""Dependency-free microbenchmarks for synchronous ledger storage hot paths.

Run from ``sdk/``:

    python benchmarks/ledger_hot_paths.py

The benchmark creates fresh storage for mutating cases, performs warmups, and
reports per-operation median, p95, IQR, MAD, and raw samples. It intentionally
has no fixed performance assertions; compare runs made on the same machine.
"""

from __future__ import annotations

import argparse
import gc
import os
import platform
import statistics
import sys
import tempfile
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from mycelium.action_ledger import FileLedgerStorage, InMemoryLedgerStorage, LedgerEntry


@dataclass(frozen=True)
class BenchmarkCase:
    name: str
    cardinality: int
    operations: int
    run_sample: Callable[[], None]


def _entry(index: int) -> LedgerEntry:
    return LedgerEntry(
        request_id=f"request-{index}",
        effect_id=f"effect-{index}",
        tool="benchmark_tool",
        args=[index],
        kwargs={"value": index},
        status="completed",
        terminal_outcome="completed",
        started_at=float(index),
    )


def _percentile(sorted_values: list[float], percentile: float) -> float:
    position = (len(sorted_values) - 1) * percentile
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower
    return sorted_values[lower] + (sorted_values[upper] - sorted_values[lower]) * fraction


def _measure(case: BenchmarkCase, *, warmups: int, repeats: int) -> dict[str, object]:
    for _ in range(warmups):
        case.run_sample()

    samples_us: list[float] = []
    gc_enabled = gc.isenabled()
    try:
        gc.disable()
        for _ in range(repeats):
            started = time.perf_counter_ns()
            case.run_sample()
            elapsed_ns = time.perf_counter_ns() - started
            samples_us.append(elapsed_ns / case.operations / 1_000)
    finally:
        if gc_enabled:
            gc.enable()

    ordered = sorted(samples_us)
    median = statistics.median(ordered)
    return {
        "name": case.name,
        "cardinality": case.cardinality,
        "operations": case.operations,
        "median_us": median,
        "p95_us": _percentile(ordered, 0.95),
        "iqr_us": _percentile(ordered, 0.75) - _percentile(ordered, 0.25),
        "mad_us": statistics.median(abs(value - median) for value in ordered),
        "samples_us": samples_us,
    }


def _inmemory_set_case(cardinality: int) -> BenchmarkCase:
    entries = tuple(_entry(index) for index in range(cardinality))

    def run_sample() -> None:
        storage = InMemoryLedgerStorage()
        for entry in entries:
            storage.set(entry)

    return BenchmarkCase(
        name="inmemory_set_unique",
        cardinality=cardinality,
        operations=cardinality,
        run_sample=run_sample,
    )


def _inmemory_lookup_case(cardinality: int, *, hit: bool) -> BenchmarkCase:
    storage = InMemoryLedgerStorage()
    for index in range(cardinality):
        storage.set(_entry(index))
    operations = max(1_000, cardinality)
    if hit:

        def run_sample() -> None:
            for index in range(operations):
                storage.get_by_effect_id(f"effect-{index % cardinality}")

        name = "inmemory_effect_hit"
    else:

        def run_sample() -> None:
            for index in range(operations):
                storage.get_by_effect_id(f"missing-effect-{index}")

        name = "inmemory_effect_miss"
    return BenchmarkCase(
        name=name,
        cardinality=cardinality,
        operations=operations,
        run_sample=run_sample,
    )


def _file_lookup_case(root: Path, cardinality: int) -> BenchmarkCase:
    storage = FileLedgerStorage(root / f"ledger-{cardinality}.json")
    for index in range(cardinality):
        storage.set(_entry(index))
    operations = cardinality

    def run_sample() -> None:
        for index in range(operations):
            storage.get_by_effect_id(f"effect-{index}")

    return BenchmarkCase(
        name="file_effect_hit",
        cardinality=cardinality,
        operations=operations,
        run_sample=run_sample,
    )


def _print_context(*, warmups: int, repeats: int) -> None:
    print("context:")
    print(f"  python={platform.python_version()} ({platform.python_implementation()})")
    print(f"  compiler={platform.python_compiler()}")
    print(f"  executable={sys.executable}")
    print(f"  platform={platform.platform()}")
    print(f"  machine={platform.machine()}")
    print(f"  processor={platform.processor() or 'unknown'}")
    print(f"  cpu_count={os.cpu_count()}")
    print(f"  warmups={warmups}")
    print(f"  repeats={repeats}")
    print("  clock=perf_counter_ns")


def _print_result(result: dict[str, object]) -> None:
    print(
        f"{result['name']} cardinality={result['cardinality']} "
        f"operations/sample={result['operations']}"
    )
    print(
        f"  median_us/op={result['median_us']:.6f} "
        f"p95_us/op={result['p95_us']:.6f} "
        f"iqr_us/op={result['iqr_us']:.6f} "
        f"mad_us/op={result['mad_us']:.6f}"
    )
    samples = ", ".join(f"{sample:.6f}" for sample in result["samples_us"])
    print(f"  samples_us/op=[{samples}]")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        choices=("all", "inmemory-set", "inmemory-hit", "inmemory-miss", "file-hit"),
        default="all",
    )
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument("--repeats", type=int, default=7)
    args = parser.parse_args()
    if args.warmups < 0 or args.repeats < 3:
        parser.error("--warmups must be >= 0 and --repeats must be >= 3")

    _print_context(warmups=args.warmups, repeats=args.repeats)
    cases: list[BenchmarkCase] = []
    if args.scenario in ("all", "inmemory-set"):
        cases.extend(_inmemory_set_case(size) for size in (100, 1_000))
    if args.scenario in ("all", "inmemory-hit"):
        cases.extend(_inmemory_lookup_case(size, hit=True) for size in (100, 1_000))
    if args.scenario in ("all", "inmemory-miss"):
        cases.extend(_inmemory_lookup_case(size, hit=False) for size in (100, 1_000))

    with tempfile.TemporaryDirectory(prefix="mycelium-benchmark-") as directory:
        if args.scenario in ("all", "file-hit"):
            cases.extend(
                _file_lookup_case(Path(directory), size)
                for size in (10, 100)
            )
        for case in cases:
            _print_result(_measure(case, warmups=args.warmups, repeats=args.repeats))


if __name__ == "__main__":
    main()
