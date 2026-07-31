"""Repeatable throughput and producer-latency benchmark for YydsLogger.

Examples:

    python benchmarks/bench_logging.py --mode all --messages 20000 --threads 4
    python benchmarks/bench_logging.py --mode enqueue --serialize --defer-format --json
"""

import argparse
import json
import math
import sys
import tempfile
import threading
from pathlib import Path
from time import perf_counter, perf_counter_ns
from typing import Any, Dict, List

# Executing a script makes ``benchmarks/`` the first import location. Add the
# repository root explicitly so measurements always target the checked-out
# source instead of an unrelated installed yyds-logger distribution.
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from yyds_logger import YydsLogger  # noqa: E402


def _percentile(values_ns: List[int], percentile: float) -> float:
    if not values_ns:
        return 0.0
    ordered = sorted(values_ns)
    index = max(0, math.ceil(len(ordered) * percentile) - 1)
    return ordered[index] / 1_000_000.0


def _case_options(mode: str) -> Dict[str, Any]:
    if mode == "sync":
        return {"enqueue": False, "overflow_policy": "block"}
    if mode == "enqueue":
        return {"enqueue": True, "overflow_policy": "block"}
    if mode == "drop":
        return {"enqueue": True, "overflow_policy": "drop"}
    raise ValueError("Unsupported benchmark mode: %s" % mode)


def run_case(args: argparse.Namespace, mode: str) -> Dict[str, Any]:
    options = _case_options(mode)
    per_thread, remainder = divmod(args.messages, args.threads)
    latencies: List[int] = []
    errors: List[BaseException] = []
    result_lock = threading.Lock()
    start_barrier = threading.Barrier(args.threads + 1)

    with tempfile.TemporaryDirectory(prefix="yyds-logger-bench-") as log_dir:
        logger = YydsLogger(
            "benchmark",
            log_dir=log_dir,
            env="prod",
            console_level="CRITICAL",
            file_level="DEBUG",
            queue_size=args.queue_size,
            queue_timeout=args.queue_timeout,
            queue_backend=args.queue_backend,
            process_isolation=args.process_isolation,
            serialize=args.serialize,
            defer_format=getattr(args, "defer_format", False) and options["enqueue"],
            compression=None,
            max_size=1024,
            **options
        )

        def worker(worker_index: int, count: int) -> None:
            local_latencies: List[int] = []
            try:
                start_barrier.wait()
                for sequence in range(count):
                    started_ns = perf_counter_ns()
                    logger.log(args.level, "benchmark worker={} sequence={}", worker_index, sequence)
                    local_latencies.append(perf_counter_ns() - started_ns)
            except BaseException as exc:
                with result_lock:
                    errors.append(exc)
            finally:
                with result_lock:
                    latencies.extend(local_latencies)

        threads = [
            threading.Thread(
                target=worker,
                args=(index, per_thread + (1 if index < remainder else 0)),
                name="benchmark-%d" % index,
            )
            for index in range(args.threads)
        ]
        for thread in threads:
            thread.start()

        started = perf_counter()
        start_barrier.wait()
        for thread in threads:
            thread.join()
        producer_seconds = perf_counter() - started

        try:
            if errors:
                raise errors[0]
            drain_started = perf_counter()
            logger.flush()
            drain_seconds = perf_counter() - drain_started
            queue_status = logger.get_queue_status()
            dropped = queue_status["dropped_messages"]
            output_bytes = sum(path.stat().st_size for path in Path(log_dir).glob("*.log"))
        finally:
            logger.cleanup()

    attempted = len(latencies)
    delivered = max(0, attempted - dropped)
    return {
        "mode": mode,
        "queue_backend": queue_status["backend"],
        "threads": args.threads,
        "attempted_messages": attempted,
        "delivered_messages": delivered,
        "dropped_messages": dropped,
        "producer_seconds": round(producer_seconds, 6),
        "producer_messages_per_second": round(attempted / producer_seconds, 2) if producer_seconds else 0.0,
        "delivered_messages_per_second": round(delivered / producer_seconds, 2) if producer_seconds else 0.0,
        "drain_seconds": round(drain_seconds, 6),
        "latency_ms": {
            "p50": round(_percentile(latencies, 0.50), 4),
            "p95": round(_percentile(latencies, 0.95), 4),
            "p99": round(_percentile(latencies, 0.99), 4),
        },
        "output_bytes": output_bytes,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("sync", "enqueue", "drop", "all"), default="all")
    parser.add_argument("--messages", type=int, default=20_000)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--queue-size", type=int, default=10_000)
    parser.add_argument("--queue-timeout", type=float)
    parser.add_argument("--queue-backend", choices=("auto", "multiprocessing", "thread"), default="auto")
    parser.add_argument("--process-isolation", action="store_true")
    parser.add_argument("--level", choices=("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    parser.add_argument("--serialize", action="store_true")
    parser.add_argument("--defer-format", action="store_true")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.messages <= 0:
        parser.error("--messages must be positive")
    if args.threads <= 0:
        parser.error("--threads must be positive")
    if args.queue_size <= 0:
        parser.error("--queue-size must be positive")
    if args.queue_timeout is not None and args.queue_timeout < 0:
        parser.error("--queue-timeout must be non-negative")
    return args


def main() -> None:
    args = parse_args()
    modes = ("sync", "enqueue", "drop") if args.mode == "all" else (args.mode,)
    results = [run_case(args, mode) for mode in modes]
    if args.json:
        print(json.dumps(results, ensure_ascii=False, indent=2))
        return

    for result in results:
        latency = result["latency_ms"]
        print(
            "{mode:7} backend={queue_backend:15} attempted={attempted_messages:<7} delivered={delivered_messages:<7} "
            "dropped={dropped_messages:<7} producer={producer_messages_per_second:>10.2f}/s "
            "p50/p95/p99={p50:.4f}/{p95:.4f}/{p99:.4f}ms drain={drain_seconds:.4f}s".format(
                **result, **latency
            )
        )


if __name__ == "__main__":
    main()
