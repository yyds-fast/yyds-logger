"""Optional line-by-line profiling used by ``YydsLogger.time_it``."""

import inspect
import sys
from collections import defaultdict
from time import perf_counter


def run_sync(logger, func, *args, **kwargs):
    try:
        from line_profiler import LineProfiler  # type: ignore
    except ImportError:
        LineProfiler = None
    if LineProfiler is not None:
        profiler = LineProfiler()
        start = perf_counter()
        result = profiler(func)(*args, **kwargs)
        duration_ms = (perf_counter() - start) * 1000.0
        line_times, line_hits = _extract_stats(profiler.get_stats(), func)
    else:
        result, duration_ms, line_times, line_hits = _trace_sync(func, *args, **kwargs)
    print_report(logger, func, duration_ms, line_times, line_hits)
    return result


def run_async(logger, func, *args, **kwargs):
    return _run_async(logger, func, *args, **kwargs)


async def _run_async(logger, func, *args, **kwargs):
    try:
        from line_profiler import LineProfiler  # type: ignore
    except ImportError:
        LineProfiler = None

    if LineProfiler is not None:
        profiler = LineProfiler()
        start = perf_counter()
        result = await profiler(func)(*args, **kwargs)
        duration_ms = (perf_counter() - start) * 1000.0
        line_times, line_hits = _extract_stats(profiler.get_stats(), func)
    else:
        result, duration_ms, line_times, line_hits = await _trace_async(func, *args, **kwargs)
    print_report(logger, func, duration_ms, line_times, line_hits)
    return result


def _extract_stats(stats, func):
    unit = getattr(stats, "unit", 1e-6)
    line_times, line_hits = {}, {}
    for key, timings in stats.timings.items():
        if key[2].split(".")[-1] == func.__name__:
            for line_no, hits, total_time in timings:
                line_times[line_no] = total_time * unit
                line_hits[line_no] = hits
    return line_times, line_hits


def _trace_sync(func, *args, **kwargs):
    return_value = None
    line_times = defaultdict(float)
    line_hits = defaultdict(int)
    state = {"last_time": perf_counter(), "last_line": None}
    target_code = func.__code__

    def tracer(frame, event, arg):
        if frame.f_code == target_code:
            if event == "line":
                now = perf_counter()
                if state["last_line"] is not None:
                    line_times[state["last_line"]] += now - state["last_time"]
                state["last_line"] = frame.f_lineno
                line_hits[frame.f_lineno] += 1
                state["last_time"] = now
            elif event == "return" and state["last_line"] is not None:
                line_times[state["last_line"]] += perf_counter() - state["last_time"]
                state["last_line"] = None
        return tracer

    old_trace = sys.gettrace()
    sys.settrace(tracer)
    start = perf_counter()
    try:
        return_value = func(*args, **kwargs)
    finally:
        sys.settrace(old_trace)
    return return_value, (perf_counter() - start) * 1000.0, line_times, line_hits


async def _trace_async(func, *args, **kwargs):
    state = {"last_time": perf_counter(), "last_line": None}
    line_times = defaultdict(float)
    line_hits = defaultdict(int)
    target_code = func.__code__

    def tracer(frame, event, arg):
        if frame.f_code == target_code:
            if event == "line":
                now = perf_counter()
                if state["last_line"] is not None:
                    line_times[state["last_line"]] += now - state["last_time"]
                state["last_line"] = frame.f_lineno
                line_hits[frame.f_lineno] += 1
                state["last_time"] = now
            elif event == "return" and state["last_line"] is not None:
                line_times[state["last_line"]] += perf_counter() - state["last_time"]
                state["last_line"] = None
        return tracer

    old_trace = sys.gettrace()
    sys.settrace(tracer)
    start = perf_counter()
    try:
        result = await func(*args, **kwargs)
    finally:
        sys.settrace(old_trace)
    return result, (perf_counter() - start) * 1000.0, line_times, line_hits


def print_report(logger, func, total_duration_ms, line_times, line_hits):
    try:
        lines, start_line = inspect.getsourcelines(func)
    except Exception as exc:
        logger.logger.opt(depth=3).warning(
            logger._msg("CANNOT_GET_SOURCE", func=func.__name__, error=str(exc))
        )
        return

    report = ["=" * 80, logger._msg("LINE_PROFILE_REPORT_HEADER", func=func.__name__), "=" * 80]
    report.append(
        f"{logger._msg('COL_LINE_NO'):<6} | {logger._msg('COL_HITS'):<8} | "
        f"{logger._msg('COL_TOTAL_TIME'):<12} | {logger._msg('COL_AVG_TIME'):<12} | "
        f"{logger._msg('COL_PCT'):<8} | {logger._msg('COL_SOURCE')}"
    )
    report.append("-" * 80)
    percentages = {}
    for index in range(len(lines)):
        line_no = start_line + index
        milliseconds = line_times.get(line_no, 0.0) * 1000.0
        percentages[line_no] = milliseconds / total_duration_ms * 100.0 if total_duration_ms else 0.0
    slowest_line = max(percentages, key=percentages.get, default=None)
    for index, source in enumerate(lines):
        line_no = start_line + index
        hits = line_hits.get(line_no, 0)
        milliseconds = line_times.get(line_no, 0.0) * 1000.0
        percent = percentages[line_no]
        average = milliseconds / hits if hits else 0.0
        source_text = source.rstrip("\n").replace("<", "\\<")
        row = f" {line_no:>4} | {hits:>8} | {milliseconds:>11.2f} | {average:>11.2f} | {percent:>7.1f}% | {source_text}"
        if (percent >= 20.0 and hits) or (line_no == slowest_line and percent > 5.0):
            row = f"<red><b>{row}  <-- 🚨 {logger._msg('PERF_BOTTLENECK')}</b></red>"
        report.append(row)
    report.extend(["-" * 80, logger._msg("TOTAL_DURATION_MS", duration=f"{total_duration_ms:.2f}"), "=" * 80])
    logger.logger.opt(depth=3, colors=True).info("\n" + "\n".join(report))
