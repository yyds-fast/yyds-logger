"""Optional line-by-line profiling used by ``YydsLogger.time_it``."""

import inspect
import re
import sys
import threading
from collections import defaultdict
from time import perf_counter


_TRACE_STATE = threading.local()
_TAG_ESCAPE_RE = re.compile(r"(\\*)(</?(?:[fb]g\s)?[^<>\s]*>)")


def _escape_color_tags(text: str) -> str:
    """Escape Loguru markup tags in source code while preserving literal backslashes."""
    return _TAG_ESCAPE_RE.sub(
        lambda match: "\\" * (2 * len(match.group(1)) + 1) + match.group(2),
        text,
    )


class ProfilingBusyError(RuntimeError):
    """Raised when another fallback tracer is active in this thread."""


def _enter_trace() -> None:
    # ``sys.settrace()`` is local to the current OS thread.  A process-wide
    # lock needlessly serialized independent profilers running in different
    # threads, while still needing to reject overlapping asyncio tasks in the
    # same thread.
    if getattr(_TRACE_STATE, "active", False):
        raise ProfilingBusyError
    _TRACE_STATE.active = True


def _leave_trace() -> None:
    _TRACE_STATE.active = False


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
        try:
            result, duration_ms, line_times, line_hits = _trace_sync(func, *args, **kwargs)
        except ProfilingBusyError as exc:
            raise RuntimeError(logger._msg("ERR_PROFILING_BUSY")) from exc
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
        try:
            result, duration_ms, line_times, line_hits = await _trace_async(func, *args, **kwargs)
        except ProfilingBusyError as exc:
            raise RuntimeError(logger._msg("ERR_PROFILING_BUSY")) from exc
    print_report(logger, func, duration_ms, line_times, line_hits)
    return result


def _extract_stats(stats, func):
    unit = getattr(stats, "unit", 1e-6)
    code = getattr(func, "__code__", None)
    target_file = getattr(code, "co_filename", None)
    target_line = getattr(code, "co_firstlineno", None)
    line_times, line_hits = {}, {}
    for key, timings in stats.timings.items():
        matched = (
            target_file is not None
            and key[0] == target_file
            and key[1] == target_line
        )
        if not matched and target_file is None:
            matched = key[2].split(".")[-1] == func.__name__
        if matched:
            for line_no, hits, total_time in timings:
                line_times[line_no] = total_time * unit
                line_hits[line_no] = hits
    return line_times, line_hits


def _build_line_tracer(target_code):
    line_times = defaultdict(float)
    line_hits = defaultdict(int)
    frame_states = {}

    def local_tracer(frame, event, arg):
        if event == "line":
            now = perf_counter()
            state = frame_states.get(frame)
            if state is not None:
                if state[0] is not None:
                    line_times[state[0]] += now - state[1]
                lineno = frame.f_lineno
                state[0] = lineno
                state[1] = now
                line_hits[lineno] += 1
        elif event == "return":
            state = frame_states.pop(frame, None)
            if state is not None and state[0] is not None:
                line_times[state[0]] += perf_counter() - state[1]
        return local_tracer

    def global_tracer(frame, event, arg):
        if event == "call" and frame.f_code == target_code:
            frame_states[frame] = [None, perf_counter()]
            return local_tracer
        return None

    return global_tracer, line_times, line_hits


def _trace_sync(func, *args, **kwargs):
    return_value = None
    tracer, line_times, line_hits = _build_line_tracer(func.__code__)

    _enter_trace()
    try:
        old_trace = sys.gettrace()
        sys.settrace(tracer)
        start = perf_counter()
        try:
            return_value = func(*args, **kwargs)
        finally:
            sys.settrace(old_trace)
    finally:
        _leave_trace()
    return return_value, (perf_counter() - start) * 1000.0, line_times, line_hits


async def _trace_async(func, *args, **kwargs):
    tracer, line_times, line_hits = _build_line_tracer(func.__code__)

    _enter_trace()
    try:
        old_trace = sys.gettrace()
        sys.settrace(tracer)
        start = perf_counter()
        try:
            result = await func(*args, **kwargs)
        finally:
            sys.settrace(old_trace)
    finally:
        _leave_trace()
    return result, (perf_counter() - start) * 1000.0, line_times, line_hits


def print_report(logger, func, total_duration_ms, line_times, line_hits):
    if getattr(logger, "_cleanup_state", "open") != "open":
        return
    d3_logger = logger.logger.opt(depth=3)
    try:
        lines, start_line = inspect.getsourcelines(func)
    except Exception as exc:
        d3_logger.warning(
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
        source_text = _escape_color_tags(source.rstrip("\n"))
        row = f" {line_no:>4} | {hits:>8} | {milliseconds:>11.2f} | {average:>11.2f} | {percent:>7.1f}% | {source_text}"
        if (percent >= 20.0 and hits) or (line_no == slowest_line and percent > 5.0):
            row = f"<red><b>{row}  <-- 🚨 {logger._msg('PERF_BOTTLENECK')}</b></red>"
        report.append(row)
    report.extend(["-" * 80, logger._msg("TOTAL_DURATION_MS", duration=f"{total_duration_ms:.2f}"), "=" * 80])
    logger.logger.opt(depth=3, colors=True).info("\n" + "\n".join(report))
