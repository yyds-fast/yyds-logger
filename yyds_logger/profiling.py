"""Optional line-by-line profiling used by ``YydsLogger.time_it``."""

import asyncio
import inspect
import re
import sys
import threading
from collections import defaultdict
from time import perf_counter
from typing import Any, Dict, List


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


def _enter_trace(target_code=None, task=None) -> None:
    # ``sys.settrace()`` is local to the current OS thread.  A process-wide
    # lock needlessly serialized independent profilers running in different
    # threads, while still needing to reject overlapping asyncio tasks in the
    # same thread.
    if getattr(_TRACE_STATE, "active", False):
        raise ProfilingBusyError
    _TRACE_STATE.active = True
    _TRACE_STATE.target_code = target_code
    _TRACE_STATE.task = task


def _leave_trace() -> None:
    _TRACE_STATE.active = False
    _TRACE_STATE.target_code = None
    _TRACE_STATE.task = None


def _unwrap_target(func):
    try:
        unwrapped = inspect.unwrap(func)
    except Exception:
        unwrapped = func
    if not hasattr(unwrapped, "__code__") and callable(unwrapped):
        call_method = getattr(unwrapped, "__call__", None)
        if hasattr(call_method, "__code__"):
            return call_method
    return unwrapped


def _callable_name(func) -> str:
    return getattr(func, "__name__", None) or getattr(func, "__qualname__", None) or type(func).__name__


def run_sync(logger, func, *args, **kwargs):
    profile_target = _unwrap_target(func)
    target_code = getattr(profile_target, "__code__", None)
    if getattr(_TRACE_STATE, "active", False):
        if (
            target_code is not None
            and getattr(_TRACE_STATE, "task", None) is None
            and getattr(_TRACE_STATE, "target_code", None) is target_code
        ):
            return func(*args, **kwargs)
        raise RuntimeError(logger._msg("ERR_PROFILING_BUSY"))

    if target_code is None:
        start = perf_counter()
        result = func(*args, **kwargs)
        duration_ms = (perf_counter() - start) * 1000.0
        print_report(logger, profile_target, duration_ms, {}, {})
        return result

    LineProfiler: Any = None
    try:
        from line_profiler import LineProfiler as _LineProfiler  # type: ignore

        LineProfiler = _LineProfiler
    except ImportError:
        pass
    if LineProfiler is not None:
        profiler = LineProfiler()
        _enter_trace(target_code, task=None)
        try:
            start = perf_counter()
            if profile_target is func:
                result = profiler(func)(*args, **kwargs)
            elif hasattr(profiler, "add_function") and hasattr(profiler, "enable_by_count"):
                profiler.add_function(profile_target)
                profiler.enable_by_count()
                try:
                    result = func(*args, **kwargs)
                finally:
                    profiler.disable_by_count()
            else:
                result = profiler(func)(*args, **kwargs)
            duration_ms = (perf_counter() - start) * 1000.0
            line_times, line_hits = _extract_stats(profiler.get_stats(), profile_target)
        finally:
            _leave_trace()
    else:
        try:
            result, duration_ms, line_times, line_hits = _trace_sync(
                func, *args, _target_code=target_code, **kwargs
            )
        except ProfilingBusyError as exc:
            raise RuntimeError(logger._msg("ERR_PROFILING_BUSY")) from exc
    print_report(logger, profile_target, duration_ms, line_times, line_hits)
    return result


def run_async(logger, func, *args, **kwargs):
    return _run_async(logger, func, *args, **kwargs)


async def _run_async(logger, func, *args, **kwargs):
    profile_target = _unwrap_target(func)
    target_code = getattr(profile_target, "__code__", None)
    current_task = asyncio.current_task()
    if getattr(_TRACE_STATE, "active", False):
        if (
            target_code is not None
            and getattr(_TRACE_STATE, "task", None) is current_task
            and getattr(_TRACE_STATE, "target_code", None) is target_code
        ):
            return await func(*args, **kwargs)
        raise RuntimeError(logger._msg("ERR_PROFILING_BUSY"))

    if target_code is None:
        start = perf_counter()
        result = await func(*args, **kwargs)
        duration_ms = (perf_counter() - start) * 1000.0
        print_report(logger, profile_target, duration_ms, {}, {})
        return result

    LineProfiler: Any = None
    try:
        from line_profiler import LineProfiler as _LineProfiler  # type: ignore

        LineProfiler = _LineProfiler
    except ImportError:
        pass

    if LineProfiler is not None:
        profiler = LineProfiler()
        _enter_trace(target_code, task=current_task)
        try:
            start = perf_counter()
            if profile_target is func:
                result = await profiler(func)(*args, **kwargs)
            elif hasattr(profiler, "add_function") and hasattr(profiler, "enable_by_count"):
                profiler.add_function(profile_target)
                profiler.enable_by_count()
                try:
                    result = await func(*args, **kwargs)
                finally:
                    profiler.disable_by_count()
            else:
                result = await profiler(func)(*args, **kwargs)
            duration_ms = (perf_counter() - start) * 1000.0
            line_times, line_hits = _extract_stats(profiler.get_stats(), profile_target)
        finally:
            _leave_trace()
    else:
        try:
            result, duration_ms, line_times, line_hits = await _trace_async(
                func, *args, _current_task=current_task, _target_code=target_code, **kwargs
            )
        except ProfilingBusyError as exc:
            raise RuntimeError(logger._msg("ERR_PROFILING_BUSY")) from exc
    print_report(logger, profile_target, duration_ms, line_times, line_hits)
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
    line_times: Dict[int, float] = defaultdict(float)
    line_hits: Dict[int, int] = defaultdict(int)
    frame_states: Dict[Any, List[Any]] = {}

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


def _trace_sync(func, *args, _target_code=None, **kwargs):
    return_value = None
    code = _target_code if _target_code is not None else func.__code__
    tracer, line_times, line_hits = _build_line_tracer(code)

    _enter_trace(code)
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


async def _trace_async(func, *args, _current_task=None, _target_code=None, **kwargs):
    code = _target_code if _target_code is not None else func.__code__
    tracer, line_times, line_hits = _build_line_tracer(code)

    _enter_trace(code, task=_current_task)
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
    func_name = _callable_name(func)
    d3_logger = logger.logger.opt(depth=3)
    try:
        lines, start_line = inspect.getsourcelines(func)
    except Exception as exc:
        d3_logger.warning(
            logger._msg("CANNOT_GET_SOURCE", func=func_name, error=str(exc))
        )
        return

    safe_func_name = _escape_color_tags(func_name)
    report = ["=" * 80, logger._msg("LINE_PROFILE_REPORT_HEADER", func=safe_func_name), "=" * 80]
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
    slowest_line = max(percentages, key=lambda ln: percentages[ln], default=None)
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
