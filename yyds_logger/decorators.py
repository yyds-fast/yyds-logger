"""Optional function decorators for YydsLogger."""

import inspect
from functools import wraps
from time import perf_counter


def _callable_name(obj) -> str:
    return getattr(obj, "__name__", None) or getattr(obj, "__qualname__", None) or type(obj).__name__


def _is_async_callable(obj) -> bool:
    return inspect.iscoroutinefunction(obj) or inspect.iscoroutinefunction(
        getattr(obj, "__call__", None)
    )


def log_decorator(logger, msg=None, level="ERROR", trace=True, reraise=True):
    """Log function calls and exceptions.

    ``trace`` controls detailed exception logging while ``reraise`` controls
    whether a caught exception propagates.
    """
    def decorator(func):
        msg_key = msg or "UNHANDLED_EXCEPTION"
        log_level = str(level)
        func_name = _callable_name(func)
        if _is_async_callable(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                logger._log_start(func_name, args, kwargs, is_async=True)
                start = perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    logger._log_end(func_name, result, perf_counter() - start, is_async=True)
                    return result
                except Exception as exc:
                    logger._log_exception(func_name, exc, msg_key, log_level, trace, is_async=True)
                    if reraise:
                        raise
                    return None
            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger._log_start(func_name, args, kwargs, is_async=False)
            start = perf_counter()
            try:
                result = func(*args, **kwargs)
                logger._log_end(func_name, result, perf_counter() - start, is_async=False)
                return result
            except Exception as exc:
                logger._log_exception(func_name, exc, msg_key, log_level, trace, is_async=False)
                if reraise:
                    raise
                return None
        return sync_wrapper
    return decorator


def time_it(logger, func=None, *, line_by_line=False):
    def decorator(target):
        target_name = _callable_name(target)
        if _is_async_callable(target):
            @wraps(target)
            async def async_wrapper(*args, **kwargs):
                if line_by_line:
                    from .profiling import run_async
                    return await run_async(logger, target, *args, **kwargs)
                start = perf_counter()
                result = await target(*args, **kwargs)
                _log_duration(logger, target_name, (perf_counter() - start) * 1000.0)
                return result
            return async_wrapper

        @wraps(target)
        def sync_wrapper(*args, **kwargs):
            if line_by_line:
                from .profiling import run_sync
                return run_sync(logger, target, *args, **kwargs)
            start = perf_counter()
            result = target(*args, **kwargs)
            _log_duration(logger, target_name, (perf_counter() - start) * 1000.0)
            return result
        return sync_wrapper
    return decorator(func) if func is not None else decorator


def _log_duration(logger, function_name, duration_ms):
    if getattr(logger, "_cleanup_state", "open") != "open":
        return
    core = logger.logger._core
    if not core.handlers or logger._info_level_no < core.min_level:
        return
    message = logger._msg("TIMER_SUMMARY", func=function_name, duration=f"{duration_ms:.2f}")
    target_logger = getattr(logger, "_logger_d2", None)
    if target_logger is None:
        target_logger = logger.logger.opt(depth=2)
    target_logger.info(message)
