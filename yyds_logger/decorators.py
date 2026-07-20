"""Optional function decorators for YydsLogger."""

import inspect
from functools import wraps
from time import perf_counter


def log_decorator(logger, msg=None, level="ERROR", trace=True):
    def decorator(func):
        msg_key = msg or "UNHANDLED_EXCEPTION"
        log_level = level.upper()
        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def async_wrapper(*args, **kwargs):
                logger._log_start(func.__name__, args, kwargs, is_async=True)
                start = perf_counter()
                try:
                    result = await func(*args, **kwargs)
                    logger._log_end(func.__name__, result, perf_counter() - start, is_async=True)
                    return result
                except Exception as exc:
                    logger._log_exception(func.__name__, exc, msg_key, log_level, trace, is_async=True)
                    if trace:
                        raise
                    return None
            return async_wrapper

        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger._log_start(func.__name__, args, kwargs, is_async=False)
            start = perf_counter()
            try:
                result = func(*args, **kwargs)
                logger._log_end(func.__name__, result, perf_counter() - start, is_async=False)
                return result
            except Exception as exc:
                logger._log_exception(func.__name__, exc, msg_key, log_level, trace, is_async=False)
                if trace:
                    raise
                return None
        return sync_wrapper
    return decorator


def time_it(logger, func=None, *, line_by_line=False):
    def decorator(target):
        if inspect.iscoroutinefunction(target):
            @wraps(target)
            async def async_wrapper(*args, **kwargs):
                if line_by_line:
                    from .profiling import run_async
                    return await run_async(logger, target, *args, **kwargs)
                start = perf_counter()
                result = await target(*args, **kwargs)
                _log_duration(logger, target.__name__, (perf_counter() - start) * 1000.0)
                return result
            return async_wrapper

        @wraps(target)
        def sync_wrapper(*args, **kwargs):
            if line_by_line:
                from .profiling import run_sync
                return run_sync(logger, target, *args, **kwargs)
            start = perf_counter()
            result = target(*args, **kwargs)
            _log_duration(logger, target.__name__, (perf_counter() - start) * 1000.0)
            return result
        return sync_wrapper
    return decorator(func) if func is not None else decorator


def _log_duration(logger, function_name, duration_ms):
    if logger._info_level_no >= logger._min_level_no:
        message = logger._msg("TIMER_SUMMARY", func=function_name, duration=f"{duration_ms:.2f}")
        logger.logger.opt(depth=2).info(message)
