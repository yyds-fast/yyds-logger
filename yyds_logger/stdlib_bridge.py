"""Bridge for routing :mod:`logging` records through a YydsLogger instance."""

import inspect
import logging
import os
import sys
import weakref
from typing import List, Optional, Union

from .i18n import get_message
from .yyds_loguru._logger import _get_file_and_module
from .yyds_loguru._recattrs import RecordFile


_STD_LEVEL_ALIASES = {
    "WARN": "WARNING",
    "FATAL": "CRITICAL",
}


def capture_std_logging(logger, level: Union[str, int] = "DEBUG",
                        names: Optional[List[str]] = None,
                        clear_existing: bool = False) -> None:
    """Install an intercept handler and save the target logger state."""
    if logger._std_logging_state is not None:
        raise RuntimeError(get_message(logger.language, "ERR_STD_CAPTURE_DUPLICATE"))
    if isinstance(level, bool):
        raise ValueError(get_message(logger.language, "ERR_STD_LEVEL", level=repr(level)))
    if isinstance(level, int):
        if level < 0:
            raise ValueError(get_message(logger.language, "ERR_STD_LEVEL", level=repr(level)))
        level_no = level
    else:
        level_str = str(level)
        try:
            level_no = logger.logger.level(level_str).no
        except Exception as exc:
            try:
                level_no = logger.logger.level(level_str.strip().upper()).no
            except Exception:
                raise ValueError(get_message(logger.language, "ERR_STD_LEVEL", level=repr(level))) from exc
    if names is not None:
        if not isinstance(names, (list, tuple)) or not all(isinstance(name, str) for name in names):
            raise TypeError(get_message(logger.language, "ERR_STD_NAMES"))
    log_module = logging
    log_file = log_module.__file__
    get_frame = getattr(sys, "_getframe", None)
    logger_ref = weakref.ref(logger)

    class _InterceptHandler(log_module.Handler):
        def emit(self, record: "logging.LogRecord") -> None:
            owner = logger_ref()
            if owner is None or getattr(owner, "_cleanup_state", "open") != "open":
                return
            bound_logger = owner.logger
            core = bound_logger._core
            if not core.handlers:
                return
            level_entry = core.levels_lookup.get(record.levelname)
            if level_entry is None:
                alias = _STD_LEVEL_ALIASES.get(record.levelname)
                if alias is not None:
                    level_entry = core.levels_lookup.get(alias)
            if level_entry is not None:
                level_name = level_entry[1]
                level_no = level_entry[2]
            else:
                level_name = record.levelno
                level_no = record.levelno
            if level_no < core.min_level:
                return
            if get_frame is not None:
                try:
                    frame = get_frame(1)
                    depth = 1
                    while frame is not None and frame.f_code.co_filename == log_file:
                        frame = frame.f_back
                        depth += 1
                except ValueError:
                    depth = 0
            else:
                frame, depth = inspect.currentframe(), 0
                while frame and (depth == 0 or frame.f_code.co_filename == log_file):
                    frame = frame.f_back
                    depth += 1
            target_logger = bound_logger
            if record.pathname and record.pathname != "(unknown file)" and record.lineno:
                cached_file, _ = _get_file_and_module(record.pathname)
                rec_file = (
                    cached_file
                    if not record.filename or record.filename == cached_file.name
                    else RecordFile(record.filename or os.path.basename(record.pathname), record.pathname)
                )
                rec_line = record.lineno
                rec_func = record.funcName or "<unknown>"
                rec_mod = record.module

                def _apply_std_location(log_record) -> None:
                    log_record["file"] = rec_file
                    log_record["line"] = rec_line
                    log_record["function"] = rec_func
                    if rec_mod:
                        log_record["module"] = rec_mod

                target_logger = bound_logger.patch(_apply_std_location)
            target_logger.opt(depth=depth, exception=record.exc_info).log(
                level_name, record.getMessage()
            )

    handler = _InterceptHandler()
    raw_targets = [log_module.getLogger(n) for n in names] if names else [log_module.getLogger()]
    # The same logger may be named more than once (including aliases for the
    # root logger). Saving it twice used to restore the intermediate state and
    # leave our intercept handler installed after cleanup.
    targets = list({id(target): target for target in raw_targets}.values())
    logger._claim_global_resource("stdlib_logging")
    saved = []
    try:
        for target in targets:
            saved.append((target, list(target.handlers), target.level, target.propagate))
            if clear_existing:
                target.handlers = [handler]
            else:
                target.addHandler(handler)
            target.setLevel(level_no)
            if names:
                target.propagate = False
    except Exception:
        for target, handlers, old_level, propagate in saved:
            target.handlers = handlers
            target.setLevel(old_level)
            target.propagate = propagate
        logger._release_global_resource("stdlib_logging")
        raise
    state = {
        "targets": saved,
        "handler": handler,
        "clear_existing": bool(clear_existing),
        "level": level_no,
        "named": bool(names),
    }
    logger._std_logging_state = state
    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        finalizer_state["std_logging_state"] = state


def restore_std_logging_state(state: Optional[dict], *, strict: bool = True) -> Optional[dict]:
    """Restore standard logging targets from a saved state dictionary."""
    if not state:
        return None
    targets = state["targets"]
    intercept_handler = state["handler"]
    clear_existing = state["clear_existing"]
    captured_level = state["level"]
    named = state["named"]

    failed_targets = []
    first_error = None
    for target, handlers, level, propagate in targets:
        try:
            current_handlers = list(target.handlers)
            additions = [
                item for item in current_handlers
                if item is not intercept_handler and item not in handlers
            ]
            if clear_existing:
                target.handlers = handlers + additions
            else:
                # Existing handlers were never removed. Preserve any runtime
                # additions/removals and only take out our own.
                target.handlers = [
                    item for item in current_handlers if item is not intercept_handler
                ]
            if target.level == captured_level:
                target.setLevel(level)
            if named and target.propagate is False:
                target.propagate = propagate
        except Exception as exc:
            failed_targets.append((target, handlers, level, propagate))
            if first_error is None:
                first_error = exc

    if failed_targets:
        state["targets"] = failed_targets
        if strict:
            raise RuntimeError("Failed to restore standard logging state") from first_error
        return state
    return None


def restore_std_logging(logger) -> None:
    """Restore logging handlers and levels changed by ``capture_std_logging``."""
    state = logger._std_logging_state
    if not state:
        return
    try:
        remaining = restore_std_logging_state(state, strict=True)
    except Exception:
        logger._std_logging_state = state
        finalizer_state = getattr(logger, "_finalizer_state", None)
        if isinstance(finalizer_state, dict):
            finalizer_state["std_logging_state"] = state
        raise
    logger._std_logging_state = remaining
    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        finalizer_state["std_logging_state"] = remaining
