"""Bridge for routing :mod:`logging` records through a YydsLogger instance."""

import inspect
import logging
import sys
import weakref
from typing import List, Optional

from .i18n import get_message


def capture_std_logging(logger, level: str = "DEBUG",
                        names: Optional[List[str]] = None,
                        clear_existing: bool = False) -> None:
    """Install an intercept handler and save the target logger state."""
    if logger._std_logging_state is not None:
        raise RuntimeError(get_message(logger.language, "ERR_STD_CAPTURE_DUPLICATE"))
    try:
        level_no = logger.logger.level(str(level)).no
    except Exception as exc:
        raise ValueError(get_message(logger.language, "ERR_STD_LEVEL", level=repr(level))) from exc
    if names is not None:
        if not isinstance(names, (list, tuple)) or not all(isinstance(name, str) for name in names):
            raise TypeError(get_message(logger.language, "ERR_STD_NAMES"))
    log_module = logging
    logger_ref = weakref.ref(logger)

    class _InterceptHandler(log_module.Handler):
        def emit(self, record: "logging.LogRecord") -> None:
            owner = logger_ref()
            if owner is None:
                return
            bound_logger = owner.logger
            try:
                level_name = bound_logger.level(record.levelname).name
            except (ValueError, AttributeError):
                level_name = record.levelno
            try:
                frame = sys._getframe(6)
                depth = 6
                while frame and frame.f_code.co_filename == log_module.__file__:
                    frame = frame.f_back
                    depth += 1
            except (ValueError, AttributeError):
                frame, depth = inspect.currentframe(), 0
                while frame and (depth == 0 or frame.f_code.co_filename == log_module.__file__):
                    frame = frame.f_back
                    depth += 1
            bound_logger.opt(depth=depth, exception=record.exc_info).log(
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
    logger._std_logging_state = {
        "targets": saved,
        "handler": handler,
        "clear_existing": bool(clear_existing),
        "level": level_no,
        "named": bool(names),
    }


def restore_std_logging(logger) -> None:
    """Restore logging handlers and levels changed by ``capture_std_logging``."""
    state = logger._std_logging_state
    if not state:
        return
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
            if not named or target.propagate is False:
                target.propagate = propagate
        except Exception as exc:
            failed_targets.append((target, handlers, level, propagate))
            if first_error is None:
                first_error = exc

    if failed_targets:
        state["targets"] = failed_targets
        logger._std_logging_state = state
        raise RuntimeError("Failed to restore standard logging state") from first_error
    logger._std_logging_state = None
