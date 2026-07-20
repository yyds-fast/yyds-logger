"""Bridge for routing :mod:`logging` records through a YydsLogger instance."""

import inspect
import logging
import sys
from typing import List, Optional

from .i18n import get_message


def capture_std_logging(logger, level: str = "DEBUG",
                        names: Optional[List[str]] = None,
                        clear_existing: bool = True) -> None:
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
    logger._claim_global_resource("stdlib_logging")
    log_module = logging
    bound_logger = logger.logger

    class _InterceptHandler(log_module.Handler):
        def emit(self, record: "logging.LogRecord") -> None:
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
    targets = [log_module.getLogger(n) for n in names] if names else [log_module.getLogger()]
    saved = []
    for target in targets:
        saved.append((target, list(target.handlers), target.level, target.propagate))
        if clear_existing:
            target.handlers = [handler]
        else:
            target.addHandler(handler)
        target.setLevel(level_no)
        if names:
            target.propagate = False
    logger._std_logging_state = saved


def restore_std_logging(logger) -> None:
    """Restore logging handlers and levels changed by ``capture_std_logging``."""
    state = logger._std_logging_state
    if not state:
        return
    for target, handlers, level, propagate in state:
        try:
            target.handlers = handlers
            target.setLevel(level)
            target.propagate = propagate
        except Exception:
            continue
    logger._std_logging_state = None
