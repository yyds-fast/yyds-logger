"""
The Loguru library provides a pre-instanced logger to facilitate dealing with logging in Python.

Just ``from loguru import logger``.
"""

import atexit as _atexit
import sys as _sys
import threading as _threading

from . import _defaults
from ._logger import Core as _Core
from ._logger import Logger as _Logger

__all__ = ["logger", "create_logger"]

_default_logger = None
_default_logger_lock = _threading.Lock()
_default_logger_registered = False


def _get_default_logger():
    global _default_logger, _default_logger_registered
    if _default_logger is not None:
        return _default_logger
    with _default_logger_lock:
        if _default_logger is None:
            default_logger = _Logger(
                core=_Core(),
                exception=None,
                depth=0,
                record=False,
                lazy=False,
                colors=False,
                raw=False,
                capture=True,
                patchers=[],
                extra={},
            )
            if _defaults.LOGURU_AUTOINIT and _sys.stderr:
                default_logger.add(_sys.stderr)
            _default_logger = default_logger
            if not _default_logger_registered:
                _atexit.register(_remove_default_logger)
                _default_logger_registered = True
    return _default_logger


def _remove_default_logger():
    default_logger = _default_logger
    if default_logger is not None:
        default_logger.remove()


def __getattr__(name):
    if name == "logger":
        return _get_default_logger()
    raise AttributeError("module %r has no attribute %r" % (__name__, name))


def __dir__():
    return sorted(set(globals()) | {"logger"})


def create_logger(stderr=False, register_atexit=True):
    lg = _Logger(
        core=_Core(),
        exception=None,
        depth=0,
        record=False,
        lazy=False,
        colors=False,
        raw=False,
        capture=True,
        patchers=[],
        extra={},
    )
    if stderr and _sys.stderr:
        lg.add(_sys.stderr)
    if register_atexit:
        _atexit.register(lg.remove)
    return lg
