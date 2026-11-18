"""Process-level lifecycle hooks for YydsLogger."""

import os
import signal
import sys
import threading
import traceback
import weakref


def setup_exception_handler(logger) -> None:
    # Reconfiguration must not wrap our own hooks repeatedly.  Apart from
    # duplicate log records, repeated wrapping retained an ever-growing chain
    # of closures until cleanup.
    if logger._exception_hook is not None:
        return

    logger._claim_global_resource("exception_hooks")
    previous_excepthook = sys.excepthook
    previous_threading_hook = getattr(threading, "excepthook", None)
    default_threading_hook = getattr(threading, "__excepthook__", None)
    logger_ref = weakref.ref(logger)

    def call_previous(previous, default, *args) -> None:
        if not callable(previous) or previous is default:
            return
        try:
            previous(*args)
        except Exception:
            pass

    def exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            previous = previous_excepthook if callable(previous_excepthook) else sys.__excepthook__
            try:
                previous(exc_type, exc_value, exc_traceback)
            except Exception:
                pass
            return
        owner = logger_ref()
        if owner is None or getattr(owner, "_cleanup_state", "open") != "open":
            call_previous(
                previous_excepthook,
                sys.__excepthook__,
                exc_type,
                exc_value,
                exc_traceback,
            )
            return
        try:
            tb = traceback.extract_tb(exc_traceback)
            error_msg = owner._msg("UNHANDLED_EXCEPTION")
            exc_value_str = str(exc_value) if exc_value is not None else "None"
            if tb:
                last_frame = tb[-1]
                error_location = f"{last_frame.filename}:{last_frame.lineno}:{last_frame.name}"
                line_content = last_frame.line.strip() if last_frame.line else owner._msg("UNKNOWN_CODE_LINE")
            else:
                error_location = owner._msg("UNKNOWN_LOCATION")
                line_content = owner._msg("UNKNOWN_CODE_LINE")
            full_error_msg = (
                f"{error_msg}: {exc_type.__name__}: {exc_value_str} | "
                f"{owner._msg('LABEL_LOCATION')}: {error_location} | "
                f"{owner._msg('LABEL_CODE')}: {line_content}"
            )
            owner.logger.opt(exception=(exc_type, exc_value, exc_traceback)).error(full_error_msg)
            if len(tb) > 1:
                chain = " -> ".join(
                    f"{frame.filename}:{frame.lineno}:{frame.name}" for frame in tb[-3:]
                )
                owner.logger.error(owner._msg("CALL_CHAIN", chain=chain))
        except Exception:
            try:
                owner.logger.error(
                    f"{owner._msg('UNHANDLED_EXCEPTION')}: {exc_type.__name__}"
                )
            except Exception:
                pass
        call_previous(
            previous_excepthook,
            sys.__excepthook__,
            exc_type,
            exc_value,
            exc_traceback,
        )

    def thread_excepthook(args):
        if issubclass(args.exc_type, KeyboardInterrupt):
            previous = (
                previous_threading_hook
                if callable(previous_threading_hook)
                else default_threading_hook
            )
            try:
                previous(args)
            except Exception:
                pass
            return
        owner = logger_ref()
        if owner is not None and getattr(owner, "_cleanup_state", "open") == "open":
            try:
                thread_name = getattr(args.thread, "name", "?")
                owner.logger.opt(exception=(args.exc_type, args.exc_value, args.exc_traceback)).error(
                    owner._msg(
                        "THREAD_UNHANDLED_EXCEPTION",
                        thread=thread_name,
                        error_type=args.exc_type.__name__,
                    )
                )
            except Exception:
                pass
        call_previous(previous_threading_hook, default_threading_hook, args)

    try:
        sys.excepthook = exception_handler
        threading.excepthook = thread_excepthook
    except Exception:
        if sys.excepthook is exception_handler:
            sys.excepthook = previous_excepthook
        if getattr(threading, "excepthook", None) is thread_excepthook:
            threading.excepthook = previous_threading_hook
        logger._release_global_resource("exception_hooks")
        raise

    logger._prev_excepthook = previous_excepthook
    logger._prev_threading_excepthook = previous_threading_hook
    logger._exception_hook = exception_handler
    logger._threading_exception_hook = thread_excepthook
    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        finalizer_state["exception_hook_state"] = {
            "exception_hook": exception_handler,
            "prev_excepthook": previous_excepthook,
            "threading_exception_hook": thread_excepthook,
            "prev_threading_excepthook": previous_threading_hook,
        }


def restore_exception_hook_state(state) -> None:
    """Restore process exception hooks from a saved state dictionary."""
    if not state:
        return
    exc_hook = state.get("exception_hook")
    prev_exc = state.get("prev_excepthook")
    thr_hook = state.get("threading_exception_hook")
    prev_thr = state.get("prev_threading_excepthook")
    if exc_hook is not None and sys.excepthook is exc_hook:
        sys.excepthook = prev_exc or sys.__excepthook__
    if thr_hook is not None and getattr(threading, "excepthook", None) is thr_hook:
        threading.excepthook = (
            prev_thr or getattr(threading, "__excepthook__", threading.excepthook)
        )


def restore_exception_handler(logger) -> None:
    """Restore hooks only while they are still owned by this logger."""
    restore_exception_hook_state(
        {
            "exception_hook": logger._exception_hook,
            "prev_excepthook": logger._prev_excepthook,
            "threading_exception_hook": logger._threading_exception_hook,
            "prev_threading_excepthook": logger._prev_threading_excepthook,
        }
    )
    logger._exception_hook = None
    logger._threading_exception_hook = None
    logger._prev_excepthook = None
    logger._prev_threading_excepthook = None
    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        finalizer_state["exception_hook_state"] = None


def _start_signal_cleanup_worker(logger) -> None:
    """Start/restart the process-local worker used by signal notifications."""
    existing = getattr(logger, "_signal_cleanup_thread", None)
    if existing is not None and existing.is_alive():
        return

    cleanup_event = threading.Event()
    cleanup_stop = threading.Event()
    logger._signal_cleanup_event = cleanup_event
    logger._signal_cleanup_stop = cleanup_stop
    logger._signal_cleanup_default_signal = None
    logger._signal_cleanup_error = None
    logger_ref = weakref.ref(logger)
    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        sig_state = finalizer_state.get("signal_state")
        if isinstance(sig_state, dict):
            sig_state["event"] = cleanup_event
            sig_state["stop"] = cleanup_stop

    def cleanup_worker() -> None:
        while True:
            cleanup_event.wait()
            cleanup_event.clear()
            if cleanup_stop.is_set():
                return
            owner = logger_ref()
            if owner is None:
                return
            try:
                owner.cleanup()
            except BaseException as exc:
                # ``cleanup()`` records its state and remains retryable. Keep
                # the exception available for diagnostics without allowing a
                # daemon-thread traceback to terminate the application.
                owner._signal_cleanup_error = exc

            # Defer the default action until queues have had a chance to
            # drain. A ``None`` frame denotes a direct/manual hook call; do
            # not terminate the hosting process in that case.
            pending_signal = owner._signal_cleanup_default_signal
            owner._signal_cleanup_default_signal = None
            if pending_signal is not None:
                try:
                    os.kill(os.getpid(), pending_signal)
                except OSError:
                    pass
                return

            if cleanup_stop.is_set():
                return

    cleanup_thread = threading.Thread(
        target=cleanup_worker,
        daemon=True,
        name="yyds-logger-signal-cleanup",
    )
    logger._signal_cleanup_thread = cleanup_thread
    try:
        cleanup_thread.start()
    except Exception:
        logger._signal_cleanup_thread = None
        raise


def setup_signal_handlers(logger) -> None:
    if threading.current_thread() is not threading.main_thread():
        return
    if logger._signal_handlers:
        return
    logger._claim_global_resource("signal_handlers")

    # Signal handlers run on the interpreter's main thread and must stay
    # minimal: queue locks, file I/O and user callbacks are unsafe there.
    # Start a daemon worker ahead of time and let the actual handler only set
    # an Event. The worker performs the existing bounded cleanup path.
    try:
        _start_signal_cleanup_worker(logger)
    except Exception:
        logger._release_global_resource("signal_handlers")
        raise

    logger_ref = weakref.ref(logger)
    sig_state = {
        "event": logger._signal_cleanup_event,
        "stop": logger._signal_cleanup_stop,
        "prev_handlers": logger._prev_signal_handlers,
        "installed_handlers": logger._signal_handlers,
    }

    def handler(signum, frame):
        owner = logger_ref()
        prev_map = owner._prev_signal_handlers if owner is not None else sig_state["prev_handlers"]
        inst_map = owner._signal_handlers if owner is not None else sig_state["installed_handlers"]
        previous = prev_map.get(signum)
        if previous == signal.SIG_IGN:
            return
        # ``signal.signal()`` is only legal from the main thread. Restore all
        # handlers here before handing cleanup to the worker, otherwise the
        # worker's normal cleanup path could never release this resource.
        remaining_previous = {}
        remaining_installed = {}
        for installed_signal, previous_handler in list(prev_map.items()):
            installed = inst_map.get(installed_signal)
            try:
                if installed is not None and signal.getsignal(installed_signal) is installed:
                    signal.signal(installed_signal, previous_handler)
            except Exception:
                remaining_previous[installed_signal] = previous_handler
                if installed is not None:
                    remaining_installed[installed_signal] = installed
        sig_state["prev_handlers"] = remaining_previous
        sig_state["installed_handlers"] = remaining_installed
        if owner is not None:
            owner._prev_signal_handlers = remaining_previous
            owner._signal_handlers = remaining_installed
            if previous == signal.SIG_DFL and frame is not None:
                owner._signal_cleanup_default_signal = signum
            try:
                if callable(previous):
                    try:
                        previous(signum, frame)
                    except Exception:
                        pass
            finally:
                owner._signal_cleanup_event.set()
            return

        if callable(previous):
            try:
                previous(signum, frame)
            except Exception:
                pass
        elif previous == signal.SIG_DFL and frame is not None:
            try:
                os.kill(os.getpid(), signum)
            except OSError:
                pass

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            logger._prev_signal_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
            logger._signal_handlers[sig] = handler
        except Exception:
            logger._prev_signal_handlers.pop(sig, None)
            continue
    if not logger._signal_handlers:
        logger._signal_cleanup_stop.set()
        logger._signal_cleanup_event.set()
        logger._signal_cleanup_thread = None
        logger._release_global_resource("signal_handlers")
        return

    finalizer_state = getattr(logger, "_finalizer_state", None)
    if isinstance(finalizer_state, dict):
        finalizer_state["signal_state"] = sig_state


def restore_signal_handler_state(sig_state, *, strict: bool = True) -> None:
    """Stop the signal cleanup worker and restore signal handlers when on main thread."""
    if not sig_state:
        return
    cleanup_stop = sig_state.get("stop")
    cleanup_event = sig_state.get("event")
    if cleanup_stop is not None:
        cleanup_stop.set()
    if cleanup_event is not None:
        cleanup_event.set()

    prev_map = sig_state.get("prev_handlers") or {}
    inst_map = sig_state.get("installed_handlers") or {}
    if not prev_map or threading.current_thread() is not threading.main_thread():
        return

    remaining_previous = {}
    remaining_installed = {}
    first_error = None
    for sig, previous in list(prev_map.items()):
        installed = inst_map.get(sig)
        try:
            if installed is not None and signal.getsignal(sig) is installed:
                signal.signal(sig, previous)
        except Exception as exc:
            remaining_previous[sig] = previous
            if installed is not None:
                remaining_installed[sig] = installed
            if first_error is None:
                first_error = exc
    sig_state["prev_handlers"] = remaining_previous
    sig_state["installed_handlers"] = remaining_installed
    if strict and first_error is not None:
        raise RuntimeError("Failed to restore signal handlers") from first_error


def restore_signal_handlers(logger) -> None:
    cleanup_stop = getattr(logger, "_signal_cleanup_stop", None)
    cleanup_event = getattr(logger, "_signal_cleanup_event", None)
    if cleanup_stop is not None:
        cleanup_stop.set()
    if cleanup_event is not None:
        # Wake a worker which is waiting for its first notification. Do not
        # join it here: when the worker itself owns cleanup, joining would
        # deadlock, and it is daemonized by design.
        cleanup_event.set()

    finalizer_state = getattr(logger, "_finalizer_state", None)
    if not logger._prev_signal_handlers:
        if isinstance(finalizer_state, dict):
            finalizer_state["signal_state"] = None
        return
    remaining_previous = {}
    remaining_installed = {}
    first_error = None
    for sig, previous in list(logger._prev_signal_handlers.items()):
        installed = logger._signal_handlers.get(sig)
        try:
            # Do not overwrite a handler installed by the application after us.
            if installed is not None and signal.getsignal(sig) is installed:
                signal.signal(sig, previous)
        except Exception as exc:
            remaining_previous[sig] = previous
            if installed is not None:
                remaining_installed[sig] = installed
            if first_error is None:
                first_error = exc
    logger._prev_signal_handlers = remaining_previous
    logger._signal_handlers = remaining_installed
    if isinstance(finalizer_state, dict):
        if remaining_previous:
            sig_state = finalizer_state.get("signal_state")
            if isinstance(sig_state, dict):
                sig_state["prev_handlers"] = remaining_previous
                sig_state["installed_handlers"] = remaining_installed
        else:
            finalizer_state["signal_state"] = None
    if first_error is not None:
        raise RuntimeError("Failed to restore signal handlers") from first_error
