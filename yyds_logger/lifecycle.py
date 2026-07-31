"""Process-level lifecycle hooks for YydsLogger."""

import os
import signal
import sys
import threading


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
        try:
            import traceback
            tb = traceback.extract_tb(exc_traceback)
            error_msg = logger._msg("UNHANDLED_EXCEPTION")
            exc_value_str = str(exc_value) if exc_value is not None else "None"
            if tb:
                last_frame = tb[-1]
                error_location = f"{last_frame.filename}:{last_frame.lineno}:{last_frame.name}"
                line_content = last_frame.line.strip() if last_frame.line else logger._msg("UNKNOWN_CODE_LINE")
            else:
                error_location = logger._msg("UNKNOWN_LOCATION")
                line_content = logger._msg("UNKNOWN_CODE_LINE")
            full_error_msg = (
                f"{error_msg}: {exc_type.__name__}: {exc_value_str} | "
                f"{logger._msg('LABEL_LOCATION')}: {error_location} | "
                f"{logger._msg('LABEL_CODE')}: {line_content}"
            )
            logger.logger.opt(exception=(exc_type, exc_value, exc_traceback)).error(full_error_msg)
            if len(tb) > 1:
                chain = " -> ".join(
                    f"{frame.filename}:{frame.lineno}:{frame.name}" for frame in tb[-3:]
                )
                logger.logger.error(logger._msg("CALL_CHAIN", chain=chain))
        except Exception:
            try:
                logger.logger.error(
                    f"{logger._msg('UNHANDLED_EXCEPTION')}: {exc_type.__name__}"
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
        try:
            thread_name = getattr(args.thread, "name", "?")
            logger.logger.opt(exception=(args.exc_type, args.exc_value, args.exc_traceback)).error(
                logger._msg("THREAD_UNHANDLED_EXCEPTION", thread=thread_name,
                            error_type=args.exc_type.__name__)
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


def restore_exception_handler(logger) -> None:
    """Restore hooks only while they are still owned by this logger."""
    if logger._exception_hook is not None and sys.excepthook is logger._exception_hook:
        sys.excepthook = logger._prev_excepthook or sys.__excepthook__
    if (
        logger._threading_exception_hook is not None
        and getattr(threading, "excepthook", None) is logger._threading_exception_hook
    ):
        threading.excepthook = (
            logger._prev_threading_excepthook
            or getattr(threading, "__excepthook__", threading.excepthook)
        )
    logger._exception_hook = None
    logger._threading_exception_hook = None
    logger._prev_excepthook = None
    logger._prev_threading_excepthook = None


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

    def cleanup_worker() -> None:
        while True:
            cleanup_event.wait()
            cleanup_event.clear()
            if cleanup_stop.is_set():
                return
            try:
                logger.cleanup()
            except BaseException as exc:
                # ``cleanup()`` records its state and remains retryable. Keep
                # the exception available for diagnostics without allowing a
                # daemon-thread traceback to terminate the application.
                logger._signal_cleanup_error = exc

            # Defer the default action until queues have had a chance to
            # drain. A ``None`` frame denotes a direct/manual hook call; do
            # not terminate the hosting process in that case.
            pending_signal = logger._signal_cleanup_default_signal
            logger._signal_cleanup_default_signal = None
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

    def handler(signum, frame):
        previous = logger._prev_signal_handlers.get(signum)
        # ``signal.signal()`` is only legal from the main thread. Restore all
        # handlers here before handing cleanup to the worker, otherwise the
        # worker's normal cleanup path could never release this resource.
        remaining_previous = {}
        remaining_installed = {}
        for installed_signal, previous_handler in list(
            logger._prev_signal_handlers.items()
        ):
            installed = logger._signal_handlers.get(installed_signal)
            try:
                if installed is not None and signal.getsignal(installed_signal) is installed:
                    signal.signal(installed_signal, previous_handler)
            except Exception:
                remaining_previous[installed_signal] = previous_handler
                if installed is not None:
                    remaining_installed[installed_signal] = installed
        logger._prev_signal_handlers = remaining_previous
        logger._signal_handlers = remaining_installed
        if previous == signal.SIG_DFL and frame is not None:
            logger._signal_cleanup_default_signal = signum
        logger._signal_cleanup_event.set()
        if callable(previous):
            try:
                previous(signum, frame)
            except Exception:
                pass
        elif previous == signal.SIG_IGN:
            return
        # The default action is re-issued by the worker after cleanup. This
        # keeps SIGTERM/SIGINT graceful while preserving normal termination.

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

    if not logger._prev_signal_handlers:
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
    if first_error is not None:
        raise RuntimeError("Failed to restore signal handlers") from first_error
