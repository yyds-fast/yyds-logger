"""Process-level lifecycle hooks for YydsLogger."""

import os
import signal
import sys
import threading


def setup_exception_handler(logger) -> None:
    logger._claim_global_resource("exception_hooks")

    def exception_handler(exc_type, exc_value, exc_traceback):
        if issubclass(exc_type, KeyboardInterrupt):
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
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
            logger.logger.opt(exception=True).error(full_error_msg)
            if len(tb) > 1:
                chain = " -> ".join(
                    f"{frame.filename}:{frame.lineno}:{frame.name}" for frame in tb[-3:]
                )
                logger.logger.error(logger._msg("CALL_CHAIN", chain=chain))
        except Exception:
            logger.logger.opt(exception=True).error(
                f"{logger._msg('UNHANDLED_EXCEPTION')}: {exc_type.__name__}"
            )

    if logger._prev_excepthook is None:
        logger._prev_excepthook = sys.excepthook
    logger._exception_hook = exception_handler
    sys.excepthook = exception_handler
    try:
        if logger._prev_threading_excepthook is None:
            logger._prev_threading_excepthook = threading.excepthook

        def thread_excepthook(args):
            if issubclass(args.exc_type, KeyboardInterrupt):
                return
            try:
                thread_name = getattr(args.thread, "name", "?")
                logger.logger.opt(exception=(args.exc_type, args.exc_value, args.exc_traceback)).error(
                    logger._msg("THREAD_UNHANDLED_EXCEPTION", thread=thread_name,
                                error_type=args.exc_type.__name__)
                )
            except Exception:
                pass

        threading.excepthook = thread_excepthook
    except Exception:
        pass


def setup_signal_handlers(logger) -> None:
    if threading.current_thread() is not threading.main_thread():
        return
    logger._claim_global_resource("signal_handlers")

    def handler(signum, frame):
        previous = logger._prev_signal_handlers.get(signum)
        try:
            logger.cleanup()
        except Exception:
            pass
        if callable(previous):
            previous(signum, frame)
        elif previous == signal.SIG_IGN:
            return
        else:
            signal.signal(signum, signal.SIG_DFL)
            os.kill(os.getpid(), signum)

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            logger._prev_signal_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
        except Exception:
            continue


def restore_signal_handlers(logger) -> None:
    if not logger._prev_signal_handlers:
        return
    try:
        for sig, previous in logger._prev_signal_handlers.items():
            try:
                signal.signal(sig, previous)
            except Exception:
                continue
    except Exception:
        pass
    logger._prev_signal_handlers = {}
