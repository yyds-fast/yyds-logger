import functools
import json
import math
import multiprocessing
import os
import queue
import threading
from contextlib import contextmanager
from multiprocessing.reduction import ForkingPickler
from threading import Thread

from ._colorizer import Colorizer
from ._locks_machinery import create_handler_lock


def prepare_colored_format(format_, ansi_level):
    colored = Colorizer.prepare_format(format_)
    return colored, colored.colorize(ansi_level)


def prepare_stripped_format(format_):
    colored = Colorizer.prepare_format(format_)
    return colored.strip()


def memoize(function):
    return functools.lru_cache(maxsize=64)(function)


class Message(str):
    __slots__ = ("record",)


class Handler:
    def __init__(
        self,
        *,
        sink,
        name,
        levelno,
        formatter,
        is_formatter_dynamic,
        filter_,
        colorize,
        serialize,
        enqueue,
        queue_size,
        overflow_policy,
        queue_timeout,
        queue_backend,
        shutdown_timeout,
        multiprocessing_context,
        error_interceptor,
        exception_formatter,
        id_,
        levels_ansi_codes
    ):
        self._name = name
        self._sink = sink
        self._levelno = levelno
        self._formatter = formatter
        self._is_formatter_dynamic = is_formatter_dynamic
        self._filter = filter_
        self._colorize = colorize
        self._serialize = serialize
        self._enqueue = enqueue
        self._queue_size = None if queue_size is None else max(1, int(queue_size))
        self._overflow_policy = str(overflow_policy).lower()
        self._queue_timeout = queue_timeout
        self._queue_backend = str(queue_backend).lower()
        self._shutdown_timeout = shutdown_timeout
        if self._overflow_policy not in {"block", "drop"}:
            raise ValueError("overflow_policy must be 'block' or 'drop'")
        if self._queue_timeout is not None and float(self._queue_timeout) < 0:
            raise ValueError("queue_timeout must be >= 0")
        if self._queue_backend not in {"multiprocessing", "thread"}:
            raise ValueError("queue_backend must be 'multiprocessing' or 'thread'")
        if self._queue_backend == "thread" and multiprocessing_context is not None:
            raise ValueError("queue_backend='thread' cannot use a multiprocessing context")
        if self._shutdown_timeout is not None and (
            float(self._shutdown_timeout) <= 0
            or not math.isfinite(float(self._shutdown_timeout))
        ):
            raise ValueError("shutdown_timeout must be > 0")
        self._dropped_messages = 0
        self._serialization_errors = 0
        self._multiprocessing_context = multiprocessing_context
        self._error_interceptor = error_interceptor
        self._exception_formatter = exception_formatter
        self._id = id_
        self._levels_ansi_codes = levels_ansi_codes  # Warning, reference shared among handlers

        self._decolorized_format = None
        self._precolorized_formats = {}
        self._memoize_dynamic_format = None

        self._stopped = False
        self._lock = create_handler_lock()
        self._lock_acquired = threading.local()
        self._queue = None
        self._queue_lock = None
        self._confirmation_event = None
        self._confirmation_lock = None
        self._owner_process_pid = None
        self._thread = None

        if self._is_formatter_dynamic:
            if self._colorize:
                self._memoize_dynamic_format = memoize(prepare_colored_format)
            else:
                self._memoize_dynamic_format = memoize(prepare_stripped_format)
        else:
            if self._colorize:
                for level_name in self._levels_ansi_codes:
                    self.update_format(level_name)
            else:
                self._decolorized_format = self._formatter.strip()

        if self._enqueue:
            if self._queue_backend == "thread":
                self._queue = queue.Queue(maxsize=self._queue_size or 0)
                self._confirmation_event = threading.Event()
                self._confirmation_lock = threading.Lock()
            elif self._multiprocessing_context is None:
                self._queue = multiprocessing.Queue(maxsize=self._queue_size or 0)
                self._confirmation_event = multiprocessing.Event()
                self._confirmation_lock = multiprocessing.Lock()
            else:
                self._queue = self._multiprocessing_context.Queue(maxsize=self._queue_size or 0)
                self._confirmation_event = self._multiprocessing_context.Event()
                self._confirmation_lock = self._multiprocessing_context.Lock()
            self._queue_lock = create_handler_lock()
            self._owner_process_pid = os.getpid()
            self._thread = Thread(
                target=self._queued_writer, daemon=True, name="loguru-writer-%d" % self._id
            )
            self._thread.start()

    def __repr__(self):
        return "(id=%d, level=%d, sink=%s)" % (self._id, self._levelno, self._name)

    @contextmanager
    def _protected_lock(self):
        """Acquire the lock, but fail fast if its already acquired by the current thread."""
        if getattr(self._lock_acquired, "acquired", False):
            raise RuntimeError(
                "Could not acquire internal lock because it was already in use (deadlock avoided). "
                "This likely happened because the logger was re-used inside a sink, a signal "
                "handler or a '__del__' method. This is not permitted because the logger and its "
                "handlers are not re-entrant."
            )
        self._lock_acquired.acquired = True
        try:
            with self._lock:
                yield
        finally:
            self._lock_acquired.acquired = False

    def emit(self, record, level_id, from_decorator, is_raw, colored_message):
        try:
            if self._levelno > record["level"].no:
                return

            if self._filter is not None:
                if not self._filter(record):
                    return

            if self._is_formatter_dynamic:
                dynamic_format = self._formatter(record)

            formatter_record = record.copy()

            if not record["exception"]:
                formatter_record["exception"] = ""
            else:
                type_, value, tb = record["exception"]
                formatter = self._exception_formatter
                lines = formatter.format_exception(type_, value, tb, from_decorator=from_decorator)
                formatter_record["exception"] = "".join(lines)

            if colored_message is not None and colored_message.stripped != record["message"]:
                colored_message = None

            if is_raw:
                if colored_message is None or not self._colorize:
                    formatted = record["message"]
                else:
                    ansi_level = self._levels_ansi_codes[level_id]
                    formatted = colored_message.colorize(ansi_level)
            elif self._is_formatter_dynamic:
                if not self._colorize:
                    precomputed_format = self._memoize_dynamic_format(dynamic_format)
                    formatted = precomputed_format.format_map(formatter_record)
                elif colored_message is None:
                    ansi_level = self._levels_ansi_codes[level_id]
                    _, precomputed_format = self._memoize_dynamic_format(dynamic_format, ansi_level)
                    formatted = precomputed_format.format_map(formatter_record)
                else:
                    ansi_level = self._levels_ansi_codes[level_id]
                    formatter, precomputed_format = self._memoize_dynamic_format(
                        dynamic_format, ansi_level
                    )
                    coloring_message = formatter.make_coloring_message(
                        record["message"], ansi_level=ansi_level, colored_message=colored_message
                    )
                    formatter_record["message"] = coloring_message
                    formatted = precomputed_format.format_map(formatter_record)

            else:
                if not self._colorize:
                    precomputed_format = self._decolorized_format
                    formatted = precomputed_format.format_map(formatter_record)
                elif colored_message is None:
                    ansi_level = self._levels_ansi_codes[level_id]
                    precomputed_format = self._precolorized_formats[level_id]
                    formatted = precomputed_format.format_map(formatter_record)
                else:
                    ansi_level = self._levels_ansi_codes[level_id]
                    precomputed_format = self._precolorized_formats[level_id]
                    coloring_message = self._formatter.make_coloring_message(
                        record["message"], ansi_level=ansi_level, colored_message=colored_message
                    )
                    formatter_record["message"] = coloring_message
                    formatted = precomputed_format.format_map(formatter_record)

            if self._serialize:
                formatted = self._serialize_record(formatted, record)

            str_record = Message(formatted)
            str_record.record = record

            serialization_error = None
            with self._protected_lock():
                if self._stopped:
                    return
                if self._enqueue:
                    if self._queue_backend == "multiprocessing":
                        try:
                            # ``multiprocessing.Queue`` normally pickles in its feeder thread.
                            # Validate synchronously so serialization failures are observable and
                            # included in the dropped-message counter instead of being silently
                            # printed by the feeder after ``put()`` has already returned.
                            ForkingPickler.dumps(str_record)
                        except Exception as exc:
                            self._dropped_messages += 1
                            self._serialization_errors += 1
                            serialization_error = exc
                    try:
                        if serialization_error is not None:
                            pass
                        elif self._overflow_policy == "drop":
                            self._queue.put_nowait(str_record)
                        elif self._queue_timeout is None:
                            self._queue.put(str_record)
                        else:
                            self._queue.put(str_record, timeout=float(self._queue_timeout))
                    except Exception as exc:
                        if isinstance(exc, queue.Full):
                            self._dropped_messages += 1
                            return
                        raise
                else:
                    self._sink.write(str_record)
            if serialization_error is not None:
                if not self._error_interceptor.should_catch():
                    raise serialization_error
                self._error_interceptor.print(record, exception=serialization_error)
        except Exception:
            if not self._error_interceptor.should_catch():
                raise
            self._error_interceptor.print(record)

    def stop(self):
        with self._protected_lock():
            self._stopped = True
            if self._enqueue:
                if self._owner_process_pid != os.getpid():
                    return
                self._put_control(None)
                self._thread.join(timeout=self._shutdown_timeout)
                if self._thread.is_alive():
                    raise TimeoutError(
                        "Timed out while stopping Loguru handler #%d" % self._id
                    )
                if hasattr(self._queue, "close"):
                    self._queue.close()

            self._sink.stop()

    def complete_queue(self):
        if not self._enqueue:
            return

        with self._confirmation_lock:
            self._confirmation_event.clear()
            self._put_control(True)
            confirmed = self._confirmation_event.wait(timeout=self._shutdown_timeout)
            self._confirmation_event.clear()
            if not confirmed:
                raise TimeoutError(
                    "Timed out while flushing Loguru handler #%d" % self._id
                )

    def _put_control(self, message):
        try:
            if self._shutdown_timeout is None:
                self._queue.put(message)
            else:
                self._queue.put(message, timeout=float(self._shutdown_timeout))
        except queue.Full as exc:
            raise TimeoutError(
                "Timed out while controlling Loguru handler #%d" % self._id
            ) from exc

    def tasks_to_complete(self):
        if self._enqueue and self._owner_process_pid != os.getpid():
            return []
        lock = self._queue_lock if self._enqueue else self._protected_lock()
        with lock:
            return self._sink.tasks_to_complete()

    @property
    def dropped_messages(self):
        return self._dropped_messages

    @property
    def serialization_errors(self):
        return self._serialization_errors

    def update_format(self, level_id):
        if not self._colorize or self._is_formatter_dynamic:
            return
        ansi_code = self._levels_ansi_codes[level_id]
        self._precolorized_formats[level_id] = self._formatter.colorize(ansi_code)

    @property
    def levelno(self):
        return self._levelno

    @staticmethod
    def _serialize_record(text, record):
        exception = record["exception"]

        if exception is not None:
            exception = {
                "type": None if exception.type is None else exception.type.__name__,
                "value": exception.value,
                "traceback": bool(exception.traceback),
            }

        serializable = {
            "text": text,
            "record": {
                "elapsed": {
                    "repr": record["elapsed"],
                    "seconds": record["elapsed"].total_seconds(),
                },
                "exception": exception,
                "extra": record["extra"],
                "file": {"name": record["file"].name, "path": record["file"].path},
                "function": record["function"],
                "level": {
                    "icon": record["level"].icon,
                    "name": record["level"].name,
                    "no": record["level"].no,
                },
                "line": record["line"],
                "message": record["message"],
                "module": record["module"],
                "name": record["name"],
                "process": {"id": record["process"].id, "name": record["process"].name},
                "thread": {"id": record["thread"].id, "name": record["thread"].name},
                "time": {"repr": record["time"], "timestamp": record["time"].timestamp()},
            },
        }

        return json.dumps(serializable, default=str, ensure_ascii=False) + "\n"

    def _queued_writer(self):
        message = None
        queue = self._queue

        # We need to use a lock to protect sink during fork.
        # Particularly, writing to stderr may lead to deadlock in child process.
        lock = self._queue_lock

        while True:
            try:
                message = queue.get()
            except Exception:
                with lock:
                    self._error_interceptor.print(None)
                continue

            if message is None:
                break

            if message is True:
                self._confirmation_event.set()
                continue

            with lock:
                try:
                    self._sink.write(message)
                except Exception:
                    self._error_interceptor.print(message.record)

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_lock"] = None
        state["_lock_acquired"] = None
        state["_memoize_dynamic_format"] = None
        if self._enqueue:
            state["_sink"] = None
            state["_thread"] = None
            state["_owner_process"] = None
            state["_queue_lock"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = create_handler_lock()
        self._lock_acquired = threading.local()
        if self._enqueue:
            self._queue_lock = create_handler_lock()
        if self._is_formatter_dynamic:
            if self._colorize:
                self._memoize_dynamic_format = memoize(prepare_colored_format)
            else:
                self._memoize_dynamic_format = memoize(prepare_stripped_format)
