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
from time import monotonic

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


class DeferredMessage:
    """Record envelope which is formatted by the queue writer.

    This is intentionally only used with the in-process thread queue.  Keeping
    the record and the pre-parsed color message together preserves the normal
    Loguru semantics while moving the expensive format/JSON/exception work out
    of the caller's logging thread.
    """

    __slots__ = ("record", "level_id", "from_decorator", "is_raw", "colored_message")

    def __init__(self, record, level_id, from_decorator, is_raw, colored_message):
        self.record = record
        self.level_id = level_id
        self.from_decorator = from_decorator
        self.is_raw = is_raw
        self.colored_message = colored_message


_DEFAULT_TIMEOUT = object()


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
        defer_format,
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
        self._defer_format = bool(defer_format)
        self._enqueue = enqueue
        if queue_size is not None and (
            isinstance(queue_size, bool)
            or not isinstance(queue_size, int)
            or queue_size <= 0
        ):
            raise TypeError("queue_size must be a positive integer or None")
        self._queue_size = queue_size
        self._overflow_policy = str(overflow_policy).lower()
        self._queue_timeout = queue_timeout
        self._queue_backend = str(queue_backend).lower()
        self._shutdown_timeout = shutdown_timeout
        if self._overflow_policy not in {"block", "drop"}:
            raise ValueError("overflow_policy must be 'block' or 'drop'")
        if self._queue_timeout is not None and (
            isinstance(self._queue_timeout, bool)
            or not isinstance(self._queue_timeout, (int, float))
            or self._queue_timeout < 0
            or not math.isfinite(float(self._queue_timeout))
        ):
            raise ValueError("queue_timeout must be a finite non-negative number")
        if self._queue_backend not in {"multiprocessing", "thread"}:
            raise ValueError("queue_backend must be 'multiprocessing' or 'thread'")
        if self._queue_backend == "thread" and multiprocessing_context is not None:
            raise ValueError("queue_backend='thread' cannot use a multiprocessing context")
        if self._defer_format and (not self._enqueue or self._queue_backend != "thread"):
            raise ValueError("defer_format requires enqueue=True and queue_backend='thread'")
        if self._shutdown_timeout is not None and (
            isinstance(self._shutdown_timeout, bool)
            or not isinstance(self._shutdown_timeout, (int, float))
            or float(self._shutdown_timeout) <= 0
            or not math.isfinite(float(self._shutdown_timeout))
        ):
            raise ValueError("shutdown_timeout must be > 0")
        self._dropped_messages = 0
        self._serialization_errors = 0
        self._drop_reasons = {
            "overflow": 0,
            "block_timeout": 0,
            "serialization": 0,
        }
        self._counter_lock = threading.Lock()
        self._sink_errors = 0
        self._metrics_collected = False
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
        self._queue_stop_sent = False
        self._queue_close_called = False
        self._queue_join_started = False
        self._queue_join_event = threading.Event()
        self._queue_join_error = None
        self._queue_join_lock = threading.Lock()
        self._queue_closed = False
        self._sink_stop_started = False
        self._sink_stop_event = threading.Event()
        self._sink_stop_error = None
        self._sink_stop_lock = threading.Lock()
        self._last_error = None

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

            if self._defer_format:
                # A shallow copy freezes the record fields assembled by
                # Logger._log while avoiding an expensive format/JSON pass in
                # the producer thread.  The thread backend is required so no
                # arbitrary ``extra`` value has to cross a process boundary.
                queued_record = record.copy()
                # ``extra`` is the only mutable container created for every
                # record. Copy the mapping as well so later handlers or a
                # caller retaining a patcher reference cannot change which
                # keys the writer observes after this emit returns.
                queued_record["extra"] = record["extra"].copy()
                queued_message = DeferredMessage(
                    queued_record,
                    level_id,
                    from_decorator,
                    is_raw,
                    colored_message,
                )
            else:
                queued_message = self._format_message(
                    record,
                    level_id,
                    from_decorator,
                    is_raw,
                    colored_message,
                )

            self._enqueue_or_write(queued_message, record)
        except Exception as exc:
            self._last_error = exc
            if not self._error_interceptor.should_catch():
                raise
            self._error_interceptor.print(record)

    def _format_message(self, record, level_id, from_decorator, is_raw, colored_message=None):
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
        return str_record

    def _enqueue_or_write(self, message, record):
        serialization_error = None
        with self._protected_lock():
            if self._stopped:
                return
            if self._enqueue:
                if self._queue_backend == "multiprocessing":
                    try:
                        # ``multiprocessing.Queue`` normally pickles in its
                        # feeder thread.  Validate synchronously so failures
                        # remain visible to callers and metrics.
                        ForkingPickler.dumps(message)
                    except Exception as exc:
                        self._record_drop("serialization")
                        self._last_error = exc
                        serialization_error = exc
                if serialization_error is None:
                    try:
                        if self._overflow_policy == "drop":
                            self._queue.put_nowait(message)
                        elif self._queue_timeout is None:
                            self._queue.put(message)
                        else:
                            self._queue.put(message, timeout=float(self._queue_timeout))
                    except Exception as exc:
                        if isinstance(exc, queue.Full):
                            self._record_drop(
                                "overflow" if self._overflow_policy == "drop" else "block_timeout"
                            )
                            return
                        self._last_error = exc
                        raise
            else:
                try:
                    self._sink.write(message)
                except Exception as exc:
                    self._sink_errors += 1
                    self._last_error = exc
                    raise
        if serialization_error is not None:
            if not self._error_interceptor.should_catch():
                raise serialization_error
            self._error_interceptor.print(record, exception=serialization_error)

    def _record_drop(self, reason):
        with self._counter_lock:
            self._dropped_messages += 1
            self._drop_reasons[reason] = self._drop_reasons.get(reason, 0) + 1
            if reason == "serialization":
                self._serialization_errors += 1

    def stop(self, timeout=_DEFAULT_TIMEOUT):
        """Stop this handler, retrying safely after a bounded timeout.

        ``timeout`` is an optional *remaining* deadline supplied by the owning
        logger.  Keeping the stop marker and sink-stop task state on the
        handler makes a later cleanup retry idempotent instead of enqueueing a
        second marker or invoking a user sink twice.
        """
        effective_timeout = (
            self._shutdown_timeout if timeout is _DEFAULT_TIMEOUT else timeout
        )
        deadline = (
            None
            if effective_timeout is None
            else monotonic() + max(0.0, float(effective_timeout))
        )
        with self._protected_lock():
            self._stopped = True
            if self._enqueue:
                if self._owner_process_pid != os.getpid():
                    return
                if not self._queue_stop_sent:
                    self._put_control(None, timeout=self._remaining(deadline))
                    self._queue_stop_sent = True
                writer = self._thread
            else:
                writer = None

        if writer is not None:
            self._wait_thread(writer, self._remaining(deadline), "stopping")
            self._close_queue(self._remaining(deadline))

        self._stop_sink(self._remaining(deadline))

    def complete_queue(self, timeout=_DEFAULT_TIMEOUT):
        if not self._enqueue:
            return
        effective_timeout = (
            self._shutdown_timeout if timeout is _DEFAULT_TIMEOUT else timeout
        )
        deadline = (
            None
            if effective_timeout is None
            else monotonic() + max(0.0, float(effective_timeout))
        )
        acquired = False
        try:
            remaining = self._remaining(deadline)
            if remaining is None:
                self._confirmation_lock.acquire()
                acquired = True
            else:
                acquired = self._confirmation_lock.acquire(timeout=remaining)
            if not acquired:
                raise TimeoutError(
                    "Timed out while controlling Loguru handler #%d" % self._id
                )
            self._confirmation_event.clear()
            self._put_control(True, timeout=self._remaining(deadline))
            confirmed = self._confirmation_event.wait(timeout=self._remaining(deadline))
            self._confirmation_event.clear()
            if not confirmed:
                raise TimeoutError(
                    "Timed out while flushing Loguru handler #%d" % self._id
                )
        finally:
            if acquired:
                self._confirmation_lock.release()

    @staticmethod
    def _remaining(deadline):
        if deadline is None:
            return None
        return max(0.0, deadline - monotonic())

    def _put_control(self, message, timeout=_DEFAULT_TIMEOUT):
        effective_timeout = (
            self._shutdown_timeout if timeout is _DEFAULT_TIMEOUT else timeout
        )
        try:
            if effective_timeout is None:
                self._queue.put(message)
            else:
                self._queue.put(message, timeout=max(0.0, float(effective_timeout)))
        except queue.Full as exc:
            raise TimeoutError(
                "Timed out while controlling Loguru handler #%d" % self._id
            ) from exc

    @staticmethod
    def _wait_thread(thread, timeout, action):
        if timeout is None:
            thread.join()
            return
        thread.join(timeout=max(0.0, float(timeout)))
        if thread.is_alive():
            raise TimeoutError("Timed out while %s Loguru handler writer" % action)

    def _close_queue(self, timeout):
        if not hasattr(self._queue, "close"):
            return
        join_thread = getattr(self._queue, "join_thread", None)
        if join_thread is None:
            if not self._queue_close_called:
                self._queue.close()
                self._queue_close_called = True
            self._queue_closed = True
            return

        # ``multiprocessing.Queue.join_thread`` has no timeout.  Start one
        # daemon waiter and let subsequent cleanup attempts wait for that same
        # operation.  This avoids invoking ``join_thread()`` concurrently when
        # a first bounded attempt is still stuck in a broken feeder thread.
        with self._queue_join_lock:
            if not self._queue_close_called:
                self._queue.close()
                self._queue_close_called = True
            if not self._queue_join_started:
                self._queue_join_started = True

                def join_queue():
                    try:
                        join_thread()
                    except BaseException as exc:
                        self._queue_join_error = exc
                    finally:
                        self._queue_join_event.set()

                join_worker = Thread(
                    target=join_queue,
                    daemon=True,
                    name="loguru-queue-join-%d" % self._id,
                )
                try:
                    join_worker.start()
                except BaseException:
                    self._queue_join_started = False
                    raise

        if timeout is None:
            self._queue_join_event.wait()
        elif not self._queue_join_event.wait(timeout=max(0.0, float(timeout))):
            raise TimeoutError("Timed out while joining queue feeder")
        if self._queue_join_error is not None:
            raise self._queue_join_error
        self._queue_closed = True

    def _stop_sink(self, timeout):
        with self._sink_stop_lock:
            if not self._sink_stop_started:
                self._sink_stop_started = True

                def stop_sink():
                    try:
                        self._sink.stop()
                    except BaseException as exc:  # preserve user sink failures
                        self._sink_stop_error = exc
                    finally:
                        self._sink_stop_event.set()

                sink_thread = Thread(
                    target=stop_sink,
                    daemon=True,
                    name="loguru-sink-stop-%d" % self._id,
                )
                try:
                    sink_thread.start()
                except BaseException:
                    self._sink_stop_started = False
                    raise
        if timeout is None:
            self._sink_stop_event.wait()
        elif not self._sink_stop_event.wait(timeout=max(0.0, float(timeout))):
            raise TimeoutError("Timed out while stopping Loguru sink #%d" % self._id)
        if self._sink_stop_error is not None:
            raise self._sink_stop_error

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

    @property
    def drop_reasons(self):
        with self._counter_lock:
            return dict(self._drop_reasons)

    @property
    def sink_errors(self):
        return self._sink_errors

    @property
    def queue_depth(self):
        if not self._enqueue or self._queue is None:
            return 0
        try:
            return max(0, int(self._queue.qsize()))
        except (NotImplementedError, OSError, ValueError):
            return None

    @property
    def queue_capacity(self):
        return self._queue_size

    @property
    def writer_alive(self):
        return bool(self._thread is not None and self._thread.is_alive())

    @property
    def last_error(self):
        return self._last_error

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

            if isinstance(message, DeferredMessage):
                record = message.record
                try:
                    message = self._format_message(
                        record,
                        message.level_id,
                        message.from_decorator,
                        message.is_raw,
                        message.colored_message,
                    )
                except Exception as exc:
                    self._last_error = exc
                    with lock:
                        if not self._error_interceptor.should_catch():
                            raise
                        self._error_interceptor.print(record, exception=exc)
                    continue

            with lock:
                try:
                    self._sink.write(message)
                except Exception as exc:
                    self._sink_errors += 1
                    self._last_error = exc
                    self._error_interceptor.print(message.record)

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_lock"] = None
        state["_lock_acquired"] = None
        state["_counter_lock"] = None
        state["_memoize_dynamic_format"] = None
        # These are process-local coordination primitives and cannot be
        # pickled (notably when a logger is sent through a spawn context).
        state["_queue_join_event"] = None
        state["_queue_join_lock"] = None
        state["_sink_stop_event"] = None
        state["_sink_stop_lock"] = None
        state["_queue_join_started"] = False
        state["_queue_join_error"] = None
        state["_sink_stop_started"] = False
        state["_sink_stop_error"] = None
        state["_last_error"] = None
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
        self._counter_lock = threading.Lock()
        self._queue_join_event = threading.Event()
        self._queue_join_lock = threading.Lock()
        self._sink_stop_event = threading.Event()
        self._sink_stop_lock = threading.Lock()
        if self._enqueue:
            self._queue_lock = create_handler_lock()
        if self._is_formatter_dynamic:
            if self._colorize:
                self._memoize_dynamic_format = memoize(prepare_colored_format)
            else:
                self._memoize_dynamic_format = memoize(prepare_stripped_format)
