import collections
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
from ._recattrs import RecordException

_FLUSH_MARKER = "__loguru_flush__"
_PICKLED_MARKER = "__loguru_pickled__"
_HANDLER_PID = os.getpid()


def _refresh_handler_pid():
    global _HANDLER_PID
    _HANDLER_PID = os.getpid()


if hasattr(os, "register_at_fork"):
    os.register_at_fork(after_in_child=_refresh_handler_pid)


class _ThreadQueue:
    """Single-lock bounded deque queue with stop awareness and batch draining."""

    def __init__(self, maxsize=0):
        self._maxsize = maxsize
        self._deque = collections.deque()
        self._mutex = threading.Lock()
        self._not_empty = threading.Condition(self._mutex)
        self._not_full = threading.Condition(self._mutex)
        self._stopped = False

    def qsize(self):
        with self._mutex:
            return len(self._deque)

    def __getstate__(self):
        raise TypeError(
            "Cannot pickle an in-process thread queue (queue_backend='thread'); "
            "use queue_backend='multiprocessing' to share a logger across spawned processes."
        )

    def put_log(self, item, block=True, timeout=None):
        """Enqueue a log message unless the queue is already stopped."""
        with self._mutex:
            if self._stopped:
                return False
            if self._maxsize > 0:
                if not block:
                    if len(self._deque) >= self._maxsize:
                        raise queue.Full
                elif timeout is None:
                    while len(self._deque) >= self._maxsize:
                        if self._stopped:
                            return False
                        self._not_full.wait()
                    if self._stopped:
                        return False
                elif timeout < 0:
                    raise ValueError("'timeout' must be a non-negative number")
                else:
                    endtime = monotonic() + timeout
                    while len(self._deque) >= self._maxsize:
                        if self._stopped:
                            return False
                        remaining = endtime - monotonic()
                        if remaining <= 0.0:
                            raise queue.Full
                        self._not_full.wait(remaining)
                    if self._stopped:
                        return False
            self._deque.append(item)
            if len(self._deque) == 1:
                self._not_empty.notify()
            return True

    def put(self, item, block=True, timeout=None):
        """Enqueue a control marker (permitted after stop)."""
        with self._mutex:
            if self._maxsize > 0:
                if not block:
                    if len(self._deque) >= self._maxsize:
                        raise queue.Full
                elif timeout is None:
                    while len(self._deque) >= self._maxsize:
                        self._not_full.wait()
                else:
                    endtime = monotonic() + timeout
                    while len(self._deque) >= self._maxsize:
                        remaining = endtime - monotonic()
                        if remaining <= 0.0:
                            raise queue.Full
                        self._not_full.wait(remaining)
            self._deque.append(item)
            if len(self._deque) == 1:
                self._not_empty.notify()

    def put_nowait(self, item):
        return self.put(item, block=False)

    def stop(self):
        """Reject new log messages and wake blocked producers immediately."""
        with self._mutex:
            self._stopped = True
            self._not_full.notify_all()

    def get(self):
        with self._mutex:
            while not self._deque:
                self._not_empty.wait()
            item = self._deque.popleft()
            if self._maxsize > 0:
                self._not_full.notify()
            return item

    def drain_batch(self, max_items=256):
        """Wait for an item, then pop up to ``max_items`` under one lock."""
        with self._mutex:
            while not self._deque:
                self._not_empty.wait()
            batch = []
            popleft = self._deque.popleft
            while self._deque and len(batch) < max_items:
                batch.append(popleft())
            if self._maxsize > 0 and batch:
                self._not_full.notify_all()
            return batch


def prepare_colored_format(format_, ansi_level):
    colored = Colorizer.prepare_format(format_)
    return colored, colored.colorize(ansi_level)


def prepare_stripped_format(format_):
    colored = Colorizer.prepare_format(format_)
    return colored.strip()


def memoize(function):
    return functools.lru_cache(maxsize=64)(function)


def _safe_json_default(obj):
    try:
        return str(obj)
    except Exception:
        return "<unprintable %s object>" % type(obj).__name__


class Message(str):
    __slots__ = ("record",)


class DeferredMessage:
    """Record envelope which is formatted by the queue writer.

    This is intentionally only used with the in-process thread queue.  Keeping
    the record and the pre-parsed color message together preserves the normal
    Loguru semantics while moving the expensive format/JSON work out of the
    caller's logging thread.
    """

    __slots__ = (
        "record",
        "level_id",
        "from_decorator",
        "is_raw",
        "colored_message",
        "preformatted_exception",
    )

    def __init__(
        self,
        record,
        level_id,
        from_decorator,
        is_raw,
        colored_message,
        preformatted_exception=None,
    ):
        self.record = record
        self.level_id = level_id
        self.from_decorator = from_decorator
        self.is_raw = is_raw
        self.colored_message = colored_message
        self.preformatted_exception = preformatted_exception


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
        self._counter_lock = create_handler_lock()
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
        self._queue_join_lock = create_handler_lock()
        self._queue_closed = False
        self._sink_stop_started = False
        self._sink_stop_event = threading.Event()
        self._sink_stop_error = None
        self._sink_stop_lock = create_handler_lock()
        self._last_error = None
        self._flush_state_lock = create_handler_lock()
        self._flush_seq = 0
        self._enqueue_state_lock = create_handler_lock()
        self._enqueue_drained_cond = threading.Condition(self._enqueue_state_lock)
        self._active_enqueues = 0

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
                self._queue = _ThreadQueue(maxsize=self._queue_size or 0)
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
                preformatted_exception = None
                if record["exception"] is not None:
                    type_, value, tb = record["exception"]
                    try:
                        lines = self._exception_formatter.format_exception(
                            type_, value, tb, from_decorator=from_decorator
                        )
                        preformatted_exception = "".join(lines)
                    except Exception:
                        self._record_drop("serialization")
                        raise
                    # Drop the live traceback frame graph (including
                    # ``value.__traceback__``) after capturing exact
                    # caller-thread locals/line numbers, while preserving
                    # serialized JSON fields.
                    value_text = None if value is None else _safe_json_default(value)
                    queued_record["exception"] = RecordException(
                        type_, value_text, True if tb is not None else None
                    )
                queued_message = DeferredMessage(
                    queued_record,
                    level_id,
                    from_decorator,
                    is_raw,
                    colored_message,
                    preformatted_exception,
                )
            else:
                try:
                    queued_message = self._format_message(
                        record,
                        level_id,
                        from_decorator,
                        is_raw,
                        colored_message,
                    )
                except Exception:
                    self._record_drop("serialization")
                    raise

            self._enqueue_or_write(queued_message, record)
        except Exception as exc:
            self._last_error = exc
            if not self._error_interceptor.should_catch():
                raise
            self._error_interceptor.print(record)

    def _format_message(
        self,
        record,
        level_id,
        from_decorator,
        is_raw,
        colored_message=None,
        preformatted_exception=None,
    ):
        if self._is_formatter_dynamic:
            dynamic_format = self._formatter(record)

        formatter_record = record.copy()

        if preformatted_exception is not None:
            formatter_record["exception"] = preformatted_exception
        elif not record["exception"]:
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

    def _ensure_thread_writer_current_process(self):
        """Reinitialize the in-process thread queue and writer after os.fork()."""
        if self._owner_process_pid == os.getpid():
            return
        with self._protected_lock():
            if self._owner_process_pid == os.getpid() or self._stopped:
                return
            self._queue = _ThreadQueue(maxsize=self._queue_size or 0)
            self._confirmation_event = threading.Event()
            self._confirmation_lock = threading.Lock()
            self._flush_state_lock = create_handler_lock()
            self._flush_seq = 0
            self._queue_stop_sent = False
            self._queue_close_called = False
            self._queue_join_started = False
            self._queue_join_event = threading.Event()
            self._queue_join_error = None
            self._queue_join_lock = create_handler_lock()
            self._queue_closed = False
            self._sink_stop_started = False
            self._sink_stop_event = threading.Event()
            self._sink_stop_error = None
            self._sink_stop_lock = create_handler_lock()
            self._owner_process_pid = os.getpid()
            self._thread = Thread(
                target=self._queued_writer, daemon=True, name="loguru-writer-%d" % self._id
            )
            self._thread.start()

    def _enqueue_or_write(self, message, record):
        if not self._enqueue:
            with self._protected_lock():
                if self._stopped:
                    return
                try:
                    self._sink.write(message)
                except Exception as exc:
                    with self._counter_lock:
                        self._sink_errors += 1
                    self._last_error = exc
                    raise
            return

        if self._queue_backend == "thread":
            if self._owner_process_pid != _HANDLER_PID:
                self._ensure_thread_writer_current_process()
            try:
                if self._overflow_policy == "drop":
                    self._queue.put_log(message, block=False)
                elif self._queue_timeout is None:
                    self._queue.put_log(message)
                else:
                    self._queue.put_log(message, timeout=float(self._queue_timeout))
            except queue.Full:
                self._record_drop(
                    "overflow" if self._overflow_policy == "drop" else "block_timeout"
                )
                return
            except Exception as exc:
                self._last_error = exc
                raise
            return

        serialization_error = None
        with self._enqueue_state_lock:
            if self._stopped:
                return
            self._active_enqueues += 1
        try:
            try:
                payload = (_PICKLED_MARKER, bytes(ForkingPickler.dumps(message)))
            except Exception as exc:
                self._record_drop("serialization")
                self._last_error = exc
                serialization_error = exc
            if serialization_error is None:
                try:
                    if self._overflow_policy == "drop":
                        self._queue.put_nowait(payload)
                    elif self._queue_timeout is None:
                        self._queue.put(payload)
                    else:
                        self._queue.put(payload, timeout=float(self._queue_timeout))
                except Exception as exc:
                    if isinstance(exc, queue.Full):
                        self._record_drop(
                            "overflow" if self._overflow_policy == "drop" else "block_timeout"
                        )
                        return
                    self._last_error = exc
                    raise
        finally:
            with self._enqueue_state_lock:
                self._active_enqueues -= 1
                if self._active_enqueues == 0 and self._stopped:
                    self._enqueue_drained_cond.notify_all()
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
        if not self._enqueue:
            with self._protected_lock():
                self._stopped = True
            self._stop_sink(self._remaining(deadline))
            return

        if self._owner_process_pid != os.getpid():
            with self._protected_lock():
                self._stopped = True
            return

        if self._queue_backend == "thread":
            self._stopped = True
            self._queue.stop()
        else:
            with self._enqueue_state_lock:
                self._stopped = True
                while self._active_enqueues > 0:
                    remaining = self._remaining(deadline)
                    if remaining is not None and remaining <= 0:
                        raise TimeoutError(
                            "Timed out while waiting for active enqueue producers on handler #%d"
                            % self._id
                        )
                    self._enqueue_drained_cond.wait(timeout=remaining)

        with self._protected_lock():
            if not self._queue_stop_sent:
                self._put_control(None, timeout=self._remaining(deadline))
                self._queue_stop_sent = True
            writer = self._thread

        if writer is not None:
            self._wait_thread(writer, self._remaining(deadline), "stopping")
            self._close_queue(self._remaining(deadline))

        self._stop_sink(self._remaining(deadline))

    def complete_queue(self, timeout=_DEFAULT_TIMEOUT):
        if not self._enqueue:
            return
        if self._queue_backend == "thread" and self._owner_process_pid != os.getpid():
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
            with self._flush_state_lock:
                self._flush_seq += 1
                seq = self._flush_seq
                self._confirmation_event.clear()
            self._put_control((_FLUSH_MARKER, seq), timeout=self._remaining(deadline))
            confirmed = self._confirmation_event.wait(timeout=self._remaining(deadline))
            with self._flush_state_lock:
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
        with self._counter_lock:
            return self._dropped_messages

    @property
    def serialization_errors(self):
        with self._counter_lock:
            return self._serialization_errors

    @property
    def drop_reasons(self):
        with self._counter_lock:
            return dict(self._drop_reasons)

    @property
    def sink_errors(self):
        with self._counter_lock:
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
        if not self._enqueue or self._stopped:
            return False
        if self._owner_process_pid != os.getpid():
            return not self._queue_closed
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
                    "repr": str(record["elapsed"]),
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
                "time": {"repr": str(record["time"]), "timestamp": record["time"].timestamp()},
            },
        }

        return json.dumps(serializable, default=_safe_json_default, ensure_ascii=False) + "\n"

    def _flush_pending_writes(self, lock, pending_writes):
        if not pending_writes:
            return
        sink = self._sink
        with lock:
            if (
                len(pending_writes) > 1
                and hasattr(sink, "write_batch")
                and "write" not in getattr(sink, "__dict__", ())
            ):
                try:
                    sink.write_batch(pending_writes)
                    pending_writes.clear()
                    return
                except Exception as exc:
                    idx = getattr(exc, "_batch_index", None)
                    with self._counter_lock:
                        self._sink_errors += 1
                    self._last_error = exc
                    if idx is None:
                        self._error_interceptor.print(pending_writes[0].record, exception=exc)
                        pending_writes.clear()
                        return
                    self._error_interceptor.print(pending_writes[idx].record, exception=exc)
                    remaining = pending_writes[idx + 1 :]
                    pending_writes.clear()
                    for msg in remaining:
                        try:
                            sink.write(msg)
                        except Exception as item_exc:
                            with self._counter_lock:
                                self._sink_errors += 1
                            self._last_error = item_exc
                            self._error_interceptor.print(msg.record, exception=item_exc)
                    return
            for msg in pending_writes:
                try:
                    sink.write(msg)
                except Exception as exc:
                    with self._counter_lock:
                        self._sink_errors += 1
                    self._last_error = exc
                    self._error_interceptor.print(msg.record, exception=exc)
        pending_writes.clear()

    def _ack_flush(self, seq=None):
        with self._flush_state_lock:
            if seq is None or seq >= self._flush_seq:
                self._confirmation_event.set()

    def _queued_writer(self):
        queue_obj = self._queue
        drain_batch = getattr(queue_obj, "drain_batch", None)

        # We need to use a lock to protect sink during fork.
        # Particularly, writing to stderr may lead to deadlock in child process.
        lock = self._queue_lock
        pending_writes = []

        while True:
            try:
                batch = drain_batch() if drain_batch is not None else [queue_obj.get()]
            except Exception:
                self._flush_pending_writes(lock, pending_writes)
                with lock:
                    self._error_interceptor.print(None)
                continue

            for message in batch:
                if (
                    isinstance(message, tuple)
                    and len(message) == 2
                    and message[0] == _PICKLED_MARKER
                ):
                    try:
                        message = ForkingPickler.loads(message[1])
                    except Exception as exc:
                        self._record_drop("serialization")
                        self._last_error = exc
                        self._flush_pending_writes(lock, pending_writes)
                        with lock:
                            self._error_interceptor.print(None, exception=exc)
                        continue

                if message is None:
                    self._flush_pending_writes(lock, pending_writes)
                    return

                if message is True:
                    self._flush_pending_writes(lock, pending_writes)
                    self._ack_flush(None)
                    continue

                if (
                    isinstance(message, tuple)
                    and len(message) == 2
                    and message[0] == _FLUSH_MARKER
                ):
                    self._flush_pending_writes(lock, pending_writes)
                    self._ack_flush(message[1])
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
                            message.preformatted_exception,
                        )
                    except Exception as exc:
                        self._record_drop("serialization")
                        self._last_error = exc
                        with lock:
                            self._error_interceptor.print(record, exception=exc)
                        continue

                pending_writes.append(message)

            self._flush_pending_writes(lock, pending_writes)

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_lock"] = None
        state["_lock_acquired"] = None
        state["_counter_lock"] = None
        state["_flush_state_lock"] = None
        state["_enqueue_state_lock"] = None
        state["_enqueue_drained_cond"] = None
        state["_active_enqueues"] = 0
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
            state["_owner_process_pid"] = None
            state["_queue_lock"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._lock = create_handler_lock()
        self._lock_acquired = threading.local()
        self._counter_lock = create_handler_lock()
        self._flush_state_lock = create_handler_lock()
        self._enqueue_state_lock = create_handler_lock()
        self._enqueue_drained_cond = threading.Condition(self._enqueue_state_lock)
        self._active_enqueues = 0
        self._queue_join_event = threading.Event()
        self._queue_join_lock = create_handler_lock()
        self._sink_stop_event = threading.Event()
        self._sink_stop_lock = create_handler_lock()
        if self._enqueue:
            self._queue_lock = create_handler_lock()
        if self._is_formatter_dynamic:
            if self._colorize:
                self._memoize_dynamic_format = memoize(prepare_colored_format)
            else:
                self._memoize_dynamic_format = memoize(prepare_stripped_format)
