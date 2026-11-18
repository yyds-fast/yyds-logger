import asyncio
import inspect
import logging
import threading

from ._asyncio_loop import get_running_loop, get_task_loop


class StreamSink:
    def __init__(self, stream):
        self._stream = stream
        self._flushable = callable(getattr(stream, "flush", None))
        self._stoppable = callable(getattr(stream, "stop", None))
        self._completable = inspect.iscoroutinefunction(getattr(stream, "complete", None))

    def write(self, message):
        self._stream.write(message)
        if self._flushable:
            self._stream.flush()

    def stop(self):
        if self._stoppable:
            self._stream.stop()

    def tasks_to_complete(self):
        if not self._completable:
            return []
        return [self._stream.complete()]


class StandardSink:
    def __init__(self, handler):
        self._handler = handler

    def write(self, message):
        raw_record = message.record
        message = str(message)
        exc = raw_record["exception"]
        record = logging.getLogger().makeRecord(
            raw_record["name"],
            raw_record["level"].no,
            raw_record["file"].path,
            raw_record["line"],
            message,
            (),
            (exc.type, exc.value, exc.traceback) if exc else None,
            raw_record["function"],
            {"extra": raw_record["extra"]},
        )
        if exc:
            record.exc_text = "\n"
        record.levelname = raw_record["level"].name
        self._handler.handle(record)

    def stop(self):
        self._handler.close()

    def tasks_to_complete(self):
        return []


class AsyncSink:
    def __init__(self, function, loop, error_interceptor):
        self._function = function
        self._loop = loop
        self._error_interceptor = error_interceptor
        self._lock = threading.Lock()
        self._tasks = set()
        self._pending_by_loop = {}
        self._stopped = False

    def _decrement_pending(self, loop):
        remaining = self._pending_by_loop.get(loop, 0) - 1
        if remaining > 0:
            self._pending_by_loop[loop] = remaining
        else:
            self._pending_by_loop.pop(loop, None)

    def _schedule_task(self, loop, message):
        with self._lock:
            if self._stopped:
                return None
        coroutine = self._function(message)
        try:
            task = loop.create_task(coroutine)
        except BaseException:
            coroutine.close()
            raise

        def check_exception(future):
            with self._lock:
                self._tasks.discard(future)
            if future.cancelled() or future.exception() is None:
                return
            if not self._error_interceptor.should_catch():
                raise future.exception()
            self._error_interceptor.print(message.record, exception=future.exception())

        with self._lock:
            self._tasks.add(task)
        task.add_done_callback(check_exception)
        return task

    def write(self, message):
        try:
            loop = self._loop or get_running_loop()
        except RuntimeError:
            return

        try:
            running_loop = get_running_loop()
        except RuntimeError:
            running_loop = None

        if running_loop is loop:
            self._schedule_task(loop, message)
            return

        if loop.is_closed():
            return

        def schedule_on_loop():
            try:
                if not loop.is_closed():
                    self._schedule_task(loop, message)
            finally:
                with self._lock:
                    self._decrement_pending(loop)

        with self._lock:
            if self._stopped or loop.is_closed():
                return
            try:
                loop.call_soon_threadsafe(schedule_on_loop)
            except RuntimeError:
                return
            self._pending_by_loop[loop] = self._pending_by_loop.get(loop, 0) + 1

    def stop(self):
        with self._lock:
            self._stopped = True
            self._pending_by_loop.clear()
            tasks = list(self._tasks)
        for task in tasks:
            if task.done():
                continue
            loop = get_task_loop(task)
            try:
                current_loop = get_running_loop()
            except RuntimeError:
                current_loop = None
            try:
                if current_loop is loop or not loop.is_running():
                    task.cancel()
                else:
                    loop.call_soon_threadsafe(task.cancel)
            except RuntimeError:
                # The owning loop may already be closed.  There is no safe
                # thread on which the task can be cancelled at this point.
                pass

    def tasks_to_complete(self):
        try:
            running_loop = get_running_loop()
        except RuntimeError:
            running_loop = None
        with self._lock:
            if running_loop is not None:
                if self._loop is not None and self._loop is not running_loop:
                    return []
                has_loop_work = self._pending_by_loop.get(running_loop, 0) > 0 or any(
                    get_task_loop(task) is running_loop for task in self._tasks
                )
                if not has_loop_work:
                    return []
            elif not self._tasks and not self._pending_by_loop:
                return []
        return [self._complete_pending()]

    async def _complete_pending(self):
        loop = get_running_loop()
        if self._loop is not None and self._loop is not loop:
            return
        while True:
            with self._lock:
                pending = self._pending_by_loop.get(loop, 0)
            if pending <= 0 or loop.is_closed():
                break
            await asyncio.sleep(0)
        with self._lock:
            tasks = [task for task in self._tasks if get_task_loop(task) is loop]
        for task in tasks:
            if task.cancelled():
                continue
            try:
                await task
            except Exception:
                pass  # Handled in "check_exception()"

    def __getstate__(self):
        state = self.__dict__.copy()
        state["_tasks"] = None
        state["_lock"] = None
        state["_pending_by_loop"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._tasks = set()
        self._lock = threading.Lock()
        self._pending_by_loop = {}


class CallableSink:
    def __init__(self, function):
        self._function = function

    def write(self, message):
        self._function(message)

    def stop(self):
        pass

    def tasks_to_complete(self):
        return []
