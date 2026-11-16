import os
import threading
import weakref

if not hasattr(os, "register_at_fork"):

    def create_logger_lock():
        return threading.Lock()

    def create_handler_lock():
        return threading.Lock()

else:
    # While forking, we need to sanitize all locks to make sure the child process doesn't run into
    # a deadlock (if a lock already acquired is inherited) and to protect sink from corrupted state.
    # It's very important to acquire logger locks before handlers one to prevent possible deadlock
    # while 'remove()' is called for example.

    logger_locks = weakref.WeakSet()
    handler_locks = weakref.WeakSet()
    _registry_lock = threading.Lock()
    _acquired_before_fork = []

    def acquire_locks():
        _registry_lock.acquire()
        acquired = []
        try:
            for lock in list(logger_locks):
                lock.acquire()
                acquired.append(lock)

            for lock in list(handler_locks):
                lock.acquire()
                acquired.append(lock)
        except BaseException:
            for lock in reversed(acquired):
                lock.release()
            _registry_lock.release()
            raise
        _acquired_before_fork.clear()
        _acquired_before_fork.extend(acquired)

    def release_locks():
        try:
            for lock in reversed(_acquired_before_fork):
                lock.release()
        finally:
            _acquired_before_fork.clear()
            _registry_lock.release()

    os.register_at_fork(
        before=acquire_locks,
        after_in_parent=release_locks,
        after_in_child=release_locks,
    )

    def create_logger_lock():
        lock = threading.Lock()
        with _registry_lock:
            logger_locks.add(lock)
        return lock

    def create_handler_lock():
        lock = threading.Lock()
        with _registry_lock:
            handler_locks.add(lock)
        return lock
