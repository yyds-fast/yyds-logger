import logging
import asyncio
import contextlib
import io
import json
import os
import signal
import subprocess
import sys
import threading
import time
import pytest

from yyds_logger import LogHealthChecker, YydsLogger
from yyds_logger.i18n import LANG_MAP
from yyds_logger.profiling import _trace_sync


def test_vendored_global_logger_is_initialized_lazily():
    code = """
import importlib
module = importlib.import_module('yyds_logger.yyds_loguru')
assert module._default_logger is None
module.create_logger(stderr=False, register_atexit=False)
assert module._default_logger is None
assert module.logger is module._default_logger
"""
    subprocess.run([sys.executable, "-c", code], check=True)


def test_cleanup_drains_current_instance(tmp_path):
    logger = YydsLogger("drain", log_dir=str(tmp_path))
    logger.info("drain-marker")
    logger.cleanup()
    assert "drain-marker" in (tmp_path / "drain.log").read_text(encoding="utf-8")


def test_flush_keeps_logger_open_and_close_releases(tmp_path):
    logger = YydsLogger("lifecycle", log_dir=str(tmp_path))
    logger.info("before-flush")
    logger.flush()
    logger.info("after-flush")
    logger.close()
    content = (tmp_path / "lifecycle.log").read_text(encoding="utf-8")
    assert "before-flush" in content
    assert "after-flush" in content


def test_error_records_are_written_only_to_main_file(tmp_path):
    logger = YydsLogger("error-main-file", log_dir=str(tmp_path), enqueue=False)
    try:
        logger.error("error")
    finally:
        logger.cleanup()
    assert "error" in (tmp_path / "error-main-file.log").read_text(encoding="utf-8")
    assert not (tmp_path / "error-main-file_error.log").exists()


def test_dev_environment_enables_diagnostics_by_default(tmp_path):
    logger = YydsLogger("development", log_dir=str(tmp_path), env="dev", enqueue=False)
    try:
        assert logger.env == "dev"
        assert logger.diagnose is True
        assert logger.backtrace is True
    finally:
        logger.cleanup()


def test_invalid_language_and_environment_fail_fast(tmp_path):
    with pytest.raises(ValueError):
        YydsLogger("invalid-language", log_dir=str(tmp_path), language="fr")
    with pytest.raises(ValueError):
        YydsLogger("invalid-env", log_dir=str(tmp_path), env="staging")


def test_invalid_queue_configuration_fails_fast(tmp_path):
    with pytest.raises(TypeError):
        YydsLogger("zero-queue", log_dir=str(tmp_path), queue_size=0)
    with pytest.raises(TypeError):
        YydsLogger("float-queue", log_dir=str(tmp_path), queue_size=1.5)
    with pytest.raises(ValueError):
        YydsLogger("bad-timeout", log_dir=str(tmp_path), queue_timeout="never")
    with pytest.raises(ValueError):
        YydsLogger("bad-queue-backend", log_dir=str(tmp_path), queue_backend="invalid")
    with pytest.raises(ValueError):
        YydsLogger("bad-shutdown-timeout", log_dir=str(tmp_path), shutdown_timeout=0)


def test_async_flush_is_available_inside_event_loop(tmp_path):
    async def scenario():
        logger = YydsLogger("async-flush", log_dir=str(tmp_path))
        try:
            logger.info("async flush marker")
            await logger.flush_async()
        finally:
            logger.cleanup()

    asyncio.run(scenario())
    assert "async flush marker" in (tmp_path / "async-flush.log").read_text(encoding="utf-8")


def test_async_flush_and_close_do_not_block_event_loop(tmp_path):
    async def scenario():
        logger = YydsLogger(
            "async-non-blocking",
            log_dir=str(tmp_path),
            queue_backend="thread",
            console_level="CRITICAL",
            compression=None,
        )
        file_handler = next(
            handler for handler in logger.logger._core.handlers.values()
            if handler._enqueue
        )
        original_write = file_handler._sink.write

        def slow_write(message):
            time.sleep(0.003)
            original_write(message)

        file_handler._sink.write = slow_write
        for index in range(40):
            logger.info("async marker {}", index)

        ticks = 0
        running = True

        async def heartbeat():
            nonlocal ticks
            while running:
                ticks += 1
                await asyncio.sleep(0.005)

        task = asyncio.create_task(heartbeat())
        await asyncio.sleep(0)
        ticks_before_flush = ticks
        await logger.flush_async()
        assert ticks > ticks_before_flush
        await logger.aclose()
        assert logger._cleanup_state == "closed"
        running = False
        await task

    asyncio.run(scenario())


def test_async_flush_waits_for_user_async_sink_without_enqueue(tmp_path):
    received = []

    async def scenario():
        logger = YydsLogger(
            "async-user-sink",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
        )

        async def sink(message):
            await asyncio.sleep(0.01)
            received.append(str(message))

        logger.add(sink, format="{message}")
        logger.info("user async marker")
        await logger.flush_async()
        assert any("user async marker" in message for message in received)
        await logger.aclose()

    asyncio.run(scenario())


def test_async_context_manager_flushes_and_closes(tmp_path):
    async def scenario():
        logger = YydsLogger(
            "async-context",
            log_dir=str(tmp_path),
            console_level="CRITICAL",
        )
        async with logger:
            logger.info("async context marker")
        assert logger._cleanup_state == "closed"

    asyncio.run(scenario())
    assert "async context marker" in (
        tmp_path / "async-context.log"
    ).read_text(encoding="utf-8")


def test_async_close_rejects_new_records_before_drain(tmp_path):
    async def scenario():
        logger = YydsLogger(
            "async-closing-gate",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
        )
        entered = asyncio.Event()
        release = asyncio.Event()

        async def sink(message):
            entered.set()
            await release.wait()

        logger.add(sink, format="{message}")
        logger.info("before-close")
        await entered.wait()

        close_task = asyncio.create_task(logger.aclose())
        while logger._cleanup_state != "closing":
            await asyncio.sleep(0)
        with pytest.raises(RuntimeError):
            logger.info("must-be-rejected")
        release.set()
        await close_task
        assert logger._cleanup_state == "closed"

    asyncio.run(scenario())


def test_concurrent_async_close_waits_for_same_shutdown(tmp_path):
    async def scenario():
        logger = YydsLogger(
            "concurrent-async-close",
            log_dir=str(tmp_path),
            console_level="CRITICAL",
        )
        logger.info("close-once")
        await asyncio.gather(logger.aclose(), logger.aclose())
        assert logger._cleanup_state == "closed"
        assert logger.logger._core.handlers == {}

    asyncio.run(scenario())


def test_async_sink_flush_honors_end_to_end_timeout(tmp_path):
    async def scenario():
        logger = YydsLogger(
            "async-sink-timeout",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
            shutdown_timeout=0.05,
        )

        async def sink(message):
            await asyncio.sleep(5)

        handler_id = logger.add(sink, format="{message}")
        logger.info("slow-async-sink")
        # Keep a strong reference so the cancelled task remains visible to
        # the retry below (this is what Python 3.12 does while the timeout
        # traceback is still alive).
        handler = logger.logger._core.handlers[handler_id]
        retained_task = next(iter(handler._sink._tasks))
        with pytest.raises(TimeoutError):
            await logger.flush_async()
        assert logger._cleanup_state == "open"
        assert retained_task.cancelled()
        await logger.aclose()

    asyncio.run(scenario())


def test_blocked_user_sink_stop_times_out_and_can_be_retried(tmp_path):
    release = threading.Event()

    class BlockingSink:
        def write(self, message):
            return None

        def stop(self):
            release.wait()

    logger = YydsLogger(
        "bounded-user-stop",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        shutdown_timeout=0.05,
    )
    logger.add(BlockingSink(), format="{message}")
    with pytest.raises((RuntimeError, TimeoutError)):
        logger.cleanup()
    assert logger._cleanup_state == "failed"
    release.set()
    logger.cleanup()
    assert logger._cleanup_state == "closed"


def test_flush_timeout_is_bounded_and_retryable(tmp_path, monkeypatch):
    logger = YydsLogger(
        "flush-timeout",
        log_dir=str(tmp_path),
        queue_backend="thread",
        shutdown_timeout=0.1,
        console_level="CRITICAL",
    )
    handler = next(
        item for item in logger.logger._core.handlers.values()
        if item._enqueue
    )
    original_wait = handler._confirmation_event.wait
    monkeypatch.setattr(handler._confirmation_event, "wait", lambda timeout=None: False)
    try:
        with pytest.raises(TimeoutError):
            logger.flush()
    finally:
        monkeypatch.setattr(handler._confirmation_event, "wait", original_wait)
        logger.cleanup()


def test_cleanup_drain_timeout_preserves_managed_state_for_retry(tmp_path, monkeypatch):
    logger = YydsLogger(
        "cleanup-timeout-retry",
        log_dir=str(tmp_path),
        queue_backend="thread",
        shutdown_timeout=0.1,
        console_level="CRITICAL",
    )
    handler = next(
        item for item in logger.logger._core.handlers.values()
        if item._enqueue
    )
    managed_ids = list(logger._handler_ids)
    original_wait = handler._confirmation_event.wait
    monkeypatch.setattr(handler._confirmation_event, "wait", lambda timeout=None: False)

    with pytest.raises(TimeoutError):
        logger.cleanup()

    assert logger._cleanup_state == "failed"
    assert logger._handler_ids == managed_ids

    monkeypatch.setattr(handler._confirmation_event, "wait", original_wait)
    logger.cleanup()
    assert logger._cleanup_state == "closed"


def test_serialized_file_output_is_valid_json(tmp_path):
    logger = YydsLogger(
        "serialized",
        log_dir=str(tmp_path),
        serialize=True,
        enqueue=False,
    )
    try:
        logger.bind(trace_id="trace-1").info("structured message")
    finally:
        logger.cleanup()
    records = [json.loads(line) for line in (tmp_path / "serialized.log").read_text(encoding="utf-8").splitlines()]
    assert records[-1]["record"]["message"] == "structured message"
    assert records[-1]["record"]["extra"]["trace_id"] == "trace-1"
    assert records[-1]["record"]["extra"]["request_id"] == "-"


def test_deferred_thread_writer_formats_json_file(tmp_path):
    logger = YydsLogger(
        "deferred-serialized",
        log_dir=str(tmp_path),
        serialize=True,
        queue_backend="thread",
        defer_format=True,
        console_level="CRITICAL",
        compression=None,
    )
    try:
        file_handler = next(
            item for item in logger.logger._core.handlers.values() if item._enqueue
        )
        assert file_handler._defer_format is True
        logger.bind(trace_id="deferred").info("deferred {}", "message")
        logger.flush()
    finally:
        logger.cleanup()

    records = [
        json.loads(line)
        for line in (tmp_path / "deferred-serialized.log")
        .read_text(encoding="utf-8")
        .splitlines()
    ]
    assert records[-1]["record"]["message"] == "deferred message"
    assert records[-1]["record"]["extra"]["trace_id"] == "deferred"


def test_contextualize_does_not_leak_after_scope(tmp_path):
    logger = YydsLogger("context", log_dir=str(tmp_path),
                        serialize=True, enqueue=False)
    try:
        with logger.contextualize(trace_id="scoped"):
            logger.info("inside")
        logger.info("outside")
    finally:
        logger.cleanup()
    records = [json.loads(line) for line in (tmp_path / "context.log").read_text(encoding="utf-8").splitlines()]
    trace_values = [record["record"]["extra"].get("trace_id") for record in records]
    assert trace_values == ["scoped", None]


def test_reconfigure_and_cleanup_are_idempotent(tmp_path):
    logger = YydsLogger("reconfigure", log_dir=str(tmp_path),
                        enqueue=False, file_level="INFO")
    logger.info("before-reconfigure")
    logger.file_level = "DEBUG"
    logger.configure_logger()
    logger.debug("after-reconfigure")
    logger.cleanup()
    logger.cleanup()
    content = (tmp_path / "reconfigure.log").read_text(encoding="utf-8")
    assert "before-reconfigure" in content
    assert "after-reconfigure" in content


def test_level_gate_considers_independent_sink_levels(tmp_path):
    logger = YydsLogger(
        "independent-levels",
        log_dir=str(tmp_path),
        file_level="DEBUG",
        console_level="CRITICAL",
        enqueue=False,
    )
    try:
        assert logger.is_level_enabled("DEBUG") is True
        assert logger.is_level_enabled("INFO") is True
        assert logger.is_level_enabled("ERROR") is True
    finally:
        logger.cleanup()


def test_default_sinks_do_not_filter_trace_level(tmp_path):
    logger = YydsLogger("unfiltered", log_dir=str(tmp_path), enqueue=False)
    try:
        assert logger.is_level_enabled("TRACE") is True
        logger.log("TRACE", "trace-marker")
    finally:
        logger.cleanup()
    assert "trace-marker" in (tmp_path / "unfiltered.log").read_text(encoding="utf-8")


def test_size_rotation_applies_to_main_file(tmp_path):
    logger = YydsLogger("size-rotation", log_dir=str(tmp_path), enqueue=False)

    try:
        logger.max_size = 20
        logger.configure_logger()
        file_handler = next(
            handler
            for handler in logger.logger._core.handlers.values()
            if hasattr(getattr(handler, "_sink", None), "_rotation_function")
        )
        rotation = file_handler._sink._rotation_function
        assert rotation.keywords["size_limit"] == 20_000_000.0
    finally:
        logger.cleanup()


@pytest.mark.parametrize(
    ("compression", "retained_suffix", "other_suffix"),
    [(None, ".log", ".log.gz"), ("gz", ".log.gz", ".log")],
)
def test_retention_targets_only_matching_archives(
    tmp_path, compression, retained_suffix, other_suffix
):
    logger = YydsLogger(
        "retention",
        log_dir=str(tmp_path),
        retention="1 second",
        compression=compression,
        enqueue=False,
    )
    active = tmp_path / "retention.log"
    matching_archive = tmp_path / ("retention.2000-01-01_00-00-00" + retained_suffix)
    other_archive_type = tmp_path / ("retention.2000-01-01_00-00-01" + other_suffix)
    other_sink_archive = tmp_path / ("retention_other.2000-01-01_00-00-00" + retained_suffix)
    try:
        for path in (matching_archive, other_archive_type, other_sink_archive):
            path.write_text("archive", encoding="utf-8")
            os.utime(path, (time.time() - 10, time.time() - 10))

        logger._archive_retention(str(active))(
            [str(active), str(matching_archive), str(other_archive_type), str(other_sink_archive)]
        )

        assert active.exists()
        assert not matching_archive.exists()
        assert other_archive_type.exists()
        assert other_sink_archive.exists()
    finally:
        logger.cleanup()


def test_cleanup_can_retry_after_failure(tmp_path, monkeypatch):
    logger = YydsLogger("cleanup-retry", log_dir=str(tmp_path),
                        enqueue=False)
    original_remove = logger._remove_handlers
    failed = {"once": False}

    def fail_once(wait=False, strict=False):
        if not failed["once"]:
            failed["once"] = True
            raise RuntimeError("synthetic cleanup failure")
        return original_remove(wait=wait, strict=strict)

    monkeypatch.setattr(logger, "_remove_handlers", fail_once)
    with pytest.raises(RuntimeError):
        logger.cleanup()
    assert logger._cleanup_state == "failed"
    logger.cleanup()
    assert logger._cleanup_state == "closed"


def test_reconfigure_failure_keeps_previous_handlers(tmp_path):
    logger = YydsLogger("reconfigure-rollback", log_dir=str(tmp_path),
                        enqueue=False, file_level="INFO")
    try:
        logger.info("before-failure")
        logger.file_level = "NOT_A_LEVEL"
        with pytest.raises(ValueError):
            logger.configure_logger()
        assert logger.file_level == "INFO"
        logger.info("after-failure")
    finally:
        logger.cleanup()
    content = (tmp_path / "reconfigure-rollback.log").read_text(encoding="utf-8")
    assert "before-failure" in content
    assert "after-failure" in content


def test_hook_conflict_keeps_logger_usable(tmp_path):
    owner = YydsLogger("hook-owner", log_dir=str(tmp_path), enqueue=False)
    logger = YydsLogger(
        "hook-rollback", log_dir=str(tmp_path), enqueue=False,
    )
    try:
        owner.setup_exception_handler()
        with pytest.raises(RuntimeError):
            logger.setup_exception_handler()
        logger.info("still-active-after-hook-conflict")
    finally:
        logger.cleanup()
        owner.cleanup()
    content = (tmp_path / "hook-rollback.log").read_text(encoding="utf-8")
    assert "still-active-after-hook-conflict" in content


def test_closed_logger_rejects_resource_reconfiguration(tmp_path):
    logger = YydsLogger(
        "closed", log_dir=str(tmp_path), enqueue=False,
    )
    logger.cleanup()
    with pytest.raises(RuntimeError):
        logger.configure_logger()
    with pytest.raises(RuntimeError):
        logger.capture_std_logging(names=["closed-stdlib"])


def test_line_profiling_restores_trace_after_exception(tmp_path):
    logger = YydsLogger("profiling", log_dir=str(tmp_path),
                        enqueue=False, console_level="CRITICAL")
    try:
        @logger.time_it(line_by_line=True)
        def failing_function():
            raise ValueError("profile failure")

        @logger.time_it(line_by_line=True)
        def successful_function():
            return 42

        with pytest.raises(ValueError):
            failing_function()
        assert successful_function() == 42
    finally:
        logger.cleanup()


def test_fallback_profilers_can_run_in_independent_threads():
    barrier = threading.Barrier(2)
    results = []
    errors = []

    def profiled():
        barrier.wait(timeout=2)
        return threading.get_ident()

    def worker():
        try:
            result, _, _, _ = _trace_sync(profiled)
            results.append(result)
        except Exception as exc:
            errors.append(exc)

    threads = [threading.Thread(target=worker) for _ in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert errors == []
    assert len(results) == 2
    assert all(not thread.is_alive() for thread in threads)


def test_fallback_profiler_restores_trace_after_exception():
    previous_trace = sys.gettrace()

    def failing():
        raise ValueError("fallback profile failure")

    with pytest.raises(ValueError):
        _trace_sync(failing)
    assert sys.gettrace() is previous_trace


def test_local_sink_queue_configuration(tmp_path):
    logger = YydsLogger(
        "bounded",
        log_dir=str(tmp_path),
        queue_size=1,
        overflow_policy="drop",
    )
    try:
        for _ in range(50):
            logger.info("burst")
        dropped_before_cleanup = logger.get_queue_dropped()
        logger.cleanup()
        assert logger.get_queue_dropped() == dropped_before_cleanup
        assert (tmp_path / "bounded.log").exists()
    finally:
        logger.cleanup()


def test_queue_status_classifies_block_timeout_drops(tmp_path):
    class SlowSink:
        def write(self, message):
            time.sleep(0.02)

    logger = YydsLogger(
        "queue-reasons",
        log_dir=str(tmp_path),
        queue_size=1,
        overflow_policy="block",
        queue_timeout=0,
        console_level="CRITICAL",
        compression=None,
    )
    logger.add(
        SlowSink(),
        enqueue=True,
        queue_size=1,
        overflow_policy="block",
        queue_timeout=0,
        format="{message}",
    )
    try:
        for index in range(20):
            logger.info("burst {}", index)
        status = logger.get_queue_status()
        assert status["drop_reasons"]["block_timeout"] > 0
        assert status["dropped_messages"] >= status["drop_reasons"]["block_timeout"]
        assert "handlers" in status
    finally:
        logger.cleanup()


def test_multiprocessing_serialization_failure_is_counted(tmp_path):
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        logger = YydsLogger(
            "serialization-failure",
            log_dir=str(tmp_path),
            queue_backend="multiprocessing",
            console_level="CRITICAL",
            compression=None,
        )
        try:
            logger.bind(lock=threading.Lock()).info("must-not-be-silent")
            logger.flush()
            status = logger.get_queue_status()
            assert status["dropped_messages"] == 1
            assert status["serialization_errors"] == 1
        finally:
            logger.cleanup()
    assert "cannot pickle" in stderr.getvalue()
    assert "must-not-be-silent" not in (
        tmp_path / "serialization-failure.log"
    ).read_text(encoding="utf-8")


def test_cleanup_removes_handlers_added_through_embedded_logger(tmp_path):
    logger = YydsLogger(
        "managed-close", log_dir=str(tmp_path), enqueue=False,
        console_level="CRITICAL",
    )
    extra_path = tmp_path / "extra.log"
    logger.add(str(extra_path), enqueue=False, format="{message}")
    assert logger.get_health()["logger"]["handler_count"] == 3
    logger.info("before-close")
    logger.cleanup()

    assert logger.logger._core.handlers == {}
    with pytest.raises(RuntimeError):
        logger.info("after-close")
    assert "after-close" not in extra_path.read_text(encoding="utf-8")


def test_queue_status_and_health_include_logger_state(tmp_path):
    logger = YydsLogger(
        "queue-status",
        log_dir=str(tmp_path),
        queue_size=12,
        overflow_policy="drop",
        enqueue=False,
    )
    try:
        queue_status = logger.get_queue_status()
        assert queue_status["enabled"] is False
        assert queue_status["size"] == 12
        assert queue_status["capacity"] == 0
        assert queue_status["depth"] == 0
        assert queue_status["overflow_policy"] == "drop"
        assert queue_status["timeout"] == 1.0
        assert queue_status["backend"] == "thread"
        assert queue_status["shutdown_timeout"] == 30.0
        assert queue_status["dropped_messages"] == 0
        assert queue_status["serialization_errors"] == 0
        assert queue_status["drop_reasons"] == {
            "overflow": 0,
            "block_timeout": 0,
            "serialization": 0,
        }
        health = logger.get_health()
        assert health["logger"]["state"] == "open"
        assert health["logger"]["enqueue"] is False
        assert health["logger"]["queue_backend"] == "thread"
        assert health["logger"]["queue_serialization_errors"] == 0
        assert health["logger"]["handler_count"] == 2
    finally:
        logger.cleanup()


def test_auto_queue_backend_uses_thread_with_or_without_process_isolation(tmp_path):
    normal = YydsLogger("normal-queue", log_dir=str(tmp_path), enqueue=False)
    isolated = YydsLogger(
        "isolated-queue", log_dir=str(tmp_path), process_isolation=True, enqueue=True,
    )
    forced = YydsLogger(
        "forced-queue", log_dir=str(tmp_path), process_isolation=True,
        queue_backend="multiprocessing", enqueue=False,
    )
    try:
        assert normal.get_queue_status()["backend"] == "thread"
        assert isolated.get_queue_status()["backend"] == "thread"
        assert forced.get_queue_status()["backend"] == "multiprocessing"
    finally:
        forced.cleanup()
        isolated.cleanup()
        normal.cleanup()


def test_pid_file_isolation(tmp_path):
    logger = YydsLogger(
        "service",
        log_dir=str(tmp_path),
        process_isolation=True,
        enqueue=False,
    )
    try:
        logger.info("isolated")
    finally:
        logger.cleanup()
    assert (tmp_path / f"service.pid{__import__('os').getpid()}.log").exists()


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(),
    reason="PID reinitialization is specific to fork-capable platforms",
)
@pytest.mark.parametrize("enqueue", [False, True])
def test_pid_file_isolation_rebuilds_sinks_after_prefork(tmp_path, enqueue):
    """A logger created by a pre-fork master must not keep its master's PID."""
    import multiprocessing

    logger = YydsLogger(
        "prefork-service",
        log_dir=str(tmp_path),
        process_isolation=True,
        enqueue=enqueue,
        console_level="CRITICAL",
    )
    parent_pid = os.getpid()

    def child_main():
        logger.info("child-only")
        logger.cleanup()

    try:
        logger.info("parent-only")
        process = multiprocessing.get_context("fork").Process(target=child_main)
        process.start()
        child_pid = process.pid
        process.join(timeout=5)
        assert process.exitcode == 0
    finally:
        logger.cleanup()

    parent_file = tmp_path / f"prefork-service.pid{parent_pid}.log"
    child_file = tmp_path / f"prefork-service.pid{child_pid}.log"
    assert "parent-only" in parent_file.read_text(encoding="utf-8")
    assert "child-only" not in parent_file.read_text(encoding="utf-8")
    assert "child-only" in child_file.read_text(encoding="utf-8")


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(),
    reason="PID reinitialization is specific to fork-capable platforms",
)
def test_stdlib_capture_uses_rebuilt_engine_after_prefork(tmp_path):
    import multiprocessing

    name = "prefork-stdlib-bridge"
    target = logging.getLogger(name)
    original_handlers = list(target.handlers)
    original_level = target.level
    original_propagate = target.propagate
    logger = YydsLogger(
        "prefork-bridge",
        log_dir=str(tmp_path),
        process_isolation=True,
        enqueue=False,
        console_level="CRITICAL",
    )
    parent_pid = os.getpid()

    def child_main():
        logging.getLogger(name).warning("child-stdlib-only")
        logger.cleanup()

    try:
        logger.capture_std_logging(names=[name], level="WARNING")
        process = multiprocessing.get_context("fork").Process(target=child_main)
        process.start()
        child_pid = process.pid
        process.join(timeout=5)
        assert process.exitcode == 0
    finally:
        logger.cleanup()
        target.handlers = original_handlers
        target.setLevel(original_level)
        target.propagate = original_propagate

    parent_file = tmp_path / f"prefork-bridge.pid{parent_pid}.log"
    assert "child-stdlib-only" not in parent_file.read_text(encoding="utf-8")
    child_file = tmp_path / f"prefork-bridge.pid{child_pid}.log"
    assert "child-stdlib-only" in child_file.read_text(encoding="utf-8")


def test_basic_stats_are_returned(tmp_path):
    logger = YydsLogger("stats", log_dir=str(tmp_path),
                        enable_stats=True, enqueue=False)
    try:
        logger.info("info")
        logger.warning("warning")
        logger.error("error")
        stats = logger.get_stats()
        assert stats["total"] == 3
        assert stats["info"] == 1
        assert stats["warning"] == 1
        assert stats["error"] == 1
        assert stats["error_rate"] == 1 / 3
    finally:
        logger.cleanup()


def test_stats_cover_bound_and_contextualized_loggers(tmp_path):
    logger = YydsLogger("bound-stats", log_dir=str(tmp_path),
                        enable_stats=True, enqueue=False)
    try:
        logger.bind(component="bound").info("bound")
        with logger.contextualize(component="scoped"):
            logger.info("contextualized")
        logger.logger.warning("raw")
        stats = logger.get_stats()
        assert stats["total"] == 3
        assert stats["info"] == 2
        assert stats["warning"] == 1
    finally:
        logger.cleanup()


def test_health_checker_returns_stable_result(tmp_path):
    (tmp_path / "service.log").write_text("health\n", encoding="utf-8")
    result = LogHealthChecker().check_health(str(tmp_path))
    assert result["status"] in {"healthy", "warning"}
    assert result["errors"] == []
    assert result["metrics"]["log_files_count"] == 1
    assert "warnings" in result


def test_health_checker_reports_missing_directory(tmp_path):
    result = LogHealthChecker().check_health(str(tmp_path / "missing"))
    assert result["status"] == "error"
    assert result["errors"]


def test_i18n_has_matching_chinese_and_english_keys():
    assert set(LANG_MAP["zh"]) == set(LANG_MAP["en"])


def test_health_checker_supports_english_messages(tmp_path):
    result = LogHealthChecker(language="en").check_health(str(tmp_path / "missing"))
    assert "does not exist" in result["errors"][0]


def test_process_global_hooks_are_exclusive(tmp_path):
    first = YydsLogger("first", log_dir=str(tmp_path))
    try:
        first.capture_std_logging(names=["hardening-test"], level="WARNING")
        second = YydsLogger("second", log_dir=str(tmp_path))
        try:
            with pytest.raises(RuntimeError):
                second.capture_std_logging(names=["hardening-test"], level="WARNING")
        finally:
            second.cleanup()
    finally:
        first.cleanup()


def test_std_logging_rejects_invalid_or_duplicate_capture(tmp_path):
    logger = YydsLogger("std-validation", log_dir=str(tmp_path))
    try:
        with pytest.raises(ValueError):
            logger.capture_std_logging(level="NOT_A_LEVEL")
        logger.capture_std_logging(names=["std-validation"], level="WARNING")
        with pytest.raises(RuntimeError):
            logger.capture_std_logging(names=["std-validation"], level="WARNING")
    finally:
        logger.cleanup()


def test_std_logging_preserves_existing_handlers_by_default(tmp_path):
    name = "preserve-handlers"
    target = logging.getLogger(name)
    original_handlers = list(target.handlers)
    custom_handler = logging.NullHandler()
    target.addHandler(custom_handler)
    logger = YydsLogger("preserve", log_dir=str(tmp_path))
    try:
        logger.capture_std_logging(names=[name], level="WARNING")
        assert custom_handler in target.handlers
    finally:
        logger.cleanup()
        target.handlers = original_handlers


def test_std_logging_deduplicates_names_and_preserves_runtime_handlers(tmp_path):
    name = "deduplicated-handlers"
    target = logging.getLogger(name)
    original_handlers = list(target.handlers)
    original_level = target.level
    original_propagate = target.propagate
    runtime_handler = logging.NullHandler()
    logger = YydsLogger("deduplicated", log_dir=str(tmp_path))
    try:
        logger.capture_std_logging(names=[name, name], level="WARNING")
        target.addHandler(runtime_handler)
        logger.cleanup()
        assert target.handlers == original_handlers + [runtime_handler]
    finally:
        logger.cleanup()
        target.handlers = original_handlers
        target.setLevel(original_level)
        target.propagate = original_propagate


def test_std_logging_restore_can_retry_after_failure(tmp_path, monkeypatch):
    name = "stdlib-restore-retry"
    target = logging.getLogger(name)
    original_handlers = list(target.handlers)
    original_level = target.level
    original_propagate = target.propagate
    logger = YydsLogger("stdlib-retry", log_dir=str(tmp_path))
    original_set_level = target.setLevel
    failed = {"once": False}

    def fail_once(level):
        if not failed["once"]:
            failed["once"] = True
            raise RuntimeError("synthetic stdlib restore failure")
        return original_set_level(level)

    try:
        logger.capture_std_logging(names=[name], level="WARNING")
        monkeypatch.setattr(target, "setLevel", fail_once)
        with pytest.raises(RuntimeError):
            logger.cleanup()
        assert logger._cleanup_state == "failed"
        logger.cleanup()
        assert logger._cleanup_state == "closed"
        assert target.handlers == original_handlers
        assert target.level == original_level
        assert target.propagate == original_propagate
    finally:
        monkeypatch.setattr(target, "setLevel", original_set_level)
        logger.cleanup()
        target.handlers = original_handlers
        target.setLevel(original_level)
        target.propagate = original_propagate


def test_std_logging_state_is_restored(tmp_path):
    name = "hardening-restore"
    target = logging.getLogger(name)
    original_level = target.level
    logger = YydsLogger("restore", log_dir=str(tmp_path))
    try:
        logger.capture_std_logging(names=[name], level="WARNING")
        assert target.level == logging.WARNING
    finally:
        logger.cleanup()
    assert target.level == original_level


def test_global_hooks_are_restored(tmp_path):
    previous_excepthook = sys.excepthook
    previous_thread_hook = threading.excepthook
    previous_signals = {
        signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        signal.SIGINT: signal.getsignal(signal.SIGINT),
    }
    logger = YydsLogger(
        "hooks",
        log_dir=str(tmp_path),
        enqueue=False,
    )
    try:
        logger.setup_exception_handler()
        logger.setup_signal_handlers()
        assert sys.excepthook is not previous_excepthook
        assert threading.excepthook is not previous_thread_hook
        assert signal.getsignal(signal.SIGTERM) is not previous_signals[signal.SIGTERM]
    finally:
        logger.cleanup()
    assert sys.excepthook is previous_excepthook
    assert threading.excepthook is previous_thread_hook
    assert signal.getsignal(signal.SIGTERM) is previous_signals[signal.SIGTERM]
    assert signal.getsignal(signal.SIGINT) is previous_signals[signal.SIGINT]


def test_signal_handler_notifies_background_cleanup(tmp_path):
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    previous_called = threading.Event()
    cleanup_started = threading.Event()
    release_cleanup = threading.Event()
    cleanup_threads = []

    def application_handler(signum, frame):
        previous_called.set()

    signal.signal(signal.SIGTERM, application_handler)
    logger = YydsLogger(
        "signal-notification",
        log_dir=str(tmp_path),
        console_level="CRITICAL",
    )
    original_cleanup = logger.cleanup

    def blocking_cleanup():
        cleanup_threads.append(threading.current_thread().name)
        cleanup_started.set()
        release_cleanup.wait()
        original_cleanup()

    logger.cleanup = blocking_cleanup
    try:
        logger.setup_signal_handlers()
        installed = signal.getsignal(signal.SIGTERM)
        started = time.monotonic()
        installed(signal.SIGTERM, None)
        assert time.monotonic() - started < 0.1
        assert previous_called.wait(timeout=0.5)
        assert cleanup_started.wait(timeout=0.5)
        assert cleanup_threads == ["yyds-logger-signal-cleanup"]

        release_cleanup.set()
        deadline = time.monotonic() + 1
        while logger._cleanup_state != "closed" and time.monotonic() < deadline:
            time.sleep(0.01)
        assert logger._cleanup_state == "closed"
    finally:
        release_cleanup.set()
        logger.cleanup = original_cleanup
        logger.cleanup()
        signal.signal(signal.SIGTERM, previous_sigterm)


def test_exception_hook_chains_application_hook(tmp_path):
    calls = []
    previous = sys.excepthook

    def application_hook(exc_type, exc_value, exc_traceback):
        calls.append((exc_type, exc_value))

    sys.excepthook = application_hook
    logger = YydsLogger("hook-chain", log_dir=str(tmp_path), enqueue=False)
    try:
        logger.setup_exception_handler()
        installed_hook = sys.excepthook
        logger.configure_logger()
        assert sys.excepthook is installed_hook
        error = ValueError("hook-chain")
        sys.excepthook(ValueError, error, error.__traceback__)
    finally:
        logger.cleanup()
        sys.excepthook = previous
    assert calls == [(ValueError, error)]


def test_log_decorator_reraise_is_independent_from_trace(tmp_path):
    logger = YydsLogger("decorator-reraise", log_dir=str(tmp_path), enqueue=False)

    @logger.log_decorator(trace=False)
    def default_must_reraise():
        raise ValueError("default-reraise")

    @logger.log_decorator(trace=True, reraise=False)
    def may_be_swallowed():
        raise ValueError("swallowed")

    try:
        with pytest.raises(ValueError, match="default-reraise"):
            default_must_reraise()
        assert may_be_swallowed() is None
    finally:
        logger.cleanup()


def test_cleanup_preserves_application_hooks_installed_later(tmp_path):
    previous_excepthook = sys.excepthook
    previous_thread_hook = threading.excepthook
    previous_sigterm = signal.getsignal(signal.SIGTERM)

    def later_thread_hook(args):
        return None

    def later_signal_hook(signum, frame):
        return None

    logger = YydsLogger(
        "later-hooks", log_dir=str(tmp_path),
        enqueue=False,
    )
    try:
        logger.setup_exception_handler()
        logger.setup_signal_handlers()
        threading.excepthook = later_thread_hook
        signal.signal(signal.SIGTERM, later_signal_hook)
        logger.cleanup()
        assert threading.excepthook is later_thread_hook
        assert signal.getsignal(signal.SIGTERM) is later_signal_hook
    finally:
        logger.cleanup()
        sys.excepthook = previous_excepthook
        threading.excepthook = previous_thread_hook
        signal.signal(signal.SIGTERM, previous_sigterm)


def test_same_path_reconfigure_preserves_all_logs_during_rotation(tmp_path):
    logger = YydsLogger(
        "same-path-reconf",
        log_dir=str(tmp_path),
        max_size=1,
        compression=None,
        console_level="CRITICAL",
        file_level="INFO",
        queue_backend="thread",
    )
    try:
        # Lower rotation limit so rotation happens while draining/reconfiguring.
        file_handler = next(
            h for h in logger.logger._core.handlers.values()
            if getattr(getattr(h, "_sink", None), "_size_limit", None) is not None
        )
        file_handler._sink._size_limit = 500
        for index in range(30):
            logger.info("before-reconf-{}", index)
        logger.reconfigure(file_level="DEBUG", max_size=1)
        for index in range(30):
            logger.debug("after-reconf-{}", index)
        logger.flush()
    finally:
        logger.cleanup()

    combined = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(tmp_path.glob("same-path-reconf*.log"))
    )
    for index in range(30):
        assert f"before-reconf-{index}" in combined
        assert f"after-reconf-{index}" in combined


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(),
    reason="PID reinitialization is specific to fork-capable platforms",
)
def test_reconfigure_process_isolation_updates_fork_registration(tmp_path):
    import multiprocessing

    logger = YydsLogger(
        "reconf-iso",
        log_dir=str(tmp_path),
        process_isolation=False,
        console_level="CRITICAL",
        compression=None,
    )
    try:
        logger.reconfigure(process_isolation=True)
        parent_pid = os.getpid()

        def child_main():
            logger.info("child-after-reconf")
            logger.cleanup()

        process = multiprocessing.get_context("fork").Process(target=child_main)
        process.start()
        child_pid = process.pid
        process.join(timeout=5)
        assert process.exitcode == 0

        logger.reconfigure(process_isolation=False)
        logger.info("parent-non-isolated")
    finally:
        logger.cleanup()

    assert "child-after-reconf" in (tmp_path / f"reconf-iso.pid{child_pid}.log").read_text(encoding="utf-8")
    assert not (tmp_path / f"reconf-iso.pid{parent_pid}.log").exists() or (
        "child-after-reconf" not in (tmp_path / f"reconf-iso.pid{parent_pid}.log").read_text(encoding="utf-8")
    )
    assert "parent-non-isolated" in (tmp_path / "reconf-iso.log").read_text(encoding="utf-8")


def test_reconfigure_validation_failure_does_not_mutate_config(tmp_path):
    logger = YydsLogger("reconf-atomic", log_dir=str(tmp_path), language="zh", max_size=5)
    try:
        with pytest.raises(ValueError):
            logger.reconfigure(language="en", max_size=-1)
        cfg = logger.config
        assert cfg.language == "zh"
        assert cfg.max_size == 5
    finally:
        logger.cleanup()


def test_flush_timeout_does_not_cause_next_flush_to_return_early(tmp_path):
    written = []
    release_first = threading.Event()
    first_seen = threading.Event()

    def slow_sink(message):
        text = str(message).strip()
        if "first" in text:
            first_seen.set()
            release_first.wait(timeout=2)
        written.append(text)

    logger = YydsLogger(
        "flush-seq",
        log_dir=str(tmp_path),
        console_level="CRITICAL",
        shutdown_timeout=0.05,
        compression=None,
    )
    logger.add(slow_sink, enqueue=True, queue_backend="thread", shutdown_timeout=0.05, format="{message}")
    try:
        logger.info("first")
        assert first_seen.wait(timeout=1)
        logger.info("second")
        with pytest.raises(TimeoutError):
            logger.flush()
        # A stale flush marker from the timed-out flush is still ahead of "third".
        logger.info("third")
        release_first.set()
        logger.reconfigure(shutdown_timeout=2.0)
        for handler in logger.logger._core.handlers.values():
            handler._shutdown_timeout = 2.0
        logger.flush()
        assert any("third" in item for item in written)
    finally:
        release_first.set()
        logger.cleanup()


def test_defer_format_error_with_catch_false_preserves_writer_thread(tmp_path):
    stderr = io.StringIO()
    calls = {"count": 0}

    def flaky_format(record):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("synthetic format error")
        return "{message}\n"

    logger = YydsLogger(
        "defer-catch-false",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        compression=None,
    )
    out_file = tmp_path / "defer_custom.log"
    handler_id = logger.add(
        str(out_file),
        enqueue=True,
        queue_backend="thread",
        defer_format=True,
        catch=False,
        format=flaky_format,
    )
    try:
        with contextlib.redirect_stderr(stderr):
            logger.info("bad-msg")
            logger.info("good-msg")
            logger.flush()
        handler = logger.logger._core.handlers[handler_id]
        assert handler.writer_alive is True
        assert handler.serialization_errors == 1
        assert "good-msg" in out_file.read_text(encoding="utf-8")
    finally:
        logger.cleanup()


def test_retention_ignores_other_prefix_active_and_archive_files(tmp_path):
    logger = YydsLogger(
        "app",
        log_dir=str(tmp_path),
        retention="1 second",
        compression=None,
        enqueue=False,
    )
    active = tmp_path / "app.log"
    own_archive = tmp_path / "app.2000-01-01_00-00-00.log"
    other_active = tmp_path / "app.worker.log"
    other_archive = tmp_path / "app.worker.2000-01-01_00-00-00.log"
    try:
        for p in (own_archive, other_active, other_archive):
            p.write_text("data\n", encoding="utf-8")
            os.utime(p, (time.time() - 10, time.time() - 10))
        logger._archive_retention(str(active))(
            [str(active), str(own_archive), str(other_active), str(other_archive)]
        )
        assert active.exists()
        assert not own_archive.exists()
        assert other_active.exists()
        assert other_archive.exists()
    finally:
        logger.cleanup()


def test_uncleaned_logger_is_gc_collected_and_stops_writer(tmp_path):
    import gc
    import weakref

    logger = YydsLogger("gc-collect", log_dir=str(tmp_path), console_level="CRITICAL", compression=None)
    writer_thread = next(
        h._thread for h in logger.logger._core.handlers.values() if h._enqueue
    )
    logger.info("gc-flushed-message")
    ref = weakref.ref(logger)
    del logger
    gc.collect()
    assert ref() is None
    writer_thread.join(timeout=2)
    assert not writer_thread.is_alive()
    assert "gc-flushed-message" in (tmp_path / "gc-collect.log").read_text(encoding="utf-8")


def test_blocked_queue_timeout_does_not_serialize_stop(tmp_path):
    gate = threading.Event()

    def blocking_sink(message):
        gate.wait(timeout=2)

    logger = YydsLogger(
        "stop-unblocks",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        shutdown_timeout=0.5,
    )
    logger.add(
        blocking_sink,
        enqueue=True,
        queue_backend="thread",
        queue_size=1,
        overflow_policy="block",
        queue_timeout=5.0,
        shutdown_timeout=0.5,
        format="{message}",
    )
    logger.info("fill-worker")
    logger.info("fill-queue")

    def blocked_producer():
        logger.info("blocked")

    threads = [threading.Thread(target=blocked_producer) for _ in range(3)]
    for t in threads:
        t.start()
    time.sleep(0.05)

    started = time.monotonic()
    gate.set()
    logger.cleanup()
    elapsed = time.monotonic() - started
    for t in threads:
        t.join(timeout=2)
    assert elapsed < 1.5
    assert all(not t.is_alive() for t in threads)


def test_level_gate_respects_custom_handlers_and_case(tmp_path):
    captured = []
    logger = YydsLogger(
        "custom-gate",
        log_dir=str(tmp_path),
        console_level="CRITICAL",
        file_level="ERROR",
        enable_stats=True,
        enqueue=False,
    )
    try:
        logger.add(lambda m: captured.append(str(m)), level="DEBUG", format="{message}")
        assert logger.is_level_enabled("DEBUG") is True
        assert logger.is_level_enabled("debug") is True
        assert logger.is_level_enabled("NONEXISTENT") is False

        @logger.time_it
        def work():
            return 1

        work()
        logger.debug("debug-custom")
        stats = logger.get_stats()
        assert stats["total"] == 2
        assert stats["debug"] == 1
        assert stats["info"] == 1
        assert any("debug-custom" in item for item in captured)
    finally:
        logger.cleanup()


def test_signal_handler_ignores_sig_ign_and_allows_custom_handler_logging(tmp_path):
    previous_sigterm = signal.getsignal(signal.SIGTERM)
    try:
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        logger_ign = YydsLogger("sig-ign", log_dir=str(tmp_path), enqueue=False, console_level="CRITICAL")
        try:
            logger_ign.setup_signal_handlers()
            installed = signal.getsignal(signal.SIGTERM)
            installed(signal.SIGTERM, None)
            assert logger_ign._cleanup_state == "open"
            logger_ign.info("still-open")
        finally:
            logger_ign.cleanup()

        custom_logged = []

        def custom_handler(signum, frame):
            logger_custom.info("graceful-shutdown-log")
            custom_logged.append(signum)

        signal.signal(signal.SIGTERM, custom_handler)
        logger_custom = YydsLogger("sig-custom", log_dir=str(tmp_path), enqueue=False, console_level="CRITICAL")
        try:
            logger_custom.setup_signal_handlers()
            installed = signal.getsignal(signal.SIGTERM)
            installed(signal.SIGTERM, None)
            deadline = time.monotonic() + 2.0
            while logger_custom._cleanup_state != "closed" and time.monotonic() < deadline:
                time.sleep(0.01)
            assert custom_logged == [signal.SIGTERM]
            assert logger_custom._cleanup_state == "closed"
            assert "graceful-shutdown-log" in (tmp_path / "sig-custom.log").read_text(encoding="utf-8")
        finally:
            logger_custom.cleanup()
    finally:
        signal.signal(signal.SIGTERM, previous_sigterm)


def test_async_sink_with_enqueue_true_schedules_safely(tmp_path):
    received = []

    async def scenario():
        logger = YydsLogger(
            "async-enqueued",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
        )

        async def sink(message):
            await asyncio.sleep(0.005)
            received.append(str(message).strip())

        logger.add(sink, enqueue=True, queue_backend="thread", format="{message}")
        logger.info("hello-enqueued-async")
        await logger.flush_async()
        await logger.aclose()

    asyncio.run(scenario())
    assert received == ["hello-enqueued-async"]


def test_line_profiler_escapes_backslash_tags_and_supports_recursion(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "line_profiler", None)
    logger = YydsLogger("prof-escape", log_dir=str(tmp_path), enqueue=False, console_level="CRITICAL")
    try:
        @logger.time_it(line_by_line=True)
        def recursive_fn(n):
            marker = r"\<red>"
            if n < 1:
                return len(marker)
            return recursive_fn(n - 1)

        assert recursive_fn(2) == len(r"\<red>")
        content = (tmp_path / "prof-escape.log").read_text(encoding="utf-8")
        assert "recursive_fn" in content
    finally:
        logger.cleanup()


def test_stdlib_bridge_early_filter_and_shallow_stack(tmp_path):
    class ExplodingStr:
        def __str__(self):
            raise AssertionError("must not format filtered stdlib record")

    name = "early-filter-bridge"
    target = logging.getLogger(name)
    orig_handlers = list(target.handlers)
    orig_level = target.level
    orig_propagate = target.propagate
    logger = YydsLogger(
        "bridge-opt",
        log_dir=str(tmp_path),
        file_level="WARNING",
        console_level="CRITICAL",
        enqueue=False,
    )
    try:
        logger.capture_std_logging(level="DEBUG", names=[name])
        target.debug("ignored %s", ExplodingStr())
        # Direct shallow emit call (< 6 frames)
        intercept = logger._std_logging_state["handler"]
        record = logging.LogRecord(name, logging.WARNING, __file__, 1, "shallow-warning", (), None)
        intercept.emit(record)
    finally:
        logger.cleanup()
        target.handlers = orig_handlers
        target.setLevel(orig_level)
        target.propagate = orig_propagate

    content = (tmp_path / "bridge-opt.log").read_text(encoding="utf-8")
    assert "shallow-warning" in content


def test_health_checker_counts_tar_archives(tmp_path):
    (tmp_path / "app.2026-01-01.log.tar").write_text("tar", encoding="utf-8")
    result = LogHealthChecker().check_health(str(tmp_path))
    assert result["metrics"]["log_files_count"] == 1


def test_multiprocessing_backend_writes_and_flushes_records(tmp_path):
    logger = YydsLogger(
        "mp-e2e",
        log_dir=str(tmp_path),
        queue_backend="multiprocessing",
        console_level="CRITICAL",
        compression=None,
    )
    try:
        logger.bind(user="alice").info("mp-ok-{}", 42)
        logger.flush()
    finally:
        logger.cleanup()
    content = (tmp_path / "mp-e2e.log").read_text(encoding="utf-8")
    assert "mp-ok-42" in content


def test_async_line_profiler_rejects_concurrent_sibling_tasks_and_allows_recursion(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "line_profiler", None)
    logger = YydsLogger("async-prof", log_dir=str(tmp_path), enqueue=False, console_level="CRITICAL")

    async def scenario():
        @logger.time_it(line_by_line=True)
        async def rec_async(n):
            if n <= 0:
                return 0
            return n + await rec_async(n - 1)

        assert await rec_async(2) == 3

        started = asyncio.Event()
        release = asyncio.Event()

        @logger.time_it(line_by_line=True)
        async def slow_coro():
            started.set()
            await release.wait()
            return "ok"

        t1 = asyncio.create_task(slow_coro())
        await started.wait()
        with pytest.raises(RuntimeError):
            await slow_coro()
        release.set()
        assert await t1 == "ok"

    try:
        asyncio.run(scenario())
    finally:
        logger.cleanup()


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(),
    reason="PID reinitialization is specific to fork-capable platforms",
)
def test_prefork_preserves_request_id_var_and_bound_logger(tmp_path):
    import multiprocessing

    logger = YydsLogger(
        "prefork-bound",
        log_dir=str(tmp_path),
        process_isolation=True,
        console_level="CRITICAL",
        compression=None,
    )
    logger.level("PREFORK", no=35)
    bound = logger.bind(component="prebound")
    req_var = logger.request_id_var
    parent_pid = os.getpid()

    def child_main():
        token = req_var.set("child-req-1")
        try:
            bound.log("PREFORK", "from-prebound")
        finally:
            req_var.reset(token)
        logger.cleanup()

    try:
        logger.info("parent-entry")
        process = multiprocessing.get_context("fork").Process(target=child_main)
        process.start()
        child_pid = process.pid
        process.join(timeout=5)
        assert process.exitcode == 0
    finally:
        logger.cleanup()

    parent_text = (tmp_path / f"prefork-bound.pid{parent_pid}.log").read_text(encoding="utf-8")
    child_text = (tmp_path / f"prefork-bound.pid{child_pid}.log").read_text(encoding="utf-8")
    assert "from-prebound" not in parent_text
    assert "from-prebound" in child_text
    assert "ReqID:child-req-1" in child_text


def test_log_decorator_preserves_mixed_case_custom_level(tmp_path):
    logger = YydsLogger(
        "mixed-level",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        compression=None,
    )
    logger.level("AuditLog", no=35)

    @logger.log_decorator(level="AuditLog", trace=False, reraise=False)
    def explode():
        raise ValueError("audit-boom")

    try:
        assert explode() is None
    finally:
        logger.cleanup()

    content = (tmp_path / "mixed-level.log").read_text(encoding="utf-8")
    assert "AuditLog" in content
    assert "audit-boom" in content


def test_capture_std_logging_accepts_int_level(tmp_path):
    name = "int-level-bridge"
    target = logging.getLogger(name)
    orig_handlers = list(target.handlers)
    orig_level = target.level
    orig_propagate = target.propagate
    logger = YydsLogger("std-int", log_dir=str(tmp_path), enqueue=False, console_level="CRITICAL")
    try:
        logger.capture_std_logging(level=logging.WARNING, names=[name])
        target.info("ignored-info")
        target.warning("captured-warning")
    finally:
        logger.cleanup()
        target.handlers = orig_handlers
        target.setLevel(orig_level)
        target.propagate = orig_propagate

    content = (tmp_path / "std-int.log").read_text(encoding="utf-8")
    assert "captured-warning" in content
    assert "ignored-info" not in content


def test_reconfigure_invalid_retention_or_format_keeps_attributes_untouched(tmp_path):
    logger = YydsLogger("reconf-validate", log_dir=str(tmp_path), language="zh", enqueue=False)
    try:
        with pytest.raises(ValueError):
            logger.reconfigure(language="en", retention="not-a-duration")
        assert logger.config.language == "zh"

        with pytest.raises(ValueError):
            logger.reconfigure(language="en", custom_format="<red>unclosed")
        assert logger.config.language == "zh"
    finally:
        logger.cleanup()


def test_aclose_timeout_retry_waits_for_previous_worker_cleanly(tmp_path):
    release = threading.Event()

    class BlockingSink:
        def write(self, message):
            return None

        def stop(self):
            release.wait()

    async def scenario():
        logger = YydsLogger(
            "aclose-retry",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
            shutdown_timeout=0.05,
        )
        logger.add(BlockingSink(), format="{message}")
        with pytest.raises((RuntimeError, TimeoutError)):
            await logger.aclose()
        assert logger._cleanup_state == "failed"
        release.set()
        await logger.aclose()
        assert logger._cleanup_state == "closed"

    asyncio.run(scenario())


@pytest.mark.skipif(
    "fork" not in __import__("multiprocessing").get_all_start_methods(),
    reason="Fork queue recovery is specific to fork-capable platforms",
)
def test_default_thread_queue_fork_logs_and_cleans_up_without_timeout(tmp_path):
    import multiprocessing

    logger = YydsLogger(
        "fork-default-thread",
        log_dir=str(tmp_path),
        process_isolation=False,
        enqueue=True,
        queue_backend="thread",
        console_level="CRITICAL",
        compression=None,
        shutdown_timeout=0.3,
    )

    def idle_child():
        logger.cleanup()

    def active_child():
        logger.info("from-active-child")
        logger.flush()
        logger.cleanup()

    try:
        logger.info("from-parent")
        ctx = multiprocessing.get_context("fork")

        p_idle = ctx.Process(target=idle_child)
        p_idle.start()
        p_idle.join(timeout=5)
        assert p_idle.exitcode == 0

        p_active = ctx.Process(target=active_child)
        p_active.start()
        p_active.join(timeout=5)
        assert p_active.exitcode == 0
    finally:
        logger.cleanup()

    content = (tmp_path / "fork-default-thread.log").read_text(encoding="utf-8")
    assert "from-parent" in content
    assert "from-active-child" in content


def test_reset_after_fork_fallback_keeps_stderr_usable_and_child_pid(tmp_path, monkeypatch):
    logger = YydsLogger(
        "fork-fallback",
        log_dir=str(tmp_path),
        process_isolation=True,
        enqueue=False,
        console_level="CRITICAL",
        compression=None,
    )
    stderr_buf = io.StringIO()
    try:
        logger._last_good_config["_process_file_name"] = "fork-fallback.pid999999"

        def fail_dir():
            raise OSError("read-only filesystem")

        monkeypatch.setattr(logger, "_ensure_log_directory", fail_dir)
        with contextlib.redirect_stderr(stderr_buf):
            logger._reset_after_fork()
            assert logger._process_file_name == f"fork-fallback.pid{os.getpid()}"
            assert logger._cleanup_state == "open"
            logger.error("stderr-fallback-after-fork")
    finally:
        logger.cleanup()

    assert "stderr-fallback-after-fork" in stderr_buf.getvalue()


def test_retention_and_file_name_validation_reject_invalid_boundaries(tmp_path):
    for bad_name in (".", "..", "a/b", "a\\b", "bad\x00name"):
        with pytest.raises(ValueError):
            YydsLogger(bad_name, log_dir=str(tmp_path), enqueue=False)

    for bad_retention in ("0s", "0 days", "-7 days", "1e100 days"):
        with pytest.raises(ValueError):
            YydsLogger("ret-check", log_dir=str(tmp_path), retention=bad_retention, enqueue=False)


def test_level_gating_when_all_handlers_removed_and_case_insensitive_sink_levels(tmp_path):
    logger = YydsLogger(
        "level-check",
        log_dir=str(tmp_path),
        console_level="warning",
        file_level=logging.INFO,
        enqueue=False,
        compression=None,
    )
    try:
        assert logger.is_level_enabled("INFO") is True
        assert logger.is_level_enabled("DEBUG") is False
        assert logger.is_level_enabled("LATE_CUSTOM") is False

        logger.level("LATE_CUSTOM", no=25)
        assert logger.is_level_enabled("LATE_CUSTOM") is True

        logger.reconfigure(console_level="error", file_level=logging.WARNING)
        assert logger.is_level_enabled("INFO") is False
        assert logger.is_level_enabled("WARNING") is True

        logger.remove()
        assert logger.is_level_enabled("CRITICAL") is False
    finally:
        logger.cleanup()


def test_handler_pickle_state_clears_owner_process_pid_and_sync_flush_skips_asyncio_run(
    tmp_path, monkeypatch
):
    logger = YydsLogger(
        "pickle-and-flush",
        log_dir=str(tmp_path),
        enqueue=True,
        queue_backend="thread",
        console_level="CRITICAL",
        compression=None,
    )
    try:
        file_handler = next(h for h in logger.logger._core.handlers.values() if h._enqueue)
        state = file_handler.__getstate__()
        assert state["_owner_process_pid"] is None
        assert "_owner_process" not in state

        def forbid_asyncio_run(*args, **kwargs):
            raise AssertionError("asyncio.run should not be called when there are no async tasks")

        monkeypatch.setattr(asyncio, "run", forbid_asyncio_run)
        logger.info("fast-flush-record")
        logger.flush()
    finally:
        logger.cleanup()

    assert "fast-flush-record" in (tmp_path / "pickle-and-flush.log").read_text(encoding="utf-8")


def test_windows_ctime_and_colorama_work_without_optional_packages(tmp_path, monkeypatch):
    from yyds_logger.yyds_loguru import _colorama, _ctime_functions

    monkeypatch.setitem(sys.modules, "win32_setctime", None)
    monkeypatch.setitem(sys.modules, "colorama", None)
    monkeypatch.setitem(sys.modules, "colorama.win32", None)
    monkeypatch.setitem(sys.modules, "colorama.winterm", None)
    monkeypatch.setattr(os, "name", "nt")

    get_ct, set_ct = _ctime_functions.load_ctime_functions()
    sample = tmp_path / "win.txt"
    sample.write_text("ok", encoding="utf-8")
    assert isinstance(get_ct(str(sample)), float)
    set_ct(str(sample), time.time())

    class FakeTTY:
        def isatty(self):
            return True

        def fileno(self):
            raise OSError("no console handle")

    fake_stdout = FakeTTY()
    monkeypatch.setattr(sys, "__stdout__", fake_stdout)
    monkeypatch.delenv("TERM", raising=False)
    monkeypatch.delenv("CI", raising=False)
    monkeypatch.delenv("PYCHARM_HOSTED", raising=False)

    assert _colorama.should_colorize(fake_stdout) is False
    assert _colorama.should_wrap(fake_stdout) is False
    assert _colorama.wrap(fake_stdout) is fake_stdout


def test_getattr_delegates_with_zero_extra_depth_and_closed_hasattr_is_safe(tmp_path):
    logger = YydsLogger(
        "getattr-depth",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        compression=None,
    )
    patched = logger.patch(lambda record: None)
    patched.info("patched-msg")
    with logger.catch():
        raise ZeroDivisionError("caught-div")
    logger.cleanup()

    assert hasattr(logger, "nonexistent_attribute_xyz") is False
    assert getattr(logger, "nonexistent_attribute_xyz", "fallback") == "fallback"
    assert hasattr(logger, "add") is True
    with pytest.raises(RuntimeError):
        logger.add(lambda m: None)

    content = (tmp_path / "getattr-depth.log").read_text(encoding="utf-8")
    assert "test_hardening.py:" in content
    assert "<unknown>:0" not in content
    assert "patched-msg" in content
    assert "caught-div" in content


@pytest.mark.parametrize("disable_line_profiler", [False, True])
def test_stacked_decorators_and_lambda_line_profiling_and_partial_callables(
    tmp_path, monkeypatch, disable_line_profiler
):
    import functools

    if disable_line_profiler:
        monkeypatch.setitem(sys.modules, "line_profiler", None)

    mod_path = tmp_path / "prof_sample_mod.py"
    mod_path.write_text(
        "def make_callables(logger):\n"
        "    @logger.time_it(line_by_line=True)\n"
        "    @logger.log_decorator()\n"
        "    def stacked(x):\n"
        "        y = x + 10\n"
        "        return y * 2\n"
        "    lam = logger.time_it(lambda z: z + 5, line_by_line=True)\n"
        "    return stacked, lam\n",
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    import importlib

    prof_mod = importlib.import_module("prof_sample_mod")
    importlib.reload(prof_mod)

    logger = YydsLogger(
        f"prof-stack-{disable_line_profiler}",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        compression=None,
    )
    try:
        stacked_fn, lam_fn = prof_mod.make_callables(logger)
        assert stacked_fn(3) == 26
        assert lam_fn(4) == 9

        part_fn = functools.partial(lambda a, b: a + b, 10)
        assert logger.log_decorator()(part_fn)(5) == 15
        assert logger.time_it(part_fn)(7) == 17
    finally:
        logger.cleanup()

    content = (tmp_path / f"prof-stack-{disable_line_profiler}.log").read_text(encoding="utf-8")
    assert "y = x + 10" in content
    assert "|        1 |" in content
    assert "<lambda>" in content


def test_gc_restores_stdlib_logging_and_releases_exception_and_signal_hooks(tmp_path):
    import gc
    import signal

    prev_exc = sys.excepthook
    prev_thr = threading.excepthook
    prev_sigterm = signal.getsignal(signal.SIGTERM)
    prev_sigint = signal.getsignal(signal.SIGINT)

    stream = io.StringIO()
    root = logging.getLogger()
    root_handler = logging.StreamHandler(stream)
    orig_root_level = root.level
    root.addHandler(root_handler)
    root.setLevel(logging.INFO)

    svc_name = "gc.ephemeral.service"
    svc_logger = logging.getLogger(svc_name)
    orig_svc_handlers = list(svc_logger.handlers)
    orig_svc_level = svc_logger.level
    orig_svc_prop = svc_logger.propagate

    try:
        def create_and_abandon():
            ephemeral = YydsLogger(
                "ephemeral-gc",
                log_dir=str(tmp_path),
                console_level="CRITICAL",
                compression=None,
            )
            ephemeral.capture_std_logging(names=[svc_name], level="INFO")
            ephemeral.setup_exception_handler()
            ephemeral.setup_signal_handlers()
            svc_logger.info("captured-in-ephemeral")

        create_and_abandon()
        gc.collect()

        assert sys.excepthook is prev_exc
        assert threading.excepthook is prev_thr
        assert signal.getsignal(signal.SIGTERM) is prev_sigterm
        assert signal.getsignal(signal.SIGINT) is prev_sigint
        assert svc_logger.propagate is True
        assert all(type(h).__name__ != "_InterceptHandler" for h in svc_logger.handlers)

        svc_logger.info("restored-after-gc")
        assert "restored-after-gc" in stream.getvalue()
        assert "captured-in-ephemeral" in (tmp_path / "ephemeral-gc.log").read_text(encoding="utf-8")

        successor = YydsLogger(
            "successor",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="CRITICAL",
            compression=None,
        )
        try:
            successor.setup_exception_handler()
            successor.setup_signal_handlers()
            successor.capture_std_logging(names=[svc_name], level="INFO")
        finally:
            successor.cleanup()
    finally:
        root.removeHandler(root_handler)
        root.setLevel(orig_root_level)
        svc_logger.handlers = orig_svc_handlers
        svc_logger.setLevel(orig_svc_level)
        svc_logger.propagate = orig_svc_prop
        sys.excepthook = prev_exc
        threading.excepthook = prev_thr
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)


def test_rotation_preserves_current_record_when_compression_fails(tmp_path):
    import functools
    from yyds_logger.yyds_loguru._file_sink import Compression, FileSink

    stderr_buf = io.StringIO()
    logger = YydsLogger(
        "rot-resilient",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="CRITICAL",
        compression="gz",
    )
    try:
        file_handler = next(
            h for h in logger.logger._core.handlers.values() if isinstance(h._sink, FileSink)
        )
        file_handler._sink._size_limit = 80

        def broken_compress(path_in, path_out):
            with open(path_out, "wb") as f:
                f.write(b"partial-corrupt")
            raise OSError("simulated disk full during gzip")

        file_handler._sink._compression_function = functools.partial(
            Compression.compression, ext=".gz", compress_function=broken_compress
        )

        with contextlib.redirect_stderr(stderr_buf):
            logger.info("first-record-" + "x" * 60)
            logger.info("second-record-survives-rotation")
    finally:
        logger.cleanup()

    active_content = (tmp_path / "rot-resilient.log").read_text(encoding="utf-8")
    assert "second-record-survives-rotation" in active_content
    gz_files = list(tmp_path.glob("*.gz"))
    assert gz_files == []


def test_async_sink_enqueue_complete_fast_and_strong_task_retention(tmp_path):
    import gc

    async def scenario():
        logger = YydsLogger(
            "async-sink-enqueue",
            log_dir=str(tmp_path),
            console_level="OFF",
            compression=None,
        )
        received = []
        gate = asyncio.Event()
        loop = asyncio.get_running_loop()

        async def fast_sink(msg):
            await asyncio.sleep(0.005)
            received.append(str(msg).strip())

        async def gated_sink(msg):
            await gate.wait()
            received.append("gated:" + str(msg).strip())

        try:
            fast_id = logger.logger.add(
                fast_sink,
                format="{message}",
                enqueue=True,
                loop=loop,
            )
            start = time.monotonic()
            for idx in range(10):
                logger.info(f"enqueued-{idx}")
            await logger.flush_async()
            elapsed = time.monotonic() - start
            assert elapsed < 0.8
            assert len(received) == 10
            logger.logger.remove(fast_id)

            gated_id = logger.logger.add(gated_sink, format="{message}", enqueue=False)
            gated_handler = logger.logger._core.handlers[gated_id]
            logger.info("hold-in-flight")
            await asyncio.sleep(0)
            gc.collect()
            assert len(gated_handler._sink._tasks) == 1
            gate.set()
            await logger.flush_async()
            assert "gated:hold-in-flight" in received
            assert len(gated_handler._sink._tasks) == 0
        finally:
            await logger.aclose()

    asyncio.run(scenario())


def test_defer_format_captures_exact_exception_locals_and_drops_live_frames(tmp_path):
    import gc
    import types
    import weakref
    from yyds_logger.yyds_loguru._file_sink import FileSink

    class FramePayload:
        pass

    logger = YydsLogger(
        "defer-exc",
        log_dir=str(tmp_path),
        env="dev",
        enqueue=True,
        defer_format=True,
        serialize=True,
        console_level="OFF",
        compression=None,
    )
    try:
        file_handler = next(
            h for h in logger.logger._core.handlers.values() if isinstance(h._sink, FileSink)
        )
        writer_gate = threading.Event()
        orig_write = file_handler._sink.write

        def paused_write(msg):
            writer_gate.wait(timeout=2.0)
            return orig_write(msg)

        file_handler._sink.write = paused_write
        # Block the writer thread on a warm-up record so the subsequent
        # DeferredMessage stays queued while we inspect frame retention.
        logger.info("warmup-pause")
        time.sleep(0.03)

        payload_ref = None

        def trigger_in_loop():
            nonlocal payload_ref
            for item_id in range(5):
                if item_id == 0:
                    local_payload = FramePayload()
                    payload_ref = weakref.ref(local_payload)
                    try:
                        _ = 1 / item_id
                    except ZeroDivisionError:
                        logger.exception("loop-failure")
                    del local_payload

        trigger_in_loop()
        gc.collect()
        assert payload_ref is not None
        assert payload_ref() is None

        queued_items = list(file_handler._queue._deque)
        deferred_items = [
            item for item in queued_items if hasattr(item, "preformatted_exception")
        ]
        assert len(deferred_items) == 1
        exc_rec = deferred_items[0].record["exception"]
        assert exc_rec is not None
        assert not isinstance(exc_rec.traceback, types.TracebackType)
        assert exc_rec.traceback is True

        writer_gate.set()
        logger.flush()
    finally:
        logger.cleanup()

    lines = [
        json.loads(line)
        for line in (tmp_path / "defer-exc.log").read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    err_entry = next(item for item in lines if item["record"]["message"] == "loop-failure")
    assert err_entry["record"]["exception"]["type"] == "ZeroDivisionError"
    assert "division by zero" in err_entry["record"]["exception"]["value"]
    assert err_entry["record"]["exception"]["traceback"] is True
    assert "_ = 1 / item_id" in err_entry["text"]
    assert "└ 0" in err_entry["text"]
    assert "└ 4" not in err_entry["text"]


def test_serialize_handles_unprintable_exception_and_extra_objects(tmp_path):
    class BadStrError(Exception):
        def __str__(self):
            raise RuntimeError("broken exception __str__")

    class BadStrObj:
        def __str__(self):
            raise RuntimeError("broken object __str__")

    logger = YydsLogger(
        "unprintable-json",
        log_dir=str(tmp_path),
        enqueue=False,
        serialize=True,
        console_level="OFF",
        compression=None,
    )
    try:
        err = BadStrError("hidden")
        with contextlib.redirect_stderr(io.StringIO()):
            logger.bind(bad_obj=BadStrObj()).opt(exception=err).error("unprintable-check")
    finally:
        logger.cleanup()

    raw = (tmp_path / "unprintable-json.log").read_text(encoding="utf-8").strip()
    parsed = json.loads(raw)
    assert parsed["record"]["message"] == "unprintable-check"
    assert parsed["record"]["exception"]["type"] == "BadStrError"
    assert "<unprintable BadStrError object>" in parsed["record"]["exception"]["value"]
    assert "<unprintable BadStrObj object>" in parsed["record"]["extra"]["bad_obj"]


def test_health_and_metrics_track_sink_errors_and_sync_format_drops_across_reconfigure(tmp_path):
    logger = YydsLogger(
        "health-metrics",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="OFF",
        language="en",
        compression=None,
    )
    try:
        def bad_formatter(record):
            raise KeyError("boom_dynamic_format")

        bad_fmt_id = logger.logger.add(
            lambda m: None,
            format=bad_formatter,
            enqueue=False,
            catch=True,
        )
        with contextlib.redirect_stderr(io.StringIO()):
            logger.info("triggers-sync-format-error")
        assert logger.get_queue_dropped() >= 1
        assert logger.get_queue_serialization_errors() >= 1
        assert logger.get_queue_status()["drop_reasons"]["serialization"] >= 1
        logger.logger.remove(bad_fmt_id)

        def broken_sink(msg):
            raise OSError("disk I/O write failure")

        broken_id = logger.logger.add(broken_sink, enqueue=False, catch=True)
        with contextlib.redirect_stderr(io.StringIO()):
            logger.error("triggers-sink-error")
        assert logger.get_sink_errors() == 1
        health_warn = logger.get_health()
        assert health_warn["status"] == "warning"
        assert any("sink write errors: 1" in w for w in health_warn["warnings"])

        logger.logger.remove(broken_id)
        logger.reconfigure(enqueue=True, file_level="DEBUG")
        file_handler = logger.logger._core.handlers[logger._handler_ids[-1]]
        file_handler._sink_errors = 2
        assert logger.get_sink_errors() == 2
        logger.reconfigure(file_level="INFO")
        assert logger.get_sink_errors() == 2
        health_after_reconfig = logger.get_health()
        assert health_after_reconfig["status"] == "warning"

        # Simulate dead writer thread on active enqueued handler -> critical health.
        active_qh = logger._queue_handlers()[0]
        orig_thread = active_qh._thread
        active_qh._thread = None
        try:
            health_crit = logger.get_health()
            assert health_crit["status"] == "critical"
            assert any("writer is not alive" in e for e in health_crit["errors"])
        finally:
            active_qh._thread = orig_thread
    finally:
        logger.cleanup()


def test_console_level_off_or_false_skips_console_handler_and_validates_strictly(tmp_path):
    from yyds_logger.yyds_loguru._colorizer import ColoringMessage

    cm = ColoringMessage("hello")
    assert not hasattr(cm, "__dict__")

    stderr_buf = io.StringIO()
    with contextlib.redirect_stderr(stderr_buf):
        logger = YydsLogger(
            "headless-console",
            log_dir=str(tmp_path),
            enqueue=False,
            console_level="off",
            compression=None,
        )
        try:
            assert len(logger._handler_ids) == 1
            logger.error("only-in-file")
            assert stderr_buf.getvalue() == ""

            logger.reconfigure(console_level="INFO")
            assert len(logger._handler_ids) == 2

            logger.reconfigure(console_level=False)
            assert len(logger._handler_ids) == 1

            with pytest.raises(ValueError):
                logger.reconfigure(console_level=True)
            with pytest.raises(ValueError):
                logger.reconfigure(file_level="OFF")
            with pytest.raises(ValueError):
                logger.reconfigure(file_level=False)
        finally:
            logger.cleanup()

    assert "only-in-file" in (tmp_path / "headless-console.log").read_text(encoding="utf-8")


@pytest.mark.skipif(not hasattr(os, "fork"), reason="POSIX fork only")
def test_fork_and_multiprocessing_child_get_health_does_not_false_alarm(tmp_path):
    import pickle

    lg_thread = YydsLogger(
        "fork-health-thread",
        log_dir=str(tmp_path),
        console_level="OFF",
        language="en",
        compression=None,
    )
    lg_mp = YydsLogger(
        "fork-health-mp",
        log_dir=str(tmp_path),
        enqueue=True,
        queue_backend="multiprocessing",
        console_level="OFF",
        language="en",
        compression=None,
    )
    try:
        with pytest.raises(TypeError, match="queue_backend='multiprocessing'"):
            pickle.dumps(lg_thread.logger)

        r_fd, w_fd = os.pipe()
        pid = os.fork()
        if pid == 0:
            os.close(r_fd)
            h_before = lg_thread.get_health()
            lg_thread.info("wake-child-writer")
            lg_thread.flush()
            h_after = lg_thread.get_health()
            h_mp = lg_mp.get_health()
            payload = json.dumps(
                {
                    "before": h_before["status"],
                    "before_errors": h_before["errors"],
                    "after": h_after["status"],
                    "mp": h_mp["status"],
                    "mp_errors": h_mp["errors"],
                }
            ).encode("utf-8")
            os.write(w_fd, payload)
            os.close(w_fd)
            os._exit(0)

        os.close(w_fd)
        raw = os.read(r_fd, 8192)
        os.close(r_fd)
        _, wait_status = os.waitpid(pid, 0)
        assert wait_status == 0
        report = json.loads(raw.decode("utf-8"))
        assert report["before"] == "healthy", report
        assert report["after"] == "healthy", report
        assert report["mp"] == "healthy", report
    finally:
        lg_mp.cleanup()
        lg_thread.cleanup()


def test_log_decorator_and_log_method_normalize_levels_and_survive_unprintable_errors(tmp_path):
    class UnprintableBoom(Exception):
        def __str__(self):
            raise RuntimeError("broken __str__")

    logger = YydsLogger(
        "decorator-levels",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="OFF",
        language="en",
        compression=None,
    )
    try:
        logger.log("info", "lowercase-info-works")
        logger.log(" warning ", "padded-warning-works")
        with pytest.raises(TypeError):
            logger.log(True, "bool-rejected")  # type: ignore[arg-type]

        @logger.log_decorator(level=logging.WARNING, reraise=False, trace=False)
        def fail_int():
            raise ValueError("int-level-boom")

        @logger.log_decorator(level=" warning ", reraise=False, trace=False)
        def fail_padded():
            raise UnprintableBoom("hidden")

        fail_int()
        fail_padded()
    finally:
        logger.cleanup()

    content = (tmp_path / "decorator-levels.log").read_text(encoding="utf-8")
    assert "INFO     |" in content and "lowercase-info-works" in content
    assert "padded-warning-works" in content
    assert "WARNING  |" in content and "int-level-boom" in content
    assert "<unprintable UnprintableBoom object>" in content
    assert "Error occurred while logging exception" not in content


def test_stdlib_bridge_honors_stacklevel_and_replayed_log_records(tmp_path):
    logger = YydsLogger(
        "stdlib-stacklevel",
        log_dir=str(tmp_path),
        enqueue=False,
        console_level="OFF",
        compression=None,
    )
    svc_name = f"stdlib.stacklevel.{id(tmp_path)}"
    std_logger = logging.getLogger(svc_name)
    try:
        logger.capture_std_logging(names=[svc_name], level="INFO")

        def wrapper_helper(msg):
            std_logger.warning(msg, stacklevel=2)

        expected_caller_line = None

        def real_caller():
            nonlocal expected_caller_line
            expected_caller_line = inspect.currentframe().f_lineno + 1
            wrapper_helper("stacklevel-two-msg")

        import inspect
        real_caller()

        replayed = logging.makeLogRecord(
            {
                "name": svc_name,
                "levelno": logging.ERROR,
                "levelname": "ERROR",
                "pathname": "/srv/remote_worker/job.py",
                "filename": "job.py",
                "module": "job",
                "lineno": 777,
                "funcName": "run_job",
                "msg": "replayed-from-queue-listener",
                "args": (),
            }
        )
        std_logger.handle(replayed)
    finally:
        logger.cleanup()

    content = (tmp_path / "stdlib-stacklevel.log").read_text(encoding="utf-8")
    assert f"test_hardening.py:{expected_caller_line}" in content
    assert "job.py:777" in content
    assert "replayed-from-queue-listener" in content


def test_exception_formatter_tuple_none_value_and_12h_parse_time(tmp_path):
    import datetime as dt
    from yyds_logger.yyds_loguru._string_parsers import parse_time

    assert parse_time("10:30 PM") == dt.time(22, 30)
    assert parse_time("02:15 am") == dt.time(2, 15)

    class BadAssertionError(AssertionError):
        def __str__(self):
            raise RuntimeError("cannot stringify assertion")

    logger = YydsLogger(
        "exc-tuple-none",
        log_dir=str(tmp_path),
        env="dev",
        enqueue=False,
        console_level="OFF",
        compression=None,
    )
    try:
        logger.logger.opt(exception=(ValueError, None, None)).error("valueless-tuple")
        try:
            raise BadAssertionError("bad")
        except BadAssertionError:
            logger.exception("bad-assertion-check")
    finally:
        logger.cleanup()

    content = (tmp_path / "exc-tuple-none.log").read_text(encoding="utf-8")
    assert "valueless-tuple\nValueError\n" in content
    assert "NoneType: None" not in content
    assert "bad-assertion-check" in content
