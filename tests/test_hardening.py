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

        logger.add(sink, format="{message}")
        logger.info("slow-async-sink")
        with pytest.raises(TimeoutError):
            await logger.flush_async()
        assert logger._cleanup_state == "open"
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
