import logging
import asyncio
import json
import os
import signal
import sys
import threading
import time
import pytest

from yyds_logger import LogHealthChecker, YydsLogger
from yyds_logger.i18n import LANG_MAP
from yyds_logger.profiling import _trace_sync


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


def test_error_file_is_always_created(tmp_path):
    logger = YydsLogger("default-error-file", log_dir=str(tmp_path), enqueue=False)
    try:
        logger.error("error")
    finally:
        logger.cleanup()
    assert (tmp_path / "default-error-file.log").exists()
    assert (tmp_path / "default-error-file_error.log").exists()


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


def test_size_rotation_applies_to_main_and_error_files(tmp_path, monkeypatch):
    logger = YydsLogger("size-rotation", log_dir=str(tmp_path), enqueue=False)
    original_add = logger.logger.add
    seen_rotations = []

    def record_add(sink, *args, **kwargs):
        if isinstance(sink, str):
            seen_rotations.append(kwargs.get("rotation"))
        return original_add(sink, *args, **kwargs)

    try:
        monkeypatch.setattr(logger.logger, "add", record_add)
        logger.max_size = 20
        logger.configure_logger()
        assert seen_rotations == ["20 MB", "20 MB"]
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
    error_archive = tmp_path / ("retention_error.2000-01-01_00-00-00" + retained_suffix)
    try:
        for path in (matching_archive, other_archive_type, error_archive):
            path.write_text("archive", encoding="utf-8")
            os.utime(path, (time.time() - 10, time.time() - 10))

        logger._archive_retention(str(active))(
            [str(active), str(matching_archive), str(other_archive_type), str(error_archive)]
        )

        assert active.exists()
        assert not matching_archive.exists()
        assert other_archive_type.exists()
        assert error_archive.exists()
    finally:
        logger.cleanup()


def test_cleanup_can_retry_after_failure(tmp_path, monkeypatch):
    logger = YydsLogger("cleanup-retry", log_dir=str(tmp_path),
                        enqueue=False)
    original_remove = logger._remove_handlers
    failed = {"once": False}

    def fail_once(wait=False):
        if not failed["once"]:
            failed["once"] = True
            raise RuntimeError("synthetic cleanup failure")
        return original_remove(wait=wait)

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
        assert queue_status == {
            "enabled": False,
            "size": 12,
            "overflow_policy": "drop",
            "timeout": None,
            "dropped_messages": 0,
        }
        health = logger.get_health()
        assert health["logger"]["state"] == "open"
        assert health["logger"]["enqueue"] is False
        assert health["logger"]["handler_count"] == 3
    finally:
        logger.cleanup()


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
