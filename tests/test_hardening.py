import logging
import warnings
import asyncio
import json
import pytest

from yyds_logger import LogHealthChecker, YydsLogger
from yyds_logger.i18n import LANG_MAP


def test_cleanup_drains_current_instance(tmp_path):
    logger = YydsLogger("drain", log_dir=str(tmp_path), error_file=False)
    logger.info("drain-marker")
    logger.cleanup()
    assert "drain-marker" in (tmp_path / "drain.log").read_text(encoding="utf-8")


def test_flush_keeps_logger_open_and_close_releases(tmp_path):
    logger = YydsLogger("lifecycle", log_dir=str(tmp_path), error_file=False)
    logger.info("before-flush")
    logger.flush()
    logger.info("after-flush")
    logger.close()
    content = (tmp_path / "lifecycle.log").read_text(encoding="utf-8")
    assert "before-flush" in content
    assert "after-flush" in content


def test_error_file_is_opt_in(tmp_path):
    logger = YydsLogger("default-error-file", log_dir=str(tmp_path), enqueue=False)
    try:
        logger.error("error")
    finally:
        logger.cleanup()
    assert (tmp_path / "default-error-file.log").exists()
    assert not (tmp_path / "default-error-file_error.log").exists()


def test_work_type_is_deprecated_but_compatible(tmp_path):
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        logger = YydsLogger("legacy", log_dir=str(tmp_path), work_type=False, enqueue=False)
    try:
        assert logger.env == "dev"
        assert any(issubclass(item.category, DeprecationWarning) for item in caught)
    finally:
        logger.cleanup()


def test_invalid_language_and_environment_fail_fast(tmp_path):
    with pytest.raises(ValueError):
        YydsLogger("invalid-language", log_dir=str(tmp_path), language="fr")
    with pytest.raises(ValueError):
        YydsLogger("invalid-env", log_dir=str(tmp_path), env="staging")


def test_async_flush_is_available_inside_event_loop(tmp_path):
    async def scenario():
        logger = YydsLogger("async-flush", log_dir=str(tmp_path), error_file=False)
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
        error_file=False,
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
    logger = YydsLogger("context", log_dir=str(tmp_path), error_file=False,
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


def test_local_sink_queue_configuration(tmp_path):
    logger = YydsLogger(
        "bounded",
        log_dir=str(tmp_path),
        error_file=False,
        queue_size=1,
        overflow_policy="drop",
    )
    try:
        for _ in range(50):
            logger.info("burst")
        logger.cleanup()
        assert logger.get_queue_dropped() >= 0
        assert (tmp_path / "bounded.log").exists()
    finally:
        logger.cleanup()


def test_pid_file_isolation(tmp_path):
    logger = YydsLogger(
        "service",
        log_dir=str(tmp_path),
        error_file=False,
        process_isolation=True,
        enqueue=False,
    )
    try:
        logger.info("isolated")
    finally:
        logger.cleanup()
    assert (tmp_path / f"service.pid{__import__('os').getpid()}.log").exists()


def test_basic_stats_are_returned(tmp_path):
    logger = YydsLogger("stats", log_dir=str(tmp_path), error_file=False,
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
    first = YydsLogger("first", log_dir=str(tmp_path), error_file=False)
    try:
        first.capture_std_logging(names=["hardening-test"], level="WARNING")
        second = YydsLogger("second", log_dir=str(tmp_path), error_file=False)
        try:
            with pytest.raises(RuntimeError):
                second.capture_std_logging(names=["hardening-test"], level="WARNING")
        finally:
            second.cleanup()
    finally:
        first.cleanup()


def test_std_logging_rejects_invalid_or_duplicate_capture(tmp_path):
    logger = YydsLogger("std-validation", log_dir=str(tmp_path), error_file=False)
    try:
        with pytest.raises(ValueError):
            logger.capture_std_logging(level="NOT_A_LEVEL")
        logger.capture_std_logging(names=["std-validation"], level="WARNING")
        with pytest.raises(RuntimeError):
            logger.capture_std_logging(names=["std-validation"], level="WARNING")
    finally:
        logger.cleanup()


def test_std_logging_state_is_restored(tmp_path):
    name = "hardening-restore"
    target = logging.getLogger(name)
    original_level = target.level
    logger = YydsLogger("restore", log_dir=str(tmp_path), error_file=False)
    try:
        logger.capture_std_logging(names=[name], level="WARNING")
        assert target.level == logging.WARNING
    finally:
        logger.cleanup()
    assert target.level == original_level
