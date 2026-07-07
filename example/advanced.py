import asyncio
import contextvars
import hashlib
import os
import sys
import threading
import time
from datetime import datetime

# sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from yyds_logger import YydsLogger
from yyds_logger.advanced_features import (
    DistributedLogger,
    LogAggregator,
    LogAnalyzer,
    LogArchiver,
    LogBackupManager,
    LogDatabase,
    LogHealthChecker,
    LogSecurity,
    LogStreamProcessor,
    PerformanceMonitor,
)


def run_pipeline() -> None:
    processor = LogStreamProcessor(max_queue_size=1000)

    def add_timestamp(entry):
        entry["processed_ts"] = datetime.now().isoformat(timespec="seconds")
        return entry

    def add_checksum(entry):
        msg = str(entry.get("message") or "")
        entry["checksum"] = hashlib.md5(msg.encode("utf-8")).hexdigest()[:8]
        return entry

    processor.add_processor(add_timestamp)
    processor.add_processor(add_checksum)

    ok = processor.process_log({"level": "INFO", "message": "pipeline message"})
    processed = processor.get_processed_log(timeout=1.0) if ok else None
    print("pipeline processed:", processed)
    processor.stop()


def run_security() -> None:
    security = LogSecurity()
    print(security.sanitize_message("用户密码: 123456, token=abcd"))
    print(security.sanitize_mapping({"user": "alice", "password": "p", "nested": {"api_key": "k"}}))

    try:
        import cryptography as _cryptography
        _ = _cryptography.__name__

        enc = LogSecurity(enable_encryption=True)
        encrypted = enc.encrypt_bytes(b"hello")
        decrypted = enc.decrypt_bytes(encrypted)
        print("encryption ok:", decrypted == b"hello")
    except Exception as e:
        print("encryption unavailable:", str(e))


def run_storage_and_ops(base_dir: str) -> None:
    log_dir = os.path.join(base_dir, "logs_adv")
    os.makedirs(log_dir, exist_ok=True)

    logger = YydsLogger(file_name="advanced", log_dir=log_dir, language="zh", enable_stats=True)
    token = logger.request_id_var.set("req-adv-001")
    try:
        logger.info("advanced start")
        logger.error("advanced error sample")
    finally:
        logger.request_id_var.reset(token)

    db_path = os.path.join(base_dir, "logs_adv.db")
    db = LogDatabase(db_path)
    db.insert_log(
        {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "level": "ERROR",
            "message": "db insert sample",
            "file": "advanced.py",
            "line": 1,
            "function": "run_storage_and_ops",
            "extra_data": {"request_id": "req-adv-001"},
        }
    )
    print("db query:", db.query_logs({"level": "ERROR"}, limit=5))
    db.close()

    archiver = LogArchiver(os.path.join(base_dir, "archives"))
    archived = archiver.archive_logs(log_dir, days_old=0, compression_type="gzip", delete_original=False)
    print("archived:", archived)

    backup_mgr = LogBackupManager(os.path.join(base_dir, "backups"))
    backup_path = backup_mgr.create_backup(log_dir, "daily_backup")
    print("backup created:", backup_path)
    restore_dir = os.path.join(base_dir, "restore")
    ok = backup_mgr.restore_backup(backup_path, restore_dir)
    print("restore ok:", ok)
    print("backups:", backup_mgr.list_backups()[:1])

    checker = LogHealthChecker()
    print("health:", checker.check_health(log_dir))

    monitor = PerformanceMonitor(sample_interval=1.0)
    monitor.record_log("INFO", 0.01)
    monitor.record_log("ERROR", 0.05)
    print("monitor:", monitor.get_metrics())
    monitor.stop()

    logger.cleanup()


async def run_concurrency_examples() -> None:
    base_dir = os.path.dirname(__file__)
    log_dir = os.path.join(base_dir, "logs_adv")
    logger = YydsLogger(file_name="adv_concurrency", log_dir=log_dir, language="zh", enable_stats=True)

    async def handle(req_id: str) -> None:
        token = logger.request_id_var.set(req_id)
        try:
            logger.info("async handle start")
            await asyncio.sleep(0.05)
            logger.info("async handle end")
        finally:
            logger.request_id_var.reset(token)

    await asyncio.gather(handle("req-1"), handle("req-2"))

    token = logger.request_id_var.set("req-thread-1")
    try:
        ctx = contextvars.copy_context()

        def worker():
            ctx.run(logger.info, "thread log with inherited request_id")

        t = threading.Thread(target=worker)
        t.start()
        t.join()
    finally:
        logger.request_id_var.reset(token)

    logger.cleanup()


def run_aggregation_and_analyze() -> None:
    analyzer = LogAnalyzer()
    print(
        "analysis:",
        analyzer.analyze_log({"message": "Connection refused", "level": "ERROR"}),
    )

    aggregated_out = []

    def on_flush(items):
        aggregated_out.extend(items)

    aggregator = LogAggregator(window_size=10, flush_interval=1.0, on_flush=on_flush)
    for _ in range(5):
        aggregator.add_log({"level": "INFO", "message": "same message", "timestamp": time.time()})
    aggregator.flush()
    aggregator.stop()
    print("aggregated:", aggregated_out)


def run_distributed_id() -> None:
    dist = DistributedLogger("node-001", persist_every=10)
    ids = [dist.get_log_id() for _ in range(3)]
    dist.flush()
    print("distributed ids:", ids)


def main() -> None:
    base_dir = os.path.dirname(__file__)
    run_pipeline()
    run_security()
    run_distributed_id()
    run_aggregation_and_analyze()
    run_storage_and_ops(base_dir)
    asyncio.run(run_concurrency_examples())
    print("advanced example finished at", datetime.now().isoformat(timespec="seconds"))


if __name__ == "__main__":
    main()
