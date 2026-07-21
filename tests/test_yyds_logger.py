import shutil
import logging
import threading
import time
import pytest
from pathlib import Path

from yyds_logger import YydsLogger

TEST_DIR = Path(__file__).parent.parent / "test_logs"


@pytest.fixture(autouse=True)
def setup_and_teardown():
    # 确保测试日志目录存在且干净
    if TEST_DIR.exists():
        shutil.rmtree(TEST_DIR)
    TEST_DIR.mkdir(parents=True, exist_ok=True)
    yield
    # 清理日志目录
    if TEST_DIR.exists():
        try:
            shutil.rmtree(TEST_DIR)
        except Exception:
            pass


def test_multi_instance_isolation():
    """测试多实例日志引擎的完全物理隔离，确保无重复或交叉写入"""
    logger_a = YydsLogger("logger_a", log_dir=str(TEST_DIR))
    logger_b = YydsLogger("logger_b", log_dir=str(TEST_DIR))

    logger_a.info("This is message A")
    logger_b.info("This is message B")

    # 强制清理排空缓冲以确保写入磁盘
    logger_a.cleanup()
    logger_b.cleanup()

    log_file_a = TEST_DIR / "logger_a.log"
    log_file_b = TEST_DIR / "logger_b.log"

    assert log_file_a.exists()
    assert log_file_b.exists()

    content_a = log_file_a.read_text(encoding="utf-8")
    content_b = log_file_b.read_text(encoding="utf-8")

    # logger_a 的日志文件中只应包含 A 消息，绝对不包含 B 消息
    assert "This is message A" in content_a
    assert "This is message B" not in content_a

    # logger_b 的日志文件中只应包含 B 消息，绝对不包含 A 消息
    assert "This is message B" in content_b
    assert "This is message A" not in content_b


def test_concurrency_logging():
    """测试高频并发日志写入的稳定性"""
    logger = YydsLogger("concurrency_test", log_dir=str(TEST_DIR))
    
    stop_event = threading.Event()

    def log_worker():
        while not stop_event.is_set():
            logger.info("Concurrency stress log message")
            time.sleep(0.001)

    # 启动多个高频写入线程
    threads = []
    for _ in range(5):
        t = threading.Thread(target=log_worker)
        threads.append(t)
        t.start()

    # 运行压测一段时间
    time.sleep(1.5)
    stop_event.set()

    for t in threads:
        t.join()

    logger.cleanup()

    # 确保主文件正常创建且有日志内容写入
    log_file = TEST_DIR / "concurrency_test.log"
    assert log_file.exists()
    assert len(log_file.read_text(encoding="utf-8").strip()) > 0


def test_capture_std_logging():
    """测试接管标准 Python logging 库日志的正确性"""
    logger = YydsLogger("intercept_test", log_dir=str(TEST_DIR))
    
    # 接管特定的 stdlib logger "test_intercept"
    logger.capture_std_logging(level="WARNING", names=["test_intercept"])

    std_log = logging.getLogger("test_intercept")
    std_log.warning("This standard warning should be intercepted")
    std_log.info("This info should NOT be intercepted (due to level WARNING)")

    logger.cleanup()

    log_file = TEST_DIR / "intercept_test.log"
    assert log_file.exists()
    content = log_file.read_text(encoding="utf-8")

    assert "This standard warning should be intercepted" in content
    assert "This info should NOT be intercepted" not in content


def test_contextualize_isolation():
    """测试不同日志实例之间的 contextualize 上下文完全物理隔离，防止上下文泄漏"""
    logger_a = YydsLogger("logger_a", log_dir=str(TEST_DIR))
    logger_b = YydsLogger("logger_b", log_dir=str(TEST_DIR))

    # 仅在 logger_a 的上下文管理器中绑定变量
    with logger_a.contextualize(instance_a_val="AAA"):
        # 用 logger_b 记录日志，期望其日志不包含该变量
        logger_b.info("Message from B")

    logger_a.cleanup()
    logger_b.cleanup()

    log_file_b = TEST_DIR / "logger_b.log"
    assert log_file_b.exists()
    content_b = log_file_b.read_text(encoding="utf-8")

    # logger_b 的日志文件中不应包含 instance_a_val 变量的值
    assert "instance_a_val" not in content_b
    assert "AAA" not in content_b
