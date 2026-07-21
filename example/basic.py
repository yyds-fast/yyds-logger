import asyncio
import os
import sys
import json
import time
import threading
import contextvars
from datetime import datetime

# sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from yyds_logger import YydsLogger


# ---------------------------------------------------------------------------
# 1. 基础日志 —— 各级别与结构化上下文
# ---------------------------------------------------------------------------
def run_basic() -> None:
    """演示最常用的日志方法"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic",
        log_dir=log_dir,
        language="zh",
        enable_stats=True,
        compression="gz",
    )

    token = logger.request_id_var.set("req-basic-001")
    try:
        # 各级别日志
        logger.debug("基础日志: debug")
        logger.info("基础日志: info")
        logger.warning("基础日志: warning")
        logger.error("基础日志: error")
        logger.critical("基础日志: critical")

        with logger.contextualize(
            user_id=123,
            method="POST",
            path="/api/orders",
            ip="192.168.1.100",
        ):
            logger.info("API 请求处理")
            logger.bind(
                db_query_ms=23,
                cache_lookup_ms=2,
                serialization_ms=5,
                total_ms=81,
            ).info("请求耗时分解")
    finally:
        logger.request_id_var.reset(token)

    # 统计信息
    print("stats:", logger.get_stats())
    logger.cleanup()


# ---------------------------------------------------------------------------
# 2. 函数装饰器
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 3. 同步装饰器 —— 自动记录出入参和耗时
# ---------------------------------------------------------------------------
def run_sync_decorator() -> None:
    """演示 log_decorator 在同步函数上的使用"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_sync_dec", log_dir=log_dir, language="zh")

    @logger.log_decorator()
    def calculate_order_total(items: list, discount: float = 0.0) -> float:
        total = sum(items)
        return total * (1 - discount)

    token = logger.request_id_var.set("req-calc-001")
    try:
        result = calculate_order_total([99.9, 49.5, 199.0], discount=0.1)
        logger.info(f"订单总额: {result:.2f}")
    finally:
        logger.request_id_var.reset(token)
    logger.cleanup()


# ---------------------------------------------------------------------------
# 4. 异常捕获 —— 装饰器自动记录异常堆栈
# ---------------------------------------------------------------------------
def run_exception_handling() -> None:
    """演示 log_decorator 捕获异常的行为"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_exception", log_dir=log_dir, language="zh")

    @logger.log_decorator(level="ERROR", trace=True)
    def risky_operation(data: dict) -> str:
        # 故意触发 KeyError
        return data["missing_key"]

    token = logger.request_id_var.set("req-err-001")
    try:
        try:
            risky_operation({"name": "test"})
        except KeyError:
            logger.warning("已捕获预期异常，业务继续")
    finally:
        logger.request_id_var.reset(token)
    logger.cleanup()


# ---------------------------------------------------------------------------
# 5. Context Manager (with 语句) —— 自动资源清理
# ---------------------------------------------------------------------------
def run_context_manager() -> None:
    """演示 with 语句管理 logger 生命周期"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    with YydsLogger(file_name="basic_ctx", log_dir=log_dir, language="zh", enable_stats=True) as logger:
        token = logger.request_id_var.set("req-ctx-001")
        try:
            logger.info("进入 with 作用域")
            with logger.contextualize(step=1, status="running"):
                logger.info("处理中")
            logger.info("即将离开 with 作用域，资源将自动清理")
        finally:
            logger.request_id_var.reset(token)
    print("context manager: logger 已自动 cleanup")


# ---------------------------------------------------------------------------
# 9. 异步装饰器 + 并发请求
# ---------------------------------------------------------------------------
async def run_async_decorator() -> None:
    """演示异步装饰器和并发任务"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_async", log_dir=log_dir, language="zh", enable_stats=True)

    @logger.log_decorator()
    async def fetch_user(user_id: int) -> dict:
        await asyncio.sleep(0.03)
        return {"id": user_id, "name": f"user_{user_id}"}

    @logger.log_decorator()
    async def process_order(order_id: str) -> str:
        await asyncio.sleep(0.05)
        return f"order_{order_id}_processed"

    async def handle_request(req_id: str, user_id: int, order_id: str) -> None:
        token = logger.request_id_var.set(req_id)
        try:
            user = await fetch_user(user_id)
            logger.info(f"获取用户: {user}")
            result = await process_order(order_id)
            logger.info(f"订单处理结果: {result}")
        finally:
            logger.request_id_var.reset(token)

    # 模拟 3 个并发请求
    await asyncio.gather(
        handle_request("req-a", 1001, "A001"),
        handle_request("req-b", 1002, "B002"),
        handle_request("req-c", 1003, "C003"),
    )
    print("async stats:", logger.get_stats())
    logger.cleanup()


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# 10. 多线程 —— 使用 contextvars 传递 request_id
# ---------------------------------------------------------------------------
def run_multithreading() -> None:
    """演示多线程中 request_id 的隔离与传递"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_thread", log_dir=log_dir, language="zh", enable_stats=True)

    def worker(name: str, req_id: str) -> None:
        token = logger.request_id_var.set(req_id)
        try:
            logger.info(f"线程 {name} 开始工作")
            time.sleep(0.02)
            logger.info(f"线程 {name} 完成工作")
        finally:
            logger.request_id_var.reset(token)

    # 也可以通过 contextvars.copy_context() 继承父线程的 request_id
    parent_token = logger.request_id_var.set("req-parent")
    ctx = contextvars.copy_context()

    def inherited_worker():
        ctx.run(logger.info, "子线程继承了父线程的 request_id")

    threads = [
        threading.Thread(target=worker, args=("T1", "req-t1")),
        threading.Thread(target=worker, args=("T2", "req-t2")),
        threading.Thread(target=inherited_worker),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.request_id_var.reset(parent_token)
    logger.cleanup()


# ---------------------------------------------------------------------------
# 14. 统计信息重置
# ---------------------------------------------------------------------------
def run_stats_reset() -> None:
    """演示统计信息的重置"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_reset", log_dir=log_dir, language="zh", enable_stats=True)

    logger.info("写入一些日志")
    logger.error("写入错误日志")
    print("重置前:", logger.get_stats())

    logger.reset_stats()
    print("重置后:", logger.get_stats())

    logger.info("重置后的新日志")
    print("新统计:", logger.get_stats())
    logger.cleanup()


# ---------------------------------------------------------------------------
# 15. 英文语言模式
# ---------------------------------------------------------------------------
def run_english_mode() -> None:
    """演示英文语言模式"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_en", log_dir=log_dir, language="en", enable_stats=True)

    @logger.log_decorator()
    def greet(name: str) -> str:
        return f"Hello, {name}!"

    token = logger.request_id_var.set("req-en-001")
    try:
        result = greet("World")
        logger.info(f"greeting result: {result}")
        logger.info("startup complete")
        logger.bind(category="SYSTEM").warning("disk usage high")
    finally:
        logger.request_id_var.reset(token)

    print("english stats:", logger.get_stats())
    logger.cleanup()


# ---------------------------------------------------------------------------
# 16. @time_it 装饰器 —— 函数级耗时统计与行级耗时分析
# ---------------------------------------------------------------------------
def run_time_it() -> None:
    """演示 time_it 装饰器的使用"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_timer", log_dir=log_dir, language="zh")

    # 0. 最简使用：只查看耗时，不带括号直接装饰
    @logger.time_it
    def no_paren_task():
        time.sleep(0.03)

    # 1. 基础版耗时统计 (只记录总耗时，带括号)
    @logger.time_it(line_by_line=False)
    def my_heavy_task(n: int):
        total = 0
        for i in range(n):
            total += i
        time.sleep(0.05)
        return total

    # 2. 进阶版行级耗时分析 (扒开内部看哪行代码慢)
    @logger.time_it(line_by_line=True)
    def analyze_this():
        x = 100
        time.sleep(0.02)  # 这一行稍慢
        y = x * 200
        time.sleep(0.08)  # 这一行最慢，应当被高亮标出瓶颈
        return y

    # 3. 异步函数耗时与行级分析
    @logger.time_it(line_by_line=True)
    async def async_heavy_task():
        a = 1
        await asyncio.sleep(0.03)  # 协程挂起
        b = a + 2
        await asyncio.sleep(0.07)  # 协程挂起
        return b

    # --- 开始执行演示 ---
    print("执行 no_paren_task (不带括号直接装饰)...")
    no_paren_task()

    print("\n执行 my_heavy_task (基础耗时统计，带括号)...")
    my_heavy_task(100000)

    print("\n执行 analyze_this (行级耗时分析，预期显示表格报告)...")
    analyze_this()

    print("\n执行 async_heavy_task (异步行分析)...")
    asyncio.run(async_heavy_task())

    logger.cleanup()


# ---------------------------------------------------------------------------
# 17. JSON 结构化日志 (serialize) + bind/contextualize 上下文
# ---------------------------------------------------------------------------
def run_json_serialize() -> None:
    """演示 JSON 结构化输出，便于 ELK/Loki/Datadog 采集"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic_json",
        log_dir=log_dir,
        language="zh",
        serialize=True,            # 文件写 JSON
        console_serialize=False,   # 控制台仍保留可读格式
    )

    # 绑定结构化字段（会自动出现在 JSON 的 extra 中，可与 trace 系统对齐）
    logger.bind(service="order", trace_id="trace-abc-123").info("处理下单请求")
    with logger.contextualize(trace_id="trace-abc-123", span_id="01"):
        logger.info("查询库存")
        logger.warning("库存偏低")

    logger.cleanup()

    # 读取文件，展示第一行 JSON
    json_file = os.path.join(log_dir, "basic_json.log")
    if os.path.exists(json_file):
        with open(json_file, "r", encoding="utf-8") as f:
            first_line = f.readline().strip()
        record = json.loads(first_line)
        print("JSON 字段示例:", {
            "level": record["record"]["level"]["name"],
            "message": record["record"]["message"],
            "extra": record["record"]["extra"],
        })


# ---------------------------------------------------------------------------
# 18. 生产环境配置 (env=prod) + 按 sink 独立级别
# ---------------------------------------------------------------------------
def run_env_prod() -> None:
    """演示生产环境推荐配置：env=prod + 按 sink 独立级别"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic_prod",
        log_dir=log_dir,
        language="zh",
        env="prod",              # 生产模式：关闭 diagnose/backtrace，使用异步文件写入
        console_level="WARNING", # 控制台只看 WARNING 及以上
        file_level="INFO",       # 文件保留 INFO 全量
    )
    print(f"env={logger.env}, enqueue={logger.enqueue}, "
          f"diagnose={logger.diagnose}, backtrace={logger.backtrace}")

    logger.info("这条 INFO 只进文件，不在控制台显示")
    logger.warning("这条 WARNING 控制台和文件都会显示")
    logger.cleanup()


# ---------------------------------------------------------------------------
# 19. 接管标准库 logging
# ---------------------------------------------------------------------------
def run_capture_std_logging() -> None:
    """演示把三方库(标准库 logging)的日志统一汇入本管道"""
    import logging

    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic_std",
        log_dir=log_dir,
        language="zh",
    )
    logger.capture_std_logging()  # 显式接管 root logger

    # 模拟三方库通过标准库 logging 输出
    third_party = logging.getLogger("third_party.lib")
    third_party.warning("来自标准库 logging 的警告，已被统一汇入 YydsLogger")
    third_party.error("来自标准库 logging 的错误")

    logger.info("本机日志与三方库日志现在格式一致")
    logger.cleanup()  # cleanup 会自动恢复标准库 logging 状态
    print("capture_std_logging done")


# ===========================================================================
# main
# ===========================================================================
def main() -> None:
    print("=" * 60)
    print(">>> 1. 基础日志")
    print("=" * 60)
    run_basic()

    print("\n" + "=" * 60)
    print(">>> 2. 批量日志")
    print("=" * 60)

    print("\n" + "=" * 60)
    print(">>> 3. 同步装饰器")
    print("=" * 60)
    run_sync_decorator()

    print("\n" + "=" * 60)
    print(">>> 4. 异常捕获")
    print("=" * 60)
    run_exception_handling()

    print("\n" + "=" * 60)
    print(">>> 5. Context Manager")
    print("=" * 60)
    run_context_manager()

    print("\n" + "=" * 60)
    print("\n" + "=" * 60)
    print(">>> 9. 异步装饰器 + 并发请求")
    print("=" * 60)
    asyncio.run(run_async_decorator())

    print("\n" + "=" * 60)
    print(">>> 10. 多线程")
    print("=" * 60)
    run_multithreading()

    print("\n" + "=" * 60)
    print(">>> 14. 统计信息重置")
    print("=" * 60)
    run_stats_reset()

    print("\n" + "=" * 60)
    print(">>> 15. 英文语言模式")
    print("=" * 60)
    run_english_mode()

    print("\n" + "=" * 60)
    print(">>> 16. @time_it 装饰器 (耗时统计与行分析)")
    print("=" * 60)
    run_time_it()

    print("\n" + "=" * 60)
    print(">>> 17. JSON 结构化日志 (serialize) + bind/contextualize")
    print("=" * 60)
    run_json_serialize()

    print("\n" + "=" * 60)
    print(">>> 18. 生产环境配置 (env=prod) + 按 sink 独立级别")
    print("=" * 60)
    run_env_prod()

    print("\n" + "=" * 60)
    print(">>> 19. 接管标准库 logging")
    print("=" * 60)
    run_capture_std_logging()

    print("\n" + "=" * 60)
    print(f"basic example finished at {datetime.now().isoformat(timespec='seconds')}")
    print("=" * 60)


if __name__ == "__main__":
    main()
