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
# 1. 基础日志 —— 各级别 + tag / category / location / context / timing
# ---------------------------------------------------------------------------
def run_basic() -> None:
    """演示最常用的日志方法"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic",
        log_dir=log_dir,
        language="zh",
        enable_stats=True,
        rotation_time="1 day",
        compression="zip",
    )

    token = logger.request_id_var.set("req-basic-001")
    try:
        # 各级别日志
        logger.debug("基础日志: debug")
        logger.info("基础日志: info")
        logger.warning("基础日志: warning")
        logger.error("基础日志: error")
        logger.critical("基础日志: critical")

        # 带 tag / category / location
        logger.log_with_tag("INFO", "服务启动完成", tag="BOOT")
        logger.log_with_tag("WARNING", "配置项缺失，使用默认值", tag="CONFIG")
        logger.log_with_category("INFO", "用户登录成功", category="AUTH")
        logger.log_with_category("ERROR", "数据库连接超时", category="DB")
        logger.log_with_location("INFO", "带位置信息的日志")

        # 带上下文 / 计时
        logger.log_with_context("INFO", "API 请求处理", {
            "user_id": 123,
            "method": "POST",
            "path": "/api/orders",
            "ip": "192.168.1.100",
        })
        logger.log_with_timing("INFO", "请求耗时分解", {
            "db_query": 0.023,
            "cache_lookup": 0.002,
            "serialization": 0.005,
            "total": 0.081,
        })
    finally:
        logger.request_id_var.reset(token)

    # 统计信息
    print("stats:", logger.get_stats())
    print("summary:", logger.get_stats_summary())
    print("performance_stats:", logger.get_performance_stats())
    print("category_distribution:", logger.get_category_distribution())
    print("error_trend:", logger.get_error_trend())
    logger.cleanup()


# ---------------------------------------------------------------------------
# 2. 批量日志
# ---------------------------------------------------------------------------
def run_batch_log() -> None:
    """演示 batch_log 一次写入多条日志"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_batch", log_dir=log_dir, language="zh", enable_stats=True)

    logger.batch_log([
        {"level": "INFO",    "message": "订单创建成功", "tag": "ORDER"},
        {"level": "INFO",    "message": "发送确认邮件", "category": "NOTIFY"},
        {"level": "WARNING", "message": "库存不足，即将触发补货", "category": "INVENTORY"},
        {"level": "ERROR",   "message": "支付回调验签失败"},
        {"level": "INFO",    "message": "用户会话已续期", "tag": "SESSION"},
    ])

    print("batch stats:", logger.get_stats_summary())
    logger.cleanup()


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
            logger.log_with_context("INFO", "处理中", {"step": 1, "status": "running"})
            logger.info("即将离开 with 作用域，资源将自动清理")
        finally:
            logger.request_id_var.reset(token)
    print("context manager: logger 已自动 cleanup")


# ---------------------------------------------------------------------------
# 6. 自定义日志级别
# ---------------------------------------------------------------------------
def run_custom_level() -> None:
    """演示添加自定义日志级别"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_custom_lvl", log_dir=log_dir, language="zh")

    # 添加 AUDIT 级别 (介于 INFO 和 WARNING 之间)
    logger.add_custom_level("AUDIT", no=25, color="<yellow>", icon="🔍")
    logger.log("AUDIT", "用户 alice 修改了订单 #12345 的收货地址")
    logger.log("AUDIT", "管理员 bob 导出了 2024-Q1 销售报表")
    logger.cleanup()


# ---------------------------------------------------------------------------
# 7. 性能模式切换
# ---------------------------------------------------------------------------
def run_performance_mode() -> None:
    """演示性能模式的开关"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic_perf",
        log_dir=log_dir,
        language="zh",
        enable_stats=True,
    )

    logger.info("正常模式下的 info 日志")
    logger.debug("正常模式下的 debug 日志")

    # 开启性能模式 —— 自动提级到 WARNING，减少 IO
    logger.enable_performance_mode()
    logger.info("性能模式下的 info（不会输出）")
    logger.warning("性能模式下的 warning（会输出）")

    # 关闭性能模式 —— 恢复正常
    logger.disable_performance_mode()
    logger.info("恢复正常模式后的 info 日志")

    print("performance toggle done")
    logger.cleanup()


# ---------------------------------------------------------------------------
# 8. 日志分析 & 报告 & JSON 导出
# ---------------------------------------------------------------------------
def run_analysis_and_export() -> None:
    """演示日志分析、报告生成和 JSON 导出"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic", log_dir=log_dir, language="zh", enable_stats=True)

    # 写几条日志以便分析
    token = logger.request_id_var.set("req-analysis")
    try:
        for i in range(5):
            logger.info(f"分析测试日志 #{i}")
        logger.error("模拟 ConnectionError 异常")
        logger.warning("模拟 DeprecationWarning 警告")
    finally:
        logger.request_id_var.reset(token)

    # 分析最近 24 小时的日志
    analysis = logger.analyze_logs(hours=24)
    print("log_analysis:", {k: v for k, v in analysis.items() if k not in ("hourly_distribution", "file_distribution", "function_distribution")})

    # 生成文字报告
    report = logger.generate_log_report(hours=24)
    print("log_report:", report.strip())

    # 导出为 JSON
    export_path = os.path.join(log_dir, "export.json")
    logger.export_logs_to_json(export_path, hours=24)
    if os.path.exists(export_path):
        with open(export_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        print(f"exported {len(data)} log entries to {export_path}")

    logger.cleanup()


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
    print("async stats:", logger.get_stats_summary())
    logger.cleanup()


# ---------------------------------------------------------------------------
# 10. 异步批量日志
# ---------------------------------------------------------------------------
async def run_async_batch_log() -> None:
    """演示 async_batch_log，可在事件循环中定时让出"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic_async_batch", log_dir=log_dir, language="zh", enable_stats=True)

    logs = [
        {"level": "INFO",    "message": f"异步批量日志 #{i}", "tag": "ASYNC_BATCH"}
        for i in range(10)
    ]
    await logger.async_batch_log(logs, yield_every=3, sleep_s=0.01)
    print("async_batch done, total:", logger.get_stats()["total"])
    logger.cleanup()


# ---------------------------------------------------------------------------
# 11. 多线程 —— 使用 contextvars 传递 request_id
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
# 12. 自适应日志级别
# ---------------------------------------------------------------------------
def run_adaptive_level() -> None:
    """演示根据错误率自动调整日志级别"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(
        file_name="basic_adaptive",
        log_dir=log_dir,
        language="zh",
        enable_stats=True,
        adaptive_level=True,
    )

    logger.info(f"初始日志级别: {logger.filter_level}")
    # 模拟高错误率
    for _ in range(8):
        logger.error("模拟频繁错误")
    for _ in range(2):
        logger.info("正常日志")
    # 手动触发自适应检查
    logger.set_adaptive_level(error_rate_threshold=0.5)
    logger.info(f"调整后日志级别: {logger.filter_level}")
    logger.cleanup()


# ---------------------------------------------------------------------------
# 13. 日志压缩 & 清理
# ---------------------------------------------------------------------------
def run_compress_and_cleanup() -> None:
    """演示日志压缩和旧日志清理"""
    log_dir = os.path.join(os.path.dirname(__file__), "logs_basic")
    logger = YydsLogger(file_name="basic", log_dir=log_dir, language="zh")

    # 压缩 0 天前的日志（即所有日志，仅做演示）
    logger.compress_logs(days_old=0)
    # 清理 30 天前的旧日志
    logger.cleanup_old_logs(max_days=30)

    print("compress & cleanup done")
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
    print("重置前:", logger.get_stats_summary())

    logger.reset_stats()
    print("重置后:", logger.get_stats_summary())

    logger.info("重置后的新日志")
    print("新统计:", logger.get_stats_summary())
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
        logger.log_with_tag("INFO", "startup complete", tag="BOOT")
        logger.log_with_category("WARNING", "disk usage high", category="SYSTEM")
    finally:
        logger.request_id_var.reset(token)

    print("english summary:", logger.get_stats_summary())
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
        env="prod",              # 生产模式：关闭 diagnose/backtrace，保持非阻塞
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
        capture_std_logging=True,  # 构造时直接接管 root logger
    )

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
    run_batch_log()

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
    print(">>> 6. 自定义日志级别")
    print("=" * 60)
    run_custom_level()

    print("\n" + "=" * 60)
    print(">>> 7. 性能模式切换")
    print("=" * 60)
    run_performance_mode()

    print("\n" + "=" * 60)
    print(">>> 8. 日志分析 & 报告 & JSON 导出")
    print("=" * 60)
    run_analysis_and_export()

    print("\n" + "=" * 60)
    print(">>> 9. 异步装饰器 + 并发请求")
    print("=" * 60)
    asyncio.run(run_async_decorator())

    print("\n" + "=" * 60)
    print(">>> 10. 异步批量日志")
    print("=" * 60)
    asyncio.run(run_async_batch_log())

    print("\n" + "=" * 60)
    print(">>> 11. 多线程")
    print("=" * 60)
    run_multithreading()

    print("\n" + "=" * 60)
    print(">>> 12. 自适应日志级别")
    print("=" * 60)
    run_adaptive_level()

    print("\n" + "=" * 60)
    print(">>> 13. 日志压缩 & 清理")
    print("=" * 60)
    run_compress_and_cleanup()

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
