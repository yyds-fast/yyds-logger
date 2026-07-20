# YydsLogger

基于 Loguru 的增强日志记录器，提供稳定的本地日志写入、多语言、异步队列、结构化输出和生命周期管理能力。

## 特性

- 多语言输出（zh/en）
- 所有运行时中英文提示集中维护在 `yyds_logger/i18n.py`
- 自定义格式、级别过滤、按大小或按时间轮转、保留策略、gzip 压缩
- **多实例物理隔离**：底层内聚了完全独立的 `yyds_loguru` 引擎，彻底打破全局单例限制。每个 `YydsLogger` 实例拥有独立的 Core，且 `contextualize()` 上下文绑定同样物理隔离，互不串扰。
- **JSON 结构化输出（serialize）**：直接对接 ELK / Loki / Datadog / CloudWatch
- **接管标准库 logging**：把 uvicorn / sqlalchemy 等三方库日志统一汇入本管道（通过 `sys._getframe` C 级原生帧跳过深度优化性能）
- **环境感知（env='prod'/'dev'）**：默认为 `'prod'` (生产环境优先)，生产自动关闭 diagnose/backtrace，保持非阻塞高吞吐写入
- **有界本地队列**：支持 `queue_size`、`block/drop` 溢出策略和 `queue_timeout`
- **按 sink 独立级别**：控制台、主文件、错误文件可各自设级别
- request_id 上下文注入（ContextVar）+ `bind/contextualize` 结构化字段与 trace 关联
- 装饰器记录函数调用与耗时（同步/异步），级别关闭时自动跳过昂贵格式化
- 轻量基础统计（总量、级别计数、错误率）
- 可选健康检查（磁盘、内存、日志文件数量和大小）
- **纯零核心依赖**：移除了对外部 `loguru` 包的硬性安装依赖，基础版零额外依赖，轻量可靠且彻底杜绝依赖版本冲突风险
- **优雅资源管理**：支持 `flush()`、`flush_async()`、`close()`、`cleanup()` 和可选 SIGTERM 处理

## 安装

```bash
pip install -U yyds-logger
```

核心安装为**纯零额外依赖**（移除了对外部 loguru 的强制依赖，无版本冲突风险）。按需安装可选能力：

```bash
pip install "yyds-logger[profile]"   # 行级耗时分析（line_profiler，更精确）
pip install "yyds-logger[monitoring]"  # 系统指标与健康检查（psutil）
pip install "yyds-logger[all]"       # 一次性安装全部可选依赖
```

> 未装 `line_profiler` 时，`time_it(line_by_line=True)` 自动回退到原生 `sys.settrace`。

## 快速开始

### 基本使用

```python
from yyds_logger import YydsLogger

# 创建日志记录器实例
logger = YydsLogger(
    file_name="app",
    log_dir="logs",
    language="zh"  # 使用中文输出
)

# 设置 request_id（每条日志会带 ReqID:xxx）
token = logger.request_id_var.set("req-001")
try:
    logger.info("这是一条信息日志")
    logger.warning("这是一条警告日志")
    logger.error("这是一条错误日志")
finally:
    logger.request_id_var.reset(token)

# 程序退出前清理（排空本地队列、恢复 hook、移除 handler）
logger.cleanup()
```

需要在程序继续运行时仅排空异步队列，可调用 `logger.flush()`；完全关闭并释放资源可调用
`logger.close()`，它是 `cleanup()` 的明确别名。

在已有 asyncio 事件循环的协程中使用：

```python
await logger.flush_async()
```

#### request_id 用法与并发说明

`request_id_var` 是一个 `ContextVar`，用来给“同一条业务链路”的日志自动带上 `ReqID:...`：

- `set(value)`：把当前上下文的 request_id 设为 `value`，并返回一个 token（表示 set 之前的旧值）
- `reset(token)`：把当前上下文恢复到 set 之前的值，避免 request_id 泄漏到下一次请求/任务

推荐写法用 `try/finally` 确保一定 reset（如上例）。

多并发下的行为：

- 多线程：不同线程之间的 `request_id` 互不影响；但新线程不会自动继承父线程的 request_id，需要显式传递。
- asyncio：不同 Task 之间的 `request_id` 互不影响；创建 Task 时会复制一份当前上下文，所以要在 `create_task()` 之前 set。

线程/线程池中传递 request_id 的示例：

```python
import contextvars
import threading

token = logger.request_id_var.set("req-001")
try:
    ctx = contextvars.copy_context()
    t = threading.Thread(target=lambda: ctx.run(logger.info, "子线程日志也带 ReqID"))
    t.start()
    t.join()
finally:
    logger.request_id_var.reset(token)
```

asyncio 并发示例（每个请求独立 request_id）：

```python
import asyncio

async def handle(req_id: str):
    token = logger.request_id_var.set(req_id)
    try:
        logger.info("开始处理")
        await asyncio.sleep(0.1)
        logger.info("处理完成")
    finally:
        logger.request_id_var.reset(token)

async def main():
    await asyncio.gather(handle("req-1"), handle("req-2"))

asyncio.run(main())
```

### 生产环境推荐配置（env）

`env` 优先于旧的 `work_type`。**默认值已调整为 `'prod'`**。生产模式会关闭 `diagnose/backtrace`（避免在日志里泄漏变量值并降低开销），
同时保持 `enqueue=True`（异步非阻塞写入）：

```python
logger = YydsLogger(
    file_name="app",
    log_dir="logs",
    # env="prod",          # 默认即为 "prod"，生产关闭诊断回溯，保持非阻塞
    serialize=True,        # 文件输出 JSON，便于日志平台采集
    filter_level="INFO",   # 生产通常 INFO 起步
)
```

### 多进程配置

多进程部署必须使用 `process_isolation=True`，让不同进程写入独立的 PID 日志文件，避免轮转和写入竞争；默认值为 `False` 以保持单进程文件名兼容。

本地文件 sink 的 enqueue 队列可通过 `queue_size`、`overflow_policy` 和 `queue_timeout` 配置；
队列满时可选择阻塞或丢弃，并通过 `get_queue_dropped()` 查看丢弃数量。

如需精细控制，可用 `enqueue` / `diagnose` / `backtrace` 三个可选参数显式覆盖：

```python
logger = YydsLogger("app", env="prod", diagnose=True)  # 临时排障：单独打开 diagnose
```

> 兼容性：不显式传 `env` 且不传 `work_type` 时，默认使用安全生产配置。如果传入了 `work_type`，行为完全与旧版本保持兼容（`False`=测试，`True`=旧生产）。

### JSON 结构化日志（serialize）

容器 / k8s / 日志平台（ELK、Loki、Datadog）更适合消费 JSON。`serialize` 控制文件输出，
`console_serialize` 控制控制台输出：

```python
logger = YydsLogger(
    file_name="app",
    serialize=True,          # 文件写 JSON（每行一条）
    console_serialize=False, # 控制台仍保留彩色可读格式
)
logger.info("用户登录")
# 文件中为一行 JSON，包含 time/level/message/extra（含 request_id 及 bind/contextualize 字段）
```

> 注意：序列化模式下日志为 JSON，自定义彩色 `format` 的颜色不再生效（这是 loguru 序列化语义）。

### 结构化上下文与 trace 关联（bind / contextualize）

配合 `serialize=True`，绑定的字段会自动出现在 JSON 中，方便与 OpenTelemetry / 链路追踪对齐：

```python
# 永久绑定，返回带上下文的 logger
logger.bind(service="order", trace_id="abc123").info("处理下单")

# with 作用域内临时注入（线程/协程安全）
with logger.contextualize(trace_id="abc123", span_id="01"):
    logger.info("查询库存")
    logger.info("扣减库存")

# request_id 便捷方法
token = logger.set_request_id("req-001")
logger.info(logger.get_request_id())
```

### 接管标准库 logging

把三方库（uvicorn、sqlalchemy、requests 等）通过标准库 `logging` 输出的日志统一汇入本管道：

```python
logger = YydsLogger("app", capture_std_logging=True)  # 构造时直接接管 root

# 或运行时按需接管指定 logger
logger.capture_std_logging(level="INFO", names=["uvicorn", "sqlalchemy.engine"])
```

默认不会删除目标 logger 已有的 handlers；如需完全接管，可显式传入
`clear_existing=True`。`cleanup()` 时会自动恢复被修改的标准库 logging 状态。

### 按 sink 设置独立级别

控制台、主文件、错误文件可分别设级别（例如控制台只看 WARNING，文件留全量 DEBUG）：

```python
logger = YydsLogger(
    file_name="app",
    filter_level="DEBUG",   # 默认级别
    console_level="WARNING",
    file_level="DEBUG",
    error_level="ERROR",
)
```

### 优雅退出（SIGTERM）

容器 / k8s 用 SIGTERM 停服时 `atexit` 不一定触发，开启后会在退出前排空 enqueue 队列，避免丢日志：

```python
logger = YydsLogger("app", install_signal_handlers=True)  # 拦截并链回原 SIGTERM/SIGINT 处理器
```

### 环境变量配置（read_env）

适合多环境部署，无需改代码即可覆盖配置：

```python
# 环境变量：YYDS_LOG_DIR / YYDS_LOG_LEVEL / YYDS_LOG_LANG / YYDS_LOG_SERIALIZE / YYDS_LOG_ENV
logger = YydsLogger("app", read_env=True)
```

### 异步函数支持

```python
import asyncio

@logger.log_decorator()
async def async_function():
    await asyncio.sleep(1)
    return "异步操作完成"

# 使用异步函数
async def main():
    result = await async_function()
    logger.info(f"异步函数结果: {result}")

asyncio.run(main())
```

### 增强错误信息

```python
# 错误日志现在会显示详细的位置信息
@logger.log_decorator("除零错误", level="ERROR")
def divide_numbers(a, b):
    return a / b

try:
    result = divide_numbers(1, 0)
except ZeroDivisionError:
    logger.exception("捕获到除零错误")
    # 输出示例：
    # 2025-01-03 10:30:15.123 | ERROR    | ReqID:REQ-123 | app.py:25:divide_numbers | 12345 | 除零错误 [ZeroDivisionError]: division by zero | 位置: app.py:25:divide_numbers | 代码: return a / b
    # 调用链: app.py:25:divide_numbers -> main.py:10:main
```

### 耗时统计与行分析 (@time_it)

提供 `@logger.time_it` 装饰器，不仅可以记录函数的整体执行耗时，还能开启行级性能分析，扒开函数内部看哪一行代码在拖后腿（支持同步和异步函数）。

```python
import time
import asyncio

# 1. 最简使用：只查看耗时，不带括号直接装饰（每次都打印耗时）
@logger.time_it
def my_task():
    time.sleep(0.05)

my_task()
# 输出示例：
# 2026-05-25 15:37:14.790 | INFO     | ReqID:no-request-id | yyds_logger.py:778 | [TIMER] 函数 `my_task` 执行完毕 | 总耗时: 52.15ms


# 2. 进阶版：开启行级性能分析（支持可选依赖 line_profiler，未安装时自动降级为原生 sys.settrace）
@logger.time_it(line_by_line=True)
def heavy_task():
    a = 1 + 2
    time.sleep(0.02)  # 耗时较短
    b = a * 3
    time.sleep(0.08)  # 耗时最长，被标记为瓶颈
    return b

heavy_task()
# 输出示例：
# ================================================================================
# [FN-TIMER] 行性能分析报告 -> 函数: `heavy_task`
# ================================================================================
# 行号     | 命中次数     | 总耗时 (ms)     | 每步耗时 (ms)    | 耗时占比     | 源代码
# --------------------------------------------------------------------------------
#    45 |        1 |        0.00 |        0.00 |     0.0% |         a = 1 + 2
#    46 |        1 |       25.05 |       25.05 |    22.8% |         time.sleep(0.02)  # 这行稍慢  <-- 🚨 性能瓶颈
#    47 |        1 |        0.00 |        0.00 |     0.0% |         b = a * 3
#    48 |        1 |       84.66 |       84.66 |    77.1% |         time.sleep(0.08)  # 这行最慢，应当被标记为瓶颈  <-- 🚨 性能瓶颈
#    49 |        1 |        0.00 |        0.00 |     0.0% |         return b
# --------------------------------------------------------------------------------
# 总耗时: 109.78 毫秒
# ================================================================================
```

### 健康检查（可选）

健康检查依赖 `psutil`，可通过可选依赖安装：

```bash
pip install yyds-logger[monitoring]
```

```python
from yyds_logger import LogHealthChecker

checker = LogHealthChecker()
health = checker.check_health("logs")
print(health["status"])
print(health["metrics"]["disk_usage_percent"])
```

### 日志统计功能

```python
# 启用统计功能
logger = YydsLogger(
    file_name="app",
    enable_stats=True
)

# 获取统计信息
stats = logger.get_stats()
print(stats)

```

## 高级配置

### 完整配置示例

```python
logger = YydsLogger(
    file_name="app",                    # 日志文件名
    log_dir="logs",                     # 日志目录
    max_size=14,                        # 单个日志文件最大大小（MB）
    retention="7 days",                 # 日志保留时间
    work_type=False,                    # 已废弃，建议用 env；False=测试，True=旧生产
    language="zh",                      # 日志语言（zh/en）
    rotation_time="1 day",              # 日志轮转时间
    custom_format=None,                 # 自定义日志格式
    filter_level="DEBUG",               # 默认日志过滤级别
    compression="gz",                   # 日志压缩格式，默认 gzip
    enable_stats=False,                 # 是否启用统计
    enable_exception_hook=False,        # 是否接管 sys.excepthook（同时兜底子线程异常）
    # —— 新增参数 ——
    env="prod",                        # "dev"/"prod"，优先于 work_type
    enqueue=None,                       # 显式覆盖 enqueue（异步非阻塞写入）
    diagnose=None,                      # 显式覆盖 diagnose
    backtrace=None,                     # 显式覆盖 backtrace
    serialize=False,                    # 文件输出 JSON 结构化日志
    console_serialize=False,            # 控制台输出 JSON 结构化日志
    console_level=None,                 # 控制台独立级别（默认随 filter_level）
    file_level=None,                    # 主文件独立级别（默认随 filter_level）
    error_level="ERROR",                # 错误文件级别
    capture_std_logging=False,          # 接管标准库 logging
    install_signal_handlers=False,      # 注册 SIGTERM/SIGINT 优雅退出
    read_env=False,                     # 从环境变量读取配置覆盖
    error_file=False,                   # 是否单独输出错误日志文件
    queue_size=10000,                   # 本地 enqueue 队列容量，None 表示无界
    overflow_policy="block",           # 队列满时 block 或 drop
    queue_timeout=None,                 # block 模式最大等待时间
    process_isolation=False,             # 多进程部署时必须改为 True
)
```

### 自定义日志格式

```python
custom_format = (
    "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
    "<level>{level: <8}</level> | "
    "ReqID:{extra[request_id]} | "
    "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
    "<level>{message}</level>"
)

logger = YydsLogger(
    file_name="app",
    custom_format=custom_format
)
```

## 主要功能

### 1. 日志记录
- 支持所有标准日志级别（DEBUG, INFO, WARNING, ERROR, CRITICAL）
- 支持 `log()` 通用级别入口
- 支持 request_id、bind/contextualize 结构化上下文

### 2. 日志管理
- 自动日志轮转
- gzip 日志压缩
- 日志保留策略
- 可选错误日志文件
- 本地 enqueue 队列与溢出策略

### 3. 统计功能
- 日志总数统计
- 错误率统计
- DEBUG/INFO/WARNING/ERROR 级别计数
- 统计开关关闭时不产生统计开销

### 4. 装饰器支持
- 函数执行时间记录
- 异常捕获和记录
- 支持同步和异步函数

### 5. 增强错误信息
- 显示错误发生的具体文件、行号和函数名
- 显示错误发生时的代码行内容
- 显示调用链信息（最后3层调用）
- 支持全局异常处理器

### 6. 性能优化
- 级别门控减少无效日志构造
- 可选 enqueue 异步文件写入
- 有界队列支持阻塞或丢弃策略
- 行级性能分析作为独立可选能力

### 7. 运维能力
- 可选接管标准库 `logging`
- 可选异常钩子和 SIGTERM/SIGINT 生命周期处理
- 可选健康检查：磁盘、内存和日志文件状态

## 错误处理

```python
try:
    logger = YydsLogger("app", log_dir="/path/to/logs")
except RuntimeError as e:
    print(f"日志配置失败: {e}")
```

## 注意事项

1. 确保日志目录具有写入权限
2. 生产环境建议使用 `env="prod"` + `serialize=True`，并按需 `install_signal_handlers=True`
3. 异步操作时注意正确处理异常
4. `serialize=True` 时日志为 JSON，自定义彩色 `format` 的颜色不再生效
5. 全局 logger 为 loguru 单例，建议单进程内仅实例化一个 YydsLogger，避免多实例 handler 互相叠加

## 贡献

欢迎提交 Issue 和 Pull Request！

## 许可证

MIT License
