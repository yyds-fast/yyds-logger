#!/usr/bin/env python
# -*- coding:utf-8 -*-

# Author: zhibo.wang
# E-mail: gm.zhibo.wang@gmail.com
# Date  : 2025-01-03
# Desc  : Enhanced Logger with Loguru (with async support) + Language Option

import os
import sys
import time
import atexit
import inspect
import logging
import asyncio

from typing import Optional, Dict, Any, List, Tuple

from functools import wraps
from time import perf_counter
from contextvars import ContextVar
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict, deque
from datetime import datetime, timedelta
import threading

from loguru import logger


class YydsLogger:
    """
    基于 Loguru 的增强日志记录器，具有以下功能：
    - 自定义日志格式
    - 日志轮转和保留策略
    - 上下文信息管理(如 request_id)
    - 远程日志收集(使用线程池防止阻塞)
    - 装饰器用于记录函数调用和执行时间，支持同步/异步函数
    - 自定义日志级别(避免与 Loguru 预定义的冲突)
    - 统一异常处理

    新增：
    - 可指定语言(中文/英文)，默认中文
    - 支持按时间轮转日志
    - 支持自定义日志格式
    - 支持日志级别过滤
    - 支持自定义压缩格式
    - 支持自定义文件命名模式
    """

    # 在 _LANG_MAP 中添加新的语言项
    _LANG_MAP = {
        'zh': {
            'LOG_STATS': "日志统计: 总计 {total} 条, 错误 {error} 条, 警告 {warning} 条, 信息 {info} 条",
            'LOG_TAGGED': "[{tag}] {message}",
            'LOG_CATEGORY': "分类: {category} - {message}",
            'UNHANDLED_EXCEPTION': "未处理的异常",
            'FAILED_REMOTE': "远程日志发送失败: {error}",
            'START_FUNCTION_CALL': "开始函数调用",
            'END_FUNCTION_CALL': "结束函数调用",
            'START_ASYNC_FUNCTION_CALL': "开始异步函数调用",
            'END_ASYNC_FUNCTION_CALL': "结束异步函数调用",
            'CALLING_FUNCTION': "调用函数: {func}，参数: {args}，关键字参数: {kwargs}",
            'CALLING_ASYNC_FUNCTION': "调用异步函数: {func}，参数: {args}，关键字参数: {kwargs}",
            'FUNCTION_RETURNED': "函数 {func} 返回结果: {result}，耗时: {duration}秒",
            'ASYNC_FUNCTION_RETURNED': "异步函数 {func} 返回结果: {result}，耗时: {duration}秒",
            'TIMER_SUMMARY': "[TIMER] 函数 `{func}` 执行完毕 | 总耗时: {duration}ms",
            'FINAL_ATTEMPT_FAILED': "最终尝试失败: {error}",
            'REMOTE_DISABLED_NO_LIBS': "未安装 aiohttp/requests，远程日志已禁用",
            'CANNOT_GET_SOURCE': "无法获取函数 `{func}` 的源代码进行行性能分析: {error}",
            'LINE_PROFILE_REPORT_HEADER': "[FN-TIMER] 行性能分析报告 -> 函数: `{func}`",
            'COL_LINE_NO': "行号",
            'COL_HITS': "命中次数",
            'COL_TOTAL_TIME': "总耗时 (ms)",
            'COL_AVG_TIME': "每步耗时 (ms)",
            'COL_PCT': "耗时占比",
            'COL_SOURCE': "源代码",
            'PERF_BOTTLENECK': "性能瓶颈",
            'TOTAL_DURATION_MS': "总耗时: {duration} 毫秒",
            'OCCURRENCE_EXCEPTION': "发生异常: {error}",
            'UNKNOWN_CODE_LINE': "未知代码行",
            'UNKNOWN_LOCATION': "未知位置",
            'LABEL_LOCATION': "位置",
            'LABEL_CODE': "代码",
            'FULL_EXCEPTION_STACK': "完整异常堆栈:",
            'CALL_CHAIN': "调用链: {chain}",
            'ERROR_RECORDING_EXCEPTION': "记录异常时发生错误: {error}",
            'ORIGINAL_EXCEPTION_STACK': "原始异常堆栈:",
            'UPDATE_LEVEL_FAILED': "更新日志级别失败: {error}",
            'LOG_COMPRESSED': "已压缩日志文件: {file}",
            'LOG_COMPRESS_FAILED': "压缩日志文件失败 {file}: {error}",
            'LOG_ARCHIVED': "已归档日志文件: {file}",
            'LOG_ARCHIVE_FAILED': "归档日志文件失败 {file}: {error}",
            'LOG_DELETED': "已删除旧日志文件: {file}",
            'LOG_DELETE_FAILED': "删除旧日志文件失败 {file}: {error}",
            'LOG_ANALYZE_FAILED': "分析日志文件失败 {file}: {error}",
            'LOG_EXPORT_FAILED': "导出日志文件失败 {file}: {error}",
            'LOG_EXPORTED': "日志已导出到: {file}",
            'JSON_EXPORT_FAILED': "导出JSON文件失败: {error}",
            'CLEANUP_COMPLETED': "YydsLogger 资源清理完成",
            'SWITCH_ADAPTIVE_FAILED': "切换自适应模式失败: {error}",
            'FORMAT_ERR_MISSING_PARAM': " (格式化错误: 缺少参数 {error})",
            'FORMAT_ERR_GENERIC': " (格式化错误: {error})",
            'THREAD_UNHANDLED_EXCEPTION': "线程未处理异常 [{thread}]: {error_type}",
            'REPORT_TEMPLATE': "\n=== 日志分析报告 ({hours}小时) ===\n总日志数: {total}\n错误数: {error}\n警告数: {warning}\n信息数: {info}\n调试数: {debug}\n错误率: {error_rate:.2%}",
            'REPORT_TOP_ERRORS': "\n最常见的错误类型:\n",
            'REPORT_TOP_WARNINGS': "\n最常见的警告类型:\n",
            'REPORT_COUNT_SUFFIX': "  {type}: {count}次\n",
            'ERR_MAX_SIZE': "max_size 必须是正整数",
            'ERR_RETENTION': "retention 必须是字符串",
            'ERR_REMOTE_URL': "remote_log_url 必须是有效的 HTTP(S) URL",
            'ERR_LANGUAGE': "language 必须是 'zh' 或 'en'",
            'ERR_COMPRESSION': "compression 必须是 'zip', 'gz' 或 'tar'",
            'ERR_INVALID_LEVEL': "{name} 不是有效的日志级别: {value}",
            'ERR_MAX_WORKERS': "max_workers 必须是正整数",
            'ERR_DIR_NOT_WRITABLE': "日志目录不可写: {dir}",
            'ERR_CANNOT_CREATE_DIR': "无法创建日志目录: {error}",
            'ERR_CONFIG_FAILED': "日志配置失败: {error}",
            'WARN_MULTIPLE_INSTANCES': "检测到多个活跃的 YydsLogger 实例；loguru 使用全局 logger，多实例的 handler 会互相叠加导致日志重复。建议单进程仅创建一个实例，并在不再使用时调用 cleanup()（或使用 with 语句）。",
        },
        'en': {
            'LOG_STATS': "Log statistics: Total {total}, Errors {error}, Warnings {warning}, Info {info}",
            'LOG_TAGGED': "[{tag}] {message}",
            'LOG_CATEGORY': "Category: {category} - {message}",
            'UNHANDLED_EXCEPTION': "Unhandled exception",
            'FAILED_REMOTE': "Remote logging failed: {error}",
            'START_FUNCTION_CALL': "Starting function call",
            'END_FUNCTION_CALL': "Ending function call",
            'START_ASYNC_FUNCTION_CALL': "Starting async function call",
            'END_ASYNC_FUNCTION_CALL': "Ending async function call",
            'CALLING_FUNCTION': "Calling function: {func}, args: {args}, kwargs: {kwargs}",
            'CALLING_ASYNC_FUNCTION': "Calling async function: {func}, args: {args}, kwargs: {kwargs}",
            'FUNCTION_RETURNED': "Function {func} returned: {result}, duration: {duration}s",
            'ASYNC_FUNCTION_RETURNED': "Async function {func} returned: {result}, duration: {duration}s",
            'TIMER_SUMMARY': "[TIMER] Function `{func}` execution completed | Total duration: {duration}ms",
            'FINAL_ATTEMPT_FAILED': "Final attempt failed: {error}",
            'REMOTE_DISABLED_NO_LIBS': "aiohttp/requests not installed, remote logging disabled",
            'CANNOT_GET_SOURCE': "Cannot get source code of function `{func}` for line profiling: {error}",
            'LINE_PROFILE_REPORT_HEADER': "[FN-TIMER] Line profiling report -> Function: `{func}`",
            'COL_LINE_NO': "Line",
            'COL_HITS': "Hits",
            'COL_TOTAL_TIME': "Total (ms)",
            'COL_AVG_TIME': "Per-step (ms)",
            'COL_PCT': "Percent",
            'COL_SOURCE': "Source Code",
            'PERF_BOTTLENECK': "Performance Bottleneck",
            'TOTAL_DURATION_MS': "Total duration: {duration} ms",
            'OCCURRENCE_EXCEPTION': "Exception occurred: {error}",
            'UNKNOWN_CODE_LINE': "Unknown code line",
            'UNKNOWN_LOCATION': "Unknown location",
            'LABEL_LOCATION': "Location",
            'LABEL_CODE': "Code",
            'FULL_EXCEPTION_STACK': "Full exception traceback:",
            'CALL_CHAIN': "Call chain: {chain}",
            'ERROR_RECORDING_EXCEPTION': "Error occurred while logging exception: {error}",
            'ORIGINAL_EXCEPTION_STACK': "Original exception traceback:",
            'UPDATE_LEVEL_FAILED': "Failed to update log level: {error}",
            'LOG_COMPRESSED': "Compressed log file: {file}",
            'LOG_COMPRESS_FAILED': "Failed to compress log file {file}: {error}",
            'LOG_ARCHIVED': "Archived log file: {file}",
            'LOG_ARCHIVE_FAILED': "Failed to archive log file {file}: {error}",
            'LOG_DELETED': "Deleted old log file: {file}",
            'LOG_DELETE_FAILED': "Failed to delete old log file {file}: {error}",
            'LOG_ANALYZE_FAILED': "Failed to analyze log file {file}: {error}",
            'LOG_EXPORT_FAILED': "Failed to export log file {file}: {error}",
            'LOG_EXPORTED': "Logs successfully exported to: {file}",
            'JSON_EXPORT_FAILED': "Failed to export JSON file: {error}",
            'CLEANUP_COMPLETED': "YydsLogger resources cleaned up successfully",
            'SWITCH_ADAPTIVE_FAILED': "Failed to switch adaptive mode: {error}",
            'FORMAT_ERR_MISSING_PARAM': " (Formatting error: missing parameter {error})",
            'FORMAT_ERR_GENERIC': " (Formatting error: {error})",
            'THREAD_UNHANDLED_EXCEPTION': "Thread unhandled exception [{thread}]: {error_type}",
            'REPORT_TEMPLATE': "\n=== Log Analysis Report ({hours} hours) ===\nTotal Logs: {total}\nErrors: {error}\nWarnings: {warning}\nInfo: {info}\nDebug: {debug}\nError Rate: {error_rate:.2%}",
            'REPORT_TOP_ERRORS': "\nMost Common Error Types:\n",
            'REPORT_TOP_WARNINGS': "\nMost Common Warning Types:\n",
            'REPORT_COUNT_SUFFIX': "  {type}: {count} times\n",
            'ERR_MAX_SIZE': "max_size must be a positive integer",
            'ERR_RETENTION': "retention must be a string",
            'ERR_REMOTE_URL': "remote_log_url must be a valid HTTP(S) URL",
            'ERR_LANGUAGE': "language must be 'zh' or 'en'",
            'ERR_COMPRESSION': "compression must be 'zip', 'gz' or 'tar'",
            'ERR_INVALID_LEVEL': "{name} is not a valid log level: {value}",
            'ERR_MAX_WORKERS': "max_workers must be a positive integer",
            'ERR_DIR_NOT_WRITABLE': "Log directory is not writable: {dir}",
            'ERR_CANNOT_CREATE_DIR': "Cannot create log directory: {error}",
            'ERR_CONFIG_FAILED': "Failed to configure logger: {error}",
            'WARN_MULTIPLE_INSTANCES': "Multiple active YydsLogger instances detected. loguru uses a global logger, so handlers from multiple instances will stack up and cause duplicate log messages. It is recommended to create only one instance per process and call cleanup() (or use a 'with' statement) when it's no longer needed.",
        }
    }

    # 多实例防御：loguru 为全局单例，统计同进程内活跃实例数
    _instances_lock = threading.Lock()
    _active_instances = 0

    def __init__(
        self,
        file_name: str,                         # 日志文件基准名（用于区分同目录下不同日志器的日志文件以实现安全隔离操作）
        log_dir: str = 'logs',                 # 日志保存目录
        max_size: int = 14,                    # 单个文件最大大小（单位：MB）
        retention: str = '7 days',             # 日志保留策略
        remote_log_url: Optional[str] = None,  # 远程日志收集服务的 URL（可选）
        max_workers: int = 3,                  # 异步发送远程日志的线程池最大线程数
        work_type: bool = False,               # 兼容旧逻辑的运行模式，True 代表生产环境
        language: str = 'zh',                  # 语言选项，默认为中文
        rotation_time: Optional[str] = None,   # 新增：按时间轮转，如 "1 day", "1 week"
        custom_format: Optional[str] = None,   # 新增：自定义日志格式
        filter_level: str = "DEBUG",           # 新增：日志过滤级别
        compression: str = "zip",              # 新增：压缩格式，支持 zip, gz, tar
        enable_stats: bool = False,            # 新增：是否启用日志统计
        categories: Optional[list] = None,     # 新增：日志分类列表
        cache_size: int = 128,                 # 新增：缓存大小配置
        adaptive_level: bool = False,          # 新增：自适应日志级别
        performance_mode: bool = False,        # 新增：性能模式
        enable_exception_hook: bool = False,
        env: Optional[str] = None,             # 新增：环境，'dev'/'prod'（优先于 work_type）
        enqueue: Optional[bool] = None,        # 新增：显式覆盖 enqueue
        diagnose: Optional[bool] = None,       # 新增：显式覆盖 diagnose
        backtrace: Optional[bool] = None,      # 新增：显式覆盖 backtrace
        serialize: bool = False,               # 新增：文件输出 JSON 结构化日志
        console_serialize: bool = False,       # 新增：控制台输出 JSON 结构化日志
        console_level: Optional[str] = None,   # 新增：控制台独立级别
        file_level: Optional[str] = None,      # 新增：主文件独立级别
        error_level: str = "ERROR",            # 新增：错误文件级别
        capture_std_logging: bool = False,     # 新增：接管标准库 logging
        install_signal_handlers: bool = False, # 新增：注册 SIGTERM/SIGINT 优雅退出
        remote_queue_size: int = 10000,        # 新增：远程日志队列上限（背压）
        read_env: bool = False,                # 新增：从环境变量读取配置覆盖
        error_file: bool = True,               # 新增：是否单独输出错误日志文件
    ) -> None:
        """
        初始化日志记录器。

        Args:
            file_name (str): 日志文件名称(主日志文件前缀)。
            log_dir (str): 日志文件目录。
            max_size (int): 日志文件大小(MB)超过时进行轮转。
            retention (str): 日志保留策略。
            remote_log_url (str, optional): 远程日志收集的URL。如果提供，将启用远程日志收集。
            max_workers (int): 线程池的最大工作线程数。
            work_type (bool): 已废弃，建议使用 env。False=测试环境(开启诊断/回溯)，True=生产环境。
            language (str): 'zh' 或 'en'，表示日志输出语言，默认为中文。
            env (str, optional): 'dev'/'prod'，优先于 work_type。生产环境默认关闭 diagnose/backtrace
                以避免泄漏变量值并降低开销，同时保持 enqueue=True 实现非阻塞写入。
            enqueue/diagnose/backtrace (bool, optional): 显式覆盖对应行为。
            serialize (bool): 文件输出 JSON 结构化日志（便于 ELK/Loki/Datadog 采集）。
            console_serialize (bool): 控制台输出 JSON 结构化日志。
            console_level/file_level/error_level (str): 各 sink 的独立级别。
            capture_std_logging (bool): 接管标准库 logging，把三方库日志统一汇入本管道。
            install_signal_handlers (bool): 注册 SIGTERM/SIGINT，退出前排空队列避免丢日志。
            remote_queue_size (int): 远程日志队列上限，满时丢弃并计数（背压保护）。
            read_env (bool): 从环境变量读取配置覆盖（YYDS_LOG_DIR/YYDS_LOG_LEVEL/YYDS_LOG_LANG/
                YYDS_LOG_SERIALIZE/YYDS_LOG_ENV）。
        """
        # 环境变量覆盖（在所有解析之前执行）
        if read_env:
            log_dir = os.getenv("YYDS_LOG_DIR", log_dir)
            filter_level = os.getenv("YYDS_LOG_LEVEL", filter_level)
            language = os.getenv("YYDS_LOG_LANG", language)
            serialize = self._env_bool("YYDS_LOG_SERIALIZE", serialize)
            env = os.getenv("YYDS_LOG_ENV", env)

        self.file_name = file_name
        self.log_dir = log_dir
        self.max_size = max_size
        self.retention = retention
        self.remote_log_url = remote_log_url
        
        # 保存新增的参数为实例属性
        self.rotation_time = rotation_time
        self.custom_format = custom_format
        self.filter_level = filter_level
        self.serialize = bool(serialize)
        self.console_serialize = bool(console_serialize)
        self.console_level = console_level
        self.file_level = file_level
        self.error_level = error_level or "ERROR"
        self._error_file = bool(error_file)
        self.compression = compression
        # 级别号缓存与统计相关阈值
        self._level_no_cache: Dict[str, int] = {}
        self._error_level_no = 10 ** 9   # 由 configure_logger 精确计算
        self._by_hour_max = 168          # by_hour 最多保留的小时桶数（7 天），防无界增长
        self.enable_stats = enable_stats
        self.categories = categories or []
        self._cache_size = cache_size
        self._message_cache: Dict[Any, str] = {}
        self._stats_cache: Dict[str, Any] = {}
        self._stats_cache_time = 0.0
        self._stats_cache_ttl = 5
        # 直接设置后备字段，避免初始化阶段触发 property 的重建逻辑
        self._adaptive_level = bool(adaptive_level)
        self.performance_mode = performance_mode
        self._handler_ids: List[int] = []
        self._removed_default_handler = False
        self._exception_hook_enabled = bool(enable_exception_hook)
        self._exception_hook = None
        self._prev_excepthook = None
        self._remote_loop = None
        self._remote_thread = None
        self._remote_queue = None
        self._remote_ready = threading.Event()
        self._remote_queue_size = max(1, int(remote_queue_size))
        self._remote_dropped = 0
        self._capture_std_logging = bool(capture_std_logging)
        self._install_signal_handlers = bool(install_signal_handlers)
        self._std_logging_state = None
        self._prev_signal_handlers = {}
        self._prev_threading_excepthook = None

        # 语言选项
        self.language = language if language in ('zh', 'en') else 'zh'

        # 定义上下文变量，用于存储 request_id
        self.request_id_var = ContextVar("request_id", default="no-request-id")

        # 使用 patch 确保每条日志记录都包含 'request_id'
        self.logger = logger.patch(
            lambda record: record["extra"].update(
                request_id=self.request_id_var.get() or "no-request-id"
            )
        )
        # 缓存常用的 opt(depth=1)，减少热路径对象创建开销
        self._logger_d1 = self.logger.opt(depth=1)

        # 解析运行环境：env 优先于 work_type；显式 enqueue/diagnose/backtrace 再覆盖。
        if env is not None:
            self.env = str(env).strip().lower()
            is_prod = self.env in ('prod', 'production', 'release')
            # env 模式下采用更合理的默认：生产关闭诊断/回溯，但保持 enqueue=True（非阻塞）
            default_enqueue, default_diagnose, default_backtrace = True, (not is_prod), (not is_prod)
        else:
            # 完全兼容旧的 work_type 语义
            self.env = 'prod' if work_type else 'dev'
            if work_type:
                default_enqueue, default_diagnose, default_backtrace = False, False, False
            else:
                default_enqueue, default_diagnose, default_backtrace = True, True, True

        self.enqueue = default_enqueue if enqueue is None else bool(enqueue)
        self.diagnose = default_diagnose if diagnose is None else bool(diagnose)
        self.backtrace = default_backtrace if backtrace is None else bool(backtrace)

        # 级别号缓存：用于热路径门控与“无重建”改级别（核心优化）。
        self._info_level_no = self._safe_level_no("INFO")
        self._min_level_no = self._safe_level_no(self.filter_level)
        # 仅在需要运行期动态改级别（自适应）时使用 filter，避免破坏 loguru 的级别快路径。
        self._use_level_filter = bool(adaptive_level)

        # 用于远程日志发送的线程池
        self._max_workers = max_workers
        self._executor = None
        if self.remote_log_url:
            self._executor = ThreadPoolExecutor(max_workers=max_workers)
            self._start_remote_sender()

        # 初始化 Logger 配置
        self.configure_logger()

        self._stats_lock = threading.Lock()
        self._error_history_size = 200
        self._stats = {
            'total': 0,
            'error': 0,
            'warning': 0,
            'info': 0,
            'debug': 0,
            'by_category': defaultdict(int),
            'by_hour': defaultdict(int),
            'errors': deque(maxlen=self._error_history_size),
            'last_error_time': None,
            'error_rate': 0.0
        }
        self._stats_start_time = datetime.now()
        self._cleaned_up = False

        # 小时桶字符串缓存：避免每条日志都执行 strftime（热路径优化）
        self._cached_hour_index = -1
        self._cached_hour_str = ""

        # 注册 atexit 钩子，确保程序退出时自动清理 enqueue 队列和信号灯
        atexit.register(self.cleanup)

        # 可选：接管标准库 logging，把三方库日志统一汇入本管道
        if self._capture_std_logging:
            try:
                self.capture_std_logging()
            except Exception:
                pass

        # 可选：注册信号处理，容器/k8s SIGTERM 退出前排空队列避免丢日志
        if self._install_signal_handlers:
            try:
                self._setup_signal_handlers()
            except Exception:
                pass

        # 多实例防御告警：loguru 全局单例，多个活跃实例的 handler 会互相叠加导致日志重复
        self._instance_counted = False
        try:
            with YydsLogger._instances_lock:
                YydsLogger._active_instances += 1
                self._instance_counted = True
                active = YydsLogger._active_instances
            if active > 1:
                import warnings
                warnings.warn(
                    self._msg('WARN_MULTIPLE_INSTANCES'),
                    RuntimeWarning,
                    stacklevel=2,
                )
        except Exception:
            pass

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        """从环境变量解析布尔值"""
        raw = os.getenv(name)
        if raw is None:
            return bool(default)
        return raw.strip().lower() in ('1', 'true', 'yes', 'on', 'y')

    def _safe_level_no(self, name: Any) -> int:
        """安全获取日志级别编号，失败返回 0（最低，等于不过滤）"""
        try:
            if isinstance(name, int):
                return name
            return self.logger.level(str(name)).no
        except Exception:
            return 0

    def _level_no(self, name: Any) -> int:
        """带缓存的级别号查询，避免每条日志都走 loguru 的级别表查找（热路径优化）。"""
        if isinstance(name, int):
            return name
        cache = self._level_no_cache
        v = cache.get(name)
        if v is not None:
            return v
        v = self._safe_level_no(name)
        if v:  # 只缓存有效(非0)结果，避免缓存"级别尚未定义"的瞬态 0
            cache[name] = v
        return v

    def _emits(self, level_upper: str) -> bool:
        """该级别当前是否会被任一 sink 输出（主/控制台按 _min_level_no，错误文件按 _error_level_no）。"""
        no = self._level_no(level_upper)
        return no >= self._min_level_no or no >= self._error_level_no

    def _safe_level_name(self, name: Any) -> str:
        """规范化日志级别名称"""
        try:
            return self.logger.level(str(name)).name
        except Exception:
            return "INFO"

    def _level_filter(self, record) -> bool:
        """动态级别过滤器：仅在自适应模式下使用，改级别只需更新 _min_level_no，无需重建 handler"""
        try:
            return record["level"].no >= self._min_level_no
        except Exception:
            return True

    def is_level_enabled(self, level: str) -> bool:
        """判断指定级别当前是否会被输出（用于业务侧的昂贵日志构造前置判断）"""
        return self._safe_level_no(level) >= self._min_level_no

    @property
    def adaptive_level(self) -> bool:
        """是否启用自适应日志级别。"""
        return self._adaptive_level

    @adaptive_level.setter
    def adaptive_level(self, value) -> None:
        """运行期切换自适应模式：同步 _use_level_filter 并重建 handler。

        切换 filter 模式必须在 add() 时挂载/移除动态 filter，因此运行期翻转需重建 handler
        （一次性，非热路径）。初始化阶段（handler 尚未建立）不重建，由 configure_logger 统一处理。
        """
        value = bool(value)
        changed = getattr(self, '_adaptive_level', None) != value
        self._adaptive_level = value
        self._use_level_filter = value
        if changed and getattr(self, '_handler_ids', None):
            try:
                self.configure_logger()
            except Exception as e:
                try:
                    self.logger.warning(self._msg('SWITCH_ADAPTIVE_FAILED', error=str(e)))
                except Exception:
                    pass

    def _msg(self, key: str, **kwargs) -> str:
        """消息格式化处理，优化性能

        对无参消息缓存模板文本；对有参消息直接格式化（参数通常每次不同，
        缓存命中率极低，省去昂贵的 key 序列化开销）。
        """
        try:
            # 无参消息：直接缓存模板
            if not kwargs:
                cache_key = (self.language, key)
                cached = self._message_cache.get(cache_key)
                if cached is not None:
                    return cached
                text = self._LANG_MAP.get(self.language, {}).get(key, key)
                self._message_cache[cache_key] = text
                return text

            # 有参消息：直接格式化，不做缓存
            text = self._LANG_MAP.get(self.language, {}).get(key, key)
            str_kwargs = {}
            for k, v in kwargs.items():
                try:
                    str_kwargs[k] = str(v)
                except Exception:
                    str_kwargs[k] = f"<{type(v).__name__}>"
            return text.format(**str_kwargs)

        except KeyError as e:
            text = self._LANG_MAP.get(self.language, {}).get(key, key)
            err_tpl = self._LANG_MAP.get(self.language, {}).get('FORMAT_ERR_MISSING_PARAM', " (格式化错误: 缺少参数 {error})")
            return f"{text}{err_tpl.format(error=str(e))}"
        except Exception as e:
            text = self._LANG_MAP.get(self.language, {}).get(key, key)
            err_tpl = self._LANG_MAP.get(self.language, {}).get('FORMAT_ERR_GENERIC', " (格式化错误: {error})")
            return f"{text}{err_tpl.format(error=str(e))}"

    def _remove_handlers(self, wait: bool = False) -> None:
        handler_ids = list(self._handler_ids)
        self._handler_ids.clear()

        # 仅当本实例存在 enqueue 文件 handler 时才需要排空与 gc：
        # 控制台始终非 enqueue；enqueue=False 时没有 multiprocessing 队列/信号灯，
        # 无需 complete()/gc，也避免无意义地等待全局所有 handler。
        need_drain = bool(wait and handler_ids and getattr(self, 'enqueue', True))

        if need_drain:
            # 在 remove 之前调用 complete()，等待所有 enqueue 队列排空
            # （remove 之后 handler 已不存在，complete 无法等待）
            # logger.complete() 返回协程，需要用 asyncio 驱动
            try:
                asyncio.run(logger.complete())
            except RuntimeError:
                # 已有事件循环运行中，回退等待
                time.sleep(0.1)
            except Exception:
                time.sleep(0.1)

        for handler_id in handler_ids:
            try:
                self.logger.remove(handler_id)
            except Exception:
                continue

        if need_drain:
            # 强制垃圾回收，确保 multiprocessing.SimpleQueue 及其底层信号灯被释放
            import gc
            gc.collect()

    def configure_logger(self) -> None:
        """配置日志记录器，添加错误处理和安全性检查"""
        # 先做只读校验与目录检查：非法配置必须在改动任何全局 logger 状态之前快速失败，
        # 否则失败实例会残留 fallback handler，污染后续所有 logger（曾导致重复输出）。
        self._validate_config()
        self._ensure_log_directory()

        try:
            # 如果已有 handler（重新配置场景），等待旧队列排空再移除
            self._remove_handlers(wait=bool(self._handler_ids))
            if not self._removed_default_handler:
                try:
                    self.logger.remove(0)
                except Exception:
                    pass
                self._removed_default_handler = True

            # 重新计算最小级别号（filter_level 可能在性能模式/自适应中被改过）
            self._min_level_no = self._safe_level_no(self.filter_level)
            # 错误文件级别号：用于统计门控判断"是否会被错误文件捕获"；关闭错误文件时置为极大值
            self._error_level_no = self._level_no(self.error_level) if self._error_file else 10 ** 9
            
            # 配置日志格式
            log_format = self._get_log_format()
            
            # 添加控制台处理器
            self._add_console_handler(log_format)
            
            # 添加文件处理器
            self._add_file_handlers(log_format)
            
            # 配置远程日志(如果启用)
            if self.remote_log_url:
                self._configure_remote_logging()
            
            # 设置异常处理器
            if self._exception_hook_enabled:
                self.setup_exception_handler()

            # 重新缓存 opt(depth=1)，确保使用最新的 logger 配置
            self._logger_d1 = self.logger.opt(depth=1)
            
        except Exception as e:
            # 如果配置失败，使用基本配置
            self._fallback_configuration()
            raise RuntimeError(self._msg('ERR_CONFIG_FAILED', error=str(e)))
    
    def _validate_config(self) -> None:
        """验证配置参数"""
        if not isinstance(self.max_size, int) or self.max_size <= 0:
            raise ValueError(self._msg('ERR_MAX_SIZE'))
        
        if not isinstance(self.retention, str):
            raise ValueError(self._msg('ERR_RETENTION'))
        
        if self.remote_log_url and not self.remote_log_url.startswith(('http://', 'https://')):
            raise ValueError(self._msg('ERR_REMOTE_URL'))
        
        if self.language not in ('zh', 'en'):
            raise ValueError(self._msg('ERR_LANGUAGE'))
        
        if self.compression not in ('zip', 'gz', 'tar'):
            raise ValueError(self._msg('ERR_COMPRESSION'))

        # 校验级别名是否被 loguru 识别（自定义级别在此之前 add 的也会通过）
        for lvl_name, lvl_value in (
            ("filter_level", self.filter_level),
            ("console_level", self.console_level),
            ("file_level", self.file_level),
            ("error_level", self.error_level),
        ):
            if lvl_value is None:
                continue
            try:
                self.logger.level(str(lvl_value))
            except Exception:
                raise ValueError(self._msg('ERR_INVALID_LEVEL', name=lvl_name, value=repr(lvl_value)))

        if not isinstance(self._max_workers, int) or self._max_workers <= 0:
            raise ValueError(self._msg('ERR_MAX_WORKERS'))
    
    def _ensure_log_directory(self) -> None:
        """确保日志目录存在且可写"""
        try:
            os.makedirs(self.log_dir, exist_ok=True)
            if not os.access(self.log_dir, os.W_OK):
                raise RuntimeError(self._msg('ERR_DIR_NOT_WRITABLE', dir=self.log_dir))
        except OSError as e:
            raise RuntimeError(self._msg('ERR_CANNOT_CREATE_DIR', error=str(e)))
    
    def _get_log_format(self) -> str:
        """获取日志格式"""
        if self.custom_format:
            return self.custom_format
        
        return (
            "<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | "
            "<level>{level: <8}</level> | "
            "ReqID:{extra[request_id]} | "
            "<cyan>{file}</cyan>:<cyan>{line}</cyan> | "
            "<magenta>{process}</magenta> | "
            "<level>{message}</level>"
        )
    
    def _dynamic_level_kwargs(self, static_level: str) -> Dict[str, Any]:
        """根据是否启用自适应模式，返回 sink 的 level/filter 组合。

        - 非自适应：使用静态 level=，保留 loguru 的级别快路径（最高性能）。
        - 自适应：level 设为可达到的最低级别作为地板，filter 动态判级，
          改级别只需更新 self._min_level_no，无需重建 handler（核心优化）。
        """
        if not self._use_level_filter:
            return {"level": static_level}
        floor_no = min(self._safe_level_no(static_level), self._safe_level_no("DEBUG"))
        return {"level": floor_no, "filter": self._level_filter}

    def _add_console_handler(self, log_format: str) -> None:
        """添加控制台处理器
        
        注意：控制台输出不使用 enqueue，避免额外创建 multiprocessing 队列和信号灯。
        stdout 写入足够快，不需要异步队列缓冲。
        """
        kwargs = self._dynamic_level_kwargs(self.console_level or self.filter_level)
        handler_id = self.logger.add(
            sys.stdout,
            format=log_format,
            enqueue=False,
            diagnose=self.diagnose,
            backtrace=self.backtrace,
            serialize=self.console_serialize,
            **kwargs,
        )
        self._handler_ids.append(handler_id)
    
    def _add_file_handlers(self, log_format: str) -> None:
        """添加文件处理器"""
        # 主日志文件
        kwargs = self._dynamic_level_kwargs(self.file_level or self.filter_level)
        handler_id = self.logger.add(
            os.path.join(self.log_dir, f"{self.file_name}.log"),
            format=log_format,
            rotation=self.rotation_time or f"{self.max_size} MB",
            retention=self.retention,
            compression=self.compression,
            encoding='utf-8',
            enqueue=self.enqueue,
            diagnose=self.diagnose,
            backtrace=self.backtrace,
            serialize=self.serialize,
            **kwargs,
        )
        self._handler_ids.append(handler_id)
        
        # 错误日志文件（可选；关闭时省下一条常驻 enqueue 线程/队列/信号量）
        if self._error_file:
            handler_id = self.logger.add(
                self._get_level_log_path("error"),
                format=log_format,
                level=self.error_level,
                rotation=f"{self.max_size} MB",
                retention=self.retention,
                compression=self.compression,
                encoding='utf-8',
                enqueue=self.enqueue,
                diagnose=self.diagnose,
                backtrace=self.backtrace,
                serialize=self.serialize,
            )
            self._handler_ids.append(handler_id)
    
    def _fallback_configuration(self) -> None:
        """配置失败时的后备方案"""
        self._remove_handlers()
        handler_id = self.logger.add(
            sys.stderr,
            format="<red>{time:YYYY-MM-DD HH:mm:ss}</red> | <level>{level: <8}</level> | <level>{message}</level>",
            level="ERROR"
        )
        self._handler_ids.append(handler_id)

    def _configure_remote_logging(self):
        """
        配置远程日志收集。
        """
        # 当远程日志收集启用时，只发送 ERROR 及以上级别的日志
        handler_id = self.logger.add(
            self.remote_sink,
            level="ERROR",
            enqueue=self.enqueue,
        )
        self._handler_ids.append(handler_id)

    def log_with_tag(self, level: str, message: str, tag: str):
        """
        使用标签记录日志消息。
        
        Args:
            level: 日志级别 (info, debug, warning, error, critical)
            message: 日志消息
            tag: 标签名称
        """
        self._update_stats(level)
        # 级别门控：不会输出就跳过消息拼接
        if not self._emits(level.upper()):
            return
        logger_opt = self._logger_d1
        log_method = getattr(logger_opt, level.lower(), logger_opt.info)
        tagged_message = self._msg('LOG_TAGGED', tag=tag, message=message)
        log_method(tagged_message)
    
    def log_with_category(self, level: str, message: str, category: str):
        """
        使用分类记录日志消息。
        
        Args:
            level: 日志级别 (info, debug, warning, error, critical)
            message: 日志消息
            category: 分类名称
        """
        self._update_stats(level, category=category)
        # 级别门控：不会输出就跳过消息拼接
        if not self._emits(level.upper()):
            return
        logger_opt = self._logger_d1
        log_method = getattr(logger_opt, level.lower(), logger_opt.info)
        categorized_message = self._msg('LOG_CATEGORY', category=category, message=message)
        log_method(categorized_message)
    
    def setup_exception_handler(self):
        """
        设置统一的异常处理函数，将未处理的异常记录到日志。
        """
        def exception_handler(exc_type, exc_value, exc_traceback):
            if issubclass(exc_type, KeyboardInterrupt):
                # 允许程序被 Ctrl+C 中断
                sys.__excepthook__(exc_type, exc_value, exc_traceback)
                return
            
            try:
                # 获取调用栈信息
                import traceback
                tb = traceback.extract_tb(exc_traceback)
                
                # 安全地格式化异常信息
                error_msg = self._msg('UNHANDLED_EXCEPTION')
                
                # 安全地格式化异常值
                exc_value_str = str(exc_value) if exc_value is not None else "None"
                
                # 获取错误发生的具体位置
                if tb:
                    # 获取最后一个调用帧(通常是错误发生的地方)
                    last_frame = tb[-1]
                    error_location = f"{last_frame.filename}:{last_frame.lineno}:{last_frame.name}"
                    line_content = last_frame.line.strip() if last_frame.line else self._msg('UNKNOWN_CODE_LINE')
                else:
                    error_location = self._msg('UNKNOWN_LOCATION')
                    line_content = self._msg('UNKNOWN_CODE_LINE')
                
                # 组合详细的错误消息
                full_error_msg = (
                    f"{error_msg}: {exc_type.__name__}: {exc_value_str} | "
                    f"{self._msg('LABEL_LOCATION')}: {error_location} | "
                    f"{self._msg('LABEL_CODE')}: {line_content}"
                )
                
                # 记录详细错误信息
                self.logger.opt(exception=True).error(full_error_msg)
                
                # 记录调用链信息
                if len(tb) > 1:
                    call_chain = []
                    for frame in tb[-3:]:  # 只显示最后3层调用
                        call_chain.append(f"{frame.filename}:{frame.lineno}:{frame.name}")
                    self.logger.error(self._msg('CALL_CHAIN', chain=' -> '.join(call_chain)))
                    
            except Exception as e:
                # 如果格式化失败，使用最基本的错误记录
                self.logger.opt(exception=True).error(f"{self._msg('UNHANDLED_EXCEPTION')}: {exc_type.__name__}")

        if self._prev_excepthook is None:
            self._prev_excepthook = sys.excepthook
        self._exception_hook = exception_handler
        sys.excepthook = exception_handler

        # 子线程未捕获异常（Python 3.8+），避免子线程异常被静默吞掉
        try:
            if self._prev_threading_excepthook is None:
                self._prev_threading_excepthook = threading.excepthook

            def _thread_excepthook(args):
                if issubclass(args.exc_type, KeyboardInterrupt):
                    return
                try:
                    thread_name = getattr(args.thread, 'name', '?')
                    self.logger.opt(exception=(args.exc_type, args.exc_value, args.exc_traceback)).error(
                        self._msg('THREAD_UNHANDLED_EXCEPTION', thread=thread_name, error_type=args.exc_type.__name__)
                    )
                except Exception:
                    pass

            threading.excepthook = _thread_excepthook
        except Exception:
            pass

    def _get_level_log_path(self, level_name):
        """
        获取不同级别日志文件的路径。
        """
        return os.path.join(self.log_dir, f"{self.file_name}_{level_name}.log")

    _KNOWN_LEVELS = frozenset({
        "TRACE", "DEBUG", "INFO", "SUCCESS", "WARNING", "ERROR", "CRITICAL",
    })

    def _parse_log_level(self, line: str) -> Optional[str]:
        """从一行日志中精确解析级别名，支持 JSON(serialize) 与文本格式。

        - JSON 行：解析 record.level.name。
        - 文本行：按 '|' 分隔，取与已知级别完全相等的字段（不受消息正文中
          出现 'ERROR' 等字样的干扰，避免子串误判）。
        - 解析不出则返回 None。
        """
        s = line.strip()
        if not s:
            return None
        if s.startswith('{'):
            try:
                import json
                rec = json.loads(s)
                name = rec.get('record', {}).get('level', {}).get('name')
                if isinstance(name, str):
                    return name.upper()
            except Exception:
                return None
            return None
        # 文本格式：级别是某个独立的 '|' 分隔字段
        for part in line.split('|'):
            token = part.strip()
            if token in self._KNOWN_LEVELS:
                return token
        return None

    def _is_error_log_file(self, filename: str) -> bool:
        """判断是否为按级别拆分的错误日志文件（其内容是主日志的子集，分析时应排除以避免双计）。"""
        return filename.startswith(f"{self.file_name}_error")

    def _is_own_log_file(self, filename: str) -> bool:
        """判断文件名是否属于当前实例生成的日志文件（包括其轮转、压缩文件）。"""
        if not filename.startswith(self.file_name):
            return False

        if ".log" not in filename:
            return False

        if filename in self._active_log_filenames():
            return True

        if filename.startswith(f"{self.file_name}."):
            return True

        prefix = f"{self.file_name}_"
        if filename.startswith(prefix):
            rest = filename[len(prefix):]
            dot_idx = rest.find('.')
            if dot_idx != -1:
                level_part = rest[:dot_idx].upper()
                if level_part in self._KNOWN_LEVELS:
                    return True

        return False


    def _active_log_filenames(self) -> set:
        """返回当前正被 Loguru 写入的活跃日志文件名集合。

        这些文件不可被外部压缩/移动/删除，否则会与 Loguru 的写入和轮转竞争，
        导致日志损坏或丢失。手动维护方法必须跳过它们。
        """
        names = {f"{self.file_name}.log"}
        if self._error_file:
            names.add(os.path.basename(self._get_level_log_path("error")))
        return names

    def get_log_path(self, message):
        """
        如果需要将所有日志按照级别分文件时，可使用此方法。
        """
        log_level = message.record["level"].name.lower()
        log_file = f"{log_level}.log"
        return os.path.join(self.log_dir, log_file)

    def _start_remote_sender(self) -> None:
        if self._remote_thread and self._remote_thread.is_alive():
            return

        def remote_thread_target():
            try:
                import aiohttp
            except Exception:
                # aiohttp 未安装：放弃异步发送，标记就绪以便 remote_sink 回退到同步线程池
                self._remote_loop = None
                self._remote_queue = None
                self._remote_ready.set()
                return

            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            queue = asyncio.Queue(maxsize=self._remote_queue_size)

            async def remote_worker():
                connector = aiohttp.TCPConnector(limit=10, limit_per_host=5)
                timeout = aiohttp.ClientTimeout(total=5)
                session = aiohttp.ClientSession(connector=connector, timeout=timeout)
                try:
                    while True:
                        payload = await queue.get()
                        if payload is None:
                            queue.task_done()
                            break
                        try:
                            await self._post_remote_payload(session, payload)
                        finally:
                            queue.task_done()
                finally:
                    try:
                        await session.close()
                    except Exception:
                        pass
                    loop.stop()

            self._remote_loop = loop
            self._remote_queue = queue
            self._remote_ready.set()
            loop.create_task(remote_worker())
            loop.run_forever()
            try:
                loop.close()
            except Exception:
                pass

        self._remote_ready.clear()
        self._remote_thread = threading.Thread(target=remote_thread_target, daemon=True)
        self._remote_thread.start()
        self._remote_ready.wait(timeout=2)

    async def _post_remote_payload(self, session, payload: Dict[str, Any]) -> None:
        for attempt in range(3):
            try:
                async with session.post(
                    self.remote_log_url,
                    json=payload,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    response.raise_for_status()
                    return
            except Exception as e:
                if attempt == 2:
                    self.logger.warning(self._msg('FAILED_REMOTE', error=f"最终尝试失败: {e}"))
                    return
                await asyncio.sleep(1 * (attempt + 1))

    def _build_remote_payload(self, message: Any) -> Dict[str, Any]:
        log_entry = message.record
        try:
            time_str = log_entry["time"].strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            time_str = str(log_entry.get("time"))

        file_path = ""
        file_obj = log_entry.get("file")
        if file_obj:
            try:
                file_path = os.path.basename(file_obj.path)
            except Exception:
                file_path = str(file_obj)

        return {
            "time": time_str,
            "level": getattr(log_entry.get("level"), "name", str(log_entry.get("level"))),
            "message": log_entry.get("message", ""),
            "file": file_path,
            "line": log_entry.get("line"),
            "function": log_entry.get("function"),
            "request_id": log_entry.get("extra", {}).get("request_id", "no-request-id"),
        }

    def remote_sink(self, message):
        payload = self._build_remote_payload(message)
        if self._remote_loop and self._remote_queue and self._remote_ready.is_set():
            try:
                # 背压保护：队列接近上限时丢弃并计数，避免远端不可用时内存无限增长
                if self._remote_queue.qsize() >= self._remote_queue_size:
                    self._remote_dropped += 1
                    return
                self._remote_loop.call_soon_threadsafe(self._remote_queue.put_nowait, payload)
                return
            except Exception:
                pass
        if self._executor is None:
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
        self._executor.submit(self._send_payload_sync, payload)

    def get_remote_dropped(self) -> int:
        """返回因背压被丢弃的远程日志条数"""
        return self._remote_dropped

    def _stop_remote_sender(self) -> None:
        loop = self._remote_loop
        queue = self._remote_queue
        thread = self._remote_thread
        if loop and queue and thread and thread.is_alive():
            try:
                loop.call_soon_threadsafe(queue.put_nowait, None)
            except Exception:
                pass
            thread.join(timeout=2)
        self._remote_loop = None
        self._remote_queue = None
        self._remote_thread = None
        self._remote_ready.clear()

    def _send_payload_sync(self, payload: Dict[str, Any]) -> None:
        try:
            import requests
        except Exception:
            # requests 未安装且 aiohttp 也不可用：无法发送，仅记录一次告警
            if not getattr(self, '_remote_unavailable_warned', False):
                self._remote_unavailable_warned = True
                self.logger.warning(self._msg('FAILED_REMOTE', error="未安装 aiohttp/requests，远程日志已禁用"))
            return

        headers = {"Content-Type": "application/json"}
        max_retries = 3
        retry_delay = 1
        for attempt in range(max_retries):
            try:
                response = requests.post(
                    self.remote_log_url,
                    headers=headers,
                    json=payload,
                    timeout=5,
                )
                response.raise_for_status()
                return
            except requests.RequestException as e:
                if attempt == max_retries - 1:
                    self.logger.warning(self._msg('FAILED_REMOTE', error=f"Final attempt failed: {e}"))
                else:
                    time.sleep(retry_delay * (attempt + 1))

    def add_custom_level(self, level_name, no, color, icon):
        """
        增加自定义日志级别。

        Args:
            level_name (str): 日志级别名称。
            no (int): 日志级别编号。
            color (str): 日志级别颜色。
            icon (str): 日志级别图标。
        """
        try:
            self.logger.level(level_name, no=no, color=color, icon=icon)
            self.logger.opt(depth=1).debug(f"Custom log level '{level_name}' added.")
        except TypeError:
            # 如果日志级别已存在，记录调试信息
            self.logger.opt(depth=1).debug(f"Log level '{level_name}' already exists, skipping.")

    def capture_std_logging(self, level: str = "DEBUG",
                            names: Optional[List[str]] = None,
                            clear_existing: bool = True) -> None:
        """接管标准库 logging，把三方库（uvicorn/sqlalchemy/requests 等）日志统一汇入本管道。

        Args:
            level: 拦截的最低级别。
            names: 仅接管指定 logger 名称列表；None 表示接管 root（全局）。
            clear_existing: 是否清空目标 logger 既有的 handler。
        """
        log_module = logging
        bound_logger = self.logger

        class _InterceptHandler(log_module.Handler):
            def emit(self, record: "logging.LogRecord") -> None:
                try:
                    level_name = bound_logger.level(record.levelname).name
                except (ValueError, AttributeError):
                    level_name = record.levelno
                # loguru 官方推荐的调用深度计算：从 emit 自身帧向上跳过所有 logging 内部帧，
                # 精确定位到真实调用处（旧式 logging.currentframe() 会落在 logging 内部）。
                frame, depth = inspect.currentframe(), 0
                while frame and (depth == 0 or frame.f_code.co_filename == log_module.__file__):
                    frame = frame.f_back
                    depth += 1
                bound_logger.opt(depth=depth, exception=record.exc_info).log(
                    level_name, record.getMessage()
                )

        handler = _InterceptHandler()
        level_no = self._safe_level_no(level)

        if names:
            targets = [log_module.getLogger(n) for n in names]
        else:
            targets = [log_module.getLogger()]

        # 记录原状态以便 cleanup 时恢复
        saved = []
        for tgt in targets:
            saved.append((tgt, list(tgt.handlers), tgt.level, tgt.propagate))
            if clear_existing:
                tgt.handlers = [handler]
            else:
                tgt.addHandler(handler)
            tgt.setLevel(level_no)
            if names:
                tgt.propagate = False
        self._std_logging_state = saved

    def _restore_std_logging(self) -> None:
        """恢复被 capture_std_logging 修改的标准库 logging 状态"""
        state = self._std_logging_state
        if not state:
            return
        for tgt, handlers, level, propagate in state:
            try:
                tgt.handlers = handlers
                tgt.setLevel(level)
                tgt.propagate = propagate
            except Exception:
                continue
        self._std_logging_state = None

    def _setup_signal_handlers(self) -> None:
        """注册 SIGTERM/SIGINT，退出前调用 cleanup 排空 enqueue 队列，避免容器停服丢日志。"""
        import signal

        if threading.current_thread() is not threading.main_thread():
            return

        def _handler(signum, frame):
            # 先捕获原处理器：cleanup() 内部会调用 _restore_signal_handlers() 清空该字典
            prev = self._prev_signal_handlers.get(signum)
            try:
                self.cleanup()
            except Exception:
                pass
            if callable(prev):
                # 链式调用用户原有的信号处理器
                prev(signum, frame)
            elif prev == signal.SIG_IGN:
                # 原本忽略该信号：保持忽略，不强制退出
                return
            else:
                # 原本为默认行为(SIG_DFL/None)：恢复默认并重新触发，正常终止
                signal.signal(signum, signal.SIG_DFL)
                os.kill(os.getpid(), signum)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                self._prev_signal_handlers[sig] = signal.getsignal(sig)
                signal.signal(sig, _handler)
            except Exception:
                continue

    def _restore_signal_handlers(self) -> None:
        """恢复被 _setup_signal_handlers 替换的信号处理函数"""
        if not self._prev_signal_handlers:
            return
        try:
            import signal
            for sig, prev in self._prev_signal_handlers.items():
                try:
                    signal.signal(sig, prev)
                except Exception:
                    continue
        except Exception:
            pass
        self._prev_signal_handlers = {}

    def bind(self, **kwargs):
        """绑定结构化上下文字段（如 trace_id/span_id/user_id），返回带上下文的 logger。

        配合 serialize=True 时，这些字段会自动出现在 JSON 日志中。
        """
        return self.logger.bind(**kwargs)

    def contextualize(self, **kwargs):
        """在 with 作用域内临时注入结构化上下文字段（线程/协程安全）。

        示例：
            with logger.contextualize(trace_id="abc", span_id="01"):
                logger.info("处理中")
        """
        return self.logger.contextualize(**kwargs)

    def set_request_id(self, request_id: str):
        """设置当前上下文的 request_id，返回 token（可用于 reset）"""
        return self.request_id_var.set(request_id or "no-request-id")

    def get_request_id(self) -> str:
        """获取当前上下文的 request_id"""
        return self.request_id_var.get()

    def __getattr__(self, name: str):
        """
        使 YydsLogger 支持直接调用 Loguru 的日志级别方法。

        Args:
            name (str): 属性名称。
        """
        # 防止初始化异常时 _logger_d1 尚未创建导致无限递归
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        try:
            return getattr(self._logger_d1, name)
        except AttributeError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def log(self, level: str, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats(level)
        return self._logger_d1.log(level, message, *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("DEBUG")
        return self._logger_d1.debug(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("INFO")
        return self._logger_d1.info(message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("WARNING")
        return self._logger_d1.warning(message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("ERROR")
        return self._logger_d1.error(message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("CRITICAL")
        return self._logger_d1.critical(message, *args, **kwargs)

    def exception(self, message: str, *args, **kwargs):
        if self.enable_stats:
            self._update_stats("ERROR")
        return self._logger_d1.exception(message, *args, **kwargs)

    def log_decorator(self, msg: Optional[str] = None, level: str = "ERROR", trace: bool = True):
        """
        增强版日志装饰器，支持自定义日志级别和跟踪配置

        Args:
            msg (str): 支持多语言的异常提示信息key(使用_LANG_MAP中的键)
            level (str): 记录异常的日志级别(默认ERROR)
            trace (bool): 是否记录完整堆栈跟踪(默认True)
        """
        def decorator(func):
            _msg_key = msg or 'UNHANDLED_EXCEPTION'
            log_level = level.upper()

            if inspect.iscoroutinefunction(func):
                @wraps(func)
                async def async_wrapper(*args, **kwargs):
                    self._log_start(func.__name__, args, kwargs, is_async=True)
                    start_time = perf_counter()
                    try:
                        result = await func(*args, **kwargs)
                        duration = perf_counter() - start_time
                        self._log_end(func.__name__, result, duration, is_async=True)
                        return result
                    except Exception as e:
                        self._log_exception(func.__name__, e, _msg_key, log_level, trace, is_async=True)
                        if trace:
                            raise
                        return None
                return async_wrapper
            else:
                @wraps(func)
                def sync_wrapper(*args, **kwargs):
                    self._log_start(func.__name__, args, kwargs, is_async=False)
                    start_time = perf_counter()
                    try:
                        result = func(*args, **kwargs)
                        duration = perf_counter() - start_time
                        self._log_end(func.__name__, result, duration, is_async=False)
                        return result
                    except Exception as e:
                        self._log_exception(func.__name__, e, _msg_key, log_level, trace, is_async=False)
                        if trace:
                            raise
                        return None
                return sync_wrapper
        return decorator

    def time_it(self, func=None, *, line_by_line: bool = False):
        """
        函数耗时统计装饰器，支持同步和异步函数，可进行行级耗时分析。

        支持以下两种使用方式：
        1. 不带括号直接使用：
           @logger.time_it
           def my_func(): ...

        2. 带括号传递参数：
           @logger.time_it(line_by_line=True)
           def my_func(): ...

        Args:
            func: 被装饰的函数（不带括号使用时自动传入）。
            line_by_line (bool): 是否开启行级性能分析 (进阶功能)。
        """
        def decorator(f):
            if inspect.iscoroutinefunction(f):
                @wraps(f)
                async def async_wrapper(*args, **kwargs):
                    if not line_by_line:
                        start = perf_counter()
                        res = await f(*args, **kwargs)
                        duration_ms = (perf_counter() - start) * 1000.0

                        if self._info_level_no >= self._min_level_no:
                            log_msg = self._msg('TIMER_SUMMARY', func=f.__name__, duration=f"{duration_ms:.2f}")
                            self.logger.opt(depth=1).info(log_msg)
                        return res
                    else:
                        return await self._run_async_line_profiler(f, *args, **kwargs)
                return async_wrapper
            else:
                @wraps(f)
                def sync_wrapper(*args, **kwargs):
                    if not line_by_line:
                        start = perf_counter()
                        res = f(*args, **kwargs)
                        duration_ms = (perf_counter() - start) * 1000.0

                        if self._info_level_no >= self._min_level_no:
                            log_msg = self._msg('TIMER_SUMMARY', func=f.__name__, duration=f"{duration_ms:.2f}")
                            self.logger.opt(depth=1).info(log_msg)
                        return res
                    else:
                        return self._run_sync_line_profiler(f, *args, **kwargs)
                return sync_wrapper

        if func is not None:
            return decorator(func)
        return decorator

    def _run_sync_line_profiler(self, func, *args, **kwargs):
        try:
            from line_profiler import LineProfiler  # type: ignore
            has_lp = True
        except ImportError:
            has_lp = False

        if has_lp:
            lp = LineProfiler()
            lp_wrapper = lp(func)
            start_total = perf_counter()
            res = lp_wrapper(*args, **kwargs)
            duration_ms = (perf_counter() - start_total) * 1000.0
            
            stats = lp.get_stats()
            unit = getattr(stats, 'unit', 1e-6)
            line_times = {}
            line_hits = {}
            for key, timings in stats.timings.items():
                if key[2].split('.')[-1] == func.__name__:
                    for line_no, hits, total_time in timings:
                        line_times[line_no] = total_time * unit
                        line_hits[line_no] = hits
            self._print_line_profile_report(func, duration_ms, line_times, line_hits)
            return res
        else:
            import sys
            from collections import defaultdict

            target_code = func.__code__
            line_times = defaultdict(float)
            line_hits = defaultdict(int)
            
            last_time = [perf_counter()]
            last_line = [None]

            def tracer(frame, event, arg):
                if frame.f_code == target_code:
                    if event == 'line':
                        now = perf_counter()
                        if last_line[0] is not None:
                            line_times[last_line[0]] += now - last_time[0]
                        last_line[0] = frame.f_lineno
                        line_hits[last_line[0]] += 1
                        last_time[0] = perf_counter()
                    elif event == 'return':
                        now = perf_counter()
                        if last_line[0] is not None:
                            line_times[last_line[0]] += now - last_time[0]
                        last_line[0] = None
                return tracer

            old_trace = sys.gettrace()
            sys.settrace(tracer)
            
            start_total = perf_counter()
            try:
                res = func(*args, **kwargs)
            finally:
                sys.settrace(old_trace)
            duration_ms = (perf_counter() - start_total) * 1000.0
            
            self._print_line_profile_report(func, duration_ms, line_times, line_hits)
            return res

    async def _run_async_line_profiler(self, func, *args, **kwargs):
        try:
            from line_profiler import LineProfiler  # type: ignore
            has_lp = True
        except ImportError:
            has_lp = False

        if has_lp:
            lp = LineProfiler()
            lp_wrapper = lp(func)
            start_total = perf_counter()
            res = await lp_wrapper(*args, **kwargs)
            duration_ms = (perf_counter() - start_total) * 1000.0
            
            stats = lp.get_stats()
            unit = getattr(stats, 'unit', 1e-6)
            line_times = {}
            line_hits = {}
            for key, timings in stats.timings.items():
                if key[2].split('.')[-1] == func.__name__:
                    for line_no, hits, total_time in timings:
                        line_times[line_no] = total_time * unit
                        line_hits[line_no] = hits
            self._print_line_profile_report(func, duration_ms, line_times, line_hits)
            return res
        else:
            import sys
            from collections import defaultdict

            target_code = func.__code__
            line_times = defaultdict(float)
            line_hits = defaultdict(int)
            
            last_time = [perf_counter()]
            last_line = [None]

            def tracer(frame, event, arg):
                if frame.f_code == target_code:
                    if event == 'line':
                        now = perf_counter()
                        if last_line[0] is not None:
                            line_times[last_line[0]] += now - last_time[0]
                        last_line[0] = frame.f_lineno
                        line_hits[last_line[0]] += 1
                        last_time[0] = perf_counter()
                    elif event == 'return':
                        now = perf_counter()
                        if last_line[0] is not None:
                            line_times[last_line[0]] += now - last_time[0]
                        last_line[0] = None
                return tracer

            old_trace = sys.gettrace()
            sys.settrace(tracer)
            
            start_total = perf_counter()
            try:
                res = await func(*args, **kwargs)
            finally:
                sys.settrace(old_trace)
            duration_ms = (perf_counter() - start_total) * 1000.0
            
            self._print_line_profile_report(func, duration_ms, line_times, line_hits)
            return res

    def _print_line_profile_report(self, func, total_duration_ms, line_times, line_hits):
        import inspect
        try:
            lines, start_line = inspect.getsourcelines(func)
        except Exception as e:
            self.logger.opt(depth=3).warning(self._msg('CANNOT_GET_SOURCE', func=func.__name__, error=str(e)))
            return

        report = []
        border = "=" * 80
        report.append(border)
        report.append(self._msg('LINE_PROFILE_REPORT_HEADER', func=func.__name__))
        report.append(border)
        report.append(
            f"{self._msg('COL_LINE_NO'):<6} | "
            f"{self._msg('COL_HITS'):<8} | "
            f"{self._msg('COL_TOTAL_TIME'):<12} | "
            f"{self._msg('COL_AVG_TIME'):<12} | "
            f"{self._msg('COL_PCT'):<8} | "
            f"{self._msg('COL_SOURCE')}"
        )
        report.append("-" * 80)

        # 找出占比最高的行进行高亮提示
        max_pct = 0.0
        slowest_line = None
        for i in range(len(lines)):
            line_no = start_line + i
            time_spent_sec = line_times.get(line_no, 0.0)
            time_spent_ms = time_spent_sec * 1000.0
            pct = (time_spent_ms / total_duration_ms) * 100.0 if total_duration_ms > 0.0 else 0.0
            if pct > max_pct:
                max_pct = pct
                slowest_line = line_no

        for i, line_content in enumerate(lines):
            line_no = start_line + i
            hits = line_hits.get(line_no, 0)
            time_spent_sec = line_times.get(line_no, 0.0)
            time_spent_ms = time_spent_sec * 1000.0
            pct = (time_spent_ms / total_duration_ms) * 100.0 if total_duration_ms > 0.0 else 0.0
            avg_spent_ms = time_spent_ms / hits if hits > 0 else 0.0
            
            line_str = line_content.rstrip('\n')
            # 转义 loguru 自带的标签符号，防止解析报错
            line_str_escaped = line_str.replace("<", "\\<")
            
            is_bottleneck = (pct >= 20.0 and hits > 0) or (line_no == slowest_line and pct > 5.0)
            
            row_str = f" {line_no:>4} | {hits:>8} | {time_spent_ms:>11.2f} | {avg_spent_ms:>11.2f} | {pct:>7.1f}% | {line_str_escaped}"
            if is_bottleneck:
                row_str = f"<red><b>{row_str}  <-- 🚨 {self._msg('PERF_BOTTLENECK')}</b></red>"
                
            report.append(row_str)
            
        report.append("-" * 80)
        report.append(self._msg('TOTAL_DURATION_MS', duration=f"{total_duration_ms:.2f}"))
        report.append(border)
        
        report_text = "\n" + "\n".join(report)
        self.logger.opt(depth=3, colors=True).info(report_text)

    def _log_exception(self, func_name: str, error: Exception, msg_key: str,
                     level: str, trace: bool, is_async: bool):
        """统一的异常记录处理，增强错误信息显示"""
        try:
            _logger_d2 = self.logger.opt(depth=2)
            log_method = getattr(_logger_d2, level.lower(), _logger_d2.error)
            
            # 获取调用栈信息
            import traceback
            tb = traceback.extract_tb(error.__traceback__)
            
            # 安全地获取消息
            error_msg = self._msg(msg_key) if msg_key in self._LANG_MAP[self.language] else self._msg('OCCURRENCE_EXCEPTION', error=msg_key)
            
            # 安全地格式化错误信息
            error_type = type(error).__name__
            error_value = str(error) if error is not None else "None"
            
            # 获取错误发生的具体位置
            if tb:
                # 获取最后一个调用帧(通常是错误发生的地方)
                last_frame = tb[-1]
                error_location = f"{last_frame.filename}:{last_frame.lineno}:{last_frame.name}"
                line_content = last_frame.line.strip() if last_frame.line else self._msg('UNKNOWN_CODE_LINE')
            else:
                error_location = self._msg('UNKNOWN_LOCATION')
                line_content = self._msg('UNKNOWN_CODE_LINE')
            
            # 组合详细的错误消息
            full_error_msg = (
                f"{error_msg} [{error_type}]: {error_value} | "
                f"{self._msg('LABEL_LOCATION')}: {error_location} | "
                f"{self._msg('LABEL_CODE')}: {line_content}"
            )

            if trace:
                # 记录详细错误消息
                log_method(full_error_msg)
                # 记录完整的异常堆栈
                self.logger.opt(depth=2, exception=True).error(self._msg('FULL_EXCEPTION_STACK'))
                
                # 记录调用链信息
                if len(tb) > 1:
                    call_chain = []
                    for frame in tb[-3:]:  # 只显示最后3层调用
                        call_chain.append(f"{frame.filename}:{frame.lineno}:{frame.name}")
                    self.logger.opt(depth=2).error(self._msg('CALL_CHAIN', chain=' -> '.join(call_chain)))
            else:
                log_method(full_error_msg)  # log_method already has depth=2

            # 记录函数调用结束（INFO 不输出时跳过）
            if self._info_level_no >= self._min_level_no:
                end_msg = self._msg('END_ASYNC_FUNCTION_CALL' if is_async else 'END_FUNCTION_CALL')
                self.logger.opt(depth=2).info(end_msg)
            
        except Exception as e:
            # 如果格式化失败，使用最基本的错误记录
            self.logger.opt(depth=2).error(self._msg('ERROR_RECORDING_EXCEPTION', error=str(e)))
            if trace:
                self.logger.opt(depth=2, exception=True).error(self._msg('ORIGINAL_EXCEPTION_STACK'))

    @staticmethod
    def _format_value(val):
        """通用的参数/结果格式化"""
        try:
            if isinstance(val, (str, int, float, bool)):
                return str(val)
            elif isinstance(val, (list, tuple)):
                return f"[{len(val)} items]"
            elif isinstance(val, dict):
                return f"{{{len(val)} items}}"
            return str(val)
        except Exception:
            return f"<{type(val).__name__}>"

    def _log_start(self, func_name, args, kwargs, is_async=False):
        """
        记录函数调用开始的公共逻辑。
        """
        # 级别门控：INFO 不会输出时，跳过昂贵的逐参数格式化（热路径优化）
        if self._info_level_no < self._min_level_no:
            return
        args_str = [self._format_value(arg) for arg in args]
        kwargs_str = {k: self._format_value(v) for k, v in kwargs.items()}
        
        if is_async:
            self.logger.opt(depth=2).info(
                self._msg('CALLING_ASYNC_FUNCTION', 
                         func=func_name, 
                         args=args_str, 
                         kwargs=kwargs_str)
            )
        else:
            self.logger.opt(depth=2).info(
                self._msg('CALLING_FUNCTION', 
                         func=func_name, 
                         args=args_str, 
                         kwargs=kwargs_str)
            )

    def _log_end(self, func_name, result, duration, is_async=False):
        """
        记录函数调用结束的公共逻辑。
        """
        # 级别门控：INFO 不会输出时，跳过结果格式化（热路径优化）
        if self._info_level_no < self._min_level_no:
            return
        result_str = self._format_value(result)
        duration_str = f"{duration:.6f}"
        
        if is_async:
            self.logger.opt(depth=2).info(
                self._msg('ASYNC_FUNCTION_RETURNED', 
                         func=func_name, 
                         result=result_str, 
                         duration=duration_str)
            )
        else:
            self.logger.opt(depth=2).info(
                self._msg('FUNCTION_RETURNED', 
                         func=func_name, 
                         result=result_str, 
                         duration=duration_str)
            )
            
    def _update_stats(self, level: str, category: Optional[str] = None) -> None:
        """更新日志统计信息。

        - O2：仅统计真正会被输出的日志（被级别过滤掉的不计数，省热路径开销且统计更准）。
        - O1：by_hour 桶按上限裁剪，避免长期运行无界增长。
        - strftime / datetime.now() 等耗时操作尽量移到锁外或按需执行。
        """
        if not self.enable_stats:
            return

        level_upper = level.upper()
        # 级别门控：不会被任何 sink 输出的日志不计入统计
        if not self._emits(level_upper):
            return

        level_key = level_upper.lower()
        is_error = level_upper == 'ERROR'

        # 小时桶字符串缓存：仅跨小时时执行一次 strftime（热路径优化）
        now_ts = time.time()
        hour_index = int(now_ts // 3600)
        if hour_index != self._cached_hour_index:
            self._cached_hour_index = hour_index
            self._cached_hour_str = datetime.fromtimestamp(hour_index * 3600).strftime('%Y-%m-%d %H:00')
        current_hour = self._cached_hour_str

        # 仅错误日志才需要精确时间戳，普通日志省去 datetime.now() 开销
        now_dt = datetime.now() if is_error else None

        with self._stats_lock:
            self._stats['total'] += 1
            if level_key not in self._stats:
                self._stats[level_key] = 0
            self._stats[level_key] += 1

            if category:
                self._stats['by_category'][category] += 1

            by_hour = self._stats['by_hour']
            is_new_hour = current_hour not in by_hour
            by_hour[current_hour] += 1
            # 仅在新增小时桶且超过上限时裁剪最旧的桶，避免无界增长
            if is_new_hour and len(by_hour) > self._by_hour_max:
                for old_key in sorted(by_hour)[:len(by_hour) - self._by_hour_max]:
                    del by_hour[old_key]

            if is_error:
                self._stats['errors'].append({
                    'time': now_dt,
                    'message': f"Error occurred at {current_hour}"
                })
                self._stats['last_error_time'] = now_dt

        # 自适应日志级别：每 500 条日志自动检查一次
        if self.adaptive_level and self._stats['total'] % 500 == 0:
            try:
                self.set_adaptive_level()
            except Exception:
                pass
    
    @staticmethod
    def _safe_copy_stats(stats: Dict[str, Any]) -> Dict[str, Any]:
        """返回 stats 的安全拷贝：顶层 + 已知可变嵌套结构各自拷贝，避免调用方改动污染缓存。"""
        out = dict(stats)
        for k in ('by_category', 'by_hour'):
            v = out.get(k)
            if isinstance(v, dict):
                out[k] = dict(v)
        if isinstance(out.get('recent_errors'), list):
            out['recent_errors'] = [dict(e) if isinstance(e, dict) else e for e in out['recent_errors']]
        return out

    def get_stats(self) -> Dict[str, Any]:
        """获取详细的日志统计信息，优化性能"""
        current_time = datetime.now()
        
        # 检查缓存是否有效（返回深一层的安全拷贝，避免调用方改动污染缓存）
        if (current_time.timestamp() - self._stats_cache_time) < self._stats_cache_ttl:
            return self._safe_copy_stats(self._stats_cache)
        
        with self._stats_lock:
            total = int(self._stats.get('total', 0))
            error = int(self._stats.get('error', 0))
            error_rate = (error / total) if total else 0.0
            stats = {
                'total': total,
                'error': error,
                'warning': int(self._stats.get('warning', 0)),
                'info': int(self._stats.get('info', 0)),
                'debug': int(self._stats.get('debug', 0)),
                'duration': str(current_time - self._stats_start_time),
                'by_category': dict(self._stats['by_category']),
                'by_hour': dict(self._stats['by_hour']),
                'error_rate': float(error_rate),
                'time_since_last_error': str(current_time - self._stats['last_error_time']) if self._stats['last_error_time'] else None
            }
            
            # 计算每小时的平均日志数
            if stats['by_hour']:
                stats['avg_logs_per_hour'] = sum(stats['by_hour'].values()) / len(stats['by_hour'])
            
            # 获取最近的错误
            if self._stats['errors']:
                recent_errors = list(self._stats['errors'])[-10:]
                stats['recent_errors'] = [
                    {
                        'time': str(error['time']),
                        'message': str(error['message'])
                    }
                    for error in recent_errors
                ]
            
            # 更新缓存（缓存持有本次构建的对象；返回独立的安全拷贝，二者互不污染）
            self._stats_cache = stats
            self._stats_cache_time = current_time.timestamp()
            
            return self._safe_copy_stats(stats)
    
    def get_stats_summary(self) -> str:
        """获取统计信息的摘要"""
        stats = self.get_stats()
        return self._msg('LOG_STATS',
            total=stats['total'],
            error=stats['error'],
            warning=stats['warning'],
            info=stats['info']
        )
    
    def get_error_trend(self) -> List[Tuple[str, int]]:
        """获取错误趋势数据"""
        with self._stats_lock:
            return sorted(
                [(hour, count) for hour, count in self._stats['by_hour'].items()],
                key=lambda x: x[0]
            )
    
    def get_category_distribution(self) -> Dict[str, int]:
        """获取日志分类分布"""
        with self._stats_lock:
            return dict(self._stats['by_category'])
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        with self._stats_lock:
            self._stats = {
                'total': 0,
                'error': 0,
                'warning': 0,
                'info': 0,
                'debug': 0,
                'by_category': defaultdict(int),
                'by_hour': defaultdict(int),
                'errors': deque(maxlen=self._error_history_size),
                'last_error_time': None,
                'error_rate': 0.0
            }
            self._stats_start_time = datetime.now()

    def get_current_location(self) -> str:
        """获取当前调用位置信息，优化性能"""
        try:
            frame = inspect.currentframe()
            if not frame:
                return "未知位置"
            caller = frame.f_back
            if caller and caller.f_code.co_name == "log_with_location":
                caller = caller.f_back
            if not caller:
                return "未知位置"
            filename = caller.f_code.co_filename
            lineno = caller.f_lineno
            function = caller.f_code.co_name
            return f"{filename}:{lineno}:{function}"
        except Exception:
            return "未知位置"
        finally:
            try:
                del frame
            except Exception:
                pass

    def log_with_location(self, level: str, message: str, include_location: bool = True):
        """带位置信息的日志记录"""
        self._update_stats(level)
        # 级别门控：不会输出就跳过昂贵的 inspect 取栈与拼接
        if not self._emits(level.upper()):
            return
        if include_location:
            location = self.get_current_location()
            full_message = f"[{location}] {message}"
        else:
            full_message = message
        
        logger_opt = self._logger_d1
        log_method = getattr(logger_opt, level.lower(), logger_opt.info)
        log_method(full_message)

    def get_performance_stats(self) -> Dict[str, Any]:
        """获取性能统计信息"""
        return {
            'cache_sizes': {
                'message_cache': len(self._message_cache),
                'stats_cache': len(self._stats_cache)
            },
            'memory_usage': {
                'total_cache_size': (
                    len(self._message_cache) + 
                    len(self._stats_cache)
                )
            },
            'config': {
                'cache_size': self._cache_size,
                'stats_cache_ttl': self._stats_cache_ttl
            }
        }

    def clear_caches(self) -> None:
        """清除所有缓存"""
        self._message_cache.clear()
        self._stats_cache.clear()
        self._stats_cache_time = 0

    def batch_log(self, logs: List[Dict[str, Any]]) -> None:
        """批量记录日志，提高性能"""
        logger_opt = self.logger.opt(depth=1)
        for log_entry in logs:
            level = log_entry.get('level', 'INFO')
            message = log_entry.get('message', '')
            tag = log_entry.get('tag')
            category = log_entry.get('category')
            
            if tag:
                self._update_stats(level)
                if not self._emits(level.upper()):
                    continue
                tagged_message = self._msg('LOG_TAGGED', tag=tag, message=message)
                log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                log_method(tagged_message)
            elif category:
                self._update_stats(level, category=category)
                if not self._emits(level.upper()):
                    continue
                categorized_message = self._msg('LOG_CATEGORY', category=category, message=message)
                log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                log_method(categorized_message)
            else:
                self._update_stats(level)
                if not self._emits(level.upper()):
                    continue
                log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                log_method(message)

    async def async_batch_log(self, logs: List[Dict[str, Any]], yield_every: int = 0, sleep_s: float = 0.0) -> None:
        """异步批量记录日志

        Args:
            logs: 日志条目列表
            yield_every: 每处理 N 条日志让出一次事件循环(0 表示不主动让出)
            sleep_s: 让出时 sleep 的秒数；为 0 时使用 await asyncio.sleep(0)
        """
        logger_opt = self.logger.opt(depth=1)
        for i, log_entry in enumerate(logs, 1):
            level = log_entry.get('level', 'INFO')
            message = log_entry.get('message', '')
            tag = log_entry.get('tag')
            category = log_entry.get('category')
            
            if tag:
                self._update_stats(level)
                if self._emits(level.upper()):
                    tagged_message = self._msg('LOG_TAGGED', tag=tag, message=message)
                    log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                    log_method(tagged_message)
            elif category:
                self._update_stats(level, category=category)
                if self._emits(level.upper()):
                    categorized_message = self._msg('LOG_CATEGORY', category=category, message=message)
                    log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                    log_method(categorized_message)
            else:
                self._update_stats(level)
                if self._emits(level.upper()):
                    log_method = getattr(logger_opt, level.lower(), logger_opt.info)
                    log_method(message)

            if yield_every and (i % yield_every == 0):
                await asyncio.sleep(sleep_s if sleep_s > 0 else 0)

    def log_with_context(self, level: str, message: str, context: Optional[Dict[str, Any]] = None):
        """带上下文的日志记录"""
        self._update_stats(level)
        # 级别门控：不会输出就跳过上下文拼接
        if not self._emits(level.upper()):
            return
        if context:
            context_str = " | ".join([f"{k}={v}" for k, v in context.items()])
            full_message = f"{message} | {context_str}"
        else:
            full_message = message
        
        logger_opt = self._logger_d1
        log_method = getattr(logger_opt, level.lower(), logger_opt.info)
        log_method(full_message)

    def log_with_timing(self, level: str, message: str, timing_data: Dict[str, float]):
        """带计时信息的日志记录"""
        self._update_stats(level)
        # 级别门控：不会输出就跳过计时拼接
        if not self._emits(level.upper()):
            return
        timing_str = " | ".join([f"{k}={v:.3f}s" for k, v in timing_data.items()])
        full_message = f"{message} | {timing_str}"
        
        logger_opt = self._logger_d1
        log_method = getattr(logger_opt, level.lower(), logger_opt.info)
        log_method(full_message)

    def set_adaptive_level(self, error_rate_threshold: float = 0.1, 
                          log_rate_threshold: int = 1000) -> None:
        """设置自适应日志级别"""
        if not self.adaptive_level:
            return
        
        # 获取当前统计信息
        stats = self.get_stats()
        current_error_rate = stats.get('error_rate', 0.0)
        current_log_rate = stats.get('total', 0) / max(1, (datetime.now() - self._stats_start_time).total_seconds())
        
        # 根据错误率和日志频率调整级别
        if current_error_rate > error_rate_threshold or current_log_rate > log_rate_threshold:
            # 提高日志级别，减少日志输出
            if self.filter_level == "DEBUG":
                self.filter_level = "INFO"
                self._update_logger_level()
            elif self.filter_level == "INFO":
                self.filter_level = "WARNING"
                self._update_logger_level()
        else:
            # 降低日志级别，增加日志输出
            if self.filter_level == "WARNING":
                self.filter_level = "INFO"
                self._update_logger_level()
            elif self.filter_level == "INFO":
                self.filter_level = "DEBUG"
                self._update_logger_level()

    def _update_logger_level(self) -> None:
        """更新日志记录器级别。

        - 自适应模式（_use_level_filter=True）：sink 使用动态 filter，改级别只需更新
          _min_level_no（O(1)），不重建任何 handler，避免队列/信号灯反复创建（核心优化）。
        - 非自适应（如性能模式显式切换）：才重建 handler，这类调用很少。
        """
        self._min_level_no = self._safe_level_no(self.filter_level)
        if self._use_level_filter:
            return
        try:
            # configure_logger 内部已包含 _remove_handlers，无需重复调用
            self.configure_logger()
        except Exception as e:
            self.logger.warning(self._msg('UPDATE_LEVEL_FAILED', error=str(e)))

    def enable_performance_mode(self) -> None:
        """启用性能模式：提升过滤级别、增大缓存、关闭统计以降低开销。

        进入前会记录原始状态，disable 时精确恢复（而非硬编码 INFO）。
        """
        if self.performance_mode:
            return
        # 记录进入性能模式前的真实状态，便于精确恢复
        self._pre_perf_state = {
            'filter_level': self.filter_level,
            'enable_stats': self.enable_stats,
            'cache_size': self._cache_size,
        }
        self.performance_mode = True
        # 减少日志输出
        self.filter_level = "WARNING"
        self._update_logger_level()
        # 增加缓存大小
        self._cache_size = min(self._cache_size * 2, 2048)
        # 禁用详细统计
        self.enable_stats = False

    def disable_performance_mode(self) -> None:
        """禁用性能模式，恢复到进入前的原始状态。"""
        if not self.performance_mode:
            return
        self.performance_mode = False
        prev = getattr(self, '_pre_perf_state', None) or {}
        # 恢复日志级别（无记录时回退到 INFO 以兼容旧行为）
        self.filter_level = prev.get('filter_level', "INFO")
        self._update_logger_level()
        # 恢复缓存大小与统计开关
        self._cache_size = prev.get('cache_size', max(self._cache_size // 2, 128))
        self.enable_stats = prev.get('enable_stats', True)
        self._pre_perf_state = None

    def compress_logs(self, days_old: int = 7) -> None:
        """压缩指定天数之前的【已轮转】日志文件。

        注意：Loguru 已内置 rotation/compression/retention，通常无需手动调用本方法。
        本方法会自动跳过当前活跃的日志文件，避免与 Loguru 写入竞争导致日志损坏。
        """
        import gzip
        import shutil
        from pathlib import Path
        
        log_path = Path(self.log_dir)
        current_time = datetime.now()
        active = self._active_log_filenames()
        
        for log_file in log_path.glob(f"{self.file_name}*"):
            if not self._is_own_log_file(log_file.name):
                continue
            # 跳过活跃日志文件，禁止压缩正在写入的文件
            if log_file.name in active:
                continue
            try:
                # 检查文件修改时间
                file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                days_diff = (current_time - file_time).days
                
                if days_diff >= days_old and not log_file.name.endswith('.gz'):
                    # 压缩文件
                    with open(log_file, 'rb') as f_in:
                        gz_file = log_file.with_suffix('.log.gz')
                        with gzip.open(gz_file, 'wb') as f_out:
                            shutil.copyfileobj(f_in, f_out)
                    
                    # 删除原文件
                    log_file.unlink()
                    self.logger.opt(depth=1).info(self._msg('LOG_COMPRESSED', file=log_file.name))
                    
            except Exception as e:
                self.logger.opt(depth=1).error(self._msg('LOG_COMPRESS_FAILED', file=log_file.name, error=str(e)))

    def archive_logs(self, archive_dir: str = None) -> None:
        """归档【已轮转】日志文件。

        自动跳过当前活跃日志文件，避免移动正在写入的文件造成日志丢失。
        """
        import shutil
        from pathlib import Path
        
        if archive_dir is None:
            archive_dir = os.path.join(self.log_dir, "archive")
        
        os.makedirs(archive_dir, exist_ok=True)
        log_path = Path(self.log_dir)
        active = self._active_log_filenames()
        
        for log_file in log_path.glob(f"{self.file_name}*"):
            if not self._is_own_log_file(log_file.name):
                continue
            # 跳过活跃日志文件，禁止移动正在写入的文件
            if log_file.name in active:
                continue
            try:
                # 移动文件到归档目录
                archive_file = Path(archive_dir) / log_file.name
                shutil.move(str(log_file), str(archive_file))
                self.logger.opt(depth=1).info(self._msg('LOG_ARCHIVED', file=log_file.name))
                
            except Exception as e:
                self.logger.opt(depth=1).error(self._msg('LOG_ARCHIVE_FAILED', file=log_file.name, error=str(e)))

    def cleanup_old_logs(self, max_days: int = 30) -> None:
        """清理旧日志文件（自动跳过当前活跃日志文件）。"""
        from pathlib import Path
        
        log_path = Path(self.log_dir)
        current_time = datetime.now()
        active = self._active_log_filenames()
        
        for log_file in log_path.glob(f"{self.file_name}*"):
            if not self._is_own_log_file(log_file.name):
                continue
            # 跳过活跃日志文件，禁止删除正在写入的文件
            if log_file.name in active:
                continue
            try:
                # 检查文件修改时间
                file_time = datetime.fromtimestamp(log_file.stat().st_mtime)
                days_diff = (current_time - file_time).days
                
                if days_diff > max_days:
                    log_file.unlink()
                    self.logger.opt(depth=1).info(self._msg('LOG_DELETED', file=log_file.name))
                    
            except Exception as e:
                self.logger.opt(depth=1).error(self._msg('LOG_DELETE_FAILED', file=log_file.name, error=str(e)))

    def analyze_logs(self, hours: int = 24) -> Dict[str, Any]:
        """分析指定时间范围内的日志"""
        from pathlib import Path
        import re
        from collections import Counter
        
        log_path = Path(self.log_dir)
        current_time = datetime.now()
        start_time = current_time - timedelta(hours=hours)
        
        analysis = {
            'total_logs': 0,
            'error_count': 0,
            'warning_count': 0,
            'info_count': 0,
            'debug_count': 0,
            'error_rate': 0.0,
            'top_errors': [],
            'top_warnings': [],
            'hourly_distribution': defaultdict(int),
            'file_distribution': defaultdict(int),
            'function_distribution': defaultdict(int)
        }
        
        error_pattern = re.compile(r'ERROR.*?(\w+Error|Exception)', re.IGNORECASE)
        warning_pattern = re.compile(r'WARNING.*?(\w+Warning)', re.IGNORECASE)
        # 匹配日志行开头的时间戳，格式如 2025-01-03 14:05:23.456
        timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})')
        
        for log_file in log_path.glob(f"{self.file_name}*"):
            if not self._is_own_log_file(log_file.name):
                continue
            # 跳过错误日志文件：其内容是主日志的子集，否则 ERROR 会被双计
            if self._is_error_log_file(log_file.name):
                continue
            try:
                # 按文件修改时间快速跳过明显过旧的文件
                file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                if file_mtime < start_time:
                    continue
                
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        # 解析日志行时间戳，过滤不在时间范围内的行
                        ts_match = timestamp_pattern.search(line)
                        if ts_match:
                            try:
                                log_time = datetime.strptime(ts_match.group(1), '%Y-%m-%d %H:%M:%S')
                                if log_time < start_time:
                                    continue
                            except ValueError:
                                pass  # 解析失败则不过滤，继续处理
                        
                        analysis['total_logs'] += 1
                        
                        # 记录每小时分布
                        if ts_match:
                            try:
                                hour_key = log_time.strftime('%Y-%m-%d %H:00')
                                analysis['hourly_distribution'][hour_key] += 1
                            except Exception:
                                pass
                        
                        # 记录文件分布
                        analysis['file_distribution'][log_file.name] += 1
                        
                        # 精确解析日志级别（支持 JSON 与文本格式，避免消息正文中
                        # 含 ERROR/WARNING 字样导致的误判）
                        level = self._parse_log_level(line)
                        if level == 'ERROR' or level == 'CRITICAL':
                            analysis['error_count'] += 1
                            error_match = error_pattern.search(line)
                            if error_match:
                                analysis['top_errors'].append(error_match.group(1))
                        elif level == 'WARNING':
                            analysis['warning_count'] += 1
                            warning_match = warning_pattern.search(line)
                            if warning_match:
                                analysis['top_warnings'].append(warning_match.group(1))
                        elif level == 'INFO' or level == 'SUCCESS':
                            analysis['info_count'] += 1
                        elif level == 'DEBUG' or level == 'TRACE':
                            analysis['debug_count'] += 1
                        
            except Exception as e:
                self.logger.opt(depth=1).error(self._msg('LOG_ANALYZE_FAILED', file=log_file.name, error=str(e)))
        
        # 计算错误率
        if analysis['total_logs'] > 0:
            analysis['error_rate'] = analysis['error_count'] / analysis['total_logs']
        
        # 统计最常见的错误和警告
        analysis['top_errors'] = Counter(analysis['top_errors']).most_common(10)
        analysis['top_warnings'] = Counter(analysis['top_warnings']).most_common(10)
        
        return analysis

    def generate_log_report(self, hours: int = 24) -> str:
        """生成日志报告"""
        analysis = self.analyze_logs(hours)
        
        report = self._msg('REPORT_TEMPLATE',
            hours=hours,
            total=analysis['total_logs'],
            error=analysis['error_count'],
            warning=analysis['warning_count'],
            info=analysis['info_count'],
            debug=analysis['debug_count'],
            error_rate=analysis['error_rate']
        )
        
        report += self._msg('REPORT_TOP_ERRORS')
        for error_type, count in analysis['top_errors']:
            report += self._msg('REPORT_COUNT_SUFFIX', type=error_type, count=count)
        
        report += self._msg('REPORT_TOP_WARNINGS')
        for warning_type, count in analysis['top_warnings']:
            report += self._msg('REPORT_COUNT_SUFFIX', type=warning_type, count=count)
        
        return report

    def export_logs_to_json(self, output_file: str, hours: int = 24) -> None:
        """导出日志到JSON文件（流式写入，避免大文件 OOM）"""
        import json
        import re
        from pathlib import Path
        
        log_path = Path(self.log_dir)
        current_time = datetime.now()
        start_time = current_time - timedelta(hours=hours)
        timestamp_pattern = re.compile(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})')
        
        try:
            with open(output_file, 'w', encoding='utf-8') as out:
                out.write('[\n')
                first = True
                
                for log_file in log_path.glob(f"{self.file_name}*"):
                    if not self._is_own_log_file(log_file.name):
                        continue
                    # 跳过错误日志文件（主日志子集），避免导出重复内容
                    if self._is_error_log_file(log_file.name):
                        continue
                    try:
                        # 跳过明显过旧的文件
                        file_mtime = datetime.fromtimestamp(log_file.stat().st_mtime)
                        if file_mtime < start_time:
                            continue
                        
                        with open(log_file, 'r', encoding='utf-8') as f:
                            for line_num, line in enumerate(f, 1):
                                # 解析时间戳用于过滤和记录
                                log_timestamp = current_time.isoformat()
                                ts_match = timestamp_pattern.search(line)
                                if ts_match:
                                    try:
                                        log_time = datetime.strptime(ts_match.group(1), '%Y-%m-%d %H:%M:%S')
                                        if log_time < start_time:
                                            continue
                                        log_timestamp = log_time.isoformat()
                                    except ValueError:
                                        pass
                                
                                if not first:
                                    out.write(',\n')
                                json.dump({
                                    'file': log_file.name,
                                    'line_number': line_num,
                                    'content': line.strip(),
                                    'timestamp': log_timestamp
                                }, out, ensure_ascii=False)
                                first = False
                                
                    except Exception as e:
                        self.logger.opt(depth=1).error(self._msg('LOG_EXPORT_FAILED', file=log_file.name, error=str(e)))
                
                out.write('\n]')
            self.logger.opt(depth=1).info(self._msg('LOG_EXPORTED', file=output_file))
        except Exception as e:
            self.logger.opt(depth=1).error(self._msg('JSON_EXPORT_FAILED', error=str(e)))

    def __enter__(self):
        """支持 with 语句，自动管理资源生命周期"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 语句时自动清理资源"""
        self.cleanup()
        return False

    def cleanup(self) -> None:
        """清理资源，释放 enqueue 队列和信号灯。

        此方法是幂等的，多次调用安全（atexit + 手动调用不会冲突）。
        """
        if getattr(self, '_cleaned_up', False):
            return
        self._cleaned_up = True

        # 取消 atexit 注册，避免重复调用
        try:
            atexit.unregister(self.cleanup)
        except Exception:
            pass

        try:
            self._stop_remote_sender()
        except Exception:
            pass
        if self._exception_hook and self._prev_excepthook and sys.excepthook is self._exception_hook:
            try:
                sys.excepthook = self._prev_excepthook
            except Exception:
                pass
        # 恢复子线程异常钩子
        if self._prev_threading_excepthook is not None:
            try:
                threading.excepthook = self._prev_threading_excepthook
            except Exception:
                pass
            self._prev_threading_excepthook = None
        # 恢复标准库 logging 与信号处理
        try:
            self._restore_std_logging()
        except Exception:
            pass
        try:
            self._restore_signal_handlers()
        except Exception:
            pass
        if self._executor is not None:
            try:
                self._executor.shutdown(wait=False, cancel_futures=True)
            except Exception:
                try:
                    self._executor.shutdown(wait=False)
                except Exception:
                    pass
            self._executor = None
        # wait=True: 等待 enqueue 队列排空，确保信号灯正确释放
        self._remove_handlers(wait=True)
        self.clear_caches()
        # 多实例计数递减
        if getattr(self, '_instance_counted', False):
            try:
                with YydsLogger._instances_lock:
                    YydsLogger._active_instances = max(0, YydsLogger._active_instances - 1)
            except Exception:
                pass
            self._instance_counted = False
        _logger = logging.getLogger(__name__)
        _logger.info(self._msg('CLEANUP_COMPLETED'))
