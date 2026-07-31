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
import logging
import asyncio
import math

from typing import TYPE_CHECKING, Optional, Dict, Any, List

from contextvars import ContextVar
from datetime import datetime
import threading
import weakref
from time import monotonic
from dataclasses import dataclass

from .yyds_loguru import create_logger
from .i18n import LANG_MAP, get_message

if TYPE_CHECKING:
    from .yyds_loguru import Record


@dataclass(frozen=True)
class LoggerConfig:
    """Immutable snapshot of the effective logger configuration."""

    file_name: str
    log_dir: str
    max_size: int
    retention: str
    language: str
    custom_format: Optional[str]
    compression: Optional[str]
    enable_stats: bool
    env: str
    enqueue: bool
    diagnose: bool
    backtrace: bool
    serialize: bool
    console_serialize: bool
    console_level: Optional[str]
    file_level: Optional[str]
    queue_size: Optional[int]
    overflow_policy: str
    queue_timeout: Optional[float]
    process_isolation: bool
    queue_backend: str
    shutdown_timeout: Optional[float]
    defer_format: bool = False


class YydsLogger:
    """
    基于 Loguru 的增强日志记录器，具有以下功能：
    - 自定义日志格式
    - 日志轮转和保留策略
    - 上下文信息管理(如 request_id)
    - 装饰器用于记录函数调用和执行时间，支持同步/异步函数
    - 自定义日志级别(避免与 Loguru 预定义的冲突)
    - 统一异常处理

    新增：
    - 可指定语言(中文/英文)，默认中文
    - 支持按文件大小轮转日志
    - 支持自定义日志格式
    - 支持自定义压缩格式
    - 支持自定义文件命名模式
    """

    _global_resource_lock = threading.RLock()
    _global_resource_owners: Dict[str, weakref.ReferenceType] = {}
    _process_isolated_instances: weakref.WeakSet = weakref.WeakSet()
    _process_isolated_registry_lock = threading.Lock()
    _process_isolated_at_fork_registered = False
    _cleanup_owner_thread_id: Optional[int]
    _cleanup_error: Optional[BaseException]
    _active_shutdown_deadline: Optional[float]
    _signal_cleanup_error: Optional[BaseException]
    _CONFIG_FIELDS = (
        "log_dir", "max_size", "retention", "custom_format", "language", "enable_stats",
        "compression", "serialize", "console_serialize",
        "console_level", "file_level", "queue_size",
        "overflow_policy", "queue_timeout", "queue_backend", "shutdown_timeout",
        "defer_format",
        "process_isolation", "_process_file_name",
        "enqueue", "diagnose", "backtrace",
    )
    _RECONFIGURABLE_FIELDS = (
        frozenset(_CONFIG_FIELDS) - {"_process_file_name"}
    ) | {"enable_stats"}


    def __init__(
        self,
        file_name: str,                         # 日志文件基准名（用于区分同目录下不同日志器的日志文件以实现安全隔离操作）
        log_dir: str = 'logs',                 # 日志保存目录
        max_size: int = 10,                    # 单个文件最大大小（单位：MB）
        retention: str = '7 days',             # 日志保留策略
        language: str = 'zh',                  # 语言选项，默认为中文
        custom_format: Optional[str] = None,   # 新增：自定义日志格式
        compression: Optional[str] = "gz",     # 压缩格式；None 表示不压缩
        enable_stats: bool = False,            # 新增：是否启用日志统计
        env: str = 'prod',                     # 运行环境：'dev'/'prod'
        enqueue: Optional[bool] = None,        # 新增：显式覆盖 enqueue
        diagnose: Optional[bool] = None,       # 新增：显式覆盖 diagnose
        backtrace: Optional[bool] = None,      # 新增：显式覆盖 backtrace
        serialize: bool = False,               # 新增：文件输出 JSON 结构化日志
        console_serialize: bool = False,       # 新增：控制台输出 JSON 结构化日志
        console_level: Optional[str] = None,   # 新增：控制台独立级别
        file_level: Optional[str] = None,      # 新增：主文件独立级别
        queue_size: Optional[int] = 10000,     # 本地 enqueue 队列容量；None 表示无界队列
        overflow_policy: str = "block",       # block 或 drop
        queue_timeout: Optional[float] = 1.0,  # block 策略的最大等待时间
        process_isolation: bool = False,       # 多进程时将文件名隔离到 PID
        queue_backend: str = "auto",          # auto / multiprocessing / thread
        shutdown_timeout: Optional[float] = 30.0, # 队列控制/writer join 的单次等待上限
        defer_format: bool = False,            # 在线程队列 writer 侧执行格式化/序列化
    ) -> None:
        """
        初始化日志记录器。

        Args:
            file_name (str): 日志文件名称(主日志文件前缀)。
            log_dir (str): 日志文件目录。
            max_size (int): 日志文件大小(MB)超过时进行轮转。
            retention (str): 日志保留策略。
            language (str): 'zh' 或 'en'，表示日志输出语言，默认为中文。
            env (str): 'dev'/'prod'。生产环境默认关闭 diagnose/backtrace
                以避免泄漏变量值并降低开销，同时保持 enqueue=True 的异步文件写入。
            enqueue/diagnose/backtrace (bool, optional): 显式覆盖对应行为。
            serialize (bool): 文件输出 JSON 结构化日志（便于 ELK/Loki/Datadog 采集）。
            console_serialize (bool): 控制台输出 JSON 结构化日志。
            console_level/file_level (str): 控制台和主文件的独立级别。
            queue_backend (str): enqueue 队列实现。"auto" 默认使用本地线程队列；
                只有明确需要跨进程共享队列时才应选择 "multiprocessing"。
            shutdown_timeout (float, optional): 队列控制或 writer join 的单次最大等待秒数。
            defer_format (bool): 在线程 enqueue 队列的 writer 侧执行格式化、异常渲染和
                JSON 序列化，降低调用线程延迟；仅适用于 ``enqueue=True`` 且
                ``queue_backend="thread"`` 的文件 sink。
        """
        if not isinstance(file_name, str) or not file_name.strip():
            raise ValueError(get_message(language, "ERR_FILE_NAME"))
        if os.path.basename(file_name) != file_name:
            raise ValueError(get_message(language, "ERR_FILE_NAME"))
        self.file_name = file_name
        self._config_lock = threading.RLock()
        self.log_dir = log_dir
        self.max_size = max_size
        self.retention = retention
        
        # 保存新增的参数为实例属性
        self.custom_format = custom_format
        self.serialize = bool(serialize)
        self.console_serialize = bool(console_serialize)
        self.console_level = console_level
        self.file_level = file_level
        self.defer_format = bool(defer_format)
        if queue_size is not None:
            if isinstance(queue_size, bool) or not isinstance(queue_size, int) or queue_size <= 0:
                raise TypeError(get_message(language, "ERR_QUEUE_SIZE"))
        self.queue_size = queue_size
        self.overflow_policy = str(overflow_policy).lower()
        if isinstance(queue_timeout, bool):
            raise ValueError(get_message(language, "ERR_QUEUE_TIMEOUT"))
        try:
            self.queue_timeout = None if queue_timeout is None else float(queue_timeout)
        except (TypeError, ValueError) as exc:
            raise ValueError(get_message(language, "ERR_QUEUE_TIMEOUT")) from exc
        if self.overflow_policy not in {"block", "drop"}:
            raise ValueError(get_message(language, "ERR_OVERFLOW_POLICY"))
        if self.queue_timeout is not None and (
            self.queue_timeout < 0 or not math.isfinite(self.queue_timeout)
        ):
            raise ValueError(get_message(language, "ERR_QUEUE_TIMEOUT"))
        if isinstance(shutdown_timeout, bool):
            raise ValueError(get_message(language, "ERR_SHUTDOWN_TIMEOUT"))
        try:
            self.shutdown_timeout = (
                None if shutdown_timeout is None else float(shutdown_timeout)
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(get_message(language, "ERR_SHUTDOWN_TIMEOUT")) from exc
        if self.shutdown_timeout is not None and (
            self.shutdown_timeout <= 0 or not math.isfinite(self.shutdown_timeout)
        ):
            raise ValueError(get_message(language, "ERR_SHUTDOWN_TIMEOUT"))
        self.process_isolation = bool(process_isolation)
        requested_queue_backend = str(queue_backend).strip().lower()
        if requested_queue_backend == "auto":
            self.queue_backend = "thread"
        elif requested_queue_backend in {"multiprocessing", "thread"}:
            self.queue_backend = requested_queue_backend
        else:
            raise ValueError(get_message(language, "ERR_QUEUE_BACKEND"))
        self._process_file_name = (
            f"{file_name}.pid{os.getpid()}"
            if self.process_isolation else file_name
        )
        self.compression = compression
        # 级别号缓存与统计相关阈值
        self._level_no_cache: Dict[str, int] = {}
        self.enable_stats = enable_stats
        self._stats_ready = False
        self._handler_ids: List[int] = []
        self._failed_handler_stops: List[Any] = []
        self._dropped_messages_total = 0
        self._serialization_errors_total = 0
        self._drop_reasons_total = {
            "overflow": 0,
            "block_timeout": 0,
            "serialization": 0,
        }
        self._removed_default_handler = False
        self._cleaned_up = False
        self._cleanup_state = "open"
        self._cleanup_event = threading.Event()
        self._cleanup_event.set()
        self._cleanup_owner_thread_id = None
        self._cleanup_error = None
        self._active_shutdown_deadline = None
        self._exception_hook = None
        self._threading_exception_hook = None
        self._prev_excepthook = None
        self._std_logging_state = None
        self._prev_signal_handlers: Dict[Any, Any] = {}
        self._signal_handlers: Dict[Any, Any] = {}
        self._signal_cleanup_event = threading.Event()
        self._signal_cleanup_stop = threading.Event()
        self._signal_cleanup_thread = None
        self._signal_cleanup_default_signal = None
        self._signal_cleanup_error = None
        self._prev_threading_excepthook = None

        # 语言选项
        self.language = str(language).strip().lower()

        # 定义上下文变量，用于存储 request_id
        self.request_id_var = ContextVar("request_id", default="-")

        # 使用 patch 确保每条日志记录都包含 'request_id'
        self._raw_logger = create_logger(stderr=False, register_atexit=False)
        self.logger = self._raw_logger.patch(self._patch_record)
        # 缓存常用的 opt(depth=1)，减少热路径对象创建开销
        self._logger_d1 = self.logger.opt(depth=1)

        # 解析运行环境；显式 enqueue/diagnose/backtrace 可覆盖环境默认值。
        self.env = str(env).strip().lower()
        if self.env in {"development", "debug", "test"}:
            self.env = "dev"
        elif self.env in {"production", "release"}:
            self.env = "prod"
        elif self.env not in {"dev", "prod"}:
            raise ValueError(get_message(language, "ERR_ENV"))
        is_prod = self.env == "prod"
        # 生产环境关闭诊断/回溯，但仍以 enqueue=True 使用异步文件写入。
        default_enqueue, default_diagnose, default_backtrace = True, (not is_prod), (not is_prod)

        self.enqueue = default_enqueue if enqueue is None else bool(enqueue)
        self.diagnose = default_diagnose if diagnose is None else bool(diagnose)
        self.backtrace = default_backtrace if backtrace is None else bool(backtrace)

        # 级别号缓存：用于热路径门控。
        self._info_level_no = self._safe_level_no("INFO")
        self._refresh_sink_level_nos()
        # 初始化 Logger 配置
        self.configure_logger()

        self._stats_lock = threading.Lock()
        self._stats = {
            'total': 0,
            'error': 0,
            'warning': 0,
            'info': 0,
            'debug': 0,
        }
        self._stats_start_time = datetime.now()
        self._stats_ready = True
        # 注册 atexit 钩子，确保程序退出时自动清理 enqueue 队列和信号灯
        atexit.register(self.cleanup)
        self._register_process_isolated_instance()

        self._instance_counted = False

    def _register_process_isolated_instance(self) -> None:
        """Register one lightweight child-fork hook for PID-isolated loggers.

        ``process_isolation`` used to only calculate the PID once in
        ``__init__``.  A pre-fork server therefore made every worker inherit
        the master's filename and queue handlers.  Keep weak references so
        the process-wide hook does not prolong a logger's lifetime.
        """
        if not self.process_isolation or not hasattr(os, "register_at_fork"):
            return

        cls = type(self)
        with cls._process_isolated_registry_lock:
            cls._process_isolated_instances.add(self)
            if not cls._process_isolated_at_fork_registered:
                os.register_at_fork(after_in_child=cls._reset_process_isolated_loggers_after_fork)
                cls._process_isolated_at_fork_registered = True

    @classmethod
    def _reset_process_isolated_loggers_after_fork(cls) -> None:
        """Recreate PID-isolated engines in a forked child process.

        This callback deliberately avoids inherited instance locks: a parent
        thread may have held one at fork time.  The embedded engine is rebuilt
        instead of attempting to stop queues whose writer threads only exist
        in the parent process.
        """
        cls._global_resource_lock = threading.RLock()
        for instance in list(cls._process_isolated_instances):
            instance._reset_after_fork()

    def _reset_after_fork(self) -> None:
        """Replace parent-owned sinks with fresh child-local PID sinks."""
        if getattr(self, "_cleanup_state", "open") != "open":
            return

        self._config_lock = threading.RLock()
        self._stats_lock = threading.Lock()
        self._stats_ready = False
        self._stats = {
            "total": 0,
            "error": 0,
            "warning": 0,
            "info": 0,
            "debug": 0,
        }
        self._stats_start_time = datetime.now()
        self.request_id_var = ContextVar("request_id", default="-")
        self._process_file_name = f"{self.file_name}.pid{os.getpid()}"
        self._handler_ids = []
        self._failed_handler_stops = []
        self._dropped_messages_total = 0
        self._serialization_errors_total = 0
        self._drop_reasons_total = {
            "overflow": 0,
            "block_timeout": 0,
            "serialization": 0,
        }
        self._removed_default_handler = False
        self._level_no_cache = {}
        self._cleanup_event = threading.Event()
        self._cleanup_event.set()
        self._cleanup_owner_thread_id = None
        self._cleanup_error = None
        self._active_shutdown_deadline = None
        self._signal_cleanup_event = threading.Event()
        self._signal_cleanup_stop = threading.Event()
        self._signal_cleanup_thread = None
        self._signal_cleanup_default_signal = None
        self._signal_cleanup_error = None
        if self._signal_handlers:
            # Signal callbacks are inherited across fork, but their parent
            # worker thread is not. Recreate only the local notifier; the
            # already-installed callbacks read the refreshed event from the
            # logger instance.
            from .lifecycle import _start_signal_cleanup_worker

            try:
                _start_signal_cleanup_worker(self)
            except Exception as exc:
                self._signal_cleanup_error = exc

        # Do not remove inherited handlers: their queue writers belong to the
        # parent process.  Replacing the whole Core is safe in the child and
        # leaves no parent-owned queue or file descriptor in use.
        self._raw_logger = create_logger(stderr=False, register_atexit=False)
        self.logger = self._raw_logger.patch(self._patch_record)
        self._logger_d1 = self.logger.opt(depth=1)
        self._info_level_no = self._safe_level_no("INFO")
        self._refresh_sink_level_nos()

        try:
            self._configure_logger()
        except Exception:
            # ``_configure_logger()`` already installs its stderr fallback
            # when a runtime resource (for example the log directory) fails.
            self._cleanup_state = "failed"
            return

        self._stats_ready = True

    def _patch_record(self, record: "Record") -> None:
        """Inject request context and collect stats for every logger entry point."""
        record["extra"].setdefault("request_id", self.request_id_var.get() or "-")
        if self._stats_ready and self.enable_stats:
            self._update_stats(record["level"].name)

    def _claim_global_resource(self, resource: str) -> None:
        """Prevent multiple instances from silently overwriting process-wide hooks."""
        with self._global_resource_lock:
            owner_ref = self._global_resource_owners.get(resource)
            owner = owner_ref() if owner_ref is not None else None
            if owner is not None and owner is not self:
                raise RuntimeError(
                    get_message(self.language, "ERR_GLOBAL_RESOURCE", resource=resource)
                )
            self._global_resource_owners[resource] = weakref.ref(self)

    def _release_global_resource(self, resource: str) -> None:
        with self._global_resource_lock:
            owner_ref = self._global_resource_owners.get(resource)
            if owner_ref is not None and owner_ref() is self:
                self._global_resource_owners.pop(resource, None)

    def _ensure_open(self) -> None:
        if getattr(self, "_cleanup_state", "open") != "open":
            raise RuntimeError(self._msg("ERR_LOGGER_CLOSED"))

    def _config_snapshot(self) -> Dict[str, Any]:
        return {name: getattr(self, name) for name in self._CONFIG_FIELDS}

    @property
    def config(self) -> LoggerConfig:
        """Return an immutable snapshot of all effective public settings."""
        return LoggerConfig(
            file_name=self.file_name,
            log_dir=self.log_dir,
            max_size=self.max_size,
            retention=self.retention,
            language=self.language,
            custom_format=self.custom_format,
            compression=self.compression,
            enable_stats=bool(self.enable_stats),
            env=self.env,
            enqueue=bool(self.enqueue),
            diagnose=bool(self.diagnose),
            backtrace=bool(self.backtrace),
            serialize=bool(self.serialize),
            console_serialize=bool(self.console_serialize),
            console_level=self.console_level,
            file_level=self.file_level,
            queue_size=self.queue_size,
            overflow_policy=self.overflow_policy,
            queue_timeout=self.queue_timeout,
            process_isolation=bool(self.process_isolation),
            queue_backend=self.queue_backend,
            shutdown_timeout=self.shutdown_timeout,
            defer_format=bool(self.defer_format),
        )

    def reconfigure(self, **changes: Any) -> LoggerConfig:
        """Validate and atomically apply a set of configuration changes."""
        unknown = sorted(set(changes) - self._RECONFIGURABLE_FIELDS)
        if unknown:
            raise TypeError(self._msg("ERR_RECONFIGURE_FIELDS", fields=", ".join(unknown)))

        with self._config_lock:
            self._ensure_open()
            normalized = dict(changes)
            if "language" in normalized:
                normalized["language"] = str(normalized["language"]).strip().lower()
            if "overflow_policy" in normalized:
                normalized["overflow_policy"] = str(normalized["overflow_policy"]).strip().lower()
            if "queue_backend" in normalized:
                backend = str(normalized["queue_backend"]).strip().lower()
                normalized["queue_backend"] = "thread" if backend == "auto" else backend
            for name in ("queue_timeout", "shutdown_timeout"):
                if name in normalized and normalized[name] is not None:
                    if isinstance(normalized[name], bool):
                        key = "ERR_QUEUE_TIMEOUT" if name == "queue_timeout" else "ERR_SHUTDOWN_TIMEOUT"
                        raise ValueError(self._msg(key))
                    try:
                        normalized[name] = float(normalized[name])
                    except (TypeError, ValueError) as exc:
                        key = "ERR_QUEUE_TIMEOUT" if name == "queue_timeout" else "ERR_SHUTDOWN_TIMEOUT"
                        raise ValueError(self._msg(key)) from exc
            for name in (
                "serialize",
                "console_serialize",
                "process_isolation",
                "enqueue",
                "diagnose",
                "backtrace",
                "enable_stats",
                "defer_format",
            ):
                if name in normalized:
                    normalized[name] = bool(normalized[name])

            for name, value in normalized.items():
                setattr(self, name, value)
            if "process_isolation" in normalized:
                self._process_file_name = (
                    f"{self.file_name}.pid{os.getpid()}"
                    if self.process_isolation else self.file_name
                )
            self._configure_logger()
            return self.config

    def _restore_config(self, snapshot: Dict[str, Any]) -> None:
        for name, value in snapshot.items():
            setattr(self, name, value)
        self._refresh_sink_level_nos()

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

    def _refresh_sink_level_nos(self) -> None:
        """Refresh derived level thresholds for every active sink."""
        self._console_level_no = (
            0 if self.console_level is None else self._safe_level_no(self.console_level)
        )
        self._file_level_no = (
            0 if self.file_level is None else self._safe_level_no(self.file_level)
        )
        self._min_level_no = min(
            self._console_level_no,
            self._file_level_no,
        )

    def _emits(self, level_upper: str) -> bool:
        """Return whether any active sink emits the given level."""
        no = self._level_no(level_upper)
        return any(
            no >= sink_level
            for sink_level in (
                self._console_level_no,
                self._file_level_no,
            )
        )

    def is_level_enabled(self, level: str) -> bool:
        """判断指定级别是否会被任一已启用 sink 输出。"""
        return self._emits(level)

    def _msg(self, key: str, **kwargs) -> str:
        """消息格式化处理，优化性能

        对无参消息缓存模板文本；对有参消息直接格式化（参数通常每次不同，
        缓存命中率极低，省去昂贵的 key 序列化开销）。
        """
        try:
            messages = LANG_MAP.get(self.language) or LANG_MAP["zh"]
            if not kwargs:
                return messages.get(key, key)

            # 有参消息：直接格式化，不做缓存
            text = messages.get(key, key)
            str_kwargs = {}
            for k, v in kwargs.items():
                try:
                    str_kwargs[k] = str(v)
                except Exception:
                    str_kwargs[k] = f"<{type(v).__name__}>"
            return text.format(**str_kwargs)

        except KeyError as e:
            messages = LANG_MAP.get(self.language) or LANG_MAP["zh"]
            text = messages.get(key, key)
            err_tpl = messages.get('FORMAT_ERR_MISSING_PARAM', " (格式化错误: 缺少参数 {error})")
            return f"{text}{err_tpl.format(error=str(e))}"
        except Exception as e:
            messages = LANG_MAP.get(self.language) or LANG_MAP["zh"]
            text = messages.get(key, key)
            err_tpl = messages.get('FORMAT_ERR_GENERIC', " (格式化错误: {error})")
            return f"{text}{err_tpl.format(error=str(e))}"

    @staticmethod
    def _remaining_timeout(deadline: Optional[float]) -> Optional[float]:
        if deadline is None:
            return None
        return max(0.0, deadline - monotonic())

    def _shutdown_deadline(self) -> Optional[float]:
        if self.shutdown_timeout is None:
            return None
        return monotonic() + float(self.shutdown_timeout)

    def _remove_handlers(
        self,
        wait: bool = False,
        strict: bool = False,
        deadline: Optional[float] = None,
    ) -> None:
        if deadline is None:
            deadline = self._active_shutdown_deadline
        handler_ids = list(self._handler_ids)
        active_handlers = {
            getattr(handler, "_id", None): handler
            for handler in getattr(self.logger._core, "handlers", {}).values()
            if getattr(handler, "_id", None) in handler_ids
        }

        # 仅当本实例存在 enqueue 文件 handler 时才需要排空与 gc：
        # 控制台始终非 enqueue；enqueue=False 时没有 multiprocessing 队列/信号灯，
        # 无需 complete()/gc，也避免无意义地等待全局所有 handler。
        # Derive this from the handlers being retired, not the newly edited
        # ``self.enqueue`` value during reconfiguration.
        need_drain = bool(
            wait and any(getattr(handler, "_enqueue", False) for handler in active_handlers.values())
        )

        if need_drain:
            # 在 remove 之前调用 complete()，等待所有 enqueue 队列排空
            try:
                asyncio.get_running_loop()
            except RuntimeError:
                self._run_complete(deadline=deadline)
            else:
                # ``remove()`` below still drains via its FIFO stop marker.  A
                # synchronous close inside an event loop can therefore proceed
                # without the former unconditional ``time.sleep(0.1)`` delay.
                pass

        # Keep managed IDs and counters unchanged if draining fails so a later
        # cleanup can retry this phase without taking a fallback path or
        # counting the same dropped records twice.
        self._handler_ids.clear()
        for handler in active_handlers.values():
            self._collect_handler_metrics(handler)

        first_error = None
        for handler_id in handler_ids:
            try:
                remaining = self._remaining_timeout(deadline)
                if deadline is None:
                    self.logger.remove(handler_id)
                else:
                    self.logger.remove(handler_id, timeout=remaining)
            except Exception as exc:
                handler = active_handlers.get(handler_id)
                if handler is not None and handler not in self._failed_handler_stops:
                    self._failed_handler_stops.append(handler)
                if first_error is None:
                    first_error = exc
                continue

        if need_drain:
            # 强制垃圾回收，确保 multiprocessing.SimpleQueue 及其底层信号灯被释放
            import gc
            gc.collect()
        if strict and first_error is not None:
            raise RuntimeError("Failed to remove one or more logger handlers") from first_error

    def _remove_remaining_handlers(
        self,
        strict: bool = False,
        deadline: Optional[float] = None,
    ) -> None:
        if deadline is None:
            deadline = self._active_shutdown_deadline
        """Remove sinks added through the exposed embedded logger as well."""
        remaining = {
            getattr(handler, "_id", None): handler
            for handler in getattr(self.logger._core, "handlers", {}).values()
        }
        for handler in remaining.values():
            self._collect_handler_metrics(handler)
        first_error = None
        for handler_id in remaining:
            try:
                remaining_timeout = self._remaining_timeout(deadline)
                if deadline is None:
                    self.logger.remove(handler_id)
                else:
                    self.logger.remove(handler_id, timeout=remaining_timeout)
            except Exception as exc:
                handler = remaining.get(handler_id)
                if handler is not None and handler not in self._failed_handler_stops:
                    self._failed_handler_stops.append(handler)
                if first_error is None:
                    first_error = exc
        if strict and first_error is not None:
            raise RuntimeError("Failed to remove one or more unmanaged logger handlers") from first_error

    def _collect_handler_metrics(self, handler) -> None:
        if getattr(handler, "_metrics_collected", False):
            return
        handler._metrics_collected = True
        self._dropped_messages_total += int(getattr(handler, "dropped_messages", 0))
        self._serialization_errors_total += int(
            getattr(handler, "serialization_errors", 0)
        )
        for reason, count in getattr(handler, "drop_reasons", {}).items():
            self._drop_reasons_total[reason] = self._drop_reasons_total.get(reason, 0) + int(count)

    def _retry_failed_handler_stops(
        self,
        strict: bool = False,
        deadline: Optional[float] = None,
    ) -> None:
        if deadline is None:
            deadline = self._active_shutdown_deadline
        pending = list(self._failed_handler_stops)
        self._failed_handler_stops.clear()
        first_error = None
        for handler in pending:
            try:
                remaining = self._remaining_timeout(deadline)
                if deadline is None:
                    handler.stop()
                else:
                    handler.stop(timeout=remaining)
            except Exception as exc:
                self._failed_handler_stops.append(handler)
                if first_error is None:
                    first_error = exc
        if strict and first_error is not None:
            raise RuntimeError("Failed to finish stopping logger handlers") from first_error

    def _run_complete(self, deadline: Optional[float] = None) -> None:
        """在同步上下文中正确驱动 Loguru 的 awaitable complete。"""
        if deadline is None:
            deadline = self._active_shutdown_deadline
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            async def wait_complete():
                remaining = self._remaining_timeout(deadline)
                completer = self.logger.complete(timeout=remaining)
                if remaining is None:
                    await completer
                else:
                    try:
                        await asyncio.wait_for(
                            completer,
                            timeout=self._remaining_timeout(deadline),
                        )
                    except asyncio.TimeoutError as exc:
                        raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED")) from exc

            asyncio.run(wait_complete())
            return
        raise RuntimeError(self._msg("ERR_RUNNING_LOOP"))

    def configure_logger(self) -> None:
        """配置日志记录器，添加错误处理和安全性检查"""
        with self._config_lock:
            self._ensure_open()
            self._configure_logger()

    def _configure_logger(self) -> None:
        old_handler_ids = list(self._handler_ids)
        old_config = getattr(self, "_last_good_config", None)
        has_previous_config = old_config is not None
        staged_logger = None
        staged_handlers = {}
        committed = False
        try:
            # 所有校验必须在修改 handler 之前完成。
            self._validate_config()
            self._ensure_log_directory()

            # Recompute effective levels before building the isolated handler
            # set.  The current core remains untouched while files are opened
            # and formats are compiled.
            self._refresh_sink_level_nos()
            log_format = self._get_log_format()

            staged_logger, new_handler_ids, staged_handlers = self._build_staged_handlers(log_format)
            old_handlers = self._swap_managed_handlers(old_handler_ids, new_handler_ids, staged_handlers)
            committed = True
            self._handler_ids = list(new_handler_ids)

            # 重新缓存 opt(depth=1)，确保使用最新的 logger 配置
            self._logger_d1 = self.logger.opt(depth=1)
            self._last_good_config = self._config_snapshot()
            # The cutover is already committed.  Retire old handlers after
            # the swap; a slow/custom sink is kept in the retry list without
            # exposing both generations to new records.
            if old_handlers:
                self._retire_detached_handlers(old_handlers, wait=True, strict=False)
            return
        except Exception as e:
            if committed:
                # The active core has already been atomically switched.  Do
                # not roll it back after a bookkeeping/retirement failure;
                # keep the new configuration and leave detached handlers in
                # the retry list instead.
                raise
            if staged_handlers:
                self._retire_detached_handlers(staged_handlers, wait=False, strict=False)
            if has_previous_config:
                assert old_config is not None
                self._restore_config(old_config)
                self._handler_ids = old_handler_ids
                self._logger_d1 = self.logger.opt(depth=1)
                raise
            if isinstance(e, (ValueError, TypeError)):
                raise
            # 初次配置失败时没有旧 handler，只能提供 stderr 后备输出。
            self._handler_ids = []
            self._fallback_configuration()
            raise RuntimeError(self._msg('ERR_CONFIG_FAILED', error=str(e))) from e

    def _build_staged_handlers(self, log_format: str):
        """Build managed handlers on an isolated core and reserve unique IDs."""
        staged_logger = create_logger(stderr=False, register_atexit=False)
        current_core = self.logger._core
        staged_core = staged_logger._core
        with current_core.lock:
            start_id = current_core.handlers_count
            current_core.handlers_count += 2
            # Preserve custom levels registered through the exposed embedded
            # logger.  Handler construction only needs these immutable lookup
            # tables; the staged core never receives user records.
            staged_core.levels = current_core.levels.copy()
            staged_core.levels_lookup = current_core.levels_lookup.copy()
            staged_core.levels_ansi_codes = current_core.levels_ansi_codes
        staged_core.handlers_count = start_id

        handler_ids = []
        try:
            handler_ids.append(self._add_console_handler(log_format, target_logger=staged_logger))
            handler_ids.append(self._add_file_handler(log_format, target_logger=staged_logger))
        except Exception:
            for handler_id in list(handler_ids):
                try:
                    staged_logger.remove(handler_id)
                except Exception:
                    pass
            raise
        handlers = {
            handler_id: staged_core.handlers[handler_id]
            for handler_id in handler_ids
        }
        return staged_logger, handler_ids, handlers

    def _swap_managed_handlers(self, old_handler_ids, new_handler_ids, new_handlers):
        """Atomically replace only this instance's managed handlers."""
        core = self.logger._core
        with core.lock:
            old_handlers = {
                handler_id: core.handlers[handler_id]
                for handler_id in old_handler_ids
                if handler_id in core.handlers
            }
            handlers = {
                handler_id: handler
                for handler_id, handler in core.handlers.items()
                if handler_id not in old_handler_ids
            }
            handlers.update(new_handlers)
            core.handlers = handlers
            core.min_level = min(
                (handler.levelno for handler in handlers.values()),
                default=float("inf"),
            )
        return old_handlers

    def _retire_detached_handlers(
        self,
        handlers,
        wait: bool = False,
        strict: bool = False,
        deadline: Optional[float] = None,
    ) -> None:
        """Drain and stop handlers which are no longer in the active core."""
        first_error = None
        if wait:
            for handler in handlers.values():
                try:
                    timeout = self._remaining_timeout(deadline)
                    if timeout is None:
                        handler.complete_queue()
                    else:
                        handler.complete_queue(timeout=timeout)
                except Exception as exc:
                    if handler not in self._failed_handler_stops:
                        self._failed_handler_stops.append(handler)
                    if first_error is None:
                        first_error = exc
        for handler in handlers.values():
            stop_succeeded = False
            try:
                timeout = self._remaining_timeout(deadline)
                if timeout is None:
                    handler.stop()
                else:
                    handler.stop(timeout=timeout)
                stop_succeeded = True
            except Exception as exc:
                if handler not in self._failed_handler_stops:
                    self._failed_handler_stops.append(handler)
                if first_error is None:
                    first_error = exc
            self._collect_handler_metrics(handler)
            # A stop which succeeded after a failed drain no longer needs a
            # retry; only genuinely unfinished handlers remain pending.
            if stop_succeeded and handler in self._failed_handler_stops:
                self._failed_handler_stops.remove(handler)
        if strict and first_error is not None:
            raise RuntimeError("Failed to retire one or more logger handlers") from first_error
    
    def _validate_config(self) -> None:
        """验证配置参数"""
        if isinstance(self.max_size, bool) or not isinstance(self.max_size, int) or self.max_size <= 0:
            raise ValueError(self._msg('ERR_MAX_SIZE'))
        
        if not isinstance(self.retention, str) or not self.retention.strip():
            raise ValueError(self._msg('ERR_RETENTION'))
        
        if self.language not in ('zh', 'en'):
            raise ValueError(self._msg('ERR_LANGUAGE'))
        
        if self.compression is not None and self.compression not in ('zip', 'gz', 'tar'):
            raise ValueError(self._msg('ERR_COMPRESSION'))

        if self.queue_backend not in {"multiprocessing", "thread"}:
            raise ValueError(self._msg("ERR_QUEUE_BACKEND"))

        if self.queue_size is not None and (
            isinstance(self.queue_size, bool)
            or not isinstance(self.queue_size, int)
            or self.queue_size <= 0
        ):
            raise TypeError(self._msg("ERR_QUEUE_SIZE"))
        if self.overflow_policy not in {"block", "drop"}:
            raise ValueError(self._msg("ERR_OVERFLOW_POLICY"))
        if self.queue_timeout is not None and (
            isinstance(self.queue_timeout, bool)
            or not isinstance(self.queue_timeout, (int, float))
            or self.queue_timeout < 0
            or not math.isfinite(self.queue_timeout)
        ):
            raise ValueError(self._msg("ERR_QUEUE_TIMEOUT"))
        if self.shutdown_timeout is not None and (
            isinstance(self.shutdown_timeout, bool)
            or not isinstance(self.shutdown_timeout, (int, float))
            or self.shutdown_timeout <= 0
            or not math.isfinite(self.shutdown_timeout)
        ):
            raise ValueError(self._msg("ERR_SHUTDOWN_TIMEOUT"))

        if self.defer_format and (not self.enqueue or self.queue_backend != "thread"):
            raise ValueError(
                "defer_format requires enqueue=True and queue_backend='thread'"
            )

        # 校验级别名是否被 loguru 识别（自定义级别在此之前 add 的也会通过）
        for lvl_name, lvl_value in (
            ("console_level", self.console_level),
            ("file_level", self.file_level),
        ):
            if lvl_value is None:
                continue
            try:
                self.logger.level(str(lvl_value))
            except Exception:
                raise ValueError(self._msg('ERR_INVALID_LEVEL', name=lvl_name, value=repr(lvl_value)))

    
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
    
    def _dynamic_level_kwargs(self, static_level: Optional[str]) -> Dict[str, Any]:
        """Return a sink level, using 0 when no filtering is requested."""
        return {"level": 0 if static_level is None else static_level}

    def _archive_retention(self, active_path: str):
        """Build retention for this sink's rotated archives only.

        With compression enabled, only archives ending in ``.log.<format>``
        are eligible. Without compression, only rotated ``.log`` files are
        eligible. The active file and files of another sink are never removed.
        """
        from .yyds_loguru._string_parsers import parse_duration

        duration = parse_duration(self.retention)
        if duration is None:
            raise ValueError("Cannot parse retention from: '%s'" % self.retention)

        active_path = os.path.abspath(active_path)
        root, extension = os.path.splitext(active_path)
        archive_prefix = root + "."
        archive_suffix = extension
        if self.compression:
            archive_suffix += "." + self.compression.lstrip(".")
        expiry_seconds = duration.total_seconds()

        def retain_archives(logs) -> None:
            deadline = time.time() - expiry_seconds
            for log_path in logs:
                absolute_path = os.path.abspath(log_path)
                if (
                    absolute_path == active_path
                    or not absolute_path.startswith(archive_prefix)
                    or not absolute_path.endswith(archive_suffix)
                ):
                    continue
                try:
                    if os.path.getmtime(absolute_path) <= deadline:
                        os.remove(absolute_path)
                except FileNotFoundError:
                    continue

        return retain_archives

    def _add_console_handler(self, log_format: str, target_logger=None) -> int:
        """添加控制台处理器
        
        注意：控制台输出不使用 enqueue，避免额外创建 multiprocessing 队列和信号灯。
        stdout 写入足够快，不需要异步队列缓冲。
        """
        kwargs = self._dynamic_level_kwargs(self.console_level)
        target = self.logger if target_logger is None else target_logger
        handler_id = target.add(
            sys.stdout,
            format=log_format,
            enqueue=False,
            diagnose=self.diagnose,
            backtrace=self.backtrace,
            serialize=self.console_serialize,
            **kwargs,
            queue_size=self.queue_size,
            overflow_policy=self.overflow_policy,
            queue_timeout=self.queue_timeout,
            queue_backend=self.queue_backend,
            shutdown_timeout=self.shutdown_timeout,
        )
        return handler_id
    
    def _add_file_handler(self, log_format: str, target_logger=None) -> int:
        """添加主日志文件处理器。"""
        kwargs = self._dynamic_level_kwargs(self.file_level)
        main_log_path = os.path.join(self.log_dir, f"{self._process_file_name}.log")
        target = self.logger if target_logger is None else target_logger
        handler_id = target.add(
            main_log_path,
            format=log_format,
            rotation=f"{self.max_size} MB",
            retention=self._archive_retention(main_log_path),
            compression=self.compression,
            encoding='utf-8',
            enqueue=self.enqueue,
            diagnose=self.diagnose,
            backtrace=self.backtrace,
            serialize=self.serialize,
            defer_format=self.defer_format,
            **kwargs,
            queue_size=self.queue_size,
            overflow_policy=self.overflow_policy,
            queue_timeout=self.queue_timeout,
            queue_backend=self.queue_backend,
            shutdown_timeout=self.shutdown_timeout,
        )
        return handler_id
    
    def _fallback_configuration(self) -> None:
        """配置失败时的后备方案"""
        self._remove_handlers()
        handler_id = self.logger.add(
            sys.stderr,
            format="<red>{time:YYYY-MM-DD HH:mm:ss}</red> | <level>{level: <8}</level> | <level>{message}</level>",
            level="ERROR"
        )
        self._handler_ids.append(handler_id)

    def setup_exception_handler(self) -> None:
        """
        设置统一的异常处理函数，将未处理的异常记录到日志。
        """
        self._ensure_open()
        from .lifecycle import setup_exception_handler
        return setup_exception_handler(self)

    def _restore_exception_handler(self) -> None:
        from .lifecycle import restore_exception_handler
        return restore_exception_handler(self)

    def get_queue_dropped(self) -> int:
        """Return records dropped by queue overflow or serialization failure."""
        current = sum(int(getattr(handler, "dropped_messages", 0)) for handler in self._queue_handlers())
        return self._dropped_messages_total + current

    def get_queue_serialization_errors(self) -> int:
        """Return multiprocessing records rejected before queue insertion."""
        current = sum(int(getattr(handler, "serialization_errors", 0)) for handler in self._queue_handlers())
        return self._serialization_errors_total + current

    def _queue_handlers(self):
        return [
            handler
            for handler in getattr(self.logger._core, "handlers", {}).values()
            if getattr(handler, "_enqueue", False)
        ]

    def _queue_drop_reasons(self) -> Dict[str, int]:
        reasons = dict(self._drop_reasons_total)
        for handler in self._queue_handlers():
            for reason, count in getattr(handler, "drop_reasons", {}).items():
                reasons[reason] = reasons.get(reason, 0) + int(count)
        return reasons

    def get_queue_status(self) -> Dict[str, Any]:
        """Return the configured queue policy and the cumulative drop count."""
        handlers = self._queue_handlers()
        depths = [getattr(handler, "queue_depth", None) for handler in handlers]
        capacities = [getattr(handler, "queue_capacity", None) for handler in handlers]
        known_depths = [depth for depth in depths if depth is not None]
        total_depth = None if len(known_depths) != len(depths) else sum(known_depths)
        finite_capacities = [capacity for capacity in capacities if capacity is not None]
        total_capacity = None if len(finite_capacities) != len(capacities) else sum(finite_capacities)
        utilization = (
            (total_depth / total_capacity)
            if total_depth is not None and total_capacity
            else None
        )
        backends = sorted({str(getattr(handler, "_queue_backend", self.queue_backend)) for handler in handlers})
        return {
            "enabled": bool(handlers),
            "size": self.queue_size,
            "capacity": total_capacity,
            "depth": total_depth,
            "utilization": utilization,
            "overflow_policy": self.overflow_policy,
            "timeout": self.queue_timeout,
            "backend": backends[0] if len(backends) == 1 else ("mixed" if backends else self.queue_backend),
            "defer_format": bool(self.defer_format),
            "shutdown_timeout": self.shutdown_timeout,
            "dropped_messages": self.get_queue_dropped(),
            "serialization_errors": self.get_queue_serialization_errors(),
            "drop_reasons": self._queue_drop_reasons(),
            "writer_alive": bool(handlers) and all(
                bool(getattr(handler, "writer_alive", False)) for handler in handlers
            ),
            "sink_errors": sum(int(getattr(handler, "sink_errors", 0)) for handler in handlers),
            "handlers": [
                {
                    "id": getattr(handler, "_id", None),
                    "backend": getattr(handler, "_queue_backend", self.queue_backend),
                    "defer_format": bool(getattr(handler, "_defer_format", False)),
                    "depth": getattr(handler, "queue_depth", None),
                    "capacity": getattr(handler, "queue_capacity", None),
                    "writer_alive": bool(getattr(handler, "writer_alive", False)),
                    "dropped_messages": int(getattr(handler, "dropped_messages", 0)),
                    "serialization_errors": int(getattr(handler, "serialization_errors", 0)),
                    "drop_reasons": getattr(handler, "drop_reasons", {}),
                    "last_error": (
                        repr(getattr(handler, "last_error", None))
                        if getattr(handler, "last_error", None) is not None
                        else None
                    ),
                }
                for handler in handlers
            ],
        }

    def get_health(self) -> Dict[str, Any]:
        """Return local log-storage health together with this logger's state."""
        from .health import LogHealthChecker

        result = LogHealthChecker(language=self.language).check_health(self.log_dir)
        queue_status = self.get_queue_status()
        result["logger"] = {
            "state": self._cleanup_state,
            "enqueue": bool(self.enqueue),
            "queue_backend": self.queue_backend,
            "defer_format": bool(self.defer_format),
            "queue_dropped": self.get_queue_dropped(),
            "queue_serialization_errors": self.get_queue_serialization_errors(),
            "handler_count": len(getattr(self.logger._core, "handlers", {})),
            "queue_depth": queue_status["depth"],
            "queue_capacity": queue_status["capacity"],
            "queue_utilization": queue_status["utilization"],
            "queue_writer_alive": queue_status["writer_alive"],
            "queue_drop_reasons": queue_status["drop_reasons"],
            "queue_sink_errors": queue_status["sink_errors"],
            "pending_handler_stops": len(self._failed_handler_stops),
        }
        return result

    def capture_std_logging(self, level: str = "DEBUG",
                            names: Optional[List[str]] = None,
                            clear_existing: bool = False) -> None:
        """接管标准库 logging，把三方库（uvicorn/sqlalchemy/requests 等）日志统一汇入本管道。

        Args:
            level: 拦截的最低级别。
            names: 仅接管指定 logger 名称列表；None 表示接管 root（全局）。
            clear_existing: 是否清空目标 logger 既有的 handler。
        """
        self._ensure_open()
        from .stdlib_bridge import capture_std_logging
        return capture_std_logging(self, level=level, names=names,
                                   clear_existing=clear_existing)

    def _restore_std_logging(self) -> None:
        """恢复被 capture_std_logging 修改的标准库 logging 状态"""
        from .stdlib_bridge import restore_std_logging
        return restore_std_logging(self)

    def setup_signal_handlers(self) -> None:
        """注册 SIGTERM/SIGINT，退出前调用 cleanup 排空 enqueue 队列，避免容器停服丢日志。"""
        self._ensure_open()
        from .lifecycle import setup_signal_handlers
        return setup_signal_handlers(self)

    def _restore_signal_handlers(self) -> None:
        """恢复被 setup_signal_handlers 替换的信号处理函数"""
        from .lifecycle import restore_signal_handlers
        return restore_signal_handlers(self)

    def bind(self, **kwargs: Any) -> Any:
        """绑定结构化上下文字段（如 trace_id/span_id/user_id），返回带上下文的 logger。

        配合 serialize=True 时，这些字段会自动出现在 JSON 日志中。
        """
        self._ensure_open()
        return self.logger.bind(**kwargs)

    def contextualize(self, **kwargs: Any) -> Any:
        """在 with 作用域内临时注入结构化上下文字段（线程/协程安全）。

        示例：
            with logger.contextualize(trace_id="abc", span_id="01"):
                logger.info("处理中")
        """
        self._ensure_open()
        return self.logger.contextualize(**kwargs)

    def set_request_id(self, request_id: str) -> Any:
        """设置当前上下文的 request_id，返回 token（可用于 reset）"""
        return self.request_id_var.set(request_id or "-")

    def get_request_id(self) -> str:
        """获取当前上下文的 request_id"""
        return self.request_id_var.get()

    def __getattr__(self, name: str) -> Any:
        """
        使 YydsLogger 支持直接调用 Loguru 的日志级别方法。

        Args:
            name (str): 属性名称。
        """
        # 防止初始化异常时 _logger_d1 尚未创建导致无限递归
        if name.startswith('_'):
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")
        self._ensure_open()
        try:
            return getattr(self._logger_d1, name)
        except AttributeError:
            raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def log(self, level: str, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.log(level, message, *args, **kwargs)

    def debug(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.debug(message, *args, **kwargs)

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.info(message, *args, **kwargs)

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.warning(message, *args, **kwargs)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.error(message, *args, **kwargs)

    def critical(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.critical(message, *args, **kwargs)

    def exception(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.exception(message, *args, **kwargs)

    def success(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.success(message, *args, **kwargs)

    def trace(self, message: str, *args: Any, **kwargs: Any) -> None:
        self._ensure_open()
        return self._logger_d1.trace(message, *args, **kwargs)

    def log_decorator(self, msg: Any = None, level: str = "ERROR", trace: bool = True,
                      reraise: bool = True) -> Any:
        from .decorators import log_decorator
        return log_decorator(self, msg=msg, level=level, trace=trace, reraise=reraise)

    def time_it(self, func: Any = None, *, line_by_line: bool = False) -> Any:
        from .decorators import time_it
        return time_it(self, func=func, line_by_line=line_by_line)


    def _run_sync_line_profiler(self, func, *args, **kwargs):
        from .profiling import run_sync
        return run_sync(self, func, *args, **kwargs)

    async def _run_async_line_profiler(self, func, *args, **kwargs):
        from .profiling import run_async
        return await run_async(self, func, *args, **kwargs)

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
            error_msg = self._msg(msg_key) if msg_key in LANG_MAP[self.language] else self._msg('OCCURRENCE_EXCEPTION', error=msg_key)
            
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
            
    def _update_stats(self, level: str) -> None:
        """更新轻量级日志计数。"""
        if not self.enable_stats:
            return

        level_upper = level.upper()
        # 级别门控：不会被任何 sink 输出的日志不计入统计
        if not self._emits(level_upper):
            return

        with self._stats_lock:
            self._stats['total'] += 1
            if level_upper in {'ERROR', 'CRITICAL'}:
                self._stats['error'] += 1
            elif level_upper == 'WARNING':
                self._stats['warning'] += 1
            elif level_upper in {'INFO', 'SUCCESS'}:
                self._stats['info'] += 1
            elif level_upper in {'DEBUG', 'TRACE'}:
                self._stats['debug'] += 1

    def get_stats(self) -> Dict[str, Any]:
        """获取轻量级日志统计信息。"""
        current_time = datetime.now()
        
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
                'duration_seconds': max(0.0, (current_time - self._stats_start_time).total_seconds()),
                'error_rate': float(error_rate),
            }
            return stats
    
    def reset_stats(self) -> None:
        """重置统计信息"""
        with self._stats_lock:
            self._stats = {
                'total': 0,
                'error': 0,
                'warning': 0,
                'info': 0,
                'debug': 0,
            }
            self._stats_start_time = datetime.now()

    def __enter__(self):
        """支持 with 语句，自动管理资源生命周期"""
        self._ensure_open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 语句时自动清理资源"""
        self.cleanup()
        return False

    async def __aenter__(self):
        """Support ``async with`` for non-blocking shutdown."""
        self._ensure_open()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()
        return False

    def _has_enqueued_handlers(self) -> bool:
        return any(
            bool(getattr(handler, "_enqueue", False))
            for handler in getattr(self.logger._core, "handlers", {}).values()
        )

    async def _run_in_worker(self, function, *args, deadline=None):
        """Run bounded blocking shutdown work without blocking the event loop."""
        done = threading.Event()
        outcome = {}

        def run():
            try:
                outcome["result"] = function(*args)
            except BaseException as exc:
                outcome["error"] = exc
            finally:
                done.set()

        worker = threading.Thread(
            target=run,
            daemon=True,
            name="yyds-logger-shutdown",
        )
        worker.start()
        while not done.is_set():
            remaining = self._remaining_timeout(deadline)
            if remaining is not None and remaining <= 0:
                raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED"))
            await asyncio.sleep(0.01 if remaining is None else min(0.01, remaining))
        worker.join(timeout=0)
        if "error" in outcome:
            raise outcome["error"]
        return outcome.get("result")

    async def _flush_async_internal(self, deadline: Optional[float]) -> None:
        remaining = self._remaining_timeout(deadline)
        completer = await self._run_in_worker(
            self.logger.complete,
            remaining,
            deadline=deadline,
        )
        remaining = self._remaining_timeout(deadline)
        if remaining is None:
            await completer
            return
        if remaining <= 0:
            raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED"))
        try:
            await asyncio.wait_for(completer, timeout=remaining)
        except asyncio.TimeoutError as exc:
            raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED")) from exc

    def flush(self) -> None:
        """排空当前实例的 enqueue 队列，但保留 logger 继续使用。"""
        self._ensure_open()
        if not self._has_enqueued_handlers():
            return
        self._run_complete(deadline=self._shutdown_deadline())

    async def flush_async(self) -> None:
        """Without blocking the event loop, drain queues and async sinks."""
        self._ensure_open()
        await self._flush_async_internal(self._shutdown_deadline())

    def close(self) -> None:
        """关闭 logger 并释放资源；cleanup() 的明确别名。"""
        self.cleanup()

    async def aclose(self) -> None:
        """Close the logger without blocking the running event loop."""
        action = self._claim_cleanup()
        if action == "closed":
            return
        if action == "wait":
            await self._wait_for_cleanup_async(self._shutdown_deadline())
            return

        deadline = self._shutdown_deadline()
        self._active_shutdown_deadline = deadline
        try:
            self._prepare_cleanup()
            await self._run_in_worker(
                self._retry_failed_handler_stops,
                True,
                deadline=deadline,
            )
            await self._flush_async_internal(deadline)
            await self._run_in_worker(
                self._remove_handlers,
                False,
                True,
                deadline=deadline,
            )
            await self._run_in_worker(
                self._remove_remaining_handlers,
                True,
                deadline=deadline,
            )
        except BaseException as exc:
            self._mark_cleanup_failed(exc)
            raise
        self._finish_cleanup()

    def cleanup(self) -> None:
        """清理资源，释放 enqueue 队列和信号灯。

        此方法是幂等的，多次调用安全（atexit + 手动调用不会冲突）。
        """
        self._cleanup()

    def _cleanup(self) -> None:
        action = self._claim_cleanup()
        if action == "closed":
            return
        if action == "wait":
            if self._cleanup_owner_thread_id == threading.get_ident():
                raise RuntimeError(self._msg("ERR_CLEANUP_REENTRANT"))
            self._wait_for_cleanup_sync(self._shutdown_deadline())
            return

        deadline = self._shutdown_deadline()
        self._active_shutdown_deadline = deadline
        try:
            self._prepare_cleanup()
            self._retry_failed_handler_stops(strict=True)
            # wait=True drains managed queues before retiring their sinks.
            self._remove_handlers(wait=True, strict=True)
            self._remove_remaining_handlers(strict=True)
        except BaseException as exc:
            self._mark_cleanup_failed(exc)
            raise
        self._finish_cleanup()

    def _claim_cleanup(self) -> str:
        with self._config_lock:
            state = getattr(self, "_cleanup_state", "open")
            if state == "closed":
                return "closed"
            if state == "closing":
                return "wait"
            self._cleanup_state = "closing"
            self._cleanup_owner_thread_id = threading.get_ident()
            self._cleanup_error = None
            self._cleanup_event.clear()
            return "owner"

    def _prepare_cleanup(self) -> None:
        self._restore_exception_handler()
        self._release_global_resource("exception_hooks")

        self._restore_std_logging()
        self._release_global_resource("stdlib_logging")

        self._restore_signal_handlers()
        self._release_global_resource("signal_handlers")

    def _wait_for_cleanup_sync(self, deadline: Optional[float]) -> None:
        remaining = self._remaining_timeout(deadline)
        if not self._cleanup_event.wait(timeout=remaining):
            raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED"))
        self._raise_waited_cleanup_error()

    async def _wait_for_cleanup_async(self, deadline: Optional[float]) -> None:
        while not self._cleanup_event.is_set():
            remaining = self._remaining_timeout(deadline)
            if remaining is not None and remaining <= 0:
                raise TimeoutError(self._msg("ERR_SHUTDOWN_TIMEOUT_REACHED"))
            await asyncio.sleep(0.01 if remaining is None else min(0.01, remaining))
        self._raise_waited_cleanup_error()

    def _raise_waited_cleanup_error(self) -> None:
        with self._config_lock:
            if self._cleanup_state == "closed":
                return
            error = self._cleanup_error
        raise RuntimeError(self._msg("ERR_CLEANUP_FAILED")) from error

    def _mark_cleanup_failed(self, error: BaseException) -> None:
        with self._config_lock:
            self._cleanup_error = error
            self._cleanup_owner_thread_id = None
            self._cleanup_state = "failed"
            self._cleanup_event.set()

    def _finish_cleanup(self) -> None:
        # Only unregister after every retryable cleanup phase has succeeded.
        # If a manual cleanup fails, atexit still gets a final chance.
        try:
            atexit.unregister(self.cleanup)
        except Exception:
            pass
        with self._config_lock:
            self._cleaned_up = True
            self._cleanup_error = None
            self._cleanup_owner_thread_id = None
            self._active_shutdown_deadline = None
            self._cleanup_state = "closed"
            self._cleanup_event.set()
        try:
            logging.getLogger(__name__).info(self._msg('CLEANUP_COMPLETED'))
        except Exception:
            pass
