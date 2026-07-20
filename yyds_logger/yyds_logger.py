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
import warnings

from typing import Optional, Dict, Any, List

from contextvars import ContextVar
from datetime import datetime
import threading
import weakref

from .yyds_loguru import create_logger
from .i18n import LANG_MAP, get_message


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
    - 支持按时间轮转日志
    - 支持自定义日志格式
    - 支持日志级别过滤
    - 支持自定义压缩格式
    - 支持自定义文件命名模式
    """

    # 保留类级别别名，兼容依赖该内部属性的旧代码；词典定义位于 i18n.py。
    _LANG_MAP = LANG_MAP

    _global_resource_lock = threading.RLock()
    _global_resource_owners = {}


    def __init__(
        self,
        file_name: str,                         # 日志文件基准名（用于区分同目录下不同日志器的日志文件以实现安全隔离操作）
        log_dir: str = 'logs',                 # 日志保存目录
        max_size: int = 14,                    # 单个文件最大大小（单位：MB）
        retention: str = '7 days',             # 日志保留策略
        work_type: Optional[bool] = None,      # 已弃用；请使用 env
        language: str = 'zh',                  # 语言选项，默认为中文
        rotation_time: Optional[str] = None,   # 新增：按时间轮转，如 "1 day", "1 week"
        custom_format: Optional[str] = None,   # 新增：自定义日志格式
        filter_level: str = "DEBUG",           # 新增：日志过滤级别
        compression: str = "gz",               # 压缩格式，默认 gzip；支持 zip, gz, tar
        enable_stats: bool = False,            # 新增：是否启用日志统计
        enable_exception_hook: bool = False,
        env: Optional[str] = 'prod',             # 新增：环境，'dev'/'prod'（优先于 work_type）
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
        read_env: bool = False,                # 新增：从环境变量读取配置覆盖
        error_file: bool = False,              # 是否单独输出错误日志文件
        queue_size: Optional[int] = 10000,     # 本地 enqueue 队列容量；None 表示无界队列
        overflow_policy: str = "block",       # block 或 drop
        queue_timeout: Optional[float] = None, # block 策略的最大等待时间
        process_isolation: bool = False,       # 多进程时将文件名隔离到 PID
    ) -> None:
        """
        初始化日志记录器。

        Args:
            file_name (str): 日志文件名称(主日志文件前缀)。
            log_dir (str): 日志文件目录。
            max_size (int): 日志文件大小(MB)超过时进行轮转。
            retention (str): 日志保留策略。
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
        self.rotation_time = rotation_time
        self.custom_format = custom_format
        self.filter_level = filter_level
        self.serialize = bool(serialize)
        self.console_serialize = bool(console_serialize)
        self.console_level = console_level
        self.file_level = file_level
        self.error_level = error_level or "ERROR"
        self._error_file = bool(error_file)
        if queue_size is not None:
            if isinstance(queue_size, bool) or not isinstance(queue_size, int) or queue_size <= 0:
                raise TypeError(get_message(language, "ERR_QUEUE_SIZE"))
        self.queue_size = queue_size
        self.overflow_policy = str(overflow_policy).lower()
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
        self.process_isolation = bool(process_isolation)
        self._process_file_name = (
            f"{file_name}.pid{os.getpid()}"
            if self.process_isolation else file_name
        )
        self.compression = compression
        # 级别号缓存与统计相关阈值
        self._level_no_cache: Dict[str, int] = {}
        self._error_level_no = 10 ** 9   # 由 configure_logger 精确计算
        self.enable_stats = enable_stats
        self._stats_ready = False
        self._handler_ids: List[int] = []
        self._dropped_messages_total = 0
        self._removed_default_handler = False
        self._exception_hook_enabled = bool(enable_exception_hook)
        self._exception_hook = None
        self._prev_excepthook = None
        self._capture_std_logging = bool(capture_std_logging)
        self._install_signal_handlers = bool(install_signal_handlers)
        self._std_logging_state = None
        self._prev_signal_handlers = {}
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

        # 解析运行环境：env 优先于 work_type；显式 enqueue/diagnose/backtrace 再覆盖。
        if work_type is not None:
            warnings.warn(
                get_message(language, "WARN_WORK_TYPE_DEPRECATED"),
                DeprecationWarning,
                stacklevel=2,
            )
            # 兼容显式传入旧参数的调用；显式 env 仍以 env 为准。
            if env == "prod":
                env = "prod" if work_type else "dev"

        if env is not None:
            self.env = str(env).strip().lower()
            if self.env in {"development", "debug", "test"}:
                self.env = "dev"
            elif self.env in {"production", "release"}:
                self.env = "prod"
            elif self.env not in {"dev", "prod"}:
                raise ValueError(get_message(language, "ERR_ENV"))
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

        # 级别号缓存：用于热路径门控。
        self._info_level_no = self._safe_level_no("INFO")
        self._min_level_no = self._safe_level_no(self.filter_level)
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
        self._cleaned_up = False


        # 注册 atexit 钩子，确保程序退出时自动清理 enqueue 队列和信号灯
        atexit.register(self.cleanup)

        # 可选：接管标准库 logging，把三方库日志统一汇入本管道
        if self._capture_std_logging:
            try:
                self.capture_std_logging()
            except Exception:
                self.cleanup()
                raise

        # 可选：注册信号处理，容器/k8s SIGTERM 退出前排空队列避免丢日志
        if self._install_signal_handlers:
            try:
                self._setup_signal_handlers()
            except Exception:
                self.cleanup()
                raise

        self._instance_counted = False

    @staticmethod
    def _env_bool(name: str, default: bool) -> bool:
        """从环境变量解析布尔值"""
        raw = os.getenv(name)
        if raw is None:
            return bool(default)
        return raw.strip().lower() in ('1', 'true', 'yes', 'on', 'y')

    def _patch_record(self, record: Dict[str, Any]) -> None:
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

    def is_level_enabled(self, level: str) -> bool:
        """判断指定级别当前是否会被输出（用于业务侧的昂贵日志构造前置判断）"""
        return self._safe_level_no(level) >= self._min_level_no

    def _msg(self, key: str, **kwargs) -> str:
        """消息格式化处理，优化性能

        对无参消息缓存模板文本；对有参消息直接格式化（参数通常每次不同，
        缓存命中率极低，省去昂贵的 key 序列化开销）。
        """
        try:
            messages = self._LANG_MAP.get(self.language) or self._LANG_MAP["zh"]
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
            messages = self._LANG_MAP.get(self.language) or self._LANG_MAP["zh"]
            text = messages.get(key, key)
            err_tpl = messages.get('FORMAT_ERR_MISSING_PARAM', " (格式化错误: 缺少参数 {error})")
            return f"{text}{err_tpl.format(error=str(e))}"
        except Exception as e:
            messages = self._LANG_MAP.get(self.language) or self._LANG_MAP["zh"]
            text = messages.get(key, key)
            err_tpl = messages.get('FORMAT_ERR_GENERIC', " (格式化错误: {error})")
            return f"{text}{err_tpl.format(error=str(e))}"

    def _remove_handlers(self, wait: bool = False) -> None:
        handler_ids = list(self._handler_ids)
        self._handler_ids.clear()
        self._dropped_messages_total += sum(
            int(getattr(handler, "dropped_messages", 0))
            for handler in getattr(self.logger._core, "handlers", {}).values()
            if getattr(handler, "_id", None) in handler_ids
        )

        # 仅当本实例存在 enqueue 文件 handler 时才需要排空与 gc：
        # 控制台始终非 enqueue；enqueue=False 时没有 multiprocessing 队列/信号灯，
        # 无需 complete()/gc，也避免无意义地等待全局所有 handler。
        need_drain = bool(wait and handler_ids and getattr(self, 'enqueue', True))

        if need_drain:
            # 在 remove 之前调用 complete()，等待所有 enqueue 队列排空
            try:
                self._run_complete()
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

    def _run_complete(self) -> None:
        """在同步上下文中正确驱动 Loguru 的 awaitable complete。"""
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            async def wait_complete():
                await self.logger.complete()

            asyncio.run(wait_complete())
            return
        raise RuntimeError(self._msg("ERR_RUNNING_LOOP"))

    def configure_logger(self) -> None:
        """配置日志记录器，添加错误处理和安全性检查"""
        with self._config_lock:
            self._configure_logger()

    def _configure_logger(self) -> None:
        # 先做只读校验与目录检查：“非法配置在改动全局状态前快速失败”逻辑现已内聚在独立实例中。
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

            # 重新计算最小级别号（配置可能在重新配置前被修改）
            self._min_level_no = self._safe_level_no(self.filter_level)
            # 错误文件级别号：用于统计门控判断"是否会被错误文件捕获"；关闭错误文件时置为极大值
            self._error_level_no = self._level_no(self.error_level) if self._error_file else 10 ** 9
            
            # 配置日志格式
            log_format = self._get_log_format()
            
            # 添加控制台处理器
            self._add_console_handler(log_format)
            
            # 添加文件处理器
            self._add_file_handlers(log_format)
            
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
        if isinstance(self.max_size, bool) or not isinstance(self.max_size, int) or self.max_size <= 0:
            raise ValueError(self._msg('ERR_MAX_SIZE'))
        
        if not isinstance(self.retention, str) or not self.retention.strip():
            raise ValueError(self._msg('ERR_RETENTION'))
        
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
        """返回 sink 的静态级别配置。"""
        return {"level": static_level}

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
            queue_size=self.queue_size,
            overflow_policy=self.overflow_policy,
            queue_timeout=self.queue_timeout,
        )
        self._handler_ids.append(handler_id)
    
    def _add_file_handlers(self, log_format: str) -> None:
        """添加文件处理器"""
        # 主日志文件
        kwargs = self._dynamic_level_kwargs(self.file_level or self.filter_level)
        handler_id = self.logger.add(
            os.path.join(self.log_dir, f"{self._process_file_name}.log"),
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
            queue_size=self.queue_size,
            overflow_policy=self.overflow_policy,
            queue_timeout=self.queue_timeout,
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
                queue_size=self.queue_size,
                overflow_policy=self.overflow_policy,
                queue_timeout=self.queue_timeout,
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

    def setup_exception_handler(self):
        """
        设置统一的异常处理函数，将未处理的异常记录到日志。
        """
        from .lifecycle import setup_exception_handler
        return setup_exception_handler(self)

    def _get_level_log_path(self, level_name):
        """
        获取不同级别日志文件的路径。
        """
        return os.path.join(self.log_dir, f"{self._process_file_name}_{level_name}.log")

    def get_queue_dropped(self) -> int:
        """返回本实例本地 enqueue sink 因队列满而丢弃的日志数量。"""
        current = sum(
            int(getattr(handler, "dropped_messages", 0))
            for handler in getattr(self.logger._core, "handlers", {}).values()
            if getattr(handler, "_id", None) in self._handler_ids
        )
        return self._dropped_messages_total + current

    def capture_std_logging(self, level: str = "DEBUG",
                            names: Optional[List[str]] = None,
                            clear_existing: bool = True) -> None:
        """接管标准库 logging，把三方库（uvicorn/sqlalchemy/requests 等）日志统一汇入本管道。

        Args:
            level: 拦截的最低级别。
            names: 仅接管指定 logger 名称列表；None 表示接管 root（全局）。
            clear_existing: 是否清空目标 logger 既有的 handler。
        """
        from .stdlib_bridge import capture_std_logging
        return capture_std_logging(self, level=level, names=names,
                                   clear_existing=clear_existing)

    def _restore_std_logging(self) -> None:
        """恢复被 capture_std_logging 修改的标准库 logging 状态"""
        from .stdlib_bridge import restore_std_logging
        return restore_std_logging(self)

    def _setup_signal_handlers(self) -> None:
        """注册 SIGTERM/SIGINT，退出前调用 cleanup 排空 enqueue 队列，避免容器停服丢日志。"""
        from .lifecycle import setup_signal_handlers
        return setup_signal_handlers(self)

    def _restore_signal_handlers(self) -> None:
        """恢复被 _setup_signal_handlers 替换的信号处理函数"""
        from .lifecycle import restore_signal_handlers
        return restore_signal_handlers(self)

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
        return self.request_id_var.set(request_id or "-")

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
        return self._logger_d1.log(level, message, *args, **kwargs)

    def debug(self, message: str, *args, **kwargs):
        return self._logger_d1.debug(message, *args, **kwargs)

    def info(self, message: str, *args, **kwargs):
        return self._logger_d1.info(message, *args, **kwargs)

    def warning(self, message: str, *args, **kwargs):
        return self._logger_d1.warning(message, *args, **kwargs)

    def error(self, message: str, *args, **kwargs):
        return self._logger_d1.error(message, *args, **kwargs)

    def critical(self, message: str, *args, **kwargs):
        return self._logger_d1.critical(message, *args, **kwargs)

    def exception(self, message: str, *args, **kwargs):
        return self._logger_d1.exception(message, *args, **kwargs)

    def log_decorator(self, msg=None, level="ERROR", trace=True):
        from .decorators import log_decorator
        return log_decorator(self, msg=msg, level=level, trace=trace)

    def time_it(self, func=None, *, line_by_line=False):
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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """退出 with 语句时自动清理资源"""
        self.cleanup()
        return False

    def flush(self) -> None:
        """排空当前实例的 enqueue 队列，但保留 logger 继续使用。"""
        if not self.enqueue or not self._handler_ids:
            return
        try:
            self._run_complete()
        except RuntimeError:
            raise RuntimeError(self._msg("ERR_RUNNING_LOOP"))

    async def flush_async(self) -> None:
        """异步排空当前实例的 enqueue 队列，但保留 logger 继续使用。"""
        if not self.enqueue or not self._handler_ids:
            return
        await self.logger.complete()

    def close(self) -> None:
        """关闭 logger 并释放资源；cleanup() 的明确别名。"""
        self.cleanup()

    def cleanup(self) -> None:
        """清理资源，释放 enqueue 队列和信号灯。

        此方法是幂等的，多次调用安全（atexit + 手动调用不会冲突）。
        """
        with self._config_lock:
            self._cleanup()

    def _cleanup(self) -> None:
        if getattr(self, '_cleaned_up', False):
            return
        self._cleaned_up = True

        # 取消 atexit 注册，避免重复调用
        try:
            atexit.unregister(self.cleanup)
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
        self._release_global_resource("stdlib_logging")
        self._release_global_resource("signal_handlers")
        self._release_global_resource("exception_hooks")
        # wait=True: 等待 enqueue 队列排空，确保信号灯正确释放
        self._remove_handlers(wait=True)
        _logger = logging.getLogger(__name__)
        _logger.info(self._msg('CLEANUP_COMPLETED'))
