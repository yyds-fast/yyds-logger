"""
YydsLogger 高级功能模块（实用版）

该模块聚焦可直接集成到业务中的能力：
脱敏与可选加密、安全备份恢复、日志压缩归档、SQLite 结构化存储、日志处理管道、健康检查与轻量性能指标。
"""

from __future__ import annotations

import json
import logging
import os
import queue
import re
import shutil
import sqlite3
import tarfile
import threading
import time
import zipfile
from collections import defaultdict, deque
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple, Union

try:
    import psutil
except Exception:
    psutil = None

_logger = logging.getLogger(__name__)


class LogFilter(Enum):
    NONE = "none"
    REGEX = "regex"
    KEYWORD = "keyword"
    CUSTOM = "custom"


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _ensure_dir(path: Union[str, Path]) -> str:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _is_within_directory(base_dir: Path, target_path: Path) -> bool:
    try:
        base = base_dir.resolve()
        target = target_path.resolve()
        return str(target).startswith(str(base) + os.sep) or target == base
    except Exception:
        return False


class LogSecurity:
    def __init__(
        self,
        sensitive_keys: Optional[Sequence[str]] = None,
        replacement: str = "***",
        enable_encryption: bool = False,
        encryption_key: Optional[Union[str, bytes]] = None,
    ):
        self.replacement = replacement
        self.sensitive_keys = {k.lower() for k in (sensitive_keys or self._default_sensitive_keys())}
        self._patterns = self._compile_patterns()

        self._cipher = None
        self._encryption_key: Optional[bytes] = None
        if enable_encryption or encryption_key is not None:
            self._init_cipher(encryption_key)

    @staticmethod
    def _default_sensitive_keys() -> List[str]:
        return [
            "password",
            "passwd",
            "pwd",
            "密码",
            "口令",
            "secret",
            "token",
            "api_key",
            "apikey",
            "密钥",
            "access_token",
            "refresh_token",
            "private_key",
        ]

    def _compile_patterns(self) -> List[re.Pattern[str]]:
        keys = sorted({re.escape(k) for k in self.sensitive_keys}, key=len, reverse=True)
        if not keys:
            return []
        key_alt = "|".join(keys)
        return [
            re.compile(
                rf'(?i)((?:{key_alt}))(\s*[:=：]\s*)(["\']?)([^"\',\s\}}\]\n\r]+)(\3)',
                re.IGNORECASE,
            ),
            re.compile(
                rf'(?i)("?(?:{key_alt})"?)\s*:\s*(["\'])(.*?)\2',
                re.IGNORECASE,
            ),
        ]

    def sanitize_message(self, message: str) -> str:
        sanitized = message

        for pattern in self._patterns:
            def _repl(m: re.Match[str]) -> str:
                if len(m.groups()) >= 5:
                    key = m.group(1)
                    sep = m.group(2)
                    quote = m.group(3) or ""
                    end_quote = m.group(5) or quote
                    return f"{key}{sep}{quote}{self.replacement}{end_quote}"
                key = m.group(1)
                quote = m.group(2)
                return f"{key}: {quote}{self.replacement}{quote}"

            sanitized = pattern.sub(_repl, sanitized)

        return sanitized

    def sanitize_mapping(self, data: Any, _depth: int = 0, max_depth: int = 20) -> Any:
        if _depth >= max_depth:
            return self.replacement
        if isinstance(data, Mapping):
            out: Dict[str, Any] = {}
            for k, v in data.items():
                key_str = str(k)
                if key_str.lower() in self.sensitive_keys:
                    out[key_str] = self.replacement
                else:
                    out[key_str] = self.sanitize_mapping(v, _depth + 1, max_depth)
            return out
        if isinstance(data, list):
            return [self.sanitize_mapping(x, _depth + 1, max_depth) for x in data]
        if isinstance(data, tuple):
            return tuple(self.sanitize_mapping(x, _depth + 1, max_depth) for x in data)
        return data

    def _init_cipher(self, encryption_key: Optional[Union[str, bytes]]) -> None:
        try:
            from cryptography.fernet import Fernet
        except Exception as e:
            raise RuntimeError("cryptography 未安装，无法启用加密功能") from e

        if encryption_key is None:
            self._encryption_key = Fernet.generate_key()
        elif isinstance(encryption_key, bytes):
            self._encryption_key = encryption_key
        else:
            self._encryption_key = encryption_key.encode("utf-8")

        self._cipher = Fernet(self._encryption_key)

    def get_encryption_key(self) -> Optional[bytes]:
        return self._encryption_key

    def encrypt_bytes(self, data: bytes) -> bytes:
        if self._cipher is None:
            raise RuntimeError("加密未启用")
        return self._cipher.encrypt(data)

    def decrypt_bytes(self, data: bytes) -> bytes:
        if self._cipher is None:
            raise RuntimeError("加密未启用")
        return self._cipher.decrypt(data)


class DistributedLogger:
    def __init__(
        self,
        node_id: str,
        sequence_dir: Optional[str] = None,
        persist_every: int = 100,
    ):
        self.node_id = node_id
        self.persist_every = max(1, int(persist_every))
        self._lock = threading.Lock()
        self._sequence_number = 0

        base_dir = Path(sequence_dir) if sequence_dir else Path(os.getenv("YYDS_LOGGER_SEQ_DIR", ""))
        if not str(base_dir):
            base_dir = Path(os.path.expanduser("~")) / ".yyds_logger"
        _ensure_dir(base_dir)
        self._sequence_file = str(base_dir / f"sequence_{self.node_id}.txt")
        self._load_sequence()

        # 进程退出时自动持久化序列号，避免崩溃后丢失
        import atexit
        atexit.register(self.flush)

    def _load_sequence(self) -> None:
        try:
            if os.path.exists(self._sequence_file):
                with open(self._sequence_file, "r", encoding="utf-8") as f:
                    value = f.read().strip()
                self._sequence_number = int(value) if value else 0
        except Exception:
            self._sequence_number = 0

    def _save_sequence(self) -> None:
        tmp = f"{self._sequence_file}.tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(str(self._sequence_number))
            os.replace(tmp, self._sequence_file)
        except Exception:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

    def get_log_id(self) -> str:
        with self._lock:
            self._sequence_number += 1
            if self._sequence_number % self.persist_every == 0:
                self._save_sequence()
            ts_ms = int(time.time() * 1000)
            return f"{self.node_id}_{ts_ms}_{self._sequence_number}"

    def flush(self) -> None:
        with self._lock:
            self._save_sequence()


class LogAggregator:
    def __init__(
        self,
        window_size: int = 200,
        flush_interval: float = 5.0,
        key_fn: Optional[Callable[[Dict[str, Any]], str]] = None,
        on_flush: Optional[Callable[[List[Dict[str, Any]]], None]] = None,
        include_samples: bool = True,
    ):
        self.window_size = max(1, int(window_size))
        self.flush_interval = max(0.1, float(flush_interval))
        self._key_fn = key_fn or (lambda e: f"{e.get('level', 'INFO')}:{str(e.get('message', ''))[:80]}")
        self._on_flush = on_flush
        self._include_samples = include_samples

        self._buffer: deque[Dict[str, Any]] = deque(maxlen=self.window_size)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._output: "queue.Queue[List[Dict[str, Any]]]" = queue.Queue(maxsize=10)
        self._last_flush_at = time.time()

        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def add_log(self, log_entry: Dict[str, Any]) -> None:
        with self._lock:
            self._buffer.append(log_entry)
            if len(self._buffer) >= self.window_size:
                self._flush_locked()

    def flush(self) -> List[Dict[str, Any]]:
        with self._lock:
            return self._flush_locked()

    def get_aggregated(self, timeout: float = 0.0) -> Optional[List[Dict[str, Any]]]:
        try:
            return self._output.get(timeout=timeout)
        except queue.Empty:
            return None

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)
        try:
            self.flush()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()
        return False

    def _flush_locked(self) -> List[Dict[str, Any]]:
        if not self._buffer:
            return []

        groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for entry in self._buffer:
            groups[self._key_fn(entry)].append(entry)

        aggregated: List[Dict[str, Any]] = []
        for _, entries in groups.items():
            if len(entries) == 1:
                aggregated.append(entries[0])
                continue
            first = entries[0]
            item: Dict[str, Any] = dict(first)
            item["count"] = len(entries)
            item["message"] = f"[聚合] {first.get('message', '')} (重复 {len(entries)} 次)"
            if self._include_samples:
                item["sample"] = {"first": entries[0], "last": entries[-1]}
            aggregated.append(item)

        self._buffer.clear()
        self._last_flush_at = time.time()

        if self._on_flush is not None:
            try:
                self._on_flush(aggregated)
            except Exception:
                _logger.exception("LogAggregator on_flush 执行失败")
        else:
            try:
                self._output.put_nowait(aggregated)
            except queue.Full:
                pass

        return aggregated

    def _worker(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(self.flush_interval)
            if self._stop.is_set():
                break
            with self._lock:
                if self._buffer and (time.time() - self._last_flush_at) >= self.flush_interval:
                    self._flush_locked()


class PerformanceMonitor:
    def __init__(self, sample_interval: float = 5.0):
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._sample_interval = max(0.5, float(sample_interval))

        self._start_time = time.time()
        self._processing_times: deque[float] = deque(maxlen=1000)
        self._running_sum: float = 0.0
        self._metrics: Dict[str, Any] = {
            "log_count": 0,
            "error_count": 0,
            "avg_processing_time_ms": 0.0,
            "memory_usage_mb": None,
            "cpu_usage_percent": None,
            "throughput_per_sec": 0.0,
            "updated_at": _now_iso(),
        }

        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def record_log(self, level: str, processing_time_sec: float) -> None:
        with self._lock:
            self._metrics["log_count"] += 1
            if str(level).upper() == "ERROR":
                self._metrics["error_count"] += 1

            pt = float(processing_time_sec)

            # 当 deque 已满时，减去即将被淘汰的最旧值
            if len(self._processing_times) >= self._processing_times.maxlen:
                self._running_sum -= self._processing_times[0]

            self._processing_times.append(pt)
            self._running_sum += pt

            n = len(self._processing_times)
            if n > 0:
                self._metrics["avg_processing_time_ms"] = (self._running_sum / n) * 1000.0

    def get_metrics(self) -> Dict[str, Any]:
        with self._lock:
            return dict(self._metrics)

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=2)

    def _worker(self) -> None:
        process = None
        if psutil is not None:
            try:
                process = psutil.Process()
                process.cpu_percent(interval=None)
            except Exception:
                process = None

        while not self._stop.is_set():
            self._stop.wait(self._sample_interval)
            if self._stop.is_set():
                break
            with self._lock:
                if process is not None:
                    try:
                        self._metrics["memory_usage_mb"] = process.memory_info().rss / 1024 / 1024
                        self._metrics["cpu_usage_percent"] = process.cpu_percent(interval=None)
                    except Exception:
                        self._metrics["memory_usage_mb"] = None
                        self._metrics["cpu_usage_percent"] = None

                elapsed = time.time() - self._start_time
                if elapsed > 0:
                    self._metrics["throughput_per_sec"] = self._metrics["log_count"] / elapsed
                self._metrics["updated_at"] = _now_iso()


class LogArchiver:
    def __init__(self, archive_dir: str = "archives"):
        self.archive_dir = _ensure_dir(archive_dir)

    def compress_file(
        self,
        file_path: str,
        compression_type: str = "gzip",
        output_name: Optional[str] = None,
    ) -> str:
        src = Path(file_path)
        if not src.exists() or not src.is_file():
            raise FileNotFoundError(str(src))

        compression_type = str(compression_type).lower()
        if compression_type not in {"gzip", "zip"}:
            raise ValueError("compression_type 仅支持 gzip/zip")

        base_name = output_name or src.name
        out_path = Path(self.archive_dir) / base_name
        if compression_type == "gzip":
            if not str(out_path).endswith(".gz"):
                out_path = out_path.with_suffix(out_path.suffix + ".gz")
            import gzip

            with open(src, "rb") as f_in, gzip.open(out_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
            return str(out_path)

        if not str(out_path).endswith(".zip"):
            out_path = out_path.with_suffix(out_path.suffix + ".zip")
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as zf:
            zf.write(str(src), arcname=src.name)
        return str(out_path)

    def archive_logs(
        self,
        log_dir: str,
        days_old: int = 7,
        compression_type: str = "gzip",
        delete_original: bool = True,
    ) -> List[str]:
        log_path = Path(log_dir)
        if not log_path.exists():
            return []

        archived: List[str] = []
        cutoff = time.time() - (max(0, int(days_old)) * 86400)
        for fp in log_path.glob("*.log"):
            try:
                if fp.stat().st_mtime > cutoff:
                    continue
                out = self.compress_file(str(fp), compression_type=compression_type)
                archived.append(out)
                if delete_original:
                    fp.unlink(missing_ok=True)
            except Exception:
                _logger.exception("归档失败: %s", fp)
        return archived


class LogDatabase:
    _allowed_columns = {
        "id",
        "timestamp",
        "level",
        "message",
        "file",
        "line",
        "function",
        "process_id",
        "thread_id",
        "extra_data",
    }

    _INSERT_SQL = (
        "INSERT INTO logs (timestamp, level, message, file, line, function, process_id, thread_id, extra_data) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )

    def __init__(
        self,
        db_path: str = "logs.db",
        enable_wal: bool = True,
        busy_timeout_ms: int = 5000,
        batch_size: int = 100,
        flush_interval: float = 1.0,
    ):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if enable_wal:
            try:
                self._conn.execute("PRAGMA journal_mode=WAL;")
            except Exception:
                pass
        try:
            self._conn.execute(f"PRAGMA busy_timeout={int(busy_timeout_ms)};")
        except Exception:
            pass

        self._batch_size = max(1, int(batch_size))
        self._flush_interval = max(0.1, float(flush_interval))
        self._pending: List[tuple] = []
        self._stop = threading.Event()
        self._flush_thread = threading.Thread(target=self._flush_worker, daemon=True)

        self._init_database()
        self._flush_thread.start()

    def close(self) -> None:
        self._stop.set()
        try:
            self._flush_thread.join(timeout=2)
        except Exception:
            pass
        with self._lock:
            self._flush_pending()
            try:
                self._conn.close()
            except Exception:
                pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False

    def _init_database(self) -> None:
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    level TEXT NOT NULL,
                    message TEXT NOT NULL,
                    file TEXT,
                    line INTEGER,
                    function TEXT,
                    process_id INTEGER,
                    thread_id INTEGER,
                    extra_data TEXT
                )
                """
            )
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_timestamp ON logs(timestamp)")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_logs_level ON logs(level)")
            self._conn.commit()

    @staticmethod
    def _to_row(e: Mapping[str, Any]) -> tuple:
        return (
            e.get("timestamp") or _now_iso(),
            e.get("level") or "INFO",
            e.get("message") or "",
            e.get("file"),
            e.get("line"),
            e.get("function"),
            e.get("process_id"),
            e.get("thread_id"),
            json.dumps(e.get("extra_data") or {}, ensure_ascii=False),
        )

    def insert_log(self, log_entry: Mapping[str, Any]) -> None:
        """单条插入，缓冲到批次后统一提交"""
        row = self._to_row(log_entry)
        with self._lock:
            self._pending.append(row)
            if len(self._pending) >= self._batch_size:
                self._flush_pending()

    def insert_many(self, log_entries: Sequence[Mapping[str, Any]]) -> None:
        """批量插入，立即提交"""
        rows = [self._to_row(e) for e in log_entries]
        with self._lock:
            self._conn.executemany(self._INSERT_SQL, rows)
            self._conn.commit()

    def flush(self) -> None:
        """手动刷新缓冲区"""
        with self._lock:
            self._flush_pending()

    def _flush_pending(self) -> None:
        """将缓冲区中的行写入数据库（调用方需持有 _lock）"""
        if not self._pending:
            return
        self._conn.executemany(self._INSERT_SQL, self._pending)
        self._conn.commit()
        self._pending.clear()

    def _flush_worker(self) -> None:
        """后台定时刷新线程"""
        while not self._stop.is_set():
            self._stop.wait(self._flush_interval)
            if self._stop.is_set():
                break
            with self._lock:
                self._flush_pending()


    def query_logs(
        self,
        conditions: Optional[Mapping[str, Any]] = None,
        limit: int = 1000,
        order_desc: bool = True,
    ) -> List[Dict[str, Any]]:
        where_parts: List[str] = []
        params: List[Any] = []
        if conditions:
            for k, v in conditions.items():
                if k not in self._allowed_columns:
                    raise ValueError(f"不支持的查询字段: {k}")
                where_parts.append(f"{k} = ?")
                params.append(v)

        sql = "SELECT * FROM logs"
        if where_parts:
            sql += " WHERE " + " AND ".join(where_parts)
        sql += " ORDER BY timestamp " + ("DESC" if order_desc else "ASC")
        sql += " LIMIT ?"
        params.append(int(limit))

        with self._lock:
            cur = self._conn.execute(sql, params)
            result = []
            for row in cur.fetchall():
                d = dict(row)
                try:
                    d["extra_data"] = json.loads(d.get("extra_data") or "{}")
                except Exception:
                    pass
                result.append(d)
            return result

    def purge_older_than(self, days: int) -> int:
        days = int(days)
        if days <= 0:
            return 0
        cutoff = datetime.fromtimestamp(time.time() - days * 86400).isoformat(timespec="seconds")
        with self._lock:
            cur = self._conn.execute("DELETE FROM logs WHERE timestamp < ?", (cutoff,))
            self._conn.commit()
            return int(cur.rowcount or 0)


class LogStreamProcessor:
    def __init__(
        self,
        processors: Optional[List[Callable[[Dict[str, Any]], Dict[str, Any]]]] = None,
        max_queue_size: int = 10000,
        error_handler: Optional[Callable[[Exception, Dict[str, Any]], None]] = None,
    ):
        self.processors = list(processors or [])
        self._error_handler = error_handler

        self._input: "queue.Queue[Optional[Dict[str, Any]]]" = queue.Queue(maxsize=max(1, int(max_queue_size)))
        self._output: "queue.Queue[Dict[str, Any]]" = queue.Queue(maxsize=max(1, int(max_queue_size)))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._worker, daemon=True)
        self._thread.start()

    def add_processor(self, processor: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        # processors 列表可能被 _worker 线程遍历，使用锁保护
        with threading.Lock():
            self.processors = list(self.processors) + [processor]

    def process_log(self, log_entry: Dict[str, Any], block: bool = True, timeout: Optional[float] = None) -> bool:
        try:
            self._input.put(log_entry, block=block, timeout=timeout)
            return True
        except queue.Full:
            return False

    def get_processed_log(self, timeout: float = 0.0) -> Optional[Dict[str, Any]]:
        try:
            return self._output.get(timeout=timeout)
        except queue.Empty:
            return None

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        try:
            self._input.put_nowait(None)
        except Exception:
            pass
        self._thread.join(timeout=timeout)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.stop()
        return False

    def _handle_error(self, exc: Exception, entry: Dict[str, Any]) -> None:
        if self._error_handler is not None:
            try:
                self._error_handler(exc, entry)
            except Exception:
                _logger.exception("LogStreamProcessor error_handler 执行失败")
            return
        _logger.exception("日志处理失败")

    def _worker(self) -> None:
        while not self._stop.is_set():
            try:
                item = self._input.get(timeout=0.5)
            except queue.Empty:
                continue
            if item is None:
                break
            processed = item
            try:
                for p in self.processors:
                    processed = p(processed)
                try:
                    self._output.put_nowait(processed)
                except queue.Full:
                    pass
            except Exception as e:
                self._handle_error(e, item)


class LogAnalyzer:
    def __init__(self, patterns: Optional[Mapping[str, Sequence[str]]] = None):
        self.patterns: Dict[str, List[str]] = {
            "error": [
                r"Exception|Error|Failed|Timeout|Connection refused",
                r"HTTP \d{3}",
                r"ORA-\d{5}",
                r"MySQL.*error",
            ],
            "warning": [
                r"Warning|Deprecated|Deprecation",
                r"Slow query|Performance issue",
                r"Resource.*low|Memory.*high",
            ],
            "security": [
                r"Unauthorized|Forbidden|Authentication failed",
                r"SQL injection|XSS|CSRF",
                r"Failed login|Invalid credentials",
            ],
        }
        if patterns:
            for k, v in patterns.items():
                self.patterns[str(k)] = list(v)

        self._compiled: Dict[str, List[re.Pattern[str]]] = {}
        for category, pats in self.patterns.items():
            self._compiled[category] = [re.compile(p, re.IGNORECASE) for p in pats]

    def analyze_log(self, log_entry: Mapping[str, Any]) -> Dict[str, Any]:
        message = str(log_entry.get("message") or "")

        categories: List[str] = []
        patterns_found: List[str] = []
        severity = "normal"

        for pattern in self._compiled.get("error", []):
            if pattern.search(message):
                categories.append("error")
                patterns_found.append(pattern.pattern)
                severity = "high"
        for pattern in self._compiled.get("warning", []):
            if pattern.search(message):
                categories.append("warning")
                patterns_found.append(pattern.pattern)
                if severity == "normal":
                    severity = "medium"
        for pattern in self._compiled.get("security", []):
            if pattern.search(message):
                categories.append("security")
                patterns_found.append(pattern.pattern)
                severity = "critical"

        suggestions: List[str] = []
        if "error" in categories:
            suggestions.append("检查相关服务和依赖")
        if "security" in categories:
            suggestions.append("立即检查安全配置")
        if "warning" in categories:
            suggestions.append("监控系统性能")

        return {
            "severity": severity,
            "categories": list(dict.fromkeys(categories)),
            "suggestions": suggestions,
            "patterns_found": patterns_found,
        }


class LogHealthChecker:
    def __init__(self):
        self._process = None
        if psutil is not None:
            try:
                self._process = psutil.Process()
            except Exception:
                pass

    def check_health(self, log_dir: str) -> Dict[str, Any]:
        try:
            total, used, _ = shutil.disk_usage(log_dir)
            disk_usage_percent = (used / total) * 100 if total else 0.0

            memory_usage_mb = None
            if self._process is not None:
                try:
                    memory_usage_mb = self._process.memory_info().rss / 1024 / 1024
                except Exception:
                    memory_usage_mb = None

            log_files = list(Path(log_dir).glob("*.log"))
            total_size = sum(f.stat().st_size for f in log_files) if log_files else 0

            status = "healthy"
            warnings: List[str] = []
            if disk_usage_percent > 90:
                status = "critical"
                warnings.append("磁盘使用率过高")
            elif disk_usage_percent > 80:
                status = "warning"
                warnings.append("磁盘使用率较高")

            if memory_usage_mb is not None and memory_usage_mb > 1024:
                if status == "healthy":
                    status = "warning"
                warnings.append("内存使用量较高")

            result: Dict[str, Any] = {
                "status": status,
                "disk_usage_percent": float(disk_usage_percent),
                "memory_usage_mb": memory_usage_mb,
                "log_files_count": len(log_files),
                "total_log_size_mb": total_size / 1024 / 1024,
                "checked_at": _now_iso(),
            }
            if warnings:
                result["warnings"] = warnings
            return result
        except Exception as e:
            return {"status": "error", "error": str(e), "checked_at": _now_iso()}


class LogBackupManager:
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = _ensure_dir(backup_dir)

    def create_backup(self, log_dir: str, backup_name: Optional[str] = None) -> str:
        backup_name = backup_name or f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        backup_path = str(Path(self.backup_dir) / f"{backup_name}.tar.gz")

        with tarfile.open(backup_path, "w:gz") as tar:
            for log_file in Path(log_dir).glob("*.log"):
                tar.add(str(log_file), arcname=log_file.name)
        return backup_path

    def restore_backup(self, backup_path: str, restore_dir: str) -> bool:
        try:
            base = Path(restore_dir)
            _ensure_dir(base)
            with tarfile.open(backup_path, "r:gz") as tar:
                # Python 3.12+ 支持 filter 参数，更安全
                if hasattr(tarfile, "data_filter"):
                    tar.extractall(str(base), filter="data")
                else:
                    # 逐个验证并提取，避免 TOCTOU 竞态条件
                    for member in tar.getmembers():
                        if not member.name or member.name.startswith("/") or ".." in Path(member.name).parts:
                            raise RuntimeError(f"不安全的备份成员路径: {member.name}")
                        target = base / member.name
                        if not _is_within_directory(base, target):
                            raise RuntimeError(f"不安全的备份成员路径: {member.name}")
                        # 仅提取普通文件和目录，跳过符号链接等
                        if member.isfile() or member.isdir():
                            tar.extract(member, str(base))
                        else:
                            _logger.warning("跳过非常规备份成员: %s (类型: %s)", member.name, member.type)
            return True
        except Exception:
            _logger.exception("恢复备份失败")
            return False

    def list_backups(self) -> List[Dict[str, Any]]:
        backups: List[Dict[str, Any]] = []
        for backup_file in Path(self.backup_dir).glob("*.tar.gz"):
            stat = backup_file.stat()
            backups.append(
                {
                    "name": backup_file.name,
                    "path": str(backup_file),
                    "size_mb": stat.st_size / 1024 / 1024,
                    "created": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
                }
            )
        return sorted(backups, key=lambda x: x["created"], reverse=True)


__all__ = [
    "LogFilter",
    "LogSecurity",
    "DistributedLogger",
    "LogAggregator",
    "PerformanceMonitor",
    "LogArchiver",
    "LogDatabase",
    "LogStreamProcessor",
    "LogAnalyzer",
    "LogHealthChecker",
    "LogBackupManager",
]
