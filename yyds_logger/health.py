"""Optional health checks for a logger's local storage and process."""

from __future__ import annotations

import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from .i18n import get_message

try:
    import psutil
except Exception:  # pragma: no cover - optional dependency
    psutil = None


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


class LogHealthChecker:
    """Check disk, process memory, and local log-file health."""

    def __init__(
        self,
        disk_warning_percent: float = 80.0,
        disk_critical_percent: float = 90.0,
        memory_warning_mb: Optional[float] = 1024.0,
        language: str = "zh",
    ) -> None:
        self.language = str(language).strip().lower()
        if self.language not in {"zh", "en"}:
            raise ValueError(get_message(self.language, "ERR_LANGUAGE"))
        if not 0 <= disk_warning_percent <= 100:
            raise ValueError(get_message(self.language, "ERR_HEALTH_DISK_WARNING"))
        if not 0 <= disk_critical_percent <= 100:
            raise ValueError(get_message(self.language, "ERR_HEALTH_DISK_CRITICAL"))
        if disk_warning_percent > disk_critical_percent:
            raise ValueError(get_message(self.language, "ERR_HEALTH_THRESHOLD_ORDER"))
        if memory_warning_mb is not None and memory_warning_mb < 0:
            raise ValueError(get_message(self.language, "ERR_HEALTH_MEMORY"))

        self.disk_warning_percent = float(disk_warning_percent)
        self.disk_critical_percent = float(disk_critical_percent)
        self.memory_warning_mb = memory_warning_mb
        self._process = None
        if psutil is not None:
            try:
                self._process = psutil.Process(os.getpid())
            except Exception:
                pass

    def check_health(self, log_dir: str) -> Dict[str, Any]:
        """Return a stable health result for ``log_dir``."""
        checked_at = _now_iso()
        path = Path(log_dir)
        if not path.exists() or not path.is_dir():
            return {
                "status": "error",
                "warnings": [],
                "errors": [get_message(self.language, "HEALTH_DIR_INVALID", dir=log_dir)],
                "metrics": {},
                "checked_at": checked_at,
            }

        warnings: List[str] = []
        errors: List[str] = []
        disk_critical = False
        try:
            total, used, _ = shutil.disk_usage(path)
            disk_usage_percent = (used / total) * 100 if total else 0.0
        except OSError as exc:
            disk_usage_percent = None
            errors.append(get_message(self.language, "HEALTH_DISK_READ_FAILED", error=exc))

        memory_usage_mb = None
        if self._process is not None:
            try:
                memory_usage_mb = self._process.memory_info().rss / 1024 / 1024
            except Exception as exc:
                warnings.append(get_message(self.language, "HEALTH_MEMORY_READ_FAILED", error=exc))

        try:
            log_files = [
                file_path for file_path in path.iterdir()
                if file_path.is_file() and (
                    file_path.name.endswith(".log") or
                    file_path.name.endswith(".log.gz") or
                    file_path.name.endswith(".log.zip")
                )
            ]
        except OSError as exc:
            log_files = []
            errors.append(get_message(self.language, "HEALTH_DIR_READ_FAILED", error=exc))
        total_size = 0
        for file_path in log_files:
            try:
                total_size += file_path.stat().st_size
            except OSError:
                warnings.append(get_message(self.language, "HEALTH_FILE_SIZE_FAILED", file=file_path))

        if disk_usage_percent is not None:
            if disk_usage_percent >= self.disk_critical_percent:
                disk_critical = True
                warnings.append(get_message(self.language, "HEALTH_DISK_CRITICAL"))
            elif disk_usage_percent >= self.disk_warning_percent:
                warnings.append(get_message(self.language, "HEALTH_DISK_WARNING"))
        if self.memory_warning_mb is not None and memory_usage_mb is not None:
            if memory_usage_mb >= self.memory_warning_mb:
                warnings.append(get_message(self.language, "HEALTH_MEMORY_WARNING"))

        status = "critical" if errors or disk_critical else "healthy"
        if warnings and status == "healthy":
            status = "warning"
        return {
            "status": status,
            "warnings": warnings,
            "errors": errors,
            "metrics": {
                "disk_usage_percent": disk_usage_percent,
                "memory_usage_mb": memory_usage_mb,
                "log_files_count": len(log_files),
                "total_log_size_mb": total_size / 1024 / 1024,
            },
            "checked_at": checked_at,
        }


__all__ = ["LogHealthChecker"]
