"""Backward-compatible import for the optional health checker.

The former aggregation, archiving, analysis, monitoring, and processing
helpers were removed because they did not belong to the core logger.
"""

from .health import LogHealthChecker

__all__ = ["LogHealthChecker"]
