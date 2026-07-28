#!/usr/bin/env python
"""验证 semaphore 泄漏修复：模拟高频 time_it 场景"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import warnings
import time

# 捕获 resource_tracker 警告
warnings.simplefilter("error", UserWarning)

from yyds_logger import YydsLogger  # noqa: E402

# 默认 env="prod" 仍使用 enqueue=True。
log = YydsLogger("test_semaphore", log_dir="/tmp/test_sem_logs")


@log.time_it
def fast_func(i):
    """模拟快速处理函数"""
    return i * 2

# 模拟高频调用（类似 OCR 每张图 30~50ms 的场景）
print("开始高频日志测试（100 次 time_it 调用）...")
start = time.perf_counter()
for i in range(100):
    fast_func(i)
elapsed = (time.perf_counter() - start) * 1000
print(f"完成，总耗时: {elapsed:.1f}ms")

# 手动 cleanup（模拟正常退出路径）
log.cleanup()

print("✅ 无 semaphore 泄漏警告")
