"""Optional health-check example."""

import asyncio
import contextvars
import os

from yyds_logger import LogHealthChecker, YydsLogger


async def main() -> None:
    log_dir = os.path.join(os.path.dirname(__file__), "logs_adv")
    logger = YydsLogger("advanced", log_dir=log_dir, language="zh")
    try:
        async def handle(request_id: str) -> None:
            token = logger.set_request_id(request_id)
            try:
                logger.info("async request started")
                await asyncio.sleep(0.01)
                logger.info("async request finished")
            finally:
                logger.request_id_var.reset(token)

        await asyncio.gather(handle("req-1"), handle("req-2"))

        token = logger.set_request_id("req-thread")
        try:
            context = contextvars.copy_context()
            context.run(logger.info, "context propagation example")
        finally:
            logger.request_id_var.reset(token)
    finally:
        logger.cleanup()

    print("health:", LogHealthChecker().check_health(log_dir))


if __name__ == "__main__":
    asyncio.run(main())
