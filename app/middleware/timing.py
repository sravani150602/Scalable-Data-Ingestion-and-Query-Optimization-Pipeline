"""Request timing middleware for latency monitoring."""

import logging
import time

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response

logger = logging.getLogger(__name__)


class TimingMiddleware(BaseHTTPMiddleware):
    """Adds X-Response-Time header and logs request latency."""

    async def dispatch(self, request: Request, call_next) -> Response:
        start = time.time()
        response = await call_next(request)
        duration_ms = (time.time() - start) * 1000

        response.headers["X-Response-Time"] = f"{duration_ms:.2f}ms"

        logger.info(
            f"{request.method} {request.url.path} - {response.status_code} - {duration_ms:.2f}ms"
        )
        return response
