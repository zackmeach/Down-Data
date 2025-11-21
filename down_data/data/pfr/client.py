"""HTTP client helpers for interacting with Pro-Football-Reference."""

from __future__ import annotations

import logging
import time
from typing import Optional
from urllib.parse import urljoin

import requests

try:  # pragma: no cover - optional dependency for caching
    import requests_cache
except ImportError:  # pragma: no cover - cache support is optional
    requests_cache = None  # type: ignore[assignment]

logger = logging.getLogger(__name__)

BASE_URL = "https://www.pro-football-reference.com"
DEFAULT_USER_AGENT = (
    "down-data-pfr-scraper/0.1 "
    "(+https://github.com/zdm80/down-data; respecting Sports-Reference policies)"
)


class PFRClient:
    """Small convenience wrapper around :class:`requests.Session`.

    The client is intentionally minimal; it focuses on:

    - identifying the application via a descriptive ``User-Agent``
    - enforcing a polite delay between requests
    - optional integration with :mod:`requests_cache` for local caching
    """

    def __init__(
        self,
        *,
        user_agent: str = DEFAULT_USER_AGENT,
        base_url: str = BASE_URL,
        min_delay: float = 1.0,
        timeout: float = 30.0,
        enable_cache: bool = True,
        cache_name: str = "pfr_cache",
        cache_backend: str = "sqlite",
        cache_expire: Optional[int] = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.min_delay = max(0.0, min_delay)
        self.timeout = timeout
        self._last_request_ts: Optional[float] = None
        self.session = self._create_session(
            user_agent=user_agent,
            enable_cache=enable_cache,
            cache_name=cache_name,
            cache_backend=cache_backend,
            cache_expire=cache_expire,
        )

    def _create_session(
        self,
        *,
        user_agent: str,
        enable_cache: bool,
        cache_name: str,
        cache_backend: str,
        cache_expire: Optional[int],
    ) -> requests.Session:
        if enable_cache and requests_cache is not None:
            session = requests_cache.CachedSession(
                cache_name=cache_name,
                backend=cache_backend,
                expire_after=cache_expire,
            )
        else:
            if enable_cache and requests_cache is None:
                logger.warning(
                    "requests-cache is not installed; proceeding without HTTP caching."
                )
            session = requests.Session()

        session.headers.update({"User-Agent": user_agent})
        return session

    def build_url(self, path: str) -> str:
        """Return an absolute URL for ``path``."""

        if path.startswith("http://") or path.startswith("https://"):
            return path

        normalized = path if path.startswith("/") else f"/{path}"
        return urljoin(f"{self.base_url}/", normalized.lstrip("/"))

    def _sleep_if_needed(self) -> None:
        if self._last_request_ts is None or self.min_delay <= 0:
            return

        elapsed = time.time() - self._last_request_ts
        if elapsed < self.min_delay:
            wait = self.min_delay - elapsed
            logger.debug("Sleeping %.2fs to respect min_delay.", wait)
            time.sleep(wait)

    def get(self, path: str, **kwargs) -> requests.Response:
        """Perform a ``GET`` request and return the :class:`Response` object."""

        self._sleep_if_needed()

        url = self.build_url(path)
        response = self.session.get(url, timeout=self.timeout, **kwargs)
        self._last_request_ts = time.time()
        response.raise_for_status()
        return response

    def close(self) -> None:
        """Close the underlying :class:`requests.Session`."""

        self.session.close()

    def __enter__(self) -> "PFRClient":
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

