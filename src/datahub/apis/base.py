"""Base HTTP client primitives for DataHub API-backed integrations."""

from __future__ import annotations

import json
import logging
import time
from copy import deepcopy
from pathlib import Path
from typing import Any, Iterable, Mapping

import requests


logger = logging.getLogger(__name__)


class ApiClientError(RuntimeError):
    """Raised when a remote API call fails."""


class JsonFileApiCache:
    """Simple namespace/key JSON cache persisted to disk."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path else None
        self._payload: dict[str, dict[str, Any]] = {}
        self._load()

    def _load(self) -> None:
        if self.path is None or not self.path.exists():
            return
        loaded = json.loads(self.path.read_text())
        if not isinstance(loaded, dict):
            return
        self._payload = {
            str(namespace): dict(values or {})
            for namespace, values in loaded.items()
            if isinstance(values, dict)
        }

    def get(self, namespace: str, key: str) -> Any | None:
        namespace_payload = self._payload.get(namespace)
        if namespace_payload is None or key not in namespace_payload:
            return None
        return deepcopy(namespace_payload[key])

    def set(self, namespace: str, key: str, value: Any) -> None:
        namespace_payload = self._payload.setdefault(namespace, {})
        namespace_payload[key] = deepcopy(value)

    def persist(self) -> None:
        if self.path is None:
            return
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(self._payload, indent=2, sort_keys=True))


class RestApiClient:
    """Base class for JSON-speaking REST API clients."""

    base_url: str = ""

    def __init__(
        self,
        *,
        base_url: str | None = None,
        timeout_seconds: float = 30.0,
        sleep_seconds: float = 0.0,
        cache: JsonFileApiCache | None = None,
        cache_path: str | Path | None = None,
        session: requests.Session | None = None,
        max_retries: int = 6,
        retry_backoff_seconds: float = 1.0,
        retry_status_codes: Iterable[int] = (429, 500, 502, 503, 504),
    ) -> None:
        resolved_base_url = (base_url or self.base_url).strip().rstrip("/")
        if not resolved_base_url:
            raise ValueError("base_url must be provided")

        self.base_url = resolved_base_url
        self.timeout_seconds = float(timeout_seconds)
        self.sleep_seconds = max(float(sleep_seconds), 0.0)
        self.cache = cache or JsonFileApiCache(cache_path)
        self._session = session or requests.Session()
        self._owns_session = session is None
        self.max_retries = max(int(max_retries), 0)
        self.retry_backoff_seconds = max(float(retry_backoff_seconds), 0.0)
        self.retry_status_codes = {int(status_code) for status_code in retry_status_codes}

    def close(self) -> None:
        self.cache.persist()
        if self._owns_session:
            self._session.close()

    def get_json(
        self,
        endpoint: str,
        *,
        params: Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
        cache_namespace: str | None = None,
        cache_key: str | None = None,
    ) -> Any:
        if cache_namespace and cache_key:
            cached = self.cache.get(cache_namespace, cache_key)
            if cached is not None:
                return cached

        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(dict(headers))

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        response: requests.Response | None = None
        for attempt in range(self.max_retries + 1):
            try:
                response = self._session.get(
                    url,
                    params=dict(params or {}),
                    headers=request_headers,
                    timeout=self.timeout_seconds,
                )
                response.raise_for_status()
                break
            except requests.RequestException as exc:
                status_code = self._status_code_from_exception(exc)
                if attempt >= self.max_retries or status_code not in self.retry_status_codes:
                    raise ApiClientError(f"API request failed for {url}: {exc}") from exc

                delay = self._retry_delay_seconds(exc, attempt=attempt)
                logger.warning(
                    "API request failed with retryable status %s for %s; retrying in %.1fs (%d/%d)",
                    status_code,
                    url,
                    delay,
                    attempt + 1,
                    self.max_retries,
                )
                if delay:
                    time.sleep(delay)

        if response is None:
            raise ApiClientError(f"API request failed for {url}: no response")

        try:
            payload = response.json()
        except ValueError as exc:
            raise ApiClientError(f"API response for {url} was not valid JSON") from exc

        if cache_namespace and cache_key:
            self.cache.set(cache_namespace, cache_key, payload)

        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)

        return deepcopy(payload)

    def _status_code_from_exception(self, exc: requests.RequestException) -> int | None:
        response = getattr(exc, "response", None)
        status_code = getattr(response, "status_code", None)
        try:
            return int(status_code) if status_code is not None else None
        except (TypeError, ValueError):
            return None

    def _retry_delay_seconds(self, exc: requests.RequestException, *, attempt: int) -> float:
        response = getattr(exc, "response", None)
        retry_after = getattr(response, "headers", {}).get("Retry-After") if response is not None else None
        if retry_after:
            try:
                return max(float(retry_after), 0.0)
            except ValueError:
                pass
        return self.retry_backoff_seconds * (2**attempt)
