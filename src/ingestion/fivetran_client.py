"""Fivetran REST API client with retry, pagination, and connection pooling."""
from __future__ import annotations
import logging
from typing import Optional
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from src.utils.config import get_settings

log = logging.getLogger(__name__)

def _build_session(max_retries: int = 3) -> requests.Session:
    session = requests.Session()
    cfg     = get_settings().fivetran
    session.auth    = (cfg.api_key, cfg.api_secret)
    session.headers.update({"Accept": "application/json;version=2"})
    retry_cfg = Retry(
        total=max_retries, backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry_cfg, pool_connections=10, pool_maxsize=20)
    session.mount("https://", adapter)
    return session

class FivetranClient:
    def __init__(self):
        cfg = get_settings().fivetran
        self._base    = cfg.base_url.rstrip("/")
        self._timeout = cfg.request_timeout
        self._session = _build_session(cfg.max_retries)

    def _get(self, path: str, params: dict | None = None) -> dict:
        url  = f"{self._base}{path}"
        resp = self._session.get(url, params=params, timeout=self._timeout)
        resp.raise_for_status()
        return resp.json()

    def list_connectors(self, group_id: Optional[str] = None) -> list[dict]:
        connectors, cursor = [], None
        while True:
            params = {"limit": 100}
            if group_id: params["group_id"] = group_id
            if cursor:   params["cursor"]   = cursor
            data = self._get("/connectors", params=params)
            connectors.extend(data.get("data", {}).get("items", []))
            cursor = data.get("data", {}).get("next_cursor")
            if not cursor:
                break
        log.info("Fetched %d connectors", len(connectors))
        return connectors

    def get_connector(self, connector_id: str) -> dict:
        return self._get(f"/connectors/{connector_id}").get("data", {})

    def get_sync_history(self, connector_id: str, limit: int = 10) -> list[dict]:
        """Try multiple endpoints; return normalised list."""
        for path in (
            f"/connectors/{connector_id}/sync-status",
            f"/connectors/{connector_id}/connector-history",
        ):
            try:
                data  = self._get(path, params={"limit": limit})
                items = data.get("data", {}).get("items") or data.get("data", [])
                if items:
                    return items
            except requests.HTTPError as exc:
                if exc.response is not None and exc.response.status_code in (400, 404):
                    continue
                raise
        # Schema fallback
        try:
            schema_data = self._get(f"/connectors/{connector_id}/schemas")
            schemas = schema_data.get("data", {}).get("schemas", {})
            table_rows: dict = {}
            for sname, sbody in schemas.items():
                for tname, tbody in (sbody.get("tables") or {}).items():
                    table_rows[f"{sname}.{tname}"] = {
                        "rows_updated": tbody.get("rows_updated", 0)
                    }
            if table_rows:
                return [{"status": "SUCCESSFUL", "data": table_rows}]
        except requests.HTTPError:
            pass
        return []

    def list_groups(self) -> list[dict]:
        return self._get("/groups").get("data", {}).get("items", [])
