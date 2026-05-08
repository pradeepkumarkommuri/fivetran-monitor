"""ntfy.sh alert sender."""
from __future__ import annotations

import logging

import requests

from src.utils.config import get_settings

log = logging.getLogger(__name__)

_SEV_PRIORITY = {"critical": "urgent", "warning": "high", "info": "default"}
_ISSUE_EMOJI = {
    "broken": "💥",
    "delayed": "⏰",
    "paused": "⏸",
    "rescheduled": "🔄",
    "drop": "📉",
    "spike": "📈",
    "zero": "🚫",
    "ml_anomaly": "🤖",
}


def _ascii(v: str) -> str:
    return v.encode("latin-1", errors="replace").decode("latin-1")


def send_ntfy(alert: object) -> bool:
    cfg = get_settings().ntfy
    url = f"{cfg.server.rstrip('/')}/{cfg.topic}"
    emoji = _ISSUE_EMOJI.get(getattr(alert, "issue", ""), "⚠️")
    sev = getattr(alert, "severity", "warning")
    sev_label = {"critical": "[CRITICAL]", "warning": "[WARNING]"}.get(sev, "[ALERT]")
    cname = getattr(alert, "connector_name", "")
    issue = getattr(alert, "issue", "")
    title = _ascii(f"{sev_label} Fivetran {issue.upper()}: {cname}")
    body = (
        f"{emoji} {getattr(alert, 'alert_type', '').replace('_', ' ').title()}\n"
        f"{'─' * 40}\n"
        f"Connector : {cname} ({getattr(alert, 'connector_id', '')})\n"
        f"Issue     : {issue.upper()}\n"
        f"Severity  : {sev.upper()}\n"
        f"Detail    : {getattr(alert, 'reason', '')}\n"
        f"Time      : {getattr(alert, 'fired_at', '')}"
    )
    headers = {
        "Title": title,
        "Priority": _SEV_PRIORITY.get(sev, "default"),
        "Tags": f"fivetran,{issue}",
        "Content-Type": "text/plain; charset=utf-8",
    }
    auth = None
    if cfg.auth_mode == "token":
        headers["Authorization"] = f"Bearer {cfg.access_token}"
    elif cfg.auth_mode == "basic":
        auth = (cfg.username, cfg.password)

    try:
        resp = requests.post(
            url, data=body.encode("utf-8"), headers=headers, auth=auth, timeout=10
        )
        resp.raise_for_status()
        return True
    except requests.HTTPError as exc:
        code = exc.response.status_code if exc.response is not None else "?"
        log.error("ntfy HTTP %s: %s", code, exc)
    except requests.RequestException as exc:
        log.error("ntfy error: %s", exc)
    return False
