"""
Alert dispatcher — deduplication, severity routing, multi-channel dispatch.
Combines rule-based checks with ML signals.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import text

from src.utils import metrics
from src.utils.config import get_settings
from src.utils.db import get_db_session

log = logging.getLogger(__name__)


@dataclass
class Alert:
    connector_id: str
    connector_name: str
    alert_type: str
    issue: str
    severity: str
    reason: str
    details: dict = field(default_factory=dict)
    fired_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


def _parse_utc(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    ts = ts.rstrip("Z")
    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(ts, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


def check_connector_health(connector: dict) -> list[Alert]:
    cfg = get_settings().thresholds
    alerts: list[Alert] = []
    cid = connector["id"]
    cname = connector.get("schema", cid)
    now = datetime.now(timezone.utc)

    status_block = connector.get("status", {})
    sync_state = (status_block.get("sync_state") or "").lower()
    update_state = (status_block.get("update_state") or "").lower()
    paused = connector.get("paused", False)
    failed_at = connector.get("failed_at")
    succeeded_at = connector.get("succeeded_at")
    sync_freq = connector.get("sync_frequency")
    broken_states = cfg.broken_statuses | {"error", "broken", "failed"}

    if sync_state in broken_states or update_state in broken_states:
        last_err = _parse_utc(failed_at)
        down = ""
        if last_err:
            hours = int((now - last_err).total_seconds() // 3600)
            down = f" (failing ~{hours}h)"
        alerts.append(
            Alert(
                cid,
                cname,
                "connector_health",
                "broken",
                "critical",
                f"Connector in '{sync_state}'{down}. Last success: {succeeded_at or 'never'}",
            )
        )
    elif paused or sync_state == "paused":
        alerts.append(
            Alert(
                cid,
                cname,
                "connector_health",
                "paused",
                "warning",
                f"Connector paused. Last sync: {succeeded_at or 'never'}",
            )
        )
    elif sync_state == "rescheduled":
        alerts.append(
            Alert(
                cid,
                cname,
                "connector_health",
                "rescheduled",
                "warning",
                f"Sync rescheduled by Fivetran. Last sync: {succeeded_at or 'never'}",
            )
        )

    if sync_freq and succeeded_at and not paused and sync_state not in broken_states:
        last_ok = _parse_utc(succeeded_at)
        if last_ok and (now - last_ok) > timedelta(
            minutes=sync_freq * cfg.delay_multiplier
        ):
            elapsed = int((now - last_ok).total_seconds() // 60)
            alerts.append(
                Alert(
                    cid,
                    cname,
                    "connector_health",
                    "delayed",
                    "warning",
                    f"Last sync {elapsed} min ago; expected every {sync_freq} min",
                )
            )

    return alerts


def check_row_anomalies(connector: dict, history: list[dict]) -> list[Alert]:
    cfg = get_settings().thresholds
    alerts: list[Alert] = []
    cid = connector["id"]
    cname = connector.get("schema", cid)
    ok = [
        h
        for h in history
        if h.get("status", "").upper() == "SUCCESSFUL" and h.get("data")
    ]
    if len(ok) < 2:
        return alerts

    cur = ok[0].get("data", {})
    prev = ok[1].get("data", {})

    for table in set(cur) | set(prev):
        c = cur.get(table) or {}
        p = prev.get(table) or {}
        cr = c.get("rows_updated") or c.get("rows_inserted") or 0
        pr = p.get("rows_updated") or p.get("rows_inserted") or 0

        if cfg.zero_rows and cr == 0 and pr > 0:
            alerts.append(
                Alert(
                    cid,
                    cname,
                    "row_anomaly",
                    "zero",
                    "critical",
                    f"Rows dropped to ZERO (was {pr:,})",
                    {"table": table, "previous": pr, "current": cr, "pct_change": -100.0},
                )
            )
            continue

        if pr == 0:
            continue

        pct = ((cr - pr) / pr) * 100

        if pct <= -cfg.drop_pct:
            sev = "critical" if abs(pct) >= 50 else "warning"
            alerts.append(
                Alert(
                    cid,
                    cname,
                    "row_anomaly",
                    "drop",
                    sev,
                    f"Row count dropped {abs(pct):.1f}%",
                    {"table": table, "previous": pr, "current": cr, "pct_change": pct},
                )
            )
        elif pct >= cfg.spike_pct:
            alerts.append(
                Alert(
                    cid,
                    cname,
                    "row_anomaly",
                    "spike",
                    "warning",
                    f"Row count spiked +{pct:.1f}%",
                    {"table": table, "previous": pr, "current": cr, "pct_change": pct},
                )
            )

    return alerts


def load_ml_alerts() -> list[dict]:
    """Load unacknowledged ML alerts from the predictions table."""
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with get_db_session() as session:
        rows = session.execute(
            text("""
                SELECT connector_id, table_name, scored_at,
                       rows_updated, anomaly_score, forecast_value,
                       forecast_lower, forecast_upper, alert_reasons
                FROM ml_predictions
                WHERE is_anomaly = TRUE
                  AND scored_at >= :cutoff
                  AND alerted = FALSE
                ORDER BY scored_at DESC
            """),
            {"cutoff": cutoff},
        ).fetchall()

    keys = [
        "connector_id", "table_name", "scored_at", "rows_updated",
        "anomaly_score", "forecast_value", "forecast_lower",
        "forecast_upper", "alert_reasons",
    ]
    return [dict(zip(keys, r)) for r in rows]


def _is_duplicate(alert: Alert) -> bool:
    cfg = get_settings().thresholds
    window = datetime.now(timezone.utc) - timedelta(minutes=cfg.dedup_window_minutes)
    with get_db_session() as session:
        count = session.execute(
            text("""
                SELECT COUNT(*) FROM alert_log
                WHERE connector_id = :cid
                  AND issue         = :issue
                  AND alert_type    = :atype
                  AND fired_at     >= :window
            """),
            {
                "cid": alert.connector_id,
                "issue": alert.issue,
                "atype": alert.alert_type,
                "window": window,
            },
        ).scalar()
    return bool(count and count > 0)


def _log_alert(alert: Alert) -> None:
    with get_db_session() as session:
        session.execute(
            text("""
                INSERT INTO alert_log
                    (connector_id, connector_name, alert_type,
                     issue, severity, reason, details, fired_at)
                VALUES
                    (:cid, :cname, :atype, :issue, :sev,
                     :reason, :details, :fired_at)
            """),
            {
                "cid": alert.connector_id,
                "cname": alert.connector_name,
                "atype": alert.alert_type,
                "issue": alert.issue,
                "sev": alert.severity,
                "reason": alert.reason,
                "details": str(alert.details),
                "fired_at": alert.fired_at,
            },
        )


def dispatch_all(all_alerts: dict) -> int:
    """Route all alerts through dedup → ntfy + email. Returns count sent."""
    from src.alerts.email_sender import send_email_alert, send_summary_email
    from src.alerts.ntfy_sender import send_ntfy

    sent = 0
    all_alert_objs: list[Alert] = []

    for raw in all_alerts.get("health", []):
        all_alert_objs.append(Alert(**raw) if isinstance(raw, dict) else raw)
    for raw in all_alerts.get("row", []):
        all_alert_objs.append(Alert(**raw) if isinstance(raw, dict) else raw)

    for alert in all_alert_objs:
        if _is_duplicate(alert):
            metrics.alerts_suppressed.labels(reason="dedup").inc()
            log.debug("Suppressed duplicate alert: %s/%s", alert.connector_id, alert.issue)
            continue
        try:
            send_ntfy(alert)
            send_email_alert(alert)
            _log_alert(alert)
            metrics.alerts_fired.labels(
                severity=alert.severity,
                type=alert.alert_type,
                channel="all",
            ).inc()
            sent += 1
        except Exception as exc:
            log.error("Failed to dispatch alert %s: %s", alert.issue, exc)

    for ml in all_alerts.get("ml", []):
        a = Alert(
            connector_id=ml["connector_id"],
            connector_name=ml.get("connector_id", ""),
            alert_type="ml_anomaly",
            issue="ml_anomaly",
            severity="warning",
            reason=ml.get("alert_reasons", "ML anomaly detected"),
            details=ml,
        )
        if not _is_duplicate(a):
            send_ntfy(a)
            send_email_alert(a)
            _log_alert(a)
            sent += 1

    send_summary_email(all_alert_objs, all_alerts.get("ml", []))

    metrics.connectors_broken.set(
        sum(1 for a in all_alert_objs if a.issue == "broken")
    )
    metrics.connectors_delayed.set(
        sum(1 for a in all_alert_objs if a.issue == "delayed")
    )
    metrics.connectors_paused.set(
        sum(1 for a in all_alert_objs if a.issue == "paused")
    )

    log.info(
        "Dispatched %d alerts (%d total before dedup)", sent, len(all_alert_objs)
    )
    return sent
