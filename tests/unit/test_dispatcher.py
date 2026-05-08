"""Unit tests for alert dispatcher rule-based checks."""
import pytest

def _make_connector(sync_state="scheduled", update_state="on_schedule",
                    paused=False, succeeded_at="2024-01-01T10:00:00",
                    failed_at=None, sync_frequency=60):
    return {
        "id": "test_conn",
        "schema": "test_connector",
        "paused": paused,
        "failed_at": failed_at,
        "succeeded_at": succeeded_at,
        "sync_frequency": sync_frequency,
        "status": {"sync_state": sync_state, "update_state": update_state},
    }

def test_broken_connector_detected():
    from src.alerts.dispatcher import check_connector_health
    conn   = _make_connector(sync_state="error", failed_at="2024-01-01T08:00:00")
    alerts = check_connector_health(conn)
    assert any(a.issue == "broken" and a.severity == "critical" for a in alerts)

def test_paused_connector_detected():
    from src.alerts.dispatcher import check_connector_health
    conn   = _make_connector(paused=True)
    alerts = check_connector_health(conn)
    assert any(a.issue == "paused" for a in alerts)

def test_healthy_connector_no_alerts():
    from src.alerts.dispatcher import check_connector_health
    from datetime import datetime, timezone
    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    conn    = _make_connector(sync_state="scheduled", succeeded_at=now_str)
    alerts  = check_connector_health(conn)
    broken  = [a for a in alerts if a.issue == "broken"]
    assert len(broken) == 0

def test_zero_rows_alert():
    from src.alerts.dispatcher import check_row_anomalies
    conn    = {"id": "c1", "schema": "conn1"}
    history = [
        {"status": "SUCCESSFUL", "data": {"s.t": {"rows_updated": 0}}},
        {"status": "SUCCESSFUL", "data": {"s.t": {"rows_updated": 1000}}},
    ]
    alerts = check_row_anomalies(conn, history)
    assert any(a.issue == "zero" and a.severity == "critical" for a in alerts)

def test_row_drop_alert():
    from src.alerts.dispatcher import check_row_anomalies
    conn    = {"id": "c1", "schema": "conn1"}
    history = [
        {"status": "SUCCESSFUL", "data": {"s.t": {"rows_updated": 100}}},
        {"status": "SUCCESSFUL", "data": {"s.t": {"rows_updated": 1000}}},
    ]
    alerts = check_row_anomalies(conn, history)
    assert any(a.issue == "drop" for a in alerts)
