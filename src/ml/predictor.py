"""ML inference — scores latest sync data and writes results to PostgreSQL."""
from __future__ import annotations
import logging
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import text
from src.utils.config import get_settings
from src.utils.db import get_db_session
from src.utils import metrics
from src.ml import anomaly_detector, forecaster

log = logging.getLogger(__name__)

def run_prediction(partition: str | None = None) -> list[dict]:
    """
    Load latest features, run both models, write predictions to DB.
    Returns list of anomaly dicts for downstream alert_dag.
    """
    cfg      = get_settings()
    results  = []

    # Load latest features from DB
    with get_db_session() as session:
        rows = session.execute(text("""
            SELECT DISTINCT ON (connector_id, table_name)
                connector_id, connector_name, table_name, sync_time,
                rows_updated, rows_pct_change, z_score,
                rolling_mean_7d, rolling_std_7d,
                rolling_mean_30d, rolling_std_30d,
                rows_lag1, rows_lag2, rows_lag3,
                day_of_week, hour_of_day, is_weekend
            FROM feature_store
            ORDER BY connector_id, table_name, sync_time DESC
        """)).fetchall()
        col_names = session.execute(
            text("SELECT column_name FROM information_schema.columns "
                 "WHERE table_name='feature_store' ORDER BY ordinal_position")
        ).scalars().all()

    if not rows:
        log.warning("No features available for prediction.")
        return []

    df = pd.DataFrame(rows, columns=[
        "connector_id","connector_name","table_name","sync_time",
        "rows_updated","rows_pct_change","z_score",
        "rolling_mean_7d","rolling_std_7d",
        "rolling_mean_30d","rolling_std_30d",
        "rows_lag1","rows_lag2","rows_lag3",
        "day_of_week","hour_of_day","is_weekend",
    ])

    scored_at = datetime.now(timezone.utc)

    for _, row in df.iterrows():
        cid   = row["connector_id"]
        table = row["table_name"]
        rec   = row.to_dict()

        result = {
            "connector_id":   cid,
            "connector_name": row.get("connector_name", cid),
            "table_name":     table,
            "sync_time":      str(row["sync_time"]),
            "rows_updated":   int(row["rows_updated"] or 0),
            "scored_at":      scored_at.isoformat(),
            "anomaly":        None,
            "forecast":       None,
            "is_alert":       False,
            "alert_reasons":  [],
        }

        # ── Isolation Forest ──────────────────────────────────────────────────
        if_model = anomaly_detector.load(cid, table)
        if if_model:
            anomaly_result = anomaly_detector.predict(if_model, rec)
            result["anomaly"] = anomaly_result
            metrics.anomaly_score.labels(connector_id=cid, table=table).set(
                anomaly_result["score"]
            )
            if anomaly_result["is_anomaly"]:
                result["is_alert"] = True
                result["alert_reasons"].append(
                    f"ML anomaly detected (score={anomaly_result['score']:.3f}, "
                    f"confidence={anomaly_result['confidence']:.1%})"
                )

        # ── Prophet forecast ──────────────────────────────────────────────────
        prophet_model = forecaster.load(cid)
        if prophet_model:
            try:
                fc = forecaster.predict_next(prophet_model)
                result["forecast"] = fc
                deviation = 0.0
                if fc["forecast"] > 0:
                    deviation = ((row["rows_updated"] - fc["forecast"]) / fc["forecast"]) * 100
                metrics.forecast_deviation.labels(connector_id=cid).set(deviation)
                if forecaster.is_outside_bounds(float(row["rows_updated"]), fc):
                    result["is_alert"] = True
                    result["alert_reasons"].append(
                        f"Outside forecast bounds "
                        f"(actual={row['rows_updated']:,}, "
                        f"expected={fc['forecast']:,.0f} "
                        f"[{fc['lower']:,.0f}–{fc['upper']:,.0f}])"
                    )
            except Exception as exc:
                log.warning("Prophet prediction failed for %s: %s", cid, exc)

        results.append(result)

        # Persist prediction to DB
        _write_prediction(result)

    anomalies = [r for r in results if r["is_alert"]]
    log.info("Scored %d rows — %d ML alerts", len(results), len(anomalies))
    return anomalies

def _write_prediction(rec: dict) -> None:
    with get_db_session() as session:
        session.execute(text("""
            INSERT INTO ml_predictions
                (connector_id, table_name, sync_time, scored_at,
                 rows_updated, anomaly_score, is_anomaly,
                 forecast_value, forecast_lower, forecast_upper, alert_reasons)
            VALUES
                (:connector_id, :table_name, :sync_time, :scored_at,
                 :rows_updated, :anomaly_score, :is_anomaly,
                 :forecast_value, :forecast_lower, :forecast_upper, :alert_reasons)
            ON CONFLICT (connector_id, table_name, sync_time)
            DO UPDATE SET
                scored_at       = EXCLUDED.scored_at,
                anomaly_score   = EXCLUDED.anomaly_score,
                is_anomaly      = EXCLUDED.is_anomaly,
                forecast_value  = EXCLUDED.forecast_value,
                forecast_lower  = EXCLUDED.forecast_lower,
                forecast_upper  = EXCLUDED.forecast_upper,
                alert_reasons   = EXCLUDED.alert_reasons
        """), {
            "connector_id":    rec["connector_id"],
            "table_name":      rec["table_name"],
            "sync_time":       rec["sync_time"],
            "scored_at":       rec["scored_at"],
            "rows_updated":    rec["rows_updated"],
            "anomaly_score":   rec.get("anomaly", {}).get("score") if rec.get("anomaly") else None,
            "is_anomaly":      rec.get("anomaly", {}).get("is_anomaly", False) if rec.get("anomaly") else False,
            "forecast_value":  rec.get("forecast", {}).get("forecast") if rec.get("forecast") else None,
            "forecast_lower":  rec.get("forecast", {}).get("lower") if rec.get("forecast") else None,
            "forecast_upper":  rec.get("forecast", {}).get("upper") if rec.get("forecast") else None,
            "alert_reasons":   str(rec.get("alert_reasons", [])),
        })
