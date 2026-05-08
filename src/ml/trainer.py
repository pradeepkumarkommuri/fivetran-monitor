"""ML training orchestrator — called by Airflow ml_train_dag."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from sqlalchemy import text

from src.ml import anomaly_detector, forecaster
from src.utils import metrics
from src.utils.config import get_settings
from src.utils.db import get_db_session

log = logging.getLogger(__name__)


def _load_features(lookback_days: int) -> pd.DataFrame:
    """Load feature store from PostgreSQL."""
    since = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    with get_db_session() as session:
        result = session.execute(
            text("""
                SELECT connector_id, connector_name, table_name, sync_time,
                       rows_updated, rows_pct_change, z_score,
                       rolling_mean_7d, rolling_std_7d,
                       rolling_mean_30d, rolling_std_30d,
                       rows_lag1, rows_lag2, rows_lag3,
                       day_of_week, hour_of_day, is_weekend
                FROM feature_store
                WHERE sync_time >= :since
                ORDER BY connector_id, table_name, sync_time
            """),
            {"since": since},
        )
        rows = result.fetchall()
        cols = list(result.keys())
    return pd.DataFrame(rows, columns=cols)


def run_training() -> dict:
    """
    Train Isolation Forest (per connector+table) and
    Prophet (per connector). Returns summary dict.
    """
    cfg = get_settings().ml
    df = _load_features(cfg.training_lookback_days)
    summary = {
        "isolation_forest": {"trained": 0, "skipped": 0},
        "prophet": {"trained": 0, "skipped": 0},
    }

    if df.empty:
        log.warning("No feature data found — skipping training.")
        return summary

    for (cid, table), group in df.groupby(["connector_id", "table_name"]):
        with metrics.model_train_duration.labels(model_type="isolation_forest").time():
            model = anomaly_detector.train(str(cid), str(table), group)
        if model:
            summary["isolation_forest"]["trained"] += 1
            metrics.model_last_trained.labels(connector_id=str(cid)).set(
                datetime.now(timezone.utc).timestamp()
            )
        else:
            summary["isolation_forest"]["skipped"] += 1

    connector_ts = (
        df.groupby(["connector_id", "sync_time"])["rows_updated"]
        .sum()
        .reset_index()
    )
    for cid, group in connector_ts.groupby("connector_id"):
        with metrics.model_train_duration.labels(model_type="prophet").time():
            model = forecaster.train(str(cid), group)
        if model:
            summary["prophet"]["trained"] += 1
        else:
            summary["prophet"]["skipped"] += 1

    log.info(
        "Training complete — IF: %d trained / %d skipped | Prophet: %d trained / %d skipped",
        summary["isolation_forest"]["trained"],
        summary["isolation_forest"]["skipped"],
        summary["prophet"]["trained"],
        summary["prophet"]["skipped"],
    )
    return summary
