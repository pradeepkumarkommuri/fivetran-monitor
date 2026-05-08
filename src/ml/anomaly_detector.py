"""
Isolation Forest anomaly detector.
One model trained per connector_id + table_name combination.
"""
from __future__ import annotations
import io, logging
import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from src.utils.config import get_settings
from src.utils import storage

log = logging.getLogger(__name__)

FEATURE_COLS = [
    "rows_updated", "rows_pct_change", "z_score",
    "rolling_mean_7d", "rolling_std_7d",
    "rolling_mean_30d", "rolling_std_30d",
    "rows_lag1", "rows_lag2", "rows_lag3",
    "day_of_week", "hour_of_day", "is_weekend",
]

def _model_key(connector_id: str, table: str) -> str:
    safe = table.replace("/", "_").replace(".", "_")
    return f"{connector_id}/{safe}.joblib"

def train(
    connector_id: str,
    table: str,
    df: pd.DataFrame,
) -> Pipeline | None:
    """
    Train an Isolation Forest on historical feature data.
    Returns None if insufficient data.
    """
    cfg = get_settings().ml
    if len(df) < cfg.min_training_samples:
        log.warning("Skipping %s/%s — only %d samples (need %d)",
                    connector_id, table, len(df), cfg.min_training_samples)
        return None

    available = [c for c in FEATURE_COLS if c in df.columns]
    X = df[available].fillna(0).values

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("iforest", IsolationForest(
            n_estimators=cfg.anomaly_n_estimators,
            contamination=cfg.anomaly_contamination,
            random_state=42,
            n_jobs=-1,
        )),
    ])
    pipeline.fit(X)
    log.info("Trained IsolationForest for %s/%s on %d samples", connector_id, table, len(X))

    # Persist to MinIO
    buf = io.BytesIO()
    joblib.dump(pipeline, buf)
    storage.put_bytes(
        get_settings().storage.models_prefix,
        f"isolation_forest/{_model_key(connector_id, table)}",
        buf.getvalue(),
    )
    return pipeline

def load(connector_id: str, table: str) -> Pipeline | None:
    """Load a trained model from MinIO. Returns None if not found."""
    try:
        data = storage.get_bytes(
            get_settings().storage.models_prefix,
            f"isolation_forest/{_model_key(connector_id, table)}",
        )
        return joblib.load(io.BytesIO(data))
    except Exception as exc:
        log.debug("No model found for %s/%s: %s", connector_id, table, exc)
        return None

def predict(
    pipeline: Pipeline,
    row: dict,
) -> dict:
    """
    Score a single data point.
    Returns {"is_anomaly": bool, "score": float, "confidence": float}
    """
    cfg = get_settings().ml
    available = [c for c in FEATURE_COLS if c in row]
    X = np.array([[row.get(c, 0) for c in available]])
    score      = pipeline.decision_function(X)[0]   # negative = more anomalous
    prediction = pipeline.predict(X)[0]              # -1 = anomaly, 1 = normal
    # Normalise score to 0-1 confidence
    confidence = float(np.clip(1 - (score + 0.5), 0, 1))
    return {
        "is_anomaly": bool(prediction == -1),
        "score":      float(score),
        "confidence": confidence,
        "threshold":  cfg.anomaly_threshold,
    }
