"""
Prophet time-series forecaster.
One model per connector_id. Predicts expected row counts for next sync window.
"""
from __future__ import annotations
import io, logging
import joblib
import pandas as pd
from src.utils.config import get_settings
from src.utils import storage

log = logging.getLogger(__name__)

def _model_key(connector_id: str) -> str:
    return f"{connector_id}/prophet.joblib"

def train(
    connector_id: str,
    df: pd.DataFrame,
    date_col: str = "sync_time",
    value_col: str = "rows_updated",
) -> object | None:
    """
    Train a Prophet model for a connector's total row counts over time.
    Returns None if insufficient data.
    """
    try:
        from prophet import Prophet  # lazy import — Prophet is heavy
    except ImportError:
        log.error("prophet package not installed. Run: pip install prophet")
        return None

    cfg = get_settings().ml
    ts  = (
        df[[date_col, value_col]]
        .rename(columns={date_col: "ds", value_col: "y"})
        .dropna()
        .sort_values("ds")
    )
    if len(ts) < cfg.min_training_samples:
        log.warning("Skipping Prophet for %s — only %d rows", connector_id, len(ts))
        return None

    model = Prophet(
        interval_width=cfg.forecast_interval_width,
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=False,
        changepoint_prior_scale=0.05,
    )
    model.fit(ts)
    log.info("Trained Prophet for %s on %d points", connector_id, len(ts))

    buf = io.BytesIO()
    joblib.dump(model, buf)
    storage.put_bytes(
        get_settings().storage.models_prefix,
        f"prophet/{_model_key(connector_id)}",
        buf.getvalue(),
    )
    return model

def load(connector_id: str) -> object | None:
    try:
        data = storage.get_bytes(
            get_settings().storage.models_prefix,
            f"prophet/{_model_key(connector_id)}",
        )
        return joblib.load(io.BytesIO(data))
    except Exception as exc:
        log.debug("No Prophet model for %s: %s", connector_id, exc)
        return None

def predict_next(
    model,
    horizon_hours: int | None = None,
) -> dict:
    """
    Forecast the next N hours.
    Returns {"forecast": float, "lower": float, "upper": float, "periods": int}
    """
    cfg     = get_settings().ml
    periods = horizon_hours or cfg.forecast_horizon_hours
    future  = model.make_future_dataframe(periods=periods, freq="H")
    forecast = model.predict(future).tail(periods)
    last     = forecast.iloc[-1]
    return {
        "forecast":    float(last["yhat"]),
        "lower":       float(last["yhat_lower"]),
        "upper":       float(last["yhat_upper"]),
        "periods":     periods,
        "horizon_h":   periods,
    }

def is_outside_bounds(actual: float, forecast_result: dict) -> bool:
    """Return True if actual rows fall outside the forecast confidence interval."""
    return actual < forecast_result["lower"] or actual > forecast_result["upper"]
