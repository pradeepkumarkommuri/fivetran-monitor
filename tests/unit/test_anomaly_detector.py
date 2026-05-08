"""Unit tests for Isolation Forest anomaly detector."""
import numpy as np
import pandas as pd
import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def sample_df():
    rng = np.random.default_rng(42)
    n   = 60
    return pd.DataFrame({
        "rows_updated":     rng.integers(800, 1200, n).tolist(),
        "rows_pct_change":  rng.uniform(-5, 5, n).tolist(),
        "z_score":          rng.uniform(-1, 1, n).tolist(),
        "rolling_mean_7d":  [1000.0] * n,
        "rolling_std_7d":   [50.0] * n,
        "rolling_mean_30d": [1000.0] * n,
        "rolling_std_30d":  [50.0] * n,
        "rows_lag1":        rng.integers(800, 1200, n).tolist(),
        "rows_lag2":        rng.integers(800, 1200, n).tolist(),
        "rows_lag3":        rng.integers(800, 1200, n).tolist(),
        "day_of_week":      rng.integers(1, 7, n).tolist(),
        "hour_of_day":      rng.integers(0, 23, n).tolist(),
        "is_weekend":       rng.integers(0, 1, n).tolist(),
    })

def test_train_returns_pipeline(sample_df):
    from src.ml.anomaly_detector import train, FEATURE_COLS
    with patch("src.ml.anomaly_detector.storage") as mock_storage:
        mock_storage.put_bytes = MagicMock()
        mock_storage.get_settings = MagicMock()
        with patch("src.ml.anomaly_detector.get_settings") as mock_cfg:
            mock_cfg.return_value.ml.anomaly_contamination = 0.05
            mock_cfg.return_value.ml.anomaly_n_estimators  = 10
            mock_cfg.return_value.ml.min_training_samples  = 30
            mock_cfg.return_value.storage.models_prefix    = "models/"
            pipeline = train("conn_1", "schema.table", sample_df)
    assert pipeline is not None

def test_train_skips_insufficient_data():
    from src.ml.anomaly_detector import train
    small_df = pd.DataFrame({"rows_updated": [100, 200]})
    with patch("src.ml.anomaly_detector.get_settings") as mock_cfg:
        mock_cfg.return_value.ml.min_training_samples = 30
        result = train("conn_1", "schema.table", small_df)
    assert result is None

def test_predict_returns_expected_keys(sample_df):
    from src.ml.anomaly_detector import train, predict
    with patch("src.ml.anomaly_detector.storage") as mock_storage:
        mock_storage.put_bytes = MagicMock()
        with patch("src.ml.anomaly_detector.get_settings") as mock_cfg:
            mock_cfg.return_value.ml.anomaly_contamination  = 0.05
            mock_cfg.return_value.ml.anomaly_n_estimators   = 10
            mock_cfg.return_value.ml.min_training_samples   = 30
            mock_cfg.return_value.ml.anomaly_threshold      = -0.1
            mock_cfg.return_value.storage.models_prefix     = "models/"
            pipeline = train("conn_1", "schema.table", sample_df)
    row = sample_df.iloc[0].to_dict()
    result = predict(pipeline, row)
    assert {"is_anomaly", "score", "confidence", "threshold"} <= result.keys()
    assert isinstance(result["is_anomaly"], bool)
