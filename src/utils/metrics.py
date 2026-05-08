"""Prometheus metrics registry."""
from prometheus_client import Counter, Gauge, Histogram, start_http_server

# Ingestion
connectors_total = Gauge("fivetran_connectors_total", "Total connectors monitored")
ingestion_duration = Histogram(
    "fivetran_ingestion_duration_seconds", "Ingest job duration"
)
ingestion_errors = Counter(
    "fivetran_ingestion_errors_total", "Ingestion errors", ["connector_id"]
)

# ML
anomaly_score = Gauge(
    "fivetran_anomaly_score", "Latest anomaly score", ["connector_id", "table"]
)
forecast_deviation = Gauge(
    "fivetran_forecast_deviation_pct", "Actual vs forecast %", ["connector_id"]
)
model_train_duration = Histogram(
    "fivetran_model_train_seconds", "Model training duration", ["model_type"]
)
model_last_trained = Gauge(
    "fivetran_model_last_trained_timestamp",
    "Unix ts of last training",
    ["connector_id"],
)

# Alerts
alerts_fired = Counter(
    "fivetran_alerts_fired_total", "Alerts fired", ["severity", "type", "channel"]
)
alerts_suppressed = Counter(
    "fivetran_alerts_suppressed_total", "Deduplicated alerts", ["reason"]
)

# Connector health
connectors_broken = Gauge("fivetran_connectors_broken", "Currently broken connectors")
connectors_delayed = Gauge("fivetran_connectors_delayed", "Currently delayed connectors")
connectors_paused = Gauge("fivetran_connectors_paused", "Currently paused connectors")


def start_metrics_server(port: int = 9090) -> None:
    start_http_server(port)
