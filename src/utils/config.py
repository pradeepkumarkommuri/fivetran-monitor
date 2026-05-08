"""Centralised configuration using Pydantic Settings v2."""
from __future__ import annotations
from functools import lru_cache
from pathlib import Path
from typing import Optional
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

_ENV_FILE = str(Path(__file__).parents[2] / ".env")
_CFG = SettingsConfigDict(env_file=_ENV_FILE, env_file_encoding="utf-8",
                          extra="ignore", populate_by_name=True)

class FivetranSettings(BaseSettings):
    api_key: str         = Field(...,   alias="FIVETRAN_API_KEY")
    api_secret: str      = Field(...,   alias="FIVETRAN_API_SECRET")
    base_url: str        = Field("https://api.fivetran.com/v1", alias="FIVETRAN_BASE_URL")
    request_timeout: int = Field(30,    alias="FIVETRAN_REQUEST_TIMEOUT")
    max_retries: int     = Field(3,     alias="FIVETRAN_MAX_RETRIES")
    model_config = _CFG

class NtfySettings(BaseSettings):
    topic: str        = Field("fivetran-alerts", alias="NTFY_TOPIC")
    server: str       = Field("https://ntfy.sh",  alias="NTFY_SERVER")
    access_token: str = Field("", alias="NTFY_ACCESS_TOKEN")
    username: str     = Field("", alias="NTFY_USERNAME")
    password: str     = Field("", alias="NTFY_PASSWORD")
    @property
    def auth_mode(self) -> str:
        if self.access_token: return "token"
        if self.username and self.password: return "basic"
        return "none"
    model_config = _CFG

class EmailSettings(BaseSettings):
    enabled: bool       = Field(False, alias="EMAIL_ENABLED")
    smtp_host: str      = Field("smtp.office365.com", alias="SMTP_HOST")
    smtp_port: int      = Field(587,   alias="SMTP_PORT")
    smtp_username: str  = Field("",    alias="SMTP_USERNAME")
    smtp_password: str  = Field("",    alias="SMTP_PASSWORD")
    from_addr: str      = Field("",    alias="EMAIL_FROM")
    use_tls: bool       = Field(True,  alias="EMAIL_USE_TLS")
    to_addrs: list[str] = Field(default_factory=list, alias="EMAIL_TO")
    @field_validator("to_addrs", mode="before")
    @classmethod
    def parse_to(cls, v):
        if isinstance(v, str): return [e.strip() for e in v.split(",") if e.strip()]
        return v or []
    @property
    def is_outlook(self) -> bool:
        return "office365" in self.smtp_host or "outlook.com" in self.smtp_host
    model_config = _CFG

class StorageSettings(BaseSettings):
    endpoint_url: str    = Field("http://minio:9000",        alias="MINIO_ENDPOINT")
    access_key: str      = Field("minioadmin",               alias="MINIO_ACCESS_KEY")
    secret_key: str      = Field("minioadmin",               alias="MINIO_SECRET_KEY")
    bucket: str          = Field("fivetran-monitor",         alias="MINIO_BUCKET")
    raw_prefix: str      = Field("raw/",                     alias="MINIO_RAW_PREFIX")
    features_prefix: str = Field("features/",                alias="MINIO_FEATURES_PREFIX")
    models_prefix: str   = Field("models/",                  alias="MINIO_MODELS_PREFIX")
    model_config = _CFG

class DatabaseSettings(BaseSettings):
    url: str          = Field(
        "postgresql+psycopg2://monitor:monitor@postgres:5432/fivetran_monitor",
        alias="DATABASE_URL")
    pool_size: int    = Field(5,     alias="DB_POOL_SIZE")
    max_overflow: int = Field(10,    alias="DB_MAX_OVERFLOW")
    echo: bool        = Field(False, alias="DB_ECHO")
    model_config = _CFG

class SparkSettings(BaseSettings):
    master: str              = Field("k8s://https://kubernetes:443", alias="SPARK_MASTER")
    app_name: str            = Field("fivetran-monitor",             alias="SPARK_APP_NAME")
    executor_instances: int  = Field(2,    alias="SPARK_EXECUTOR_INSTANCES")
    executor_cores: int      = Field(2,    alias="SPARK_EXECUTOR_CORES")
    executor_memory: str     = Field("2g", alias="SPARK_EXECUTOR_MEMORY")
    driver_memory: str       = Field("1g", alias="SPARK_DRIVER_MEMORY")
    image: str               = Field("your-registry/fivetran-monitor-spark:latest",
                                     alias="SPARK_IMAGE")
    model_config = _CFG

class MLSettings(BaseSettings):
    anomaly_contamination: float  = Field(0.05,  alias="ML_ANOMALY_CONTAMINATION")
    anomaly_n_estimators: int     = Field(100,   alias="ML_ANOMALY_N_ESTIMATORS")
    anomaly_threshold: float      = Field(-0.1,  alias="ML_ANOMALY_THRESHOLD")
    forecast_horizon_hours: int   = Field(24,    alias="ML_FORECAST_HORIZON_HOURS")
    forecast_interval_width: float = Field(0.95, alias="ML_FORECAST_INTERVAL_WIDTH")
    training_lookback_days: int   = Field(90,    alias="ML_TRAINING_LOOKBACK_DAYS")
    min_training_samples: int     = Field(30,    alias="ML_MIN_TRAINING_SAMPLES")
    retrain_every_hours: int      = Field(24,    alias="ML_RETRAIN_EVERY_HOURS")
    model_config = _CFG

class AlertThresholds(BaseSettings):
    drop_pct: float          = Field(20.0,  alias="DROP_THRESHOLD_PCT")
    spike_pct: float         = Field(200.0, alias="SPIKE_THRESHOLD_PCT")
    zero_rows: bool          = Field(True,  alias="ZERO_ROWS_ALERT")
    delay_multiplier: float  = Field(2.0,   alias="DELAY_MULTIPLIER")
    broken_statuses: set[str] = Field(
        default_factory=lambda: {"error","broken","incomplete","failed"},
        alias="BROKEN_STATUSES")
    dedup_window_minutes: int = Field(60, alias="ALERT_DEDUP_WINDOW_MINUTES")
    @field_validator("broken_statuses", mode="before")
    @classmethod
    def parse_statuses(cls, v):
        if isinstance(v, str): return {s.strip().lower() for s in v.split(",") if s.strip()}
        return v or {"error","broken","incomplete","failed"}
    model_config = _CFG

class SchedulerSettings(BaseSettings):
    interval_minutes: int = Field(60,   alias="SCHEDULE_INTERVAL_MINUTES")
    run_on_start: bool    = Field(True,  alias="SCHEDULE_RUN_ON_START")
    model_config = _CFG

class Settings(BaseSettings):
    fivetran:   FivetranSettings   = Field(default_factory=FivetranSettings)
    ntfy:       NtfySettings       = Field(default_factory=NtfySettings)
    email:      EmailSettings      = Field(default_factory=EmailSettings)
    storage:    StorageSettings    = Field(default_factory=StorageSettings)
    database:   DatabaseSettings   = Field(default_factory=DatabaseSettings)
    spark:      SparkSettings      = Field(default_factory=SparkSettings)
    ml:         MLSettings         = Field(default_factory=MLSettings)
    thresholds: AlertThresholds    = Field(default_factory=AlertThresholds)
    scheduler:  SchedulerSettings  = Field(default_factory=SchedulerSettings)
    env: str = Field("production", alias="APP_ENV")
    model_config = SettingsConfigDict(env_file=_ENV_FILE, env_file_encoding="utf-8",
                                      extra="ignore", populate_by_name=True)

@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Cached singleton — use everywhere."""
    return Settings()
