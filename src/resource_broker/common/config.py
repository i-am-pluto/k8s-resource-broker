from __future__ import annotations

from enum import StrEnum
from pathlib import Path
from typing import Literal, Self

from pydantic import PostgresDsn, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class LogLevel(StrEnum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class WebhookMode(StrEnum):
    ADMISSION = "admission"
    POST_CREATE = "post-create"
    BOTH = "both"


class MetricsAdapterType(StrEnum):
    PROMETHEUS = "prometheus"
    THANOS = "thanos"
    VICTORIA_METRICS = "victoria_metrics"
    MIMIR = "mimir"
    KUBECOST = "kubecost"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        env_prefix="BROKER_",
        case_sensitive=False,
    )

    # ── Application ──────────────────────────────────────────────────────
    service_name: str = "k8s-resource-broker"
    log_level: LogLevel = LogLevel.INFO
    environment: Literal["development", "staging", "production"] = "development"

    # ── API / Webhook ────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8080
    api_workers: int = 1
    api_root_path: str = ""

    webhook_mode: WebhookMode = WebhookMode.BOTH
    webhook_fail_open: bool = True
    webhook_timeout_seconds: int = 10

    # ── PostgreSQL ───────────────────────────────────────────────────────
    database_url: PostgresDsn = PostgresDsn("postgresql+asyncpg://broker:broker@localhost:5432/broker")

    # ── Kubernetes ───────────────────────────────────────────────────────
    k8s_in_cluster: bool = True
    k8s_namespace: str = "resource-broker"
    k8s_config_file: Path | None = None
    watch_namespace: str = ""  # empty = watch all namespaces cluster-wide

    # ── Metrics (PromQL backend) ───────────────────────────────────────
    metrics_adapter_type: MetricsAdapterType = MetricsAdapterType.PROMETHEUS
    metrics_url: str = "http://prometheus:9090"
    metrics_timeout_seconds: int = 30

    # ── Scraper ──────────────────────────────────────────────────────────
    scraper_interval_seconds: int = 60
    scraper_lookback_minutes: int = 5
    pressure_threshold: float = 0.85
    performance_monitor_namespaces: list[str] = []

    # ── Profiles ─────────────────────────────────────────────────────────
    default_profile_name: str = "default"
    profile_annotation_key: str = "resource-broker/profile"
    profile_annotation_namespace: str = "resource-broker"
    default_profile_mode: str = "recommendation"

    # ── TLS (webhook) ────────────────────────────────────────────────────
    tls_cert_file: Path | None = None
    tls_key_file: Path | None = None

    @model_validator(mode="after")
    def _validate_database_scheme(self) -> Self:
        dsn = str(self.database_url)
        if "+asyncpg" not in dsn:
            self.database_url = PostgresDsn(dsn.replace("postgresql://", "postgresql+asyncpg://"))
        return self


settings = Settings()
