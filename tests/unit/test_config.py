from __future__ import annotations

from resource_broker.config import LogLevel, Settings, WebhookMode


def test_default_settings() -> None:
    s = Settings(_env_file=None)  # noqa: bypass .env so we test coded defaults, not local dev overrides
    assert s.service_name == "k8s-resource-broker"
    assert s.log_level == LogLevel.INFO
    assert s.webhook_mode == WebhookMode.BOTH
    assert s.webhook_fail_open is True
    assert s.api_port == 8080


def test_database_url_enforces_asyncpg() -> None:
    s = Settings(_env_file=None)  # noqa
    # The model_validator should ensure +asyncpg is present
    assert "+asyncpg" in str(s.database_url)


def test_annotation_key() -> None:
    s = Settings(_env_file=None)  # noqa
    assert s.profile_annotation_key == "resource-broker/profile"
