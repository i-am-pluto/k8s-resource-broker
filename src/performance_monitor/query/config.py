from dataclasses import dataclass


@dataclass
class PromQLRunnerConfig:
    default_lookback_days: int = 10
    retry_attempts: int = 3
    retry_backoff_seconds: float = 2.0
    range_step: str = "30s"
