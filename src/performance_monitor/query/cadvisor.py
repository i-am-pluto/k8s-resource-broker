from __future__ import annotations

CADVISOR_DEFAULTS: dict[str, dict[str, str]] = {
    "cpu": {
        "usage": (
            'sum(rate(container_cpu_usage_seconds_total'
            '{namespace="$namespace",pod="$pod",container="$container"}[5m]))'
        ),
        "configured": (
            'sum('
            '  container_spec_cpu_quota{namespace="$namespace",pod="$pod",container="$container"}'
            '  / on(namespace,pod,container) '
            '  container_spec_cpu_period{namespace="$namespace",pod="$pod",container="$container"}'
            ')'
        ),
    },
    "memory": {
        "usage": (
            'sum(container_memory_usage_bytes'
            '{namespace="$namespace",pod="$pod",container="$container"})'
        ),
        "configured": (
            'sum(container_spec_memory_limit_bytes'
            '{namespace="$namespace",pod="$pod",container="$container"})'
        ),
    },
}


def get_cadvisor_default(field: str, query_type: str) -> str | None:
    per_field = CADVISOR_DEFAULTS.get(field)
    if per_field is None:
        return None
    return per_field.get(query_type)
