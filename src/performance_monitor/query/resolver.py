from __future__ import annotations

import re

from generated.models import MetricConfig

from .cadvisor import get_cadvisor_default

_TEMPLATE_RE = re.compile(r"\$\{?(\w+)\}?")


def _substitute(promql: str, context: dict[str, str]) -> str:
    def _replacer(m: re.Match[str]) -> str:
        key = m.group(1)
        return context.get(key, m.group(0))

    return _TEMPLATE_RE.sub(_replacer, promql)


class QueryResolver:
    def resolve(
        self,
        metric_config: MetricConfig | None,
        field: str,
        query_type: str,
        context: dict[str, str],
    ) -> str | None:
        if metric_config is not None and metric_config.query:
            return _substitute(metric_config.query, context)

        if metric_config is not None and metric_config.metric:
            return self._build_from_metric(metric_config, context)

        default = get_cadvisor_default(field, query_type)
        if default is not None:
            return _substitute(default, context)
        return None

    def _build_from_metric(
        self,
        metric_config: MetricConfig,
        context: dict[str, str],
    ) -> str:
        metric = metric_config.metric or ""
        label_map = metric_config.label_map or {}

        labels: list[str] = []
        for label_name, value_or_key in label_map.items():
            label_value = context.get(value_or_key, value_or_key)
            labels.append(f'{label_name}="{label_value}"')

        if labels:
            return f'{metric}{{{",".join(labels)}}}'
        return metric
