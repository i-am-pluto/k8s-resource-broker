from __future__ import annotations

_MEMORY_MULTIPLIERS = {
    "K": 1e3,
    "M": 1e6,
    "G": 1e9,
    "T": 1e12,
    "KI": 1024,
    "MI": 1024**2,
    "GI": 1024**3,
    "TI": 1024**4,
}


def parse_quantity(value: str | int | float, *, is_cpu: bool = False) -> float:
    """Parse a Kubernetes resource quantity into a numeric value.

    CPU quantities are returned in cores (e.g. "500m" -> 0.5, "2" -> 2.0).
    Memory quantities are returned in bytes, honoring the K/M/G/T (decimal)
    and Ki/Mi/Gi/Ti (binary) suffixes (e.g. "1Gi" -> 1073741824.0).
    Numeric (`int`/`float`) input passes through unchanged as a float.
    """
    if isinstance(value, (int, float)):
        return float(value)

    if is_cpu:
        raw = value.lower().strip()
        if raw.endswith("m"):
            return float(raw[:-1]) / 1000.0
        return float(raw)

    raw = value.upper().strip()
    for suffix, mult in sorted(_MEMORY_MULTIPLIERS.items(), key=lambda x: -len(x[0])):
        if raw.endswith(suffix):
            return float(raw[: -len(suffix)]) * mult
    return float(raw)
