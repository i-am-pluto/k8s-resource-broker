# ── Build stage ───────────────────────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app

ENV \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_COMPILE_BYTECODE=1

RUN pip install --no-cache-dir uv

COPY pyproject.toml .
RUN uv export --no-dev --format=requirements-txt > requirements.txt
RUN pip install --no-cache-dir --target=/deps -r requirements.txt

# ── Runtime stage ─────────────────────────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app

ENV \
    BROKER_ENVIRONMENT=production \
    BROKER_LOG_LEVEL=INFO \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN groupadd -r broker && useradd -r -g broker -d /app -s /sbin/nologin broker

COPY --from=builder /deps /usr/local/lib/python3.12/site-packages
COPY src/ /app/src/
COPY pyproject.toml /app/

RUN pip install --no-cache-dir -e /app 2>/dev/null || true

RUN chown -R broker:broker /app
USER broker

EXPOSE 8080

ENTRYPOINT ["python", "-m", "resource_broker"]

CMD ["serve"]
