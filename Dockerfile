# =============================================================================
# WAP Reconciliation Framework — Production Image
# Base: python:3.12-slim (Debian Bookworm, minimal attack surface)
# Runs as non-root user `wap` (uid 1000) — mandatory for any public framework.
# =============================================================================

FROM python:3.12-slim AS base

# ── System dependencies ────────────────────────────────────────────────────
# libpq-dev: required by psycopg2 at runtime (Postgres C client library).
# gcc + python3-dev: needed to compile psycopg2 if the binary wheel is
#   unavailable for this platform (e.g. linux/arm64 on M2). Using
#   psycopg2-binary in requirements.txt avoids compilation in most cases,
#   but we keep build deps here for correctness on all platforms.
RUN apt-get update && apt-get install -y --no-install-recommends \
        libpq-dev \
        gcc \
        python3-dev \
    && rm -rf /var/lib/apt/lists/*

# ── Non-root user ─────────────────────────────────────────────────────────
RUN useradd --create-home --uid 1000 --shell /bin/bash wap

# ── Working directory ─────────────────────────────────────────────────────
WORKDIR /app

# ── Dependencies (own layer — invalidated only when requirements change) ──
# Copy requirements first so Docker cache is not busted by source changes.
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# ── Application source ────────────────────────────────────────────────────
COPY . .

# ── Ownership ─────────────────────────────────────────────────────────────
RUN chown -R wap:wap /app

# ── Switch to non-root ────────────────────────────────────────────────────
USER wap

# ── Python path ───────────────────────────────────────────────────────────
# Ensures `from core.models import ...` works without installing the package.
ENV PYTHONPATH=/app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# ── Default command: run migrations then drop to shell ────────────────────
# Override in docker-compose for specific services (app, test, migrate).
CMD ["python", "-m", "db.migrations.run_migrations"]


# =============================================================================
# Test stage — extends base with dev dependencies
# Not shipped in production images; used only in docker-compose test service.
# =============================================================================

FROM base AS test

USER root
COPY requirements-dev.txt ./
RUN pip install --no-cache-dir -r requirements-dev.txt
USER wap

CMD ["pytest", "tests/", "-v", "--tb=short"]