FROM docker.io/library/python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1
RUN apt-get update && apt-get install -y --no-install-recommends git ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 codehub && useradd --uid 10001 --gid 10001 --no-create-home codehub
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN pip install --no-cache-dir uv==0.8.22 && uv sync --locked --no-dev --no-install-project
COPY src/codehub/ ./src/codehub/
COPY migrations/ ./migrations/
COPY docs/ ./docs/
RUN uv sync --locked --no-dev --no-editable \
    && mkdir -p /var/lib/codehub && chown -R 10001:10001 /var/lib/codehub
ENV PATH="/app/.venv/bin:$PATH" CODEHUB_DATA_DIR=/var/lib/codehub
USER 10001:10001
EXPOSE 8080
CMD ["python", "-m", "codehub.api"]
