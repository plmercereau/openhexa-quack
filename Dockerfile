FROM apache/superset:latest

USER root

# Install dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential curl && \
    rm -rf /var/lib/apt/lists/* && \
    sed -i 's/include-system-site-packages = false/include-system-site-packages = true/' /app/.venv/pyvenv.cfg

# Install duckdb_openhexa package (dependencies from pyproject.toml)
COPY pyproject.toml /app/
COPY duckdb_openhexa /app/duckdb_openhexa
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir /app

# Copy Superset customization (config + init script)
COPY superset /app/superset
RUN chmod +x /app/superset/init.sh

USER superset
WORKDIR /app
ENV SUPERSET_CONFIG_PATH=/app/superset/superset_config.py

CMD ["/app/superset/init.sh"]
