#!/bin/bash
set -e

superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin 2>/dev/null || true
superset init

superset set-database-uri -d 'OpenHexa DuckDB' -u 'duckdb:////app/superset_home/openhexa.duckdb?enable_object_cache=true&parquet_metadata_cache=true&enable_http_metadata_cache=true&temp_directory=/app/superset_home/duck_tmp&memory_limit=4GB&threads=4' 2>/dev/null || true
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload
