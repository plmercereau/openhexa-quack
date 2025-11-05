#!/bin/bash
set -e

superset db upgrade
superset fab create-admin --username admin --firstname Admin --lastname User --email admin@example.com --password admin 2>/dev/null || true
superset init
superset set-database-uri -d 'OpenHexa DuckDB' -u 'duckdb:///:memory:' 2>/dev/null || true
exec superset run -h 0.0.0.0 -p 8088 --with-threads --reload

