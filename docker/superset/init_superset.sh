#!/bin/bash
set -e

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Initializing Apache Superset..."
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Wait for PostgreSQL to be ready
until python -c "import psycopg2; psycopg2.connect(
    host='${POSTGRES_HOST:-postgres}',
    port=${POSTGRES_PORT:-5432},
    dbname='${SUPERSET_DB_NAME:-superset_db}',
    user='${SUPERSET_DB_USER:-superset}',
    password='${SUPERSET_DB_PASSWORD:-superset}'
)" 2>/dev/null; do
  echo "  Waiting for PostgreSQL..."
  sleep 3
done
echo "  ✓ PostgreSQL ready"

# Upgrade Superset metadata DB
superset db upgrade

# Create admin user (idempotent - fails silently if exists)
superset fab create-admin \
    --username admin \
    --firstname Admin \
    --lastname User \
    --email admin@onlenpajak.com \
    --password admin 2>/dev/null || echo "  (Admin user already exists)"

# Initialize roles and permissions
superset init

echo "  ✓ Superset initialized"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  Superset ready at http://localhost:8088"
echo "  Login: admin / admin"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

# Start Superset
exec gunicorn \
    --bind "0.0.0.0:8088" \
    --access-logfile "-" \
    --error-logfile "-" \
    --workers 2 \
    --worker-class gthread \
    --threads 20 \
    --timeout 120 \
    --limit-request-line 0 \
    --limit-request-field_size 0 \
    "superset.app:create_app()"
