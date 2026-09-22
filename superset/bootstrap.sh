#!/usr/bin/env bash
# ==============================================================================
# Apache Superset Initialization Script
# Runs once via superset-init container
# ==============================================================================
set -e

echo "Running Superset DB migrations..."
superset db upgrade

echo "Ensuring admin user exists..."
superset fab create-admin \
    --username "${SUPERSET_ADMIN_USER:-admin}" \
    --firstname Admin \
    --lastname User \
    --email "${SUPERSET_ADMIN_EMAIL:-admin@example.com}" \
    --password "${SUPERSET_ADMIN_PASSWORD:-admin}" || echo "Admin user already exists, continuing."

echo "Initializing Superset roles/permissions..."
superset init

echo "Registering the Crypto Data Warehouse database connection..."
python3 - <<'PYEOF'
import os
from superset.app import create_app

app = create_app()
with app.app_context():
    from superset import db
    from superset.models.core import Database

    uri = (
        f"postgresql+psycopg2://{os.environ.get('POSTGRES_DW_USER', 'crypto_admin')}:"
        f"{os.environ.get('POSTGRES_DW_PASSWORD', 'crypto_secure_password_123')}@postgres-dw:5432/"
        f"{os.environ.get('POSTGRES_DW_DB', 'crypto_dw')}"
    )
    name = "Crypto Data Warehouse"

    existing = db.session.query(Database).filter_by(database_name=name).first()
    if existing:
        print(f"'{name}' connection already exists, skipping.")
    else:
        database = Database(database_name=name, sqlalchemy_uri=uri)
        db.session.add(database)
        db.session.commit()
        print(f"Created '{name}' connection -> {uri}")
PYEOF

echo "Crypto Superset bootstrap complete."
