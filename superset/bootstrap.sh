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

    from superset.connectors.sqla.models import SqlaTable
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    import json

    dw = db.session.query(Database).filter_by(database_name=name).first()
    if dw:
        target_tables = [
            'vw_market_overview',
            'vw_coin_analysis',
            'fact_klines_min',
            'fact_klines_daily',
            'fact_predictions'
        ]
        registered_tables = {}
        for tbl_name in target_tables:
            sqla_table = db.session.query(SqlaTable).filter_by(database_id=dw.id, table_name=tbl_name).first()
            if not sqla_table:
                sqla_table = SqlaTable(table_name=tbl_name, database=dw, schema='public')
                db.session.add(sqla_table)
                db.session.flush()
                try:
                    sqla_table.fetch_metadata()
                except Exception as e:
                    print(f"Metadata fetch warning for {tbl_name}: {e}")
            registered_tables[tbl_name] = sqla_table

        dash_title = 'Crypto Market Analytics Dashboard'
        dash = db.session.query(Dashboard).filter_by(dashboard_title=dash_title).first()
        if not dash:
            dash = Dashboard(dashboard_title=dash_title, slug='crypto-market-analytics', published=True)
            db.session.add(dash)
            db.session.flush()

        t_overview = registered_tables.get('vw_market_overview')
        t_coin = registered_tables.get('vw_coin_analysis')
        if t_overview:
            c1_title = 'Cryptocurrency Market Overview'
            slice1 = db.session.query(Slice).filter_by(slice_name=c1_title).first()
            if not slice1:
                params1 = {
                    'viz_type': 'table',
                    'datasource': f'{t_overview.id}__table',
                    'all_columns': ['symbol', 'min_bars_count', 'avg_close_price', 'total_volume', 'earliest_record', 'latest_record'],
                    'row_limit': 100,
                }
                slice1 = Slice(slice_name=c1_title, viz_type='table', datasource_type='table', datasource_id=t_overview.id, params=json.dumps(params1))
                db.session.add(slice1)
                db.session.flush()
            if slice1 not in dash.slices:
                dash.slices.append(slice1)

        if t_coin:
            c2_title = 'Crypto Kline Price Trend'
            slice2 = db.session.query(Slice).filter_by(slice_name=c2_title).first()
            if not slice2:
                params2 = {
                    'viz_type': 'echarts_timeseries_line',
                    'datasource': f'{t_coin.id}__table',
                    'granularity_sqla': 'open_time',
                    'time_grain_sqla': 'PT1M',
                    'metrics': [{'expressionType': 'SQL', 'sqlExpression': 'AVG(close)', 'label': 'Avg Close Price'}],
                    'groupby': ['symbol'],
                    'row_limit': 1000,
                }
                slice2 = Slice(slice_name=c2_title, viz_type='echarts_timeseries_line', datasource_type='table', datasource_id=t_coin.id, params=json.dumps(params2))
                db.session.add(slice2)
                db.session.flush()
            if slice2 not in dash.slices:
                dash.slices.append(slice2)

        db.session.commit()
        print("Superset datasets and dashboard seeded successfully.")
PYEOF

echo "Crypto Superset bootstrap complete."
