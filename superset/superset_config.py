# Apache Superset Configuration
import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "crypto-market-analytics-superset-secret-key-2026")

# Superset metadata store (dashboards, users, charts)
SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}

ENABLE_PROXY_FIX = False
