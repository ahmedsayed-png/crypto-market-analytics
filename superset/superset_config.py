# Apache Superset Configuration
import os

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "crypto-market-analytics-superset-secret-key-2026")
