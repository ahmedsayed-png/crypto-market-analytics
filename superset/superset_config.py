# Apache Superset Configuration Placeholder
import os

FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
}

ENABLE_PROXY_FIX = True
SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
