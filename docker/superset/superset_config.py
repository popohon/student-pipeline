import os

# ─── Security ─────────────────────────────────────────────────────────────────
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "onlenpajak_superset_secret_key_2024")

# ─── Superset Metadata DB (stored in PostgreSQL) ──────────────────────────────
SQLALCHEMY_DATABASE_URI = (
    f"postgresql+psycopg2://"
    f"{os.environ.get('SUPERSET_DB_USER', 'superset')}:"
    f"{os.environ.get('SUPERSET_DB_PASSWORD', 'superset')}@"
    f"{os.environ.get('POSTGRES_HOST', 'postgres')}:"
    f"{os.environ.get('POSTGRES_PORT', '5432')}/"
    f"{os.environ.get('SUPERSET_DB_NAME', 'superset_db')}"
)

# ─── Feature Flags ────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_RBAC": False,
}

# ─── CSRF / Auth (relaxed for local dev) ──────────────────────────────────────
WTF_CSRF_ENABLED = False
TALISMAN_ENABLED = False

# ─── CORS (allow dashboard setup script to call REST API) ─────────────────────
ENABLE_CORS = True
CORS_OPTIONS = {
    "supports_credentials": True,
    "allow_headers": ["*"],
    "resources": ["*"],
    "origins": ["http://localhost:8088", "http://superset:8088"],
}

# ─── Cache ────────────────────────────────────────────────────────────────────
CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}
DATA_CACHE_CONFIG = {"CACHE_TYPE": "SimpleCache"}

# ─── Row limits ───────────────────────────────────────────────────────────────
ROW_LIMIT = 50000
