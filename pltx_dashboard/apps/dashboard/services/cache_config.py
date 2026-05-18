DEFAULT_DASHBOARD_VIEW_TYPES = (
    "business-dashboard",
    "ceo-dashboard",
    "category-dashboard",
)

DASHBOARD_PAYLOAD_CACHE_VERSION = 15

# In-memory payload cache TTL tuning
DASHBOARD_CACHE_TTL_LITE_SECONDS = 60 * 60 * 6   # 6 hours
DASHBOARD_CACHE_TTL_FULL_SECONDS = 60 * 45       # 45 minutes

# Cache schema marker to prevent stale-key collisions after key format changes.
DASHBOARD_CACHE_SCHEMA_VERSION = 2

DEFAULT_WARMUP_FILTER_SETS = (
    {},
    {"date_range": "last_7_days"},
    {"date_range": "last_15_days"},
    {"platform": "Amazon"},
    {"platform": "Flipkart"},
    {"platform": "Amazon", "date_range": "last_7_days"},
    {"platform": "Flipkart", "date_range": "last_7_days"},
)
