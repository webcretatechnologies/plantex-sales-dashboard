DEFAULT_DASHBOARD_VIEW_TYPES = (
    "ceo-dashboard",
)

# Increment this version whenever you want to invalidate all dashboard API caches.
DASHBOARD_PAYLOAD_CACHE_VERSION = "v8"
DASHBOARD_SCHEMA_VERSION = "v5"

# In-memory payload cache TTL tuning
DASHBOARD_CACHE_TTL_LITE_SECONDS = 604800   # 7 days
DASHBOARD_CACHE_TTL_FULL_SECONDS = 604800   # 7 days

# Cache schema marker to prevent stale-key collisions after key format changes.
DASHBOARD_CACHE_SCHEMA_VERSION = 11

DEFAULT_WARMUP_FILTER_SETS = (
    {},
    {"date_range": "last_7_days"},
    {"date_range": "last_15_days"},
    {"date_range": "last_3_months"},
    {"date_range": "last_6_months"},
    {"date_range": "last_1_year"},
    {"platform": "Amazon"},
    {"platform": "Flipkart"},
    {"platform": "Amazon", "date_range": "last_7_days"},
    {"platform": "Flipkart", "date_range": "last_7_days"},
    {"platform": "Amazon", "date_range": "last_3_months"},
    {"platform": "Flipkart", "date_range": "last_3_months"},
    {"platform": "Amazon", "date_range": "last_6_months"},
    {"platform": "Flipkart", "date_range": "last_6_months"},
    {"platform": "Amazon", "date_range": "last_1_year"},
    {"platform": "Flipkart", "date_range": "last_1_year"},
)
