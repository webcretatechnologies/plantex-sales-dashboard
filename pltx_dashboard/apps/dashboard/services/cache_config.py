DEFAULT_DASHBOARD_VIEW_TYPES = (
    "business-dashboard",
    "ceo-dashboard",
    "category-dashboard",
)

DASHBOARD_PAYLOAD_CACHE_VERSION = 13

DEFAULT_WARMUP_FILTER_SETS = (
    {},
    {"date_range": "last_7_days"},
    {"date_range": "last_15_days"},
    {"platform": "Amazon"},
    {"platform": "Flipkart"},
    {"platform": "Amazon", "date_range": "last_7_days"},
    {"platform": "Flipkart", "date_range": "last_7_days"},
)
