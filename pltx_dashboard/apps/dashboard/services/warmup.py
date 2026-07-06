import hashlib
import json
import logging
import time
from copy import deepcopy

from django.conf import settings
from django.core.cache import cache
from django.db.models import Sum

from apps.dashboard.models import (
    DashboardMappingFilterDailySummary,
    FlipkartProcessedDashboardData,
    ProcessedDashboardData,
    SpendData,
)
from apps.dashboard.services.analytics_services_orm_pipeline import (
    get_available_filters_orm_cached,
    run_orm_computation,
)
from apps.dashboard.services.cache_config import (
    DASHBOARD_CACHE_SCHEMA_VERSION,
    DASHBOARD_CACHE_TTL_LITE_SECONDS,
    DASHBOARD_PAYLOAD_CACHE_VERSION,
    DEFAULT_DASHBOARD_VIEW_TYPES,
    DEFAULT_WARMUP_FILTER_SETS,
)
from apps.dashboard.services.filters import (
    LIST_FILTER_FIELDS,
    apply_dashboard_entity_filters,
    cache_filter_string,
    normalize_payload_filters,
)
from apps.dashboard.services.materialized_cache import (
    get_materialized_summary,
    store_materialized_summary,
)

logger = logging.getLogger(__name__)


DEFAULT_WARMUP_SECTION_SCOPES = (
    ("overview", "kpis"),
    ("visuals", "full"),
    ("details", "full"),
)

KPI_ONLY_WARMUP_SECTION_SCOPES = (
    ("overview", "kpis"),
)

DEFAULT_MAPPING_KPI_WARMUP_FIELDS = (
    "category_manager",
    "series_name",
    "material",
    "size",
    "finish",
)

DEFAULT_MAPPING_KPI_WARMUP_DATE_RANGES = (
    "last_3_months",
    "last_6_months",
    "last_1_year",
)

SHARED_DASHBOARD_CACHE_VIEW_TYPES = {
    "business-dashboard",
    "category-dashboard",
    "ceo-dashboard",
}


def _dashboard_view_name(view_type):
    view_type = str(view_type or "").replace("-dashboard", "")
    return view_type or "business"


def _cache_view_type(view_type):
    view_type = str(view_type or "shared")
    if view_type in SHARED_DASHBOARD_CACHE_VIEW_TYPES:
        return "ceo-dashboard"
    return view_type


def _is_current_payload(payload):
    if not isinstance(payload, dict):
        return False
    try:
        schema_version = int(payload.get("_payload_schema_version") or 0)
    except (TypeError, ValueError):
        return False
    return schema_version == DASHBOARD_CACHE_SCHEMA_VERSION


def _apply_spend_filters(spend_qs, filters):
    asin_filter = filters.get("asin")
    if asin_filter:
        if isinstance(asin_filter, (list, tuple, set)):
            values = [str(v) for v in asin_filter if str(v).strip()]
            if values:
                spend_qs = spend_qs.filter(asin__in=values)
        else:
            spend_qs = spend_qs.filter(asin=asin_filter)
    return spend_qs


def _cache_key(user_id, view_type, data_version, cache_hash, section_scope="all", mode="lite"):
    view_type = _cache_view_type(view_type)
    return (
        f"dashboard_payload_v{DASHBOARD_PAYLOAD_CACHE_VERSION}_"
        f"s{DASHBOARD_CACHE_SCHEMA_VERSION}_"
        f"{user_id}_{view_type}_{section_scope}_{data_version}_{cache_hash}_{mode}"
    )


def _normalize_section_scopes(value):
    if not value:
        return DEFAULT_WARMUP_SECTION_SCOPES

    scopes = []
    for item in value:
        if isinstance(item, (list, tuple)) and len(item) == 2:
            section_scope = str(item[0] or "").strip()
            compute_scope = str(item[1] or "").strip()
            if section_scope and compute_scope:
                scopes.append((section_scope, compute_scope))
    return tuple(scopes or DEFAULT_WARMUP_SECTION_SCOPES)


def _split_warmup_filter_options(raw_filters):
    raw_filters = dict(raw_filters or {})
    options = {
        "skip_full_payload": bool(raw_filters.pop("_warmup_skip_full_payload", False)),
        "section_scopes": _normalize_section_scopes(
            raw_filters.pop("_warmup_section_scopes", None)
        ),
    }
    return raw_filters, options


def _dedupe_filter_sets(filter_sets):
    seen = set()
    deduped = []
    for raw_filters in filter_sets or []:
        filters, options = _split_warmup_filter_options(raw_filters)
        key = (
            cache_filter_string(filters),
            bool(options["skip_full_payload"]),
            tuple(options["section_scopes"]),
        )
        if key in seen:
            continue
        seen.add(key)
        merged = dict(filters)
        if options["skip_full_payload"]:
            merged["_warmup_skip_full_payload"] = True
        if options["section_scopes"] != DEFAULT_WARMUP_SECTION_SCOPES:
            merged["_warmup_section_scopes"] = tuple(options["section_scopes"])
        deduped.append(merged)
    return deduped


def build_mapping_kpi_warmup_filter_sets_for_user(user, *, max_filter_sets=None):
    """
    Build KPI-only warmups for the most-used mapping filters.

    These are intentionally overview-only because full/visual/details payloads
    can be expensive and less likely to be needed immediately after an upload.
    """
    if not getattr(settings, "DASHBOARD_MAPPING_KPI_WARMUP_ENABLED", True):
        return []

    fields = tuple(
        getattr(
            settings,
            "DASHBOARD_MAPPING_KPI_WARMUP_FIELDS",
            DEFAULT_MAPPING_KPI_WARMUP_FIELDS,
        )
    )
    date_ranges = tuple(
        getattr(
            settings,
            "DASHBOARD_MAPPING_KPI_WARMUP_DATE_RANGES",
            DEFAULT_MAPPING_KPI_WARMUP_DATE_RANGES,
        )
    )
    if not fields or not date_ranges:
        return []

    try:
        top_values_per_field = max(
            int(getattr(settings, "DASHBOARD_MAPPING_KPI_WARMUP_TOP_VALUES_PER_FIELD", 1)),
            1,
        )
    except (TypeError, ValueError):
        top_values_per_field = 1

    if max_filter_sets is None:
        max_filter_sets = getattr(settings, "DASHBOARD_MAPPING_KPI_WARMUP_MAX_FILTER_SETS", 15)
    try:
        max_filter_sets = max(int(max_filter_sets), 0)
    except (TypeError, ValueError):
        max_filter_sets = 15
    if max_filter_sets <= 0:
        return []

    top_by_field = {field: [] for field in fields}
    qs = (
        DashboardMappingFilterDailySummary.objects.filter(
            user=user,
            filter_name__in=fields,
        )
        .exclude(filter_value="")
        .values("filter_name", "filter_value")
        .annotate(revenue=Sum("revenue"))
        .order_by("filter_name", "-revenue")
    )
    for row in qs.iterator(chunk_size=1000):
        field = str(row.get("filter_name") or "").strip()
        value = str(row.get("filter_value") or "").strip()
        if not field or not value or field not in top_by_field:
            continue
        if len(top_by_field[field]) >= top_values_per_field:
            continue
        top_by_field[field].append(value)
        if all(len(values) >= top_values_per_field for values in top_by_field.values()):
            break

    filter_sets = []
    for date_range in date_ranges:
        for field in fields:
            for value in top_by_field.get(field, []):
                filter_sets.append(
                    {
                        "date_range": date_range,
                        field: [value] if field in LIST_FILTER_FIELDS else value,
                        "_warmup_skip_full_payload": True,
                        "_warmup_section_scopes": KPI_ONLY_WARMUP_SECTION_SCOPES,
                    }
                )
                if len(filter_sets) >= max_filter_sets:
                    return filter_sets
    return filter_sets


def prime_dashboard_payloads_for_user(
    user,
    *,
    view_types=None,
    filter_sets=None,
    data_version=None,
    max_filter_sets=None,
    include_mapping_kpi_filters=True,
):
    """
    Precompute common dashboard payloads into memory cache + materialized table.
    """
    base_qs = ProcessedDashboardData.objects.filter(user=user)
    base_fk_qs = FlipkartProcessedDashboardData.objects.filter(user=user)
    if not base_qs.exists() and not base_fk_qs.exists():
        return {
            "computed": 0,
            "computed_sections": 0,
            "reused_materialized": 0,
            "reused_memory": 0,
            "skipped_no_data": 0,
            "view_types": [],
            "filters_processed": 0,
            "mapping_kpi_filter_sets_added": 0,
        }

    if data_version is None:
        data_version = cache.get(f"dashboard_data_version_{user.id}", 0)

    resolved_view_types = list(view_types or DEFAULT_DASHBOARD_VIEW_TYPES)
    base_filter_sets = (
        list(DEFAULT_WARMUP_FILTER_SETS)
        if filter_sets is None
        else list(filter_sets)
    )
    resolved_filter_sets = [deepcopy(f) for f in base_filter_sets]
    if max_filter_sets is not None:
        resolved_filter_sets = resolved_filter_sets[: max(int(max_filter_sets), 0)]
    mapping_filter_sets = []
    if include_mapping_kpi_filters:
        mapping_filter_sets = build_mapping_kpi_warmup_filter_sets_for_user(user)
        resolved_filter_sets.extend(deepcopy(f) for f in mapping_filter_sets)
    resolved_filter_sets = _dedupe_filter_sets(resolved_filter_sets)

    computed = 0
    computed_sections = 0
    reused_materialized = 0
    reused_memory = 0
    skipped_no_data = 0

    for raw_filters in resolved_filter_sets:
        filters, warmup_options = _split_warmup_filter_options(raw_filters)
        filter_key_str = cache_filter_string(filters)
        cache_hash = hashlib.md5(filter_key_str.encode("utf-8")).hexdigest()
        normalized = json.dumps(normalize_payload_filters(filters), sort_keys=True)

        views_needing_compute = []
        if not warmup_options["skip_full_payload"]:
            for view_type in resolved_view_types:
                cache_view_type = _cache_view_type(view_type)
                key = _cache_key(user.id, view_type, data_version, cache_hash, section_scope="all", mode="lite")
                payload = cache.get(key)
                if payload is not None:
                    reused_memory += 1
                    continue

                payload = get_materialized_summary(
                    user_id=user.id,
                    view_type=cache_view_type,
                    data_version=data_version,
                    filter_hash=cache_hash,
                    section_scope="all",
                )
                if _is_current_payload(payload):
                    cache.set(key, payload, timeout=DASHBOARD_CACHE_TTL_LITE_SECONDS)
                    reused_materialized += 1
                    continue

                views_needing_compute.append(view_type)

        sections_needing_compute = []
        for view_type in resolved_view_types:
            cache_view_type = _cache_view_type(view_type)
            for section_scope, compute_scope in warmup_options["section_scopes"]:
                key = _cache_key(
                    user.id,
                    view_type,
                    data_version,
                    cache_hash,
                    section_scope=section_scope,
                    mode="lite",
                )
                if cache.get(key) is not None:
                    reused_memory += 1
                    continue

                section_payload = get_materialized_summary(
                    user_id=user.id,
                    view_type=cache_view_type,
                    data_version=data_version,
                    filter_hash=cache_hash,
                    section_scope=section_scope,
                )
                if _is_current_payload(section_payload):
                    cache.set(
                        key,
                        section_payload,
                        timeout=DASHBOARD_CACHE_TTL_LITE_SECONDS,
                    )
                    reused_materialized += 1
                    continue

                sections_needing_compute.append((view_type, section_scope, compute_scope))

        if not views_needing_compute and not sections_needing_compute:
            continue

        platform = (filters.get("platform") or "").strip()
        show_amazon = platform != "Flipkart"
        show_flipkart = platform != "Amazon"

        cached_filter_metadata = get_available_filters_orm_cached(
            base_qs if show_amazon else base_qs.none(),
            base_fk_qs if show_flipkart else base_fk_qs.none(),
            user.id,
            show_amazon,
            show_flipkart,
        )

        scoped_qs, scoped_fk_qs = apply_dashboard_entity_filters(
            base_qs, base_fk_qs, filters, user=user
        )
        if not scoped_qs.exists() and not scoped_fk_qs.exists():
            skipped_no_data += len(views_needing_compute) + len(sections_needing_compute)
            continue

        spend_qs = _apply_spend_filters(SpendData.objects.filter(user=user), filters)
        if (
            filters.get("category")
            or filters.get("portfolio")
            or filters.get("subcategory")
            or filters.get("category_manager")
            or filters.get("series_name")
            or filters.get("material")
            or filters.get("size")
            or filters.get("brand_name")
            or filters.get("ratings")
            or filters.get("parent_asin")
            or filters.get("finish")
            or filters.get("launch_date_range")
            or filters.get("launch_start_date")
            or filters.get("launch_end_date")
        ):
            spend_qs = spend_qs.filter(asin__in=scoped_qs.values("asin").distinct())

        if not views_needing_compute:
            payload = None
        else:
            # Compute once and cache for all view_types that need it.
            _t0 = time.monotonic()
            payload = run_orm_computation(
                scoped_qs,
                scoped_fk_qs,
                spend_qs,
                filters,
                user,
                cached_filter_metadata=cached_filter_metadata,
            )
            if isinstance(payload, dict):
                payload = deepcopy(payload)
                payload["_payload_schema_version"] = DASHBOARD_CACHE_SCHEMA_VERSION
                payload["_section_scope"] = "all"
            _elapsed = time.monotonic() - _t0
            logger.info(
                "[DashboardWarmup] Computed payload for %d view_types in %.1fs (filters=%s)",
                len(views_needing_compute), _elapsed, filter_key_str[:80],
            )
            for view_type in views_needing_compute:
                cache_view_type = _cache_view_type(view_type)
                key = _cache_key(user.id, view_type, data_version, cache_hash, section_scope="all", mode="lite")
                store_materialized_summary(
                    user_id=user.id,
                    view_type=cache_view_type,
                    data_version=data_version,
                    filter_hash=cache_hash,
                    section_scope="all",
                    normalized_filters=normalized,
                    payload=payload,
                )
                cache.set(key, payload, timeout=DASHBOARD_CACHE_TTL_LITE_SECONDS)
                computed += 1

        for view_type, section_scope, compute_scope in sections_needing_compute:
            dashboard_view = _dashboard_view_name(view_type)
            cache_view_type = _cache_view_type(view_type)
            key = _cache_key(
                user.id,
                view_type,
                data_version,
                cache_hash,
                section_scope=section_scope,
                mode="lite",
            )
            _t0 = time.monotonic()
            section_payload = run_orm_computation(
                scoped_qs,
                scoped_fk_qs,
                spend_qs,
                filters,
                user,
                cached_filter_metadata=cached_filter_metadata,
                compute_scope=compute_scope,
                section_scope=section_scope,
                dashboard_view=dashboard_view,
            )
            if isinstance(section_payload, dict):
                section_payload = deepcopy(section_payload)
                section_payload["_payload_schema_version"] = DASHBOARD_CACHE_SCHEMA_VERSION
                section_payload["_section_scope"] = section_scope
            store_materialized_summary(
                user_id=user.id,
                view_type=cache_view_type,
                data_version=data_version,
                filter_hash=cache_hash,
                section_scope=section_scope,
                normalized_filters=normalized,
                payload=section_payload,
            )
            cache.set(
                key,
                section_payload,
                timeout=DASHBOARD_CACHE_TTL_LITE_SECONDS,
            )
            computed_sections += 1
            logger.info(
                "[DashboardWarmup] Computed section=%s view=%s in %.1fs (filters=%s)",
                section_scope,
                view_type,
                time.monotonic() - _t0,
                filter_key_str[:80],
            )

    return {
        "computed": computed,
        "computed_sections": computed_sections,
        "reused_materialized": reused_materialized,
        "reused_memory": reused_memory,
        "skipped_no_data": skipped_no_data,
        "view_types": resolved_view_types,
        "filters_processed": len(resolved_filter_sets),
        "mapping_kpi_filter_sets_added": len(mapping_filter_sets),
    }
