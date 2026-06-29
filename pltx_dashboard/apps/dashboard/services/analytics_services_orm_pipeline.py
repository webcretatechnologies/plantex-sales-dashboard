import datetime
import calendar
import time

from apps.dashboard.models import DashboardDailySummary, DashboardProductDailySummary
from apps.dashboard.services.analytics_services_orm import (
    generate_charts_data_orm,
)
from apps.dashboard.services.analytics_services_orm_tables import (
    generate_bi_data_orm,
)
from apps.dashboard.services.metrics import (
    amazon_cvr,
    flipkart_cvr,
    roas as calculate_roas,
    safe_growth as calculate_growth,
    tacos as calculate_tacos,
)
from apps.dashboard.services.filters import (
    apply_dashboard_entity_filters,
    get_filtered_mapping_querysets,
    has_launch_date_filter,
)
from django.core.cache import cache
from django.db.models import Sum, Max, Case, When, F, Value, Count, Q
from django.utils import timezone

def safe_replace_year(d, year_offset=-1):
    try:
        return d.replace(year=d.year + year_offset)
    except ValueError:
        return d.replace(year=d.year + year_offset, day=28)


def safe_shift_month(d, month_offset=-1):
    month_index = (d.year * 12 + (d.month - 1)) + month_offset
    year = month_index // 12
    month = (month_index % 12) + 1
    day = min(d.day, calendar.monthrange(year, month)[1])
    return datetime.date(year, month, day)


def _parse_ymd_date(value):
    if not value:
        return None
    try:
        return datetime.datetime.strptime(str(value), "%Y-%m-%d").date()
    except Exception:
        return None


def _filter_values(value):
    if not value:
        return []
    if isinstance(value, (list, tuple, set)):
        return [str(item).strip() for item in value if str(item).strip()]
    item = str(value).strip()
    return [item] if item else []


def _apply_inventory_value_filter(qs, field_name, value):
    values = _filter_values(value)
    if not values:
        return qs
    return qs.filter(**{f"{field_name}__in": values})


def _inventory_mapping_filters_active(filters):
    mapping_fields = {
        "asin",
        "fsn",
        "sku",
        "parent_asin",
        "category_manager",
        "series_name",
        "material",
        "size",
        "brand_name",
        "ratings",
        "finish",
        "launch_date_range",
        "launch_start_date",
        "launch_end_date",
    }
    return any(filters.get(field) for field in mapping_fields)


def _has_active_inventory_dashboard_filter(filters, platform_filter=None):
    if str(platform_filter or "").strip() in {"Amazon", "Flipkart"}:
        return True

    filter_fields = {
        "date_range",
        "start_date",
        "end_date",
        "category",
        "portfolio",
        "subcategory",
        "inventory_health",
        "asin",
        "fsn",
        "sku",
        "parent_asin",
        "category_manager",
        "series_name",
        "material",
        "size",
        "brand_name",
        "ratings",
        "finish",
        "launch_date_range",
        "launch_start_date",
        "launch_end_date",
    }
    for field in filter_fields:
        value = filters.get(field)
        if isinstance(value, (list, tuple, set)):
            if any(str(item).strip() for item in value):
                return True
        elif str(value or "").strip():
            return True
    return False


def _apply_latest_inventory_date_when_unfiltered(inv_sum_qs, filters, platform_filter=None):
    if _has_active_inventory_dashboard_filter(filters or {}, platform_filter):
        return inv_sum_qs

    latest_date = (
        inv_sum_qs.order_by("-date")
        .values_list("date", flat=True)
        .first()
    )
    if latest_date:
        return inv_sum_qs.filter(date=latest_date)
    return inv_sum_qs


def _inventory_health_status_q(status, platform_filter):
    if status == "In Stock":
        az_q = Q(status="In Stock")
        fk_q = Q(fk_status="Ideal Stocking")
    elif status == "Low Stock":
        az_q = Q(status="Low Stock")
        fk_q = Q(fk_status="Understock")
    elif status == "OOS":
        az_q = Q(status="OOS")
        fk_q = Q(fk_status__in=["OOS", "Nearly OOS"])
    elif status == "Overstock":
        az_q = Q(status="Overstock")
        fk_q = Q(fk_status__in=["Over Stock", "Highly Over Stock", "Not Selling"])
    else:
        return Q()

    if platform_filter == "Amazon":
        return az_q
    if platform_filter == "Flipkart":
        return fk_q
    return az_q | fk_q


def apply_inventory_summary_filters(inv_sum_qs, user, filters, platform_filter=None, *, apply_date_filter=True):
    if apply_date_filter:
        inv_sum_qs = apply_global_filters_orm(inv_sum_qs, filters)
        inv_sum_qs = _apply_latest_inventory_date_when_unfiltered(
            inv_sum_qs,
            filters,
            platform_filter,
        )

    if platform_filter == "Amazon":
        inv_sum_qs = inv_sum_qs.exclude(Q(asin="") | Q(asin__isnull=True))
    elif platform_filter == "Flipkart":
        inv_sum_qs = inv_sum_qs.exclude(Q(fsn="") | Q(fsn__isnull=True))

    inv_sum_qs = _apply_inventory_value_filter(inv_sum_qs, "category", filters.get("category"))
    inv_sum_qs = _apply_inventory_value_filter(inv_sum_qs, "portfolio", filters.get("portfolio"))
    inv_sum_qs = _apply_inventory_value_filter(inv_sum_qs, "subcategory", filters.get("subcategory"))

    if _inventory_mapping_filters_active(filters):
        mapping_filters = dict(filters)
        mapping_filters.pop("inventory_health", None)
        az_map_qs, fk_map_qs = get_filtered_mapping_querysets(mapping_filters, user=user)
        allowed_asins = [
            str(value).strip()
            for value in az_map_qs.values_list("asin", flat=True).distinct()
            if str(value or "").strip()
        ]
        allowed_fsns = [
            str(value).strip()
            for value in fk_map_qs.values_list("fsn", flat=True).distinct()
            if str(value or "").strip()
        ]

        if platform_filter == "Amazon":
            inv_sum_qs = inv_sum_qs.filter(asin__in=allowed_asins) if allowed_asins else inv_sum_qs.none()
        elif platform_filter == "Flipkart":
            inv_sum_qs = inv_sum_qs.filter(fsn__in=allowed_fsns) if allowed_fsns else inv_sum_qs.none()
        else:
            inv_sum_qs = inv_sum_qs.filter(Q(asin__in=allowed_asins) | Q(fsn__in=allowed_fsns))

    status_values = _filter_values(filters.get("inventory_health"))
    if status_values:
        status_q = Q()
        for status in status_values:
            status_q |= _inventory_health_status_q(status, platform_filter)
        inv_sum_qs = inv_sum_qs.filter(status_q) if status_q else inv_sum_qs.none()

    return inv_sum_qs


def _average_order_value_from_qs(qs):
    if qs is None:
        return 0.0
    agg = qs.aggregate(
        revenue=Sum("revenue"),
        orders=Sum("orders"),
        units=Sum("units"),
    )
    revenue = float(agg.get("revenue") or 0.0)
    orders = int(agg.get("orders") or 0)
    units = int(agg.get("units") or 0)
    denominator = orders if orders > 0 else units
    return revenue / denominator if revenue > 0 and denominator > 0 else 0.0


def _estimate_oos_orders_lost(
    *,
    az_lost_sales,
    fk_lost_sales,
    platform_filter=None,
    qs=None,
    fk_qs=None,
    user=None,
    filters=None,
):
    filters = filters or {}
    if (qs is None or fk_qs is None) and user is not None:
        from apps.dashboard.models import ProcessedDashboardData, FlipkartProcessedDashboardData

        base_qs = ProcessedDashboardData.objects.filter(user=user)
        base_fk_qs = FlipkartProcessedDashboardData.objects.filter(user=user)
        qs, fk_qs = apply_dashboard_entity_filters(base_qs, base_fk_qs, filters, user=user)
        qs = apply_global_filters_orm(qs, filters)
        fk_qs = apply_global_filters_orm(fk_qs, filters)

    az_aov = 0.0 if platform_filter == "Flipkart" else _average_order_value_from_qs(qs)
    fk_aov = 0.0 if platform_filter == "Amazon" else _average_order_value_from_qs(fk_qs)

    az_orders_lost = round(float(az_lost_sales or 0.0) / az_aov) if az_aov > 0 else 0
    fk_orders_lost = round(float(fk_lost_sales or 0.0) / fk_aov) if fk_aov > 0 else 0

    return {
        "orders_lost": int(az_orders_lost + fk_orders_lost),
        "az_orders_lost": int(az_orders_lost),
        "fk_orders_lost": int(fk_orders_lost),
        "az_aov": round(az_aov, 2),
        "fk_aov": round(fk_aov, 2),
        "orders_rule": (
            "Orders Lost = Amazon lost sales / Amazon average order value "
            "+ Flipkart lost sales / Flipkart average order value. "
            "When order count is unavailable, units are used as the order-equivalent denominator."
        ),
    }


def _to_float(value, default=0.0):
    if value is None:
        return default
    if isinstance(value, str):
        value = value.strip().replace(",", "").replace("₹", "")
        if not value:
            return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _clean_dimension_label(value):
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "none", "null", "unknown", "unmapped", "-"}:
        return ""
    return text


def _fsn_status_bucket(value):
    status = str(value or "").strip().lower()
    if not status or status in {"nan", "none", "null", "-"}:
        return ""
    if "discontinu" in status:
        return "Discontinued"
    if "continu" in status:
        return "Continued"
    return ""


def _continue_discontinue_metrics_from_summary(fk_summary_qs, fsn_meta):
    counts = {"Continued": 0, "Discontinued": 0, "Unmapped": 0}
    revenue = {"Continued": 0.0, "Discontinued": 0.0, "Unmapped": 0.0}
    if fk_summary_qs is None:
        return counts, revenue
    fsn_meta = fsn_meta or {}

    for row in fk_summary_qs.values("fsn").annotate(revenue=Sum("revenue")):
        fsn = str(row.get("fsn") or "").strip()
        if not fsn:
            continue
        status = _fsn_status_bucket((fsn_meta.get(fsn) or {}).get("product_status"))
        if not status:
            status = "Unmapped"
        counts[status] += 1
        revenue[status] += _to_float(row.get("revenue"))
    return counts, revenue


def _merge_portfolio_revenue_from_summary(store, summary_qs, sku_field, meta_by_sku):
    if summary_qs is None:
        return

    for row in summary_qs.values(sku_field, "portfolio").annotate(revenue=Sum("revenue")):
        sku = str(row.get(sku_field) or "").strip()
        portfolio = (
            _clean_dimension_label(row.get("portfolio"))
            or _clean_dimension_label((meta_by_sku.get(sku) or {}).get("portfolio"))
        )
        if not portfolio:
            continue
        store[portfolio] = store.get(portfolio, 0.0) + _to_float(row.get("revenue"))


def resolve_growth_period(filters, reference_date):
    """
    Determine the active period for MOM/YOY growth.
    - No date filter: current month to date.
    - Preset date-range: that preset period.
    - Custom start/end: exact selected range.
    """
    start_custom = _parse_ymd_date(filters.get("start_date"))
    end_custom = _parse_ymd_date(filters.get("end_date"))
    if start_custom and end_custom:
        if end_custom < start_custom:
            start_custom, end_custom = end_custom, start_custom
        return start_custom, end_custom
    if start_custom and not end_custom:
        return start_custom, reference_date
    if end_custom and not start_custom:
        return end_custom.replace(day=1), end_custom

    date_range = filters.get("date_range")
    if date_range and date_range != "custom":
        if date_range == "yesterday":
            start = end = reference_date - datetime.timedelta(days=1)
        elif date_range == "last_7_days":
            start = reference_date - datetime.timedelta(days=6)
            end = reference_date
        elif date_range == "last_15_days":
            start = reference_date - datetime.timedelta(days=14)
            end = reference_date
        elif date_range == "last_month":
            first_day = reference_date.replace(day=1)
            end = first_day - datetime.timedelta(days=1)
            start = end.replace(day=1)
        elif date_range == "last_3_months":
            start = reference_date - datetime.timedelta(days=90)
            end = reference_date
        elif date_range == "last_6_months":
            start = reference_date - datetime.timedelta(days=180)
            end = reference_date
        elif date_range == "last_1_year":
            start = reference_date - datetime.timedelta(days=365)
            end = reference_date
        else:
            start = reference_date.replace(day=1)
            end = reference_date
        return start, end

    # Default (no date filter): current month-to-date
    return reference_date.replace(day=1), reference_date

def get_revenue_for_period(q, fk_q, start, end):
    rev = 0
    if q is not None:
        agg = q.filter(date__gte=start, date__lte=end).aggregate(t=Sum("revenue"))
        rev += float(agg["t"] or 0)
    if fk_q is not None:
        agg = fk_q.filter(date__gte=start, date__lte=end).aggregate(t=Sum("revenue"))
        rev += float(agg["t"] or 0)
    return rev

def apply_global_filters_orm(qs, filters):
    """Filters the QuerySet by date according to the UI filters."""
    if qs is None:
        return None

    start = end = None
    date_range = filters.get("date_range")
    if date_range and date_range != "custom":
        today = timezone.localdate()
        if date_range == "yesterday":
            start = end = today - datetime.timedelta(days=1)
        elif date_range == "last_7_days":
            start = today - datetime.timedelta(days=6)
            end = today
        elif date_range == "last_15_days":
            start = today - datetime.timedelta(days=14)
            end = today
        elif date_range == "last_month":
            first_day = today.replace(day=1)
            end = first_day - datetime.timedelta(days=1)
            start = end.replace(day=1)
        elif date_range == "last_3_months":
            start = today - datetime.timedelta(days=90)
            end = today
        elif date_range == "last_6_months":
            start = today - datetime.timedelta(days=180)
            end = today
        elif date_range == "last_1_year":
            start = today - datetime.timedelta(days=365)
            end = today

    if start and end:
        return qs.filter(date__gte=start, date__lte=end)

    # Manual start/end dates (only apply if non-empty strings)
    start_str = filters.get("start_date")
    if start_str and isinstance(start_str, str) and start_str.strip():
        qs = qs.filter(date__gte=start_str)

    end_str = filters.get("end_date")
    if end_str and isinstance(end_str, str) and end_str.strip():
        qs = qs.filter(date__lte=end_str)

    return qs


def get_prev_period_qs(qs, filters, reference_date=None):
    """Return queryset for the previous MOM comparison period."""
    if qs is None:
        return None

    cs = filters.get("compare_start_date")
    ce = filters.get("compare_end_date")
    if cs and ce:
        return qs.filter(date__gte=cs, date__lte=ce)

    cm_start, cm_end = resolve_growth_period(filters, reference_date or timezone.localdate())
    pm_start = safe_shift_month(cm_start, -1)
    pm_end = safe_shift_month(cm_end, -1)
    return qs.filter(date__gte=pm_start, date__lte=pm_end)


def _has_sku_filters(filters):
    return bool(filters.get("asin") or filters.get("fsn"))


def _apply_dimension_filter(qs, field_name, value):
    if qs is None or not value:
        return qs
    if isinstance(value, (list, tuple, set)):
        values = [str(item) for item in value if str(item).strip()]
        return qs.filter(**{f"{field_name}__in": values}) if values else qs
    return qs.filter(**{field_name: value})


def _get_daily_summary_base_qs(user, filters):
    if _has_sku_filters(filters) or has_launch_date_filter(filters):
        return None

    # DashboardDailySummary only stores category/portfolio/subcategory as dimensions.
    # Filters on mapping-level fields (category_manager, series_name, material, size,
    # brand_name, ratings, finish, parent_asin, sku) and inventory_health (resolved to
    # per-ASIN allow-lists from DashboardInventoryHealthSummary) cannot be applied to
    # the daily summary table — fall back to the per-ASIN path so those filters take effect.
    _MAPPING_ONLY_FIELDS = {
        "category_manager", "series_name", "material", "size",
        "brand_name", "ratings", "finish", "parent_asin", "sku",
        "inventory_health",  # resolved via DashboardInventoryHealthSummary → ASIN/FSN allow-lists
    }
    if any(filters.get(f) for f in _MAPPING_ONLY_FIELDS):
        return None

    qs = DashboardDailySummary.objects.filter(user=user)
    platform_filter = (filters.get("platform") or "").strip()
    if platform_filter == "Amazon":
        qs = qs.filter(platform="Amazon")
    elif platform_filter == "Flipkart":
        qs = qs.filter(platform="Flipkart")

    qs = _apply_dimension_filter(qs, "category", filters.get("category"))
    qs = _apply_dimension_filter(qs, "portfolio", filters.get("portfolio"))
    qs = _apply_dimension_filter(qs, "subcategory", filters.get("subcategory"))
    return qs


def _get_product_daily_summary_querysets(user, filters, *, apply_date_filter=True):
    base_qs = DashboardProductDailySummary.objects.filter(user=user)
    az_qs = base_qs.filter(platform="Amazon")
    fk_qs = base_qs.filter(platform="Flipkart")

    az_qs, fk_qs = apply_dashboard_entity_filters(az_qs, fk_qs, filters, user=user)
    if apply_date_filter:
        az_qs = apply_global_filters_orm(az_qs, filters)
        fk_qs = apply_global_filters_orm(fk_qs, filters)
    return az_qs, fk_qs


def product_insights_need_exact_dates(filters):
    """Use daily product summaries whenever the UI date filter must be exact."""
    return bool(
        filters.get("date_range")
        or filters.get("start_date")
        or filters.get("end_date")
        or filters.get("compare_start_date")
        or filters.get("compare_end_date")
    )


def _summary_metrics_by_platform(summary_qs):
    metrics = {
        "Amazon": _zero_metrics(),
        "Flipkart": _zero_metrics(),
    }
    if summary_qs is None:
        return metrics

    rows = summary_qs.values("platform").annotate(
        revenue=Sum("revenue"),
        orders=Sum("orders"),
        units=Sum("units"),
        pageviews=Sum("pageviews"),
        total_spend=Sum("total_spend"),
        spend_sp=Sum("spend_sp"),
        spend_sb=Sum("spend_sb"),
        spend_sd=Sum("spend_sd"),
    )
    for row in rows:
        platform = str(row.get("platform") or "")
        if platform not in metrics:
            continue
        metrics[platform] = {
            "revenue": _to_float(row.get("revenue")),
            "orders": int(row.get("orders") or 0),
            "units": int(row.get("units") or 0),
            "pageviews": int(row.get("pageviews") or 0),
            "total_spend": float(row.get("total_spend") or 0.0),
            "spend_sp": float(row.get("spend_sp") or 0.0),
            "spend_sb": float(row.get("spend_sb") or 0.0),
            "spend_sd": float(row.get("spend_sd") or 0.0),
        }
    return metrics


def _summary_revenue_by_dimension(summary_qs, field_name):
    if summary_qs is None:
        return {}

    grouped = {}
    for row in summary_qs.values(field_name).annotate(revenue=Sum("revenue")):
        key = row.get(field_name) or "Unknown"
        grouped[str(key)] = grouped.get(str(key), 0.0) + _to_float(row.get("revenue"))
    return grouped


def _summary_trend_map(summary_qs):
    if summary_qs is None:
        return None

    trend_rows = (
        summary_qs.values("date")
        .annotate(
            total_revenue=Sum("revenue"),
            total_spend_value=Sum("total_spend"),
            total_pageviews=Sum("pageviews"),
            total_orders=Sum("orders"),
            amazon_revenue=Sum("revenue", filter=Q(platform="Amazon")),
            flipkart_revenue=Sum("revenue", filter=Q(platform="Flipkart")),
        )
        .order_by("date")
    )
    data = {
        str(row["date"]): {
            "revenue": float(row.get("total_revenue") or 0.0),
            "total_spend": float(row.get("total_spend_value") or 0.0),
            "pageviews": int(row.get("total_pageviews") or 0),
            "orders": int(row.get("total_orders") or 0),
            "amazon_revenue": float(row.get("amazon_revenue") or 0.0),
            "flipkart_revenue": float(row.get("flipkart_revenue") or 0.0),
        }
        for row in trend_rows
    }
    return data or None


def _summary_charts_data(summary_qs):
    trend_map = _summary_trend_map(summary_qs) or {}
    dates = sorted(trend_map.keys())

    revenue_line = [trend_map[d]["revenue"] for d in dates]
    spend_line = [trend_map[d]["total_spend"] for d in dates]
    pv_line = [trend_map[d]["pageviews"] for d in dates]
    order_line = [trend_map[d]["orders"] for d in dates]
    amazon_revenue_line = [trend_map[d]["amazon_revenue"] for d in dates]
    flipkart_revenue_line = [trend_map[d]["flipkart_revenue"] for d in dates]

    merged_port = {}
    for row in summary_qs.values("portfolio").annotate(units=Sum("units")):
        portfolio = row.get("portfolio") or "Unmapped"
        merged_port[portfolio] = merged_port.get(portfolio, 0) + int(row.get("units") or 0)

    sorted_ports = sorted(merged_port.items(), key=lambda item: item[1], reverse=True)[:10]
    port_labels = [label for label, _ in sorted_ports]
    port_units = [units for _, units in sorted_ports]

    ad_agg = summary_qs.aggregate(
        sp=Sum("spend_sp"),
        sb=Sum("spend_sb"),
        sd=Sum("spend_sd"),
    )
    sp_sum = float(ad_agg.get("sp") or 0.0)
    sb_sum = float(ad_agg.get("sb") or 0.0)
    sd_sum = float(ad_agg.get("sd") or 0.0)
    ad_total = sp_sum + sb_sum + sd_sum

    ad_type_labels = ["SB", "SD", "SP"]
    ad_type_vals = [sb_sum, sd_sum, sp_sum]
    ad_legend = []
    for index, label in enumerate(ad_type_labels):
        value = ad_type_vals[index]
        pct = (value / ad_total * 100) if ad_total > 0 else 0
        ad_legend.append({"label": label, "value": value, "pct": round(pct, 1)})

    return {
        "trend": {
            "labels": dates,
            "revenue": revenue_line,
            "spend": spend_line,
            "pageviews": pv_line,
            "orders": order_line,
            "amazon_revenue": amazon_revenue_line,
            "flipkart_revenue": flipkart_revenue_line,
        },
        "portfolio": {"labels": port_labels, "units": port_units},
        "adType": {
            "labels": ad_type_labels,
            "values": ad_type_vals,
            "legend": ad_legend,
        },
    }


def _safe_growth(curr, prev):
    return calculate_growth(curr, prev)


def _zero_metrics():
    return {
        "revenue": 0.0,
        "orders": 0,
        "units": 0,
        "pageviews": 0,
        "total_spend": 0.0,
        "spend_sp": 0.0,
        "spend_sb": 0.0,
        "spend_sd": 0.0,
    }


def _aggregate_metrics(qs):
    if qs is None:
        return _zero_metrics()
    agg = qs.aggregate(
        revenue=Sum("revenue"),
        orders=Sum("orders"),
        units=Sum("units"),
        pageviews=Sum("pageviews"),
        total_spend=Sum("total_spend"),
        spend_sp=Sum("spend_sp"),
        spend_sb=Sum("spend_sb"),
        spend_sd=Sum("spend_sd"),
    )
    return {
        "revenue": float(agg.get("revenue") or 0.0),
        "orders": int(agg.get("orders") or 0),
        "units": int(agg.get("units") or 0),
        "pageviews": int(agg.get("pageviews") or 0),
        "total_spend": float(agg.get("total_spend") or 0.0),
        "spend_sp": float(agg.get("spend_sp") or 0.0),
        "spend_sb": float(agg.get("spend_sb") or 0.0),
        "spend_sd": float(agg.get("spend_sd") or 0.0),
    }


def _combined_metrics(az_metrics, fk_metrics):
    return {
        "revenue": az_metrics["revenue"] + fk_metrics["revenue"],
        "orders": az_metrics["orders"] + fk_metrics["orders"],
        "units": az_metrics["units"] + fk_metrics["units"],
        "pageviews": az_metrics["pageviews"] + fk_metrics["pageviews"],
        "total_spend": az_metrics["total_spend"] + fk_metrics["total_spend"],
        "spend_sp": az_metrics["spend_sp"] + fk_metrics["spend_sp"],
        "spend_sb": az_metrics["spend_sb"] + fk_metrics["spend_sb"],
        "spend_sd": az_metrics["spend_sd"] + fk_metrics["spend_sd"],
    }


def _empty_kpi_payload(kpis, marketing, filter_meta):
    return {
        "_compute_scope": "kpis",
        "kpis": kpis,
        "charts": {},
        "category_performance": [],
        "platforms": {},
        "filters": filter_meta,
        "oos_impact": {
            "lost_sales": 0.0,
            "skus_affected": 0,
            "orders_lost": 0,
            "selected_platform": "",
            "lost_sales_rule": "",
            "sku_rule": "",
            "orders_rule": "",
        },
        "inventory": {
            "in_stock": 0,
            "low_stock": 0,
            "oos": 0,
            "overstock": 0,
            "details": [],
            "details_total": 0,
            "details_shown": 0,
            "details_truncated": False,
            "has_stock_data": False,
            "num_sale_days": 1,
        },
        "inventory_position": [],
        "forecast": {
            "predicted": 0.0,
            "target": 0.0,
            "gap": 0.0,
            "gap_pct": 0.0,
            "labels": [],
            "actual": [],
            "forecast": [],
            "target_line": [],
            "details": [],
            "daily_rate": 0.0,
            "days_in_month": 0,
            "days_elapsed": 0,
        },
        "priorities": [],
        "marketing": marketing,
        "cluster_performance": [],
        "cat_top_products": [],
        "cat_under_products": [],
        "cat_all_top_products": [],
        "cat_all_under_products": [],
        "npd_products": [],
        "npd_products_all": [],
        "npd_trend": {"labels": [], "pageviews": [], "units": [], "conversion": []},
        "growth_opportunities": [],
    }


def _build_kpi_cache_key(user_id, cache_identity):
    if not cache_identity:
        return None

    filter_hash = str(cache_identity.get("filter_hash") or "").strip()
    if not filter_hash:
        return None

    data_version = int(cache_identity.get("data_version") or 0)
    return f"dashboard_kpi_payload_v3_{user_id}_{data_version}_{filter_hash}"


def _batch_period_aggregates(base_qs, periods, rev_field="revenue", spend_field="total_spend"):
    """Compute revenue and spend for multiple periods in a single SQL query."""
    if base_qs is None:
        return {f"{k}_rev": 0.0 for k in periods} | {f"{k}_spend": 0.0 for k in periods}

    agg_kwargs = {}
    for label, (p_start, p_end) in periods.items():
        agg_kwargs[f"{label}_rev"] = Sum(
            Case(
                When(date__gte=p_start, date__lte=p_end, then=F(rev_field)),
                default=Value(0.0),
            )
        )
        agg_kwargs[f"{label}_spend"] = Sum(
            Case(
                When(date__gte=p_start, date__lte=p_end, then=F(spend_field)),
                default=Value(0.0),
            )
        )

    all_starts = [s for s, _ in periods.values()]
    all_ends = [e for _, e in periods.values()]
    scoped = base_qs.filter(date__gte=min(all_starts), date__lte=max(all_ends))
    result = scoped.aggregate(**agg_kwargs)
    return {k: float(v or 0) for k, v in result.items()}


def _compute_sku_activity_combined_from_summary(qs, sku_field):
    active = selling = zero_selling_count = zero_sales_pv = 0
    all_zero_skus = set()
    ad_spend_skus = set()
    if qs is None:
        return active, selling, zero_selling_count, zero_sales_pv, all_zero_skus, ad_spend_skus

    for row in (
        qs.exclude(**{f"{sku_field}__isnull": True})
        .exclude(**{sku_field: ""})
        .values(sku_field)
        .annotate(
            total_units=Sum("units_sold"),
            total_pv=Sum("page_views"),
            total_rev=Sum("revenue"),
            total_orders=Sum("orders"),
            total_ad_spend=Sum("ad_spend"),
        )
    ):
        active += 1
        sku = str(row.get(sku_field) or "").strip()
        total_rev = _to_float(row.get("total_rev"))
        if total_rev > 0:
            selling += 1
        elif total_rev == 0:
            zero_selling_count += 1
            zero_sales_pv += int(row.get("total_pv") or 0)
            if sku:
                all_zero_skus.add(sku)
        if (row.get("total_ad_spend") or 0) > 0 and sku:
            ad_spend_skus.add(sku)
    return active, selling, zero_selling_count, zero_sales_pv, all_zero_skus, ad_spend_skus


def _get_fsn_meta_cached(user):
    """Load FlipkartCategoryMap for user, cached 5 minutes in Redis."""
    from apps.dashboard.models import FlipkartCategoryMap
    cache_key = f"fsn_meta_v4_{user.id}"
    fsn_meta = cache.get(cache_key)
    if fsn_meta is None:
        fsn_meta = {}
        for row in FlipkartCategoryMap.objects.filter(user=user).values(
            "fsn", "category", "portfolio", "subcategory", "product_status", "asin", "sku"
        ):
            fsn_meta[row["fsn"]] = {
                "category": row["category"] or "",
                "portfolio": row["portfolio"] or "",
                "subcategory": row["subcategory"] or "",
                "product_status": row["product_status"] or "",
                "asin": row["asin"] or "",
                "sku": row["sku"] or "",
            }
        cache.set(cache_key, fsn_meta, timeout=300)
    return fsn_meta


def _get_asin_meta_cached(user):
    """Load CategoryMapping for user, cached 5 minutes in Redis."""
    from apps.dashboard.models import CategoryMapping
    cache_key = f"asin_meta_v4_{user.id}"
    asin_meta = cache.get(cache_key)
    if asin_meta is None:
        asin_meta = {}
        for row in CategoryMapping.objects.filter(user=user).values(
            "asin", "category", "portfolio", "parent_asin", "msku"
        ):
            asin_meta[row["asin"]] = {
                "category": row["category"] or "",
                "portfolio": row["portfolio"] or "",
                "parent_asin": row["parent_asin"] or "",
                "msku": row["msku"] or "",
            }
        cache.set(cache_key, asin_meta, timeout=300)
    return asin_meta






def _empty_activity_metrics():
    return {
        "active_asins": 0,
        "selling_sku_count": 0,
        "zero_selling_sku_count": 0,
        "zero_sales_pageviews": 0,
        "az_selling_sku_count": 0,
        "fk_selling_sku_count": 0,
        "az_zero_selling_sku_count": 0,
        "fk_zero_selling_sku_count": 0,
        "az_zero_sales_pageviews": 0,
        "fk_zero_sales_pageviews": 0,
        "continue_sales_revenue": 0.0,
        "discontinue_sales_revenue": 0.0,
        "unmapped_fsn_revenue": 0.0,
        "continue_sku_count": 0,
        "discontinued_sku_count": 0,
        "unmapped_fsn_count": 0,
    }


def _normalize_activity_metrics(activity_metrics):
    normalized = _empty_activity_metrics()
    if activity_metrics:
        normalized.update(activity_metrics)
    return normalized


def _extract_kpi_metrics_from_grouped_data(table_data, _fsn_meta=None):
    if _fsn_meta is None:
        _fsn_meta = {}
    
    az_asins_with_spend = set()
    fk_fsns_with_spend = set()
    az_selling = 0
    fk_selling = 0
    az_zero_sales = 0
    fk_zero_sales = 0
    az_zero_pv = 0
    fk_zero_pv = 0
    continue_sales_revenue = 0.0
    discontinue_sales_revenue = 0.0
    unmapped_fsn_revenue = 0.0

    status_counts = {"Continued": 0, "Discontinued": 0, "Unmapped": 0}
    counted_status_fsns = set()

    for row in table_data:
        sku = str(row.get("asin", "")).strip()
        if not sku:
            continue
            
        az_rev = float(row.get("az_revenue") or 0.0)
        fk_rev = float(row.get("fk_revenue") or 0.0)
        az_units = int(row.get("az_units") or 0)
        fk_units = int(row.get("fk_units") or 0)
        az_pv = int(row.get("az_pageviews") or 0)
        fk_pv = int(row.get("fk_pageviews") or 0)
        
        if float(row.get("az_spend") or 0.0) > 0:
            az_asins_with_spend.add(sku)
        if float(row.get("fk_spend") or 0.0) > 0:
            fk_fsns_with_spend.add(sku)
            
        if az_units > 0:
            az_selling += 1
        elif az_rev > 0 or az_pv > 0 or row.get("az_orders", 0) > 0:
            az_zero_sales += 1
            az_zero_pv += az_pv
            
        if fk_units > 0:
            fk_selling += 1
        elif fk_rev > 0 or fk_pv > 0 or row.get("fk_orders", 0) > 0:
            fk_zero_sales += 1
            fk_zero_pv += fk_pv

        has_fk_activity = (
            fk_rev > 0
            or fk_units > 0
            or fk_pv > 0
            or row.get("fk_orders", 0) > 0
            or float(row.get("fk_spend") or 0.0) > 0
        )
        if has_fk_activity:
            fk_sku = str(row.get("fk_sku") or row.get("fsn") or sku).strip()
            fk_meta = _fsn_meta.get(fk_sku, {})
            status = _fsn_status_bucket(fk_meta.get("product_status"))
            if status == "Continued":
                continue_sales_revenue += fk_rev
            elif status == "Discontinued":
                discontinue_sales_revenue += fk_rev
            else:
                status = "Unmapped"
                unmapped_fsn_revenue += fk_rev
            if status and fk_sku and fk_sku not in counted_status_fsns:
                status_counts[status] += 1
                counted_status_fsns.add(fk_sku)
                
    activity_metrics = _normalize_activity_metrics({
        "active_asins": len(table_data),
        "selling_sku_count": az_selling + fk_selling,
        "zero_selling_sku_count": az_zero_sales + fk_zero_sales,
        "zero_sales_pageviews": az_zero_pv + fk_zero_pv,
        "az_selling_sku_count": az_selling,
        "fk_selling_sku_count": fk_selling,
        "az_zero_selling_sku_count": az_zero_sales,
        "fk_zero_selling_sku_count": fk_zero_sales,
        "az_zero_sales_pageviews": az_zero_pv,
        "fk_zero_sales_pageviews": fk_zero_pv,
        "continue_sales_revenue": round(continue_sales_revenue, 2),
        "discontinue_sales_revenue": round(discontinue_sales_revenue, 2),
        "unmapped_fsn_revenue": round(unmapped_fsn_revenue, 2),
        "continue_sku_count": status_counts["Continued"],
        "discontinued_sku_count": status_counts["Discontinued"],
        "unmapped_fsn_count": status_counts["Unmapped"],
    })
    
    return activity_metrics, az_asins_with_spend, fk_fsns_with_spend


def _build_period_filters(start, end):
    return {
        "date_range": "custom",
        "start_date": str(start),
        "end_date": str(end),
        "compare_start_date": "",
        "compare_end_date": "",
    }


def _compute_unique_ad_spend_sku_counts(qs_f, fk_qs_f, user, asin_meta=None, fsn_meta=None, filters=None, az_asins=None, fk_fsns=None):
    if asin_meta is None:
        asin_meta = _get_asin_meta_cached(user)
    if fsn_meta is None:
        fsn_meta = _get_fsn_meta_cached(user)

    if az_asins is None or fk_fsns is None:
        az_asins = set()
        fk_fsns = set()
        if filters is not None:
            summary_az_qs, summary_fk_qs = _get_product_daily_summary_querysets(user, filters)
            az_asins = (
                {
                    str(asin).strip()
                    for asin in summary_az_qs.filter(ad_spend__gt=0)
                    .exclude(asin__isnull=True)
                    .exclude(asin="")
                    .values_list("asin", flat=True)
                    .distinct()
                    if str(asin).strip()
                }
                if summary_az_qs is not None
                else set()
            )
            fk_fsns = (
                {
                    str(fsn).strip()
                    for fsn in summary_fk_qs.filter(ad_spend__gt=0)
                    .exclude(fsn__isnull=True)
                    .exclude(fsn="")
                    .values_list("fsn", flat=True)
                    .distinct()
                    if str(fsn).strip()
                }
                if summary_fk_qs is not None
                else set()
            )

    children_by_parent = {}
    for child_asin, meta in asin_meta.items():
        child_asin = str(child_asin or "").strip()
        parent_asin = str((meta or {}).get("parent_asin") or "").strip()
        if child_asin and parent_asin:
            children_by_parent.setdefault(parent_asin, set()).add(child_asin)

    asin_to_fsns = {}
    for fsn, meta in fsn_meta.items():
        asin = str((meta or {}).get("asin") or "").strip()
        if fsn and asin:
            asin_to_fsns.setdefault(asin, set()).add(fsn)

    advertised_parents = {
        str((asin_meta.get(asin) or {}).get("parent_asin") or "").strip()
        for asin in az_asins
    }
    
    fk_advertised_parents = set()
    for fsn in fk_fsns:
        asin = str((fsn_meta.get(fsn) or {}).get("asin") or "").strip()
        if asin:
            parent_asin = str((asin_meta.get(asin) or {}).get("parent_asin") or "").strip()
            if parent_asin:
                fk_advertised_parents.add(parent_asin)
                
    advertised_parents.discard("")
    fk_advertised_parents.discard("")

    advertised_variants = set()
    for parent_asin in advertised_parents:
        advertised_variants.update(children_by_parent.get(parent_asin, set()))
    advertised_variants.difference_update(az_asins)

    fk_advertised_variants = set()
    for parent_asin in fk_advertised_parents:
        child_asins = children_by_parent.get(parent_asin, set())
        for child_asin in child_asins:
            fk_advertised_variants.update(asin_to_fsns.get(child_asin, set()))
    fk_advertised_variants.difference_update(fk_fsns)

    az_count = len(az_asins)
    az_variant_count = len(advertised_variants)
    fk_count = len(fk_fsns)
    fk_variant_count = len(fk_advertised_variants)
    
    total_advertised_asin_count = az_count + fk_count
    total_variant_count = az_variant_count + fk_variant_count

    return {
        "az_ad_spend_sku_count": az_count,
        "fk_ad_spend_sku_count": fk_count,
        "ad_spend_sku_count": total_advertised_asin_count,
        "ad_spend_variant_count": total_variant_count,
        "ad_spend_sku_count_with_variants": total_advertised_asin_count + total_variant_count,
        
        "advertised_asin_count": total_advertised_asin_count,
        "advertised_variant_count": total_variant_count,
        "advertised_asin_count_with_variants": total_advertised_asin_count + total_variant_count,
    }


def _build_period_snapshot(qs, fk_qs, start, end, user, *, asin_meta=None, fsn_meta=None, include_activity_metrics=True):
    period_filters = _build_period_filters(start, end)
    qs_f = apply_global_filters_orm(qs, period_filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, period_filters)

    az_metrics = _aggregate_metrics(qs_f)
    fk_metrics = _aggregate_metrics(fk_qs_f)
    totals = _combined_metrics(az_metrics, fk_metrics)
    
    # Lightweight path: use _compute_sku_activity_combined_from_summary to get
    # selling/zero-selling counts via a single GROUP BY per platform, instead of
    # building the full BI table_data via generate_bi_data_orm.
    summary_az_qs, summary_fk_qs = _get_product_daily_summary_querysets(user, period_filters)
    
    az_active, az_selling, az_zero, az_zero_pv, _, az_asins = (
        _compute_sku_activity_combined_from_summary(summary_az_qs, "asin")
    )
    fk_active, fk_selling, fk_zero, fk_zero_pv, _, fk_fsns = (
        _compute_sku_activity_combined_from_summary(summary_fk_qs, "fsn")
    )
    
    unique_counts = _compute_unique_ad_spend_sku_counts(
        qs_f, fk_qs_f, user, asin_meta=asin_meta, fsn_meta=fsn_meta, filters=period_filters,
        az_asins=az_asins, fk_fsns=fk_fsns
    )
    
    activity_metrics = _normalize_activity_metrics({
        "active_asins": az_active + fk_active,
        "selling_sku_count": az_selling + fk_selling,
        "zero_selling_sku_count": az_zero + fk_zero,
        "zero_sales_pageviews": az_zero_pv + fk_zero_pv,
        "az_selling_sku_count": az_selling,
        "fk_selling_sku_count": fk_selling,
        "az_zero_selling_sku_count": az_zero,
        "fk_zero_selling_sku_count": fk_zero,
        "az_zero_sales_pageviews": az_zero_pv,
        "fk_zero_sales_pageviews": fk_zero_pv,
    })

    if not include_activity_metrics:
        activity_metrics = _empty_activity_metrics()

    az_roas = round(calculate_roas(az_metrics["revenue"], az_metrics["total_spend"]), 2)
    fk_roas = round(calculate_roas(fk_metrics["revenue"], fk_metrics["total_spend"]), 2)
    az_tacos = round(calculate_tacos(az_metrics["revenue"], az_metrics["total_spend"]), 2)
    fk_tacos = round(calculate_tacos(fk_metrics["revenue"], fk_metrics["total_spend"]), 2)
    total_roas = round(calculate_roas(totals["revenue"], totals["total_spend"]), 2)
    total_tacos = round(calculate_tacos(totals["revenue"], totals["total_spend"]), 2)

    return {
        "revenue": round(totals["revenue"], 2),
        "orders": int(totals["orders"]),
        "units": int(totals["units"]),
        "spend": round(totals["total_spend"], 2),
        "roas": total_roas,
        "tacos": total_tacos,
        "az_revenue": round(az_metrics["revenue"], 2),
        "fk_revenue": round(fk_metrics["revenue"], 2),
        "az_orders": int(az_metrics["orders"]),
        "fk_orders": int(fk_metrics["orders"]),
        "az_units": int(az_metrics["units"]),
        "fk_units": int(fk_metrics["units"]),
        "az_spend": round(az_metrics["total_spend"], 2),
        "fk_spend": round(fk_metrics["total_spend"], 2),
        "az_roas": az_roas,
        "fk_roas": fk_roas,
        "az_tacos": az_tacos,
        "fk_tacos": fk_tacos,
        "ad_spend_sku_count": unique_counts["ad_spend_sku_count"],
        "az_ad_spend_sku_count": unique_counts["az_ad_spend_sku_count"],
        "fk_ad_spend_sku_count": unique_counts["fk_ad_spend_sku_count"],
        "ad_spend_variant_count": unique_counts["ad_spend_variant_count"],
        "ad_spend_sku_count_with_variants": unique_counts["ad_spend_sku_count_with_variants"],
        "advertised_asin_count": unique_counts["advertised_asin_count"],
        "advertised_variant_count": unique_counts["advertised_variant_count"],
        "advertised_asin_count_with_variants": unique_counts["advertised_asin_count_with_variants"],
        "selling_sku_count": activity_metrics["selling_sku_count"],
        "zero_selling_sku_count": activity_metrics["zero_selling_sku_count"],
        "az_selling_sku_count": activity_metrics["az_selling_sku_count"],
        "fk_selling_sku_count": activity_metrics["fk_selling_sku_count"],
        "az_zero_selling_sku_count": activity_metrics["az_zero_selling_sku_count"],
        "fk_zero_selling_sku_count": activity_metrics["fk_zero_selling_sku_count"],
    }


def _merge_previous_revenue_map(store, qs, sku_field):
    if qs is None:
        return

    for row in (
        qs.values(sku_field).annotate(revenue=Sum("revenue")).iterator(chunk_size=5000)
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue
        store[sku] = store.get(sku, 0.0) + _to_float(row.get("revenue"))


def _build_product_metric_map_from_summary(summary_qs, sku_field):
    rows_by_sku = {}
    if summary_qs is None:
        return rows_by_sku

    for row in (
        summary_qs.values(sku_field)
        .annotate(revenue=Sum("revenue"), units=Sum("units_sold"), pageviews=Sum("page_views"))
        .iterator(chunk_size=5000)
    ):
        sku = str(row.get(sku_field) or "").strip()
        if sku:
            rows_by_sku[sku] = row
    return rows_by_sku


def _build_top_product_rows(
    qs_f,
    fk_qs_f,
    qs_prev_f,
    fk_prev_f,
    asin_meta=None,
    fsn_meta=None,
    include_full_payload=False,
    summary_qs_f=None,
    fk_summary_qs_f=None,
    summary_prev_f=None,
    fk_summary_prev_f=None,
):
    from apps.dashboard.services.metrics import safe_growth as _safe_growth

    az_curr = _build_product_metric_map_from_summary(summary_qs_f, "asin")
    fk_curr = _build_product_metric_map_from_summary(fk_summary_qs_f, "fsn")

    if not az_curr and not fk_curr:
        return []

    prev_revenue_by_sku = {}
    _merge_previous_revenue_map(prev_revenue_by_sku, summary_prev_f, "asin")
    _merge_previous_revenue_map(prev_revenue_by_sku, fk_summary_prev_f, "fsn")

    az_total_rev = sum(_to_float(r.get("revenue")) for r in az_curr.values())
    fk_total_rev = sum(_to_float(r.get("revenue")) for r in fk_curr.values())

    merged = {}
    _asin_meta = asin_meta or {}
    _fsn_meta = fsn_meta or {}

    for asin, row in az_curr.items():
        curr_rev = _to_float(row.get("revenue"))
        msku = _asin_meta.get(asin, {}).get("msku") or _asin_meta.get(asin, {}).get("sku") or ""
        cluster = _asin_meta.get(asin, {}).get("portfolio") or "Standard"
        key = msku if msku else f"az_{asin}"

        az_prev = _to_float(prev_revenue_by_sku.get(asin))
        az_growth = _safe_growth(curr_rev, az_prev)
        az_contrib = round(curr_rev / az_total_rev * 100, 1) if az_total_rev > 0 else 0.0

        merged[key] = {
            "sku": asin, "msku": msku or asin, "cluster": cluster,
            "az_sku": asin, "fk_sku": None,
            "az_revenue": round(curr_rev, 2), "fk_revenue": 0.0,
            "az_prev_revenue": round(az_prev, 2), "fk_prev_revenue": 0.0,
            "az_mom_growth": az_growth, "fk_mom_growth": 0.0,
            "az_contribution": az_contrib, "fk_contribution": 0.0,
            "az_pageviews": int(row.get("pageviews") or 0), "fk_pageviews": 0,
            "revenue": round(curr_rev, 2), "units_sold": int(row.get("units") or 0),
            "pageviews": int(row.get("pageviews") or 0), "growth": az_growth,
            "prev_revenue": round(az_prev, 2),
        }

    for fsn, row in fk_curr.items():
        curr_rev = _to_float(row.get("revenue"))
        msku = _fsn_meta.get(fsn, {}).get("sku") or ""
        cluster = _fsn_meta.get(fsn, {}).get("portfolio") or "Standard"
        key = msku if msku else f"fk_{fsn}"

        fk_prev = _to_float(prev_revenue_by_sku.get(fsn))
        fk_growth = _safe_growth(curr_rev, fk_prev)
        fk_contrib = round(curr_rev / fk_total_rev * 100, 1) if fk_total_rev > 0 else 0.0

        if key in merged:
            r = merged[key]
            r["fk_sku"] = fsn
            r["fk_revenue"] = round(curr_rev, 2)
            r["fk_prev_revenue"] = round(fk_prev, 2)
            r["fk_mom_growth"] = fk_growth
            r["fk_contribution"] = fk_contrib
            r["fk_pageviews"] = int(row.get("pageviews") or 0)
            r["revenue"] = round(r["revenue"] + curr_rev, 2)
            r["units_sold"] += int(row.get("units") or 0)
            r["pageviews"] += int(row.get("pageviews") or 0)
            r["prev_revenue"] = round(r["prev_revenue"] + fk_prev, 2)
            r["growth"] = _safe_growth(r["revenue"], r["prev_revenue"])
        else:
            merged[key] = {
                "sku": fsn, "msku": msku or fsn, "cluster": cluster,
                "az_sku": None, "fk_sku": fsn,
                "az_revenue": 0.0, "fk_revenue": round(curr_rev, 2),
                "az_prev_revenue": 0.0, "fk_prev_revenue": round(fk_prev, 2),
                "az_mom_growth": 0.0, "fk_mom_growth": fk_growth,
                "az_contribution": 0.0, "fk_contribution": fk_contrib,
                "az_pageviews": 0, "fk_pageviews": int(row.get("pageviews") or 0),
                "revenue": round(curr_rev, 2), "units_sold": int(row.get("units") or 0),
                "pageviews": int(row.get("pageviews") or 0), "growth": fk_growth,
                "prev_revenue": round(fk_prev, 2),
            }

    rows = list(merged.values())
    # Sort by Total Revenue descending (highest revenue contributor first)
    for row in rows:
        row["az_revenue"] = _to_float(row.get("az_revenue"))
        row["fk_revenue"] = _to_float(row.get("fk_revenue"))
        row["revenue"] = _to_float(row.get("revenue"))
        row["az_prev_revenue"] = _to_float(row.get("az_prev_revenue"))
        row["fk_prev_revenue"] = _to_float(row.get("fk_prev_revenue"))
        row["prev_revenue"] = _to_float(row.get("prev_revenue"))
    rows.sort(key=lambda item: item["revenue"], reverse=True)
    return rows if include_full_payload else rows[:10]


def _build_declining_product_rows(
    qs,
    fk_qs,
    cm_start,
    cm_end,
    pm_start,
    pm_end,
    include_full_payload=False,
    asin_meta=None,
    fsn_meta=None,
    summary_qs=None,
    fk_summary_qs=None,
):
    cm_az_rev, pm_az_rev = {}, {}
    cm_fk_rev, pm_fk_rev = {}, {}
    cm_az_pageviews, pm_az_pageviews = {}, {}
    cm_fk_pageviews, pm_fk_pageviews = {}, {}
    period_min = min(cm_start, pm_start)
    period_max = max(cm_end, pm_end)

    from concurrent.futures import ThreadPoolExecutor

    def fetch_az_declining():
        if summary_qs is None:
            return []
        return list(
            summary_qs.filter(date__gte=period_min, date__lte=period_max)
            .values("asin")
            .annotate(
                cm_r=Sum(Case(When(date__gte=cm_start, date__lte=cm_end, then=F("revenue")), default=Value(0.0))),
                pm_r=Sum(Case(When(date__gte=pm_start, date__lte=pm_end, then=F("revenue")), default=Value(0.0))),
                cm_pv=Sum(Case(When(date__gte=cm_start, date__lte=cm_end, then=F("page_views")), default=Value(0))),
                pm_pv=Sum(Case(When(date__gte=pm_start, date__lte=pm_end, then=F("page_views")), default=Value(0))),
            )
        )

    def fetch_fk_declining():
        if fk_summary_qs is None:
            return []
        return list(
            fk_summary_qs.filter(date__gte=period_min, date__lte=period_max)
            .values("fsn")
            .annotate(
                cm_r=Sum(Case(When(date__gte=cm_start, date__lte=cm_end, then=F("revenue")), default=Value(0.0))),
                pm_r=Sum(Case(When(date__gte=pm_start, date__lte=pm_end, then=F("revenue")), default=Value(0.0))),
                cm_pv=Sum(Case(When(date__gte=cm_start, date__lte=cm_end, then=F("page_views")), default=Value(0))),
                pm_pv=Sum(Case(When(date__gte=pm_start, date__lte=pm_end, then=F("page_views")), default=Value(0))),
            )
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        future_az = executor.submit(fetch_az_declining)
        future_fk = executor.submit(fetch_fk_declining)
        az_list = future_az.result()
        fk_list = future_fk.result()

    for row in az_list:
        sku = str(row.get("asin") or "").strip()
        if not sku:
            continue
        cm_az_rev[sku] = _to_float(row.get("cm_r") or 0.0)
        pm_az_rev[sku] = _to_float(row.get("pm_r") or 0.0)
        cm_az_pageviews[sku] = int(row.get("cm_pv") or 0)
        pm_az_pageviews[sku] = int(row.get("pm_pv") or 0)

    for row in fk_list:
        sku = str(row.get("fsn") or "").strip()
        if not sku:
            continue
        cm_fk_rev[sku] = cm_fk_rev.get(sku, 0.0) + _to_float(row.get("cm_r") or 0.0)
        pm_fk_rev[sku] = pm_fk_rev.get(sku, 0.0) + _to_float(row.get("pm_r") or 0.0)
        cm_fk_pageviews[sku] = cm_fk_pageviews.get(sku, 0) + int(row.get("cm_pv") or 0)
        pm_fk_pageviews[sku] = pm_fk_pageviews.get(sku, 0) + int(row.get("pm_pv") or 0)


    from apps.dashboard.services.metrics import safe_growth as _safe_growth

    merged = {}
    _asin_meta = asin_meta or {}
    _fsn_meta = fsn_meta or {}

    for asin in set(cm_az_rev) | set(pm_az_rev):
        curr = cm_az_rev.get(asin, 0.0)
        prev = pm_az_rev.get(asin, 0.0)
        az_pageviews = cm_az_pageviews.get(asin, 0)
        az_prev_pageviews = pm_az_pageviews.get(asin, 0)
        msku = _asin_meta.get(asin, {}).get("msku") or _asin_meta.get(asin, {}).get("sku") or ""
        key = msku if msku else f"az_{asin}"
        
        merged[key] = {
            "sku": asin, "msku": msku or asin,
            "az_sku": asin, "fk_sku": None,
            "az_revenue": curr, "fk_revenue": 0.0,
            "az_prev_revenue": prev, "fk_prev_revenue": 0.0,
            "az_drop_pct": _safe_growth(curr, prev), "fk_drop_pct": 0.0,
            "az_impact": max(prev - curr, 0.0), "fk_impact": 0.0,
            "revenue": curr, "prev_revenue": prev,
            "pageviews": az_pageviews, "az_pageviews": az_pageviews, "fk_pageviews": 0,
            "prev_pageviews": az_prev_pageviews, "az_prev_pageviews": az_prev_pageviews, "fk_prev_pageviews": 0,
            "az_pv_drop_pct": _safe_growth(az_pageviews, az_prev_pageviews), "fk_pv_drop_pct": 0.0,
            "az_pv_impact": max(az_prev_pageviews - az_pageviews, 0), "fk_pv_impact": 0,
        }

    for fsn in set(cm_fk_rev) | set(pm_fk_rev):
        curr = cm_fk_rev.get(fsn, 0.0)
        prev = pm_fk_rev.get(fsn, 0.0)
        fk_pageviews = cm_fk_pageviews.get(fsn, 0)
        fk_prev_pageviews = pm_fk_pageviews.get(fsn, 0)
        msku = _fsn_meta.get(fsn, {}).get("sku") or ""
        key = msku if msku else f"fk_{fsn}"
        
        if key in merged:
            r = merged[key]
            r["fk_sku"] = fsn
            r["fk_revenue"] = curr
            r["fk_prev_revenue"] = prev
            r["fk_drop_pct"] = _safe_growth(curr, prev)
            r["fk_impact"] = max(prev - curr, 0.0)
            r["fk_pageviews"] = fk_pageviews
            r["fk_prev_pageviews"] = fk_prev_pageviews
            r["fk_pv_drop_pct"] = _safe_growth(fk_pageviews, fk_prev_pageviews)
            r["fk_pv_impact"] = max(fk_prev_pageviews - fk_pageviews, 0)
            r["revenue"] += curr
            r["prev_revenue"] += prev
            r["pageviews"] = int(r.get("az_pageviews") or 0) + fk_pageviews
            r["prev_pageviews"] = int(r.get("az_prev_pageviews") or 0) + fk_prev_pageviews
        else:
            merged[key] = {
                "sku": fsn, "msku": msku or fsn,
                "az_sku": None, "fk_sku": fsn,
                "az_revenue": 0.0, "fk_revenue": curr,
                "az_prev_revenue": 0.0, "fk_prev_revenue": prev,
                "az_drop_pct": 0.0, "fk_drop_pct": _safe_growth(curr, prev),
                "az_impact": 0.0, "fk_impact": max(prev - curr, 0.0),
                "revenue": curr, "prev_revenue": prev,
                "pageviews": fk_pageviews, "az_pageviews": 0, "fk_pageviews": fk_pageviews,
                "prev_pageviews": fk_prev_pageviews, "az_prev_pageviews": 0, "fk_prev_pageviews": fk_prev_pageviews,
                "az_pv_drop_pct": 0.0, "fk_pv_drop_pct": _safe_growth(fk_pageviews, fk_prev_pageviews),
                "az_pv_impact": 0, "fk_pv_impact": max(fk_prev_pageviews - fk_pageviews, 0),
            }

    rows = []
    for r in merged.values():
        # Ensure all revenue fields are float before calculating drop
        r["az_revenue"] = _to_float(r.get("az_revenue"))
        r["fk_revenue"] = _to_float(r.get("fk_revenue"))
        r["revenue"] = _to_float(r.get("revenue"))
        r["az_prev_revenue"] = _to_float(r.get("az_prev_revenue"))
        r["fk_prev_revenue"] = _to_float(r.get("fk_prev_revenue"))
        r["prev_revenue"] = _to_float(r.get("prev_revenue"))
        r["az_pageviews"] = int(r.get("az_pageviews") or 0)
        r["fk_pageviews"] = int(r.get("fk_pageviews") or 0)
        r["pageviews"] = r["az_pageviews"] + r["fk_pageviews"]
        r["prev_pageviews"] = int(r.get("az_prev_pageviews") or 0) + int(r.get("fk_prev_pageviews") or 0)
        drop_pct = _safe_growth(r["revenue"], r["prev_revenue"])
        if drop_pct < 0:
            r["drop_pct"] = drop_pct
            r["impact"] = round(max(r["prev_revenue"] - r["revenue"], 0.0), 2)
            r["pv_drop_pct"] = _safe_growth(r["pageviews"], r.get("prev_pageviews", 0))
            r["pv_impact"] = round(max(r.get("prev_pageviews", 0) - r["pageviews"], 0), 2)
            rows.append(r)

    # Sort by MoM Revenue Drop % ascending (most negative = largest decline first)
    rows.sort(key=lambda item: _to_float(item.get("drop_pct")))
    return rows if include_full_payload else rows[:10]


def run_kpi_only_computation(
    qs,
    fk_qs,
    spend_qs,
    filters,
    user,
    cached_filter_metadata=None,
    cache_identity=None,
    include_activity_metrics=True,
):
    """
    Build overview-card payloads with aggregate queries only.
    This avoids the expensive SKU-level table_data construction used by charts,
    details, and modal payloads.
    """
    cache_key = _build_kpi_cache_key(user.id, cache_identity)
    lock_key = f"{cache_key}:lock" if cache_key else None
    have_lock = False

    if cache_key:
        cached_payload = cache.get(cache_key)
        if cached_payload:
            return cached_payload

        have_lock = cache.add(lock_key, "1", timeout=300)
        if not have_lock:
            for _ in range(800):
                time.sleep(0.15)
                cached_payload = cache.get(cache_key)
                if cached_payload:
                    return cached_payload

    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)
    platform_filter = (filters.get("platform") or "").strip()
    summary_base_qs = _get_daily_summary_base_qs(user, filters)
    summary_qs_f = apply_global_filters_orm(summary_base_qs, filters)

    _sm = None
    if summary_qs_f is not None:
        _sm = _summary_metrics_by_platform(summary_qs_f)
        if not any(
            _sm[p]["units"] or _sm[p]["orders"] or _sm[p]["revenue"] or _sm[p]["pageviews"]
            for p in ("Amazon", "Flipkart")
        ):
            _sm = None
            summary_base_qs = None

    if _sm:
        az_metrics = _sm["Amazon"]
        fk_metrics = _sm["Flipkart"]
    else:
        az_metrics = _aggregate_metrics(qs_f)
        fk_metrics = _aggregate_metrics(fk_qs_f)

    totals = _combined_metrics(az_metrics, fk_metrics)

    asin_meta = _get_asin_meta_cached(user) if qs_f is not None else None
    fsn_meta = None
    if include_activity_metrics and fk_qs_f is not None:
        fsn_meta = _get_fsn_meta_cached(user)

    from apps.dashboard.models import DashboardProductDailySummary
    az_qs_prod, fk_qs_prod = _get_product_daily_summary_querysets(user, filters)
    if az_qs_prod is None:
        az_qs_prod = DashboardProductDailySummary.objects.none()
    if fk_qs_prod is None:
        fk_qs_prod = DashboardProductDailySummary.objects.none()

    from concurrent.futures import ThreadPoolExecutor

    def fetch_az_activity():
        return _compute_sku_activity_combined_from_summary(az_qs_prod, "asin")

    def fetch_fk_activity():
        return _compute_sku_activity_combined_from_summary(fk_qs_prod, "fsn")

    with ThreadPoolExecutor(max_workers=3) as executor:
        future_az_act = executor.submit(fetch_az_activity)
        future_fk_act = executor.submit(fetch_fk_activity)
        future_fk_status = executor.submit(
            _continue_discontinue_metrics_from_summary,
            fk_qs_f,
            fsn_meta if include_activity_metrics else None,
        )

        az_active, az_selling, az_zero, az_zero_pv, _, az_asins = future_az_act.result()
        fk_active, fk_selling, fk_zero, fk_zero_pv, _, fk_fsns = future_fk_act.result()
        _status_counts, _status_revenue = future_fk_status.result()

    unique_counts = _compute_unique_ad_spend_sku_counts(
        qs_f, fk_qs_f, user, asin_meta=asin_meta, filters=filters,
        az_asins=az_asins, fk_fsns=fk_fsns
    )
    
    activity_metrics_extracted = _normalize_activity_metrics({
        "active_asins": az_active + fk_active,
        "selling_sku_count": az_selling + fk_selling,
        "zero_selling_sku_count": az_zero + fk_zero,
        "zero_sales_pageviews": az_zero_pv + fk_zero_pv,
        "az_selling_sku_count": az_selling,
        "fk_selling_sku_count": fk_selling,
        "az_zero_selling_sku_count": az_zero,
        "fk_zero_selling_sku_count": fk_zero,
        "az_zero_sales_pageviews": az_zero_pv,
        "fk_zero_sales_pageviews": fk_zero_pv,
        "continue_sales_revenue": round(_status_revenue["Continued"], 2),
        "discontinue_sales_revenue": round(_status_revenue["Discontinued"], 2),
        "unmapped_fsn_revenue": round(_status_revenue["Unmapped"], 2),
        "continue_sku_count": _status_counts["Continued"],
        "discontinued_sku_count": _status_counts["Discontinued"],
        "unmapped_fsn_count": _status_counts["Unmapped"],
    })
    activity_metrics = activity_metrics_extracted if include_activity_metrics else _empty_activity_metrics()

    total_revenue = totals["revenue"]
    total_spend = totals["total_spend"]
    total_pageviews = totals["pageviews"]

    roas = calculate_roas(total_revenue, total_spend)
    flipkart_cvr_mode = platform_filter == "Flipkart" or (
        not platform_filter and az_metrics["units"] == 0 and fk_metrics["units"] > 0
    )
    conversion = (
        flipkart_cvr(totals["units"], total_pageviews)
        if flipkart_cvr_mode
        else amazon_cvr(totals["orders"], total_pageviews)
    )
    tacos = calculate_tacos(total_revenue, total_spend)

    kpis = {
        "revenue": total_revenue,
        "az_revenue": az_metrics["revenue"],
        "fk_revenue": fk_metrics["revenue"],
        "orders": totals["orders"],
        "az_orders": az_metrics["orders"],
        "fk_orders": fk_metrics["orders"],
        "units": totals["units"],
        "az_units": az_metrics["units"],
        "fk_units": fk_metrics["units"],
        "pageviews": total_pageviews,
        "page_views": total_pageviews,  # alias used by templates
        "az_pageviews": int(az_metrics["pageviews"]),
        "fk_pageviews": int(fk_metrics["pageviews"]),
        "spend": total_spend,
        "az_spend": az_metrics["total_spend"],
        "fk_spend": fk_metrics["total_spend"],
        "active_asins": activity_metrics["active_asins"],
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "tacos": round(tacos, 2),
        "az_roas": round(calculate_roas(az_metrics["revenue"], az_metrics["total_spend"]), 2),
        "fk_roas": round(calculate_roas(fk_metrics["revenue"], fk_metrics["total_spend"]), 2),
        "az_tacos": round(calculate_tacos(az_metrics["revenue"], az_metrics["total_spend"]), 2),
        "fk_tacos": round(calculate_tacos(fk_metrics["revenue"], fk_metrics["total_spend"]), 2),
        "selling_sku_count": activity_metrics["selling_sku_count"],
        "az_selling_sku_count": activity_metrics["az_selling_sku_count"],
        "fk_selling_sku_count": activity_metrics["fk_selling_sku_count"],
        "zero_selling_sku_count": activity_metrics["zero_selling_sku_count"],
        "az_zero_selling_sku_count": activity_metrics["az_zero_selling_sku_count"],
        "fk_zero_selling_sku_count": activity_metrics["fk_zero_selling_sku_count"],
        "zero_sales_pageviews": activity_metrics["zero_sales_pageviews"],
        "continue_sales_revenue": activity_metrics["continue_sales_revenue"],
        "discontinue_sales_revenue": activity_metrics["discontinue_sales_revenue"],
        "unmapped_fsn_revenue": activity_metrics["unmapped_fsn_revenue"],
        "continue_sku_count": activity_metrics["continue_sku_count"],
        "discontinued_sku_count": activity_metrics["discontinued_sku_count"],
        "unmapped_fsn_count": activity_metrics["unmapped_fsn_count"],
        "mapped_fsn_count": activity_metrics["continue_sku_count"] + activity_metrics["discontinued_sku_count"],
        "active_fsn_count": (
            activity_metrics["continue_sku_count"]
            + activity_metrics["discontinued_sku_count"]
            + activity_metrics["unmapped_fsn_count"]
        ),
    }

    # Previous-period KPI changes.
    qs_prev = get_prev_period_qs(qs, filters)
    fk_prev = get_prev_period_qs(fk_qs, filters)
    if summary_base_qs is not None:
        summary_prev = get_prev_period_qs(summary_base_qs, filters)
        prev_summary_metrics = _summary_metrics_by_platform(summary_prev)
        prev_totals = _combined_metrics(
            prev_summary_metrics["Amazon"],
            prev_summary_metrics["Flipkart"],
        )
    else:
        prev_totals = _combined_metrics(
            _aggregate_metrics(qs_prev),
            _aggregate_metrics(fk_prev),
        )
    prev_roas = calculate_roas(prev_totals["revenue"], prev_totals["total_spend"])
    prev_tacos = calculate_tacos(prev_totals["revenue"], prev_totals["total_spend"])
    prev_values = {
        "revenue": prev_totals["revenue"],
        "orders": prev_totals["orders"],
        "units": prev_totals["units"],
        "spend": prev_totals["total_spend"],
        "roas": prev_roas,
        "tacos": prev_tacos,
    }
    for key in ["orders", "units", "spend", "roas", "tacos"]:
        kpis[f"{key}_change"] = _safe_growth(kpis.get(key, 0), prev_values.get(key, 0))

    if summary_base_qs is not None:
        data_anchor_date = summary_base_qs.aggregate(m=Max("date")).get("m")
    else:
        max_qs = qs.aggregate(m=Max("date"))["m"] if qs is not None else None
        max_fk = fk_qs.aggregate(m=Max("date"))["m"] if fk_qs is not None else None
        latest_dates = [d for d in (max_qs, max_fk) if d]
        data_anchor_date = max(latest_dates) if latest_dates else None
    data_anchor_date = data_anchor_date or datetime.date.today()

    date_range_val = str(filters.get("date_range") or "").strip()
    has_explicit_growth_period = bool(
        date_range_val
        or _parse_ymd_date(filters.get("start_date"))
        or _parse_ymd_date(filters.get("end_date"))
    )
    growth_ref_date = timezone.localdate() if has_explicit_growth_period else data_anchor_date
    cm_start, cm_end = resolve_growth_period(filters, growth_ref_date)
    pm_start = safe_shift_month(cm_start, -1)
    pm_end = safe_shift_month(cm_end, -1)
    ppm_start = safe_shift_month(cm_start, -2)
    ppm_end = safe_shift_month(cm_end, -2)
    yoy_cm_start = safe_replace_year(cm_start)
    yoy_cm_end = safe_replace_year(cm_end)
    yoy_pm_start = safe_replace_year(pm_start)
    yoy_pm_end = safe_replace_year(pm_end)

    growth_periods = {
        "cm": (cm_start, cm_end),
        "pm": (pm_start, pm_end),
        "ppm": (ppm_start, ppm_end),
        "yoy_cm": (yoy_cm_start, yoy_cm_end),
        "yoy_pm": (yoy_pm_start, yoy_pm_end),
    }
    from concurrent.futures import ThreadPoolExecutor
    if summary_base_qs is not None:
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_az = executor.submit(_batch_period_aggregates, summary_base_qs.filter(platform="Amazon"), growth_periods)
            f_fk = executor.submit(_batch_period_aggregates, summary_base_qs.filter(platform="Flipkart"), growth_periods)
            az_periods = f_az.result()
            fk_periods = f_fk.result()
    else:
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_az = executor.submit(_batch_period_aggregates, qs, growth_periods)
            f_fk = executor.submit(_batch_period_aggregates, fk_qs, growth_periods)
            az_periods = f_az.result()
            fk_periods = f_fk.result()

    cm_rev = az_periods["cm_rev"] + fk_periods["cm_rev"]
    pm_rev = az_periods["pm_rev"] + fk_periods["pm_rev"]
    ppm_rev = az_periods["ppm_rev"] + fk_periods["ppm_rev"]
    yoy_cm_rev = az_periods["yoy_cm_rev"] + fk_periods["yoy_cm_rev"]
    yoy_pm_rev = az_periods["yoy_pm_rev"] + fk_periods["yoy_pm_rev"]
    cm_spend = az_periods["cm_spend"] + fk_periods["cm_spend"]
    pm_spend = az_periods["pm_spend"] + fk_periods["pm_spend"]
    cm_snapshot = _build_period_snapshot(
        qs, fk_qs, cm_start, cm_end, user,
        asin_meta=asin_meta, fsn_meta=fsn_meta, include_activity_metrics=include_activity_metrics,
    )
    pm_snapshot = _build_period_snapshot(
        qs, fk_qs, pm_start, pm_end, user,
        asin_meta=asin_meta, fsn_meta=fsn_meta, include_activity_metrics=include_activity_metrics,
    )

    kpis.update(
        {
            "mom_growth": _safe_growth(cm_rev, pm_rev),
            "yoy_growth": _safe_growth(cm_rev, yoy_cm_rev),
            "az_mom_growth": _safe_growth(az_periods["cm_rev"], az_periods["pm_rev"]),
            "fk_mom_growth": _safe_growth(fk_periods["cm_rev"], fk_periods["pm_rev"]),
            "az_yoy_growth": _safe_growth(az_periods["cm_rev"], az_periods["yoy_cm_rev"]),
            "fk_yoy_growth": _safe_growth(fk_periods["cm_rev"], fk_periods["yoy_cm_rev"]),
            "prev_mom": _safe_growth(pm_rev, ppm_rev),
            "prev_yoy": _safe_growth(pm_rev, yoy_pm_rev),
            "mom_period_current_start": cm_start,
            "mom_period_current_end": cm_end,
            "mom_period_previous_start": pm_start,
            "mom_period_previous_end": pm_end,
            "yoy_period_previous_start": yoy_cm_start,
            "yoy_period_previous_end": yoy_cm_end,
            "mom_current_revenue": round(cm_rev, 2),
            "mom_previous_revenue": round(pm_rev, 2),
            "az_mom_current_revenue": round(az_periods["cm_rev"], 2),
            "az_mom_previous_revenue": round(az_periods["pm_rev"], 2),
            "fk_mom_current_revenue": round(fk_periods["cm_rev"], 2),
            "fk_mom_previous_revenue": round(fk_periods["pm_rev"], 2),
            "yoy_current_revenue": round(cm_rev, 2),
            "yoy_previous_revenue": round(yoy_cm_rev, 2),
            "az_yoy_current_revenue": round(az_periods["cm_rev"], 2),
            "az_yoy_previous_revenue": round(az_periods["yoy_cm_rev"], 2),
            "fk_yoy_current_revenue": round(fk_periods["cm_rev"], 2),
            "fk_yoy_previous_revenue": round(fk_periods["yoy_cm_rev"], 2),
            "mom_current_orders": int(cm_snapshot["az_orders"]),
            "mom_previous_orders": int(pm_snapshot["az_orders"]),
            "mom_current_units": int(cm_snapshot["units"]),
            "mom_previous_units": int(pm_snapshot["units"]),
            "mom_current_roas": cm_snapshot["roas"],
            "mom_previous_roas": pm_snapshot["roas"],
            "mom_current_tacos": cm_snapshot["tacos"],
            "mom_previous_tacos": pm_snapshot["tacos"],
            "mom_current_ad_spend_sku_count": int(cm_snapshot["ad_spend_sku_count"]),
            "mom_previous_ad_spend_sku_count": int(pm_snapshot["ad_spend_sku_count"]),
            "mom_current_selling_sku_count": int(cm_snapshot["selling_sku_count"]),
            "mom_previous_selling_sku_count": int(pm_snapshot["selling_sku_count"]),
            "mom_current_zero_selling_sku_count": int(cm_snapshot["zero_selling_sku_count"]),
            "mom_previous_zero_selling_sku_count": int(pm_snapshot["zero_selling_sku_count"]),
            "mom_spend_growth": _safe_growth(cm_spend, pm_spend),
            "mom_roas_change": round(calculate_roas(cm_rev, cm_spend) - calculate_roas(pm_rev, pm_spend), 2),
            "mom_tacos_change": round(calculate_tacos(cm_rev, cm_spend) - calculate_tacos(pm_rev, pm_spend), 1),
        }
    )

    kpis.update(
        {
            "ad_spend_sku_count": unique_counts["ad_spend_sku_count"],
            "az_ad_spend_sku_count": unique_counts["az_ad_spend_sku_count"],
            "fk_ad_spend_sku_count": unique_counts["fk_ad_spend_sku_count"],
            "ad_spend_variant_count": unique_counts["ad_spend_variant_count"],
            "ad_spend_sku_count_with_variants": unique_counts["ad_spend_sku_count_with_variants"],
            "advertised_asin_count": unique_counts["advertised_asin_count"],
            "advertised_variant_count": unique_counts["advertised_variant_count"],
            "advertised_asin_count_with_variants": unique_counts["advertised_asin_count_with_variants"],
            "selling_sku_count": activity_metrics["selling_sku_count"],
            "az_selling_sku_count": activity_metrics["az_selling_sku_count"],
            "fk_selling_sku_count": activity_metrics["fk_selling_sku_count"],
            "zero_selling_sku_count": activity_metrics["zero_selling_sku_count"],
            "az_zero_selling_sku_count": activity_metrics["az_zero_selling_sku_count"],
            "fk_zero_selling_sku_count": activity_metrics["fk_zero_selling_sku_count"],
            "zero_sales_pageviews": activity_metrics["zero_sales_pageviews"],
            "continue_sales_revenue": activity_metrics["continue_sales_revenue"],
            "discontinue_sales_revenue": activity_metrics["discontinue_sales_revenue"],
            "unmapped_fsn_revenue": activity_metrics["unmapped_fsn_revenue"],
            "continue_sku_count": activity_metrics["continue_sku_count"],
            "discontinued_sku_count": activity_metrics["discontinued_sku_count"],
            "unmapped_fsn_count": activity_metrics["unmapped_fsn_count"],
            "mapped_fsn_count": activity_metrics["continue_sku_count"] + activity_metrics["discontinued_sku_count"],
            "active_fsn_count": (
                activity_metrics["continue_sku_count"]
                + activity_metrics["discontinued_sku_count"]
                + activity_metrics["unmapped_fsn_count"]
            ),
        }
    )

    _ad_spend_sku_count = kpis.get("ad_spend_sku_count", 0)
    marketing = {
        "ad_spend": int(kpis["spend"]),
        "ad_spend_change": kpis.get("mom_spend_growth", 0),
        "roas": kpis["roas"],
        "roas_change_pct": kpis.get("mom_roas_change", 0),
        "tacos": kpis["tacos"],
        "tacos_change": kpis.get("mom_tacos_change", 0),
        "ad_spend_sku_count": _ad_spend_sku_count,
        "az_ad_spend_sku_count": kpis.get("az_ad_spend_sku_count", 0),
        "fk_ad_spend_sku_count": kpis.get("fk_ad_spend_sku_count", 0),
        "ad_spend_sku_count_with_variants": kpis.get("ad_spend_sku_count_with_variants", _ad_spend_sku_count),
        "ad_spend_variant_count": kpis.get("ad_spend_variant_count", 0),
        "advertised_asin_count": kpis.get("advertised_asin_count", kpis.get("az_ad_spend_sku_count", 0)),
        "advertised_variant_count": kpis.get("advertised_variant_count", kpis.get("ad_spend_variant_count", 0)),
        "advertised_asin_count_with_variants": kpis.get(
            "advertised_asin_count_with_variants",
            kpis.get("az_ad_spend_sku_count", 0) + kpis.get("ad_spend_variant_count", 0),
        ),
        "selling_sku_count": kpis.get("selling_sku_count", 0),
        "az_selling_sku_count": kpis.get("az_selling_sku_count", 0),
        "fk_selling_sku_count": kpis.get("fk_selling_sku_count", 0),
        "zero_selling_sku_count": kpis.get("zero_selling_sku_count", 0),
        "az_zero_selling_sku_count": kpis.get("az_zero_selling_sku_count", 0),
        "fk_zero_selling_sku_count": kpis.get("fk_zero_selling_sku_count", 0),
        "zero_sales_pageviews": kpis.get("zero_sales_pageviews", 0),
        "az_roas": kpis.get("az_roas", 0),
        "fk_roas": kpis.get("fk_roas", 0),
        "az_tacos": kpis.get("az_tacos", 0),
        "fk_tacos": kpis.get("fk_tacos", 0),
    }
    filter_meta = cached_filter_metadata or get_available_filters_orm(qs, fk_qs)
    payload = _empty_kpi_payload(kpis, marketing, filter_meta)

    if cache_key:
        cache.set(cache_key, payload, timeout=60 * 45)
        if have_lock:
            cache.delete(lock_key)

    return payload


def get_available_filters_orm(qs, fk_qs):
    """
    Build the 'filters' dict (asins, categories, fsns, portfolios, platforms,
    dates) from the querysets — replaces the Pandas get_available_filters().
    """

    def clean_qs_vals(qs, field):
        if qs is None:
            return []
        vals = (
            qs.exclude(**{f"{field}__isnull": True})
            .exclude(**{f"{field}": ""})
            .values_list(field, flat=True)
            .distinct()
        )
        return sorted(
            list(
                set(
                    str(v)
                    for v in vals
                    if v and str(v).strip() and str(v) not in ("nan", "None", "null")
                )
            )
        )

    asins = clean_qs_vals(qs, "asin") if qs is not None else []
    az_cats = clean_qs_vals(qs, "category") if qs is not None else []
    az_ports = clean_qs_vals(qs, "portfolio") if qs is not None else []
    az_subs = clean_qs_vals(qs, "subcategory") if qs is not None else []

    fsns = clean_qs_vals(fk_qs, "fsn") if fk_qs is not None else []
    fk_cats = clean_qs_vals(fk_qs, "category") if fk_qs is not None else []
    fk_ports = clean_qs_vals(fk_qs, "portfolio") if fk_qs is not None else []
    fk_subs = clean_qs_vals(fk_qs, "subcategory") if fk_qs is not None else []

    categories = sorted(set(az_cats) | set(fk_cats))
    portfolios = sorted(set(az_ports) | set(fk_ports))
    subcategories = sorted(set(az_subs) | set(fk_subs))

    def clean_years(q):
        if q is None:
            return []
        try:
            vals = q.exclude(date__isnull=True).values_list('date__year', flat=True).distinct()
            return [int(v) for v in vals if v]
        except Exception:
            return []

    az_years = clean_years(qs)
    fk_years = clean_years(fk_qs)
    years = sorted(list(set(az_years) | set(fk_years)), reverse=True)

    platforms = []
    if asins:
        platforms.append("Amazon")
    if fsns:
        platforms.append("Flipkart")

    return {
        "asins": asins,
        "fsns": fsns,
        "categories": categories,
        "portfolios": portfolios,
        "subcategories": subcategories,
        "platforms": platforms,
        "years": years,
        "dates": [],  # not used for UI dropdown
    }

def get_available_filters_orm_cached(qs, fk_qs, data_owner_id, show_amazon=True, show_flipkart=True):
    from apps.dashboard.services.cache_config import DASHBOARD_CACHE_SCHEMA_VERSION
    cache_key = f"dashboard_filters_v{DASHBOARD_CACHE_SCHEMA_VERSION}_{data_owner_id}_{show_amazon}_{show_flipkart}"
    filters = cache.get(cache_key)
    if filters:
        return filters
    filters = get_available_filters_orm(qs, fk_qs)
    
    # Ensure the platforms list always shows all platforms the user has data for,
    # so they can switch back after filtering by platform.
    from apps.dashboard.models import ProcessedDashboardData, FlipkartProcessedDashboardData
    platforms = []
    if ProcessedDashboardData.objects.filter(user_id=data_owner_id).exists():
        platforms.append("Amazon")
    if FlipkartProcessedDashboardData.objects.filter(user_id=data_owner_id).exists():
        platforms.append("Flipkart")
    filters["platforms"] = platforms
    
    cache.set(cache_key, filters, timeout=3600) # cache for 1 hour
    return filters




def _compute_inventory_summary(user, filters, platform_filter):
    from apps.dashboard.models import DashboardInventoryHealthSummary
    from django.db.models import Count, Sum
    
    total_lost_sales = 0.0
    
    inventory_position = []
    inventory = {
        "in_stock": 0, "low_stock": 0, "oos": 0, "overstock": 0,
        "amz_in_stock": 0, "amz_low_stock": 0, "amz_oos": 0, "amz_overstock": 0,
        "fk_in_stock": 0, "fk_low_stock": 0, "fk_oos": 0, "fk_overstock": 0,
        "details": [], "details_total": 0, "details_shown": 0,
        "details_truncated": False, "has_stock_data": False, "num_sale_days": 1,
    }
    
    oos_impact = {
        "lost_sales": 0.0, "skus_affected": 0, "orders_lost": 0,
        "az_lost_sales": 0.0, "fk_lost_sales": 0.0,
        "az_skus_affected": 0, "fk_skus_affected": 0,
        "az_orders_lost": 0, "fk_orders_lost": 0,
        "az_aov": 0.0, "fk_aov": 0.0,
        "selected_platform": platform_filter or "All",
        "lost_sales_rule": "", "sku_rule": "", "orders_rule": "",
        "formula": "Lost Sales = Amazon OOS revenue + Flipkart OOS revenue + Flipkart Nearly OOS revenue",
        "row_basis": "Rows come from the latest inventory-health summary after dashboard filters are applied.",
        "amazon_status_basis": "Amazon includes rows where status is OOS.",
        "flipkart_status_basis": "Flipkart includes rows where status is OOS or Nearly OOS.",
    }
    
    try:
        inv_sum_qs = DashboardInventoryHealthSummary.objects.filter(
            user=user, platform="Combined"
        )
        inv_sum_qs = apply_inventory_summary_filters(
            inv_sum_qs,
            user,
            filters,
            platform_filter,
        )

        _inv_agg = inv_sum_qs.aggregate(
            total=Count("id"),
            n_dates=Count("date", distinct=True),
        )
        summary_total_rows = _inv_agg["total"] or 0
        _inv_num_sale_days = max(_inv_agg["n_dates"] or 1, 1)
        
        if summary_total_rows > 0:
            amz_status_rows = []
            fk_status_rows = []
            if platform_filter != "Flipkart":
                amz_status_rows = inv_sum_qs.values("status").annotate(
                    cnt=Count("id"), rev=Sum("revenue")
                )
            if platform_filter != "Amazon":
                fk_status_rows = inv_sum_qs.values("fk_status").annotate(
                    cnt=Count("id"), rev=Sum("fk_revenue")
                )
            
            amz_status_count = {str(r["status"]): int(r["cnt"] or 0) for r in amz_status_rows if r["status"]}
            fk_status_count = {str(r["fk_status"]): int(r["cnt"] or 0) for r in fk_status_rows if r["fk_status"]}
            amz_status_rev = {str(r["status"]): float(r["rev"] or 0.0) for r in amz_status_rows if r["status"]}
            fk_status_rev = {str(r["fk_status"]): float(r["rev"] or 0.0) for r in fk_status_rows if r["fk_status"]}
            
            amz_in_stock = amz_status_count.get("In Stock", 0)
            amz_low_stock = amz_status_count.get("Low Stock", 0)
            amz_oos = amz_status_count.get("OOS", 0)
            amz_overstock = amz_status_count.get("Overstock", 0)

            fk_nearly_oos = fk_status_count.get("Nearly OOS", 0)
            fk_oos_only = fk_status_count.get("OOS", 0)
            fk_low_stock = fk_status_count.get("Understock", 0)
            fk_in_stock = fk_status_count.get("Ideal Stocking", 0)
            fk_overstock_1 = fk_status_count.get("Over Stock", 0)
            fk_overstock_2 = fk_status_count.get("Highly Over Stock", 0)
            fk_not_selling = fk_status_count.get("Not Selling", 0)

            fk_oos = fk_nearly_oos + fk_oos_only
            fk_overstock = fk_overstock_1 + fk_overstock_2 + fk_not_selling
            
            total_lost_sales = (
                amz_status_rev.get("OOS", 0.0)
                + fk_status_rev.get("OOS", 0.0)
                + fk_status_rev.get("Nearly OOS", 0.0)
            )
            az_lost_sales = float(amz_status_rev.get("OOS", 0.0))
            fk_lost_sales = float(fk_status_rev.get("OOS", 0.0) + fk_status_rev.get("Nearly OOS", 0.0))
            orders_estimate = _estimate_oos_orders_lost(
                az_lost_sales=az_lost_sales,
                fk_lost_sales=fk_lost_sales,
                platform_filter=platform_filter,
                user=user,
                filters=filters,
            )

            inventory.update({
                "in_stock": int(amz_in_stock + fk_in_stock),
                "low_stock": int(amz_low_stock + fk_low_stock),
                "oos": int(amz_oos + fk_oos),
                "overstock": int(amz_overstock + fk_overstock),
                "amz_in_stock": int(amz_in_stock),
                "amz_low_stock": int(amz_low_stock),
                "amz_oos": int(amz_oos),
                "amz_overstock": int(amz_overstock),
                "fk_in_stock": int(fk_in_stock),
                "fk_low_stock": int(fk_low_stock),
                "fk_oos": int(fk_oos),
                "fk_overstock": int(fk_overstock),
                "details_total": int(summary_total_rows),
                "has_stock_data": True,
                "num_sale_days": _inv_num_sale_days,
            })
            
            oos_impact.update({
                "lost_sales": float(total_lost_sales),
                "skus_affected": inventory["oos"],
                "az_lost_sales": az_lost_sales,
                "fk_lost_sales": fk_lost_sales,
                "az_skus_affected": int(amz_oos),
                "fk_skus_affected": int(fk_oos),
                "orders_lost": orders_estimate["orders_lost"],
                "az_orders_lost": orders_estimate["az_orders_lost"],
                "fk_orders_lost": orders_estimate["fk_orders_lost"],
                "az_aov": orders_estimate["az_aov"],
                "fk_aov": orders_estimate["fk_aov"],
                "lost_sales_rule": "Amazon OOS revenue + Flipkart OOS revenue + Flipkart Nearly OOS revenue.",
                "sku_rule": "SKUs Affected = Amazon OOS rows + Flipkart OOS/Nearly OOS rows after dashboard filters.",
                "orders_rule": orders_estimate["orders_rule"],
            })
            
            
            rev_in_stock = amz_status_rev.get("In Stock", 0.0) + fk_status_rev.get("Ideal Stocking", 0.0)
            rev_low_stock = amz_status_rev.get("Low Stock", 0.0) + fk_status_rev.get("Understock", 0.0)
            rev_overstock = (
                amz_status_rev.get("Overstock", 0.0) 
                + fk_status_rev.get("Over Stock", 0.0) 
                + fk_status_rev.get("Highly Over Stock", 0.0) 
                + fk_status_rev.get("Not Selling", 0.0)
            )
            rev_oos = (
                amz_status_rev.get("OOS", 0.0) 
                + fk_status_rev.get("OOS", 0.0) 
                + fk_status_rev.get("Nearly OOS", 0.0)
            )

            bucket_defs = [
                ("In Stock (15–60D)", rev_in_stock, "green", amz_status_rev.get("In Stock", 0.0), fk_status_rev.get("Ideal Stocking", 0.0)),
                ("Low Stock (<=15D)", rev_low_stock, "amber", amz_status_rev.get("Low Stock", 0.0), fk_status_rev.get("Understock", 0.0)),
                ("Overstock (>60D)", rev_overstock, "orange", amz_status_rev.get("Overstock", 0.0), fk_status_rev.get("Over Stock", 0.0) + fk_status_rev.get("Highly Over Stock", 0.0) + fk_status_rev.get("Not Selling", 0.0)),
                ("Out of Stock", rev_oos, "red", amz_status_rev.get("OOS", 0.0), fk_status_rev.get("OOS", 0.0) + fk_status_rev.get("Nearly OOS", 0.0)),
            ]
            
            tracked_rev = rev_in_stock + rev_low_stock + rev_overstock + rev_oos
            pct_den = tracked_rev
            
            for b_name, b_val, b_color, az_val, fk_val in bucket_defs:
                pct = round(b_val / pct_den * 100, 1) if pct_den > 0 else 0
                inventory_position.append({
                    "name": b_name,
                    "label": b_name,
                    "revenue": round(b_val, 2),
                    "az_revenue": round(az_val, 2),
                    "fk_revenue": round(fk_val, 2),
                    "pct": pct,
                    "color": b_color,
                })
                
    except Exception:
        pass
        
    return inventory, oos_impact, inventory_position


def run_orm_computation(
    qs,
    fk_qs,
    spend_qs,
    filters,
    user,
    cached_filter_metadata=None,
    include_full_payload=False,
    compute_scope="full",
    cache_identity=None,
    section_scope="all",
    dashboard_view=None,
):
    # 1. Apply date filters
    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)
    summary_base_qs = _get_daily_summary_base_qs(user, filters)
    summary_qs_f = apply_global_filters_orm(summary_base_qs, filters)
    product_summary_az_base, product_summary_fk_base = _get_product_daily_summary_querysets(
        user, filters, apply_date_filter=False
    )
    product_summary_az_f = apply_global_filters_orm(product_summary_az_base, filters)
    product_summary_fk_f = apply_global_filters_orm(product_summary_fk_base, filters)
    if not include_full_payload and summary_qs_f is not None:
        _ex_key = (
            f"dash_sum_ex_v1_{user.id}"
            f"_{(cache_identity or {}).get('data_version', 0)}"
            f"_{(cache_identity or {}).get('filter_hash', '')}"
        )
        _ex = cache.get(_ex_key)
        if _ex is None:
            _ex = summary_qs_f.exists()
            cache.set(_ex_key, _ex, timeout=300)
        use_summary_rollups = bool(_ex)
    else:
        use_summary_rollups = False
    summary_kpi_payload = None
    normalized_section_scope = str(section_scope or "all").lower()
    normalized_dashboard_view = str(dashboard_view or "").lower()
    # Only skip activity metrics (selling SKUs, 0-sales SKU, continue/discontinue)
    # for the visuals section — they are lightweight and needed on all other scopes.
    # The details section uses these KPIs, so must NOT be excluded.
    include_activity_metrics = not (
        normalized_section_scope == "visuals"
        and normalized_dashboard_view in {"ceo", "category"}
    )
    # Skip the heavy top/declining/NPD product blocks ONLY when section scope is
    # visuals or kpis — these sections never render those widgets.
    skip_product_tables = normalized_section_scope in {"visuals", "kpis"}

    if str(compute_scope or "full").lower() == "kpis":
        return run_kpi_only_computation(
            qs,
            fk_qs,
            spend_qs,
            filters,
            user,
            cached_filter_metadata=cached_filter_metadata,
            cache_identity=cache_identity,
            include_activity_metrics=True,
        )

    # 2. Get prev-period querysets
    qs_prev = get_prev_period_qs(qs, filters)
    fk_prev = get_prev_period_qs(fk_qs, filters)
    qs_prev_f = apply_global_filters_orm(qs_prev, {}) if qs_prev is not None else None
    fk_prev_f = apply_global_filters_orm(fk_prev, {}) if fk_prev is not None else None

    # ── Pre-fetch category/portfolio metadata ONCE for both current + prev periods ──
    # Uses Redis-cached helpers (300 s TTL) so repeated filter changes within the
    # same session never re-hit the DB for the same static mapping tables.
    from concurrent.futures import ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=2) as executor:
        f_asin_meta = executor.submit(_get_asin_meta_cached, user) if qs is not None else None
        f_fsn_meta = executor.submit(_get_fsn_meta_cached, user) if fk_qs is not None else None
        _asin_meta = f_asin_meta.result() if f_asin_meta else {}
        _fsn_meta = f_fsn_meta.result() if f_fsn_meta else {}

    # ── Master table data (used to eliminate duplicate DB hits) ──
    if use_summary_rollups:
        summary_kpi_payload = run_kpi_only_computation(
            qs,
            fk_qs,
            spend_qs,
            filters,
            user,
            cached_filter_metadata=cached_filter_metadata,
            cache_identity=cache_identity,
            include_activity_metrics=include_activity_metrics,
        )
        table_data = []
    elif normalized_section_scope == "visuals":
        table_data = None
    else:
        table_data = generate_bi_data_orm(
            product_summary_az_f, product_summary_fk_f, user=user, asin_meta=_asin_meta, fsn_meta=_fsn_meta
        )

    # ── Master prev table data for growth calculations ──
    table_data_prev = []
    prev_rev_by_port = {}
    prev_rev_by_cat = {}
    prev_az_rev = 0.0
    prev_fk_rev = 0.0
    prev_kpis_totals = None

    if use_summary_rollups:
        summary_prev = get_prev_period_qs(summary_base_qs, filters)
        # Single GROUP BY (platform, portfolio, category) replaces 3 separate queries:
        # _summary_metrics_by_platform + 2× _summary_revenue_by_dimension.
        for _row in summary_prev.values("platform", "portfolio", "category").annotate(rev=Sum("revenue")):
            _rev = float(_row.get("rev") or 0)
            _port_key = str(_row.get("portfolio") or "Unknown")
            _cat_key = str(_row.get("category") or "Unknown")
            prev_rev_by_port[_port_key] = prev_rev_by_port.get(_port_key, 0) + _rev
            prev_rev_by_cat[_cat_key] = prev_rev_by_cat.get(_cat_key, 0) + _rev
            if _row.get("platform") == "Amazon":
                prev_az_rev += _rev
            elif _row.get("platform") == "Flipkart":
                prev_fk_rev += _rev
    elif qs_prev_f is not None or fk_prev_f is not None:
        # Use a single low-cardinality GROUP BY (portfolio, category) per platform instead of
        # a full per-ASIN GROUP BY via generate_bi_data_orm — reduces scan cost 10-30× for
        # large date ranges while providing all the data downstream code actually needs.
        _prev_orders = _prev_units = _prev_spend = 0
        _prev_fk_orders = _prev_fk_units = _prev_fk_spend = 0
        if qs_prev_f is not None:
            for _r in qs_prev_f.values("portfolio", "category").annotate(
                rev=Sum("revenue"), spend=Sum("total_spend"),
                ord=Sum("orders"), u=Sum("units"),
            ):
                _rev = float(_r.get("rev") or 0)
                prev_az_rev += _rev
                _prev_spend += float(_r.get("spend") or 0)
                _prev_orders += int(_r.get("ord") or 0)
                _prev_units += int(_r.get("u") or 0)
                _port = str(_r.get("portfolio") or "Unknown")
                _cat = str(_r.get("category") or "Unknown")
                prev_rev_by_port[_port] = prev_rev_by_port.get(_port, 0) + _rev
                prev_rev_by_cat[_cat] = prev_rev_by_cat.get(_cat, 0) + _rev
        if fk_prev_f is not None:
            for _r in fk_prev_f.values("portfolio", "category").annotate(
                rev=Sum("revenue"), spend=Sum("total_spend"),
                ord=Sum("orders"), u=Sum("units"),
            ):
                _rev = float(_r.get("rev") or 0)
                prev_fk_rev += _rev
                _prev_fk_spend += float(_r.get("spend") or 0)
                _prev_fk_orders += int(_r.get("ord") or 0)
                _prev_fk_units += int(_r.get("u") or 0)
                _port = str(_r.get("portfolio") or "Unknown")
                _cat = str(_r.get("category") or "Unknown")
                prev_rev_by_port[_port] = prev_rev_by_port.get(_port, 0) + _rev
                prev_rev_by_cat[_cat] = prev_rev_by_cat.get(_cat, 0) + _rev
        _prev_total_spend = _prev_spend + _prev_fk_spend
        prev_kpis_totals = {
            "revenue": prev_az_rev + prev_fk_rev,
            "orders": _prev_orders + _prev_fk_orders,
            "units": _prev_units + _prev_fk_units,
            "total_spend": _prev_total_spend,
        }

    prev_portfolio_revenue = {}
    _merge_portfolio_revenue_from_summary(
        prev_portfolio_revenue,
        get_prev_period_qs(product_summary_az_base, filters),
        "asin",
        _asin_meta,
    )
    _merge_portfolio_revenue_from_summary(
        prev_portfolio_revenue,
        get_prev_period_qs(product_summary_fk_base, filters),
        "fsn",
        _fsn_meta,
    )
    if prev_portfolio_revenue:
        prev_rev_by_port = prev_portfolio_revenue

    if use_summary_rollups:
        # Reuse kpis already computed inside run_kpi_only_computation — avoids
        # re-issuing _summary_metrics_by_platform(summary_qs_f) (one DB round-trip).
        _skpi = dict(summary_kpi_payload.get("kpis") or {}) if summary_kpi_payload else {}
        total_revenue = float(_skpi.get("revenue") or 0)
        total_spend = float(_skpi.get("spend") or 0)
        kpis = _skpi
        kpis["active_asins"] = len(table_data)
    else:
        total_revenue = sum(r["revenue"] for r in (table_data or []))
        total_spend = sum(r["total_spend"] for r in (table_data or []))
        _az_pageviews = sum(r.get("az_pageviews", 0) for r in (table_data or []))
        _fk_pageviews = sum(r.get("fk_pageviews", 0) for r in (table_data or []))
        _total_pageviews = sum(r["pageviews"] for r in (table_data or []))
        kpis = {
            "revenue": total_revenue,
            "az_revenue": sum(r.get("az_revenue", 0) for r in (table_data or [])),
            "fk_revenue": sum(r.get("fk_revenue", 0) for r in (table_data or [])),
            "orders": sum(r["orders"] for r in (table_data or [])),
            "az_orders": sum(r.get("az_orders", 0) for r in (table_data or [])),
            "fk_orders": sum(r.get("fk_orders", 0) for r in (table_data or [])),
            "units": sum(r["units"] for r in (table_data or [])),
            "az_units": sum(r.get("az_units", 0) for r in (table_data or [])),
            "fk_units": sum(r.get("fk_units", 0) for r in (table_data or [])),
            "pageviews": _total_pageviews,
            "page_views": _total_pageviews,  # alias used by templates
            "az_pageviews": _az_pageviews,
            "fk_pageviews": _fk_pageviews,
            "spend": total_spend,
            "az_spend": sum(r.get("az_spend", 0) for r in (table_data or [])),
            "fk_spend": sum(r.get("fk_spend", 0) for r in (table_data or [])),
            "active_asins": len(table_data or []),
        }

    platform_filter = (filters.get("platform") or "").strip()
    roas = calculate_roas(total_revenue, kpis["spend"])
    flipkart_cvr_mode = platform_filter == "Flipkart" or (
        not platform_filter and kpis["az_units"] == 0 and kpis["fk_units"] > 0
    )
    if flipkart_cvr_mode:
        conversion = flipkart_cvr(kpis["units"], kpis["pageviews"])
    else:
        conversion = amazon_cvr(kpis["orders"], kpis["pageviews"])
    tacos = calculate_tacos(total_revenue, kpis["spend"])
    az_roas = round(calculate_roas(kpis.get("az_revenue", 0), kpis.get("az_spend", 0)), 2)
    fk_roas = round(calculate_roas(kpis.get("fk_revenue", 0), kpis.get("fk_spend", 0)), 2)
    az_tacos = round(calculate_tacos(kpis.get("az_revenue", 0), kpis.get("az_spend", 0)), 2)
    fk_tacos = round(calculate_tacos(kpis.get("fk_revenue", 0), kpis.get("fk_spend", 0)), 2)
    if not use_summary_rollups:
        if table_data and include_activity_metrics:
            # _extract_kpi_metrics_from_grouped_data builds spend sets AND activity metrics
            # in a single pass over table_data — avoids separate DB aggregations.
            current_activity_metrics, az_asins_with_spend, fk_fsns_with_spend = (
                _extract_kpi_metrics_from_grouped_data(table_data, _fsn_meta)
            )
        else:
            # No table_data or activity metrics not needed — build spend sets manually
            # and fall back to DB-backed computation.
            az_asins_with_spend = set()
            fk_fsns_with_spend = set()
            for row in (table_data or []):
                sku = str(row.get("asin", "")).strip()
                if not sku:
                    continue
                if row.get("az_spend", 0.0) > 0:
                    az_asins_with_spend.add(sku)
                if row.get("fk_spend", 0.0) > 0:
                    fk_fsns_with_spend.add(sku)
            current_activity_metrics = _empty_activity_metrics()

        if include_activity_metrics:
            _status_counts, _status_revenue = _continue_discontinue_metrics_from_summary(
                fk_qs_f,
                _fsn_meta,
            )
            current_activity_metrics.update({
                "continue_sales_revenue": round(_status_revenue["Continued"], 2),
                "discontinue_sales_revenue": round(_status_revenue["Discontinued"], 2),
                "unmapped_fsn_revenue": round(_status_revenue["Unmapped"], 2),
                "continue_sku_count": _status_counts["Continued"],
                "discontinued_sku_count": _status_counts["Discontinued"],
                "unmapped_fsn_count": _status_counts["Unmapped"],
            })

        current_unique_counts = _compute_unique_ad_spend_sku_counts(
            qs_f, fk_qs_f, user, asin_meta=_asin_meta, filters=filters,
            az_asins=az_asins_with_spend, fk_fsns=fk_fsns_with_spend
        )

        kpis.update({
            "roas": round(roas, 2),
            "conversion": round(conversion, 2),
            "tacos": round(tacos, 2),
            "az_roas": az_roas,
            "fk_roas": fk_roas,
            "az_tacos": az_tacos,
            "fk_tacos": fk_tacos,
            "ad_spend_sku_count": current_unique_counts["ad_spend_sku_count"],
            "az_ad_spend_sku_count": current_unique_counts["az_ad_spend_sku_count"],
            "fk_ad_spend_sku_count": current_unique_counts["fk_ad_spend_sku_count"],
            "ad_spend_variant_count": current_unique_counts["ad_spend_variant_count"],
            "ad_spend_sku_count_with_variants": current_unique_counts["ad_spend_sku_count_with_variants"],
            "advertised_asin_count": current_unique_counts["advertised_asin_count"],
            "advertised_variant_count": current_unique_counts["advertised_variant_count"],
            "advertised_asin_count_with_variants": current_unique_counts["advertised_asin_count_with_variants"],
            "selling_sku_count": current_activity_metrics["selling_sku_count"],
            "az_selling_sku_count": current_activity_metrics["az_selling_sku_count"],
            "fk_selling_sku_count": current_activity_metrics["fk_selling_sku_count"],
            "zero_selling_sku_count": current_activity_metrics["zero_selling_sku_count"],
            "az_zero_selling_sku_count": current_activity_metrics["az_zero_selling_sku_count"],
            "fk_zero_selling_sku_count": current_activity_metrics["fk_zero_selling_sku_count"],
            "zero_sales_pageviews": current_activity_metrics.get("zero_sales_pageviews", 0),
            "continue_sales_revenue": current_activity_metrics["continue_sales_revenue"],
            "discontinue_sales_revenue": current_activity_metrics["discontinue_sales_revenue"],
            "unmapped_fsn_revenue": current_activity_metrics["unmapped_fsn_revenue"],
            "continue_sku_count": current_activity_metrics["continue_sku_count"],
            "discontinued_sku_count": current_activity_metrics["discontinued_sku_count"],
            "unmapped_fsn_count": current_activity_metrics["unmapped_fsn_count"],
            "mapped_fsn_count": current_activity_metrics["continue_sku_count"] + current_activity_metrics["discontinued_sku_count"],
            "active_fsn_count": (
                current_activity_metrics["continue_sku_count"]
                + current_activity_metrics["discontinued_sku_count"]
                + current_activity_metrics["unmapped_fsn_count"]
            ),
        })
    else:
        # these will be overwritten below by summary_kpi_payload anyway
        kpis.update({
            "roas": round(roas, 2),
            "conversion": round(conversion, 2),
            "tacos": round(tacos, 2),
            "az_roas": az_roas,
            "fk_roas": fk_roas,
            "az_tacos": az_tacos,
            "fk_tacos": fk_tacos,
        })

    # Derive prev-period KPIs from summary rollups when available; otherwise
    # fall back to the already-fetched previous-period SKU table.
    if prev_kpis_totals is not None:
        kpis_prev = {
            "revenue": prev_kpis_totals["revenue"],
            "orders": prev_kpis_totals["orders"],
            "units": prev_kpis_totals["units"],
            "spend": prev_kpis_totals["total_spend"],
            "roas": calculate_roas(
                prev_kpis_totals["revenue"], prev_kpis_totals["total_spend"]
            ),
            "tacos": calculate_tacos(
                prev_kpis_totals["revenue"], prev_kpis_totals["total_spend"]
            ),
        }
    elif table_data_prev:
        _prev_rev   = sum(r["revenue"]     for r in table_data_prev)
        _prev_spend = sum(r["total_spend"] for r in table_data_prev)
        kpis_prev = {
            "revenue": _prev_rev,
            "orders":  sum(r["orders"] for r in table_data_prev),
            "units":   sum(r["units"]   for r in table_data_prev),
            "spend":   _prev_spend,
            "roas":    calculate_roas(_prev_rev, _prev_spend),
            "tacos":   calculate_tacos(_prev_rev, _prev_spend),
        }
    else:
        kpis_prev = {"revenue": 0, "orders": 0, "units": 0, "spend": 0, "roas": 0, "tacos": 0}

    for key in ["orders", "units", "spend", "roas", "tacos"]:
        curr = kpis.get(key, 0)
        prev = kpis_prev.get(key, 0)
        kpis[f"{key}_change"] = _safe_growth(curr, prev)

    if use_summary_rollups:
        data_anchor_date = summary_base_qs.aggregate(m=Max("date")).get("m")
    else:
        max_qs = qs.aggregate(m=Max("date"))["m"] if qs is not None else None
        max_fk = fk_qs.aggregate(m=Max("date"))["m"] if fk_qs is not None else None
        latest_dates = [d for d in (max_qs, max_fk) if d]
        data_anchor_date = max(latest_dates) if latest_dates else None
    data_anchor_date = data_anchor_date or datetime.date.today()

    date_range_val = str(filters.get("date_range") or "").strip()
    has_explicit_growth_period = bool(
        date_range_val
        or _parse_ymd_date(filters.get("start_date"))
        or _parse_ymd_date(filters.get("end_date"))
    )
    # With no explicit date filter, anchor MOM/YOY to the latest available data date
    # so stale uploads don't silently produce zeroed growth.
    growth_ref_date = timezone.localdate() if has_explicit_growth_period else data_anchor_date
    cm_start, cm_end = resolve_growth_period(filters, growth_ref_date)
    pm_start = safe_shift_month(cm_start, -1)
    pm_end = safe_shift_month(cm_end, -1)
    ppm_start = safe_shift_month(cm_start, -2)
    ppm_end = safe_shift_month(cm_end, -2)

    yoy_cm_start = safe_replace_year(cm_start)
    yoy_cm_end = safe_replace_year(cm_end)
    yoy_pm_start = safe_replace_year(pm_start)
    yoy_pm_end = safe_replace_year(pm_end)

    # When use_summary_rollups, all growth KPIs are already in summary_kpi_payload
    # (computed by run_kpi_only_computation). Skip the two _batch_period_aggregates
    # DB round-trips and the downstream growth KPI calculations — they would just
    # be overwritten at the kpis-override below anyway.
    if not use_summary_rollups:
        _growth_periods = {
            "cm": (cm_start, cm_end),
            "pm": (pm_start, pm_end),
            "ppm": (ppm_start, ppm_end),
            "yoy_cm": (yoy_cm_start, yoy_cm_end),
            "yoy_pm": (yoy_pm_start, yoy_pm_end),
        }
        from concurrent.futures import ThreadPoolExecutor
        with ThreadPoolExecutor(max_workers=2) as executor:
            f_az = executor.submit(_batch_period_aggregates, qs, _growth_periods)
            f_fk = executor.submit(_batch_period_aggregates, fk_qs, _growth_periods)
            az_periods = f_az.result()
            fk_periods = f_fk.result()

        cm_rev = az_periods["cm_rev"] + fk_periods["cm_rev"]
        pm_rev = az_periods["pm_rev"] + fk_periods["pm_rev"]
        ppm_rev = az_periods["ppm_rev"] + fk_periods["ppm_rev"]
        yoy_cm_rev = az_periods["yoy_cm_rev"] + fk_periods["yoy_cm_rev"]
        yoy_pm_rev = az_periods["yoy_pm_rev"] + fk_periods["yoy_pm_rev"]

        cm_az_rev = az_periods["cm_rev"]
        pm_az_rev = az_periods["pm_rev"]
        yoy_cm_az_rev = az_periods["yoy_cm_rev"]
        cm_fk_rev = fk_periods["cm_rev"]
        pm_fk_rev = fk_periods["pm_rev"]
        yoy_cm_fk_rev = fk_periods["yoy_cm_rev"]

        cm_spend = az_periods["cm_spend"] + fk_periods["cm_spend"]
        pm_spend = az_periods["pm_spend"] + fk_periods["pm_spend"]
        cm_snapshot = _build_period_snapshot(
            qs, fk_qs, cm_start, cm_end, user,
            fsn_meta=_fsn_meta, include_activity_metrics=include_activity_metrics,
        )
        pm_snapshot = _build_period_snapshot(
            qs, fk_qs, pm_start, pm_end, user,
            fsn_meta=_fsn_meta, include_activity_metrics=include_activity_metrics,
        )

        kpis["mom_growth"] = _safe_growth(cm_rev, pm_rev)
        kpis["yoy_growth"] = _safe_growth(cm_rev, yoy_cm_rev)
        kpis["az_mom_growth"] = _safe_growth(cm_az_rev, pm_az_rev)
        kpis["fk_mom_growth"] = _safe_growth(cm_fk_rev, pm_fk_rev)
        kpis["az_yoy_growth"] = _safe_growth(cm_az_rev, yoy_cm_az_rev)
        kpis["fk_yoy_growth"] = _safe_growth(cm_fk_rev, yoy_cm_fk_rev)
        kpis["prev_mom"] = _safe_growth(pm_rev, ppm_rev)
        kpis["prev_yoy"] = _safe_growth(pm_rev, yoy_pm_rev)
        kpis["mom_period_current_start"] = cm_start
        kpis["mom_period_current_end"] = cm_end
        kpis["mom_period_previous_start"] = pm_start
        kpis["mom_period_previous_end"] = pm_end
        kpis["yoy_period_previous_start"] = yoy_cm_start
        kpis["yoy_period_previous_end"] = yoy_cm_end
        kpis["mom_current_revenue"] = round(cm_rev, 2)
        kpis["mom_previous_revenue"] = round(pm_rev, 2)
        kpis["az_mom_current_revenue"] = round(cm_az_rev, 2)
        kpis["az_mom_previous_revenue"] = round(pm_az_rev, 2)
        kpis["fk_mom_current_revenue"] = round(cm_fk_rev, 2)
        kpis["fk_mom_previous_revenue"] = round(pm_fk_rev, 2)
        kpis["yoy_current_revenue"] = round(cm_rev, 2)
        kpis["yoy_previous_revenue"] = round(yoy_cm_rev, 2)
        kpis["az_yoy_current_revenue"] = round(cm_az_rev, 2)
        kpis["az_yoy_previous_revenue"] = round(yoy_cm_az_rev, 2)
        kpis["fk_yoy_current_revenue"] = round(cm_fk_rev, 2)
        kpis["fk_yoy_previous_revenue"] = round(yoy_cm_fk_rev, 2)
        kpis["mom_current_orders"] = int(cm_snapshot["az_orders"])
        kpis["mom_previous_orders"] = int(pm_snapshot["az_orders"])
        kpis["mom_current_units"] = int(cm_snapshot["units"])
        kpis["mom_previous_units"] = int(pm_snapshot["units"])
        kpis["mom_current_roas"] = cm_snapshot["roas"]
        kpis["mom_previous_roas"] = pm_snapshot["roas"]
        kpis["mom_current_tacos"] = cm_snapshot["tacos"]
        kpis["mom_previous_tacos"] = pm_snapshot["tacos"]
        kpis["mom_current_ad_spend_sku_count"] = int(cm_snapshot["ad_spend_sku_count"])
        kpis["mom_previous_ad_spend_sku_count"] = int(pm_snapshot["ad_spend_sku_count"])
        kpis["mom_current_selling_sku_count"] = int(cm_snapshot["selling_sku_count"])
        kpis["mom_previous_selling_sku_count"] = int(pm_snapshot["selling_sku_count"])
        kpis["mom_current_zero_selling_sku_count"] = int(cm_snapshot["zero_selling_sku_count"])
        kpis["mom_previous_zero_selling_sku_count"] = int(pm_snapshot["zero_selling_sku_count"])
        kpis["mom_spend_growth"] = _safe_growth(cm_spend, pm_spend)
        cm_roas = calculate_roas(cm_rev, cm_spend)
        pm_roas = calculate_roas(pm_rev, pm_spend)
        kpis["mom_roas_change"] = round(cm_roas - pm_roas, 2)
        cm_tacos = calculate_tacos(cm_rev, cm_spend)
        pm_tacos = calculate_tacos(pm_rev, pm_spend)
        kpis["mom_tacos_change"] = round(cm_tacos - pm_tacos, 1)

    # Used by forecast and other sections that should anchor to data freshness.
    today = data_anchor_date

    if not use_summary_rollups:
        _ad_spend_sku_count = kpis.get("ad_spend_sku_count", 0)
        marketing = {
            "ad_spend": int(kpis.get("spend", 0)),
            "ad_spend_change": kpis.get("mom_spend_growth", 0),
            "roas": kpis.get("roas", 0),
            "roas_change_pct": kpis.get("mom_roas_change", 0),
            "tacos": kpis.get("tacos", 0),
            "tacos_change": kpis.get("mom_tacos_change", 0),
            "ad_spend_sku_count": _ad_spend_sku_count,
            "az_ad_spend_sku_count": kpis.get("az_ad_spend_sku_count", 0),
            "fk_ad_spend_sku_count": kpis.get("fk_ad_spend_sku_count", 0),
            "ad_spend_sku_count_with_variants": kpis.get("ad_spend_sku_count_with_variants", _ad_spend_sku_count),
            "ad_spend_variant_count": kpis.get("ad_spend_variant_count", 0),
            "advertised_asin_count": kpis.get("advertised_asin_count", kpis.get("az_ad_spend_sku_count", 0)),
            "advertised_variant_count": kpis.get("advertised_variant_count", kpis.get("ad_spend_variant_count", 0)),
            "advertised_asin_count_with_variants": kpis.get(
                "advertised_asin_count_with_variants",
                kpis.get("az_ad_spend_sku_count", 0) + kpis.get("ad_spend_variant_count", 0),
            ),
            "selling_sku_count": kpis.get("selling_sku_count", 0),
            "az_selling_sku_count": kpis.get("az_selling_sku_count", 0),
            "fk_selling_sku_count": kpis.get("fk_selling_sku_count", 0),
            "zero_selling_sku_count": kpis.get("zero_selling_sku_count", 0),
            "az_zero_selling_sku_count": kpis.get("az_zero_selling_sku_count", 0),
            "fk_zero_selling_sku_count": kpis.get("fk_zero_selling_sku_count", 0),
            "zero_sales_pageviews": kpis.get("zero_sales_pageviews", 0),
            "az_roas": kpis.get("az_roas", 0),
            "fk_roas": kpis.get("fk_roas", 0),
            "az_tacos": kpis.get("az_tacos", 0),
            "fk_tacos": kpis.get("fk_tacos", 0),
        }
    else:
        marketing = {}

    if use_summary_rollups and summary_kpi_payload:
        kpis = dict(summary_kpi_payload.get("kpis") or {})
        marketing = dict(summary_kpi_payload.get("marketing") or {})

    if str(compute_scope or "full").lower() == "kpis":
        inventory_summary, oos_impact, _inventory_position = _compute_inventory_summary(
            user, filters, filters.get("platform", "")
        )
        return {
            "_compute_scope": "kpis",
            "kpis": kpis,
            "charts": {},
            "category_performance": [],
            "platforms": {},
            "filters": cached_filter_metadata or get_available_filters_orm(qs, fk_qs),
            "oos_impact": oos_impact,
            "inventory": inventory_summary,
            "inventory_position": [],
            "forecast": {
                "predicted": 0.0,
                "target": 0.0,
                "gap": 0.0,
                "gap_pct": 0.0,
                "labels": [],
                "actual": [],
                "forecast": [],
                "target_line": [],
                "details": [],
                "daily_rate": 0.0,
                "days_in_month": 0,
                "days_elapsed": 0,
            },
            "priorities": [],
            "marketing": marketing,
            "cluster_performance": [],
            "cat_top_products": [],
            "cat_under_products": [],
            "cat_all_top_products": [],
            "cat_all_under_products": [],
            "growth_opportunities": [],
        }

    # 5. Charts
    if normalized_section_scope == "details":
        charts = {}
    elif use_summary_rollups:
        charts = _summary_charts_data(summary_qs_f)
    else:
        preaggregated_trend = None
        if summary_qs_f is not None:
            try:
                preaggregated_trend = _summary_trend_map(summary_qs_f)
            except Exception:
                preaggregated_trend = None

        charts = generate_charts_data_orm(
            qs_f, fk_qs_f, table_data=table_data, preaggregated_trend=preaggregated_trend
        )

    # 6. Platform breakdown
    az_rev = kpis.get("az_revenue", 0)
    fk_rev = kpis.get("fk_revenue", 0)
    platforms_dict = {}
    if az_rev > 0:
        platforms_dict["Amazon"] = {
            "revenue": az_rev,
            "pct": round(az_rev / total_revenue * 100, 1) if total_revenue > 0 else 0,
            "growth": _safe_growth(az_rev, prev_az_rev),
        }
    if fk_rev > 0:
        platforms_dict["Flipkart"] = {
            "revenue": fk_rev,
            "pct": round(fk_rev / total_revenue * 100, 1) if total_revenue > 0 else 0,
            "growth": _safe_growth(fk_rev, prev_fk_rev),
        }

    # 7. Category performance
    if use_summary_rollups:
        cat_perf_dict = {}
        for _row in summary_qs_f.values("platform", "category").annotate(rev=Sum("revenue")):
            cat = str(_row.get("category") or "Unknown")
            platform = _row.get("platform")
            revenue = float(_row.get("rev") or 0.0)
            item = cat_perf_dict.setdefault(
                cat,
                {
                    "name": cat,
                    "amazon_revenue": 0.0,
                    "flipkart_revenue": 0.0,
                    "revenue": 0.0,
                },
            )
            if platform == "Amazon":
                item["amazon_revenue"] += revenue
            elif platform == "Flipkart":
                item["flipkart_revenue"] += revenue
            item["revenue"] += revenue
    elif table_data is None:
        cat_perf_dict = {}
        if qs_f is not None:
            for _row in qs_f.values("category").annotate(rev=Sum("revenue")):
                cat = str(_row.get("category") or "Unknown")
                revenue = float(_row.get("rev") or 0.0)
                item = cat_perf_dict.setdefault(cat, {"name": cat, "amazon_revenue": 0.0, "flipkart_revenue": 0.0, "revenue": 0.0})
                item["amazon_revenue"] += revenue
                item["revenue"] += revenue
        if fk_qs_f is not None:
            for _row in fk_qs_f.values("category").annotate(rev=Sum("revenue")):
                cat = str(_row.get("category") or "Unknown")
                revenue = float(_row.get("rev") or 0.0)
                item = cat_perf_dict.setdefault(cat, {"name": cat, "amazon_revenue": 0.0, "flipkart_revenue": 0.0, "revenue": 0.0})
                item["flipkart_revenue"] += revenue
                item["revenue"] += revenue
    else:
        cat_perf_dict = {}
        for r in table_data:
            cat = r.get("category") or "Unknown"
            if cat not in cat_perf_dict:
                cat_perf_dict[cat] = {
                    "name": cat,
                    "amazon_revenue": 0.0,
                    "flipkart_revenue": 0.0,
                    "revenue": 0.0,
                }
            amazon_revenue = float(r.get("az_revenue") or 0.0)
            flipkart_revenue = float(r.get("fk_revenue") or 0.0)
            cat_perf_dict[cat]["amazon_revenue"] += amazon_revenue
            cat_perf_dict[cat]["flipkart_revenue"] += flipkart_revenue
            cat_perf_dict[cat]["revenue"] += amazon_revenue + flipkart_revenue

    cat_perf_list = []
    for v in cat_perf_dict.values():
        cat_name = v["name"]
        # Explicit float() cast guards against string values from cached/serialized data
        cat_rev = float(v.get("revenue") or 0.0)
        cat_az_rev = float(v.get("amazon_revenue") or 0.0)
        cat_fk_rev = float(v.get("flipkart_revenue") or 0.0)
        cat_prev = float(prev_rev_by_cat.get(cat_name, 0.0) or 0.0)
        growth = _safe_growth(cat_rev, cat_prev)
        contribution = round(cat_rev / total_revenue * 100, 1) if total_revenue > 0 else 0.0
        cat_perf_list.append({
            "category": cat_name,
            "amazon_revenue": round(cat_az_rev, 2),
            "flipkart_revenue": round(cat_fk_rev, 2),
            "revenue": round(cat_rev, 2),
            "total_revenue": round(cat_rev, 2),
            "growth": growth,
            "mom_growth": growth,
            "mom_current_revenue": round(cat_rev, 2),
            "mom_previous_revenue": round(cat_prev, 2),
            "contribution": contribution,
        })
    cat_perf_list.sort(key=lambda x: float(x.get("revenue") or 0), reverse=True)

    # 8. Filter metadata for dropdowns
    filter_meta = cached_filter_metadata or get_available_filters_orm(qs, fk_qs)



    low_stock_count = oos_count = 0
    total_lost_sales = 0.0
    oos_impact = {
        "lost_sales": 0.0,
        "az_lost_sales": 0.0,
        "fk_lost_sales": 0.0,
        "skus_affected": 0,
        "az_skus_affected": 0,
        "fk_skus_affected": 0,
        "orders_lost": 0,
        "az_orders_lost": 0,
        "fk_orders_lost": 0,
        "az_aov": 0.0,
        "fk_aov": 0.0,
        "selected_platform": "",
        "lost_sales_rule": "",
        "sku_rule": "",
        "orders_rule": "",
        "formula": "Lost Sales = Amazon OOS revenue + Flipkart OOS revenue + Flipkart Nearly OOS revenue",
        "row_basis": "Rows come from the latest inventory-health summary after dashboard filters are applied.",
        "amazon_status_basis": "Amazon includes rows where status is OOS.",
        "flipkart_status_basis": "Flipkart includes rows where status is OOS or Nearly OOS.",
    }
    inventory_position = []
    inventory = {
        "in_stock": 0,
        "low_stock": 0,
        "oos": 0,
        "overstock": 0,
        "details": [],
        "details_total": 0,
        "details_shown": 0,
        "details_truncated": False,
        "has_stock_data": False,
        "num_sale_days": 1,
    }
    # ── DOC-only Inventory Health (SKU + Date level) ──
    from apps.dashboard.models import DashboardInventoryHealthSummary

    def _queue_inventory_summary_refresh(summary_platform):
        warmup_key = f"dashboard_inventory_summary_warmup_{user.id}_{summary_platform}"
        if not cache.add(warmup_key, "1", timeout=900):
            return
        try:
            from apps.dashboard.tasks import refresh_dashboard_inventory_summary_task

            refresh_dashboard_inventory_summary_task.delay(data_owner_id=user.id)
        except Exception:
            pass

    # Prefer precomputed inventory health summary rows for faster filtered reads.
    try:
        summary_platform = "Combined"
        inv_sum_qs = DashboardInventoryHealthSummary.objects.filter(
            user=user, platform=summary_platform
        )
        inv_sum_qs = apply_inventory_summary_filters(
            inv_sum_qs,
            user,
            filters,
            platform_filter,
        )

        # Single aggregate replaces: count() + two distinct date count() calls.
        _inv_agg = inv_sum_qs.aggregate(
            total=Count("id"),
            n_dates=Count("date", distinct=True),
        )
        summary_total_rows = _inv_agg["total"] or 0
        _inv_num_sale_days = max(_inv_agg["n_dates"] or 1, 1)
        if summary_total_rows > 0:
            amz_status_rows = []
            fk_status_rows = []
            if platform_filter != "Flipkart":
                amz_status_rows = inv_sum_qs.values("status").annotate(
                    cnt=Count("id"), rev=Sum("revenue")
                )
            if platform_filter != "Amazon":
                fk_status_rows = inv_sum_qs.values("fk_status").annotate(
                    cnt=Count("id"), rev=Sum("fk_revenue")
                )
            
            amz_status_count = {str(r["status"]): int(r["cnt"] or 0) for r in amz_status_rows if r["status"]}
            fk_status_count = {str(r["fk_status"]): int(r["cnt"] or 0) for r in fk_status_rows if r["fk_status"]}
            
            amz_status_rev = {str(r["status"]): float(r["rev"] or 0.0) for r in amz_status_rows if r["status"]}
            fk_status_rev = {str(r["fk_status"]): float(r["rev"] or 0.0) for r in fk_status_rows if r["fk_status"]}
            
            amz_in_stock = amz_status_count.get("In Stock", 0)
            amz_low_stock = amz_status_count.get("Low Stock", 0)
            amz_oos = amz_status_count.get("OOS", 0)
            amz_overstock = amz_status_count.get("Overstock", 0)

            fk_nearly_oos = fk_status_count.get("Nearly OOS", 0)
            fk_oos_only = fk_status_count.get("OOS", 0)
            fk_low_stock = fk_status_count.get("Understock", 0)
            fk_in_stock = fk_status_count.get("Ideal Stocking", 0)
            fk_overstock_1 = fk_status_count.get("Over Stock", 0)
            fk_overstock_2 = fk_status_count.get("Highly Over Stock", 0)
            fk_not_selling = fk_status_count.get("Not Selling", 0)

            fk_oos = fk_nearly_oos + fk_oos_only
            fk_overstock = fk_overstock_1 + fk_overstock_2 + fk_not_selling
            
            total_lost_sales = (
                amz_status_rev.get("OOS", 0.0)
                + fk_status_rev.get("OOS", 0.0)
                + fk_status_rev.get("Nearly OOS", 0.0)
            )
            az_lost_sales = float(amz_status_rev.get("OOS", 0.0))
            fk_lost_sales = float(fk_status_rev.get("OOS", 0.0) + fk_status_rev.get("Nearly OOS", 0.0))
            orders_estimate = _estimate_oos_orders_lost(
                az_lost_sales=az_lost_sales,
                fk_lost_sales=fk_lost_sales,
                platform_filter=platform_filter,
                qs=qs_f,
                fk_qs=fk_qs_f,
                filters=filters,
            )

            inventory = {
                "in_stock": int(amz_in_stock + fk_in_stock),
                "low_stock": int(amz_low_stock + fk_low_stock),
                "oos": int(amz_oos + fk_oos),
                "overstock": int(amz_overstock + fk_overstock),
                
                "amz_in_stock": int(amz_in_stock),
                "amz_low_stock": int(amz_low_stock),
                "amz_oos": int(amz_oos),
                "amz_overstock": int(amz_overstock),
                
                "fk_in_stock": int(fk_in_stock),
                "fk_low_stock": int(fk_low_stock),
                "fk_oos": int(fk_oos),
                "fk_overstock": int(fk_overstock),

                "details": [],
                "details_total": int(summary_total_rows),
                "details_shown": 0,
                "details_truncated": False,
                "has_stock_data": True,
                "num_sale_days": _inv_num_sale_days,
            }

            rev_in_stock = amz_status_rev.get("In Stock", 0.0) + fk_status_rev.get("Ideal Stocking", 0.0)
            rev_low_stock = amz_status_rev.get("Low Stock", 0.0) + fk_status_rev.get("Understock", 0.0)
            rev_overstock = (
                amz_status_rev.get("Overstock", 0.0) 
                + fk_status_rev.get("Over Stock", 0.0) 
                + fk_status_rev.get("Highly Over Stock", 0.0) 
                + fk_status_rev.get("Not Selling", 0.0)
            )
            rev_oos = (
                amz_status_rev.get("OOS", 0.0) 
                + fk_status_rev.get("OOS", 0.0) 
                + fk_status_rev.get("Nearly OOS", 0.0)
            )

            bucket_defs = [
                ("In Stock (15-60D)", rev_in_stock, "green", amz_status_rev.get("In Stock", 0.0), fk_status_rev.get("Ideal Stocking", 0.0)),
                ("Low Stock (<=15D)", rev_low_stock, "amber", amz_status_rev.get("Low Stock", 0.0), fk_status_rev.get("Understock", 0.0)),
                ("Overstock (>60D)", rev_overstock, "orange", amz_status_rev.get("Overstock", 0.0), fk_status_rev.get("Over Stock", 0.0) + fk_status_rev.get("Highly Over Stock", 0.0) + fk_status_rev.get("Not Selling", 0.0)),
                ("Out of Stock", rev_oos, "red", amz_status_rev.get("OOS", 0.0), fk_status_rev.get("OOS", 0.0) + fk_status_rev.get("Nearly OOS", 0.0)),
            ]
            
            tracked_rev = rev_in_stock + rev_low_stock + rev_overstock + rev_oos
            pct_den = tracked_rev if tracked_rev > 0 else total_revenue
            inventory_position = []
            for label, rev_val, color, az_val, fk_val in bucket_defs:
                pct = round(rev_val / pct_den * 100, 1) if pct_den > 0 else 0
                inventory_position.append({
                    "label": label,
                    "revenue": round(rev_val, 2),
                    "az_revenue": round(az_val, 2),
                    "fk_revenue": round(fk_val, 2),
                    "pct": pct,
                    "color": color,
                })

            if include_full_payload:
                details = []
                for row in inv_sum_qs.order_by("-date", "-revenue", "sku"):
                    details.append(
                        {
                            "date": row.date,
                            "sku": row.sku,
                            "category": row.category or "Unknown",
                            "stock_qty": int(row.stock_qty or 0),
                            "fba_qty": int(row.fba_qty or 0),
                            "flex_qty": int(row.flex_qty or 0),
                            "sale_qty": int(row.sale_qty or 0),
                            "total_sales_30d": int(row.total_sales_window or 0),
                            "drr": round(float(row.drr or 0), 2),
                            "doc": float(row.doc or 0),
                            "units": int(row.sale_qty or 0),
                            "revenue": round(float(row.revenue or 0), 2),
                            "status": row.status,
                            "status_class": row.status_class,
                            "reason": row.reason,
                        }
                    )
                inventory["details"] = details
                inventory["details_shown"] = len(details)
                inventory["details_truncated"] = len(details) < summary_total_rows

            oos_impact = {
                "lost_sales": round(total_lost_sales, 2),
                "az_lost_sales": round(az_lost_sales, 2),
                "fk_lost_sales": round(fk_lost_sales, 2),
                "skus_affected": int(inventory["oos"]),
                "az_skus_affected": int(amz_oos),
                "fk_skus_affected": int(fk_oos),
                "orders_lost": orders_estimate["orders_lost"],
                "az_orders_lost": orders_estimate["az_orders_lost"],
                "fk_orders_lost": orders_estimate["fk_orders_lost"],
                "az_aov": orders_estimate["az_aov"],
                "fk_aov": orders_estimate["fk_aov"],
                "selected_platform": "Combined",
                "lost_sales_rule": "Amazon OOS revenue + Flipkart OOS revenue + Flipkart Nearly OOS revenue.",
                "sku_rule": "SKUs Affected = Amazon OOS rows + Flipkart OOS/Nearly OOS rows after dashboard filters.",
                "orders_rule": orders_estimate["orders_rule"],
                "formula": "Lost Sales = Amazon OOS revenue + Flipkart OOS revenue + Flipkart Nearly OOS revenue",
                "row_basis": "Rows come from the latest inventory-health summary after dashboard filters are applied.",
                "amazon_status_basis": "Amazon includes rows where status is OOS.",
                "flipkart_status_basis": "Flipkart includes rows where status is OOS or Nearly OOS.",
            }
        else:
            _queue_inventory_summary_refresh(summary_platform)
    except Exception as _inv_exc:
        import logging
        logging.getLogger(__name__).exception("Inventory health summary failed: %s", _inv_exc)
        _queue_inventory_summary_refresh("Combined")
    # Use the dynamic `today` (latest data date) already computed above
    days_in_month = (today.replace(month=today.month % 12 + 1, day=1) - datetime.timedelta(days=1)).day if today.month < 12 else 31
    days_elapsed = max(today.day, 1)

    if today.day <= 5:
        # At start of a new month, use last week's data from previous month for forecasting
        prev_month_end = today.replace(day=1) - datetime.timedelta(days=1)
        prev_month_last_week_start = prev_month_end - datetime.timedelta(days=6)
        last_week_rev = get_revenue_for_period(qs, fk_qs, prev_month_last_week_start, prev_month_end)
        daily_rate = last_week_rev / 7 if last_week_rev > 0 else (kpis["revenue"] / days_elapsed if days_elapsed > 0 else 0)
    else:
        daily_rate = kpis["revenue"] / days_elapsed if days_elapsed > 0 else 0

    run_rate = daily_rate * days_in_month

    forecast_labels, forecast_actual, forecast_fc, forecast_target = [], [], [], []
    forecast_details = []
    
    # Cumulative calculation details
    for day_num in range(1, days_in_month + 1):
        forecast_labels.append(str(day_num))
        actual_val = None
        fc_val = None
        
        if day_num <= days_elapsed:
            actual_val = round(daily_rate * day_num, 2)
            forecast_actual.append(actual_val)
            forecast_fc.append(None)
        else:
            fc_val = round(kpis["revenue"] + daily_rate * (day_num - days_elapsed), 2)
            forecast_actual.append(None)
            forecast_fc.append(fc_val)
            
        forecast_target.append(round(run_rate, 2))
        
        forecast_details.append({
            "day": day_num,
            "actual": actual_val,
            "forecast": fc_val,
            "target": round(run_rate, 2),
            "daily_avg": round(daily_rate, 2)
        })

    forecast = {
        "predicted": round(run_rate, 2), "target": round(run_rate, 2), "gap": 0, "gap_pct": 0, "labels": forecast_labels,
        "actual": forecast_actual, "forecast": forecast_fc, "target_line": forecast_target,
        "details": forecast_details, "daily_rate": round(daily_rate, 2),
        "days_in_month": days_in_month, "days_elapsed": days_elapsed
    }

    priorities = []
    if kpis.get("tacos", 0) > 15:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": "Reduce Ad Spend", 
            "subtitle": f"TACoS is high at {kpis['tacos']:.1f}%. Review campaigns.", 
            "priority": "High",
            "calculation": f"TACoS ({kpis['tacos']:.1f}%) > Threshold (15%)"
        })
    if oos_count > 0:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": f"Restock {oos_count} Out-of-Stock SKUs", 
            "subtitle": "Act now to prevent lost sales.", 
            "priority": "High",
            "calculation": f"OOS Count ({oos_count}) > 0"
        })
    if low_stock_count > 0:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": f"Replenish {low_stock_count} Low-Stock SKUs", 
            "subtitle": "Trigger replenishment orders.", 
            "priority": "Medium",
            "calculation": f"Low Stock Count ({low_stock_count}) > 0"
        })
    if not priorities:
        priorities.append({
            "rank": 1, 
            "title": "Review Dashboard Metrics", 
            "subtitle": "All indicators normal.", 
            "priority": "Low",
            "calculation": "No critical thresholds breached"
        })

    # Fast path: use monthly summary for top/declining products on long date ranges.
    # Falls back to ProcessedDashboardData when monthly summary is not applicable.
    top_prods = None
    under_prods = None
    npd_all = []
    npd_trend = {"labels": [], "pageviews": [], "units": [], "conversion": []}

    if not skip_product_tables:
        if not product_insights_need_exact_dates(filters):
            try:
                from apps.dashboard.services.asin_monthly_summary import (
                    build_top_products_from_monthly,
                    build_declining_products_from_monthly,
                )
                top_prods = build_top_products_from_monthly(
                    user,
                    filters,
                    asin_meta=_asin_meta,
                    fsn_meta=_fsn_meta,
                    limit=10,
                    include_full_payload=include_full_payload,
                )
                under_prods = build_declining_products_from_monthly(
                    user,
                    filters,
                    cm_start,
                    cm_end,
                    pm_start,
                    pm_end,
                    include_full_payload=include_full_payload,
                )
            except Exception:
                top_prods = None
                under_prods = None

        if top_prods is None or under_prods is None:
            product_summary_az_base, product_summary_fk_base = _get_product_daily_summary_querysets(
                user, filters, apply_date_filter=False
            )
            product_summary_az_f, product_summary_fk_f = apply_dashboard_entity_filters(
                product_summary_az_base, product_summary_fk_base, filters, user=user
            )

        if top_prods is None:
            top_prods = _build_top_product_rows(
                qs_f,
                fk_qs_f,
                qs_prev_f,
                fk_prev_f,
                asin_meta=_asin_meta,
                fsn_meta=_fsn_meta,
                include_full_payload=include_full_payload,
                summary_qs_f=product_summary_az_f.filter(date__gte=cm_start, date__lte=cm_end),
                fk_summary_qs_f=product_summary_fk_f.filter(date__gte=cm_start, date__lte=cm_end),
                summary_prev_f=get_prev_period_qs(product_summary_az_base, filters),
                fk_summary_prev_f=get_prev_period_qs(product_summary_fk_base, filters),
            )
        if under_prods is None:
            under_prods = _build_declining_product_rows(
                qs, fk_qs, cm_start, cm_end, pm_start, pm_end,
                include_full_payload=include_full_payload,
                asin_meta=_asin_meta, fsn_meta=_fsn_meta,
                summary_qs=product_summary_az_f,
                fk_summary_qs=product_summary_fk_f,
            )

        try:
            from apps.dashboard.services.npd import build_npd_performance
            _npd_include_trend = True
            npd_payload = build_npd_performance(user, filters, qs_f, fk_qs_f, include_trend=_npd_include_trend)
        except Exception:
            npd_payload = {
                "rows": [],
                "trend": {"labels": [], "pageviews": [], "units": [], "conversion": []},
            }
        npd_all = npd_payload.get("rows") or []
        npd_trend = npd_payload.get("trend") or {
            "labels": [],
            "pageviews": [],
            "units": [],
            "conversion": [],
        }

    portfolio_revenue = {}
    _merge_portfolio_revenue_from_summary(
        portfolio_revenue,
        product_summary_az_f,
        "asin",
        _asin_meta,
    )
    _merge_portfolio_revenue_from_summary(
        portfolio_revenue,
        product_summary_fk_f,
        "fsn",
        _fsn_meta,
    )
    if portfolio_revenue:
        port_perf_dict = {
            portfolio: {"cluster": portfolio, "revenue": revenue}
            for portfolio, revenue in portfolio_revenue.items()
        }
    elif table_data is None:
        port_perf_dict = {}
        if qs_f is not None:
            for _row in qs_f.values("portfolio").annotate(rev=Sum("revenue")):
                port = _clean_dimension_label(_row.get("portfolio"))
                if not port:
                    continue
                revenue = float(_row.get("rev") or 0.0)
                if port not in port_perf_dict:
                    port_perf_dict[port] = {"cluster": port, "revenue": 0.0}
                port_perf_dict[port]["revenue"] += revenue
        if fk_qs_f is not None:
            for _row in fk_qs_f.values("portfolio").annotate(rev=Sum("revenue")):
                port = _clean_dimension_label(_row.get("portfolio"))
                if not port:
                    continue
                revenue = float(_row.get("rev") or 0.0)
                if port not in port_perf_dict:
                    port_perf_dict[port] = {"cluster": port, "revenue": 0.0}
                port_perf_dict[port]["revenue"] += revenue
    else:
        port_perf_dict = {}
        for r in table_data:
            port = _clean_dimension_label(r.get("portfolio"))
            if not port:
                continue
            if port not in port_perf_dict:
                port_perf_dict[port] = {"cluster": port, "revenue": 0.0}
            port_perf_dict[port]["revenue"] += r["revenue"]

    cluster_performance = []
    for port, v in port_perf_dict.items():
        curr_rev = v["revenue"]
        prev_rev = prev_rev_by_port.get(port, 0)
        growth = _safe_growth(curr_rev, prev_rev)
        cluster_performance.append({
            "cluster": port, 
            "revenue": curr_rev, 
            "growth": growth, 
            "contribution": round(curr_rev / total_revenue * 100, 1) if total_revenue > 0 else 0
        })
    cluster_performance.sort(key=lambda x: x["revenue"], reverse=True)

    # Normalize: when skip_product_tables=True these stay None; ensure empty lists so
    # templates never receive None and the [:N] slices below never raise TypeError.
    _top = top_prods or []
    _under = under_prods or []

    return {
        "_compute_scope": "full",
        "kpis": kpis, "charts": charts, "category_performance": cat_perf_list,
        "platforms": platforms_dict, "filters": filter_meta,
        "oos_impact": oos_impact,
        "inventory": inventory, "inventory_position": inventory_position, "forecast": forecast,
        "priorities": priorities, "marketing": marketing,
        "cluster_performance": cluster_performance,
        "cat_top_products": _top[:10] if include_full_payload else _top,
        "cat_under_products": _under[:10] if include_full_payload else _under,
        "cat_all_top_products": _top if include_full_payload else [],
        "cat_all_under_products": _under if include_full_payload else [],
        "npd_products": npd_all[:8],
        "npd_products_all": npd_all if include_full_payload else [],
        "npd_trend": npd_trend,
        "growth_opportunities": [],
    }
