import datetime
import calendar
import time

from apps.dashboard.models import DashboardDailySummary
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
from django.core.cache import cache
from django.db.models import Sum, Max, Case, When, F, Value, Count, Q, Subquery
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

def get_spend_for_period(q, fk_q, start, end):
    spend = 0
    if q is not None:
        agg = q.filter(date__gte=start, date__lte=end).aggregate(t=Sum("total_spend"))
        spend += float(agg["t"] or 0)
    if fk_q is not None:
        agg = fk_q.filter(date__gte=start, date__lte=end).aggregate(t=Sum("total_spend"))
        spend += float(agg["t"] or 0)
    return spend

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


def get_prev_period_qs(qs, filters):
    """Return queryset for the previous comparison period."""
    if qs is None:
        return None

    cs = filters.get("compare_start_date")
    ce = filters.get("compare_end_date")
    if cs and ce:
        return qs.filter(date__gte=cs, date__lte=ce)

    start = filters.get("start_date")
    end = filters.get("end_date")
    if start and end:
        try:
            s_dt = datetime.datetime.strptime(str(start), "%Y-%m-%d").date()
            e_dt = datetime.datetime.strptime(str(end), "%Y-%m-%d").date()
            delta = e_dt - s_dt + datetime.timedelta(days=1)
            p_end = s_dt - datetime.timedelta(days=1)
            p_start = p_end - delta + datetime.timedelta(days=1)
            return qs.filter(date__gte=p_start, date__lte=p_end)
        except Exception:
            pass
    return qs.none()


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
    if _has_sku_filters(filters):
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
            "revenue": float(row.get("revenue") or 0.0),
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
        grouped[str(key)] = grouped.get(str(key), 0.0) + float(row.get("revenue") or 0.0)
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


def _revenue_map_for_skus(qs, sku_field, sku_ids):
    if qs is None or not sku_ids:
        return {}
    rows = qs.filter(**{f"{sku_field}__in": list(sku_ids)}).values(sku_field).annotate(
        revenue=Sum("revenue")
    )
    return {
        str(row.get(sku_field)): float(row.get("revenue") or 0.0)
        for row in rows
        if row.get(sku_field)
    }


def _metric_map_for_skus(qs, sku_field, sku_ids, metric_name):
    if qs is None or not sku_ids:
        return {}
    rows = qs.filter(**{f"{sku_field}__in": list(sku_ids)}).values(sku_field).annotate(
        metric_total=Sum(metric_name)
    )
    return {
        str(row.get(sku_field)): float(row.get("metric_total") or 0.0)
        for row in rows
        if row.get(sku_field)
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


def _distinct_count(qs, field_name, extra_filter=None):
    if qs is None:
        return 0
    if extra_filter is not None:
        qs = qs.filter(extra_filter)
    return qs.values(field_name).distinct().count()


def _sum_field(qs, field_name, extra_filter=None):
    if qs is None:
        return 0
    if extra_filter is not None:
        qs = qs.filter(extra_filter)
    return int(qs.aggregate(total=Sum(field_name)).get("total") or 0)


def _empty_kpi_payload(kpis, marketing, filter_meta):
    return {
        "_compute_scope": "kpis",
        "kpis": kpis,
        "charts": {},
        "category_performance": [],
        "platforms": {},
        "filters": filter_meta,
        "oos_impact": {"lost_sales": 0.0, "skus_affected": 0, "orders_lost": 0},
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
        "growth_opportunities": [],
    }


def _build_kpi_cache_key(user_id, cache_identity):
    if not cache_identity:
        return None

    filter_hash = str(cache_identity.get("filter_hash") or "").strip()
    if not filter_hash:
        return None

    data_version = int(cache_identity.get("data_version") or 0)
    return f"dashboard_kpi_payload_v1_{user_id}_{data_version}_{filter_hash}"


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


def _distinct_value_set(qs, field_name, extra_filter=None):
    if qs is None:
        return set()
    if extra_filter is not None:
        qs = qs.filter(extra_filter)
    return {
        str(value)
        for value in qs.exclude(**{f"{field_name}__isnull": True})
        .exclude(**{field_name: ""})
        .values_list(field_name, flat=True)
        .distinct()
        if value
    }


def _distinct_value_count(qs, field_name, extra_filter=None):
    if qs is None:
        return 0
    if extra_filter is not None:
        qs = qs.filter(extra_filter)
    qs = qs.exclude(**{f"{field_name}__isnull": True}).exclude(**{field_name: ""})
    return int(qs.aggregate(total=Count(field_name, distinct=True)).get("total") or 0)


def _compute_zero_selling_metrics(qs, sku_field):
    if qs is None:
        return 0, 0

    cleaned_qs = qs.exclude(**{f"{sku_field}__isnull": True}).exclude(
        **{f"{sku_field}": ""}
    )
    positive_units_subquery = cleaned_qs.filter(units__gt=0).values(sku_field)
    zero_signal_qs = cleaned_qs.filter(
        Q(pageviews__gt=0) | Q(revenue__gt=0) | Q(orders__gt=0)
    ).exclude(**{f"{sku_field}__in": Subquery(positive_units_subquery)})

    zero_selling_sku_count = int(
        zero_signal_qs.values(sku_field).distinct().count() or 0
    )
    zero_sales_pageviews = int(
        zero_signal_qs.aggregate(total=Sum("pageviews")).get("total") or 0
    )

    return zero_selling_sku_count, zero_sales_pageviews


def _compute_activity_metrics(qs_f, fk_qs_f, filters, user, fsn_meta=None):
    active_sku_count = _distinct_value_count(qs_f, "asin") + _distinct_value_count(
        fk_qs_f, "fsn"
    )
    selling_sku_count = _distinct_value_count(
        qs_f, "asin", Q(units__gt=0)
    ) + _distinct_value_count(fk_qs_f, "fsn", Q(units__gt=0))

    az_zero_sku_count, az_zero_pageviews = _compute_zero_selling_metrics(qs_f, "asin")
    fk_zero_sku_count, fk_zero_pageviews = _compute_zero_selling_metrics(fk_qs_f, "fsn")

    status_counts = {"Continued": 0, "Discontinued": 0}
    status_revenue = {"Continued": 0.0, "Discontinued": 0.0}

    if fk_qs_f is not None:
        if fsn_meta is None:
            from apps.dashboard.models import FlipkartCategoryMap

            fsn_meta = {}
            for row in FlipkartCategoryMap.objects.filter(user=user).values(
                "fsn", "category", "portfolio", "subcategory", "product_status"
            ):
                fsn_meta[row["fsn"]] = {
                    "category": row["category"] or "",
                    "portfolio": row["portfolio"] or "",
                    "subcategory": row["subcategory"] or "",
                    "product_status": row["product_status"] or "",
                }

        cat_filter = filters.get("category")
        port_filter = filters.get("portfolio")
        sub_filter = filters.get("subcategory")
        fsn_filter = filters.get("fsn")

        def _matches(value, selected):
            if not selected:
                return True
            return value in selected if isinstance(selected, (list, tuple, set)) else value == selected

        fsn_to_status = {}
        for fsn, meta in fsn_meta.items():
            if not _matches(meta.get("category", ""), cat_filter):
                continue
            if not _matches(meta.get("portfolio", ""), port_filter):
                continue
            if not _matches(meta.get("subcategory", ""), sub_filter):
                continue
            if fsn_filter and not _matches(fsn, fsn_filter):
                continue
            raw_status = str(meta.get("product_status") or "").strip().lower()
            if raw_status in ("continued", "continue", "continued/pack of not sales"):
                fsn_to_status[fsn] = "Continued"
            elif raw_status in ("discontinued", "discontinue"):
                fsn_to_status[fsn] = "Discontinued"

        for status in fsn_to_status.values():
            if status in status_counts:
                status_counts[status] += 1

        for row in (
            fk_qs_f.values("fsn").annotate(revenue=Sum("revenue")).iterator(chunk_size=5000)
        ):
            fsn = row.get("fsn")
            if not fsn:
                continue
            status = fsn_to_status.get(str(fsn).strip())
            if status in status_revenue:
                status_revenue[status] += float(row.get("revenue") or 0.0)

    return {
        "active_asins": active_sku_count,
        "selling_sku_count": selling_sku_count,
        "zero_selling_sku_count": az_zero_sku_count + fk_zero_sku_count,
        "zero_sales_pageviews": az_zero_pageviews + fk_zero_pageviews,
        "continue_sales_revenue": round(status_revenue["Continued"], 2),
        "discontinue_sales_revenue": round(status_revenue["Discontinued"], 2),
        "continue_sku_count": int(status_counts["Continued"]),
        "discontinued_sku_count": int(status_counts["Discontinued"]),
    }


def _merge_current_product_rows(store, qs, sku_field, meta_map=None):
    if qs is None:
        return

    for row in (
        qs.values(sku_field)
        .annotate(revenue=Sum("revenue"), units=Sum("units"))
        .iterator(chunk_size=5000)
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue

        meta = meta_map.get(sku, {}) if meta_map else {}
        item = store.setdefault(
            sku,
            {
                "sku": sku,
                "cluster": meta.get("portfolio") or "Standard",
                "revenue": 0.0,
                "units_sold": 0,
            },
        )
        item["revenue"] += float(row.get("revenue") or 0.0)
        item["units_sold"] += int(row.get("units") or 0)
        if not item.get("cluster"):
            item["cluster"] = meta.get("portfolio") or "Standard"


def _limited_current_product_rows(qs, sku_field, meta_map=None, limit=5):
    if qs is None:
        return []

    rows = []
    for row in (
        qs.exclude(**{f"{sku_field}__isnull": True})
        .exclude(**{sku_field: ""})
        .values(sku_field)
        .annotate(revenue=Sum("revenue"), units=Sum("units"))
        .order_by("-revenue")[:limit]
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue
        meta = meta_map.get(sku, {}) if meta_map else {}
        rows.append(
            {
                "sku": sku,
                "cluster": meta.get("portfolio") or "Standard",
                "revenue": float(row.get("revenue") or 0.0),
                "units_sold": int(row.get("units") or 0),
            }
        )
    return rows


def _merge_previous_revenue_map(store, qs, sku_field):
    if qs is None:
        return

    for row in (
        qs.values(sku_field).annotate(revenue=Sum("revenue")).iterator(chunk_size=5000)
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue
        store[sku] = store.get(sku, 0.0) + float(row.get("revenue") or 0.0)


def _limited_previous_revenue_rows(qs, sku_field, limit=250):
    if qs is None:
        return []

    rows = []
    for row in (
        qs.exclude(**{f"{sku_field}__isnull": True})
        .exclude(**{sku_field: ""})
        .values(sku_field)
        .annotate(revenue=Sum("revenue"))
        .filter(revenue__gt=0)
        .order_by("-revenue")[:limit]
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue
        rows.append(
            {
                "sku": sku,
                "revenue": float(row.get("revenue") or 0.0),
            }
        )
    return rows


def _merge_period_revenue_map(store, qs, sku_field, start_date, end_date):
    if qs is None:
        return

    for row in (
        qs.filter(date__gte=start_date, date__lte=end_date)
        .exclude(**{f"{sku_field}__isnull": True})
        .exclude(**{sku_field: ""})
        .values(sku_field)
        .annotate(revenue=Sum("revenue"))
        .iterator(chunk_size=5000)
    ):
        sku = str(row.get(sku_field) or "").strip()
        if not sku:
            continue
        store[sku] = store.get(sku, 0.0) + float(row.get("revenue") or 0.0)


def _build_top_product_rows(
    qs_f,
    fk_qs_f,
    qs_prev_f,
    fk_prev_f,
    asin_meta=None,
    fsn_meta=None,
    include_full_payload=False,
):
    if not include_full_payload:
        limit = 5
        az_rows = _limited_current_product_rows(qs_f, "asin", asin_meta or {}, limit=limit)
        fk_rows = _limited_current_product_rows(fk_qs_f, "fsn", fsn_meta or {}, limit=limit)

        prev_revenue_by_sku = {}
        az_skus = [row["sku"] for row in az_rows]
        fk_skus = [row["sku"] for row in fk_rows]
        prev_revenue_by_sku.update(_revenue_map_for_skus(qs_prev_f, "asin", az_skus))
        for sku, revenue in _revenue_map_for_skus(fk_prev_f, "fsn", fk_skus).items():
            prev_revenue_by_sku[sku] = prev_revenue_by_sku.get(sku, 0.0) + revenue

        rows = []
        for item in az_rows + fk_rows:
            curr_revenue = float(item.get("revenue") or 0.0)
            rows.append(
                {
                    "sku": item["sku"],
                    "cluster": item.get("cluster") or "Standard",
                    "revenue": curr_revenue,
                    "growth": _safe_growth(
                        curr_revenue, prev_revenue_by_sku.get(item["sku"], 0.0)
                    ),
                    "units_sold": int(item.get("units_sold") or 0),
                }
            )

        rows.sort(key=lambda item: item["revenue"], reverse=True)
        return rows[:limit]

    current_rows = {}
    _merge_current_product_rows(current_rows, qs_f, "asin", asin_meta or {})
    _merge_current_product_rows(current_rows, fk_qs_f, "fsn", fsn_meta or {})

    if not current_rows:
        return []

    prev_revenue_by_sku = {}
    _merge_previous_revenue_map(prev_revenue_by_sku, qs_prev_f, "asin")
    _merge_previous_revenue_map(prev_revenue_by_sku, fk_prev_f, "fsn")

    rows = []
    for item in current_rows.values():
        curr_revenue = float(item.get("revenue") or 0.0)
        rows.append(
            {
                "sku": item["sku"],
                "cluster": item.get("cluster") or "Standard",
                "revenue": curr_revenue,
                "growth": _safe_growth(curr_revenue, prev_revenue_by_sku.get(item["sku"], 0.0)),
                "units_sold": int(item.get("units_sold") or 0),
            }
        )

    rows.sort(key=lambda item: item["revenue"], reverse=True)
    return rows if include_full_payload else rows[:5]


def _build_declining_product_rows(
    qs,
    fk_qs,
    cm_start,
    cm_end,
    pm_start,
    pm_end,
    include_full_payload=False,
):
    if not include_full_payload:
        candidate_limit = 250
        prev_candidates = {}
        for row in _limited_previous_revenue_rows(
            qs.filter(date__gte=pm_start, date__lte=pm_end) if qs is not None else None,
            "asin",
            limit=candidate_limit,
        ):
            prev_candidates[row["sku"]] = prev_candidates.get(row["sku"], 0.0) + float(
                row.get("revenue") or 0.0
            )
        for row in _limited_previous_revenue_rows(
            fk_qs.filter(date__gte=pm_start, date__lte=pm_end) if fk_qs is not None else None,
            "fsn",
            limit=candidate_limit,
        ):
            prev_candidates[row["sku"]] = prev_candidates.get(row["sku"], 0.0) + float(
                row.get("revenue") or 0.0
            )

        candidate_skus = list(prev_candidates.keys())
        if not candidate_skus:
            return []

        curr_revenue_by_sku = {}
        for sku, revenue in _metric_map_for_skus(
            qs.filter(date__gte=cm_start, date__lte=cm_end) if qs is not None else None,
            "asin",
            candidate_skus,
            "revenue",
        ).items():
            curr_revenue_by_sku[sku] = curr_revenue_by_sku.get(sku, 0.0) + revenue
        for sku, revenue in _metric_map_for_skus(
            fk_qs.filter(date__gte=cm_start, date__lte=cm_end) if fk_qs is not None else None,
            "fsn",
            candidate_skus,
            "revenue",
        ).items():
            curr_revenue_by_sku[sku] = curr_revenue_by_sku.get(sku, 0.0) + revenue

        rows = []
        for sku, prev_revenue in prev_candidates.items():
            curr_revenue = curr_revenue_by_sku.get(sku, 0.0)
            if prev_revenue <= 0 or curr_revenue >= prev_revenue:
                continue
            drop_pct = _safe_growth(curr_revenue, prev_revenue)
            if drop_pct >= 0:
                continue
            rows.append(
                {
                    "sku": sku,
                    "revenue": curr_revenue,
                    "drop_pct": drop_pct,
                    "impact": max(prev_revenue - curr_revenue, 0.0),
                }
            )

        rows.sort(key=lambda item: (-(item["impact"] or 0.0), item["drop_pct"]))
        return rows[:5]

    cm_sku_rev = {}
    pm_sku_rev = {}
    _merge_period_revenue_map(cm_sku_rev, qs, "asin", cm_start, cm_end)
    _merge_period_revenue_map(pm_sku_rev, qs, "asin", pm_start, pm_end)
    _merge_period_revenue_map(cm_sku_rev, fk_qs, "fsn", cm_start, cm_end)
    _merge_period_revenue_map(pm_sku_rev, fk_qs, "fsn", pm_start, pm_end)

    rows = []
    for sku in set(cm_sku_rev.keys()) | set(pm_sku_rev.keys()):
        curr_revenue = cm_sku_rev.get(sku, 0.0)
        prev_revenue = pm_sku_rev.get(sku, 0.0)
        drop_pct = _safe_growth(curr_revenue, prev_revenue)
        if drop_pct < 0:
            rows.append(
                {
                    "sku": sku,
                    "revenue": curr_revenue,
                    "drop_pct": drop_pct,
                    "impact": max(prev_revenue - curr_revenue, 0.0),
                }
            )

    rows.sort(key=lambda item: item["drop_pct"])
    return rows if include_full_payload else rows[:5]


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

        have_lock = cache.add(lock_key, "1", timeout=120)
        if not have_lock:
            for _ in range(80):
                time.sleep(0.15)
                cached_payload = cache.get(cache_key)
                if cached_payload:
                    return cached_payload

    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)
    platform_filter = (filters.get("platform") or "").strip()
    summary_base_qs = _get_daily_summary_base_qs(user, filters)
    summary_qs_f = apply_global_filters_orm(summary_base_qs, filters)

    if summary_qs_f is not None and summary_qs_f.exists():
        summary_metrics = _summary_metrics_by_platform(summary_qs_f)
        az_metrics = summary_metrics["Amazon"]
        fk_metrics = summary_metrics["Flipkart"]
    else:
        summary_base_qs = None
        az_metrics = _aggregate_metrics(qs_f)
        fk_metrics = _aggregate_metrics(fk_qs_f)

    totals = _combined_metrics(az_metrics, fk_metrics)

    fsn_meta = None
    if include_activity_metrics and fk_qs_f is not None:
        from apps.dashboard.models import FlipkartCategoryMap

        fsn_meta = {}
        for row in FlipkartCategoryMap.objects.filter(user=user).values(
            "fsn", "category", "portfolio", "subcategory", "product_status"
        ):
            fsn_meta[row["fsn"]] = {
                "category": row["category"] or "",
                "portfolio": row["portfolio"] or "",
                "subcategory": row["subcategory"] or "",
                "product_status": row["product_status"] or "",
            }

    if include_activity_metrics:
        activity_metrics = _compute_activity_metrics(
            qs_f, fk_qs_f, filters, user, fsn_meta=fsn_meta
        )
    else:
        activity_metrics = {
            "active_asins": 0,
            "selling_sku_count": 0,
            "zero_selling_sku_count": 0,
            "zero_sales_pageviews": 0,
            "continue_sales_revenue": 0.0,
            "discontinue_sales_revenue": 0.0,
            "continue_sku_count": 0,
            "discontinued_sku_count": 0,
        }

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
        "spend": total_spend,
        "az_spend": az_metrics["total_spend"],
        "fk_spend": fk_metrics["total_spend"],
        "active_asins": activity_metrics["active_asins"],
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "tacos": round(tacos, 2),
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
    for key in ["revenue", "orders", "units", "spend", "roas", "tacos"]:
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
    if summary_base_qs is not None:
        az_periods = _batch_period_aggregates(
            summary_base_qs.filter(platform="Amazon"), growth_periods
        )
        fk_periods = _batch_period_aggregates(
            summary_base_qs.filter(platform="Flipkart"), growth_periods
        )
    else:
        az_periods = _batch_period_aggregates(qs, growth_periods)
        fk_periods = _batch_period_aggregates(fk_qs, growth_periods)

    cm_rev = az_periods["cm_rev"] + fk_periods["cm_rev"]
    pm_rev = az_periods["pm_rev"] + fk_periods["pm_rev"]
    ppm_rev = az_periods["ppm_rev"] + fk_periods["ppm_rev"]
    yoy_cm_rev = az_periods["yoy_cm_rev"] + fk_periods["yoy_cm_rev"]
    yoy_pm_rev = az_periods["yoy_pm_rev"] + fk_periods["yoy_pm_rev"]
    cm_spend = az_periods["cm_spend"] + fk_periods["cm_spend"]
    pm_spend = az_periods["pm_spend"] + fk_periods["pm_spend"]

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
            "yoy_previous_revenue": round(yoy_cm_rev, 2),
            "mom_spend_growth": _safe_growth(cm_spend, pm_spend),
            "mom_roas_change": round(calculate_roas(cm_rev, cm_spend) - calculate_roas(pm_rev, pm_spend), 2),
            "mom_tacos_change": round(calculate_tacos(cm_rev, cm_spend) - calculate_tacos(pm_rev, pm_spend), 1),
        }
    )

    az_ad_spend_sku_count = 0
    if include_activity_metrics and spend_qs is not None and platform_filter != "Flipkart":
        spend_qs_f = apply_global_filters_orm(spend_qs, filters)
        az_ad_spend_sku_count = spend_qs_f.filter(spend__gt=0).count()

    fk_ad_spend_sku_count = 0
    if include_activity_metrics and platform_filter != "Amazon":
        from apps.dashboard.models import FlipkartPLA

        fk_pla_qs = apply_global_filters_orm(FlipkartPLA.objects.filter(user=user), filters)
        fk_ad_spend_sku_count = fk_pla_qs.filter(ad_spend__gt=0).count()

    kpis.update(
        {
            "ad_spend_sku_count": az_ad_spend_sku_count + fk_ad_spend_sku_count,
            "selling_sku_count": activity_metrics["selling_sku_count"],
            "zero_selling_sku_count": activity_metrics["zero_selling_sku_count"],
            "zero_sales_pageviews": activity_metrics["zero_sales_pageviews"],
            "continue_sales_revenue": activity_metrics["continue_sales_revenue"],
            "discontinue_sales_revenue": activity_metrics["discontinue_sales_revenue"],
            "continue_sku_count": activity_metrics["continue_sku_count"],
            "discontinued_sku_count": activity_metrics["discontinued_sku_count"],
        }
    )

    marketing = {
        "ad_spend": int(kpis["spend"]),
        "ad_spend_change": kpis.get("mom_spend_growth", 0),
        "roas": kpis["roas"],
        "roas_change_pct": kpis.get("mom_roas_change", 0),
        "tacos": kpis["tacos"],
        "tacos_change": kpis.get("mom_tacos_change", 0),
        "ad_spend_sku_count": kpis.get("ad_spend_sku_count", 0),
        "selling_sku_count": kpis.get("selling_sku_count", 0),
        "zero_selling_sku_count": kpis.get("zero_selling_sku_count", 0),
        "zero_sales_pageviews": kpis.get("zero_sales_pageviews", 0),
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
        "dates": [],  # not used for UI dropdown
    }

def get_available_filters_orm_cached(qs, fk_qs, data_owner_id, show_amazon=True, show_flipkart=True):
    cache_key = f"dashboard_filters_{data_owner_id}_{show_amazon}_{show_flipkart}"
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
    use_summary_rollups = (
        not include_full_payload
        and summary_qs_f is not None
        and summary_qs_f.exists()
    )
    summary_kpi_payload = None
    normalized_section_scope = str(section_scope or "all").lower()
    normalized_dashboard_view = str(dashboard_view or "").lower()
    include_activity_metrics = not (
        normalized_section_scope == "details"
        or (
            normalized_section_scope == "visuals"
            and normalized_dashboard_view in {"ceo", "category"}
        )
    )

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
    # This avoids the 4 duplicate CategoryMapping / FlipkartCategoryMap DB queries
    # that generate_bi_data_orm would otherwise fire independently each call.
    from apps.dashboard.models import CategoryMapping as _CategoryMapping, FlipkartCategoryMap as _FlipkartCategoryMap
    _asin_meta = {}
    if qs is not None:
        for _row in _CategoryMapping.objects.filter(user=user).values("asin", "category", "portfolio"):
            _asin_meta[_row["asin"]] = {
                "category": _row["category"] or "",
                "portfolio": _row["portfolio"] or "",
            }
    _fsn_meta = {}
    if fk_qs is not None:
        for _row in _FlipkartCategoryMap.objects.filter(user=user).values(
            "fsn", "category", "portfolio", "subcategory", "product_status"
        ):
            _fsn_meta[_row["fsn"]] = {
                "category": _row["category"] or "",
                "portfolio": _row["portfolio"] or "",
                "subcategory": _row["subcategory"] or "",
                "product_status": _row["product_status"] or "",
            }

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
    else:
        table_data = generate_bi_data_orm(
            qs_f, fk_qs_f, user=user, asin_meta=_asin_meta, fsn_meta=_fsn_meta
        )

    # ── Master prev table data for growth calculations ──
    table_data_prev = []
    prev_rev_by_asin = {}
    prev_rev_by_port = {}
    prev_rev_by_cat = {}
    prev_az_rev = 0.0
    prev_fk_rev = 0.0
    prev_kpis_totals = None

    if use_summary_rollups:
        summary_prev = get_prev_period_qs(summary_base_qs, filters)
        prev_summary_metrics = _summary_metrics_by_platform(summary_prev)
        prev_kpis_totals = _combined_metrics(
            prev_summary_metrics["Amazon"],
            prev_summary_metrics["Flipkart"],
        )
        prev_rev_by_port = _summary_revenue_by_dimension(summary_prev, "portfolio")
        prev_rev_by_cat = _summary_revenue_by_dimension(summary_prev, "category")
        prev_az_rev = prev_summary_metrics["Amazon"]["revenue"]
        prev_fk_rev = prev_summary_metrics["Flipkart"]["revenue"]
    elif qs_prev_f is not None or fk_prev_f is not None:
        table_data_prev = generate_bi_data_orm(
            qs_prev_f,
            fk_prev_f,
            user=user,
            asin_meta=_asin_meta,
            fsn_meta=_fsn_meta,
        )
        prev_rev_by_asin = {r["asin"]: r["revenue"] for r in table_data_prev}
        prev_az_rev = sum(r.get("az_revenue", 0) for r in table_data_prev)
        prev_fk_rev = sum(r.get("fk_revenue", 0) for r in table_data_prev)
        for r in table_data_prev:
            port = r.get("portfolio") or "Unknown"
            prev_rev_by_port[port] = prev_rev_by_port.get(port, 0) + r["revenue"]
            cat = r.get("category") or "Unknown"
            prev_rev_by_cat[cat] = prev_rev_by_cat.get(cat, 0) + r["revenue"]

    if use_summary_rollups:
        current_summary_metrics = _summary_metrics_by_platform(summary_qs_f)
        az_totals = current_summary_metrics["Amazon"]
        fk_totals = current_summary_metrics["Flipkart"]
        total_revenue = az_totals["revenue"] + fk_totals["revenue"]
        total_spend = az_totals["total_spend"] + fk_totals["total_spend"]
        kpis = {
            "revenue": total_revenue,
            "az_revenue": az_totals["revenue"],
            "fk_revenue": fk_totals["revenue"],
            "orders": az_totals["orders"] + fk_totals["orders"],
            "az_orders": az_totals["orders"],
            "fk_orders": fk_totals["orders"],
            "units": az_totals["units"] + fk_totals["units"],
            "az_units": az_totals["units"],
            "fk_units": fk_totals["units"],
            "pageviews": az_totals["pageviews"] + fk_totals["pageviews"],
            "spend": total_spend,
            "az_spend": az_totals["total_spend"],
            "fk_spend": fk_totals["total_spend"],
            "active_asins": len(table_data),
        }
    else:
        total_revenue = sum(r["revenue"] for r in table_data)
        total_spend = sum(r["total_spend"] for r in table_data)
        kpis = {
            "revenue": total_revenue,
            "az_revenue": sum(r.get("az_revenue", 0) for r in table_data),
            "fk_revenue": sum(r.get("fk_revenue", 0) for r in table_data),
            "orders": sum(r["orders"] for r in table_data),
            "az_orders": sum(r.get("az_orders", 0) for r in table_data),
            "fk_orders": sum(r.get("fk_orders", 0) for r in table_data),
            "units": sum(r["units"] for r in table_data),
            "az_units": sum(r.get("az_units", 0) for r in table_data),
            "fk_units": sum(r.get("fk_units", 0) for r in table_data),
            "pageviews": sum(r["pageviews"] for r in table_data),
            "spend": total_spend,
            "az_spend": sum(r.get("az_spend", 0) for r in table_data),
            "fk_spend": sum(r.get("fk_spend", 0) for r in table_data),
            "active_asins": len(table_data),
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

    # Ad Spend SKUs = individual spend rows with spend > 0 (from raw SpendData
    # table), NOT unique ASINs.  This matches the source file row count.
    az_ad_spend_sku_count = 0
    if spend_qs is not None and platform_filter != "Flipkart":
        spend_qs_f = apply_global_filters_orm(spend_qs, filters)
        if spend_qs_f is not None:
            az_ad_spend_sku_count = spend_qs_f.filter(spend__gt=0).count()
    fk_ad_spend_sku_count = 0
    if fk_qs_f is not None and platform_filter != "Amazon":
        from apps.dashboard.models import FlipkartPLA
        fk_pla_qs = apply_global_filters_orm(
            FlipkartPLA.objects.filter(user=user), filters
        )
        if fk_pla_qs is not None:
            fk_ad_spend_sku_count = fk_pla_qs.filter(ad_spend__gt=0).count()

    # 0-Sales SKU count: only ASINs that appear in the sales file
    # (have pageviews, revenue, or orders > 0, OR exist in the raw Sales file).
    # OPTIMISED: We only query raw SalesData/FlipkartSearchTraffic for the small
    # candidate set of ASINs that have ALL zeros in the aggregated data, avoiding
    # the expensive full-table values_list() pull into Python memory.
    from apps.dashboard.models import SalesData as _SalesData, FlipkartSearchTraffic as _FKTraffic

    # Candidate set: rows where every metric is zero (the only ambiguous case).
    _all_zero_asins = {
        r["asin"]
        for r in table_data
        if (
            r.get("az_revenue", 0) == 0
            and r.get("az_orders", 0) == 0
            and r.get("pageviews", 0) == 0
            and r.get("fk_revenue", 0) == 0
            and r.get("fk_orders", 0) == 0
        )
    }

    az_sales_asins = set()
    if _all_zero_asins:
        sales_qs_direct = apply_global_filters_orm(
            _SalesData.objects.filter(user=user), filters
        )
        if sales_qs_direct is not None:
            # Only fetch ASINs from the tiny candidate set — not the full table.
            az_sales_asins = set(
                sales_qs_direct.filter(asin__in=_all_zero_asins).values_list("asin", flat=True)
            )

    fk_sales_fsns = set()
    if _all_zero_asins and platform_filter != "Amazon":
        fk_traffic_qs = apply_global_filters_orm(
            _FKTraffic.objects.filter(user=user), filters
        )
        if fk_traffic_qs is not None:
            # Same candidate-scoping for Flipkart FSNs.
            fk_sales_fsns = set(
                fk_traffic_qs.filter(fsn__in=_all_zero_asins).values_list("fsn", flat=True)
            )

    def _has_sales_data(r):
        if r.get("fk_revenue", 0) > 0 or r.get("fk_orders", 0) > 0:
            return True
        if r.get("az_revenue", 0) > 0 or r.get("az_orders", 0) > 0 or r.get("pageviews", 0) > 0:
            return True
        # If all zeros, check if it's genuinely from the sales file
        asin = r.get("asin")
        if asin and (asin in az_sales_asins or asin in fk_sales_fsns):
            return True
        return False

    kpis.update({
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "tacos": round(tacos, 2),
        "ad_spend_sku_count": az_ad_spend_sku_count + fk_ad_spend_sku_count,
        "selling_sku_count": sum(1 for r in table_data if r.get("units", 0) > 0),
        "zero_selling_sku_count": sum(
            1 for r in table_data
            if r.get("units", 0) == 0 and _has_sales_data(r)
        ),
        "zero_sales_pageviews": sum(
            r.get("pageviews", 0) for r in table_data
            if r.get("units", 0) == 0 and _has_sales_data(r)
        ),
    })

    # ── Flipkart Product Status Metrics ──
    # OPTIMISED: 0 extra DB queries — uses pre-fetched _fsn_meta (includes product_status)
    # and table_data (has per-FSN revenue). Replaces: fk_qs_f.exists() + FK CategoryMap
    # re-query + fk_qs_f GROUP BY revenue (3 queries eliminated).
    status_counts = {"Continued": 0, "Discontinued": 0}
    status_revenue = {"Continued": 0.0, "Discontinued": 0.0}

    # Python-side existence check using already-loaded table_data (no DB hit).
    _has_fk_data = fk_qs_f is not None and any(
        r.get("fk_revenue", 0) > 0 or r.get("fk_orders", 0) > 0 or r.get("fk_units", 0) > 0
        for r in table_data
    )

    if _has_fk_data:
        _cat_f  = filters.get("category")
        _port_f = filters.get("portfolio")
        _sub_f  = filters.get("subcategory")
        _fsn_f  = filters.get("fsn")

        def _flt(val, fltr):
            """Python-side filter match (mirrors DB filter logic)."""
            if not fltr:
                return True
            return val in fltr if isinstance(fltr, (list, tuple)) else val == fltr

        # Build fsn_to_status from pre-fetched _fsn_meta — no DB query.
        fsn_to_status = {}
        for fsn, meta in _fsn_meta.items():
            if not _flt(meta.get("category", ""), _cat_f):
                continue
            if not _flt(meta.get("portfolio", ""), _port_f):
                continue
            if not _flt(meta.get("subcategory", ""), _sub_f):
                continue
            if _fsn_f and not _flt(fsn, _fsn_f):
                continue
            status_raw = str(meta.get("product_status") or "").strip().lower()
            if status_raw in ("continued", "continue", "continued/pack of not sales"):
                fsn_to_status[fsn] = "Continued"
            elif status_raw in ("discontinued", "discontinue"):
                fsn_to_status[fsn] = "Discontinued"

        # Count all FSNs in the map (not just those with current-period traffic).
        for fsn, status in fsn_to_status.items():
            if status in status_counts:
                status_counts[status] += 1

        # Revenue by status — derived from table_data (no DB query).
        for r in table_data:
            fsn = r.get("asin")  # FSNs are stored under the "asin" key in table_data
            rev = r.get("fk_revenue", 0)
            if fsn and rev:
                st = fsn_to_status.get(str(fsn).strip())
                if st in status_revenue:
                    status_revenue[st] += rev

    kpis.update({
        "continue_sales_revenue": round(status_revenue["Continued"], 2),
        "discontinue_sales_revenue": round(status_revenue["Discontinued"], 2),
        "continue_sku_count": int(status_counts["Continued"]),
        "discontinued_sku_count": int(status_counts["Discontinued"]),
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

    for key in ["revenue", "orders", "units", "spend", "roas", "tacos"]:
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

    _growth_periods = {
        "cm": (cm_start, cm_end),
        "pm": (pm_start, pm_end),
        "ppm": (ppm_start, ppm_end),
        "yoy_cm": (yoy_cm_start, yoy_cm_end),
        "yoy_pm": (yoy_pm_start, yoy_pm_end),
    }
    if use_summary_rollups:
        az_periods = _batch_period_aggregates(
            summary_base_qs.filter(platform="Amazon"), _growth_periods
        )
        fk_periods = _batch_period_aggregates(
            summary_base_qs.filter(platform="Flipkart"), _growth_periods
        )
    else:
        az_periods = _batch_period_aggregates(qs, _growth_periods)
        fk_periods = _batch_period_aggregates(fk_qs, _growth_periods)

    # Combined (Amazon + Flipkart) period values
    cm_rev = az_periods["cm_rev"] + fk_periods["cm_rev"]
    pm_rev = az_periods["pm_rev"] + fk_periods["pm_rev"]
    ppm_rev = az_periods["ppm_rev"] + fk_periods["ppm_rev"]
    yoy_cm_rev = az_periods["yoy_cm_rev"] + fk_periods["yoy_cm_rev"]
    yoy_pm_rev = az_periods["yoy_pm_rev"] + fk_periods["yoy_pm_rev"]

    # Per-platform period values
    cm_az_rev = az_periods["cm_rev"]
    pm_az_rev = az_periods["pm_rev"]
    yoy_cm_az_rev = az_periods["yoy_cm_rev"]
    cm_fk_rev = fk_periods["cm_rev"]
    pm_fk_rev = fk_periods["pm_rev"]
    yoy_cm_fk_rev = fk_periods["yoy_cm_rev"]

    cm_spend = az_periods["cm_spend"] + fk_periods["cm_spend"]
    pm_spend = az_periods["pm_spend"] + fk_periods["pm_spend"]

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
    kpis["yoy_previous_revenue"] = round(yoy_cm_rev, 2)
    kpis["mom_spend_growth"] = _safe_growth(cm_spend, pm_spend)
    
    cm_roas = calculate_roas(cm_rev, cm_spend)
    pm_roas = calculate_roas(pm_rev, pm_spend)
    kpis["mom_roas_change"] = round(cm_roas - pm_roas, 2)
    
    cm_tacos = calculate_tacos(cm_rev, cm_spend)
    pm_tacos = calculate_tacos(pm_rev, pm_spend)
    kpis["mom_tacos_change"] = round(cm_tacos - pm_tacos, 1)

    # Used by forecast and other sections that should anchor to data freshness.
    today = data_anchor_date

    marketing = {
        "ad_spend": int(kpis["spend"]),
        "ad_spend_change": kpis.get("mom_spend_growth", 0),
        "roas": kpis["roas"],
        "roas_change_pct": kpis.get("mom_roas_change", 0),
        "tacos": kpis["tacos"],
        "tacos_change": kpis.get("mom_tacos_change", 0),
        "ad_spend_sku_count": kpis.get("ad_spend_sku_count", 0),
        "selling_sku_count": kpis.get("selling_sku_count", 0),
        "zero_selling_sku_count": kpis.get("zero_selling_sku_count", 0),
        "zero_sales_pageviews": kpis.get("zero_sales_pageviews", 0),
    }

    if use_summary_rollups and summary_kpi_payload:
        kpis = dict(summary_kpi_payload.get("kpis") or {})
        marketing = dict(summary_kpi_payload.get("marketing") or {})

    if str(compute_scope or "full").lower() == "kpis":
        return {
            "_compute_scope": "kpis",
            "kpis": kpis,
            "charts": {},
            "category_performance": [],
            "platforms": {},
            "filters": cached_filter_metadata or get_available_filters_orm(qs, fk_qs),
            "oos_impact": {"lost_sales": 0.0, "skus_affected": 0, "orders_lost": 0},
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
            "growth_opportunities": [],
        }

    # 5. Charts
    if use_summary_rollups:
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
        cat_perf_dict = {
            category: {"name": category, "revenue": revenue}
            for category, revenue in _summary_revenue_by_dimension(
                summary_qs_f, "category"
            ).items()
        }
    else:
        cat_perf_dict = {}
        for r in table_data:
            cat = r.get("category") or "Unknown"
            if cat not in cat_perf_dict:
                cat_perf_dict[cat] = {"name": cat, "revenue": 0.0}
            cat_perf_dict[cat]["revenue"] += r["revenue"]

    cat_perf_list = []
    for v in cat_perf_dict.values():
        cat_name = v["name"]
        cat_rev = v["revenue"]
        cat_prev = prev_rev_by_cat.get(cat_name, 0)
        cat_perf_list.append({
            "category": cat_name,
            "revenue": cat_rev,
            "growth": _safe_growth(cat_rev, cat_prev),
            "contribution": round(cat_rev / total_revenue * 100, 1) if total_revenue > 0 else 0,
        })
    cat_perf_list.sort(key=lambda x: x["revenue"], reverse=True)

    # 8. Filter metadata for dropdowns
    filter_meta = cached_filter_metadata or get_available_filters_orm(qs, fk_qs)



    in_stock_count = low_stock_count = oos_count = overstock_count = 0
    total_lost_sales = 0.0
    oos_impact = {"lost_sales": 0.0, "skus_affected": 0, "orders_lost": 0}
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

    # Build SKU allow-list for category/portfolio/subcategory filters
    is_flipkart_only = platform_filter == "Flipkart"
    sku_filter = filters.get("fsn") if is_flipkart_only else filters.get("asin")

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
        summary_platform = "Flipkart" if is_flipkart_only else "Amazon"
        inv_sum_qs = DashboardInventoryHealthSummary.objects.filter(
            user=user, platform=summary_platform
        )
        inv_sum_qs = apply_global_filters_orm(inv_sum_qs, filters)

        cat_filter = filters.get("category")
        if cat_filter:
            inv_sum_qs = inv_sum_qs.filter(category__in=cat_filter) if isinstance(cat_filter, (list, tuple)) else inv_sum_qs.filter(category=cat_filter)
        port_filter = filters.get("portfolio")
        if port_filter:
            inv_sum_qs = inv_sum_qs.filter(portfolio__in=port_filter) if isinstance(port_filter, (list, tuple)) else inv_sum_qs.filter(portfolio=port_filter)
        sub_filter = filters.get("subcategory")
        if sub_filter:
            inv_sum_qs = inv_sum_qs.filter(subcategory__in=sub_filter) if isinstance(sub_filter, (list, tuple)) else inv_sum_qs.filter(subcategory=sub_filter)

        if sku_filter:
            inv_sum_qs = inv_sum_qs.filter(sku__in=sku_filter) if isinstance(sku_filter, (list, tuple)) else inv_sum_qs.filter(sku=sku_filter)

        # Single aggregate replaces: count() + two distinct date count() calls.
        _inv_agg = inv_sum_qs.aggregate(
            total=Count("id"),
            n_dates=Count("date", distinct=True),
        )
        summary_total_rows = _inv_agg["total"] or 0
        _inv_num_sale_days = max(_inv_agg["n_dates"] or 1, 1)
        if summary_total_rows > 0:
            status_rows = inv_sum_qs.values("status").annotate(
                cnt=Count("id"), rev=Sum("revenue")
            )
            status_count = {str(r["status"]): int(r["cnt"] or 0) for r in status_rows}
            status_rev = {str(r["status"]): float(r["rev"] or 0.0) for r in status_rows}

            if is_flipkart_only:
                nearly_oos_count = status_count.get("Nearly OOS", 0)
                understock_count = status_count.get("Understock", 0)
                ideal_count = status_count.get("Ideal Stocking", 0)
                fk_overstock_count = status_count.get("Over Stock", 0)
                highly_overstock_count = status_count.get("Highly Over Stock", 0)
                not_selling_count = status_count.get("Not Selling", 0)
                oos_only = status_count.get("OOS", 0)

                in_stock_count = ideal_count
                low_stock_count = understock_count
                oos_count = nearly_oos_count + oos_only
                overstock_count = (
                    fk_overstock_count + highly_overstock_count + not_selling_count
                )
                total_lost_sales = status_rev.get("OOS", 0.0) + status_rev.get(
                    "Nearly OOS", 0.0
                )
                inventory = {
                    "in_stock": int(in_stock_count),
                    "low_stock": int(low_stock_count),
                    "oos": int(oos_count),
                    "overstock": int(overstock_count),
                    "nearly_oos": int(nearly_oos_count),
                    "understock": int(understock_count),
                    "ideal": int(ideal_count),
                    "fk_overstock": int(fk_overstock_count),
                    "highly_overstock": int(highly_overstock_count),
                    "not_selling": int(not_selling_count),
                    "is_fk_inventory": True,
                    "details": [],
                    "details_total": int(summary_total_rows),
                    "details_shown": 0,
                    "details_truncated": False,
                    "has_stock_data": True,
                    "num_sale_days": _inv_num_sale_days,
                }
                bucket_defs = [
                    ("Nearly OOS (<5D)", "Nearly OOS", "red"),
                    ("Understock (<15D)", "Understock", "amber"),
                    ("Ideal (15–30D)", "Ideal Stocking", "green"),
                    ("Over Stock (>30D)", "Over Stock", "orange"),
                    ("Highly Over Stock (>90D)", "Highly Over Stock", "orange"),
                    ("Not Selling (>180D)", "Not Selling", "gray"),
                    ("Out of Stock", "OOS", "red"),
                ]
                tracked_rev = sum(status_rev.values())
                pct_den = tracked_rev if tracked_rev > 0 else total_revenue
                inventory_position = []
                for label, key, color in bucket_defs:
                    rev_val = float(status_rev.get(key, 0.0))
                    pct = round(rev_val / pct_den * 100, 1) if pct_den > 0 else 0
                    inventory_position.append(
                        {"label": label, "revenue": rev_val, "pct": pct, "color": color}
                    )
            else:
                in_stock_count = status_count.get("In Stock", 0)
                low_stock_count = status_count.get("Low Stock", 0)
                oos_count = status_count.get("OOS", 0)
                overstock_count = status_count.get("Overstock", 0)
                total_lost_sales = float(status_rev.get("OOS", 0.0))
                inventory = {
                    "in_stock": int(in_stock_count),
                    "low_stock": int(low_stock_count),
                    "oos": int(oos_count),
                    "overstock": int(overstock_count),
                    "details": [],
                    "details_total": int(summary_total_rows),
                    "details_shown": 0,
                    "details_truncated": False,
                    "has_stock_data": True,
                    "num_sale_days": _inv_num_sale_days,
                }
                bucket_defs = [
                    ("In Stock (15–60D)", "In Stock", "green"),
                    ("Low Stock (<=15D)", "Low Stock", "amber"),
                    ("Overstock (>60D)", "Overstock", "orange"),
                    ("Out of Stock", "OOS", "red"),
                ]
                tracked_rev = sum(status_rev.values())
                pct_den = tracked_rev if tracked_rev > 0 else total_revenue
                inventory_position = []
                for label, key, color in bucket_defs:
                    rev_val = float(status_rev.get(key, 0.0))
                    pct = round(rev_val / pct_den * 100, 1) if pct_den > 0 else 0
                    inventory_position.append(
                        {"label": label, "revenue": rev_val, "pct": pct, "color": color}
                    )

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
                "skus_affected": int(oos_count),
                "orders_lost": 0,
            }
        else:
            _queue_inventory_summary_refresh(summary_platform)
    except Exception:
        _queue_inventory_summary_refresh("Flipkart" if is_flipkart_only else "Amazon")
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
    if kpis.get("revenue_change", 0) < -5:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": "Investigate Revenue Drop", 
            "subtitle": f"Revenue declined {abs(kpis['revenue_change']):.1f}%.", 
            "priority": "High",
            "calculation": f"Revenue Change ({kpis['revenue_change']:.1f}%) < -5%"
        })
    elif kpis.get("revenue_change", 0) > 15:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": "Capitalize on Revenue Growth", 
            "subtitle": f"Revenue up {kpis['revenue_change']:.1f}%.", 
            "priority": "Medium",
            "calculation": f"Revenue Change ({kpis['revenue_change']:.1f}%) > 15%"
        })
    if not priorities:
        priorities.append({
            "rank": 1, 
            "title": "Review Dashboard Metrics", 
            "subtitle": "All indicators normal.", 
            "priority": "Low",
            "calculation": "No critical thresholds breached"
        })

    include_inline_product_cards = include_full_payload
    top_prods = []
    under_prods = []
    if include_inline_product_cards:
        top_prods = _build_top_product_rows(
            qs_f,
            fk_qs_f,
            qs_prev_f,
            fk_prev_f,
            asin_meta=_asin_meta,
            fsn_meta=_fsn_meta,
            include_full_payload=include_full_payload,
        )
        under_prods = _build_declining_product_rows(
            qs,
            fk_qs,
            cm_start,
            cm_end,
            pm_start,
            pm_end,
            include_full_payload=include_full_payload,
        )

    if use_summary_rollups:
        port_perf_dict = {
            portfolio: {"cluster": portfolio, "revenue": revenue}
            for portfolio, revenue in _summary_revenue_by_dimension(
                summary_qs_f, "portfolio"
            ).items()
        }
    else:
        port_perf_dict = {}
        for r in table_data:
            port = r.get("portfolio") or "Unknown"
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

    return {
        "_compute_scope": "full",
        "kpis": kpis, "charts": charts, "category_performance": cat_perf_list,
        "platforms": platforms_dict, "filters": filter_meta,
        "oos_impact": oos_impact,
        "inventory": inventory, "inventory_position": inventory_position, "forecast": forecast,
        "priorities": priorities, "marketing": marketing,
        "cluster_performance": cluster_performance,
        "cat_top_products": top_prods[:5] if include_full_payload else top_prods,
        "cat_under_products": under_prods[:5] if include_full_payload else under_prods,
        "cat_all_top_products": top_prods if include_full_payload else [],
        "cat_all_under_products": under_prods if include_full_payload else [],
        "growth_opportunities": [],
    }
