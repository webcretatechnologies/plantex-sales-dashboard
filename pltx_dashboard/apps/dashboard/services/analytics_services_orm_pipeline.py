import datetime
import calendar

from apps.dashboard.services.analytics_services_orm import (
    generate_kpis_orm,
    generate_charts_data_orm,
)
from apps.dashboard.services.analytics_services_orm_tables import (
    generate_bi_data_orm,
)
from django.core.cache import cache
from django.db.models import Sum, Max
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


def _safe_growth(curr, prev):
    if prev and prev != 0:
        return round((curr - prev) / abs(prev) * 100, 1)
    return 0


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
    qs, fk_qs, spend_qs, filters, user, cached_filter_metadata=None
):
    # 1. Apply date filters
    qs_f = apply_global_filters_orm(qs, filters)
    fk_qs_f = apply_global_filters_orm(fk_qs, filters)

    # 2. Get prev-period querysets
    qs_prev = get_prev_period_qs(qs, filters)
    fk_prev = get_prev_period_qs(fk_qs, filters)
    qs_prev_f = apply_global_filters_orm(qs_prev, {}) if qs_prev is not None else None
    fk_prev_f = apply_global_filters_orm(fk_prev, {}) if fk_prev is not None else None

    # ── Master table data (used to eliminate duplicate DB hits) ──
    table_data = generate_bi_data_orm(qs_f, fk_qs_f)
    
    # ── Master prev table data for growth calculations ──
    if qs_prev_f is not None or fk_prev_f is not None:
        table_data_prev = generate_bi_data_orm(qs_prev_f, fk_prev_f)
    else:
        table_data_prev = []
        
    prev_rev_by_asin = {r["asin"]: r["revenue"] for r in table_data_prev}
    prev_rev_by_port = {}
    prev_rev_by_cat = {}
    prev_az_rev = sum(r.get("az_revenue", 0) for r in table_data_prev)
    prev_fk_rev = sum(r.get("fk_revenue", 0) for r in table_data_prev)
    for r in table_data_prev:
        port = r.get("portfolio") or "Unknown"
        prev_rev_by_port[port] = prev_rev_by_port.get(port, 0) + r["revenue"]
        cat = r.get("category") or "Unknown"
        prev_rev_by_cat[cat] = prev_rev_by_cat.get(cat, 0) + r["revenue"]
    
    total_revenue = sum(r["revenue"] for r in table_data)
    total_spend = sum(r["total_spend"] for r in table_data)
    revenue_for_ads = total_revenue * 0.7  # Revenue * 0.7 for TACOS/ROAS
    
    # 3. KPIs
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
        "cogs": 0.0,
    }
    
    roas = (revenue_for_ads / kpis["spend"]) if kpis["spend"] > 0 else 0
    conversion = (kpis["orders"] / kpis["pageviews"] * 100) if kpis["pageviews"] > 0 else 0
    tacos = (kpis["spend"] / revenue_for_ads * 100) if revenue_for_ads > 0 else 0
    gross_margin = kpis["revenue"] - kpis["cogs"]
    gross_margin_pct = (gross_margin / kpis["revenue"] * 100) if kpis["revenue"] > 0 else 0
    net_profit = gross_margin - kpis["spend"]
    contribution_margin = round(gross_margin_pct - tacos, 1)

    kpis.update({
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "tacos": round(tacos, 2),
        "gross_margin": gross_margin,
        "gross_margin_pct": round(gross_margin_pct, 2),
        "net_profit": net_profit,
        "contribution_margin": contribution_margin,
        "ad_spend_sku_count": sum(1 for r in table_data if r.get("total_spend", 0) > 0),
        "selling_sku_count": sum(1 for r in table_data if r.get("orders", 0) > 0),
        "zero_sales_pageviews": sum(r.get("pageviews", 0) for r in table_data if r.get("orders", 0) == 0),
    })

    kpis_prev = generate_kpis_orm(qs_prev_f, fk_prev_f, spend_qs)

    for key in ["revenue", "orders", "units", "spend", "roas", "tacos"]:
        curr = kpis.get(key, 0)
        prev = kpis_prev.get(key, 0)
        kpis[f"{key}_change"] = _safe_growth(curr, prev)

    max_qs = qs.aggregate(m=Max("date"))["m"] if qs is not None else None
    max_fk = fk_qs.aggregate(m=Max("date"))["m"] if fk_qs is not None else None
    latest_dates = [d for d in (max_qs, max_fk) if d]
    data_anchor_date = max(latest_dates) if latest_dates else datetime.date.today()

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

    cm_rev = get_revenue_for_period(qs, fk_qs, cm_start, cm_end)
    pm_rev = get_revenue_for_period(qs, fk_qs, pm_start, pm_end)
    ppm_rev = get_revenue_for_period(qs, fk_qs, ppm_start, ppm_end)
    yoy_cm_rev = get_revenue_for_period(qs, fk_qs, yoy_cm_start, yoy_cm_end)
    yoy_pm_rev = get_revenue_for_period(qs, fk_qs, yoy_pm_start, yoy_pm_end)
    cm_az_rev = get_revenue_for_period(qs, None, cm_start, cm_end)
    pm_az_rev = get_revenue_for_period(qs, None, pm_start, pm_end)
    yoy_cm_az_rev = get_revenue_for_period(qs, None, yoy_cm_start, yoy_cm_end)
    cm_fk_rev = get_revenue_for_period(None, fk_qs, cm_start, cm_end)
    pm_fk_rev = get_revenue_for_period(None, fk_qs, pm_start, pm_end)
    yoy_cm_fk_rev = get_revenue_for_period(None, fk_qs, yoy_cm_start, yoy_cm_end)

    cm_spend = get_spend_for_period(qs, fk_qs, cm_start, cm_end)
    pm_spend = get_spend_for_period(qs, fk_qs, pm_start, pm_end)

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
    kpis["profit_change"] = _safe_growth(kpis["net_profit"], kpis_prev.get("net_profit", 0))
    kpis["gross_margin_change"] = round(kpis["gross_margin_pct"] - kpis_prev.get("gross_margin_pct", 0), 1)
    kpis["contribution_margin_change"] = round(kpis["contribution_margin"] - kpis_prev.get("contribution_margin", 0), 1)

    kpis["mom_spend_growth"] = _safe_growth(cm_spend, pm_spend)
    
    cm_rev_ads = cm_rev * 0.7
    pm_rev_ads = pm_rev * 0.7
    cm_roas = (cm_rev_ads / cm_spend) if cm_spend > 0 else 0
    pm_roas = (pm_rev_ads / pm_spend) if pm_spend > 0 else 0
    kpis["mom_roas_change"] = round(cm_roas - pm_roas, 2)
    
    cm_tacos = (cm_spend / cm_rev_ads * 100) if cm_rev_ads > 0 else 0
    pm_tacos = (pm_spend / pm_rev_ads * 100) if pm_rev_ads > 0 else 0
    kpis["mom_tacos_change"] = round(cm_tacos - pm_tacos, 1)

    # Used by forecast and other sections that should anchor to data freshness.
    today = data_anchor_date

    # 5. Charts
    charts = generate_charts_data_orm(qs_f, fk_qs_f, table_data=table_data)

    # 6. Platform breakdown
    az_rev = sum(r.get("az_revenue", 0) for r in table_data)
    fk_rev = sum(r.get("fk_revenue", 0) for r in table_data)
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



    marketing = {
        "ad_spend": kpis["spend"], 
        "ad_spend_change": kpis.get("mom_spend_growth", 0),
        "roas": kpis["roas"], 
        "roas_change_pct": kpis.get("mom_roas_change", 0), 
        "tacos": kpis["tacos"], 
        "tacos_change": kpis.get("mom_tacos_change", 0),
        "ad_spend_sku_count": kpis.get("ad_spend_sku_count", 0),
        "selling_sku_count": kpis.get("selling_sku_count", 0),
        "zero_sales_pageviews": kpis.get("zero_sales_pageviews", 0),
    }

    in_stock_count = low_stock_count = oos_count = overstock_count = 0
    total_lost_sales = 0.0

    # ── DOC-only Inventory Health (ASIN + Date level) ──
    from apps.dashboard.models import FBAStockData, FlexStockData, SalesData, CategoryMapping

    # Build ASIN allow-list for category/portfolio/subcategory filters
    allowed_asins = None
    if filters.get("category") or filters.get("portfolio") or filters.get("subcategory"):
        cat_map_qs = CategoryMapping.objects.filter(user=user)

        category_filter = filters.get("category")
        if category_filter:
            if isinstance(category_filter, (list, tuple)):
                cat_map_qs = cat_map_qs.filter(category__in=category_filter)
            else:
                cat_map_qs = cat_map_qs.filter(category=category_filter)

        portfolio_filter = filters.get("portfolio")
        if portfolio_filter:
            cat_map_qs = cat_map_qs.filter(portfolio=portfolio_filter)

        subcategory_filter = filters.get("subcategory")
        if subcategory_filter:
            if isinstance(subcategory_filter, (list, tuple)):
                cat_map_qs = cat_map_qs.filter(subcategory__in=subcategory_filter)
            else:
                cat_map_qs = cat_map_qs.filter(subcategory=subcategory_filter)

        allowed_asins = set(cat_map_qs.values_list("asin", flat=True))

    asin_filter = filters.get("asin")
    platform_filter = (filters.get("platform") or "").strip()
    inventory_enabled = platform_filter != "Flipkart"

    # FBA + Flex stock at ASIN + Date level
    if inventory_enabled:
        fba_qs = apply_global_filters_orm(FBAStockData.objects.filter(user=user), filters)
        flex_qs = apply_global_filters_orm(FlexStockData.objects.filter(user=user), filters)
    else:
        fba_qs = FBAStockData.objects.none()
        flex_qs = FlexStockData.objects.none()

    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            fba_qs = fba_qs.filter(asin__in=asin_filter)
            flex_qs = flex_qs.filter(asin__in=asin_filter)
        else:
            fba_qs = fba_qs.filter(asin=asin_filter)
            flex_qs = flex_qs.filter(asin=asin_filter)
    if allowed_asins is not None:
        fba_qs = fba_qs.filter(asin__in=allowed_asins)
        flex_qs = flex_qs.filter(asin__in=allowed_asins)

    has_stock_data = inventory_enabled and (fba_qs.exists() or flex_qs.exists())

    # Skip expensive inventory joins when inventory is not relevant for this view
    # (Flipkart-only) or when no stock snapshots exist for the current filters.
    sales_by_key = {}
    revenue_by_key = {}
    revenue_by_asin = {}
    asin_category_map = {}
    num_sale_days = 1
    if has_stock_data:
        sales_qs = SalesData.objects.filter(user=user)
        sales_qs = apply_global_filters_orm(sales_qs, filters)
        if asin_filter:
            if isinstance(asin_filter, (list, tuple)):
                sales_qs = sales_qs.filter(asin__in=asin_filter)
            else:
                sales_qs = sales_qs.filter(asin=asin_filter)
        if allowed_asins is not None:
            sales_qs = sales_qs.filter(asin__in=allowed_asins)

        sale_days = list(sales_qs.values_list("date", flat=True).distinct())
        num_sale_days = max(len(sale_days), 1)

        for row in sales_qs.values("asin", "date").annotate(total_units=Sum("units")):
            key = (str(row["asin"]), row["date"])
            sales_by_key[key] = int(row["total_units"] or 0)

        if qs_f is not None:
            for row in qs_f.values("asin", "date").annotate(total_revenue=Sum("revenue")):
                key = (str(row["asin"]), row["date"])
                revenue_by_key[key] = float(row["total_revenue"] or 0)
            for row in qs_f.values("asin").annotate(total_revenue=Sum("revenue")):
                revenue_by_asin[str(row["asin"])] = float(row["total_revenue"] or 0)

        asin_category_map = {
            str(r["asin"]): str(r["category"] or "Unknown")
            for r in CategoryMapping.objects.filter(user=user).values("asin", "category")
        }
    else:
        # Keep downstream loops no-op without extra DB calls.
        fba_qs = fba_qs.none()
        flex_qs = flex_qs.none()

    fba_stock_by_key = {}
    for row in fba_qs.values("asin", "date").annotate(total=Sum("ending_warehouse_balance")):
        key = (str(row["asin"]), row["date"])
        fba_stock_by_key[key] = int(row["total"] or 0)

    flex_stock_by_key = {}
    for row in flex_qs.values("asin", "date").annotate(total=Sum("qty")):
        key = (str(row["asin"]), row["date"])
        flex_stock_by_key[key] = int(row["total"] or 0)

    stock_keys = set(fba_stock_by_key.keys()) | set(flex_stock_by_key.keys())
    sales_keys = set(sales_by_key.keys())

    # Inventory health must be computed only on dates where BOTH sales and stock
    # exist. This keeps date alignment strict and prevents mismatched-day joins.
    stock_dates = {d for _asin, d in stock_keys if d}
    sales_dates = {d for _asin, d in sales_keys if d}
    aligned_dates = stock_dates & sales_dates

    if has_stock_data and aligned_dates:
        # Include:
        # 1) stock snapshot rows for aligned dates
        # 2) same-day sales rows even when stock row is missing for that SKU
        # This ensures Sale Qty is not dropped from inventory health tables.
        aligned_stock_keys = {k for k in stock_keys if k[1] in aligned_dates}
        aligned_sales_keys = {k for k in sales_keys if k[1] in aligned_dates}
        all_keys = aligned_stock_keys | aligned_sales_keys
    else:
        all_keys = set()

    key_count_by_asin = {}
    for asin, _row_date in all_keys:
        key_count_by_asin[asin] = key_count_by_asin.get(asin, 0) + 1

    inventory_details = []
    inventory_revenue_buckets = {
        "in_stock": 0.0,
        "low_stock": 0.0,
        "oos": 0.0,
        "overstock": 0.0,
    }
    inventory_detail_rows_total = len(all_keys)

    def _inventory_sort_key(key):
        asin, row_date = key
        sale_qty = int(sales_by_key.get((asin, row_date), 0))
        rev = float(revenue_by_key.get((asin, row_date), 0.0) or 0.0)
        return (
            row_date or datetime.date.min,
            1 if sale_qty > 0 else 0,
            sale_qty,
            rev,
            asin,
        )

    for asin, row_date in sorted(all_keys, key=_inventory_sort_key, reverse=True):
        sale_qty = int(sales_by_key.get((asin, row_date), 0))
        fba_qty = int(fba_stock_by_key.get((asin, row_date), 0))
        flex_qty = int(flex_stock_by_key.get((asin, row_date), 0))
        stock_qty = fba_qty + flex_qty
        key_count = max(int(key_count_by_asin.get(asin, 1)), 1)
        rev = float(
            revenue_by_key.get(
                (asin, row_date),
                float(revenue_by_asin.get(asin, 0.0)) / key_count,
            )
            or 0
        )
        cat = asin_category_map.get(asin, "Unknown")

        same_day_sales = float(sale_qty)
        if same_day_sales > 0:
            doc = round(stock_qty / same_day_sales, 1)
        else:
            doc = 999.0 if stock_qty > 0 else 0.0

        if stock_qty <= 0:
            status = "OOS"
            status_class = "danger"
            oos_count += 1
            total_lost_sales += rev
            reason = f"Stock Qty = 0 (FBA: {fba_qty}, Flex: {flex_qty})"
            inventory_revenue_buckets["oos"] += rev
        elif sale_qty <= 0:
            status = "Overstock"
            status_class = "neutral"
            overstock_count += 1
            reason = f"DOC = ∞ (Stock: {stock_qty}, No sales)"
            inventory_revenue_buckets["overstock"] += rev
        elif doc <= 15:
            status = "Low Stock"
            status_class = "warn"
            low_stock_count += 1
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {same_day_sales:.1f})"
            inventory_revenue_buckets["low_stock"] += rev
        elif doc > 60:
            status = "Overstock"
            status_class = "neutral"
            overstock_count += 1
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {same_day_sales:.1f})"
            inventory_revenue_buckets["overstock"] += rev
        else:
            status = "In Stock"
            status_class = "good"
            in_stock_count += 1
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {same_day_sales:.1f})"
            inventory_revenue_buckets["in_stock"] += rev

        # Only add to detailed list if within limit or if searching for a specific ASIN
        if asin_filter or len(inventory_details) < 100:
            inventory_details.append(
                {
                    "date": row_date,
                    "sku": asin,
                    "category": cat,
                    "stock_qty": stock_qty,
                    "fba_qty": fba_qty,
                    "flex_qty": flex_qty,
                    "sale_qty": sale_qty,
                    "doc": doc,
                    "units": sale_qty,
                    "revenue": round(rev, 2),
                    "status": status,
                    "status_class": status_class,
                    "reason": reason,
                }
            )

    inventory = {
        "in_stock": int(in_stock_count), 
        "low_stock": int(low_stock_count), 
        "oos": int(oos_count), 
        "overstock": int(overstock_count),
        "details": inventory_details,
        "details_total": int(inventory_detail_rows_total),
        "details_shown": int(len(inventory_details)),
        "details_truncated": inventory_detail_rows_total > len(inventory_details),
        "has_stock_data": has_stock_data,
        "num_sale_days": num_sale_days,
    }

    oos_impact = {"lost_sales": round(total_lost_sales, 2), "skus_affected": oos_count, "orders_lost": 0}

    inventory_position = []
    if total_revenue > 0:
        tracked_revenue_total = sum(inventory_revenue_buckets.values())
        pct_denominator = tracked_revenue_total if tracked_revenue_total > 0 else total_revenue

        bucket_defs = [
            ("In Stock (15–60D)", "in_stock", "green"),
            ("Low Stock (<=15D)", "low_stock", "amber"),
            ("Overstock (>60D)", "overstock", "orange"),
            ("Out of Stock", "oos", "red"),
        ]
        for label, key, color in bucket_defs:
            rev_val = inventory_revenue_buckets[key]
            pct = round(rev_val / pct_denominator * 100, 1) if pct_denominator > 0 else 0
            inventory_position.append({"label": label, "revenue": rev_val, "pct": pct, "color": color})



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

    waterfall = [
        {"label": "Revenue", "value": round(kpis["revenue"], 2)}, {"label": "Ad Spend", "value": -round(kpis["spend"], 2)},
        {"label": "Gross Profit", "value": round(kpis["gross_margin"], 2)}, {"label": "Net Profit", "value": round(kpis["net_profit"], 2)},
    ]

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
    if kpis.get("gross_margin_pct", 0) < 20:
        priorities.append({
            "rank": len(priorities)+1, 
            "title": "Improve Gross Margins", 
            "subtitle": f"Gross margin is at {kpis['gross_margin_pct']:.1f}%.", 
            "priority": "Medium",
            "calculation": f"Gross Margin ({kpis['gross_margin_pct']:.1f}%) < 20%"
        })
    if not priorities:
        priorities.append({
            "rank": 1, 
            "title": "Review Dashboard Metrics", 
            "subtitle": "All indicators normal.", 
            "priority": "Low",
            "calculation": "No critical thresholds breached"
        })

    # Fetch MOM SKU-level revenue for Declining Products
    cm_sku_rev = {}
    if qs is not None:
        for x in qs.filter(date__gte=cm_start, date__lte=cm_end).values('asin').annotate(r=Sum('revenue')):
            cm_sku_rev[x['asin']] = float(x['r'] or 0)
    if fk_qs is not None:
        for x in fk_qs.filter(date__gte=cm_start, date__lte=cm_end).values('fsn').annotate(r=Sum('revenue')):
            cm_sku_rev[x['fsn']] = cm_sku_rev.get(x['fsn'], 0) + float(x['r'] or 0)
            
    pm_sku_rev = {}
    if qs is not None:
        for x in qs.filter(date__gte=pm_start, date__lte=pm_end).values('asin').annotate(r=Sum('revenue')):
            pm_sku_rev[x['asin']] = float(x['r'] or 0)
    if fk_qs is not None:
        for x in fk_qs.filter(date__gte=pm_start, date__lte=pm_end).values('fsn').annotate(r=Sum('revenue')):
            pm_sku_rev[x['fsn']] = pm_sku_rev.get(x['fsn'], 0) + float(x['r'] or 0)

    top_prods, under_prods = [], []
    for row in table_data:
        sku = row["asin"]
        curr_rev = row["revenue"]
        prev_rev = prev_rev_by_asin.get(sku, 0)
        growth = _safe_growth(curr_rev, prev_rev)
        
        top_prods.append({"sku": sku, "cluster": row.get("portfolio") or "Standard", "revenue": curr_rev, "growth": growth, "units_sold": row["units"]})
        
        # Declining based on MOM drop
        sku_cm = cm_sku_rev.get(sku, 0)
        sku_pm = pm_sku_rev.get(sku, 0)
        mom_growth = _safe_growth(sku_cm, sku_pm)
        if mom_growth < 0:
            under_prods.append({"sku": sku, "revenue": sku_cm, "drop_pct": mom_growth, "impact": sku_cm - sku_pm})
            
    under_prods.sort(key=lambda x: x["drop_pct"])  # Sort by most negative growth

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
        "kpis": kpis, "charts": charts, "category_performance": cat_perf_list,
        "platforms": platforms_dict, "filters": filter_meta,
        "oos_impact": oos_impact,
        "inventory": inventory, "inventory_position": inventory_position, "forecast": forecast,
        "priorities": priorities, "marketing": marketing,
        "waterfall": waterfall, "cluster_performance": cluster_performance, "cat_top_products": top_prods[:5],
        "cat_under_products": under_prods[:5], "cat_all_top_products": top_prods[:100], "cat_all_under_products": under_prods[:100],
        "growth_opportunities": [],
    }
