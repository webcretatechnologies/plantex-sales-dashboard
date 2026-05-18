import datetime
import calendar

from apps.dashboard.services.analytics_services_orm import (
    generate_kpis_orm,
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
from django.db.models import Sum, Max, Case, When, F, Value, FloatField, Count
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
    return calculate_growth(curr, prev)


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
    if spend_qs is not None:
        spend_qs_f = apply_global_filters_orm(spend_qs, filters)
        if spend_qs_f is not None:
            az_ad_spend_sku_count = spend_qs_f.filter(spend__gt=0).count()
    fk_ad_spend_sku_count = 0
    if fk_qs_f is not None:
        from apps.dashboard.models import FlipkartPLA
        fk_pla_qs = apply_global_filters_orm(
            FlipkartPLA.objects.filter(user=user), filters
        )
        if fk_pla_qs is not None:
            fk_ad_spend_sku_count = fk_pla_qs.filter(ad_spend__gt=0).count()

    # 0-Sales SKU count: only ASINs that appear in the sales file
    # (have pageviews, revenue, or orders > 0, OR exist in the raw Sales file).
    # We fetch the set of raw sales ASINs for the period to correctly identify
    # sales ASINs that have 0 pageviews/orders/revenue.
    from apps.dashboard.models import SalesData as _SalesData, FlipkartSearchTraffic as _FKTraffic
    
    az_sales_asins = set()
    sales_qs_direct = apply_global_filters_orm(
        _SalesData.objects.filter(user=user), filters
    )
    if sales_qs_direct is not None:
        az_sales_asins = set(sales_qs_direct.values_list("asin", flat=True))

    fk_sales_fsns = set()
    fk_traffic_qs = apply_global_filters_orm(
        _FKTraffic.objects.filter(user=user), filters
    )
    if fk_traffic_qs is not None and platform_filter != "Amazon":
        fk_sales_fsns = set(fk_traffic_qs.values_list("fsn", flat=True))

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
    status_counts = {"Continued": 0, "Discontinued": 0}
    status_revenue = {"Continued": 0.0, "Discontinued": 0.0}
    if fk_qs_f is not None and fk_qs_f.exists():
        from apps.dashboard.models import FlipkartCategoryMap

        status_qs = FlipkartCategoryMap.objects.filter(user=user)

        category_filter = filters.get("category")
        if category_filter:
            if isinstance(category_filter, (list, tuple)):
                status_qs = status_qs.filter(category__in=category_filter)
            else:
                status_qs = status_qs.filter(category=category_filter)

        portfolio_filter = filters.get("portfolio")
        if portfolio_filter:
            status_qs = status_qs.filter(portfolio=portfolio_filter)

        subcategory_filter = filters.get("subcategory")
        if subcategory_filter:
            if isinstance(subcategory_filter, (list, tuple)):
                status_qs = status_qs.filter(subcategory__in=subcategory_filter)
            else:
                status_qs = status_qs.filter(subcategory=subcategory_filter)

        fsn_filter = filters.get("fsn")
        if fsn_filter:
            if isinstance(fsn_filter, (list, tuple)):
                status_qs = status_qs.filter(fsn__in=fsn_filter)
            else:
                status_qs = status_qs.filter(fsn=fsn_filter)

        # Build map of FSN to Product Status
        fsn_to_status = {}
        for row in status_qs.values("fsn", "product_status"):
            fsn = str(row.get("fsn") or "").strip()
            status_raw = str(row.get("product_status") or "").strip().lower()
            if status_raw in ("continued", "continue", "continued/pack of not sales"):
                fsn_to_status[fsn] = "Continued"
            elif status_raw in ("discontinued", "discontinue"):
                fsn_to_status[fsn] = "Discontinued"

        # Count ALL FSNs from category map for status counts
        # (not just those with traffic data in the period)
        for fsn, status in fsn_to_status.items():
            if status in status_counts:
                status_counts[status] += 1

        # Sum revenue by FSN for filtered period only
        for row in fk_qs_f.values("fsn").annotate(total_revenue=Sum("revenue")):
            fsn = str(row.get("fsn") or "").strip()
            status = fsn_to_status.get(fsn)
            if status in status_revenue:
                status_revenue[status] += float(row.get("total_revenue") or 0.0)

    kpis.update({
        "continue_sales_revenue": round(status_revenue["Continued"], 2),
        "discontinue_sales_revenue": round(status_revenue["Discontinued"], 2),
        "continue_sku_count": int(status_counts["Continued"]),
        "discontinued_sku_count": int(status_counts["Discontinued"]),
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

    # Batch all period-based revenue+spend queries into 2 SQL calls (one per table)
    # instead of 13+ individual calls. Uses CASE/WHEN to aggregate all periods in
    # a single pass.

    _growth_periods = {
        "cm": (cm_start, cm_end),
        "pm": (pm_start, pm_end),
        "ppm": (ppm_start, ppm_end),
        "yoy_cm": (yoy_cm_start, yoy_cm_end),
        "yoy_pm": (yoy_pm_start, yoy_pm_end),
    }

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

        # Limit the queryset to the full date range covering all periods to avoid
        # scanning the entire table.
        all_starts = [s for s, _ in periods.values()]
        all_ends = [e for _, e in periods.values()]
        min_date = min(all_starts)
        max_date = max(all_ends)
        scoped = base_qs.filter(date__gte=min_date, date__lte=max_date)

        result = scoped.aggregate(**agg_kwargs)
        return {k: float(v or 0) for k, v in result.items()}

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
    # Try to use pre-aggregated daily summary for trend lines when product-level
    # filters are not applied. This keeps trend queries fast on large datasets.
    preaggregated_trend = None
    try:
        if not filters.get("asin") and not filters.get("fsn"):
            from apps.dashboard.models import DashboardDailySummary

            ds_qs = DashboardDailySummary.objects.filter(user=user)
            if platform_filter == "Amazon":
                ds_qs = ds_qs.filter(platform="Amazon")
            elif platform_filter == "Flipkart":
                ds_qs = ds_qs.filter(platform="Flipkart")

            cat_filter = filters.get("category")
            port_filter = filters.get("portfolio")
            sub_filter = filters.get("subcategory")
            if cat_filter:
                ds_qs = ds_qs.filter(category__in=cat_filter)
            if port_filter:
                ds_qs = ds_qs.filter(portfolio__in=port_filter)
            if sub_filter:
                ds_qs = ds_qs.filter(subcategory__in=sub_filter)

            ds_qs = apply_global_filters_orm(ds_qs, filters)
            trend_rows = (
                ds_qs.values("date")
                .annotate(
                    revenue=Sum("revenue"),
                    total_spend=Sum("total_spend"),
                    pageviews=Sum("pageviews"),
                    orders=Sum("orders"),
                    amazon_revenue=Sum(
                        Case(
                            When(platform="Amazon", then=F("revenue")),
                            default=Value(0.0),
                            output_field=FloatField(),
                        )
                    ),
                    flipkart_revenue=Sum(
                        Case(
                            When(platform="Flipkart", then=F("revenue")),
                            default=Value(0.0),
                            output_field=FloatField(),
                        )
                    ),
                )
                .order_by("date")
            )
            preaggregated_trend = {
                str(row["date"]): {
                    "revenue": float(row.get("revenue") or 0),
                    "total_spend": float(row.get("total_spend") or 0),
                    "pageviews": int(row.get("pageviews") or 0),
                    "orders": int(row.get("orders") or 0),
                    "amazon_revenue": float(row.get("amazon_revenue") or 0),
                    "flipkart_revenue": float(row.get("flipkart_revenue") or 0),
                }
                for row in trend_rows
            }
            if not preaggregated_trend:
                preaggregated_trend = None
    except Exception:
        # Trend pre-aggregation is an optimization layer; fallback to raw query.
        preaggregated_trend = None

    charts = generate_charts_data_orm(
        qs_f, fk_qs_f, table_data=table_data, preaggregated_trend=preaggregated_trend
    )

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

        summary_total_rows = inv_sum_qs.count()
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
                    "num_sale_days": max(inv_sum_qs.values("date").distinct().count(), 1),
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
                    "num_sale_days": max(inv_sum_qs.values("date").distinct().count(), 1),
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

        prod_item = {
            "sku": sku,
            "cluster": row.get("portfolio") or "Standard",
            "revenue": curr_rev,
            "growth": growth,
            "units_sold": row["units"],
        }
        if include_full_payload:
            top_prods.append(prod_item)
        elif len(top_prods) < 5:
            # table_data is revenue-desc sorted already
            top_prods.append(prod_item)

    # Declining based on MOM drop:
    # Use the union of current/previous month SKU keys so drops to zero
    # are still captured (these SKUs may not appear in current-period table_data).
    for sku in set(cm_sku_rev.keys()) | set(pm_sku_rev.keys()):
        sku_cm = cm_sku_rev.get(sku, 0)
        sku_pm = pm_sku_rev.get(sku, 0)
        mom_growth = _safe_growth(sku_cm, sku_pm)
        if mom_growth < 0:
            drop_item = {
                "sku": sku,
                "revenue": sku_cm,
                "drop_pct": mom_growth,
                "impact": max(sku_pm - sku_cm, 0),
            }
            if include_full_payload:
                under_prods.append(drop_item)
            else:
                # keep only worst 5 drops for initial payload
                if len(under_prods) < 5:
                    under_prods.append(drop_item)
                    under_prods.sort(key=lambda x: x["drop_pct"])
                elif drop_item["drop_pct"] < under_prods[-1]["drop_pct"]:
                    under_prods[-1] = drop_item
                    under_prods.sort(key=lambda x: x["drop_pct"])

    if include_full_payload:
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
