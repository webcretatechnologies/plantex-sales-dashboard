"""
asin_monthly_summary.py

Pre-aggregated monthly ASIN/FSN metrics for fast dashboard queries.

Architecture:
  ProcessedDashboardData (daily, per asin) → GROUP BY month → DashboardAsinMonthlySummary
  DashboardDailySummary (daily, no asin)   → fast KPI/charts
  DashboardAsinMonthlySummary              → fast activity counts + top/declining products

For "last 3 months" this reduces DB scan from 90 rows/ASIN to 3 rows/ASIN (30× speedup).
For "last 1 year" the reduction is 365→12 rows/ASIN (30× speedup).

Only used for date ranges ≥ 45 days (last_3_months / last_6_months / last_1_year
or custom ranges). Shorter ranges use ProcessedDashboardData directly — they're already fast.
"""

import datetime

from django.db import connection
from django.db.models import Sum
from django.utils import timezone

from apps.dashboard.models import (
    DashboardAsinMonthlySummary,
    ProcessedDashboardData,
    FlipkartProcessedDashboardData,
)


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


# ---------------------------------------------------------------------------
# Build / Rebuild
# ---------------------------------------------------------------------------

def rebuild_asin_monthly_summary_for_user(user, *, only_months=None):
    """
    Rebuild DashboardAsinMonthlySummary for *user* using INSERT INTO … SELECT.

    only_months: optional list of date objects (first day of month) to limit
    the rebuild to specific months; if omitted, all months are rebuilt.
    """
    # Normalise month inputs once, then use sargable date ranges in SQL.
    only_month_starts = []
    if only_months:
        for m in only_months:
            try:
                year, month = [int(part) for part in str(m)[:7].split("-")]
                only_month_starts.append(datetime.date(year, month, 1))
            except Exception:
                pass
    only_month_starts = sorted(set(only_month_starts))
    only_month_strs = [month_start.strftime("%Y-%m") for month_start in only_month_starts]

    month_filter_sql = ""
    month_params: list = []
    month_ranges = []
    if only_month_starts:
        for month_start in only_month_starts:
            next_month = (
                datetime.date(month_start.year + 1, 1, 1)
                if month_start.month == 12
                else datetime.date(month_start.year, month_start.month + 1, 1)
            )
            month_ranges.append((month_start, next_month))

    scoped = DashboardAsinMonthlySummary.objects.filter(user=user)
    if month_ranges:
        from django.db.models import Q
        month_q = Q()
        for month_start, next_month in month_ranges:
            month_q |= Q(year_month__gte=month_start, year_month__lt=next_month)
        scoped = scoped.filter(month_q)
    scoped.delete()

    if month_ranges:
        clauses = []
        for month_start, next_month in month_ranges:
            clauses.append("(date >= %s AND date < %s)")
            month_params.extend([month_start, next_month])
        month_filter_sql = f" AND ({' OR '.join(clauses)})"

    tbl = DashboardAsinMonthlySummary._meta.db_table
    az_tbl = ProcessedDashboardData._meta.db_table
    fk_tbl = FlipkartProcessedDashboardData._meta.db_table

    az_sql = f"""
        INSERT INTO `{tbl}` (
            `user_id`, `platform`, `asin`, `year_month`,
            `portfolio`, `category`, `subcategory`,
            `revenue`, `orders`, `units`, `pageviews`,
            `total_spend`, `spend_sp`, `spend_sb`, `spend_sd`
        )
        SELECT
            user_id,
            'Amazon',
            asin,
            DATE_FORMAT(date, '%%Y-%%m-01'),
            MAX(COALESCE(portfolio, '')),
            MAX(COALESCE(category, '')),
            MAX(COALESCE(subcategory, '')),
            SUM(revenue),
            SUM(orders),
            SUM(units),
            SUM(pageviews),
            SUM(total_spend),
            SUM(spend_sp),
            SUM(spend_sb),
            SUM(spend_sd)
        FROM `{az_tbl}`
        WHERE user_id = %s{month_filter_sql}
        GROUP BY user_id, asin,
                 DATE_FORMAT(date, '%%Y-%%m-01')
        ON DUPLICATE KEY UPDATE
            portfolio = VALUES(portfolio),
            category = VALUES(category),
            subcategory = VALUES(subcategory),
            revenue = VALUES(revenue),
            orders = VALUES(orders),
            units = VALUES(units),
            pageviews = VALUES(pageviews),
            total_spend = VALUES(total_spend),
            spend_sp = VALUES(spend_sp),
            spend_sb = VALUES(spend_sb),
            spend_sd = VALUES(spend_sd)
    """

    fk_sql = f"""
        INSERT INTO `{tbl}` (
            `user_id`, `platform`, `asin`, `year_month`,
            `portfolio`, `category`, `subcategory`,
            `revenue`, `orders`, `units`, `pageviews`,
            `total_spend`, `spend_sp`, `spend_sb`, `spend_sd`
        )
        SELECT
            user_id,
            'Flipkart',
            fsn,
            DATE_FORMAT(date, '%%Y-%%m-01'),
            MAX(COALESCE(portfolio, '')),
            MAX(COALESCE(category, '')),
            MAX(COALESCE(subcategory, '')),
            SUM(revenue),
            SUM(orders),
            SUM(units),
            SUM(pageviews),
            SUM(total_spend),
            SUM(spend_sp),
            SUM(spend_sb),
            SUM(spend_sd)
        FROM `{fk_tbl}`
        WHERE user_id = %s{month_filter_sql}
        GROUP BY user_id, fsn,
                 DATE_FORMAT(date, '%%Y-%%m-01')
        ON DUPLICATE KEY UPDATE
            portfolio = VALUES(portfolio),
            category = VALUES(category),
            subcategory = VALUES(subcategory),
            revenue = VALUES(revenue),
            orders = VALUES(orders),
            units = VALUES(units),
            pageviews = VALUES(pageviews),
            total_spend = VALUES(total_spend),
            spend_sp = VALUES(spend_sp),
            spend_sb = VALUES(spend_sb),
            spend_sd = VALUES(spend_sd)
    """

    params = [user.id] + month_params
    rows_written = 0
    with connection.cursor() as cursor:
        cursor.execute(az_sql, params)
        rows_written += max(cursor.rowcount, 0)
        cursor.execute(fk_sql, params)
        rows_written += max(cursor.rowcount, 0)

    return {
        "rows_written": rows_written,
        "months_scoped": sorted(only_month_strs) if only_month_strs else [],
    }


# ---------------------------------------------------------------------------
# Date-range helpers
# ---------------------------------------------------------------------------

def _ym(d):
    """First day of the month for date d."""
    return d.replace(day=1)


def _ym_range_from_filters(filters):
    """
    Convert date filters to (ym_start, ym_end) for querying DashboardAsinMonthlySummary.

    Returns None when the range is < 45 days (monthly granularity adds no value)
    or when asin/fsn-level filters are active (monthly summary still supports these
    via direct asin column filters).

    year_month values are always the first day of the month.
    Note: boundaries are inclusive of the whole month, so a query for
    "last 3 months" will include the current partial month in its entirety.
    This slight over-count is acceptable for activity/product ranking queries.
    """
    today = timezone.localdate()
    date_range = (filters.get("date_range") or "").strip()

    if date_range == "last_3_months":
        ym_end = _ym(today)
        ym_start = _ym(today - datetime.timedelta(days=90))
        return ym_start, ym_end

    if date_range == "last_6_months":
        ym_end = _ym(today)
        ym_start = _ym(today - datetime.timedelta(days=180))
        return ym_start, ym_end

    if date_range == "last_1_year":
        ym_end = _ym(today)
        ym_start = _ym(today - datetime.timedelta(days=365))
        return ym_start, ym_end

    if date_range == "last_month":
        first_of_this = today.replace(day=1)
        last_month_end = first_of_this - datetime.timedelta(days=1)
        ym = _ym(last_month_end)
        return ym, ym

    # Custom date range — only use monthly summary for long spans (≥ 45 days).
    start_str = filters.get("start_date")
    end_str = filters.get("end_date")
    if start_str and end_str:
        try:
            start_d = datetime.datetime.strptime(str(start_str), "%Y-%m-%d").date()
            end_d = datetime.datetime.strptime(str(end_str), "%Y-%m-%d").date()
            if end_d < start_d:
                start_d, end_d = end_d, start_d
            return _ym(start_d), _ym(end_d)
        except Exception:
            pass

    if not date_range and not start_str and not end_str:
        return "all", "all"

    return None  # too short or unknown → caller falls back to ProcessedDashboardData


def _apply_dimension_filters(qs, filters):
    """Apply category / portfolio / subcategory / asin / fsn filters to an AMS queryset."""
    category = filters.get("category")
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=[str(c) for c in category])
        else:
            qs = qs.filter(category=str(category))

    portfolio = filters.get("portfolio")
    if portfolio:
        if isinstance(portfolio, (list, tuple)):
            qs = qs.filter(portfolio__in=[str(p) for p in portfolio])
        else:
            qs = qs.filter(portfolio=str(portfolio))

    subcategory = filters.get("subcategory")
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            qs = qs.filter(subcategory__in=[str(s) for s in subcategory])
        else:
            qs = qs.filter(subcategory=str(subcategory))

    asin_filter = filters.get("asin")
    fsn_filter = filters.get("fsn")
    
    if asin_filter or fsn_filter:
        from django.db.models import Q
        q_obj = Q()
        if asin_filter:
            if isinstance(asin_filter, (list, tuple)):
                q_obj |= Q(platform="Amazon", asin__in=[str(a) for a in asin_filter])
            else:
                q_obj |= Q(platform="Amazon", asin=str(asin_filter))
        if fsn_filter:
            if isinstance(fsn_filter, (list, tuple)):
                q_obj |= Q(platform="Flipkart", asin__in=[str(f) for f in fsn_filter])
            else:
                q_obj |= Q(platform="Flipkart", asin=str(fsn_filter))
        qs = qs.filter(q_obj)

    return qs


def get_ams_qs(user, filters):
    """
    Return a filtered DashboardAsinMonthlySummary queryset or None.
    Returns None when:
      - date range is too short (< 45 days)
      - mapping filters (like category_manager) are applied, since they require JOINs
      - monthly summary has no data for this user
    """
    from apps.dashboard.services.filters import _has_mapping_filters, has_launch_date_filter
    if _has_mapping_filters(filters) or has_launch_date_filter(filters):
        return None

    ym_range = _ym_range_from_filters(filters)
    if ym_range is None:
        return None

    ym_start, ym_end = ym_range
    qs = DashboardAsinMonthlySummary.objects.filter(user=user)
    if ym_start != "all" and ym_end != "all":
        qs = qs.filter(
            year_month__gte=ym_start,
            year_month__lte=ym_end,
        )

    platform = (filters.get("platform") or "").strip()
    if platform == "Amazon":
        qs = qs.filter(platform="Amazon")
    elif platform == "Flipkart":
        qs = qs.filter(platform="Flipkart")

    qs = _apply_dimension_filters(qs, filters)
    return qs


# ---------------------------------------------------------------------------
# Activity metrics (replaces _compute_activity_metrics for long date ranges)
# ---------------------------------------------------------------------------


def build_top_products_from_monthly(
    user,
    filters,
    asin_meta=None,
    fsn_meta=None,
    limit=5,
    include_full_payload=False,
):
    """
    Fast top-products list from DashboardAsinMonthlySummary.
    Returns a list of product dicts (same shape as _build_top_product_rows),
    or None if the monthly summary is not applicable.

    Growth vs. previous period is computed with a second aggregation on the
    monthly table (still much faster than querying ProcessedDashboardData).
    """
    ams_qs = get_ams_qs(user, filters)
    if ams_qs is None:
        return None

    # Current-period per-ASIN totals
    current_agg = (
        ams_qs.values("asin", "platform")
        .annotate(revenue=Sum("revenue"), units=Sum("units"), pageviews=Sum("pageviews"))
        .order_by("-revenue")
    )

    current_rows = list(current_agg)
    if not current_rows:
        return None

    # Previous-period revenue — same year_month range shifted back one month
    ym_range = _ym_range_from_filters(filters)
    platform_filter = (filters.get("platform") or "").strip()
    prev_revenue_by_asin: dict = {}
    if ym_range and ym_range[0] != "all":
        ym_start, ym_end = ym_range
        span_days = (ym_end - ym_start).days + 31  # approx span in days
        prev_ym_end = _ym(ym_start - datetime.timedelta(days=1))
        prev_ym_start = _ym(prev_ym_end - datetime.timedelta(days=span_days))

        asin_ids = [str(r["asin"]) for r in current_rows]
        prev_qs = DashboardAsinMonthlySummary.objects.filter(
            user=user,
            asin__in=asin_ids,
            year_month__gte=prev_ym_start,
            year_month__lte=prev_ym_end,
        )
        if platform_filter == "Amazon":
            prev_qs = prev_qs.filter(platform="Amazon")
        elif platform_filter == "Flipkart":
            prev_qs = prev_qs.filter(platform="Flipkart")

        for row in prev_qs.values("asin").annotate(revenue=Sum("revenue")):
            asin = str(row.get("asin") or "")
            if asin:
                prev_revenue_by_asin[asin] = _to_float(row.get("revenue"))

    from apps.dashboard.services.metrics import safe_growth as _safe_growth

    # \u2500\u2500 group by platform \u2500\u2500
    az_rows = [r for r in current_rows if (r.get("platform") or "") == "Amazon"]
    fk_rows = [r for r in current_rows if (r.get("platform") or "") == "Flipkart"]

    az_total_rev = sum(_to_float(r.get("revenue")) for r in az_rows)
    fk_total_rev = sum(_to_float(r.get("revenue")) for r in fk_rows)

    merged = {}

    for row in az_rows:
        asin = str(row.get("asin") or "").strip()
        if not asin:
            continue
        meta = (asin_meta or {}).get(asin, {})
        curr_rev = _to_float(row.get("revenue"))
        msku = meta.get("msku") or meta.get("sku") or ""
        key = msku if msku else f"az_{asin}"
        az_prev = prev_revenue_by_asin.get(asin, 0.0)
        az_growth = _safe_growth(curr_rev, az_prev)
        az_contrib = round(curr_rev / az_total_rev * 100, 1) if az_total_rev > 0 else 0.0
        merged[key] = {
            "sku": asin,
            "msku": msku or asin,
            "cluster": meta.get("portfolio") or "Standard",
            "az_sku": asin,
            "fk_sku": None,
            "az_revenue": round(curr_rev, 2),
            "fk_revenue": 0.0,
            "az_prev_revenue": round(az_prev, 2),
            "fk_prev_revenue": 0.0,
            "az_mom_growth": az_growth,
            "fk_mom_growth": 0.0,
            "az_contribution": az_contrib,
            "fk_contribution": 0.0,
            "az_pageviews": int(row.get("pageviews") or 0),
            "fk_pageviews": 0,
            "revenue": round(curr_rev, 2),
            "units_sold": int(row.get("units") or 0),
            "pageviews": int(row.get("pageviews") or 0),
            "growth": az_growth,
            "prev_revenue": round(az_prev, 2),
        }

    for row in fk_rows:
        fsn = str(row.get("asin") or "").strip()  # monthly summary uses 'asin' field for both
        if not fsn:
            continue
        meta = (fsn_meta or {}).get(fsn, {})
        curr_rev = _to_float(row.get("revenue"))
        msku = meta.get("sku") or ""
        key = msku if msku else f"fk_{fsn}"
        fk_prev = prev_revenue_by_asin.get(fsn, 0.0)
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
            r["revenue"] = round(_to_float(r.get("revenue")) + curr_rev, 2)
            r["units_sold"] += int(row.get("units") or 0)
            r["pageviews"] += int(row.get("pageviews") or 0)
            r["prev_revenue"] = round(_to_float(r.get("prev_revenue")) + fk_prev, 2)
            r["growth"] = _safe_growth(r["revenue"], r["prev_revenue"])
        else:
            merged[key] = {
                "sku": fsn,
                "msku": msku or fsn,
                "cluster": meta.get("portfolio") or "Standard",
                "az_sku": None,
                "fk_sku": fsn,
                "az_revenue": 0.0,
                "fk_revenue": round(curr_rev, 2),
                "az_prev_revenue": 0.0,
                "fk_prev_revenue": round(fk_prev, 2),
                "az_mom_growth": 0.0,
                "fk_mom_growth": fk_growth,
                "az_contribution": 0.0,
                "fk_contribution": fk_contrib,
                "az_pageviews": 0,
                "fk_pageviews": int(row.get("pageviews") or 0),
                "revenue": round(curr_rev, 2),
                "units_sold": int(row.get("units") or 0),
                "pageviews": int(row.get("pageviews") or 0),
                "growth": fk_growth,
                "prev_revenue": round(fk_prev, 2),
            }

    results = list(merged.values())
    for row in results:
        row["az_revenue"] = _to_float(row.get("az_revenue"))
        row["fk_revenue"] = _to_float(row.get("fk_revenue"))
        row["revenue"] = _to_float(row.get("revenue"))
        row["az_prev_revenue"] = _to_float(row.get("az_prev_revenue"))
        row["fk_prev_revenue"] = _to_float(row.get("fk_prev_revenue"))
        row["prev_revenue"] = _to_float(row.get("prev_revenue"))
    results.sort(key=lambda r: r["revenue"], reverse=True)
    return results if include_full_payload else results[:limit]


# ---------------------------------------------------------------------------
# Declining products (replaces _build_declining_product_rows for long ranges)
# ---------------------------------------------------------------------------

def build_declining_products_from_monthly(
    user,
    filters,
    cm_start,
    cm_end,
    pm_start,
    pm_end,
    include_full_payload=False,
    asin_meta=None,
    fsn_meta=None,
):
    ams_qs = get_ams_qs(user, filters)
    if ams_qs is None:
        return None

    period_min_ym = _ym(min(cm_start, pm_start))
    period_max_ym = _ym(max(cm_end, pm_end))
    cm_ym_start, cm_ym_end = _ym(cm_start), _ym(cm_end)
    pm_ym_start, pm_ym_end = _ym(pm_start), _ym(pm_end)

    base_qs = DashboardAsinMonthlySummary.objects.filter(
        user=user,
        year_month__gte=period_min_ym,
        year_month__lte=period_max_ym,
    )
    platform = (filters.get("platform") or "").strip()
    if platform == "Amazon":
        base_qs = base_qs.filter(platform="Amazon")
    elif platform == "Flipkart":
        base_qs = base_qs.filter(platform="Flipkart")
    base_qs = _apply_dimension_filters(base_qs, filters)

    from django.db.models import Case, F, Value, When, FloatField

    # We will fetch az and fk separately to merge by MSKU
    cm_az_rev, pm_az_rev, pv_az, pm_pv_az = {}, {}, {}, {}
    cm_fk_rev, pm_fk_rev, pv_fk, pm_pv_fk = {}, {}, {}, {}

    for row in (
        base_qs.values("asin", "platform")
        .annotate(
            cm_r=Sum(Case(When(year_month__gte=cm_ym_start, year_month__lte=cm_ym_end, then=F("revenue")), default=Value(0.0), output_field=FloatField())),
            pm_r=Sum(Case(When(year_month__gte=pm_ym_start, year_month__lte=pm_ym_end, then=F("revenue")), default=Value(0.0), output_field=FloatField())),
            pv=Sum(Case(When(year_month__gte=cm_ym_start, year_month__lte=cm_ym_end, then=F("pageviews")), default=Value(0.0), output_field=FloatField())),
            pm_pv=Sum(Case(When(year_month__gte=pm_ym_start, year_month__lte=pm_ym_end, then=F("pageviews")), default=Value(0.0), output_field=FloatField())),
        )
        .iterator(chunk_size=5000)
    ):
        sku = str(row.get("asin") or "").strip()
        pf = str(row.get("platform") or "")
        if not sku:
            continue
            
        cm_r = _to_float(row.get("cm_r"))
        pm_r = _to_float(row.get("pm_r"))
        pv = int(row.get("pv") or 0)
        pm_pv_val = int(row.get("pm_pv") or 0)
        
        if pf == "Amazon":
            cm_az_rev[sku] = cm_az_rev.get(sku, 0.0) + cm_r
            pm_az_rev[sku] = pm_az_rev.get(sku, 0.0) + pm_r
            pv_az[sku] = pv_az.get(sku, 0) + pv
            pm_pv_az[sku] = pm_pv_az.get(sku, 0) + pm_pv_val
        elif pf == "Flipkart":
            cm_fk_rev[sku] = cm_fk_rev.get(sku, 0.0) + cm_r
            pm_fk_rev[sku] = pm_fk_rev.get(sku, 0.0) + pm_r
            pv_fk[sku] = pv_fk.get(sku, 0) + pv
            pm_pv_fk[sku] = pm_pv_fk.get(sku, 0) + pm_pv_val

    if not cm_az_rev and not pm_az_rev and not cm_fk_rev and not pm_fk_rev:
        return None

    from apps.dashboard.services.metrics import safe_growth as _safe_growth

    merged = {}
    _asin_meta = asin_meta or {}
    _fsn_meta = fsn_meta or {}

    for asin in set(cm_az_rev) | set(pm_az_rev):
        curr = cm_az_rev.get(asin, 0.0)
        prev = pm_az_rev.get(asin, 0.0)
        az_pv = pv_az.get(asin, 0)
        az_prev_pv = pm_pv_az.get(asin, 0)
        msku = _asin_meta.get(asin, {}).get("msku") or _asin_meta.get(asin, {}).get("sku") or ""
        key = msku if msku else f"az_{asin}"
        
        merged[key] = {
            "sku": asin, "msku": msku or asin,
            "az_sku": asin, "fk_sku": None,
            "az_revenue": round(curr, 2), "fk_revenue": 0.0,
            "az_prev_revenue": round(prev, 2), "fk_prev_revenue": 0.0,
            "az_drop_pct": _safe_growth(curr, prev), "fk_drop_pct": 0.0,
            "az_impact": max(prev - curr, 0.0), "fk_impact": 0.0,
            "az_pageviews": az_pv, "fk_pageviews": 0,
            "az_prev_pageviews": az_prev_pv, "fk_prev_pageviews": 0,
            "az_pv_drop_pct": _safe_growth(az_pv, az_prev_pv), "fk_pv_drop_pct": 0.0,
            "az_pv_impact": max(az_prev_pv - az_pv, 0), "fk_pv_impact": 0,
            "revenue": round(curr, 2), "prev_revenue": round(prev, 2),
            "pageviews": az_pv, "prev_pageviews": az_prev_pv,
        }

    for fsn in set(cm_fk_rev) | set(pm_fk_rev):
        curr = cm_fk_rev.get(fsn, 0.0)
        prev = pm_fk_rev.get(fsn, 0.0)
        fk_pv = pv_fk.get(fsn, 0)
        fk_prev_pv = pm_pv_fk.get(fsn, 0)
        msku = _fsn_meta.get(fsn, {}).get("sku") or ""
        key = msku if msku else f"fk_{fsn}"
        
        if key in merged:
            r = merged[key]
            r["fk_sku"] = fsn
            r["fk_revenue"] = round(curr, 2)
            r["fk_prev_revenue"] = round(prev, 2)
            r["fk_drop_pct"] = _safe_growth(curr, prev)
            r["fk_impact"] = max(prev - curr, 0.0)
            r["fk_pageviews"] = fk_pv
            r["fk_prev_pageviews"] = fk_prev_pv
            r["fk_pv_drop_pct"] = _safe_growth(fk_pv, fk_prev_pv)
            r["fk_pv_impact"] = max(fk_prev_pv - fk_pv, 0)
            r["revenue"] = round(_to_float(r.get("revenue")) + curr, 2)
            r["prev_revenue"] = round(_to_float(r.get("prev_revenue")) + prev, 2)
            r["pageviews"] += fk_pv
            r["prev_pageviews"] += fk_prev_pv
        else:
            merged[key] = {
                "sku": fsn, "msku": msku or fsn,
                "az_sku": None, "fk_sku": fsn,
                "az_revenue": 0.0, "fk_revenue": round(curr, 2),
                "az_prev_revenue": 0.0, "fk_prev_revenue": round(prev, 2),
                "az_drop_pct": 0.0, "fk_drop_pct": _safe_growth(curr, prev),
                "az_impact": 0.0, "fk_impact": max(prev - curr, 0.0),
                "az_pageviews": 0, "fk_pageviews": fk_pv,
                "az_prev_pageviews": 0, "fk_prev_pageviews": fk_prev_pv,
                "az_pv_drop_pct": 0.0, "fk_pv_drop_pct": _safe_growth(fk_pv, fk_prev_pv),
                "az_pv_impact": 0, "fk_pv_impact": max(fk_prev_pv - fk_pv, 0),
                "revenue": round(curr, 2), "prev_revenue": round(prev, 2),
                "pageviews": fk_pv, "prev_pageviews": fk_prev_pv,
            }

    declining = []
    for r in merged.values():
        r["az_revenue"] = _to_float(r.get("az_revenue"))
        r["fk_revenue"] = _to_float(r.get("fk_revenue"))
        r["revenue"] = _to_float(r.get("revenue"))
        r["az_prev_revenue"] = _to_float(r.get("az_prev_revenue"))
        r["fk_prev_revenue"] = _to_float(r.get("fk_prev_revenue"))
        r["prev_revenue"] = _to_float(r.get("prev_revenue"))
        drop_pct = _safe_growth(r["revenue"], r["prev_revenue"])
        if drop_pct < 0:
            r["drop_pct"] = drop_pct
            r["impact"] = round(max(r["prev_revenue"] - r["revenue"], 0.0), 2)
            # Combined pv stats
            total_pv = int(r.get("pageviews") or 0)
            total_prev_pv = int(r.get("prev_pageviews") or 0)
            r["pv_drop_pct"] = _safe_growth(total_pv, total_prev_pv)
            r["pv_impact"] = max(total_prev_pv - total_pv, 0)
            declining.append(r)

    # Sort by MoM Revenue Impact descending (largest absolute drop in revenue first)
    declining.sort(key=lambda r: _to_float(r.get("impact")), reverse=True)
    return declining if include_full_payload else declining[:5]
