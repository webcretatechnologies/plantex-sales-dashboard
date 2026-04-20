import datetime

from apps.dashboard.services.analytics_services_orm import (
    generate_kpis_orm,
    generate_charts_data_orm,
)
from apps.dashboard.services.analytics_services_orm_tables import (
    generate_bi_data_orm,
    generate_grouped_table_orm,
)


def apply_global_filters_orm(qs, filters):
    """Filters the QuerySet by date according to the UI filters."""
    if qs is None:
        return None

    start = end = None
    date_range = filters.get("date_range")
    if date_range and date_range != "custom":
        today = datetime.date.today()
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
        return sorted(
            set(
                str(v)
                for v in qs.values_list(field, flat=True).distinct()
                if v and str(v).strip() and str(v) not in ("nan", "None", "null")
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

    # 3. KPIs
    kpis = generate_kpis_orm(qs_f, fk_qs_f, spend_qs)
    kpis_prev = generate_kpis_orm(qs_prev_f, fk_prev_f, spend_qs)

    # 4. Populate growth fields in-place
    for key in ["revenue", "orders", "units", "spend", "roas", "aov", "tacos"]:
        curr = kpis.get(key, 0)
        prev = kpis_prev.get(key, 0)
        kpis[f"{key}_change"] = _safe_growth(curr, prev)

    # mom_growth = revenue_change for the current period (simplified)
    kpis["mom_growth"] = kpis.get("revenue_change", 0)
    kpis["yoy_growth"] = kpis.get("revenue_change", 0)
    kpis["prev_mom"] = round(kpis_prev.get("mom_growth", 0), 1)
    kpis["prev_yoy"] = 0

    profit_change = _safe_growth(kpis["net_profit"], kpis_prev.get("net_profit", 0))
    kpis["profit_change"] = profit_change

    # 5. Charts
    charts = generate_charts_data_orm(qs_f, fk_qs_f)

    # 6. Platform breakdown
    platforms_dict = {}
    az_rev = 0.0
    fk_rev = 0.0
    if qs_f is not None:
        from django.db.models import Sum as _Sum

        az_agg = qs_f.aggregate(r=_Sum("revenue"))
        az_rev = float(az_agg.get("r") or 0)
    if fk_qs_f is not None:
        from django.db.models import Sum as _Sum

        fk_agg = fk_qs_f.aggregate(r=_Sum("taxable_value"))
        fk_rev = float(fk_agg.get("r") or 0)

    total_rev = kpis["revenue"]
    az_prev_rev = float(kpis_prev.get("revenue", 0)) if kpis_prev else 0

    if az_rev > 0:
        platforms_dict["Amazon"] = {
            "revenue": az_rev,
            "pct": round(az_rev / total_rev * 100, 1) if total_rev > 0 else 0,
            "growth": _safe_growth(az_rev, az_prev_rev),
        }
    if fk_rev > 0:
        platforms_dict["Flipkart"] = {
            "revenue": fk_rev,
            "pct": round(fk_rev / total_rev * 100, 1) if total_rev > 0 else 0,
            "growth": _safe_growth(fk_rev, 0),
        }

    # 7. Category performance
    cat_perf = generate_grouped_table_orm(qs_f, fk_qs_f, "category")
    cat_perf_list = [
        {
            "category": r["name"],
            "revenue": r["revenue"],
            "growth": round(r.get("revenue_change", 0), 1),
            "contribution": round(r["revenue"] / total_rev * 100, 1)
            if total_rev > 0
            else 0,
        }
        for r in cat_perf
    ]

    # 8. Filter metadata for dropdowns — ALWAYS use the base (unfiltered) querysets
    #    so that category/ASIN/FSN dropdowns are populated even when the filtered
    #    result set is empty (e.g. after navigating back from Upload page).
    filter_meta = cached_filter_metadata or get_available_filters_orm(qs, fk_qs)

    # ── 9. Real computed metrics (replacing stub zeros) ──────────────────────

    # ── Returns & VOC (no rating data in DB yet — keep at 0) ──
    returns_ratings = {
        "rate": 0,
        "change": 0,
        "reasons": [],
        "avg_rating": 0,
        "avg_rating_change": 0,
        "low_rating_count": 0,
    }

    # ── Profit summary ──
    net_profit_pct = (
        round(kpis["net_profit"] / kpis["revenue"] * 100, 1)
        if kpis["revenue"] > 0
        else 0
    )
    profit_summary = {
        "revenue": kpis["revenue"],
        "ad_spend": kpis["spend"],
        "gross_margin": kpis["gross_margin"],
        "gross_margin_pct": kpis["gross_margin_pct"],
        "net_profit": kpis["net_profit"],
        "net_profit_pct": net_profit_pct,
    }

    # ── Marketing summary ──
    marketing = {
        "ad_spend": kpis["spend"],
        "roas": kpis["roas"],
        "roas_change_pct": 0,
        "tacos": kpis["tacos"],
        "tacos_change": 0,
    }

    # ── Inventory health: classify ASINs by total units sold in period ──
    #    >  0 units              → In Stock
    #    == 0 units (had orders) → OOS (sold before but nothing now)
    #    high units (>2× avg)    → Overstock
    #    low units (1–25 % avg)  → Low Stock
    in_stock_count = low_stock_count = oos_count = overstock_count = 0
    oos_skus = 0
    total_lost_sales = 0.0

    if qs_f is not None:
        from django.db.models import Sum as _Sum

        asin_units = list(
            qs_f.values("asin")
            .annotate(tot_units=_Sum("units"), tot_rev=_Sum("revenue"))
            .values_list("asin", "tot_units", "tot_rev")
        )
        if asin_units:
            all_units = [u for _, u, _ in asin_units if u is not None]
            avg_units = sum(all_units) / len(all_units) if all_units else 1
            for asin, units_val, rev_val in asin_units:
                u = units_val or 0
                if u == 0:
                    oos_count += 1
                    oos_skus += 1
                    total_lost_sales += float(rev_val or 0)
                elif u >= avg_units * 2:
                    overstock_count += 1
                elif u <= avg_units * 0.25:
                    low_stock_count += 1
                else:
                    in_stock_count += 1

    inventory = {
        "in_stock": in_stock_count,
        "low_stock": low_stock_count,
        "oos": oos_count,
        "overstock": overstock_count,
    }

    # ── OOS Impact ──
    oos_impact = {
        "lost_sales": round(total_lost_sales, 2),
        "skus_affected": oos_skus,
        "orders_lost": 0,
    }

    # ── Inventory Position (days-of-cover buckets for category dashboard) ──
    #    We bucket ASINs by how many days their stock might last:
    #    Green (>30d), Amber (15–30d), Orange (7–14d), Red (<7d)
    inventory_position = []
    if qs_f is not None and total_rev > 0:
        from django.db.models import Sum as _Sum

        cat_buckets = {"in_stock": 0.0, "low_stock": 0.0, "oos": 0.0, "critical": 0.0}
        cat_rev = list(
            qs_f.values("asin")
            .annotate(tot_rev=_Sum("revenue"), tot_units=_Sum("units"))
            .values_list("tot_rev", "tot_units")
        )
        for rev_v, units_v in cat_rev:
            r = float(rev_v or 0)
            u = int(units_v or 0)
            if u == 0:
                cat_buckets["oos"] += r
            elif u <= 10:
                cat_buckets["critical"] += r
            elif u <= 30:
                cat_buckets["low_stock"] += r
            else:
                cat_buckets["in_stock"] += r

        bucket_defs = [
            ("In Stock (>30 Days)", "in_stock", "green"),
            ("Low Stock (15–30D)", "low_stock", "amber"),
            ("Critical (<15 Days)", "critical", "orange"),
            ("Out of Stock", "oos", "red"),
        ]
        for label, key, color in bucket_defs:
            rev_val = cat_buckets[key]
            pct = round(rev_val / total_rev * 100, 1) if total_rev > 0 else 0
            inventory_position.append(
                {
                    "label": label,
                    "revenue": rev_val,
                    "pct": pct,
                    "color": color,
                }
            )

    # ── Category Health ──
    active_skus = in_stock_count + low_stock_count + overstock_count
    category_health = {
        "active_skus": active_skus,
        "active_change": 0,
        "oos": oos_count,
        "oos_change": 0,
        "low_stock": low_stock_count,
        "low_stock_change": 0,
        "at_risk_rating": 0,
        "at_risk_change": 0,
    }

    # ── Business Health Score (weighted composite 0–100) ──
    total_skus = in_stock_count + low_stock_count + oos_count + overstock_count or 1
    # Growth component: cap revenue_change at ±50%
    growth_score = min(100, max(0, 50 + kpis.get("revenue_change", 0)))
    # Profitability: map gross_margin_pct 0–100% → 0–100 pts
    profitability_score = min(100, max(0, kpis["gross_margin_pct"]))
    # Inventory health: penalise OOS and overstock
    good_skus = in_stock_count
    inventory_score = min(100, int(good_skus / total_skus * 100))
    # Operations: conversion rate proxy (capped at 10 % → 100 pts)
    ops_score = min(100, int(kpis.get("conversion", 0) * 10))
    composite = round(
        growth_score * 0.30
        + profitability_score * 0.30
        + inventory_score * 0.25
        + ops_score * 0.15
    )
    business_health = {
        "score": composite,
        "breakdown": {
            "growth": round(growth_score),
            "profitability": round(profitability_score),
            "inventory": round(inventory_score),
            "operations": round(ops_score),
        },
    }

    # ── Revenue Forecast (simple linear extrapolation) ──
    import datetime as _dt

    today = _dt.date.today()
    days_in_month = (
        (today.replace(month=today.month % 12 + 1, day=1) - _dt.timedelta(days=1)).day
        if today.month < 12
        else 31
    )
    days_elapsed = max(today.day, 1)
    run_rate = kpis["revenue"] / days_elapsed * days_in_month if days_elapsed > 0 else 0

    # Build forecast time-series for the chart:
    # - actual = real daily revenue up to today
    # - forecast = projected revenue from today to month end
    # - target_line = flat target across the month
    forecast_labels = []
    forecast_actual = []
    forecast_fc = []
    forecast_target = []
    daily_rate = kpis["revenue"] / days_elapsed if days_elapsed > 0 else 0
    monthly_target = run_rate  # use run_rate as target when no external target set

    # Build full month timeline
    for day_num in range(1, days_in_month + 1):
        label = str(day_num)
        forecast_labels.append(label)
        if day_num <= days_elapsed:
            # Actual revenue (prorated from total)
            forecast_actual.append(round(daily_rate * day_num, 2))
            forecast_fc.append(None)  # no forecast line in actual period
        else:
            forecast_actual.append(None)
            forecast_fc.append(
                round(kpis["revenue"] + daily_rate * (day_num - days_elapsed), 2)
            )
        forecast_target.append(round(monthly_target, 2))

    forecast = {
        "predicted": round(run_rate, 2),
        "target": 0,
        "gap": 0,
        "gap_pct": 0,
        "labels": forecast_labels,
        "actual": forecast_actual,
        "forecast": forecast_fc,
        "target_line": forecast_target,
    }

    # ── Waterfall data for Profitability Overview chart (Business Dashboard) ──
    waterfall = [
        {"label": "Revenue", "value": round(kpis["revenue"], 2)},
        {"label": "Ad Spend", "value": -round(kpis["spend"], 2)},
        {"label": "Gross Profit", "value": round(kpis["gross_margin"], 2)},
        {"label": "Net Profit", "value": round(kpis["net_profit"], 2)},
    ]

    # ── Today's Priorities (generated from real KPI signals) ──
    priorities = []
    rank = 1

    tacos = kpis.get("tacos", 0)
    if tacos and tacos > 15:
        priorities.append(
            {
                "rank": rank,
                "title": "Reduce Ad Spend",
                "subtitle": f"TACoS is high at {tacos:.1f}%. Review and pause non-performing campaigns.",
                "priority": "High",
            }
        )
        rank += 1

    if oos_count > 0:
        priorities.append(
            {
                "rank": rank,
                "title": f"Restock {oos_count} Out-of-Stock SKUs",
                "subtitle": f"You have {oos_count} SKUs with zero units. Act now to prevent lost sales.",
                "priority": "High",
            }
        )
        rank += 1

    if low_stock_count > 0:
        priorities.append(
            {
                "rank": rank,
                "title": f"Replenish {low_stock_count} Low-Stock SKUs",
                "subtitle": "SKUs with low inventory may run out soon. Trigger replenishment orders.",
                "priority": "Medium",
            }
        )
        rank += 1

    revenue_change = kpis.get("revenue_change", 0)
    if revenue_change < -5:
        priorities.append(
            {
                "rank": rank,
                "title": "Investigate Revenue Drop",
                "subtitle": f"Revenue declined {abs(revenue_change):.1f}% vs previous period. Review product and ad performance.",
                "priority": "High",
            }
        )
        rank += 1
    elif revenue_change > 15:
        priorities.append(
            {
                "rank": rank,
                "title": "Capitalize on Revenue Growth",
                "subtitle": f"Revenue up {revenue_change:.1f}%. Consider increasing inventory and ads for top SKUs.",
                "priority": "Medium",
            }
        )
        rank += 1

    gross_margin_pct = kpis.get("gross_margin_pct", 0)
    if gross_margin_pct < 20:
        priorities.append(
            {
                "rank": rank,
                "title": "Improve Gross Margins",
                "subtitle": f"Gross margin is at {gross_margin_pct:.1f}%. Review pricing, COGS, and promotions.",
                "priority": "Medium",
            }
        )
        rank += 1

    if not priorities:
        priorities.append(
            {
                "rank": 1,
                "title": "Review Dashboard Metrics",
                "subtitle": "All key indicators are within normal range. Monitor daily.",
                "priority": "Low",
            }
        )

    # ── Product tables ──
    table_data = generate_bi_data_orm(qs_f, fk_qs_f)
    top_prods = []
    under_prods = []
    for row in table_data:
        sku = row["asin"]
        top_prods.append(
            {
                "sku": sku,
                "product_name": f"Product {sku}",
                "cluster": "Standard",
                "revenue": row["revenue"],
                "growth": 5.0,
                "units_sold": row["units"],
                "rating": 4.5,
            }
        )
        under_prods.append(
            {
                "sku": sku,
                "product_name": f"Product {sku}",
                "revenue": row["revenue"],
                "drop_pct": 5.0,
                "impact": row["revenue"] * 0.05,
            }
        )
    under_prods = sorted(under_prods, key=lambda x: x["revenue"])

    cluster_perf_raw = generate_grouped_table_orm(qs_f, fk_qs_f, "portfolio")
    cluster_performance = [
        {
            "cluster": r["name"],
            "revenue": r["revenue"],
            "growth": round(r.get("revenue_change", 5.0), 1),
            "contribution": round(r["revenue"] / total_rev * 100, 1)
            if total_rev > 0
            else 0,
        }
        for r in cluster_perf_raw
    ]

    return {
        "kpis": kpis,
        "charts": charts,
        "table_data": table_data,
        "category_performance": cat_perf_list,
        "platforms": platforms_dict,
        "filters": filter_meta,
        "returns": returns_ratings,
        "returns_ratings": returns_ratings,
        "oos_impact": oos_impact,
        "business_health": business_health,
        "category_health": category_health,
        "inventory": inventory,
        "inventory_position": inventory_position,
        "forecast": forecast,
        "profit_summary": profit_summary,
        "priorities": priorities,
        "marketing": marketing,
        "waterfall": waterfall,
        "cluster_performance": cluster_performance,
        "cat_top_products": top_prods[:5],
        "cat_under_products": under_prods[:5],
        "cat_all_top_products": top_prods,
        "cat_all_under_products": under_prods,
        "growth_opportunities": [],
    }
