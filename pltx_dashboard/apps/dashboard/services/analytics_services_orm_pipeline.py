import datetime

from apps.dashboard.services.analytics_services_orm import (
    generate_kpis_orm,
    generate_charts_data_orm,
)
from apps.dashboard.services.analytics_services_orm_tables import (
    generate_bi_data_orm,
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

from django.core.cache import cache

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


import time
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
    for r in table_data_prev:
        port = r.get("portfolio") or "Unknown"
        prev_rev_by_port[port] = prev_rev_by_port.get(port, 0) + r["revenue"]
    
    total_revenue = sum(r["revenue"] for r in table_data)
    total_spend = sum(r["total_spend"] for r in table_data)
    
    # 3. KPIs
    kpis = {
        "revenue": total_revenue,
        "orders": sum(r["orders"] for r in table_data),
        "units": sum(r["units"] for r in table_data),
        "pageviews": sum(r["pageviews"] for r in table_data),
        "spend": total_spend,
        "active_asins": len(table_data),
        "cogs": 0.0,
    }
    
    roas = (kpis["revenue"] / kpis["spend"]) if kpis["spend"] > 0 else 0
    conversion = (kpis["orders"] / kpis["pageviews"] * 100) if kpis["pageviews"] > 0 else 0
    aov = (kpis["revenue"] / kpis["orders"]) if kpis["orders"] > 0 else 0
    tacos = (kpis["spend"] / kpis["revenue"] * 100) if kpis["revenue"] > 0 else 0
    gross_margin = kpis["revenue"] - kpis["cogs"]
    gross_margin_pct = (gross_margin / kpis["revenue"] * 100) if kpis["revenue"] > 0 else 0
    net_profit = gross_margin - kpis["spend"]
    contribution_margin = round(gross_margin_pct - tacos, 1)

    kpis.update({
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "aov": round(aov, 2),
        "tacos": round(tacos, 2),
        "gross_margin": gross_margin,
        "gross_margin_pct": round(gross_margin_pct, 2),
        "net_profit": net_profit,
        "contribution_margin": contribution_margin,
    })

    kpis_prev = generate_kpis_orm(qs_prev_f, fk_prev_f, spend_qs)

    # 4. Populate growth fields in-place
    for key in ["revenue", "orders", "units", "spend", "roas", "aov", "tacos"]:
        curr = kpis.get(key, 0)
        prev = kpis_prev.get(key, 0)
        kpis[f"{key}_change"] = _safe_growth(curr, prev)

    kpis["mom_growth"] = kpis.get("revenue_change", 0)
    kpis["yoy_growth"] = kpis.get("revenue_change", 0)
    kpis["prev_mom"] = round(kpis_prev.get("mom_growth", 0), 1)
    kpis["prev_yoy"] = 0
    kpis["profit_change"] = _safe_growth(kpis["net_profit"], kpis_prev.get("net_profit", 0))

    # 5. Charts
    charts = generate_charts_data_orm(qs_f, fk_qs_f, table_data=table_data)

    # 6. Platform breakdown
    az_rev = sum(r.get("az_revenue", 0) for r in table_data)
    fk_rev = sum(r.get("fk_revenue", 0) for r in table_data)
    az_prev_rev = float(kpis_prev.get("revenue", 0)) if kpis_prev else 0

    platforms_dict = {}
    if az_rev > 0:
        platforms_dict["Amazon"] = {
            "revenue": az_rev,
            "pct": round(az_rev / total_revenue * 100, 1) if total_revenue > 0 else 0,
            "growth": _safe_growth(az_rev, az_prev_rev),
        }
    if fk_rev > 0:
        platforms_dict["Flipkart"] = {
            "revenue": fk_rev,
            "pct": round(fk_rev / total_revenue * 100, 1) if total_revenue > 0 else 0,
            "growth": _safe_growth(fk_rev, 0),
        }

    # 7. Category performance
    cat_perf_dict = {}
    for r in table_data:
        cat = r.get("category") or "Unknown"
        if cat not in cat_perf_dict:
            cat_perf_dict[cat] = {"name": cat, "revenue": 0.0}
        cat_perf_dict[cat]["revenue"] += r["revenue"]

    cat_perf_list = [
        {
            "category": v["name"],
            "revenue": v["revenue"],
            "growth": 0.0,
            "contribution": round(v["revenue"] / total_revenue * 100, 1) if total_revenue > 0 else 0,
        }
        for v in cat_perf_dict.values()
    ]
    cat_perf_list.sort(key=lambda x: x["revenue"], reverse=True)

    # 8. Filter metadata for dropdowns
    filter_meta = cached_filter_metadata or get_available_filters_orm(qs, fk_qs)

    returns_ratings = {
        "rate": 0, "change": 0, "reasons": [], "avg_rating": 0, "avg_rating_change": 0, "low_rating_count": 0,
    }

    net_profit_pct = round(kpis["net_profit"] / kpis["revenue"] * 100, 1) if kpis["revenue"] > 0 else 0
    profit_summary = {
        "revenue": kpis["revenue"], "ad_spend": kpis["spend"], "gross_margin": kpis["gross_margin"],
        "gross_margin_pct": kpis["gross_margin_pct"], "net_profit": kpis["net_profit"], "net_profit_pct": net_profit_pct,
    }

    marketing = {
        "ad_spend": kpis["spend"], "roas": kpis["roas"], "roas_change_pct": 0, "tacos": kpis["tacos"], "tacos_change": 0,
    }

    in_stock_count = low_stock_count = oos_count = overstock_count = oos_skus = 0
    total_lost_sales = 0.0

    all_units = [r["units"] for r in table_data if r["units"] > 0]
    avg_units = sum(all_units) / len(all_units) if all_units else 1

    for r in table_data:
        u = r["units"]
        rev = r["revenue"]
        if u == 0:
            oos_count += 1
            oos_skus += 1
            total_lost_sales += rev
        elif u >= avg_units * 2:
            overstock_count += 1
        elif u <= avg_units * 0.25:
            low_stock_count += 1
        else:
            in_stock_count += 1

    inventory = {
        "in_stock": in_stock_count, "low_stock": low_stock_count, "oos": oos_count, "overstock": overstock_count,
    }

    oos_impact = {"lost_sales": round(total_lost_sales, 2), "skus_affected": oos_skus, "orders_lost": 0}

    inventory_position = []
    if total_revenue > 0:
        cat_buckets = {"in_stock": 0.0, "low_stock": 0.0, "oos": 0.0, "critical": 0.0}
        for r in table_data:
            rev_val = r.get("az_revenue", 0)  # inventory based on amazon originally
            u = r["units"]
            if u == 0:
                cat_buckets["oos"] += rev_val
            elif u <= 10:
                cat_buckets["critical"] += rev_val
            elif u <= 30:
                cat_buckets["low_stock"] += rev_val
            else:
                cat_buckets["in_stock"] += rev_val

        bucket_defs = [
            ("In Stock (>30 Days)", "in_stock", "green"),
            ("Low Stock (15–30D)", "low_stock", "amber"),
            ("Critical (<15 Days)", "critical", "orange"),
            ("Out of Stock", "oos", "red"),
        ]
        for label, key, color in bucket_defs:
            rev_val = cat_buckets[key]
            pct = round(rev_val / total_revenue * 100, 1) if total_revenue > 0 else 0
            inventory_position.append({"label": label, "revenue": rev_val, "pct": pct, "color": color})

    category_health = {
        "active_skus": in_stock_count + low_stock_count + overstock_count, "active_change": 0,
        "oos": oos_count, "oos_change": 0, "low_stock": low_stock_count, "low_stock_change": 0,
        "at_risk_rating": 0, "at_risk_change": 0,
    }

    total_skus = in_stock_count + low_stock_count + oos_count + overstock_count or 1
    growth_score = min(100, max(0, 50 + kpis.get("revenue_change", 0)))
    profitability_score = min(100, max(0, kpis["gross_margin_pct"]))
    inventory_score = min(100, int(in_stock_count / total_skus * 100))
    ops_score = min(100, int(kpis.get("conversion", 0) * 10))
    
    business_health = {
        "score": round(growth_score * 0.3 + profitability_score * 0.3 + inventory_score * 0.25 + ops_score * 0.15),
        "breakdown": {"growth": round(growth_score), "profitability": round(profitability_score), "inventory": round(inventory_score), "operations": round(ops_score)}
    }

    import datetime as _dt
    today = _dt.date.today()
    days_in_month = (today.replace(month=today.month % 12 + 1, day=1) - _dt.timedelta(days=1)).day if today.month < 12 else 31
    days_elapsed = max(today.day, 1)
    run_rate = kpis["revenue"] / days_elapsed * days_in_month if days_elapsed > 0 else 0

    forecast_labels, forecast_actual, forecast_fc, forecast_target = [], [], [], []
    daily_rate = kpis["revenue"] / days_elapsed if days_elapsed > 0 else 0
    for day_num in range(1, days_in_month + 1):
        forecast_labels.append(str(day_num))
        if day_num <= days_elapsed:
            forecast_actual.append(round(daily_rate * day_num, 2))
            forecast_fc.append(None)
        else:
            forecast_actual.append(None)
            forecast_fc.append(round(kpis["revenue"] + daily_rate * (day_num - days_elapsed), 2))
        forecast_target.append(round(run_rate, 2))

    forecast = {
        "predicted": round(run_rate, 2), "target": 0, "gap": 0, "gap_pct": 0, "labels": forecast_labels,
        "actual": forecast_actual, "forecast": forecast_fc, "target_line": forecast_target,
    }

    waterfall = [
        {"label": "Revenue", "value": round(kpis["revenue"], 2)}, {"label": "Ad Spend", "value": -round(kpis["spend"], 2)},
        {"label": "Gross Profit", "value": round(kpis["gross_margin"], 2)}, {"label": "Net Profit", "value": round(kpis["net_profit"], 2)},
    ]

    priorities = []
    if kpis.get("tacos", 0) > 15:
        priorities.append({"rank": len(priorities)+1, "title": "Reduce Ad Spend", "subtitle": f"TACoS is high at {kpis['tacos']:.1f}%. Review campaigns.", "priority": "High"})
    if oos_count > 0:
        priorities.append({"rank": len(priorities)+1, "title": f"Restock {oos_count} Out-of-Stock SKUs", "subtitle": "Act now to prevent lost sales.", "priority": "High"})
    if low_stock_count > 0:
        priorities.append({"rank": len(priorities)+1, "title": f"Replenish {low_stock_count} Low-Stock SKUs", "subtitle": "Trigger replenishment orders.", "priority": "Medium"})
    if kpis.get("revenue_change", 0) < -5:
        priorities.append({"rank": len(priorities)+1, "title": "Investigate Revenue Drop", "subtitle": f"Revenue declined {abs(kpis['revenue_change']):.1f}%.", "priority": "High"})
    elif kpis.get("revenue_change", 0) > 15:
        priorities.append({"rank": len(priorities)+1, "title": "Capitalize on Revenue Growth", "subtitle": f"Revenue up {kpis['revenue_change']:.1f}%.", "priority": "Medium"})
    if kpis.get("gross_margin_pct", 0) < 20:
        priorities.append({"rank": len(priorities)+1, "title": "Improve Gross Margins", "subtitle": f"Gross margin is at {kpis['gross_margin_pct']:.1f}%.", "priority": "Medium"})
    if not priorities:
        priorities.append({"rank": 1, "title": "Review Dashboard Metrics", "subtitle": "All indicators normal.", "priority": "Low"})

    top_prods, under_prods = [], []
    for row in table_data:
        sku = row["asin"]
        curr_rev = row["revenue"]
        prev_rev = prev_rev_by_asin.get(sku, 0)
        growth = _safe_growth(curr_rev, prev_rev)
        
        top_prods.append({"sku": sku, "product_name": f"Product {sku}", "cluster": row.get("portfolio") or "Standard", "revenue": curr_rev, "growth": growth, "units_sold": row["units"], "rating": 4.5})
        under_prods.append({"sku": sku, "product_name": f"Product {sku}", "revenue": curr_rev, "drop_pct": growth, "impact": curr_rev - prev_rev})
    under_prods.sort(key=lambda x: x["revenue"])

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
        "platforms": platforms_dict, "filters": filter_meta, "returns": returns_ratings, "returns_ratings": returns_ratings,
        "oos_impact": oos_impact, "business_health": business_health, "category_health": category_health,
        "inventory": inventory, "inventory_position": inventory_position, "forecast": forecast,
        "profit_summary": profit_summary, "priorities": priorities, "marketing": marketing,
        "waterfall": waterfall, "cluster_performance": cluster_performance, "cat_top_products": top_prods[:5],
        "cat_under_products": under_prods[:5], "cat_all_top_products": top_prods[:100], "cat_all_under_products": under_prods[:100],
        "growth_opportunities": [],
    }
