from django.db.models import Sum


def generate_kpis_orm(qs, fk_qs, spend_qs=None):
    """
    Generate top-level KPIs strictly using Django ORM aggregations.
    Returns a dict matching the shape expected by the dashboard templates.
    """
    revenue = 0.0
    orders = 0
    units = 0
    pageviews = 0
    spend = 0.0
    active_asins = 0
    cogs = 0.0
    
    az_revenue = 0.0
    fk_revenue = 0.0
    az_orders = 0
    fk_orders = 0
    az_units = 0
    fk_units = 0
    az_spend = 0.0
    fk_spend = 0.0

    if qs is not None:
        agg = qs.aggregate(
            t_rev=Sum("revenue"),
            t_ord=Sum("orders"),
            t_uni=Sum("units"),
            t_pv=Sum("pageviews"),
            t_spend=Sum("total_spend"),
        )
        az_revenue = float(agg["t_rev"] or 0)
        az_orders = int(agg["t_ord"] or 0)
        az_units = int(agg["t_uni"] or 0)
        az_pv = int(agg["t_pv"] or 0)
        az_spend = float(agg["t_spend"] or 0)
        
        revenue += az_revenue
        orders += az_orders
        units += az_units
        pageviews += az_pv
        spend += az_spend
        active_asins += qs.values("asin").distinct().count()

    if fk_qs is not None:
        agg_fk = fk_qs.aggregate(
            t_rev=Sum("taxable_value"),
            t_ord=Sum("orders"),
            t_uni=Sum("units"),
            t_pv=Sum("pageviews"),
            t_spend=Sum("total_spend"),
        )
        fk_revenue = float(agg_fk["t_rev"] or 0)
        fk_orders = int(agg_fk["t_ord"] or 0)
        fk_units = int(agg_fk["t_uni"] or 0)
        fk_pv = int(agg_fk["t_pv"] or 0)
        fk_spend = float(agg_fk["t_spend"] or 0)
        
        revenue += fk_revenue
        orders += fk_orders
        units += fk_units
        pageviews += fk_pv
        spend += fk_spend
        active_asins += fk_qs.values("fsn").distinct().count()

    # Derived metrics
    revenue_for_ads = revenue * 0.7  # Revenue * 0.7 for TACOS/ROAS
    roas = (revenue_for_ads / spend) if spend > 0 else 0
    conversion = (orders / pageviews * 100) if pageviews > 0 else 0
    tacos = (spend / revenue_for_ads * 100) if revenue_for_ads > 0 else 0
    gross_margin = revenue - cogs
    gross_margin_pct = (gross_margin / revenue * 100) if revenue > 0 else 0
    net_profit = gross_margin - spend
    contribution_margin = round(gross_margin_pct - tacos, 1)

    return {
        # Core
        "revenue": revenue,
        "az_revenue": az_revenue,
        "fk_revenue": fk_revenue,
        "orders": orders,
        "az_orders": az_orders,
        "fk_orders": fk_orders,
        "units": units,
        "az_units": az_units,
        "fk_units": fk_units,
        "pageviews": pageviews,
        "spend": spend,
        "az_spend": az_spend,
        "fk_spend": fk_spend,
        "active_asins": active_asins,
        # Rates
        "roas": round(roas, 2),
        "conversion": round(conversion, 2),
        "tacos": round(tacos, 2),
        # Margins
        "cogs": cogs,
        "gross_margin": gross_margin,
        "gross_margin_pct": round(gross_margin_pct, 2),
        "net_profit": net_profit,
        "contribution_margin": contribution_margin,
        # Growth placeholders (populated by pipeline after prev-period comparison)
        "mom_growth": 0,
        "yoy_growth": 0,
        "prev_mom": 0,
        "prev_yoy": 0,
        "revenue_change": 0,
        "orders_change": 0,
        "units_change": 0,
        "spend_change": 0,
        "tacos_change": 0,
        "profit_change": 0,
        "gross_margin_change": 0,
        "contribution_margin_change": 0,
        "orders_pct_change": 0,
        "units_pct_change": 0,
        "run_rate_pct": 0,
        "run_rate_status": "on_track",
    }


def generate_charts_data_orm(qs, fk_qs, table_data=None):
    # ── Trend Data ──
    amazon_trend = {}  # date → revenue (Amazon only)
    flipkart_trend = {}  # date → revenue (Flipkart only)
    merged_trend = {}  # date → merged metrics

    if qs is not None:
        qs_trend = (
            qs.values("date")
            .annotate(
                revenue=Sum("revenue"),
                total_spend=Sum("total_spend"),
                pageviews=Sum("pageviews"),
                orders=Sum("orders"),
            )
            .order_by("date")
        )

        for r in qs_trend:
            dt = str(r["date"])
            rev = float(r["revenue"] or 0)
            amazon_trend[dt] = rev
            merged_trend[dt] = {
                "revenue": rev,
                "total_spend": float(r["total_spend"] or 0),
                "pageviews": int(r["pageviews"] or 0),
                "orders": int(r["orders"] or 0),
            }

    if fk_qs is not None:
        fk_trend = (
            fk_qs.values("date")
            .annotate(
                revenue=Sum("taxable_value"),
                total_spend=Sum("total_spend"),
                pageviews=Sum("pageviews"),
                orders=Sum("orders"),
            )
            .order_by("date")
        )

        for r in fk_trend:
            dt = str(r["date"])
            rev = float(r["revenue"] or 0)
            flipkart_trend[dt] = rev
            if dt not in merged_trend:
                merged_trend[dt] = {
                    "revenue": 0.0,
                    "total_spend": 0.0,
                    "pageviews": 0,
                    "orders": 0,
                }
            merged_trend[dt]["revenue"] += rev
            merged_trend[dt]["total_spend"] += float(r["total_spend"] or 0)
            merged_trend[dt]["pageviews"] += int(r["pageviews"] or 0)
            merged_trend[dt]["orders"] += int(r["orders"] or 0)

    dates = sorted(merged_trend.keys())
    revenue_line = [merged_trend[d]["revenue"] for d in dates]
    spend_line = [merged_trend[d]["total_spend"] for d in dates]
    pv_line = [merged_trend[d]["pageviews"] for d in dates]
    order_line = [merged_trend[d]["orders"] for d in dates]

    # Per-platform series (used by frontend when platform filter = "All")
    amazon_revenue_line = [amazon_trend.get(d, 0) for d in dates]
    flipkart_revenue_line = [flipkart_trend.get(d, 0) for d in dates]

    # ── Portfolio Data ──
    merged_port = {}
    sp_sum = sb_sum = sd_sum = 0.0

    if table_data is not None:
        for r in table_data:
            p = r.get("portfolio") or "Unmapped"
            merged_port[p] = merged_port.get(p, 0) + int(r.get("units", 0))
            sp_sum += float(r.get("spend_sp", 0))
            sb_sum += float(r.get("spend_sb", 0))
            sd_sum += float(r.get("spend_sd", 0))
    else:
        if qs is not None:
            qs_port = qs.values("portfolio").annotate(units=Sum("units"))
            for r in qs_port:
                p = r["portfolio"] or "Unmapped"
                merged_port[p] = merged_port.get(p, 0) + int(r["units"] or 0)

        if fk_qs is not None:
            fk_port = fk_qs.values("portfolio").annotate(units=Sum("units"))
            for r in fk_port:
                p = r["portfolio"] or "Unmapped"
                merged_port[p] = merged_port.get(p, 0) + int(r["units"] or 0)
                
        if qs is not None:
            agg = qs.aggregate(sp=Sum("spend_sp"), sb=Sum("spend_sb"), sd=Sum("spend_sd"))
            sp_sum += float(agg["sp"] or 0)
            sb_sum += float(agg["sb"] or 0)
            sd_sum += float(agg["sd"] or 0)

    sorted_ports = sorted(merged_port.items(), key=lambda x: x[1], reverse=True)[:10]
    port_labels = [k for k, v in sorted_ports]
    port_units = [v for k, v in sorted_ports]

    # ── AdType Data ──
    ad_total = sp_sum + sb_sum + sd_sum
    adTypeLabels = ["SB", "SD", "SP"]
    adTypeVals = [sb_sum, sd_sum, sp_sum]
    ad_legend = []
    for i, lbl in enumerate(adTypeLabels):
        val = adTypeVals[i]
        pct = (val / ad_total * 100) if ad_total > 0 else 0
        ad_legend.append({"label": lbl, "value": val, "pct": round(pct, 1)})

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
            "labels": adTypeLabels,
            "vals": adTypeVals,
            "total": ad_total,
            "legend": ad_legend,
        },
    }

