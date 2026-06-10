from django.db.models import Sum

def generate_charts_data_orm(qs, fk_qs, table_data=None, preaggregated_trend=None):
    # ── Trend Data ──
    amazon_trend = {}  # date → revenue (Amazon only)
    flipkart_trend = {}  # date → revenue (Flipkart only)
    merged_trend = {}  # date → merged metrics

    if preaggregated_trend:
        for dt, values in preaggregated_trend.items():
            amazon_trend[dt] = float(values.get("amazon_revenue", 0) or 0)
            flipkart_trend[dt] = float(values.get("flipkart_revenue", 0) or 0)
            merged_trend[dt] = {
                "revenue": float(values.get("revenue", 0) or 0),
                "total_spend": float(values.get("total_spend", 0) or 0),
                "pageviews": int(values.get("pageviews", 0) or 0),
                "orders": int(values.get("orders", 0) or 0),
            }

    if not preaggregated_trend and qs is not None:
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

    if not preaggregated_trend and fk_qs is not None:
        fk_trend = (
            fk_qs.values("date")
            .annotate(
                revenue=Sum("revenue"),
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
