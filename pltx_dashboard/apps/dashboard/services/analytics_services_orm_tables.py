from django.db.models import Sum


def generate_bi_data_orm(qs, fk_qs):
    az_asins = {}
    if qs is not None:
        # Group by asin only to avoid duplicates from varying category/portfolio values
        agg = qs.values("asin").annotate(
            revenue=Sum("revenue"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
            spend_sp=Sum("spend_sp"),
            spend_sb=Sum("spend_sb"),
            spend_sd=Sum("spend_sd"),
        )
        # Build a separate lookup for the best category/portfolio per ASIN
        # (pick from the row with the most revenue to get the most relevant mapping)
        asin_meta = {}
        for r in qs.values("asin", "category", "portfolio").annotate(
            rev=Sum("revenue")
        ).order_by("-rev"):
            a = r["asin"]
            if a not in asin_meta:
                asin_meta[a] = {
                    "category": r["category"] or "",
                    "portfolio": r["portfolio"] or "",
                }

        for r in agg:
            a = r["asin"]
            meta = asin_meta.get(a, {"category": "", "portfolio": ""})
            az_asins[a] = {
                "asin": a,
                "category": meta["category"],
                "portfolio": meta["portfolio"],
                "revenue": float(r["revenue"] or 0),
                "total_spend": float(r["total_spend"] or 0),
                "orders": int(r["orders"] or 0),
                "pageviews": int(r["pageviews"] or 0),
                "units": int(r["units"] or 0),
                "spend_sp": float(r["spend_sp"] or 0),
                "spend_sb": float(r["spend_sb"] or 0),
                "spend_sd": float(r["spend_sd"] or 0),
                "az_revenue": float(r["revenue"] or 0),
                "fk_revenue": 0.0,
                "az_orders": int(r["orders"] or 0),
                "fk_orders": 0,
                "az_units": int(r["units"] or 0),
                "fk_units": 0,
                "az_spend": float(r["total_spend"] or 0),
                "fk_spend": 0.0,
            }

    if fk_qs is not None:
        # Group by fsn only to avoid duplicates
        agg_fk = fk_qs.values("fsn").annotate(
            revenue=Sum("revenue"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        # Best category/portfolio per FSN
        fsn_meta = {}
        for r in fk_qs.values("fsn", "category", "portfolio").annotate(
            rev=Sum("revenue")
        ).order_by("-rev"):
            f = r["fsn"]
            if f not in fsn_meta:
                fsn_meta[f] = {
                    "category": r["category"] or "",
                    "portfolio": r["portfolio"] or "",
                }

        for r in agg_fk:
            fsn = r["fsn"]
            meta = fsn_meta.get(fsn, {"category": "", "portfolio": ""})
            if fsn in az_asins:
                az_asins[fsn]["revenue"] += float(r["revenue"] or 0)
                az_asins[fsn]["total_spend"] += float(r["total_spend"] or 0)
                az_asins[fsn]["orders"] += int(r["orders"] or 0)
                az_asins[fsn]["pageviews"] += int(r["pageviews"] or 0)
                az_asins[fsn]["units"] += int(r["units"] or 0)
                az_asins[fsn]["fk_revenue"] += float(r["revenue"] or 0)
                az_asins[fsn]["fk_orders"] = az_asins[fsn].get("fk_orders", 0) + int(r["orders"] or 0)
                az_asins[fsn]["fk_units"] = az_asins[fsn].get("fk_units", 0) + int(r["units"] or 0)
                az_asins[fsn]["fk_spend"] = az_asins[fsn].get("fk_spend", 0.0) + float(r["total_spend"] or 0)
            else:
                az_asins[fsn] = {
                    "asin": fsn,
                    "category": meta["category"],
                    "portfolio": meta["portfolio"],
                    "revenue": float(r["revenue"] or 0),
                    "total_spend": float(r["total_spend"] or 0),
                    "orders": int(r["orders"] or 0),
                    "pageviews": int(r["pageviews"] or 0),
                    "units": int(r["units"] or 0),
                    "spend_sp": 0.0,
                    "spend_sb": 0.0,
                    "spend_sd": 0.0,
                    "az_revenue": 0.0,
                    "fk_revenue": float(r["revenue"] or 0),
                    "az_orders": 0,
                    "fk_orders": int(r["orders"] or 0),
                    "az_units": 0,
                    "fk_units": int(r["units"] or 0),
                    "az_spend": 0.0,
                    "fk_spend": float(r["total_spend"] or 0),
                }

    return sorted(az_asins.values(), key=lambda x: x["revenue"], reverse=True)

