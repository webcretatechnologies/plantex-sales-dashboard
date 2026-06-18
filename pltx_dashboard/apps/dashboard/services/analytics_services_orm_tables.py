from django.db.models import Sum
from concurrent.futures import ThreadPoolExecutor

def generate_bi_data_orm(qs, fk_qs, user=None, asin_meta=None, fsn_meta=None):
    az_asins = {}
    
    # Define query execution functions to run in parallel
    def fetch_az():
        if qs is not None:
            return list(qs.values("asin").annotate(
                revenue=Sum("revenue"),
                total_spend=Sum("ad_spend"),
                orders=Sum("orders"),
                pageviews=Sum("page_views"),
                units=Sum("units_sold"),
            ))
        return []

    def fetch_fk():
        if fk_qs is not None:
            return list(fk_qs.values("fsn").annotate(
                revenue=Sum("revenue"),
                total_spend=Sum("ad_spend"),
                orders=Sum("orders"),
                pageviews=Sum("page_views"),
                units=Sum("units_sold"),
            ))
        return []

    # Evaluate queries concurrently
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_az = executor.submit(fetch_az)
        future_fk = executor.submit(fetch_fk)
        agg = future_az.result()
        agg_fk = future_fk.result()

    if agg:
        # Build a separate lookup for the best category/portfolio per ASIN.
        # Use pre-fetched metadata if provided by the caller to avoid duplicate DB queries.
        if asin_meta is None:
            from apps.dashboard.models import CategoryMapping
            asin_meta = {}
            if user:
                for row in CategoryMapping.objects.filter(user=user).values("asin", "category", "portfolio"):
                    asin_meta[row["asin"]] = {
                        "category": row["category"] or "",
                        "portfolio": row["portfolio"] or "",
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
                "spend_sp": 0.0,
                "spend_sb": 0.0,
                "spend_sd": 0.0,
                "az_revenue": float(r["revenue"] or 0),
                "fk_revenue": 0.0,
                "az_orders": int(r["orders"] or 0),
                "fk_orders": 0,
                "az_units": int(r["units"] or 0),
                "fk_units": 0,
                "az_spend": float(r["total_spend"] or 0),
                "fk_spend": 0.0,
                "az_pageviews": int(r["pageviews"] or 0),
                "fk_pageviews": 0,
            }

    if agg_fk:
        # Use pre-fetched metadata if provided by the caller to avoid duplicate DB queries.
        if fsn_meta is None:
            from apps.dashboard.models import FlipkartCategoryMap
            fsn_meta = {}
            if user:
                for row in FlipkartCategoryMap.objects.filter(user=user).values("fsn", "category", "portfolio", "asin"):
                    fsn_meta[row["fsn"]] = {
                        "category": row["category"] or "",
                        "portfolio": row["portfolio"] or "",
                        "asin": row["asin"] or "",
                    }

        for r in agg_fk:
            fsn = r["fsn"]
            meta = fsn_meta.get(fsn, {"category": "", "portfolio": "", "asin": ""})
            mapped_asin = str(meta.get("asin") or "").strip()
            
            # Target key is the mapped ASIN if it exists, otherwise fallback to FSN
            target_key = mapped_asin if mapped_asin else fsn

            fk_pv = int(r["pageviews"] or 0)
            if target_key in az_asins:
                az_asins[target_key]["revenue"] += float(r["revenue"] or 0)
                az_asins[target_key]["total_spend"] += float(r["total_spend"] or 0)
                az_asins[target_key]["orders"] += int(r["orders"] or 0)
                az_asins[target_key]["pageviews"] += fk_pv
                az_asins[target_key]["units"] += int(r["units"] or 0)
                az_asins[target_key]["fk_revenue"] += float(r["revenue"] or 0)
                az_asins[target_key]["fk_orders"] = az_asins[target_key].get("fk_orders", 0) + int(r["orders"] or 0)
                az_asins[target_key]["fk_units"] = az_asins[target_key].get("fk_units", 0) + int(r["units"] or 0)
                az_asins[target_key]["fk_spend"] = az_asins[target_key].get("fk_spend", 0.0) + float(r["total_spend"] or 0)
                az_asins[target_key]["fk_pageviews"] = az_asins[target_key].get("fk_pageviews", 0) + fk_pv
            else:
                amz_meta = asin_meta.get(target_key, {}) if asin_meta else {}
                cat = amz_meta.get("category") or meta["category"]
                port = amz_meta.get("portfolio") or meta["portfolio"]
                az_asins[target_key] = {
                    "asin": target_key,
                    "category": cat,
                    "portfolio": port,
                    "revenue": float(r["revenue"] or 0),
                    "total_spend": float(r["total_spend"] or 0),
                    "orders": int(r["orders"] or 0),
                    "pageviews": fk_pv,
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
                    "az_pageviews": 0,
                    "fk_pageviews": fk_pv,
                }

    return sorted(az_asins.values(), key=lambda x: x["revenue"], reverse=True)

