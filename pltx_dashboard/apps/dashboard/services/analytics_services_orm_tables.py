from django.db.models import Sum


def generate_bi_data_orm(qs, fk_qs):
    az_asins = {}
    if qs is not None:
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
        for r in agg:
            az_asins[r["asin"]] = {
                "asin": r["asin"],
                "revenue": float(r["revenue"] or 0),
                "total_spend": float(r["total_spend"] or 0),
                "orders": int(r["orders"] or 0),
                "pageviews": int(r["pageviews"] or 0),
                "units": int(r["units"] or 0),
                "spend_sp": float(r["spend_sp"] or 0),
                "spend_sb": float(r["spend_sb"] or 0),
                "spend_sd": float(r["spend_sd"] or 0),
            }

    if fk_qs is not None:
        agg_fk = fk_qs.values("fsn").annotate(
            revenue=Sum("taxable_value"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        for r in agg_fk:
            fsn = r["fsn"]
            if fsn in az_asins:
                az_asins[fsn]["revenue"] += float(r["revenue"] or 0)
                az_asins[fsn]["total_spend"] += float(r["total_spend"] or 0)
                az_asins[fsn]["orders"] += int(r["orders"] or 0)
                az_asins[fsn]["pageviews"] += int(r["pageviews"] or 0)
                az_asins[fsn]["units"] += int(r["units"] or 0)
            else:
                az_asins[fsn] = {
                    "asin": fsn,
                    "revenue": float(r["revenue"] or 0),
                    "total_spend": float(r["total_spend"] or 0),
                    "orders": int(r["orders"] or 0),
                    "pageviews": int(r["pageviews"] or 0),
                    "units": int(r["units"] or 0),
                    "spend_sp": 0.0,
                    "spend_sb": 0.0,
                    "spend_sd": 0.0,
                }

    return sorted(az_asins.values(), key=lambda x: x["revenue"], reverse=True)


def generate_period_table_orm(qs, fk_qs, group_col):
    group_data = {}
    dates_set = set()

    if qs is not None:
        agg = qs.values("date", group_col).annotate(
            revenue=Sum("revenue"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        for r in agg:
            dt = str(r["date"])
            dates_set.add(dt)
            key = r[group_col] or "Unknown"
            if key not in group_data:
                group_data[key] = {}
            if dt not in group_data[key]:
                group_data[key][dt] = {
                    "revenue": 0,
                    "orders": 0,
                    "units": 0,
                    "pageviews": 0,
                    "spends": 0,
                }

            group_data[key][dt]["revenue"] += float(r["revenue"] or 0)
            group_data[key][dt]["orders"] += int(r["orders"] or 0)
            group_data[key][dt]["units"] += int(r["units"] or 0)
            group_data[key][dt]["pageviews"] += int(r["pageviews"] or 0)
            group_data[key][dt]["spends"] += float(r["total_spend"] or 0)

    if fk_qs is not None:
        group_fk = "fsn" if group_col == "asin" else group_col
        agg_fk = fk_qs.values("date", group_fk).annotate(
            revenue=Sum("taxable_value"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        for r in agg_fk:
            dt = str(r["date"])
            dates_set.add(dt)
            key = r[group_fk] or "Unknown"
            if key not in group_data:
                group_data[key] = {}
            if dt not in group_data[key]:
                group_data[key][dt] = {
                    "revenue": 0,
                    "orders": 0,
                    "units": 0,
                    "pageviews": 0,
                    "spends": 0,
                }

            group_data[key][dt]["revenue"] += float(r["revenue"] or 0)
            group_data[key][dt]["orders"] += int(r["orders"] or 0)
            group_data[key][dt]["units"] += int(r["units"] or 0)
            group_data[key][dt]["pageviews"] += int(r["pageviews"] or 0)
            group_data[key][dt]["spends"] += float(r["total_spend"] or 0)

    for k in group_data:
        for dt in group_data[k]:
            v = group_data[k][dt]
            v["cvr"] = (v["orders"] / v["pageviews"] * 100) if v["pageviews"] > 0 else 0
            v["upo"] = (v["units"] / v["orders"]) if v["orders"] > 0 else 0

    return {"data": group_data, "dates": sorted(list(dates_set))}


def generate_grouped_table_orm(qs, fk_qs, group_col):
    group_data = {}
    if qs is not None:
        agg = qs.values(group_col).annotate(
            revenue=Sum("revenue"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        for r in agg:
            k = r[group_col] or "Unknown"
            if k not in group_data:
                group_data[k] = {
                    "name": k,
                    "revenue": 0,
                    "orders": 0,
                    "units": 0,
                    "pageviews": 0,
                    "spends": 0,
                }
            group_data[k]["revenue"] += float(r["revenue"] or 0)
            group_data[k]["orders"] += int(r["orders"] or 0)
            group_data[k]["units"] += int(r["units"] or 0)
            group_data[k]["pageviews"] += int(r["pageviews"] or 0)
            group_data[k]["spends"] += float(r["total_spend"] or 0)

    if fk_qs is not None:
        group_fk = "fsn" if group_col == "asin" else group_col
        agg_fk = fk_qs.values(group_fk).annotate(
            revenue=Sum("taxable_value"),
            total_spend=Sum("total_spend"),
            orders=Sum("orders"),
            pageviews=Sum("pageviews"),
            units=Sum("units"),
        )
        for r in agg_fk:
            k = r[group_fk] or "Unknown"
            if k not in group_data:
                group_data[k] = {
                    "name": k,
                    "revenue": 0,
                    "orders": 0,
                    "units": 0,
                    "pageviews": 0,
                    "spends": 0,
                }
            group_data[k]["revenue"] += float(r["revenue"] or 0)
            group_data[k]["orders"] += int(r["orders"] or 0)
            group_data[k]["units"] += int(r["units"] or 0)
            group_data[k]["pageviews"] += int(r["pageviews"] or 0)
            group_data[k]["spends"] += float(r["total_spend"] or 0)

    for k, v in group_data.items():
        v["cvr"] = (v["orders"] / v["pageviews"] * 100) if v["pageviews"] > 0 else 0
        v["upo"] = (v["units"] / v["orders"]) if v["orders"] > 0 else 0
        v["roas"] = (v["revenue"] / v["spends"]) if v["spends"] > 0 else 0

    return sorted(group_data.values(), key=lambda x: x["revenue"], reverse=True)
