import datetime

from django.db.models import Count, Q, Sum

from apps.dashboard.models import (
    CategoryMapping,
    DashboardInventoryHealthSummary,
    FBAStockData,
    FlexStockData,
    FlipkartCategoryMap,
    FlipkartInventoryStock,
    Flipkartfba,
)


def _num(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _pct(numerator, denominator):
    denominator = _num(denominator)
    if denominator <= 0:
        return 0.0
    return round((_num(numerator) / denominator) * 100, 2)


def _latest_date_for(model, user):
    from django.db.models import Max

    return model.objects.filter(user=user).aggregate(m=Max("date")).get("m")


def _date_text(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.strftime("%Y-%m-%d")
    return str(value or "")


def _load_amazon_meta(user, asins):
    rows = CategoryMapping.objects.filter(user=user, asin__in=asins)
    return {
        row["asin"]: row
        for row in rows.values(
            "asin",
            "msku",
            "launch_date",
            "category",
            "portfolio",
            "category_manager",
        )
    }


def _load_flipkart_meta(user, fsns):
    rows = FlipkartCategoryMap.objects.filter(user=user, fsn__in=fsns)
    return {
        row["fsn"]: row
        for row in rows.values(
            "fsn",
            "asin",
            "sku",
            "launch_date",
            "category",
            "portfolio",
            "category_manager",
        )
    }


def _load_flipkart_meta_by_asin(user, asins):
    if not asins:
        return {}

    meta = {}
    rows = (
        FlipkartCategoryMap.objects.filter(user=user, asin__in=asins)
        .exclude(asin__isnull=True)
        .exclude(asin="")
        .order_by("asin", "-launch_date", "fsn")
        .values(
            "fsn",
            "asin",
            "sku",
            "launch_date",
            "category",
            "portfolio",
            "category_manager",
        )
    )
    for row in rows:
        asin = str(row.get("asin") or "").strip()
        if asin and asin not in meta:
            meta[asin] = row
    return meta


def _latest_amazon_stock(user, asins):
    fba_date = _latest_date_for(FBAStockData, user)
    flex_date = _latest_date_for(FlexStockData, user)
    fba = {}
    fba_fc_count = {}
    flex = {}

    if fba_date and asins:
        fba_qs = FBAStockData.objects.filter(user=user, date=fba_date, asin__in=asins)
        for row in fba_qs.values("asin").annotate(qty=Sum("ending_warehouse_balance")):
            fba[row["asin"]] = int(row["qty"] or 0)
        for row in (
            fba_qs.filter(ending_warehouse_balance__gt=0)
            .values("asin")
            .annotate(cnt=Count("location", distinct=True))
        ):
            fba_fc_count[row["asin"]] = int(row["cnt"] or 0)

    if flex_date and asins:
        for row in (
            FlexStockData.objects.filter(user=user, date=flex_date, asin__in=asins)
            .values("asin")
            .annotate(qty=Sum("qty"))
        ):
            flex[row["asin"]] = int(row["qty"] or 0)

    return fba, flex, fba_fc_count


def _latest_flipkart_stock(user, fsns):
    fba_date = _latest_date_for(Flipkartfba, user)
    inv_date = _latest_date_for(FlipkartInventoryStock, user)
    fba = {}
    fba_fc_count = {}
    flex = {}

    if fba_date and fsns:
        fba_qs = Flipkartfba.objects.filter(user=user, date=fba_date, fsn__in=fsns)
        for row in fba_qs.values("fsn").annotate(qty=Sum("live_on_website")):
            fba[row["fsn"]] = int(row["qty"] or 0)
        for row in (
            fba_qs.filter(live_on_website__gt=0)
            .values("fsn")
            .annotate(cnt=Count("warehouse_id", distinct=True))
        ):
            fba_fc_count[row["fsn"]] = int(row["cnt"] or 0)

    if inv_date and fsns:
        for row in (
            FlipkartInventoryStock.objects.filter(user=user, date=inv_date, fsn__in=fsns)
            .values("fsn")
            .annotate(qty=Sum("qty"))
        ):
            flex[row["fsn"]] = int(row["qty"] or 0)

    return fba, flex, fba_fc_count


def _latest_docs(user, asins, fsns):
    from django.db.models import Max

    max_date = DashboardInventoryHealthSummary.objects.filter(user=user, platform="Combined").aggregate(
        m=Max("date")
    ).get("m")
    amz_doc = {}
    fk_doc = {}
    if not max_date:
        return amz_doc, fk_doc

    qs = DashboardInventoryHealthSummary.objects.filter(user=user, platform="Combined", date=max_date)
    if asins or fsns:
        qs = qs.filter(Q(asin__in=asins) | Q(fsn__in=fsns))
    for row in qs.values("asin", "fsn", "doc", "fk_doc"):
        if row["asin"]:
            amz_doc[row["asin"]] = round(float(row["doc"] or 0), 2)
        if row["fsn"]:
            fk_doc[row["fsn"]] = round(float(row["fk_doc"] or 0), 2)
    return amz_doc, fk_doc


def _fk_clicks_by_fsn(user, filters, fsns):
    if not fsns:
        return {}
    from apps.dashboard.services.analytics_services_orm_pipeline import _get_product_daily_summary_querysets

    _, summary_fk_qs = _get_product_daily_summary_querysets(user, filters)
    summary_fk_qs = summary_fk_qs.filter(fsn__in=fsns)
    return {
        row["fsn"]: {
            "product_clicks": int(row["product_clicks"] or 0),
            "sales": int(row["sales"] or 0),
        }
        for row in summary_fk_qs.values("fsn").annotate(
            product_clicks=Sum("product_clicks"),
            sales=Sum("sales"),
        )
    }


def _has_direct_product_filter(filters):
    return any(filters.get(field) for field in ("asin", "fsn", "sku", "parent_asin"))


def _distinct_non_empty(qs, field_name):
    return [
        str(value).strip()
        for value in qs.values_list(field_name, flat=True).distinct()
        if str(value or "").strip()
    ]


def build_npd_performance(user, filters, qs_f, fk_qs_f, limit=None, include_trend=True):
    az_metrics = {}
    fk_metrics = {}
    from apps.dashboard.services.analytics_services_orm_pipeline import _get_product_daily_summary_querysets
    from apps.dashboard.services.filters import get_filtered_mapping_querysets

    summary_az_qs, summary_fk_qs = _get_product_daily_summary_querysets(user, filters)
    az_map_qs, fk_map_qs = get_filtered_mapping_querysets(filters, user)

    include_missing_launch_dates = _has_direct_product_filter(filters)
    npd_az_meta = az_map_qs
    npd_fk_meta = fk_map_qs
    if not include_missing_launch_dates:
        npd_az_meta = npd_az_meta.exclude(launch_date__isnull=True)

    npd_asins = _distinct_non_empty(npd_az_meta, "asin")
    npd_fsns = _distinct_non_empty(npd_fk_meta, "fsn")

    if npd_asins:
        for row in summary_az_qs.filter(asin__in=npd_asins).values("asin").annotate(
            pageviews=Sum("page_views"),
            orders=Sum("orders"),
            units=Sum("units_sold"),
            revenue=Sum("revenue"),
            total_spend=Sum("ad_spend"),
        ):
            asin = str(row.get("asin") or "").strip()
            if asin:
                az_metrics[asin] = row
        for asin in npd_asins:
            az_metrics.setdefault(asin, {})

    if npd_fsns:
        for row in summary_fk_qs.filter(fsn__in=npd_fsns).values("fsn").annotate(
            pageviews=Sum("page_views"),
            units=Sum("units_sold"),
            revenue=Sum("revenue"),
            total_spend=Sum("ad_spend"),
        ):
            fsn = str(row.get("fsn") or "").strip()
            if fsn:
                fk_metrics[fsn] = row
        for fsn in npd_fsns:
            fk_metrics.setdefault(fsn, {})

    asins = set(az_metrics)
    fsns = set(fk_metrics)
    az_meta = _load_amazon_meta(user, asins) if asins else {}
    fk_meta = _load_flipkart_meta(user, fsns) if fsns else {}
    fk_asins = {
        str((meta or {}).get("asin") or "").strip()
        for meta in fk_meta.values()
        if str((meta or {}).get("asin") or "").strip()
    }
    if fk_asins:
        az_meta.update({
            asin: meta
            for asin, meta in _load_amazon_meta(user, fk_asins).items()
            if asin not in az_meta
        })
    fk_meta_by_asin = _load_flipkart_meta_by_asin(user, asins | fk_asins)

    if not include_missing_launch_dates:
        az_metrics = {asin: data for asin, data in az_metrics.items() if az_meta.get(asin, {}).get("launch_date")}
    asins = set(az_metrics)
    fsns = set(fk_metrics)

    az_fba, az_flex, az_fc_count = _latest_amazon_stock(user, asins)
    fk_fba, fk_flex, fk_fc_count = _latest_flipkart_stock(user, fsns)
    az_doc, fk_doc = _latest_docs(user, asins, fsns)
    fk_clicks = _fk_clicks_by_fsn(user, filters, fsns)

    merged = {}
    for asin, data in az_metrics.items():
        meta = az_meta.get(asin, {})
        paired_fk_meta = fk_meta_by_asin.get(asin, {})
        key = (meta.get("msku") or asin).strip() or asin
        merged[key] = {
            "amazon_sku": meta.get("msku") or "",
            "asin": asin,
            "flipkart_sku": paired_fk_meta.get("sku") or "",
            "fsn": paired_fk_meta.get("fsn") or "",
            "az_launch_date": meta.get("launch_date"),
            "fk_launch_date": paired_fk_meta.get("launch_date"),
            "az_pageviews": int(data.get("pageviews") or 0),
            "fk_pageviews": 0,
            "az_ad_spend": round(float(data.get("total_spend") or 0), 2),
            "fk_ad_spend": 0.0,
            "az_fc_stock": az_fba.get(asin, 0),
            "fk_fc_stock": 0,
            "az_flex_stock": az_flex.get(asin, 0),
            "fk_flex_stock": 0,
            "az_fc_stock_count": az_fc_count.get(asin, 0),
            "fk_fc_stock_count": 0,
            "az_revenue": round(float(data.get("revenue") or 0), 2),
            "fk_revenue": 0.0,
            "az_units": int(data.get("units") or 0),
            "fk_units": 0,
            "category": meta.get("category") or "",
            "portfolio": meta.get("portfolio") or "",
            "category_manager": meta.get("category_manager") or "",
            "az_doc": az_doc.get(asin, 0.0),
            "fk_doc": 0.0,
            "az_conversion": _pct(data.get("orders"), data.get("pageviews")),
            "fk_conversion": 0.0,
        }

    for fsn, data in fk_metrics.items():
        meta = fk_meta.get(fsn, {})
        mapped_asin = str(meta.get("asin") or "").strip()
        paired_az_meta = az_meta.get(mapped_asin, {}) if mapped_asin else {}
        key = (meta.get("sku") or fsn).strip() or fsn
        clicks = fk_clicks.get(fsn, {})
        if key not in merged:
            merged[key] = {
                "amazon_sku": paired_az_meta.get("msku") or "",
                "asin": mapped_asin,
                "flipkart_sku": meta.get("sku") or "",
                "fsn": fsn,
                "az_launch_date": paired_az_meta.get("launch_date"),
                "fk_launch_date": meta.get("launch_date"),
                "az_pageviews": 0,
                "fk_pageviews": int(data.get("pageviews") or 0),
                "az_ad_spend": 0.0,
                "fk_ad_spend": round(float(data.get("total_spend") or 0), 2),
                "az_fc_stock": 0,
                "fk_fc_stock": fk_fba.get(fsn, 0),
                "az_flex_stock": 0,
                "fk_flex_stock": fk_flex.get(fsn, 0),
                "az_fc_stock_count": 0,
                "fk_fc_stock_count": fk_fc_count.get(fsn, 0),
                "az_revenue": 0.0,
                "fk_revenue": round(float(data.get("revenue") or 0), 2),
                "az_units": 0,
                "fk_units": int(data.get("units") or 0),
                "category": meta.get("category") or "",
                "portfolio": meta.get("portfolio") or "",
                "category_manager": meta.get("category_manager") or "",
                "az_doc": 0.0,
                "fk_doc": fk_doc.get(fsn, 0.0),
                "az_conversion": 0.0,
                "fk_conversion": _pct(clicks.get("sales"), clicks.get("product_clicks")),
            }
        else:
            row = merged[key]
            row.update(
                {
                    "flipkart_sku": meta.get("sku") or "",
                    "fsn": fsn,
                    "fk_launch_date": meta.get("launch_date"),
                    "fk_pageviews": int(data.get("pageviews") or 0),
                    "fk_ad_spend": round(float(data.get("total_spend") or 0), 2),
                    "fk_fc_stock": fk_fba.get(fsn, 0),
                    "fk_flex_stock": fk_flex.get(fsn, 0),
                    "fk_fc_stock_count": fk_fc_count.get(fsn, 0),
                    "fk_revenue": round(float(data.get("revenue") or 0), 2),
                    "fk_units": int(data.get("units") or 0),
                    "fk_doc": fk_doc.get(fsn, 0.0),
                    "fk_conversion": _pct(clicks.get("sales"), clicks.get("product_clicks")),
                }
            )
            if not row.get("asin") and mapped_asin:
                row["asin"] = mapped_asin
            if not row.get("amazon_sku") and paired_az_meta.get("msku"):
                row["amazon_sku"] = paired_az_meta.get("msku") or ""
            if not row.get("az_launch_date") and paired_az_meta.get("launch_date"):
                row["az_launch_date"] = paired_az_meta.get("launch_date")
            if not row.get("category"):
                row["category"] = meta.get("category") or ""
            if not row.get("portfolio"):
                row["portfolio"] = meta.get("portfolio") or ""
            if not row.get("category_manager"):
                row["category_manager"] = meta.get("category_manager") or ""

    rows = []
    for row in merged.values():
        row["pageviews"] = row["az_pageviews"] + row["fk_pageviews"]
        row["ad_spend"] = round(row["az_ad_spend"] + row["fk_ad_spend"], 2)
        row["revenue"] = round(row["az_revenue"] + row["fk_revenue"], 2)
        row["units"] = row["az_units"] + row["fk_units"]
        row["doc"] = max(float(row["az_doc"] or 0), float(row["fk_doc"] or 0))
        row["conversion"] = _pct(
            (row["az_conversion"] * row["az_pageviews"] / 100.0)
            + (row["fk_conversion"] * row["fk_pageviews"] / 100.0),
            row["pageviews"],
        )
        launch_dates = [d for d in (row.get("az_launch_date"), row.get("fk_launch_date")) if d]
        row["launch_date"] = max(launch_dates) if launch_dates else None
        row["az_launch_date_display"] = _date_text(row.get("az_launch_date"))
        row["fk_launch_date_display"] = _date_text(row.get("fk_launch_date"))
        row["launch_date_display"] = _date_text(row.get("launch_date"))
        rows.append(row)

    rows.sort(key=lambda item: (item.get("launch_date") or datetime.date.min, item.get("revenue") or 0), reverse=True)
    if limit:
        rows = rows[:limit]

    trend = (
        build_npd_trend(
            user,
            filters,
            qs_f,
            fk_qs_f,
            set(az_metrics),
            set(fk_metrics),
            summary_az_qs=summary_az_qs,
            summary_fk_qs=summary_fk_qs,
        )
        if include_trend
        else {"labels": [], "pageviews": [], "units": [], "conversion": []}
    )
    return {"rows": rows, "trend": trend}


def build_npd_trend(user, filters, qs_f, fk_qs_f, asins, fsns, summary_az_qs=None, summary_fk_qs=None):
    by_date = {}
    from apps.dashboard.services.analytics_services_orm_pipeline import _get_product_daily_summary_querysets

    if summary_az_qs is None or summary_fk_qs is None:
        summary_az_qs, summary_fk_qs = _get_product_daily_summary_querysets(user, filters)

    if asins:
        for row in (
            summary_az_qs.filter(asin__in=asins)
            .values("date")
            .annotate(pageviews=Sum("page_views"), units=Sum("units_sold"), orders=Sum("orders"))
        ):
            key = _date_text(row["date"])
            item = by_date.setdefault(key, {"pageviews": 0, "units": 0, "orders": 0, "fk_sales": 0, "fk_clicks": 0})
            item["pageviews"] += int(row["pageviews"] or 0)
            item["units"] += int(row["units"] or 0)
            item["orders"] += int(row["orders"] or 0)

    if fsns:
        for row in (
            summary_fk_qs.filter(fsn__in=fsns)
            .values("date")
            .annotate(
                pageviews=Sum("page_views"),
                units=Sum("units_sold"),
                product_clicks=Sum("product_clicks"),
                sales=Sum("sales"),
            )
        ):
            key = _date_text(row["date"])
            item = by_date.setdefault(key, {"pageviews": 0, "units": 0, "orders": 0, "fk_sales": 0, "fk_clicks": 0})
            item["pageviews"] += int(row["pageviews"] or 0)
            item["units"] += int(row["units"] or 0)
            item["fk_sales"] += int(row["sales"] or 0)
            item["fk_clicks"] += int(row["product_clicks"] or 0)

    labels = sorted(by_date)
    conversion = []
    for label in labels:
        item = by_date[label]
        numerator = item["orders"] + item["fk_sales"]
        denominator = item["pageviews"] + item["fk_clicks"]
        conversion.append(_pct(numerator, denominator))

    return {
        "labels": labels,
        "pageviews": [by_date[label]["pageviews"] for label in labels],
        "units": [by_date[label]["units"] for label in labels],
        "conversion": conversion,
    }
