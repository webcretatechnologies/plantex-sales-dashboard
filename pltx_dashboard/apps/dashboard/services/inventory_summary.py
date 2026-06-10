from django.db import transaction
from django.db.models import Sum

from apps.dashboard.models import (
    AsinFsnMapping,
    CategoryMapping,
    DashboardInventoryHealthSummary,
    FBAStockData,
    FlexStockData,
    FlipkartCategoryMap,
    FlipkartInventoryStock,
    FlipkartProcessedDashboardData,
    FlipkartSearchTraffic,
    Flipkartfba,
    ProcessedDashboardData,
)


def _safe_doc(stock_qty, sale_qty):
    if sale_qty > 0:
        return round(stock_qty / float(sale_qty), 1)
    return 999.0 if stock_qty > 0 else 0.0


def _build_combined_inventory_rows(user, only_dates=None):
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}
    date_kw = {"date__in": only_dates} if only_dates else {}

    # 1. Load mappings
    mappings = list(AsinFsnMapping.objects.filter(user=user).values("asin", "fsn", "category", "portfolio", "subcategory"))
    amz_to_fk = {m["asin"]: m["fsn"] for m in mappings if m["asin"]}
    fk_to_amz = {m["fsn"]: m["asin"] for m in mappings if m["fsn"]}
    meta_by_asin = {m["asin"]: (m["category"], m["portfolio"], m["subcategory"]) for m in mappings if m["asin"]}
    meta_by_fsn = {m["fsn"]: (m["category"], m["portfolio"], m["subcategory"]) for m in mappings if m["fsn"]}

    # 2. Amazon data
    amz_fba_qs = FBAStockData.objects.filter(user=user, **date_kw).values("asin", "date").annotate(qty=Sum("ending_warehouse_balance"))
    amz_fba = {(r["asin"], r["date"]): int(r["qty"] or 0) for r in amz_fba_qs.iterator(chunk_size=5000)}

    amz_flex_qs = FlexStockData.objects.filter(user=user, **date_kw).values("asin", "date").annotate(qty=Sum("qty"))
    amz_flex = {(r["asin"], r["date"]): int(r["qty"] or 0) for r in amz_flex_qs.iterator(chunk_size=5000)}

    amz_sales_qs = ProcessedDashboardData.objects.filter(user=user, **date_kw).values("asin", "date").annotate(units=Sum("units"), revenue=Sum("revenue"))
    amz_sales = {(r["asin"], r["date"]): {"units": int(r["units"] or 0), "revenue": float(r["revenue"] or 0)} for r in amz_sales_qs.iterator(chunk_size=5000)}

    amz_cm = {r["asin"]: (r["category"], r["portfolio"], r["subcategory"]) for r in CategoryMapping.objects.filter(user=user).values("asin", "category", "portfolio", "subcategory")}

    # 3. Flipkart data
    fk_fba_qs = Flipkartfba.objects.filter(user=user, **date_kw).values("fsn", "date").annotate(qty=Sum("live_on_website"))
    fk_fba = {(r["fsn"], r["date"]): int(r["qty"] or 0) for r in fk_fba_qs.iterator(chunk_size=5000)}

    fk_inv_qs = FlipkartInventoryStock.objects.filter(user=user, **date_kw).values("fsn", "date").annotate(qty=Sum("qty"))
    fk_inv = {(r["fsn"], r["date"]): int(r["qty"] or 0) for r in fk_inv_qs.iterator(chunk_size=5000)}

    fk_sales_qs = FlipkartSearchTraffic.objects.filter(user=user, **date_kw).values("fsn", "date").annotate(s=Sum("sales"))
    fk_sales = {(r["fsn"], r["date"]): int(r["s"] or 0) for r in fk_sales_qs.iterator(chunk_size=5000)}

    fk_rev_qs = FlipkartProcessedDashboardData.objects.filter(user=user, **date_kw).values("fsn", "date").annotate(r=Sum("revenue"))
    fk_rev = {(r["fsn"], r["date"]): float(r["r"] or 0) for r in fk_rev_qs.iterator(chunk_size=5000)}

    fk_cm = {r["fsn"]: (r["category"], r["portfolio"], r["subcategory"]) for r in FlipkartCategoryMap.objects.filter(user=user).values("fsn", "category", "portfolio", "subcategory")}

    # ── CRITICAL: Only build rows for dates where stock data was ACTUALLY uploaded ──
    # Amazon: only dates present in FBA or Flex stock uploads
    amz_stock_dates = {d for (_, d) in amz_fba.keys()} | {d for (_, d) in amz_flex.keys()}
    # Flipkart: only dates present in FK FBA or FK Inventory stock uploads
    fk_stock_dates = {d for (_, d) in fk_fba.keys()} | {d for (_, d) in fk_inv.keys()}

    # Build keyed sets: Amazon rows = ASINs with stock data for valid Amazon dates
    amz_keys = set()
    for (asin, d) in list(amz_fba.keys()) + list(amz_flex.keys()):
        amz_keys.add((asin, d))
    # Also add ASINs that appear in sales for valid Amazon stock dates (to get DOC right)
    for (asin, d) in amz_sales.keys():
        if d in amz_stock_dates:
            amz_keys.add((asin, d))

    # Flipkart rows = FSNs with stock data for valid FK dates
    fk_keys = set()
    for (fsn, d) in list(fk_fba.keys()) + list(fk_inv.keys()):
        fk_keys.add((fsn, d))
    # Also add FSNs that appear in FK sales for valid FK stock dates
    for (fsn, d) in list(fk_sales.keys()) + list(fk_rev.keys()):
        if d in fk_stock_dates:
            fk_keys.add((fsn, d))

    # For each Amazon ASIN, if a mapped FSN exists AND FK stock data exists for that date → link them
    # Otherwise keep them separate (don't create phantom FK OOS rows)
    all_keys = {}  # key → {"asin": .., "fsn": .., "date": ..}

    for (asin, d) in amz_keys:
        fsn = amz_to_fk.get(asin, "")
        k = (asin or fsn, d)
        if k not in all_keys:
            all_keys[k] = {"asin": asin, "fsn": fsn if (fsn, d) in fk_keys else "", "date": d}
        else:
            # Merge — FSN only linked if FK stock exists for this date
            if fsn and (fsn, d) in fk_keys:
                all_keys[k]["fsn"] = fsn

    for (fsn, d) in fk_keys:
        asin = fk_to_amz.get(fsn, "")
        k = (asin if asin else fsn, d)
        if k not in all_keys:
            all_keys[k] = {"asin": asin if asin and (asin, d) in amz_keys else "", "fsn": fsn, "date": d}

    rows = []

    def _status(stock, sale):
        doc = _safe_doc(stock, sale)
        if stock <= 0:
            return "OOS", "danger"
        elif sale <= 0:
            return "Overstock", "neutral"
        elif doc <= 15:
            return "Low Stock", "warn"
        elif doc > 60:
            return "Overstock", "neutral"
        else:
            return "In Stock", "good"

    def _fk_status(stock, sale):
        doc = _safe_doc(stock, sale)
        if stock <= 0:
            return "OOS", "danger"
        elif doc < 5:
            return "Nearly OOS", "danger"
        elif doc < 15:
            return "Understock", "warn"
        elif doc <= 30:
            return "Ideal Stocking", "good"
        elif doc <= 90:
            return "Over Stock", "neutral"
        elif doc <= 180:
            return "Highly Over Stock", "neutral"
        else:
            return "Not Selling", "neutral"

    for _key, item in all_keys.items():
        asin = item["asin"]
        fsn = item["fsn"]
        d = item["date"]

        # Amazon metrics — only if ASIN is present and stock data uploaded for this date
        if asin:
            fba_qty = amz_fba.get((asin, d), 0)
            flex_qty = amz_flex.get((asin, d), 0)
            amz_stock = fba_qty + flex_qty
            amz_s_dict = amz_sales.get((asin, d), {"units": 0, "revenue": 0.0})
            amz_sale_qty = amz_s_dict["units"]
            amz_revenue = amz_s_dict["revenue"]
            amz_doc = _safe_doc(amz_stock, amz_sale_qty)
            amz_stat, amz_stat_cls = _status(amz_stock, amz_sale_qty)
            if amz_stock <= 0:
                amz_reason = f"Stock Qty = 0 (FBA: {fba_qty}, Flex: {flex_qty})"
            elif amz_sale_qty <= 0:
                amz_reason = f"DOC = ∞ (Stock: {amz_stock}, No sales)"
            else:
                amz_reason = f"DOC = {amz_doc} days (Stock: {amz_stock} / Same-Day Sales: {amz_sale_qty})"
        else:
            fba_qty = flex_qty = amz_stock = amz_sale_qty = 0
            amz_revenue = amz_doc = 0.0
            amz_stat = amz_stat_cls = amz_reason = ""

        # Flipkart metrics — only if FSN is present and FK stock data uploaded for this date
        if fsn:
            fk_fba_q = fk_fba.get((fsn, d), 0)
            fk_inv_q = fk_inv.get((fsn, d), 0)
            fk_stock = fk_fba_q + fk_inv_q
            fk_sale = fk_sales.get((fsn, d), 0)
            fk_rev_val = fk_rev.get((fsn, d), 0.0)
            fk_doc = _safe_doc(fk_stock, fk_sale)
            fk_stat, fk_stat_cls = _fk_status(fk_stock, fk_sale)
        else:
            fk_fba_q = fk_inv_q = fk_stock = fk_sale = 0
            fk_rev_val = fk_doc = 0.0
            fk_stat = fk_stat_cls = ""

        # Skip rows with no data at all (shouldn't happen but guard)
        if not asin and not fsn:
            continue

        # Meta
        if asin and asin in meta_by_asin:
            cat, port, sub = meta_by_asin[asin]
        elif fsn and fsn in meta_by_fsn:
            cat, port, sub = meta_by_fsn[fsn]
        elif asin and asin in amz_cm:
            cat, port, sub = amz_cm[asin]
        elif fsn and fsn in fk_cm:
            cat, port, sub = fk_cm[fsn]
        else:
            cat, port, sub = "Unknown", "", ""

        rows.append(DashboardInventoryHealthSummary(
            user=user,
            date=d,
            platform="Combined",
            sku=asin if asin else fsn,
            asin=asin,
            fsn=fsn,
            category=cat,
            portfolio=port,
            subcategory=sub,

            stock_qty=amz_stock,
            fba_qty=fba_qty,
            flex_qty=flex_qty,
            sale_qty=amz_sale_qty,
            total_sales_window=amz_sale_qty,
            drr=amz_sale_qty,
            doc=amz_doc,
            revenue=amz_revenue,
            status=amz_stat,
            status_class=amz_stat_cls,
            reason=amz_reason,

            fk_stock_qty=fk_stock,
            fk_fba_qty=fk_fba_q,
            fk_flex_qty=fk_inv_q,
            fk_sale_qty=fk_sale,
            fk_doc=fk_doc,
            fk_revenue=fk_rev_val,
            fk_status=fk_stat,
            fk_status_class=fk_stat_cls,
        ))

    return rows


def rebuild_inventory_summary_for_user(user, *, only_dates=None):
    """
    Rebuild dashboard inventory-health summary rows using the combined ASIN-FSN logic.
    Only creates rows for dates where actual stock files were uploaded — never generates
    phantom OOS rows from sales-only data.
    """
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}
    with transaction.atomic():
        scoped = DashboardInventoryHealthSummary.objects.filter(user=user)
        if only_dates:
            scoped = scoped.filter(date__in=only_dates)
        scoped.delete()

        inserts = _build_combined_inventory_rows(user, only_dates=only_dates)
        if inserts:
            DashboardInventoryHealthSummary.objects.bulk_create(inserts, batch_size=2000)

    return {"rows_written": len(inserts), "dates_scoped": sorted(only_dates)}
