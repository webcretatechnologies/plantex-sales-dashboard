from django.db import transaction
from django.db.models import Sum

from apps.dashboard.models import (
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
    SalesData,
)


def _safe_doc(stock_qty, sale_qty):
    if sale_qty > 0:
        return round(stock_qty / float(sale_qty), 1)
    return 999.0 if stock_qty > 0 else 0.0


def _build_amazon_rows(user, only_dates=None):
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}
    sales_qs = SalesData.objects.filter(user=user)
    fba_qs = FBAStockData.objects.filter(user=user)
    flex_qs = FlexStockData.objects.filter(user=user)
    rev_qs = ProcessedDashboardData.objects.filter(user=user)
    meta_qs = CategoryMapping.objects.filter(user=user)

    if only_dates:
        sales_qs = sales_qs.filter(date__in=only_dates)
        fba_qs = fba_qs.filter(date__in=only_dates)
        flex_qs = flex_qs.filter(date__in=only_dates)
        rev_qs = rev_qs.filter(date__in=only_dates)

    sales_by_key = {
        (str(r["asin"]), r["date"]): int(r["u"] or 0)
        for r in sales_qs.values("asin", "date").annotate(u=Sum("units"))
    }
    fba_by_key = {
        (str(r["asin"]), r["date"]): int(r["q"] or 0)
        for r in fba_qs.values("asin", "date").annotate(q=Sum("ending_warehouse_balance"))
    }
    flex_by_key = {
        (str(r["asin"]), r["date"]): int(r["q"] or 0)
        for r in flex_qs.values("asin", "date").annotate(q=Sum("qty"))
    }
    rev_by_key = {
        (str(r["asin"]), r["date"]): float(r["r"] or 0)
        for r in rev_qs.values("asin", "date").annotate(r=Sum("revenue"))
    }
    meta_by_sku = {
        str(r["asin"]): (
            str(r.get("category") or "Unknown"),
            str(r.get("portfolio") or ""),
            str(r.get("subcategory") or ""),
        )
        for r in meta_qs.values("asin", "category", "portfolio", "subcategory")
    }

    stock_keys = set(fba_by_key.keys()) | set(flex_by_key.keys())
    sales_keys = set(sales_by_key.keys())
    stock_dates = {d for _sku, d in stock_keys if d}
    sales_dates = {d for _sku, d in sales_keys if d}
    aligned_dates = stock_dates & sales_dates
    keys = ({k for k in stock_keys if k[1] in aligned_dates} | {k for k in sales_keys if k[1] in aligned_dates}) if aligned_dates else set()

    rows = []
    for sku, row_date in keys:
        sale_qty = int(sales_by_key.get((sku, row_date), 0))
        fba_qty = int(fba_by_key.get((sku, row_date), 0))
        flex_qty = int(flex_by_key.get((sku, row_date), 0))
        stock_qty = fba_qty + flex_qty
        doc = _safe_doc(stock_qty, sale_qty)
        category, portfolio, subcategory = meta_by_sku.get(sku, ("Unknown", "", ""))
        rev = float(rev_by_key.get((sku, row_date), 0.0) or 0.0)

        if stock_qty <= 0:
            status = "OOS"
            status_class = "danger"
            reason = f"Stock Qty = 0 (FBA: {fba_qty}, Flex: {flex_qty})"
        elif sale_qty <= 0:
            status = "Overstock"
            status_class = "neutral"
            reason = f"DOC = ∞ (Stock: {stock_qty}, No sales)"
        elif doc <= 15:
            status = "Low Stock"
            status_class = "warn"
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {sale_qty:.1f})"
        elif doc > 60:
            status = "Overstock"
            status_class = "neutral"
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {sale_qty:.1f})"
        else:
            status = "In Stock"
            status_class = "good"
            reason = f"DOC = {doc} days (Stock: {stock_qty} / Same-Day Sales: {sale_qty:.1f})"

        rows.append(
            DashboardInventoryHealthSummary(
                user=user,
                date=row_date,
                platform="Amazon",
                sku=sku,
                category=category,
                portfolio=portfolio,
                subcategory=subcategory,
                stock_qty=stock_qty,
                fba_qty=fba_qty,
                flex_qty=flex_qty,
                sale_qty=sale_qty,
                total_sales_window=sale_qty,
                drr=float(sale_qty),
                doc=float(doc),
                revenue=round(rev, 2),
                status=status,
                status_class=status_class,
                reason=reason,
            )
        )
    return rows


def _build_flipkart_rows(user, only_dates=None):
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}
    traffic_qs = FlipkartSearchTraffic.objects.filter(user=user)
    inv_qs = FlipkartInventoryStock.objects.filter(user=user)
    fba_qs = Flipkartfba.objects.filter(user=user)
    rev_qs = FlipkartProcessedDashboardData.objects.filter(user=user)
    map_qs = FlipkartCategoryMap.objects.filter(user=user)

    if only_dates:
        traffic_qs = traffic_qs.filter(date__in=only_dates)
        inv_qs = inv_qs.filter(date__in=only_dates)
        fba_qs = fba_qs.filter(date__in=only_dates)
        rev_qs = rev_qs.filter(date__in=only_dates)

    sales_by_key = {
        (str(r["fsn"]), r["date"]): int(r["s"] or 0)
        for r in traffic_qs.values("fsn", "date").annotate(s=Sum("sales"))
    }
    sales_total_by_sku = {
        str(r["fsn"]): int(r["s"] or 0)
        for r in traffic_qs.values("fsn").annotate(s=Sum("sales"))
    }
    inv_by_key = {
        (str(r["fsn"]), r["date"]): int(r["q"] or 0)
        for r in inv_qs.values("fsn", "date").annotate(q=Sum("qty"))
    }
    fba_by_key = {
        (str(r["fsn"]), r["date"]): int(r["q"] or 0)
        for r in fba_qs.values("fsn", "date").annotate(q=Sum("live_on_website"))
    }
    rev_by_key = {
        (str(r["fsn"]), r["date"]): float(r["r"] or 0)
        for r in rev_qs.values("fsn", "date").annotate(r=Sum("revenue"))
    }
    meta_by_sku = {
        str(r["fsn"]): (
            str(r.get("category") or "Unknown"),
            str(r.get("portfolio") or ""),
            str(r.get("subcategory") or ""),
        )
        for r in map_qs.values("fsn", "category", "portfolio", "subcategory")
    }

    stock_keys = set(inv_by_key.keys()) | set(fba_by_key.keys())
    sales_keys = set(sales_by_key.keys())
    stock_dates = {d for _sku, d in stock_keys if d}
    sales_dates = {d for _sku, d in sales_keys if d}
    aligned_dates = stock_dates & sales_dates
    keys = ({k for k in stock_keys if k[1] in aligned_dates} | {k for k in sales_keys if k[1] in aligned_dates}) if aligned_dates else set()

    rows = []
    for sku, row_date in keys:
        fba_qty = int(fba_by_key.get((sku, row_date), 0))
        flex_qty = int(inv_by_key.get((sku, row_date), 0))
        stock_qty = fba_qty + flex_qty
        same_day_sales = int(sales_by_key.get((sku, row_date), 0))
        total_sales = int(sales_total_by_sku.get(sku, 0))
        drr = float(total_sales) / 30.0
        if drr > 0:
            doc = round(stock_qty / drr, 1)
        else:
            doc = 999.0 if stock_qty > 0 else 0.0
        rev = float(rev_by_key.get((sku, row_date), 0.0) or 0.0)
        category, portfolio, subcategory = meta_by_sku.get(sku, ("Unknown", "", ""))

        if stock_qty <= 0:
            status = "OOS"
            status_class = "danger"
            reason = f"Stock Qty = 0 (Live on Website: {fba_qty}, FK Qty: {flex_qty})"
        elif doc < 5:
            status = "Nearly OOS"
            status_class = "danger"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"
        elif doc < 15:
            status = "Understock"
            status_class = "warn"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"
        elif doc <= 30:
            status = "Ideal Stocking"
            status_class = "good"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"
        elif doc <= 90:
            status = "Over Stock"
            status_class = "neutral"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"
        elif doc <= 180:
            status = "Highly Over Stock"
            status_class = "neutral"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"
        else:
            status = "Not Selling"
            status_class = "neutral"
            reason = f"DOC = {doc} days (Stock: {stock_qty}, DRR: {drr:.2f})"

        rows.append(
            DashboardInventoryHealthSummary(
                user=user,
                date=row_date,
                platform="Flipkart",
                sku=sku,
                category=category,
                portfolio=portfolio,
                subcategory=subcategory,
                stock_qty=stock_qty,
                fba_qty=fba_qty,
                flex_qty=flex_qty,
                sale_qty=same_day_sales,
                total_sales_window=total_sales,
                drr=round(drr, 2),
                doc=float(doc),
                revenue=round(rev, 2),
                status=status,
                status_class=status_class,
                reason=reason,
            )
        )
    return rows


def rebuild_inventory_summary_for_user(user, *, only_dates=None):
    """
    Rebuild dashboard inventory-health summary rows.
    """
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}
    with transaction.atomic():
        scoped = DashboardInventoryHealthSummary.objects.filter(user=user)
        if only_dates:
            scoped = scoped.filter(date__in=only_dates)
        scoped.delete()

        inserts = []
        inserts.extend(_build_amazon_rows(user, only_dates=only_dates))
        inserts.extend(_build_flipkart_rows(user, only_dates=only_dates))
        if inserts:
            DashboardInventoryHealthSummary.objects.bulk_create(inserts, batch_size=2000)

    return {"rows_written": len(inserts), "dates_scoped": sorted(only_dates)}
