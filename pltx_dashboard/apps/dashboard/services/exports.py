import datetime
import json
from apps.dashboard.utils import DashboardEncoder

def _clean_export_value(value):
    if value is None:
        return ""
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    if isinstance(value, (dict, list, tuple, set)):
        return json.dumps(value, cls=DashboardEncoder)
    return str(value)



def _rows_to_export_table(rows):
    if not rows:
        return [], []
    if isinstance(rows[0], dict):
        keys = []
        for row in rows:
            for key in row.keys():
                if key not in keys:
                    keys.append(key)
        headers = [str(k).replace("_", " ").title() for k in keys]
        table_rows = [[_clean_export_value(row.get(k)) for k in keys] for row in rows]
        return headers, table_rows
    headers = ["Value"]
    table_rows = [[_clean_export_value(r)] for r in rows]
    return headers, table_rows



def _npd_export_table(rows, platform):
    platform = platform or "All"
    if platform == "Amazon":
        headers = [
            "Amazon SKU", "ASIN", "Page Views", "Launch Date", "Ad Spend",
            "FC Stock", "Flex Stock", "Count of FCs with Stock > 0",
            "Revenue", "Units Sold", "Category", "Portfolio", "CM Name",
            "DOC", "Conversion %",
        ]
        table_rows = [[
            r.get("amazon_sku", ""), r.get("asin", ""), r.get("az_pageviews", 0),
            r.get("az_launch_date_display", ""), r.get("az_ad_spend", 0),
            r.get("az_fc_stock", 0), r.get("az_flex_stock", 0),
            r.get("az_fc_stock_count", 0), r.get("az_revenue", 0),
            r.get("az_units", 0), r.get("category", ""), r.get("portfolio", ""),
            r.get("category_manager", ""), r.get("az_doc", 0),
            r.get("az_conversion", 0),
        ] for r in rows]
        return headers, table_rows

    if platform == "Flipkart":
        headers = [
            "Flipkart SKU", "FSN", "Page Views", "Launch Date", "Ad Spend",
            "FC Stock", "Flex Stock", "Count of FCs with Stock > 0",
            "Revenue", "Units Sold", "Category", "Portfolio", "CM Name",
            "DOC", "Conversion %",
        ]
        table_rows = [[
            r.get("flipkart_sku", ""), r.get("fsn", ""), r.get("fk_pageviews", 0),
            r.get("fk_launch_date_display", ""), r.get("fk_ad_spend", 0),
            r.get("fk_fc_stock", 0), r.get("fk_flex_stock", 0),
            r.get("fk_fc_stock_count", 0), r.get("fk_revenue", 0),
            r.get("fk_units", 0), r.get("category", ""), r.get("portfolio", ""),
            r.get("category_manager", ""), r.get("fk_doc", 0),
            r.get("fk_conversion", 0),
        ] for r in rows]
        return headers, table_rows

    headers = [
        "Amazon SKU", "ASIN", "Flipkart SKU", "FSN",
        "Amazon Page Views", "Flipkart Page Views",
        "Amazon Launch Date", "Flipkart Launch Date",
        "Amazon Ad Spend", "Flipkart Ad Spend",
        "Amazon FC Stock", "Flipkart FC Stock",
        "Amazon Flex Stock", "Flipkart Flex Stock",
        "Amazon FCs with Stock > 0", "Flipkart FCs with Stock > 0",
        "Amazon Revenue", "Flipkart Revenue",
        "Amazon Units Sold", "Flipkart Units Sold",
        "Category", "Portfolio", "CM Name",
        "Amazon DOC", "Flipkart DOC",
        "Amazon Conversion %", "Flipkart Conversion %",
    ]
    table_rows = [[
        r.get("amazon_sku", ""), r.get("asin", ""), r.get("flipkart_sku", ""),
        r.get("fsn", ""), r.get("az_pageviews", 0), r.get("fk_pageviews", 0),
        r.get("az_launch_date_display", ""), r.get("fk_launch_date_display", ""),
        r.get("az_ad_spend", 0), r.get("fk_ad_spend", 0),
        r.get("az_fc_stock", 0), r.get("fk_fc_stock", 0),
        r.get("az_flex_stock", 0), r.get("fk_flex_stock", 0),
        r.get("az_fc_stock_count", 0), r.get("fk_fc_stock_count", 0),
        r.get("az_revenue", 0), r.get("fk_revenue", 0),
        r.get("az_units", 0), r.get("fk_units", 0),
        r.get("category", ""), r.get("portfolio", ""),
        r.get("category_manager", ""), r.get("az_doc", 0), r.get("fk_doc", 0),
        r.get("az_conversion", 0), r.get("fk_conversion", 0),
    ] for r in rows]
    return headers, table_rows



def _modal_rows_export_filename(view_name, modal_key, ext):
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_view = str(view_name).strip().replace(" ", "_").lower()
    safe_modal = str(modal_key).strip().replace(" ", "_").replace("-", "_").lower()
    return f"{safe_view}_{safe_modal}_{stamp}.{ext}"



def _category_performance_export_filename(view_name, ext):
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_view = str(view_name).strip().replace(" ", "_").lower()
    return f"{safe_view}_category_performance_{stamp}.{ext}"



def _asin_fsn_report_export_filename(ext):
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"category_asin_fsn_wise_report_{stamp}.{ext}"



def _asin_fsn_report_export_table(rows):
    headers = [
        "ASIN / FSN",
        "SKU",
        "Platform Name",
        "Page Views",
        "Units Sold",
        "Revenue",
        "Ad Spend",
    ]
    table_rows = [
        [
            row.get("product_id", ""),
            row.get("sku", ""),
            row.get("platform", ""),
            int(row.get("pageviews") or 0),
            int(row.get("units") or 0),
            _export_number(row.get("revenue")),
            _export_number(row.get("ad_spend")),
        ]
        for row in rows
    ]
    return headers, table_rows



def _export_number(value):
    """Robustly convert a value (including strings like '1234.56') to a float."""
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return round(float(value), 2)
    # Handle string values that may come from cached/serialized data
    try:
        cleaned = str(value).replace(",", "").strip()
        return round(float(cleaned), 2) if cleaned else 0.0
    except (TypeError, ValueError):
        return 0.0



def _fmt_rupee_export(value):
    """Format a numeric revenue value as a readable rupee string for exports."""
    num = _export_number(value)
    if num >= 10_000_000:
        return f"\u20b9{num / 10_000_000:.2f}Cr"
    if num >= 100_000:
        return f"\u20b9{num / 100_000:.2f}L"
    if num >= 1_000:
        return f"\u20b9{num / 1_000:.2f}K"
    return f"\u20b9{num:.2f}"



def _category_performance_export_table(rows):
    headers = [
        "Category",
        "Amazon Revenue",
        "Flipkart Revenue",
        "Total Revenue",
        "MoM Growth",
        "Contribution",
    ]
    table_rows = []
    for row in rows:
        current_revenue = _export_number(row.get("mom_current_revenue", row.get("revenue")))
        previous_revenue = _export_number(row.get("mom_previous_revenue"))
        growth = _export_number(row.get("mom_growth", row.get("growth")))
        contribution = _export_number(row.get("contribution"))
        direction = "+" if growth >= 0 else ""
        mom_cell = (
            f"{direction}{growth}% "
            f"({_fmt_rupee_export(current_revenue)} vs {_fmt_rupee_export(previous_revenue)})"
        )
        table_rows.append(
            [
                _clean_export_value(row.get("category") or "Unknown"),
                _export_number(row.get("amazon_revenue")),
                _export_number(row.get("flipkart_revenue")),
                _export_number(row.get("total_revenue", row.get("revenue"))),
                mom_cell,
                f"{contribution}%",
            ]
        )
    return headers, table_rows



