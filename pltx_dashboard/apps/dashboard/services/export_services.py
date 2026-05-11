"""
Export service: builds calculated export data from processed dashboard tables
and returns CSV / Excel (with Annexure sheet).
"""

from io import BytesIO

import pandas as pd

from apps.dashboard.models import FlipkartProcessedDashboardData, ProcessedDashboardData
from apps.dashboard.services.analytics_services_orm_pipeline import apply_global_filters_orm


def _get_filtered_querysets(user, filters):
    """Return filtered Amazon + Flipkart querysets for the current user."""
    data_owner = user.created_by if user.created_by else user

    qs = ProcessedDashboardData.objects.filter(user=data_owner)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=data_owner)

    qs = apply_global_filters_orm(qs, filters)
    fk_qs = apply_global_filters_orm(fk_qs, filters)

    platform = filters.get("platform")
    if platform == "Amazon":
        fk_qs = fk_qs.none()
    elif platform == "Flipkart":
        qs = qs.none()

    category = filters.get("category")
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=category)
            fk_qs = fk_qs.filter(category__in=category)
        else:
            qs = qs.filter(category=category)
            fk_qs = fk_qs.filter(category=category)

    asin_or_fsn_filter = filters.get("asin") or filters.get("fsn")
    if asin_or_fsn_filter:
        if isinstance(asin_or_fsn_filter, (list, tuple)):
            qs = qs.filter(asin__in=asin_or_fsn_filter)
            fk_qs = fk_qs.filter(fsn__in=asin_or_fsn_filter)
        else:
            qs = qs.filter(asin=asin_or_fsn_filter)
            fk_qs = fk_qs.filter(fsn=asin_or_fsn_filter)

    portfolio = filters.get("portfolio")
    if portfolio:
        qs = qs.filter(portfolio=portfolio)
        fk_qs = fk_qs.filter(portfolio=portfolio)

    subcategory = filters.get("subcategory")
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            qs = qs.filter(subcategory__in=subcategory)
            fk_qs = fk_qs.filter(subcategory__in=subcategory)
        else:
            qs = qs.filter(subcategory=subcategory)
            fk_qs = fk_qs.filter(subcategory=subcategory)

    return qs, fk_qs


def _build_amazon_export_dataframe(qs):
    if not qs.exists():
        return pd.DataFrame()

    df = pd.DataFrame(list(qs.values()))
    df["platform"] = "Amazon"

    agg_cols = {
        "pageviews": "sum",
        "units": "sum",
        "orders": "sum",
        "revenue": "sum",
        "spend_sp": "sum",
        "spend_sb": "sum",
        "spend_sd": "sum",
        "total_spend": "sum",
    }
    dim_cols = {}
    for col in ["portfolio", "category", "subcategory", "price", "platform"]:
        if col in df.columns:
            dim_cols[col] = "first"

    merged = df.groupby("asin").agg({**agg_cols, **dim_cols}).reset_index()

    merged.rename(
        columns={
            "pageviews": "Page Views",
            "units": "Units",
            "orders": "Orders",
            "revenue": "Revenue",
            "total_spend": "Spend",
            "spend_sp": "Spend (SP)",
            "spend_sb": "Spend (SB)",
            "spend_sd": "Spend (SD)",
            "asin": "ASIN",
            "portfolio": "Portfolio",
            "category": "Category",
            "subcategory": "Subcategory",
            "price": "Price",
            "platform": "Platform",
        },
        inplace=True,
    )

    merged["Spend"] = merged["Spend"].fillna(0)
    merged["Revenue"] = merged["Revenue"].fillna(0)
    merged["Orders"] = merged["Orders"].fillna(0)
    merged["Page Views"] = merged["Page Views"].fillna(0)
    merged["Units"] = merged["Units"].fillna(0)
    merged["Price"] = merged["Price"].fillna(0)
    merged["Category"] = merged["Category"].fillna("Unknown")
    merged["Subcategory"] = merged["Subcategory"].fillna("Unknown")
    merged["Portfolio"] = merged["Portfolio"].fillna("Unknown")

    merged["ROAS"] = merged.apply(
        lambda r: (r["Revenue"] * 0.7 / r["Spend"]) if r["Spend"] > 0 else 0, axis=1
    )
    merged["TACoS (%)"] = merged.apply(
        lambda r: (r["Spend"] / (r["Revenue"] * 0.7) * 100) if r["Revenue"] > 0 else 0,
        axis=1,
    )
    merged["CVR (%)"] = merged.apply(
        lambda r: (r["Orders"] / r["Page Views"] * 100) if r["Page Views"] > 0 else 0,
        axis=1,
    )

    round_cols = ["ROAS", "TACoS (%)", "CVR (%)"]
    merged[round_cols] = merged[round_cols].round(2)

    col_order = [
        "Platform",
        "ASIN",
        "Portfolio",
        "Category",
        "Subcategory",
        "Page Views",
        "Units",
        "Orders",
        "Revenue",
        "Price",
        "Spend",
        "Spend (SP)",
        "Spend (SB)",
        "Spend (SD)",
        "ROAS",
        "TACoS (%)",
        "CVR (%)",
    ]
    return merged[[c for c in col_order if c in merged.columns]]


def _build_flipkart_export_dataframe(fk_qs):
    if not fk_qs.exists():
        return pd.DataFrame()

    df = pd.DataFrame(list(fk_qs.values()))
    df["platform"] = "Flipkart"

    agg_cols = {
        "pageviews": "sum",
        "units": "sum",
        "revenue": "sum",
        "total_spend": "sum",
    }
    dim_cols = {}
    for col in ["portfolio", "category", "subcategory", "price", "platform"]:
        if col in df.columns:
            dim_cols[col] = "first"

    merged = df.groupby("fsn").agg({**agg_cols, **dim_cols}).reset_index()

    merged.rename(
        columns={
            "platform": "Platform",
            "fsn": "FSN",
            "portfolio": "Portfolio",
            "category": "Category",
            "subcategory": "Subcategory",
            "pageviews": "Page Views",
            "units": "Units Sold",
            "revenue": "Revenue",
            "price": "Price",
            "total_spend": "Ad Spend",
        },
        inplace=True,
    )

    for col in [
        "Page Views",
        "Units Sold",
        "Revenue",
        "Price",
        "Ad Spend",
    ]:
        if col in merged.columns:
            merged[col] = merged[col].fillna(0)

    merged["Category"] = merged["Category"].fillna("Unknown")
    merged["Subcategory"] = merged["Subcategory"].fillna("Unknown")
    merged["Portfolio"] = merged["Portfolio"].fillna("Unknown")

    merged["ROAS"] = merged.apply(
        lambda r: (r["Revenue"] * 0.7 / r["Ad Spend"]) if r["Ad Spend"] > 0 else 0,
        axis=1,
    )
    merged["TACoS (%)"] = merged.apply(
        lambda r: (r["Ad Spend"] / (r["Revenue"] * 0.7) * 100) if r["Revenue"] > 0 else 0,
        axis=1,
    )
    merged["CVR"] = merged.apply(
        lambda r: (r["Page Views"] / r["Units Sold"]) if r["Units Sold"] > 0 else 0,
        axis=1,
    )

    round_cols = ["ROAS", "TACoS (%)", "CVR"]
    merged[round_cols] = merged[round_cols].round(2)

    col_order = [
        "Platform",
        "FSN",
        "Portfolio",
        "Category",
        "Subcategory",
        "Page Views",
        "Units Sold",
        "Revenue",
        "Price",
        "Ad Spend",
        "ROAS",
        "TACoS (%)",
        "CVR",
    ]
    return merged[[c for c in col_order if c in merged.columns]]


AMAZON_ANNEXURE_DATA = [
    {
        "Metric": "ROAS",
        "Formula": "(Revenue × 0.7) / Spend",
        "Description": "Return on Ad Spend based on GST-adjusted revenue.",
    },
    {
        "Metric": "TACoS (%)",
        "Formula": "(Spend / (Revenue × 0.7)) * 100",
        "Description": "Total Advertising Cost of Sale as percentage of revenue.",
    },
    {
        "Metric": "CVR (%)",
        "Formula": "(Orders / Page Views) * 100",
        "Description": "Conversion Rate from page views to orders.",
    },
]


FLIPKART_ANNEXURE_DATA = [
    {
        "Metric": "ROAS",
        "Formula": "(Revenue × 0.7) / Ad Spend",
        "Description": "Return on Ad Spend based on GST-adjusted revenue.",
    },
    {
        "Metric": "TACoS (%)",
        "Formula": "(Ad Spend / (Revenue × 0.7)) * 100",
        "Description": "Total ad spend as percentage of GST-adjusted revenue.",
    },
    {
        "Metric": "CVR",
        "Formula": "Page Views / Units Sold",
        "Description": "Flipkart conversion metric based on page views per unit sold.",
    },
]


def _build_export_payload(user, filters):
    """
    Build export dataframe + annexure based on selected platform.
    For platform="All", combines both datasets.
    """
    qs, fk_qs = _get_filtered_querysets(user, filters)
    platform = filters.get("platform")

    if platform == "Amazon":
        return _build_amazon_export_dataframe(qs), pd.DataFrame(AMAZON_ANNEXURE_DATA)

    if platform == "Flipkart":
        return _build_flipkart_export_dataframe(fk_qs), pd.DataFrame(FLIPKART_ANNEXURE_DATA)

    df_amz = _build_amazon_export_dataframe(qs)
    df_fk = _build_flipkart_export_dataframe(fk_qs)

    if df_amz.empty and df_fk.empty:
        return pd.DataFrame(), pd.DataFrame(AMAZON_ANNEXURE_DATA + FLIPKART_ANNEXURE_DATA)
    if df_amz.empty:
        return df_fk, pd.DataFrame(FLIPKART_ANNEXURE_DATA)
    if df_fk.empty:
        return df_amz, pd.DataFrame(AMAZON_ANNEXURE_DATA)

    return (
        pd.concat([df_amz, df_fk], ignore_index=True, sort=False).fillna(""),
        pd.DataFrame(AMAZON_ANNEXURE_DATA + FLIPKART_ANNEXURE_DATA),
    )


def export_csv(user, filters):
    """Return a BytesIO buffer containing the calculated CSV."""
    df, _ = _build_export_payload(user, filters)
    buf = BytesIO()
    if df.empty:
        buf.write(b"No data available for the selected filters.\n")
    else:
        df.to_csv(buf, index=False)
    buf.seek(0)
    return buf


def export_excel(user, filters):
    """Return a BytesIO buffer containing the calculated Excel with Annexure."""
    df, annexure_df = _build_export_payload(user, filters)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        if df.empty:
            pd.DataFrame({"Message": ["No data available for the selected filters."]}).to_excel(
                writer, sheet_name="Data", index=False
            )
        else:
            df.to_excel(writer, sheet_name="Data", index=False)
        annexure_df.to_excel(writer, sheet_name="Annexure", index=False)
    buf.seek(0)
    return buf
