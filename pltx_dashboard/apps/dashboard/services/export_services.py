"""
Export service: builds a calculated DataFrame from the user's uploaded data
(ProcessedDashboardData + SpendData), computes core business metrics following
the logic in scripts/cleaning_mapping_merging.py, and returns it as CSV or
Excel (with Annexure sheet).
"""

import pandas as pd
import numpy as np
from io import BytesIO
from apps.dashboard.models import ProcessedDashboardData, SpendData
from apps.dashboard.services.analytics_services import apply_global_filters


def _build_export_dataframe(user, filters):
    """
    Query the DB for the user's data, apply dashboard filters, merge
    sales + spend, and compute all derived metrics.
    Returns a pandas DataFrame ready for export.
    """
    data_owner = user.created_by if user.created_by else user

    qs = ProcessedDashboardData.objects.filter(user=data_owner)

    # Apply entity-level filters at the DB level (same as dashboard views)
    category = filters.get('category')
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=category)
        else:
            qs = qs.filter(category=category)

    asin_filter = filters.get('asin')
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            qs = qs.filter(asin__in=asin_filter)
        else:
            qs = qs.filter(asin=asin_filter)

    portfolio = filters.get('portfolio')
    if portfolio:
        qs = qs.filter(portfolio=portfolio)

    subcategory = filters.get('subcategory')
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            qs = qs.filter(subcategory__in=subcategory)
        else:
            qs = qs.filter(subcategory=subcategory)

    if not qs.exists():
        return pd.DataFrame()

    df = pd.DataFrame(list(qs.values()))
    df = apply_global_filters(df, filters.copy())

    if df.empty:
        return pd.DataFrame()

    # ------------------------------------------------------------------
    # Aggregate at the ASIN level (mirrors cleaning_mapping_merging.py §5)
    # ------------------------------------------------------------------
    agg_cols = {
        'pageviews': 'sum',
        'units': 'sum',
        'orders': 'sum',
        'revenue': 'sum',
        'spend_sp': 'sum',
        'spend_sb': 'sum',
        'spend_sd': 'sum',
        'total_spend': 'sum',
    }
    # Keep first encounter of dimensional columns
    dim_cols = {}
    for col in ['portfolio', 'category', 'subcategory', 'price']:
        if col in df.columns:
            dim_cols[col] = 'first'

    merged = df.groupby('asin').agg({**agg_cols, **dim_cols}).reset_index()

    # Rename for readability
    merged.rename(columns={
        'pageviews': 'Page Views',
        'units': 'Units',
        'orders': 'Orders',
        'revenue': 'Revenue',
        'total_spend': 'Spend',
        'spend_sp': 'Spend (SP)',
        'spend_sb': 'Spend (SB)',
        'spend_sd': 'Spend (SD)',
        'asin': 'ASIN',
        'portfolio': 'Portfolio',
        'category': 'Category',
        'subcategory': 'Subcategory',
        'price': 'Price',
    }, inplace=True)

    # Fill NaN
    merged['Spend'] = merged['Spend'].fillna(0)
    merged['Revenue'] = merged['Revenue'].fillna(0)
    merged['Orders'] = merged['Orders'].fillna(0)
    merged['Page Views'] = merged['Page Views'].fillna(0)
    merged['Units'] = merged['Units'].fillna(0)
    merged['Price'] = merged['Price'].fillna(0)
    merged['Category'] = merged['Category'].fillna('Unknown')
    merged['Subcategory'] = merged['Subcategory'].fillna('Unknown')
    merged['Portfolio'] = merged['Portfolio'].fillna('Unknown')

    # ------------------------------------------------------------------
    # Compute core metrics (mirrors cleaning_mapping_merging.py §6)
    # ------------------------------------------------------------------
    # ROAS
    merged['ROAS'] = merged.apply(
        lambda r: (r['Revenue'] / r['Spend']) if r['Spend'] > 0 else 0, axis=1
    )
    # TACoS
    merged['TACoS (%)'] = merged.apply(
        lambda r: (r['Spend'] / r['Revenue'] * 100) if r['Revenue'] > 0 else 0, axis=1
    )
    # CVR
    merged['CVR (%)'] = merged.apply(
        lambda r: (r['Orders'] / r['Page Views'] * 100) if r['Page Views'] > 0 else 0, axis=1
    )
    # AOV
    merged['AOV'] = merged.apply(
        lambda r: (r['Revenue'] / r['Orders']) if r['Orders'] > 0 else 0, axis=1
    )
    # COGS
    merged['COGS (Total)'] = merged['Units'] * merged['Price']
    # Gross Margin
    merged['Gross Margin'] = merged['Revenue'] - merged['COGS (Total)']
    # Gross Margin %
    merged['Gross Margin (%)'] = merged.apply(
        lambda r: (r['Gross Margin'] / r['Revenue'] * 100) if r['Revenue'] > 0 else 0, axis=1
    )
    # Net Profit
    merged['Net Profit'] = merged['Gross Margin'] - merged['Spend']
    # Contribution Margin %
    merged['Contribution Margin (%)'] = merged['Gross Margin (%)'] - merged['TACoS (%)']

    # Round
    round_cols = [
        'ROAS', 'TACoS (%)', 'CVR (%)', 'AOV',
        'Gross Margin', 'Gross Margin (%)', 'Net Profit',
        'Contribution Margin (%)'
    ]
    merged[round_cols] = merged[round_cols].round(2)

    # Re-order columns for a clean export
    col_order = [
        'ASIN', 'Portfolio', 'Category', 'Subcategory',
        'Page Views', 'Units', 'Orders', 'Revenue', 'Price',
        'Spend', 'Spend (SP)', 'Spend (SB)', 'Spend (SD)',
        'ROAS', 'TACoS (%)', 'CVR (%)', 'AOV',
        'COGS (Total)', 'Gross Margin', 'Gross Margin (%)',
        'Net Profit', 'Contribution Margin (%)'
    ]
    # Only keep columns that exist
    col_order = [c for c in col_order if c in merged.columns]
    merged = merged[col_order]

    return merged


# Annexure data identical to the reference script
ANNEXURE_DATA = [
    {"Metric": "ROAS", "Formula": "Revenue / Spend",
     "Description": "Return on Ad Spend: For every ₹1 spent on ads, how much revenue was generated."},
    {"Metric": "TACoS (%)", "Formula": "(Spend / Revenue) * 100",
     "Description": "Total Advertising Cost of Sale: Percentage of total revenue spent on advertising."},
    {"Metric": "CVR (%)", "Formula": "(Orders / Page Views) * 100",
     "Description": "Conversion Rate: Percentage of people who viewed and purchased the product."},
    {"Metric": "AOV", "Formula": "Revenue / Orders",
     "Description": "Average Order Value: Average amount a customer spends per order."},
    {"Metric": "COGS (Total)", "Formula": "Units * Price",
     "Description": "Cost of Goods Sold: Estimated total cost based on pricing data."},
    {"Metric": "Gross Margin", "Formula": "Revenue - COGS (Total)",
     "Description": "Profit after product costs, before advertising."},
    {"Metric": "Gross Margin (%)", "Formula": "(Gross Margin / Revenue) * 100",
     "Description": "Profitability percentage before advertising."},
    {"Metric": "Net Profit", "Formula": "Gross Margin - Spend",
     "Description": "Money remaining after products and advertising costs."},
    {"Metric": "Contribution Margin (%)", "Formula": "Gross Margin (%) - TACoS (%)",
     "Description": "Health score: how much each sale contributes after ads."},
]


def export_csv(user, filters):
    """Return a BytesIO buffer containing the calculated CSV."""
    df = _build_export_dataframe(user, filters)
    buf = BytesIO()
    if df.empty:
        buf.write(b"No data available for the selected filters.\n")
    else:
        df.to_csv(buf, index=False)
    buf.seek(0)
    return buf


def export_excel(user, filters):
    """Return a BytesIO buffer containing the calculated Excel with Annexure."""
    df = _build_export_dataframe(user, filters)
    annexure_df = pd.DataFrame(ANNEXURE_DATA)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as writer:
        if df.empty:
            pd.DataFrame({"Message": ["No data available for the selected filters."]}).to_excel(
                writer, sheet_name='Data', index=False
            )
        else:
            df.to_excel(writer, sheet_name='Data', index=False)
        annexure_df.to_excel(writer, sheet_name='Annexure', index=False)
    buf.seek(0)
    return buf
