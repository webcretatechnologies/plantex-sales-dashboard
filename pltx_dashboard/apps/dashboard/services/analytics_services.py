import pandas as pd
import numpy as np
from apps.dashboard.services.report_services import generate_brr_data

import datetime

def apply_global_filters(df, filters):
    if df.empty:
        return df

    date_range = filters.get('date_range')
    if date_range and date_range != 'custom':
        max_dt = pd.to_datetime(df['date']).max()
        if pd.notnull(max_dt):
            today = max_dt.date()
            if date_range == 'yesterday':
                start = today - datetime.timedelta(days=1)
                end = start
            elif date_range == 'last_7_days':
                start = today - datetime.timedelta(days=6)
                end = today
            elif date_range == 'last_15_days':
                start = today - datetime.timedelta(days=14)
                end = today
            elif date_range == 'last_month':
                first_day_current = today.replace(day=1)
                end = first_day_current - datetime.timedelta(days=1)
                start = end.replace(day=1)
            elif date_range == 'last_3_months':
                start = today - datetime.timedelta(days=90)
                end = today
            elif date_range == 'last_6_months':
                start = today - datetime.timedelta(days=180)
                end = today
            elif date_range == 'last_1_year':
                start = today - datetime.timedelta(days=365)
                end = today
            else:
                start = end = None
            
            if start and end:
                filters['start_date'] = start.isoformat()
                filters['end_date'] = end.isoformat()

    if filters.get('start_date'):
        df = df[df['date'] >= pd.to_datetime(filters['start_date']).date()]
    if filters.get('end_date'):
        df = df[df['date'] <= pd.to_datetime(filters['end_date']).date()]

    portfolio = filters.get('portfolio')
    category = filters.get('category')
    subcategory = filters.get('subcategory')
    asin_filter = filters.get('asin')

    # Platform filter — check if column exists
    platform = filters.get('platform')
    if platform and platform != 'All':
        if 'platform' in df.columns:
            df = df[df['platform'] == platform]
        elif 'marketplace' in df.columns:
            df = df[df['marketplace'] == platform]

    if portfolio:
        df = df[df['portfolio'] == portfolio]
    if category:
        if isinstance(category, (list, tuple)):
            df = df[df['category'].isin(category)]
        else:
            df = df[df['category'] == category]
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            df = df[df['subcategory'].isin(subcategory)]
        else:
            df = df[df['subcategory'] == subcategory]
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            df = df[df['asin'].isin(asin_filter)]
        else:
            df = df[df['asin'] == asin_filter]

    return df


def generate_kpis(df, df_sales_only=None):
    if df.empty:
        return {
            'revenue': 0, 'orders': 0, 'units': 0, 'pageviews': 0,
            'spend': 0, 'roas': 0, 'conversion': 0, 'active_asins': 0
        }

    revenue = float(df['revenue'].sum())
    orders = int(df['orders'].sum())
    units = int(df['units'].sum())
    pageviews = int(df['pageviews'].sum())
    spend = float(df['total_spend'].sum())
    
    if df_sales_only is not None and not df_sales_only.empty:
        active_asins = int(df_sales_only['asin'].nunique())
    else:
        active_asins = int(df['asin'].nunique())

    roas = (revenue / spend) if spend > 0 else 0
    conversion = (orders / pageviews * 100) if pageviews > 0 else 0

    return {
        'revenue': revenue, 'orders': orders, 'units': units,
        'pageviews': pageviews, 'spend': spend,
        'roas': roas, 'conversion': conversion,
        'active_asins': active_asins
    }


def generate_charts_data(df):
    if df.empty:
        return {}

    df_date = df.groupby('date').agg({
        'revenue': 'sum', 'total_spend': 'sum', 'pageviews': 'sum', 'orders': 'sum'
    }).reset_index()
    df_date['date'] = df_date['date'].astype(str)

    labels = df_date['date'].tolist()
    revenue_line = df_date['revenue'].tolist()
    spend_line = df_date['total_spend'].tolist()
    pv_line = df_date['pageviews'].tolist()
    order_line = df_date['orders'].tolist()

    df_port = df.groupby('portfolio').agg({'units': 'sum'}).reset_index().sort_values('units', ascending=False).head(10)
    port_labels = df_port['portfolio'].tolist()
    port_units = df_port['units'].tolist()

    sp_sum = float(df['spend_sp'].sum()) if 'spend_sp' in df.columns else 0
    sb_sum = float(df['spend_sb'].sum()) if 'spend_sb' in df.columns else 0
    sd_sum = float(df['spend_sd'].sum()) if 'spend_sd' in df.columns else 0
    ad_total = sp_sum + sb_sum + sd_sum

    adTypeLabels = ['SB', 'SD', 'SP']
    adTypeVals = [sb_sum, sd_sum, sp_sum]

    ad_legend = []
    for i, lbl in enumerate(adTypeLabels):
        val = adTypeVals[i]
        pct = (val / ad_total * 100) if ad_total > 0 else 0
        ad_legend.append({'label': lbl, 'value': val, 'pct': round(pct, 1)})

    return {
        'trend': {
            'labels': labels, 'revenue': revenue_line, 'spend': spend_line,
            'pageviews': pv_line, 'orders': order_line,
        },
        'portfolio': {'labels': port_labels, 'units': port_units},
        'adType': {
            'labels': adTypeLabels, 'vals': adTypeVals,
            'total': ad_total, 'legend': ad_legend
        }
    }




def get_available_filters(df, valid_sales_dates=None):
    if df.empty:
        return {'dates': [], 'asins': [], 'portfolios': [], 'categories': [], 'subcategories': [], 'platforms': []}
    def clean(vals):
        return sorted([str(x) for x in set(vals) if str(x).strip() and str(x) != 'nan' and str(x) != 'None'])
        
    dates = clean(df['date'])
    if valid_sales_dates is not None:
        dates = sorted([d for d in dates if d in valid_sales_dates])

    # Detect platform column
    platforms = []
    if 'platform' in df.columns:
        platforms = clean(df['platform'])
    elif 'marketplace' in df.columns:
        platforms = clean(df['marketplace'])

    return {
        'dates': dates,
        'asins': clean(df['asin']),
        'portfolios': clean(df['portfolio']),
        'categories': clean(df['category']),
        'subcategories': clean(df['subcategory']),
        'platforms': platforms
    }


def generate_period_table(df):
    if df.empty:
        return []

    df_temp = df.copy()
    df_temp['date'] = df_temp['date'].astype(str)
    
    # Proxy active sales ASINs by checking if they had views, orders, or revenue
    active_sales = df_temp[(df_temp['pageviews'] > 0) | (df_temp['orders'] > 0) | (df_temp['revenue'] > 0)]
    asin_counts = active_sales.groupby('date')['asin'].nunique().reset_index(name='asin_count')

    grp = df_temp.groupby('date').agg({
        'revenue': 'sum', 'units': 'sum',
        'orders': 'sum', 'pageviews': 'sum', 'total_spend': 'sum'
    }).reset_index()
    
    grp = pd.merge(grp, asin_counts, on='date', how='left')
    grp['asin_count'] = grp['asin_count'].fillna(0).astype(int)

    grp['cvr'] = np.where(grp['pageviews'] > 0, (grp['orders'] / grp['pageviews']) * 100, 0)
    grp['roas'] = np.where(grp['total_spend'] > 0, grp['revenue'] / grp['total_spend'], 0)
    grp = grp.sort_values('date')
    return grp.to_dict('records')


def generate_grouped_table(df, group_col):
    if df.empty:
        return {'data': {}, 'dates': []}

    dates = sorted(df['date'].unique())
    dates_str = [str(d) for d in dates]

    grp = df.groupby([group_col, 'date']).agg({
        'pageviews': 'sum', 'units': 'sum', 'orders': 'sum',
        'revenue': 'sum', 'total_spend': 'sum'
    }).reset_index()
    grp['date'] = grp['date'].astype(str)

    grp['cvr'] = np.where(grp['pageviews'] > 0, (grp['orders'] / grp['pageviews']) * 100, 0)
    grp['upo'] = np.where(grp['orders'] > 0, grp['units'] / grp['orders'], 0)
    grp['spends'] = grp['total_spend']

    group_data = {}
    for _, row in grp.iterrows():
        key = row[group_col] if row[group_col] else 'Unknown'
        if key not in group_data:
            group_data[key] = {}
        group_data[key][row['date']] = {
            'revenue': float(row['revenue']),
            'orders': int(row['orders']),
            'units': int(row['units']),
            'pageviews': int(row['pageviews']),
            'spends': float(row['spends']),
            'cvr': float(row['cvr']),
            'upo': float(row['upo'])
        }

    return {'data': group_data, 'dates': dates_str}


def generate_spend_table(spend_df):
    if spend_df.empty:
        return []
    spend_df = spend_df[spend_df['spend'] > 0]
    grp = spend_df.groupby(['date', 'asin', 'ad_account', 'ad_type'])['spend'].sum().reset_index()
    grp['date'] = grp['date'].astype(str)
    grp = grp.sort_values(by='spend', ascending=False)
    return grp.to_dict('records')


def generate_bi_data(df):
    if df.empty:
        return []

    df_asin = df.groupby('asin').agg({
        'revenue': 'sum', 'total_spend': 'sum', 'orders': 'sum',
        'pageviews': 'sum', 'units': 'sum',
        'spend_sp': 'sum', 'spend_sb': 'sum', 'spend_sd': 'sum'
    }).reset_index()

    df_asin['roas'] = np.where(df_asin['total_spend'] > 0, df_asin['revenue'] / df_asin['total_spend'], 0)
    df_asin['cvr'] = np.where(df_asin['pageviews'] > 0, df_asin['orders'] / df_asin['pageviews'] * 100, 0)

    trev = float(df_asin['revenue'].sum())
    tspend = float(df_asin['total_spend'].sum())
    tord = int(df_asin['orders'].sum())
    tpv = int(df_asin['pageviews'].sum())
    roas = trev / tspend if tspend > 0 else 0
    cvr = (tord / tpv * 100) if tpv > 0 else 0

    high_roas = df_asin[(df_asin['roas'] > 5) & (df_asin['total_spend'] > 0)]
    low_roas = df_asin[(df_asin['roas'] > 0) & (df_asin['roas'] < 2) & (df_asin['total_spend'] > 0)]
    low_cvr = df_asin[(df_asin['pageviews'] > 500) & (df_asin['cvr'] < 1) & (df_asin['cvr'] > 0)]
    suppressed = df_asin[(df_asin['pageviews'] > 20) & (df_asin['orders'] == 0)]
    unfunded = df_asin[(df_asin['revenue'] > 0) & (df_asin['total_spend'] == 0) & (df_asin['pageviews'] > 200)]
    organic_asins = df_asin[(df_asin['revenue'] > 0) & (df_asin['total_spend'] == 0)]
    organic_rev = float(organic_asins['revenue'].sum())
    org_pct = (organic_rev / trev * 100) if trev > 0 else 0

    sorted_asins = df_asin.sort_values('revenue', ascending=False)
    top5_pct = (float(sorted_asins.head(5)['revenue'].sum()) / trev * 100) if trev > 0 else 0
    top_asin = sorted_asins.iloc[0]['asin'] if len(sorted_asins) > 0 else '—'
    top_asin_rev = float(sorted_asins.iloc[0]['revenue']) if len(sorted_asins) > 0 else 0

    sp_total = float(df_asin['spend_sp'].sum()) if 'spend_sp' in df_asin.columns else 0
    sb_total = float(df_asin['spend_sb'].sum()) if 'spend_sb' in df_asin.columns else 0
    sd_total = float(df_asin['spend_sd'].sum()) if 'spend_sd' in df_asin.columns else 0
    dom_type_map = {'sp': sp_total, 'sb': sb_total, 'sd': sd_total}
    dom_type = max(dom_type_map, key=dom_type_map.get).upper() if tspend > 0 else '—'
    dom_val = max(dom_type_map.values()) if tspend > 0 else 0

    low_cvr_top = low_cvr.sort_values('pageviews', ascending=False).head(3)
    low_cvr_desc = ', '.join(
        [f"{r['asin']}({r['cvr']:.1f}%)" for _, r in low_cvr_top.iterrows()]
    ) if len(low_cvr_top) > 0 else ''

    supp_top = suppressed.sort_values('pageviews', ascending=False).head(4)
    supp_desc = ' '.join([str(r['asin']) for _, r in supp_top.iterrows()]) if len(supp_top) > 0 else ''

    roas_str = f"{roas:.2f}x" if tspend > 0 else 'No data'
    num_days = int(df['date'].nunique())
    num_spending = len(df_asin[df_asin['total_spend'] > 0])
    extra_orders = int(round(tpv * 0.005))

    cards = [
        {
            'color': '#6366f1', 'pri': '⭐ Revenue Impact', 'title': 'ROAS Efficiency',
            'body': f"ROAS: <strong style=\"color:{'#10b981' if roas > 5 else '#f59e0b' if roas > 2 else '#ef4444'}\">{roas_str}</strong>. {len(high_roas)} ASINs >5x. {len(low_roas)} ASINs <2x.",
            'alert': 'ok' if roas > 5 else 'warn' if roas > 2 else 'error',
            'alert_text': 'Healthy' if roas > 5 else 'Monitor' if roas > 2 else 'Act now',
            'export_count': len(low_roas) + len(high_roas)
        },
        {
            'color': '#22d3ee', 'pri': '⭐ Listing Quality', 'title': 'CVR Anomaly Detection',
            'body': f"CVR: <strong style=\"color:{'#10b981' if cvr > 3 else '#f59e0b' if cvr > 1 else '#ef4444'}\">{cvr:.2f}%</strong>. "
                    + (f"{len(low_cvr)} ASINs: views>500, CVR<1%. Top: {low_cvr_desc}" if len(low_cvr) > 0 else "No anomalies.")
                    + f" +0.5% CVR = ~{extra_orders:,} orders.",
            'alert': 'error' if len(low_cvr) > 0 else 'ok',
            'alert_text': f'{len(low_cvr)} ASINs need fix' if len(low_cvr) > 0 else 'CVR healthy',
            'export_count': len(low_cvr)
        },
        {
            'color': '#ef4444', 'pri': '⭐ Availability Risk', 'title': 'Stockout & Suppression',
            'body': (f"{len(suppressed)} ASINs have views but 0 orders. Top: {supp_desc}.<br>Check inventory immediately."
                     if len(suppressed) > 0 else "No suppression detected."),
            'alert': 'error' if len(suppressed) > 0 else 'ok',
            'alert_text': f'{len(suppressed)} flagged' if len(suppressed) > 0 else 'All clear',
            'export_count': len(suppressed)
        },
        {
            'color': '#f59e0b', 'pri': '⭐ Budget Optimisation', 'title': 'Budget Reallocation Score',
            'body': (f"{len(unfunded)} ASINs: 200+ views, zero spend. " if len(unfunded) > 0 else "")
                    + (f"{len(low_roas)} ASINs burning budget <2x ROAS — reallocate to {len(high_roas)} high-ROAS ASINs. " if len(low_roas) > 0 else "")
                    + ("Budget efficient." if len(unfunded) == 0 and len(low_roas) == 0 else ""),
            'alert': 'warn' if (len(unfunded) + len(low_roas)) > 0 else 'ok',
            'alert_text': f'{len(unfunded) + len(low_roas)} opportunities' if (len(unfunded) + len(low_roas)) > 0 else 'Well allocated',
            'export_count': len(unfunded) + len(low_roas)
        },
        {
            'color': '#10b981', 'pri': 'High Value — Attribution', 'title': 'Organic vs Paid Split',
            'body': f"₹{organic_rev:,.0f} ({org_pct:.1f}%) organic. {len(organic_asins)} pure-organic ASINs. "
                    + ("Strong organic." if org_pct > 60 else "High ad dependency." if org_pct < 20 and tspend > 0 else "Healthy mix."),
            'alert': 'warn' if org_pct < 20 and tspend > 0 else 'ok',
            'alert_text': 'Ad dependent' if org_pct < 20 and tspend > 0 else f'{org_pct:.0f}% organic',
            'export_count': len(organic_asins)
        },
        {
            'color': '#a78bfa', 'pri': 'Strategic Allocation', 'title': 'Revenue Concentration Risk',
            'body': f"Top 5 ASINs = <strong style=\"color:{'#ef4444' if top5_pct > 80 else '#f59e0b' if top5_pct > 60 else '#10b981'}\">{top5_pct:.1f}%</strong>. "
                    + ("High risk." if top5_pct > 80 else "Moderate." if top5_pct > 60 else "Well spread.")
                    + f" Top: {top_asin} (₹{top_asin_rev:,.0f})",
            'alert': 'error' if top5_pct > 80 else 'warn' if top5_pct > 60 else 'ok',
            'alert_text': 'High risk' if top5_pct > 80 else 'Moderate' if top5_pct > 60 else 'Spread',
            'export_count': 0
        },
        {
            'color': '#34d399', 'pri': 'Campaign Mix', 'title': 'Ad Type Contribution',
            'body': (f"Dominant: <b>{dom_type}</b> (₹{dom_val:,.0f}). SP:₹{sp_total:,.0f} SB:₹{sb_total:,.0f} SD:₹{sd_total:,.0f}"
                     if tspend > 0 else "No spend data."),
            'alert': None, 'alert_text': None, 'export_count': 0
        },
        {
            'color': '#fb7185', 'pri': 'Scaling Signal', 'title': 'Spend Saturation',
            'body': (f"ROAS {roas:.1f}x — not budget-capped. Safe to scale." if roas > 4 and tspend > 0 else
                     f"Moderate ROAS. Check impression share." if roas > 2 and tspend > 0 else
                     f"Low ROAS — audit before scaling." if roas > 0 and tspend > 0 else "No spend."),
            'alert': 'ok' if roas > 4 else 'warn' if roas > 2 else 'error' if roas > 0 and tspend > 0 else None,
            'alert_text': 'Scale up' if roas > 4 else 'Check caps' if roas > 2 else 'Audit first' if roas > 0 and tspend > 0 else None,
            'export_count': 0
        },
        {
            'color': '#6366f1', 'pri': 'Cost Efficiency', 'title': 'Internal Auction Overlap',
            'body': f"{num_spending} spending ASINs. Subcategory siblings in SP compete against each other — use negative targeting to cut wasted CPC 10-25%.",
            'alert': None, 'alert_text': None, 'export_count': 0
        },
        {
            'color': '#22d3ee', 'pri': 'Attribution', 'title': 'Spend-to-Sales Lag',
            'body': f"SB/SD often convert 1-2 days after click. Use 3-day rolling ROAS before pausing campaigns. Segment spend: ₹{tspend:,.0f} vs revenue: ₹{trev:,.0f}.",
            'alert': None, 'alert_text': None, 'export_count': 0
        },
        {
            'color': '#f59e0b', 'pri': 'Budget Timing', 'title': 'Day-of-Week Patterns',
            'body': f"{num_days} days loaded. 4+ weeks needed to see patterns. Pre-load budgets Mon-Wed for B2C to capture peak demand days.",
            'alert': None, 'alert_text': None, 'export_count': 0
        },
    ]

    return cards


def _safe_growth(current, previous):
    """Calculate growth percentage; returns 0 when no previous data instead of 100%."""
    if previous > 0:
        return round(((current - previous) / previous) * 100, 1)
    return 0.0


def get_dashboard_payload(df, spend_df, filters, user=None):
    df_filtered = apply_global_filters(df, filters)
    
    if not df_filtered.empty:
        df_sales_only = df_filtered[
            (df_filtered['pageviews'] > 0) | 
            (df_filtered['orders'] > 0) | 
            (df_filtered['revenue'] > 0) | 
            (df_filtered['total_spend'] > 0)
        ]
    else:
        df_sales_only = df_filtered

    valid_sales_dates = set(df_sales_only['date'].astype(str)) if not df_sales_only.empty else set()
    filters_metadata = get_available_filters(df, valid_sales_dates)

    valid_asins = set(df_filtered['asin'].unique()) if not df_filtered.empty else set()
    spend_filtered = pd.DataFrame()
    if not spend_df.empty and valid_asins:
        spend_filtered = spend_df[spend_df['asin'].isin(valid_asins)]
        if filters.get('start_date'):
            spend_filtered = spend_filtered[spend_filtered['date'] >= pd.to_datetime(filters['start_date']).date()]
        if filters.get('end_date'):
            spend_filtered = spend_filtered[spend_filtered['date'] <= pd.to_datetime(filters['end_date']).date()]

    # Calculate previous period data for comparison
    df_prev = pd.DataFrame()
    compare_start_date = filters.get('compare_start_date')
    compare_end_date = filters.get('compare_end_date')
    
    if not df_filtered.empty:
        if compare_start_date and compare_end_date:
            prev_start = pd.to_datetime(compare_start_date)
            prev_end = pd.to_datetime(compare_end_date)
        else:
            max_dt = pd.to_datetime(df_filtered['date']).max()
            min_dt = pd.to_datetime(df_filtered['date']).min()
            delta = max_dt - min_dt + datetime.timedelta(days=1)
            prev_end = min_dt - datetime.timedelta(days=1)
            prev_start = prev_end - delta + datetime.timedelta(days=1)
            
        # Apply same category/portfolio filters to previous period, but different dates
        df_unfiltered_prev = df[(pd.to_datetime(df['date']) >= prev_start) & (pd.to_datetime(df['date']) <= prev_end)]
        # Apply non-date filters to prev period too (support lists)
        if filters.get('category'):
            cat = filters['category']
            if isinstance(cat, (list, tuple)):
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['category'].isin(cat)]
            else:
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['category'] == cat]
        if filters.get('portfolio'):
            port = filters['portfolio']
            if isinstance(port, (list, tuple)):
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['portfolio'].isin(port)]
            else:
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['portfolio'] == port]
        if filters.get('subcategory'):
            sub = filters['subcategory']
            if isinstance(sub, (list, tuple)):
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['subcategory'].isin(sub)]
            else:
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['subcategory'] == sub]
        platform = filters.get('platform')
        if platform and platform != 'All':
            if 'platform' in df_unfiltered_prev.columns:
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['platform'] == platform]
            elif 'marketplace' in df_unfiltered_prev.columns:
                df_unfiltered_prev = df_unfiltered_prev[df_unfiltered_prev['marketplace'] == platform]
        
        if not df_unfiltered_prev.empty:
            df_prev = df_unfiltered_prev[
                (df_unfiltered_prev['pageviews'] > 0) | 
                (df_unfiltered_prev['orders'] > 0) | 
                (df_unfiltered_prev['revenue'] > 0) | 
                (df_unfiltered_prev['total_spend'] > 0)
            ]

    payload = {
        'filters': filters_metadata,
        'kpis': generate_kpis(df_filtered, df_sales_only=df_sales_only),
        'charts': generate_charts_data(df_sales_only),
        'brr': generate_brr_data(df_sales_only),
        'bi': generate_bi_data(df_sales_only),
        'period_table': generate_period_table(df_filtered),
        'port_table': generate_grouped_table(df_sales_only, 'portfolio'),
        'cat_table': generate_grouped_table(df_sales_only, 'category'),
        'spend_table': generate_spend_table(spend_filtered)
    }
    return apply_business_logic(payload, df_sales_only, df_prev, filters)

def apply_business_logic(payload, df, df_prev, filters=None):
    revenue = payload.get('kpis', {}).get('revenue', 0)
    orders = payload.get('kpis', {}).get('orders', 0)
    spend = payload.get('kpis', {}).get('spend', 0)
    units = payload.get('kpis', {}).get('units', 0)
    pageviews = payload.get('kpis', {}).get('pageviews', 0)
    
    # Previous period KPIs
    prev_revenue = float(df_prev['revenue'].sum()) if not df_prev.empty else 0
    prev_orders = int(df_prev['orders'].sum()) if not df_prev.empty else 0
    prev_units = int(df_prev['units'].sum()) if not df_prev.empty else 0
    prev_spend = float(df_prev['total_spend'].sum()) if not df_prev.empty else 0
    prev_pageviews = int(df_prev['pageviews'].sum()) if not df_prev.empty else 0
    
    mom_growth = _safe_growth(revenue, prev_revenue)
    yoy_growth = 0  # would need year-ago data

    # Change metrics for all KPIs
    revenue_change = _safe_growth(revenue, prev_revenue)
    orders_change = _safe_growth(orders, prev_orders)
    units_change = _safe_growth(units, prev_units)
    spend_change = _safe_growth(spend, prev_spend)
    
    prev_aov = (prev_revenue / prev_orders) if prev_orders > 0 else 0
    aov = (revenue / orders) if orders > 0 else 0
    aov_change = _safe_growth(aov, prev_aov)
    
    prev_tacos = (prev_spend / prev_revenue * 100) if prev_revenue > 0 else 0
    tacos = (spend / revenue * 100) if revenue > 0 else 0
    tacos_change = round(tacos - prev_tacos, 1) if prev_revenue > 0 else 0
    
    prev_roas = (prev_revenue / prev_spend) if prev_spend > 0 else 0
    roas_val = (revenue / spend) if spend > 0 else 0
    roas_change = _safe_growth(roas_val, prev_roas)

    cogs = 0
    if not df.empty and 'cogs' in df.columns:
        cogs = float(df['cogs'].sum())
    gross_margin = revenue - cogs
    gross_margin_pct = (gross_margin / revenue * 100) if revenue > 0 else 0
    net_profit = gross_margin - spend
    
    prev_gross_margin = prev_revenue - 0  # no prev cogs data
    prev_net_profit = prev_gross_margin - prev_spend
    profit_change = _safe_growth(net_profit, prev_net_profit) if prev_net_profit != 0 else 0
    
    prev_gm_pct = (prev_gross_margin / prev_revenue * 100) if prev_revenue > 0 else 0
    gm_change = round(gross_margin_pct - prev_gm_pct, 1)
    
    prev_cm = round(prev_gm_pct - prev_tacos, 1)
    contribution_margin = round(gross_margin_pct - tacos, 1)
    contribution_margin_change = round(contribution_margin - prev_cm, 1)
    
    platforms = {}
    if hasattr(df, 'columns') and ('platform' in df.columns or 'marketplace' in df.columns):
        col = 'platform' if 'platform' in df.columns else 'marketplace'
        grp = df.groupby(col)['revenue'].sum().reset_index()
        prev_plat = df_prev.groupby(col)['revenue'].sum().to_dict() if not df_prev.empty and col in df_prev.columns else {}
        total = float(grp['revenue'].sum()) if not grp.empty else revenue
        for _, r in grp.iterrows():
            name = r[col] if r[col] and str(r[col]) != 'nan' else 'Unknown'
            rev = float(r['revenue'])
            pct = round((rev / total * 100) if total > 0 else 0, 1)
            p_rev = prev_plat.get(name, 0)
            growth = _safe_growth(rev, p_rev)
            platforms[name] = {'revenue': rev, 'pct': pct, 'growth': round(growth, 1)}
    else:
        platforms = {
            'Amazon': {'revenue': revenue, 'pct': 100 if revenue > 0 else 0, 'growth': round(mom_growth, 1)}
        }
    payload['platforms'] = platforms

    payload['kpis'].update({
        'cogs': cogs,
        'gross_margin': gross_margin,
        'gross_margin_pct': gross_margin_pct,
        'net_profit': net_profit,
        'mom_growth': mom_growth,
        'yoy_growth': yoy_growth,
        'aov': aov,
        'tacos': tacos,
        'revenue_change': revenue_change,
        'orders_change': orders_change,
        'units_change': units_change,
        'spend_change': spend_change,
        'aov_change': aov_change,
        'tacos_change': tacos_change,
        'profit_change': profit_change,
        'contribution_margin': contribution_margin,
        'contribution_margin_change': contribution_margin_change,
        'prev_mom': round(mom_growth, 1),
        'prev_yoy': 0,
        'orders_pct_change': orders_change,
        'units_pct_change': units_change,
        'run_rate_pct': 0,
        'run_rate_status': 'on_track',
    })

    # Prev revenue trend for charts
    trend = payload.get('charts', {}).get('trend', {})
    if trend and 'revenue' in trend:
        current_rev = trend.get('revenue', [])
        if current_rev and df_prev is not None and not df_prev.empty:
            prev_date_rev = df_prev.groupby('date')['revenue'].sum().sort_index()
            prev_rev_list = prev_date_rev.tolist()
            # Pad or truncate to match current period length
            if len(prev_rev_list) < len(current_rev):
                prev_rev_list.extend([0] * (len(current_rev) - len(prev_rev_list)))
            elif len(prev_rev_list) > len(current_rev):
                prev_rev_list = prev_rev_list[:len(current_rev)]
            payload['charts']['trend']['prev_revenue'] = prev_rev_list
        else:
            payload['charts']['trend']['prev_revenue'] = [0] * len(current_rev)
        
    # ═══════════════════════════════════════════════════════
    # CATEGORY PERFORMANCE (grouped by 'category' column)
    # Used by CEO & Business dashboards
    # ═══════════════════════════════════════════════════════
    cat_perf = []
    if not df.empty and 'category' in df.columns:
        total_rev = float(df['revenue'].sum())
        curr_grp = df.groupby('category')['revenue'].sum().to_dict()
        prev_grp = df_prev.groupby('category')['revenue'].sum().to_dict() if not df_prev.empty and 'category' in df_prev.columns else {}
        for cat, rev in curr_grp.items():
            name = cat if cat and str(cat) != 'nan' else 'Others'
            p_rev = prev_grp.get(cat, 0)
            growth = _safe_growth(rev, p_rev)
            cat_perf.append({
                'category': name, 'revenue': float(rev),
                'growth': round(growth, 1),
                'contribution': round((rev / total_rev * 100) if total_rev > 0 else 0, 1)
            })
    payload['category_performance'] = sorted(cat_perf, key=lambda x: x['revenue'], reverse=True)

    # ═══════════════════════════════════════════════════════
    # CLUSTER PERFORMANCE (grouped by 'subcategory' column)
    # Used by Category dashboard
    # ═══════════════════════════════════════════════════════
    cluster_perf = []
    if not df.empty and 'subcategory' in df.columns:
        total_rev = float(df['revenue'].sum())
        curr_grp = df.groupby('subcategory')['revenue'].sum().to_dict()
        prev_grp = df_prev.groupby('subcategory')['revenue'].sum().to_dict() if not df_prev.empty and 'subcategory' in df_prev.columns else {}
        for sub, rev in curr_grp.items():
            name = sub if sub and str(sub) != 'nan' else 'Others'
            p_rev = prev_grp.get(sub, 0)
            growth = _safe_growth(rev, p_rev)
            cluster_perf.append({
                'cluster': name, 'revenue': float(rev),
                'growth': round(growth, 1), 
                'contribution': round((rev / total_rev * 100) if total_rev > 0 else 0, 1)
            })
    payload['cluster_performance'] = sorted(cluster_perf, key=lambda x: x['revenue'], reverse=True)

    # ═══════════════════════════════════════════════════════
    # INVENTORY HEALTH — SKU counts
    # Used by CEO & Business dashboards
    # ═══════════════════════════════════════════════════════
    if not df.empty:
        asin_grp = df.groupby('asin').agg({
            'orders': 'sum', 'pageviews': 'sum', 'revenue': 'sum', 'units': 'sum'
        }).reset_index()
        
        total_asins = len(asin_grp)
        
        # OOS: has pageviews but zero orders (customers see it but can't buy)
        oos_mask = (asin_grp['pageviews'] > 20) & (asin_grp['orders'] == 0)
        oos_count = int(oos_mask.sum())
        
        # Low stock: has orders but very few relative to pageviews (high demand, low fulfillment)
        low_stock_mask = (asin_grp['orders'] > 0) & (asin_grp['orders'] <= 3) & (asin_grp['pageviews'] > 50)
        low_stock_count = int(low_stock_mask.sum())
        
        # Overstock: high units relative to orders (sitting inventory)
        overstock_mask = (asin_grp['units'] > 50) & (asin_grp['orders'] > 0) & (asin_grp['units'] > asin_grp['orders'] * 10)
        overstock_count = int(overstock_mask.sum())
        
        # In stock: everything else that has orders
        in_stock_count = total_asins - oos_count - low_stock_count - overstock_count
        if in_stock_count < 0:
            in_stock_count = 0
            
        payload['inventory'] = {
            'in_stock': in_stock_count,
            'low_stock': low_stock_count,
            'oos': oos_count,
            'overstock': overstock_count
        }
        
        # Inventory Position (days of cover buckets)
        total_rev = float(asin_grp['revenue'].sum())
        buckets = {
            'gt60': {'label': '> 60 Days', 'skus': 0, 'revenue': 0, 'color': 'green'},
            '30_60': {'label': '30-60 Days', 'skus': 0, 'revenue': 0, 'color': 'amber'},
            '7_30': {'label': '7-30 Days', 'skus': 0, 'revenue': 0, 'color': 'orange'},
            'lt7': {'label': '< 7 Days', 'skus': 0, 'revenue': 0, 'color': 'red'}
        }
        
        for _, row in asin_grp.iterrows():
            days_cover = ((row['units'] * 1.5) + (row['pageviews'] / 10.0)) / max(row['units'] / 30.0, 0.1)
            rev = float(row['revenue'])
            if days_cover > 60: buckets['gt60']['skus'] += 1; buckets['gt60']['revenue'] += rev
            elif days_cover > 30: buckets['30_60']['skus'] += 1; buckets['30_60']['revenue'] += rev
            elif days_cover > 7: buckets['7_30']['skus'] += 1; buckets['7_30']['revenue'] += rev
            else: buckets['lt7']['skus'] += 1; buckets['lt7']['revenue'] += rev
            
        inv_result = []
        for key in ['gt60', '30_60', '7_30', 'lt7']:
            b = buckets[key]
            inv_result.append({
                'label': b['label'], 'revenue': b['revenue'],
                'pct': int(round((b['revenue'] / total_rev * 100) if total_rev > 0 else 0, 0)),
                'color': b['color']
            })
        payload['inventory_position'] = inv_result
        
        # Category Health (for Category dashboard)
        active = len(asin_grp[asin_grp['orders'] > 0])
        at_risk = len(asin_grp[(asin_grp['pageviews'] > 100) & (asin_grp['orders'] <= 1)])
        
        # Previous period category health for changes
        if not df_prev.empty:
            prev_asin_grp = df_prev.groupby('asin').agg({'orders': 'sum', 'pageviews': 'sum'}).reset_index()
            prev_active = len(prev_asin_grp[prev_asin_grp['orders'] > 0])
            prev_oos = len(prev_asin_grp[(prev_asin_grp['pageviews'] > 20) & (prev_asin_grp['orders'] == 0)])
            prev_low = len(prev_asin_grp[(prev_asin_grp['orders'] > 0) & (prev_asin_grp['orders'] <= 3) & (prev_asin_grp['pageviews'] > 50)])
            prev_at_risk = len(prev_asin_grp[(prev_asin_grp['pageviews'] > 100) & (prev_asin_grp['orders'] <= 1)])
        else:
            prev_active = 0
            prev_oos = 0
            prev_low = 0
            prev_at_risk = 0
        
        payload['category_health'] = {
            'active_skus': active, 
            'low_stock': low_stock_count,
            'oos': oos_count, 
            'at_risk_rating': at_risk,
            'active_change': active - prev_active,
            'oos_change': oos_count - prev_oos,
            'low_stock_change': low_stock_count - prev_low,
            'at_risk_change': at_risk - prev_at_risk
        }
    else:
        payload['inventory'] = {'in_stock': 0, 'low_stock': 0, 'oos': 0, 'overstock': 0}
        payload['category_health'] = {'active_skus': 0, 'low_stock': 0, 'oos': 0, 'at_risk_rating': 0,
                                       'active_change': 0, 'oos_change': 0, 'low_stock_change': 0, 'at_risk_change': 0}
        payload['inventory_position'] = []

    # ═══════════════════════════════════════════════════════
    # RETURNS & RATINGS — No actual data in model, leave empty
    # ═══════════════════════════════════════════════════════
    payload['returns_ratings'] = {
        'rate': 0, 'change': 0, 'reasons': [],
        'avg_rating': 0, 'avg_rating_change': 0, 'low_rating_count': 0
    }
    payload['returns'] = payload['returns_ratings']

    # ═══════════════════════════════════════════════════════
    # GROWTH OPPORTUNITIES
    # ═══════════════════════════════════════════════════════
    opps = []
    if not df.empty and 'subcategory' in df.columns:
        curr_grp = df.groupby('subcategory')['pageviews'].sum().to_dict()
        prev_grp = df_prev.groupby('subcategory')['pageviews'].sum().to_dict() if not df_prev.empty and 'subcategory' in df_prev.columns else {}
        for sub, pvs in curr_grp.items():
            name = sub if sub and str(sub) != 'nan' else 'Others'
            p_pvs = prev_grp.get(sub, 0)
            growth = _safe_growth(pvs, p_pvs)
            opps.append({
                'search_term': name.lower().replace('_', ' '),
                'category': 'General', 'search_volume': int(pvs),
                'growth': round(growth, 0),
                'opportunity': 'High' if growth > 20 else 'Medium' if growth > 5 else 'Low'
            })
    payload['growth_opportunities'] = sorted(opps, key=lambda x: x['search_volume'], reverse=True)
    
    # ═══════════════════════════════════════════════════════
    # TOP / UNDERPERFORMING PRODUCTS
    # Sort top by REVENUE (highest revenue first)
    # Sort under by DROP% (highest drop first, growth < 0)
    # ═══════════════════════════════════════════════════════
    prod_growth = []
    if not df.empty and 'subcategory' in df.columns:
        curr_grp = df.groupby(['asin', 'subcategory']).agg({'revenue': 'sum', 'units': 'sum'}).reset_index()
        prev_grp = df_prev.groupby('asin')['revenue'].sum().to_dict() if not df_prev.empty else {}
        for _, row in curr_grp.iterrows():
            asin = row['asin']
            curr_rev = float(row['revenue'])
            prev_rev = prev_grp.get(asin, 0)
            growth = _safe_growth(curr_rev, prev_rev)
            prod_growth.append({
                'sku': str(asin)[:10],
                'product_name': f"Product {str(asin)[-4:]}", 
                'cluster': row['subcategory'] if row['subcategory'] and str(row['subcategory']) != 'nan' else 'Others',
                'revenue': curr_rev, 'growth': round(growth, 1),
                'units_sold': int(row['units']), 'rating': 0,
                'drop_pct': round(growth, 1), 'impact': abs(curr_rev - prev_rev)
            })
    
    # Top products: sorted by REVENUE (highest first)
    top_perf = sorted(prod_growth, key=lambda x: x['revenue'], reverse=True)
    
    # Underperforming products: only those with negative growth, sorted by drop% (most negative first)
    under_perf = sorted([p for p in prod_growth if p['growth'] < 0], key=lambda x: x['growth'])

    payload['cat_top_products'] = top_perf[:5]
    payload['cat_all_top_products'] = top_perf
    payload['cat_under_products'] = under_perf[:5]
    payload['cat_all_under_products'] = under_perf

    # ═══════════════════════════════════════════════════════
    # WATERFALL / PROFITABILITY
    # ═══════════════════════════════════════════════════════
    payload['waterfall'] = [
        {'label': 'Revenue', 'value': revenue, 'type': 'total'},
        {'label': 'COGS', 'value': -cogs, 'type': 'sub'},
        {'label': 'Gross Profit', 'value': gross_margin, 'type': 'total'},
        {'label': 'Ad Spend', 'value': -spend, 'type': 'sub'},
        {'label': 'Op Ex', 'value': 0, 'type': 'sub'},
        {'label': 'Net Profit', 'value': net_profit, 'type': 'total'}
    ]
    
    oos = payload['inventory'].get('oos', 0)
    payload['oos_impact'] = {
        'lost_sales': int(oos * (aov if aov > 0 else 500) * 2),
        'skus_affected': int(oos),
        'orders_lost': int(oos * 2)
    }
    
    # ═══════════════════════════════════════════════════════
    # BUSINESS HEALTH SCORE — calculated from actual metrics
    # ═══════════════════════════════════════════════════════
    growth_score = min(100, max(0, 50 + mom_growth)) if prev_revenue > 0 else 50
    profitability_score = min(100, max(0, int(gross_margin_pct * 2))) if revenue > 0 else 0
    
    inv_total = payload['inventory'].get('in_stock', 0) + payload['inventory'].get('low_stock', 0) + payload['inventory'].get('oos', 0) + payload['inventory'].get('overstock', 0)
    inventory_score = int((payload['inventory'].get('in_stock', 0) / inv_total * 100)) if inv_total > 0 else 0
    
    operations_score = min(100, max(0, int(50 + (roas_val - 2) * 10))) if spend > 0 else 50
    
    overall_score = int((growth_score * 0.3 + profitability_score * 0.25 + inventory_score * 0.25 + operations_score * 0.2))
    
    payload['business_health'] = {
        'score': overall_score,
        'breakdown': {
            'growth': int(growth_score),
            'profitability': int(profitability_score),
            'inventory': int(inventory_score),
            'operations': int(operations_score)
        }
    }
    
    payload['marketing'] = {
        'ad_spend': spend,
        'roas': round(roas_val, 2),
        'roas_change_pct': roas_change,
        'tacos': round(tacos, 1),
        'tacos_change': tacos_change
    }
    
    alerts = []
    if oos > 0:
        alerts.append({'severity': 'critical', 'icon': '🔴', 'title': f'{oos} SKUs OOS', 'subtitle': f'lost sales impact'})
    if mom_growth < -10 and prev_revenue > 0:
        alerts.append({'severity': 'warning', 'icon': '🟡', 'title': f'Revenue declining {mom_growth:.1f}%', 'subtitle': 'vs previous period'})
    payload['critical_alerts'] = alerts
    payload['priorities'] = [{'rank': 1, 'title': 'Fix OOS SKUs', 'subtitle': f'{oos} SKUs affected', 'priority': 'High'}]
    
    if not df.empty and 'date' in df.columns:
        dates = sorted(df['date'].unique())
        labels = [str(d) for d in dates]
        rev_by_date = df.groupby('date')['revenue'].sum().reindex(dates).fillna(0).tolist()
        payload['forecast'] = {
            'predicted': sum(rev_by_date),
            'target': sum(rev_by_date),
            'gap': 0,
            'gap_pct': 0,
            'labels': labels,
            'actual': rev_by_date,
            'forecast': [None] * len(labels),
            'target_line': rev_by_date
        }
    else:
        payload['forecast'] = {
            'predicted': 0, 'target': 0, 'gap': 0, 'gap_pct': 0,
            'labels': [], 'actual': [], 'forecast': [], 'target_line': []
        }
    payload['profit_summary'] = {
        'revenue': revenue, 'ad_spend': spend, 'gross_margin': gross_margin,
        'gross_margin_pct': gross_margin_pct, 'net_profit': net_profit, 
        'net_profit_pct': (net_profit / revenue * 100) if revenue > 0 else 0
    }

    return payload
