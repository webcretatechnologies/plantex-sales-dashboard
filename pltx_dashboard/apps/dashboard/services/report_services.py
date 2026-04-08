import pandas as pd
from dateutil.relativedelta import relativedelta

MONTH_NAMES = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
               'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']


def _period_label(ym_str, min_day, max_day):
    """E.g. '2026-03' + min=1, max=5 -> '1-5 Mar 2026'"""
    parts = ym_str.split('-')
    yr = int(parts[0])
    mo = int(parts[1])
    return f"{min_day}-{max_day} {MONTH_NAMES[mo - 1]} {yr}"


def generate_brr_data(df):
    if df.empty:
        return {}
    
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    dates = df['date'].dt.date.unique()
    if len(dates) == 0:
        return {}
        
    dates.sort()
    cur_month_str = dates[-1].strftime('%Y-%m')
    cur_dates = [d for d in dates if d.strftime('%Y-%m') == cur_month_str]
    max_day = max([d.day for d in cur_dates])
    min_day = min([d.day for d in cur_dates])
    
    cur_month_first = cur_dates[0].replace(day=1)
    prev_month_first = cur_month_first - relativedelta(months=1)
    prev_month_str = prev_month_first.strftime('%Y-%m')
    prev_dates = [d for d in dates if d.strftime('%Y-%m') == prev_month_str and min_day <= d.day <= max_day]
    
    ly_month_first = cur_month_first - relativedelta(years=1)
    ly_month_str = ly_month_first.strftime('%Y-%m')
    ly_dates = [d for d in dates if d.strftime('%Y-%m') == ly_month_str and min_day <= d.day <= max_day]
    
    cur_label = _period_label(cur_month_str, min_day, max_day)
    prev_label = _period_label(prev_month_str, min_day, max_day) if prev_dates else 'No prev month data'
    ly_label = _period_label(ly_month_str, min_day, max_day) if ly_dates else 'No last year data'

    def agg_period(date_list, extra_mask=None):
        mask = df['date'].dt.date.isin(date_list)
        if extra_mask is not None:
            mask = mask & extra_mask
            
        period_df = df[mask]
        pv = int(period_df['pageviews'].sum())
        units = int(period_df['units'].sum())
        rev = float(period_df['revenue'].sum())
        ord_ = int(period_df['orders'].sum())
        sp = float(period_df['total_spend'].sum())
        
        return {
            'pv': pv, 'units': units, 'rev': rev, 'ord': ord_, 'sp': sp,
            'cvr': (ord_ / pv * 100) if pv > 0 else 0,
            'adcost': (sp / rev * 100) if rev > 0 else 0
        }
        
    overall = {
        'cur': agg_period(cur_dates),
        'prev': agg_period(prev_dates),
        'ly': agg_period(ly_dates)
    }
    
    def get_asp_band(price):
        try:
            if price == 0 or pd.isna(price): return 'No Price'
            if price <= 500: return '0–500'
            if price <= 1000: return '501–1000'
            return '1000+'
        except:
            return 'No Price'
        
    df['asp_band'] = df['price'].apply(get_asp_band)
    asp_bands = ['0–500', '501–1000', '1000+', 'No Price']
    asp_data = {}
    for b in asp_bands:
        mask = df['asp_band'] == b
        asp_data[b] = {
            'cur': agg_period(cur_dates, mask),
            'prev': agg_period(prev_dates, mask),
            'ly': agg_period(ly_dates, mask)
        }
        
    portfolios = sorted([str(p) for p in df['portfolio'].unique() if pd.notna(p)])
    port_data = {}
    for p in portfolios:
        mask = df['portfolio'] == p
        port_data[p] = {
            'cur': agg_period(cur_dates, mask),
            'prev': agg_period(prev_dates, mask),
            'ly': agg_period(ly_dates, mask)
        }
        
    return {
        'overall': overall,
        'asp': asp_data,
        'portfolio': port_data,
        'labels': {
            'cur': cur_label,
            'prev': prev_label,
            'ly': ly_label
        }
    }
