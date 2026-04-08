import pandas as pd
import datetime
import numpy as np

from apps.dashboard.models import SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData


def clean_currency(x):
    """Removes commas and currency symbols to return a float."""
    if isinstance(x, str):
        x = x.replace('₹', '').replace(',', '').strip()
    try:
        return float(x)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Category
# ---------------------------------------------------------------------------

def process_category_file(file_obj, user):
    """
    Upsert category mappings scoped to the given user.
    - New ASINs are inserted.
    - Existing ASINs are updated so you can correct mapping data without
      having to wipe and reload everything.
    """
    df = pd.read_excel(file_obj)
    # Expected columns: ASIN, Portfolio, Category, Subcategory

    existing_asins = set(CategoryMapping.objects.filter(user=user).values_list('asin', flat=True))

    new_mappings = []

    for _, row in df.iterrows():
        asin = str(row.get('ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        portfolio   = str(row.get('Portfolio', '')).strip()
        category    = str(row.get('Category', '')).strip()
        subcategory = str(row.get('Subcategory', '')).strip()

        if asin in existing_asins:
            # Update in-place so portfolio/category corrections are applied
            CategoryMapping.objects.filter(user=user, asin=asin).update(
                portfolio=portfolio,
                category=category,
                subcategory=subcategory,
            )
        else:
            new_mappings.append(
                CategoryMapping(
                    user=user,
                    asin=asin,
                    portfolio=portfolio,
                    category=category,
                    subcategory=subcategory,
                )
            )

    if new_mappings:
        CategoryMapping.objects.bulk_create(new_mappings, ignore_conflicts=True)


# ---------------------------------------------------------------------------
# Price
# ---------------------------------------------------------------------------

def process_price_file(file_obj, user):
    """
    Upsert price data scoped to the given user.
    - New ASINs are inserted.
    - Existing ASINs are updated (price corrections should be reflected).
    """
    df = pd.read_excel(file_obj)
    # Expected columns: ASIN, Price

    existing_asins = set(PriceData.objects.filter(user=user).values_list('asin', flat=True))

    new_prices = []

    for _, row in df.iterrows():
        asin = str(row.get('ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        price_val = clean_currency(row.get('Price', 0))

        if asin in existing_asins:
            PriceData.objects.filter(user=user, asin=asin).update(price=price_val)
        else:
            new_prices.append(PriceData(user=user, asin=asin, price=price_val))

    if new_prices:
        PriceData.objects.bulk_create(new_prices, ignore_conflicts=True)


# ---------------------------------------------------------------------------
# Spend
# ---------------------------------------------------------------------------

def process_spend_file(file_obj, user):
    """
    Insert spend rows scoped to the user, ignoring duplicates.
    """
    df = pd.read_excel(file_obj)
    # Expected columns: Date, Ad Account, Ad Type, ASIN, Spend

    user_existing_keys = set(SpendData.objects.filter(user=user).values_list('date', 'asin', 'ad_account', 'ad_type'))

    new_spends = []
    skipped = 0

    for row in df.to_dict('records'):
        asin = str(row.get('ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        try:
            row_date = pd.to_datetime(row.get('Date')).date()
        except Exception:
            continue

        spend_val  = clean_currency(row.get('Spend', 0))
        ad_account = str(row.get('Ad Account', '')).strip()

        ad_type = str(row.get('Ad Type', '')).strip().upper()
        if ad_type in ('SPONSORED PRODUCTS', 'SP'):
            ad_type = 'SP'
        elif ad_type in ('SPONSORED BRANDS', 'SB'):
            ad_type = 'SB'
        elif ad_type in ('SPONSORED DISPLAY', 'SD'):
            ad_type = 'SD'
        else:
            ad_type = ad_type[:10]

        key = (row_date, asin, ad_account, ad_type)
        if key in user_existing_keys:
            skipped += 1
            continue
            
        user_existing_keys.add(key)
        new_spends.append(
            SpendData(
                user=user,
                date=row_date,
                asin=asin,
                ad_account=ad_account,
                ad_type=ad_type,
                spend=spend_val,
            )
        )

    if new_spends:
        batch_size = 10_000
        for i in range(0, len(new_spends), batch_size):
            SpendData.objects.bulk_create(new_spends[i:i + batch_size], ignore_conflicts=True)

    print(f"[SpendData] new={len(new_spends)}, skipped_duplicates={skipped}")


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------

def process_sales_file(file_obj, date_str, user):
    """
    Insert sales rows scoped to the user, ignoring duplicates.
    """
    try:
        date_obj = datetime.datetime.strptime(date_str, '%d-%m-%Y').date()
    except ValueError:
        try:
            date_obj = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()
        except ValueError:
            date_obj = datetime.date.today()

    df = pd.read_csv(file_obj)
    # Expected columns: (Child) ASIN, Page Views - Total, Units Ordered,
    #                   Ordered Product Sales, Total Order Items

    user_existing_asins = set(SalesData.objects.filter(user=user, date=date_obj).values_list('asin', flat=True))

    new_sales = []
    skipped = 0

    for _, row in df.iterrows():
        asin = str(row.get('(Child) ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        if asin in user_existing_asins:
            skipped += 1
            continue

        user_existing_asins.add(asin)
        new_sales.append(
            SalesData(
                user=user,
                date=date_obj,
                asin=asin,
                pageviews=int(clean_currency(row.get('Page Views - Total', 0))),
                units=int(clean_currency(row.get('Units Ordered', 0))),
                orders=int(clean_currency(row.get('Total Order Items', 0))),
                revenue=float(clean_currency(row.get('Ordered Product Sales', 0))),
            )
        )

    if new_sales:
        SalesData.objects.bulk_create(new_sales, ignore_conflicts=True)

    print(f"[SalesData] date={date_obj}, new={len(new_sales)}, skipped_duplicates={skipped}")


# ---------------------------------------------------------------------------
# Dashboard aggregation
# ---------------------------------------------------------------------------

def generate_dashboard_data(user):
    """
    Merges all independent tables for the given user and dumps them into
    ProcessedDashboardData to quickly serve the frontend.
    """
    ProcessedDashboardData.objects.filter(user=user).delete()

    sales_qs = SalesData.objects.filter(user=user).values()
    spend_qs = SpendData.objects.filter(user=user).values()
    cat_qs   = CategoryMapping.objects.filter(user=user).values()
    price_qs = PriceData.objects.filter(user=user).values()

    if not sales_qs and not spend_qs:
        return

    df_sales = pd.DataFrame(list(sales_qs))
    if not df_sales.empty:
        df_sales = df_sales[['date', 'asin', 'pageviews', 'units', 'orders', 'revenue']]
    else:
        df_sales = pd.DataFrame(columns=['date', 'asin', 'pageviews', 'units', 'orders', 'revenue'])

    df_spend = pd.DataFrame(list(spend_qs))
    if not df_spend.empty:
        df_spend = (
            df_spend
            .groupby(['date', 'asin', 'ad_type'])['spend']
            .sum()
            .unstack('ad_type')
            .reset_index()
            .fillna(0)
        )
        col_map = {
            c: f"spend_{str(c).lower()}"
            for c in df_spend.columns
            if c not in ('date', 'asin')
        }
        df_spend.rename(columns=col_map, inplace=True)
    else:
        df_spend = pd.DataFrame(columns=['date', 'asin'])

    df_cat = pd.DataFrame(list(cat_qs))
    if df_cat.empty:
        df_cat = pd.DataFrame(columns=['asin', 'portfolio', 'category', 'subcategory'])
    else:
        df_cat = df_cat[['asin', 'portfolio', 'category', 'subcategory']]

    df_price = pd.DataFrame(list(price_qs))
    if df_price.empty:
        df_price = pd.DataFrame(columns=['asin', 'price'])
    else:
        df_price = df_price[['asin', 'price']]

    df_merged = pd.merge(df_sales, df_spend, on=['date', 'asin'], how='outer')
    df_merged = pd.merge(df_merged, df_cat,   on='asin',           how='left')
    df_merged = pd.merge(df_merged, df_price, on='asin',           how='left')

    fill_values = {
        'portfolio': '', 'category': '', 'subcategory': '',
        'price': 0.0, 'pageviews': 0, 'units': 0, 'orders': 0, 'revenue': 0.0,
        'spend_sp': 0.0, 'spend_sb': 0.0, 'spend_sd': 0.0,
    }
    for col, fill_val in fill_values.items():
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].fillna(fill_val)

    records = []
    for row in df_merged.to_dict('records'):
        spend_sp    = float(row.get('spend_sp', 0))
        spend_sb    = float(row.get('spend_sb', 0))
        spend_sd    = float(row.get('spend_sd', 0))
        total_spend = spend_sp + spend_sb + spend_sd

        records.append(
            ProcessedDashboardData(
                user=user,
                date=row['date'],
                asin=row['asin'],
                portfolio=str(row.get('portfolio', '')) or '',
                category=str(row.get('category', ''))   or '',
                subcategory=str(row.get('subcategory', '')) or '',
                price=float(row.get('price', 0)),
                pageviews=int(row.get('pageviews', 0)),
                units=int(row.get('units', 0)),
                orders=int(row.get('orders', 0)),
                revenue=float(row.get('revenue', 0)),
                spend_sp=spend_sp,
                spend_sb=spend_sb,
                spend_sd=spend_sd,
                total_spend=total_spend,
            )
        )

    batch_size = 10_000
    for i in range(0, len(records), batch_size):
        ProcessedDashboardData.objects.bulk_create(
            records[i:i + batch_size], ignore_conflicts=True
        )