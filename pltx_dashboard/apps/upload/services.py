import pandas as pd
import datetime

from apps.dashboard.utils import clean_currency, clean_number

from apps.dashboard.models import (
    SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData,
    # Slim Flipkart models
    FlipkartSearchTraffic, FlipkartCategoryMap, FlipkartPrice,
    FlipkartPCA, FlipkartPLA, FlipkartSalesInvoice, FlipkartCoupon,
    FlipkartProcessedDashboardData,
)
from django.db import connection

def _get_upsert_kwargs(unique_fields, update_fields):
    kwargs = {'update_conflicts': True, 'update_fields': update_fields}
    if connection.vendor != 'mysql':
        kwargs['unique_fields'] = unique_fields
    return kwargs


# ---------------------------------------------------------------------------
# Category
# ---------------------------------------------------------------------------

def process_category_file(file_obj, user):
    """
    Upsert category mappings scoped to the given user.
    - Uses bulk_create with update_conflicts to elegantly update existing records.
    """
    df = pd.read_excel(file_obj)
    
    required_cols = ['ASIN', 'Portfolio', 'Category', 'Subcategory', 'Skus']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Category Mapping missing required columns: {', '.join(missing_cols)}")

    new_mappings = []

    for _, row in df.iterrows():
        asin = str(row.get('ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        portfolio   = str(row.get('Portfolio', '')).strip()
        category    = str(row.get('Category', '')).strip()
        subcategory = str(row.get('Subcategory', '')).strip()

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
        CategoryMapping.objects.bulk_create(
            new_mappings,
            **_get_upsert_kwargs(
                unique_fields=['user', 'asin'],
                update_fields=['portfolio', 'category', 'subcategory']
            )
        )


# ---------------------------------------------------------------------------
# Price
# ---------------------------------------------------------------------------

def process_price_file(file_obj, user):
    """
    Upsert price data scoped to the given user.
    - Uses bulk_create with update_conflicts to smartly update existing values.
    """
    df = pd.read_excel(file_obj)

    required_cols = ['ASIN', 'Price']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Pricing Data missing required columns: {', '.join(missing_cols)}")

    new_prices = []

    for _, row in df.iterrows():
        asin = str(row.get('ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        price_val = clean_currency(row.get('Price', 0))

        new_prices.append(PriceData(user=user, asin=asin, price=price_val))

    if new_prices:
        PriceData.objects.bulk_create(
            new_prices,
            **_get_upsert_kwargs(
                unique_fields=['user', 'asin'],
                update_fields=['price']
            )
        )


# ---------------------------------------------------------------------------
# Spend
# ---------------------------------------------------------------------------

def process_spend_file(file_obj, user):
    """
    Insert spend rows scoped to the user, or update existing records directly if they already exist.
    """
    df = pd.read_excel(file_obj)

    required_cols = ['Date', 'Ad Account', 'Ad Type', 'ASIN', 'Spend']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Ads Spends missing required columns: {', '.join(missing_cols)}")

    new_spends = []

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
            SpendData.objects.bulk_create(
                new_spends[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'date', 'asin', 'ad_account', 'ad_type'],
                    update_fields=['spend']
                )
            )

    print(f"[SpendData] Processed and upserted bulk batch of {len(new_spends)} records.")


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------

def process_sales_file(file_obj, date_str, user):
    """
    Insert sales rows scoped to the user, or update existing records directly if they already exist.
    """
    try:
        date_obj = datetime.datetime.strptime(date_str, '%d-%m-%Y').date()
    except ValueError:
        raise ValueError(f"Invalid Date format '{date_str}' in Daily Sales filename. Please strictly use DD-MM-YYYY.csv format.")

    df = pd.read_csv(file_obj)

    required_cols = ['(Child) ASIN', 'Page Views - Total', 'Units Ordered', 'Ordered Product Sales', 'Total Order Items']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Daily Sales missing required columns: {', '.join(missing_cols)}")

    new_sales = []

    for _, row in df.iterrows():
        asin = str(row.get('(Child) ASIN', '')).strip()
        if not asin or asin.lower() == 'nan':
            continue

        new_sales.append(
            SalesData(
                user=user,
                date=date_obj,
                asin=asin,
                pageviews=clean_number(row.get('Page Views - Total', 0)),
                units=clean_number(row.get('Units Ordered', 0)),
                orders=clean_number(row.get('Total Order Items', 0)),
                revenue=float(clean_currency(row.get('Ordered Product Sales', 0))),
            )
        )

    if new_sales:
        SalesData.objects.bulk_create(
            new_sales,
            **_get_upsert_kwargs(
                unique_fields=['user', 'date', 'asin'],
                update_fields=['pageviews', 'units', 'orders', 'revenue']
            )
        )

    print(f"[SalesData] date={date_obj}, Processed and upserted bulk batch of {len(new_sales)} records.")


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
                pageviews=clean_number(row.get('pageviews', 0)),
                units=clean_number(row.get('units', 0)),
                orders=clean_number(row.get('orders', 0)),
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
            records[i:i + batch_size],
            **_get_upsert_kwargs(
                unique_fields=['user', 'date', 'asin'],
                update_fields=[
                    'portfolio', 'category', 'subcategory', 'price',
                    'pageviews', 'units', 'orders', 'revenue',
                    'spend_sp', 'spend_sb', 'spend_sd', 'total_spend'
                ]
            )
        )

    # Refresh materialized-view caches for all three dashboards
    from apps.dashboard.services.materialized_services import refresh_materialized_views
    try:
        refresh_materialized_views(user)
    except Exception as exc:
        print(f"[MaterializedViews] Cache refresh failed for user {user}: {exc}")


# ===========================================================================
# SLIM FLIPKART PROCESSING FUNCTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# FK Search Traffic Report
# ---------------------------------------------------------------------------

def process_fk_search_traffic(file_obj, user):
    """
    Parse Flipkart Search Traffic Report (.xlsx).
    Extracts FSN from Listing Id using Mid(Listing Id, 4, 16) → listing_id[3:19].
    Saves per-FSN per-date traffic & sales data.
    """
    df = pd.read_excel(file_obj)

    required_cols = ['Listing Id', 'SKU Id', 'Vertical', 'Impression Date',
                     'Product Clicks', 'Sales', 'Revenue']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK Search Traffic missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        listing_id = str(row.get('Listing Id', '')).strip()
        if not listing_id or listing_id.lower() == 'nan' or len(listing_id) < 19:
            continue

        fsn = listing_id[3:19]  # Mid(Listing Id, 4, 16)

        try:
            row_date = pd.to_datetime(row.get('Impression Date')).date()
        except Exception:
            continue

        # Page Views = Product Clicks column from Search Traffic
        page_views = clean_number(row.get('Product Clicks', 0))
        product_clicks = clean_number(row.get('Product Clicks', 0))
        sales_val = clean_number(row.get('Sales', 0))
        revenue_val = float(clean_currency(row.get('Revenue', 0)))
        sku = str(row.get('SKU Id', '') or '').strip()
        vertical = str(row.get('Vertical', '') or '').strip()

        records.append(
            FlipkartSearchTraffic(
                user=user, fsn=fsn, sku=sku, vertical=vertical,
                date=row_date, page_views=page_views,
                product_clicks=product_clicks, sales=sales_val,
                revenue=revenue_val,
            )
        )

    if records:
        batch_size = 10_000
        for i in range(0, len(records), batch_size):
            FlipkartSearchTraffic.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'fsn', 'date'],
                    update_fields=['sku', 'vertical', 'page_views',
                                   'product_clicks', 'sales', 'revenue']
                )
            )

    print(f"[FK SearchTraffic] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK Category Report
# ---------------------------------------------------------------------------

def process_fk_category(file_obj, user):
    """
    Parse Flipkart Category Dashboard (.xlsx).
    Columns: FSN ID, SKU, Portfolio, Cat, Subcat.
    """
    df = pd.read_excel(file_obj)

    required_cols = ['FSN ID', 'Portfolio', 'Cat', 'Subcat']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK Category missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        fsn = str(row.get('FSN ID', '')).strip()
        if not fsn or fsn.lower() == 'nan':
            continue

        records.append(
            FlipkartCategoryMap(
                user=user, fsn=fsn,
                sku=str(row.get('SKU', '') or '').strip(),
                portfolio=str(row.get('Portfolio', '') or '').strip(),
                category=str(row.get('Cat', '') or '').strip(),
                subcategory=str(row.get('Subcat', '') or '').strip(),
            )
        )

    if records:
        FlipkartCategoryMap.objects.bulk_create(
            records,
            **_get_upsert_kwargs(
                unique_fields=['user', 'fsn'],
                update_fields=['sku', 'portfolio', 'category', 'subcategory']
            )
        )

    print(f"[FK Category] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK Price Report
# ---------------------------------------------------------------------------

def process_fk_price(file_obj, user):
    """
    Parse Flipkart Price file (.xlsx).
    Columns: Flipkart Serial Number → fsn, Deal → price.
    """
    df = pd.read_excel(file_obj)

    required_cols = ['Flipkart Serial Number', 'Deal']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK Price missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        fsn = str(row.get('Flipkart Serial Number', '')).strip()
        if not fsn or fsn.lower() == 'nan':
            continue

        price_val = float(clean_currency(row.get('Deal', 0)))
        records.append(FlipkartPrice(user=user, fsn=fsn, price=price_val))

    if records:
        FlipkartPrice.objects.bulk_create(
            records,
            **_get_upsert_kwargs(
                unique_fields=['user', 'fsn'],
                update_fields=['price']
            )
        )

    print(f"[FK Price] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK PCA Attribution Report
# ---------------------------------------------------------------------------

def process_fk_pca(file_obj, user):
    """
    Parse Flipkart PCA Attribution (.csv).
    File has 2 metadata rows (Start Time, End Time) then the header row.
    Columns: campaign_id, campaign_name, Date, fsn_id.
    """
    df = pd.read_csv(file_obj, skiprows=2)

    required_cols = ['campaign_id', 'campaign_name', 'Date', 'fsn_id']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK PCA missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        campaign_id = str(row.get('campaign_id', '')).strip()
        fsn_id = str(row.get('fsn_id', '')).strip()
        if not campaign_id or campaign_id.lower() == 'nan':
            continue
        if not fsn_id or fsn_id.lower() == 'nan':
            continue

        try:
            row_date = pd.to_datetime(row.get('Date')).date()
        except Exception:
            row_date = None

        records.append(
            FlipkartPCA(
                user=user,
                campaign_id=campaign_id,
                campaign_name=str(row.get('campaign_name', '') or '').strip(),
                date=row_date,
                fsn_id=fsn_id,
            )
        )

    if records:
        FlipkartPCA.objects.bulk_create(
            records,
            **_get_upsert_kwargs(
                unique_fields=['user', 'campaign_id', 'fsn_id', 'date'],
                update_fields=['campaign_name']
            )
        )

    print(f"[FK PCA] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK PLA FSN Report
# ---------------------------------------------------------------------------

def process_fk_pla(file_obj, user):
    """
    Parse Flipkart PLA FSN Report (.csv).
    File has 2 metadata rows then the header row.
    Columns: Campaign ID, Advertised FSN ID, Ad Spend.
    """
    df = pd.read_csv(file_obj, skiprows=2)

    required_cols = ['Campaign ID', 'Advertised FSN ID', 'Ad Spend']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK PLA missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        campaign_id = str(row.get('Campaign ID', '')).strip()
        fsn_id = str(row.get('Advertised FSN ID', '')).strip()
        if not fsn_id or fsn_id.lower() == 'nan':
            continue

        records.append(
            FlipkartPLA(
                user=user,
                campaign_id=campaign_id,
                fsn_id=fsn_id,
                ad_spend=float(clean_currency(row.get('Ad Spend', 0))),
            )
        )

    if records:
        FlipkartPLA.objects.bulk_create(
            records,
            **_get_upsert_kwargs(
                unique_fields=['user', 'campaign_id', 'fsn_id'],
                update_fields=['ad_spend']
            )
        )

    print(f"[FK PLA] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK Sales Invoice (Sales Report file — both sheets)
# ---------------------------------------------------------------------------

def process_fk_sales_invoice(file_obj, user):
    """
    Parse Flipkart Sales Report (.xlsx).
    - From 'Sales Report' sheet: get Order Item ID → FSN + Item Quantity mapping
    - From 'Cash Back Report' sheet: get Taxable Value & Invoice Amount
    - Join by Order Item ID to attach FSN to Cash Back rows.
    """
    xl = pd.ExcelFile(file_obj)

    # --- Sales Report sheet: extract FSN + Item Quantity per Order Item ---
    fsn_map = {}
    if 'Sales Report' in xl.sheet_names:
        df_sales = pd.read_excel(xl, sheet_name='Sales Report')
        for _, row in df_sales.iterrows():
            oid = str(row.get('Order Item ID', '')).strip().replace('"', '')
            fsn = str(row.get('FSN', '')).strip().replace('"', '')
            qty = clean_number(row.get('Item Quantity', 0))
            if oid and oid.lower() != 'nan' and fsn and fsn.lower() != 'nan':
                fsn_map[oid] = {'fsn': fsn, 'qty': qty}

    # --- Cash Back Report sheet: taxable value & invoice amount ---
    records = []
    if 'Cash Back Report' in xl.sheet_names:
        df_cb = pd.read_excel(xl, sheet_name='Cash Back Report')

        for _, row in df_cb.iterrows():
            order_id = str(row.get('Order ID', '')).strip()
            order_item_id = str(row.get('Order Item ID', '')).strip()
            if not order_id or order_id.lower() == 'nan':
                continue

            # Look up FSN from the Sales Report sheet
            info = fsn_map.get(order_item_id, {})
            fsn = info.get('fsn', '')
            qty = info.get('qty', 0)

            records.append(
                FlipkartSalesInvoice(
                    user=user,
                    order_id=order_id,
                    order_item_id=order_item_id,
                    fsn=fsn,
                    item_quantity=qty,
                    taxable_value=float(clean_currency(row.get('Taxable Value', 0))),
                    invoice_amount=float(clean_currency(row.get('Invoice Amount', 0))),
                )
            )

    if records:
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            FlipkartSalesInvoice.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'order_id', 'order_item_id'],
                    update_fields=['fsn', 'item_quantity', 'taxable_value', 'invoice_amount']
                )
            )

    print(f"[FK SalesInvoice] Processed {len(records)} records.")


# ---------------------------------------------------------------------------
# FK Coupon Value Report
# ---------------------------------------------------------------------------

def process_fk_coupon(file_obj, user):
    """
    Parse Flipkart Coupon Value Report (.xlsx).
    File has 2 header rows to skip.
    Columns: Flipkart Serial Number → fsn, Coupon Value.
    """
    df = pd.read_excel(file_obj, skiprows=2)

    required_cols = ['Flipkart Serial Number', 'Coupon Value']
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"FK Coupon missing columns: {', '.join(missing)}")

    records = []
    for _, row in df.iterrows():
        fsn = str(row.get('Flipkart Serial Number', '')).strip()
        if not fsn or fsn.lower() == 'nan':
            continue

        records.append(
            FlipkartCoupon(
                user=user, fsn=fsn,
                coupon_value=float(clean_currency(row.get('Coupon Value', 0))),
            )
        )

    if records:
        FlipkartCoupon.objects.bulk_create(
            records,
            **_get_upsert_kwargs(
                unique_fields=['user', 'fsn'],
                update_fields=['coupon_value']
            )
        )

    print(f"[FK Coupon] Processed {len(records)} records.")


# ===========================================================================
# Flipkart Dashboard Aggregation
# ===========================================================================

def generate_flipkart_dashboard_data(user):
    """
    Merges all 7 slim Flipkart tables at FSN level into
    FlipkartProcessedDashboardData — the Flipkart equivalent of
    ProcessedDashboardData.

    Spend chain: PCA (fsn → campaign_id) → PLA (campaign_id + fsn → ad_spend)
    Coupon validation: coupon_value × item_quantity ≈ invoice_amount
    """
    FlipkartProcessedDashboardData.objects.filter(user=user).delete()

    # 1) Base: Search Traffic (FSN + date)
    traffic_qs = FlipkartSearchTraffic.objects.filter(user=user).values()
    if not traffic_qs:
        print("[FK Dashboard] No search traffic data — skipping.")
        return

    df_traffic = pd.DataFrame(list(traffic_qs))
    df_traffic = df_traffic[['fsn', 'sku', 'vertical', 'date',
                              'page_views', 'product_clicks', 'sales', 'revenue']]

    # 2) Category mapping
    cat_qs = FlipkartCategoryMap.objects.filter(user=user).values()
    df_cat = pd.DataFrame(list(cat_qs)) if cat_qs else pd.DataFrame()
    if not df_cat.empty:
        df_cat = df_cat[['fsn', 'portfolio', 'category', 'subcategory']]

    # 3) Price
    price_qs = FlipkartPrice.objects.filter(user=user).values()
    df_price = pd.DataFrame(list(price_qs)) if price_qs else pd.DataFrame()
    if not df_price.empty:
        df_price = df_price[['fsn', 'price']]

    # 4) Spend: PCA (fsn → campaign_id) + PLA (campaign_id + fsn → ad_spend)
    pca_qs = FlipkartPCA.objects.filter(user=user).values()
    pla_qs = FlipkartPLA.objects.filter(user=user).values()

    df_spend = pd.DataFrame(columns=['fsn', 'total_spend'])
    if pca_qs and pla_qs:
        df_pca = pd.DataFrame(list(pca_qs))[['campaign_id', 'fsn_id']]
        df_pla = pd.DataFrame(list(pla_qs))[['campaign_id', 'fsn_id', 'ad_spend']]

        # Join PCA with PLA on campaign_id + fsn_id
        df_spend_raw = pd.merge(
            df_pca, df_pla,
            on=['campaign_id', 'fsn_id'],
            how='inner'
        )
        if not df_spend_raw.empty:
            # Aggregate spend per FSN
            df_spend = (
                df_spend_raw
                .groupby('fsn_id')['ad_spend']
                .sum()
                .reset_index()
                .rename(columns={'fsn_id': 'fsn', 'ad_spend': 'total_spend'})
            )

    # 5) Sales Invoice (aggregated per FSN)
    inv_qs = FlipkartSalesInvoice.objects.filter(user=user).values()
    df_inv = pd.DataFrame(columns=['fsn', 'taxable_value', 'invoice_amount', 'item_quantity'])
    if inv_qs:
        df_inv_raw = pd.DataFrame(list(inv_qs))
        if not df_inv_raw.empty and 'fsn' in df_inv_raw.columns:
            df_inv_raw = df_inv_raw[df_inv_raw['fsn'].notna() & (df_inv_raw['fsn'] != '')]
            if not df_inv_raw.empty:
                df_inv = (
                    df_inv_raw
                    .groupby('fsn')
                    .agg({
                        'taxable_value': 'sum',
                        'invoice_amount': 'sum',
                        'item_quantity': 'sum',
                    })
                    .reset_index()
                )

    # 6) Coupon
    coupon_qs = FlipkartCoupon.objects.filter(user=user).values()
    df_coupon = pd.DataFrame(columns=['fsn', 'coupon_value'])
    if coupon_qs:
        df_coupon = pd.DataFrame(list(coupon_qs))
        if not df_coupon.empty:
            df_coupon = df_coupon[['fsn', 'coupon_value']]

    # --- Merge everything onto traffic base ---
    df = df_traffic.copy()

    if not df_cat.empty:
        df = pd.merge(df, df_cat, on='fsn', how='left')
    else:
        df['portfolio'] = ''
        df['category'] = ''
        df['subcategory'] = ''

    if not df_price.empty:
        df = pd.merge(df, df_price, on='fsn', how='left')
    else:
        df['price'] = 0.0

    if not df_spend.empty:
        df = pd.merge(df, df_spend, on='fsn', how='left')
    else:
        df['total_spend'] = 0.0

    if not df_inv.empty:
        df = pd.merge(df, df_inv, on='fsn', how='left')
    else:
        df['taxable_value'] = 0.0
        df['invoice_amount'] = 0.0
        df['item_quantity'] = 0

    if not df_coupon.empty:
        df = pd.merge(df, df_coupon, on='fsn', how='left')
    else:
        df['coupon_value'] = 0.0

    # Fill NaN
    fill = {
        'portfolio': '', 'category': '', 'subcategory': '',
        'price': 0.0, 'total_spend': 0.0,
        'taxable_value': 0.0, 'invoice_amount': 0.0,
        'item_quantity': 0, 'coupon_value': 0.0,
    }
    for col, val in fill.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    # Coupon validation: coupon_value × item_quantity ≈ invoice_amount
    df['coupon_total'] = df['coupon_value'] * df['item_quantity']
    df['coupon_error'] = False
    mask = (df['coupon_total'] > 0) & (df['invoice_amount'] > 0)
    df.loc[mask, 'coupon_error'] = ~(
        (df.loc[mask, 'coupon_total'] - df.loc[mask, 'invoice_amount']).abs()
        < 1.0  # tolerance of ₹1
    )

    # Build records
    records = []
    for row in df.to_dict('records'):
        records.append(
            FlipkartProcessedDashboardData(
                user=user,
                date=row['date'],
                fsn=row['fsn'],
                platform='Flipkart',
                portfolio=str(row.get('portfolio', '')) or '',
                category=str(row.get('category', '')) or '',
                subcategory=str(row.get('subcategory', '')) or '',
                price=float(row.get('price', 0)),
                pageviews=clean_number(row.get('page_views', 0)),
                units=clean_number(row.get('sales', 0)),
                orders=0,  # No order data for Flipkart
                revenue=float(row.get('revenue', 0)),
                total_spend=float(row.get('total_spend', 0)),
                spend_sp=0.0,
                spend_sb=0.0,
                spend_sd=0.0,
                taxable_value=float(row.get('taxable_value', 0)),
                invoice_amount=float(row.get('invoice_amount', 0)),
                coupon_total=float(row.get('coupon_total', 0)),
                coupon_error=bool(row.get('coupon_error', False)),
            )
        )

    batch_size = 10_000
    for i in range(0, len(records), batch_size):
        FlipkartProcessedDashboardData.objects.bulk_create(
            records[i:i + batch_size],
            **_get_upsert_kwargs(
                unique_fields=['user', 'date', 'fsn'],
                update_fields=[
                    'platform', 'portfolio', 'category', 'subcategory', 'price',
                    'pageviews', 'units', 'orders', 'revenue',
                    'total_spend', 'spend_sp', 'spend_sb', 'spend_sd',
                    'taxable_value', 'invoice_amount', 'coupon_total', 'coupon_error',
                ]
            )
        )

    print(f"[FK Dashboard] Generated {len(records)} processed records.")

    # Refresh materialized-view caches (with combined Amazon + Flipkart data)
    from apps.dashboard.services.materialized_services import refresh_materialized_views
    try:
        refresh_materialized_views(user)
    except Exception as exc:
        print(f"[MaterializedViews] Cache refresh failed for user {user}: {exc}")