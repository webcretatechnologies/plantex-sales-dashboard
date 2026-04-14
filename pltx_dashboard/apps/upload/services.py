import pandas as pd
import datetime

from apps.dashboard.utils import clean_currency, clean_number

from apps.dashboard.models import (
    SalesData, SpendData, CategoryMapping, PriceData, ProcessedDashboardData,
    FlipkartSalesReport, FlipkartCurrentInventoryReport, PCADailyReport, PLAFSNReport
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
# Flipkart Sales Report
# ---------------------------------------------------------------------------

def process_flipkart_sales_file(file_obj, user):
    """
    Upsert Flipkart Sales Report rows scoped to the user.
    Validates required columns before processing.
    """
    df = pd.read_excel(file_obj)

    required_cols = [
        'Seller GSTIN', 'Order ID', 'Order Item ID', 'Product Title/Description',
        'FSN', 'SKU', 'HSN Code', 'Event Type', 'Event Sub Type', 'Order Type',
        'Fulfilment Type', 'Order Date', 'Order Approval Date', 'Item Quantity',
        'Order Shipped From (State)', 'Warehouse ID', 'Price before discount',
        'Total Discount', 'Seller Share', 'Bank Offer Share',
        'Price after discount (Price before discount-Total discount)',
        'Shipping Charges',
        'Final Invoice Amount (Price after discount+Shipping Charges)',
        'Type of tax', 'Taxable Value (Final Invoice Amount -Taxes)',
        'CST Rate', 'CST Amount', 'VAT Rate', 'VAT Amount',
        'Luxury Cess Rate', 'Luxury Cess Amount', 'IGST Rate', 'IGST Amount',
        'CGST Rate', 'CGST Amount',
        'SGST Rate (or UTGST as applicable)', 'SGST Amount (Or UTGST as applicable)',
        'TCS IGST Rate', 'TCS IGST Amount', 'TCS CGST Rate', 'TCS CGST Amount',
        'TCS SGST Rate', 'TCS SGST Amount', 'Total TCS Deducted',
        'Buyer Invoice ID', 'Buyer Invoice Date', 'Buyer Invoice Amount',
        "Customer's Billing Pincode", "Customer's Billing State",
        "Customer's Delivery Pincode", "Customer's Delivery State",
        'Usual Price', 'Is Shopsy Order?', 'TDS Rate', 'TDS Amount',
        'IRN', 'Business Name', 'Business GST Number', 'Beneficiary Name', 'IMEI'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Flipkart Sales Report missing required columns: {', '.join(missing_cols)}")

    records = []
    for _, row in df.iterrows():
        order_id = str(row.get('Order ID', '')).strip()
        order_item_id = str(row.get('Order Item ID', '')).strip()
        if not order_id or order_id.lower() == 'nan':
            continue

        def parse_dt(val):
            try:
                return pd.to_datetime(val)
            except Exception:
                return None

        records.append(
            FlipkartSalesReport(
                user=user,
                seller_gstin=str(row.get('Seller GSTIN', '') or '').strip(),
                order_id=order_id,
                order_item_id=order_item_id,
                product_title_description=str(row.get('Product Title/Description', '') or '').strip()[:500],
                fsn=str(row.get('FSN', '') or '').strip(),
                sku=str(row.get('SKU', '') or '').strip(),
                hsn_code=str(row.get('HSN Code', '') or '').strip(),
                event_type=str(row.get('Event Type', '') or '').strip(),
                event_sub_type=str(row.get('Event Sub Type', '') or '').strip(),
                order_type=str(row.get('Order Type', '') or '').strip(),
                fulfilment_type=str(row.get('Fulfilment Type', '') or '').strip(),
                order_date=parse_dt(row.get('Order Date')),
                order_approval_date=parse_dt(row.get('Order Approval Date')),
                item_quantity=clean_number(row.get('Item Quantity', 0)),
                order_shipped_from_state=str(row.get('Order Shipped From (State)', '') or '').strip(),
                warehouse_id=str(row.get('Warehouse ID', '') or '').strip(),
                price_before_discount=float(clean_currency(row.get('Price before discount', 0))),
                total_discount=float(clean_currency(row.get('Total Discount', 0))),
                seller_share=float(clean_currency(row.get('Seller Share', 0))),
                bank_offer_share=float(clean_currency(row.get('Bank Offer Share', 0))),
                price_after_discount=float(clean_currency(row.get('Price after discount (Price before discount-Total discount)', 0))),
                shipping_charges=float(clean_currency(row.get('Shipping Charges', 0))),
                final_invoice_amount=float(clean_currency(row.get('Final Invoice Amount (Price after discount+Shipping Charges)', 0))),
                type_of_tax=str(row.get('Type of tax', '') or '').strip(),
                taxable_value=float(clean_currency(row.get('Taxable Value (Final Invoice Amount -Taxes)', 0))),
                cst_rate=float(clean_currency(row.get('CST Rate', 0))),
                cst_amount=float(clean_currency(row.get('CST Amount', 0))),
                vat_rate=float(clean_currency(row.get('VAT Rate', 0))),
                vat_amount=float(clean_currency(row.get('VAT Amount', 0))),
                luxury_cess_rate=float(clean_currency(row.get('Luxury Cess Rate', 0))),
                luxury_cess_amount=float(clean_currency(row.get('Luxury Cess Amount', 0))),
                igst_rate=float(clean_currency(row.get('IGST Rate', 0))),
                igst_amount=float(clean_currency(row.get('IGST Amount', 0))),
                cgst_rate=float(clean_currency(row.get('CGST Rate', 0))),
                cgst_amount=float(clean_currency(row.get('CGST Amount', 0))),
                sgst_rate=float(clean_currency(row.get('SGST Rate (or UTGST as applicable)', 0))),
                sgst_amount=float(clean_currency(row.get('SGST Amount (Or UTGST as applicable)', 0))),
                tcs_igst_rate=float(clean_currency(row.get('TCS IGST Rate', 0))),
                tcs_igst_amount=float(clean_currency(row.get('TCS IGST Amount', 0))),
                tcs_cgst_rate=float(clean_currency(row.get('TCS CGST Rate', 0))),
                tcs_cgst_amount=float(clean_currency(row.get('TCS CGST Amount', 0))),
                tcs_sgst_rate=float(clean_currency(row.get('TCS SGST Rate', 0))),
                tcs_sgst_amount=float(clean_currency(row.get('TCS SGST Amount', 0))),
                total_tcs_deducted=float(clean_currency(row.get('Total TCS Deducted', 0))),
                buyer_invoice_id=str(row.get('Buyer Invoice ID', '') or '').strip(),
                buyer_invoice_date=parse_dt(row.get('Buyer Invoice Date')),
                buyer_invoice_amount=float(clean_currency(row.get('Buyer Invoice Amount', 0))),
                customer_billing_pincode=str(row.get("Customer's Billing Pincode", '') or '').strip(),
                customer_billing_state=str(row.get("Customer's Billing State", '') or '').strip(),
                customer_delivery_pincode=str(row.get("Customer's Delivery Pincode", '') or '').strip(),
                customer_delivery_state=str(row.get("Customer's Delivery State", '') or '').strip(),
                usual_price=float(clean_currency(row.get('Usual Price', 0))),
                is_shopsy_order=str(row.get('Is Shopsy Order?', '') or '').strip(),
                tds_rate=float(clean_currency(row.get('TDS Rate', 0))),
                tds_amount=float(clean_currency(row.get('TDS Amount', 0))),
                irn=str(row.get('IRN', '') or '').strip(),
                business_name=str(row.get('Business Name', '') or '').strip(),
                business_gst_number=str(row.get('Business GST Number', '') or '').strip(),
                beneficiary_name=str(row.get('Beneficiary Name', '') or '').strip(),
                imei=str(row.get('IMEI', '') or '').strip(),
            )
        )

    if records:
        update_fields = [
            'seller_gstin', 'product_title_description', 'fsn', 'sku', 'hsn_code',
            'event_type', 'event_sub_type', 'order_type', 'fulfilment_type',
            'order_date', 'order_approval_date', 'item_quantity',
            'order_shipped_from_state', 'warehouse_id', 'price_before_discount',
            'total_discount', 'seller_share', 'bank_offer_share',
            'price_after_discount', 'shipping_charges', 'final_invoice_amount',
            'type_of_tax', 'taxable_value', 'cst_rate', 'cst_amount',
            'vat_rate', 'vat_amount', 'luxury_cess_rate', 'luxury_cess_amount',
            'igst_rate', 'igst_amount', 'cgst_rate', 'cgst_amount',
            'sgst_rate', 'sgst_amount', 'tcs_igst_rate', 'tcs_igst_amount',
            'tcs_cgst_rate', 'tcs_cgst_amount', 'tcs_sgst_rate', 'tcs_sgst_amount',
            'total_tcs_deducted', 'buyer_invoice_id', 'buyer_invoice_date',
            'buyer_invoice_amount', 'customer_billing_pincode', 'customer_billing_state',
            'customer_delivery_pincode', 'customer_delivery_state', 'usual_price',
            'is_shopsy_order', 'tds_rate', 'tds_amount', 'irn', 'business_name',
            'business_gst_number', 'beneficiary_name', 'imei',
        ]
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            FlipkartSalesReport.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'order_id', 'order_item_id'],
                    update_fields=update_fields
                )
            )

    print(f"[FlipkartSalesReport] Processed and upserted {len(records)} records.")


# ---------------------------------------------------------------------------
# Flipkart Current Inventory Report
# ---------------------------------------------------------------------------

def process_flipkart_inventory_file(file_obj, user):
    """
    Upsert Flipkart Current Inventory Report rows scoped to the user.
    Validates required columns before processing.
    """
    df = pd.read_csv(file_obj)

    required_cols = [
        'Warehouse Id', 'SKU', 'Title', 'Listing Id', 'FSN', 'Brand',
        'Flipkart Selling Price', 'Live on Website',
        'Sales 7D', 'Sales 14D', 'Sales 30D', 'Sales 60D', 'Sales 90D',
        'B2B Scheduled', 'Transfers Scheduled', 'B2B Shipped', 'Transfers Shipped',
        'B2B Receiving', 'Transfers Receiving',
        'Reserved for Orders and Recalls', 'Reserved for Internal Processing',
        'Returns Processing', 'Orders to Dispatch', 'Recalls to Dispatch',
        'Damaged', 'QC Reject', 'Catalog Reject', 'Returns Reject',
        'Seller Return Reject', 'Miscellaneous',
        'Length (in cm)', 'Breadth (in cm)', 'Height (in cm)', 'Weight (in kg)',
        'Fulfilment Type', 'F Assured Badge'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Flipkart Inventory Report missing required columns: {', '.join(missing_cols)}")

    records = []
    for _, row in df.iterrows():
        sku = str(row.get('SKU', '')).strip()
        if not sku or sku.lower() == 'nan':
            continue

        records.append(
            FlipkartCurrentInventoryReport(
                user=user,
                warehouse_id=str(row.get('Warehouse Id', '') or '').strip(),
                sku=sku,
                title=str(row.get('Title', '') or '').strip()[:255],
                listing_id=str(row.get('Listing Id', '') or '').strip(),
                fsn=str(row.get('FSN', '') or '').strip(),
                brand=str(row.get('Brand', '') or '').strip(),
                flipkart_selling_price=float(clean_currency(row.get('Flipkart Selling Price', 0))),
                live_on_website=str(row.get('Live on Website', '') or '').strip(),
                sales_7d=clean_number(row.get('Sales 7D', 0)),
                sales_14d=clean_number(row.get('Sales 14D', 0)),
                sales_30d=clean_number(row.get('Sales 30D', 0)),
                sales_60d=clean_number(row.get('Sales 60D', 0)),
                sales_90d=clean_number(row.get('Sales 90D', 0)),
                b2b_scheduled=clean_number(row.get('B2B Scheduled', 0)),
                transfers_scheduled=clean_number(row.get('Transfers Scheduled', 0)),
                b2b_shipped=clean_number(row.get('B2B Shipped', 0)),
                transfers_shipped=clean_number(row.get('Transfers Shipped', 0)),
                b2b_receiving=clean_number(row.get('B2B Receiving', 0)),
                transfers_receiving=clean_number(row.get('Transfers Receiving', 0)),
                reserved_for_orders_and_recalls=clean_number(row.get('Reserved for Orders and Recalls', 0)),
                reserved_for_internal_processing=clean_number(row.get('Reserved for Internal Processing', 0)),
                returns_processing=clean_number(row.get('Returns Processing', 0)),
                orders_to_dispatch=clean_number(row.get('Orders to Dispatch', 0)),
                recalls_to_dispatch=clean_number(row.get('Recalls to Dispatch', 0)),
                damaged=clean_number(row.get('Damaged', 0)),
                qc_reject=clean_number(row.get('QC Reject', 0)),
                catalog_reject=clean_number(row.get('Catalog Reject', 0)),
                returns_reject=clean_number(row.get('Returns Reject', 0)),
                seller_return_reject=clean_number(row.get('Seller Return Reject', 0)),
                miscellaneous=clean_number(row.get('Miscellaneous', 0)),
                length_in_cm=float(clean_currency(row.get('Length (in cm)', 0))),
                breadth_in_cm=float(clean_currency(row.get('Breadth (in cm)', 0))),
                height_in_cm=float(clean_currency(row.get('Height (in cm)', 0))),
                weight_in_kg=float(clean_currency(row.get('Weight (in kg)', 0))),
                fulfilment_type=str(row.get('Fulfilment Type', '') or '').strip(),
                f_assured_badge=str(row.get('F Assured Badge', '') or '').strip(),
            )
        )

    if records:
        update_fields = [
            'title', 'listing_id', 'fsn', 'brand', 'flipkart_selling_price',
            'live_on_website', 'sales_7d', 'sales_14d', 'sales_30d', 'sales_60d',
            'sales_90d', 'b2b_scheduled', 'transfers_scheduled', 'b2b_shipped',
            'transfers_shipped', 'b2b_receiving', 'transfers_receiving',
            'reserved_for_orders_and_recalls', 'reserved_for_internal_processing',
            'returns_processing', 'orders_to_dispatch', 'recalls_to_dispatch',
            'damaged', 'qc_reject', 'catalog_reject', 'returns_reject',
            'seller_return_reject', 'miscellaneous', 'length_in_cm', 'breadth_in_cm',
            'height_in_cm', 'weight_in_kg', 'fulfilment_type', 'f_assured_badge',
        ]
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            FlipkartCurrentInventoryReport.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'warehouse_id', 'sku'],
                    update_fields=update_fields
                )
            )

    print(f"[FlipkartInventory] Processed and upserted {len(records)} records.")


# ---------------------------------------------------------------------------
# Flipkart PCA Daily Report
# ---------------------------------------------------------------------------

def process_flipkart_pca_file(file_obj, user):
    """
    Upsert Flipkart PCA Daily Report rows scoped to the user.
    Validates required columns before processing.
    """
    df = pd.read_excel(file_obj)

    required_cols = [
        'Start Time', 'End Time', 'campaign_id', 'campaign_name',
        'ad_group_id', 'ad_group_name', 'Date', 'banner_group_spend',
        'views', 'clicks', 'CTR', 'average_cpc', 'DIRECT PPV',
        'DIRECT UNITS', 'INDIRECT UNITS', 'CVR', 'DIRECT REVENUE',
        'INDIRECT REVENUE', 'Direct ROI', 'Indirect ROI'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Flipkart PCA Report missing required columns: {', '.join(missing_cols)}")

    records = []
    for _, row in df.iterrows():
        campaign_id = str(row.get('campaign_id', '')).strip()
        ad_group_id = str(row.get('ad_group_id', '')).strip()
        if not campaign_id or campaign_id.lower() == 'nan':
            continue

        def parse_dt(val):
            try:
                return pd.to_datetime(val)
            except Exception:
                return None

        try:
            row_date = pd.to_datetime(row.get('Date')).date()
        except Exception:
            row_date = None

        records.append(
            PCADailyReport(
                user=user,
                start_time=parse_dt(row.get('Start Time')),
                end_time=parse_dt(row.get('End Time')),
                campaign_id=campaign_id,
                campaign_name=str(row.get('campaign_name', '') or '').strip(),
                ad_group_id=ad_group_id,
                ad_group_name=str(row.get('ad_group_name', '') or '').strip(),
                date=row_date,
                banner_group_spend=float(clean_currency(row.get('banner_group_spend', 0))),
                views=clean_number(row.get('views', 0)),
                clicks=clean_number(row.get('clicks', 0)),
                ctr=float(clean_currency(row.get('CTR', 0))),
                average_cpc=float(clean_currency(row.get('average_cpc', 0))),
                direct_ppv=float(clean_currency(row.get('DIRECT PPV', 0))),
                direct_units=clean_number(row.get('DIRECT UNITS', 0)),
                indirect_units=clean_number(row.get('INDIRECT UNITS', 0)),
                cvr=float(clean_currency(row.get('CVR', 0))),
                direct_revenue=float(clean_currency(row.get('DIRECT REVENUE', 0))),
                indirect_revenue=float(clean_currency(row.get('INDIRECT REVENUE', 0))),
                direct_roi=float(clean_currency(row.get('Direct ROI', 0))),
                indirect_roi=float(clean_currency(row.get('Indirect ROI', 0))),
            )
        )

    if records:
        update_fields = [
            'start_time', 'end_time', 'campaign_name', 'ad_group_name',
            'banner_group_spend', 'views', 'clicks', 'ctr', 'average_cpc',
            'direct_ppv', 'direct_units', 'indirect_units', 'cvr',
            'direct_revenue', 'indirect_revenue', 'direct_roi', 'indirect_roi',
        ]
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            PCADailyReport.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'campaign_id', 'ad_group_id', 'date'],
                    update_fields=update_fields
                )
            )

    print(f"[PCADailyReport] Processed and upserted {len(records)} records.")


# ---------------------------------------------------------------------------
# Flipkart PLA FSN Report
# ---------------------------------------------------------------------------

def process_flipkart_pla_file(file_obj, user):
    """
    Upsert Flipkart PLA FSN Report rows scoped to the user.
    Validates required columns before processing.
    """
    df = pd.read_excel(file_obj)

    required_cols = [
        'Start Time', 'End Time', 'Campaign ID', 'Campaign Name',
        'Ad Group ID', 'AdGroup Name', 'Advertised FSN ID',
        'Advertised Product Name', 'Views', 'Clicks', 'CTR', 'CVR',
        'Ad Spend', 'Units Sold (Direct)', 'Units Sold (Indirect)',
        'Direct Revenue', 'Indirect Revenue', 'ROI (Direct)', 'ROI (Indirect)'
    ]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Flipkart PLA Report missing required columns: {', '.join(missing_cols)}")

    records = []
    for _, row in df.iterrows():
        fsn_id = str(row.get('Advertised FSN ID', '')).strip()
        campaign_id = str(row.get('Campaign ID', '')).strip()
        ad_group_id = str(row.get('Ad Group ID', '')).strip()
        if not fsn_id or fsn_id.lower() == 'nan':
            continue

        def parse_dt(val):
            try:
                return pd.to_datetime(val)
            except Exception:
                return None

        records.append(
            PLAFSNReport(
                user=user,
                start_time=parse_dt(row.get('Start Time')),
                end_time=parse_dt(row.get('End Time')),
                campaign_id=campaign_id,
                campaign_name=str(row.get('Campaign Name', '') or '').strip(),
                ad_group_id=ad_group_id,
                ad_group_name=str(row.get('AdGroup Name', '') or '').strip(),
                advertised_fsn_id=fsn_id,
                advertised_product_name=str(row.get('Advertised Product Name', '') or '').strip(),
                views=clean_number(row.get('Views', 0)),
                clicks=clean_number(row.get('Clicks', 0)),
                ctr=float(clean_currency(row.get('CTR', 0))),
                cvr=float(clean_currency(row.get('CVR', 0))),
                ad_spend=float(clean_currency(row.get('Ad Spend', 0))),
                units_sold_direct=clean_number(row.get('Units Sold (Direct)', 0)),
                units_sold_indirect=clean_number(row.get('Units Sold (Indirect)', 0)),
                direct_revenue=float(clean_currency(row.get('Direct Revenue', 0))),
                indirect_revenue=float(clean_currency(row.get('Indirect Revenue', 0))),
                roi_direct=float(clean_currency(row.get('ROI (Direct)', 0))),
                roi_indirect=float(clean_currency(row.get('ROI (Indirect)', 0))),
            )
        )

    if records:
        update_fields = [
            'start_time', 'end_time', 'campaign_name', 'ad_group_name',
            'advertised_product_name', 'views', 'clicks', 'ctr', 'cvr',
            'ad_spend', 'units_sold_direct', 'units_sold_indirect',
            'direct_revenue', 'indirect_revenue', 'roi_direct', 'roi_indirect',
        ]
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            PLAFSNReport.objects.bulk_create(
                records[i:i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=['user', 'campaign_id', 'ad_group_id', 'advertised_fsn_id'],
                    update_fields=update_fields
                )
            )

    print(f"[PLAFSNReport] Processed and upserted {len(records)} records.")


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