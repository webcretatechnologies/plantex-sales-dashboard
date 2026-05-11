import pandas as pd
import datetime
import re

from apps.dashboard.utils import clean_currency, clean_number

from apps.dashboard.models import (
    SalesData,
    SpendData,
    CategoryMapping,
    PriceData,
    FBAStockData,
    FlexStockData,
    ProcessedDashboardData,
    # Slim Flipkart models
    FlipkartSearchTraffic,
    FlipkartCategoryMap,
    FlipkartPrice,
    FlipkartPCA,
    FlipkartPLA,
    FlipkartSalesInvoice,
    FlipkartCoupon,
    FlipkartProcessedDashboardData,
)
from django.db import connection


def _get_upsert_kwargs(unique_fields, update_fields):
    kwargs = {"update_conflicts": True, "update_fields": update_fields}
    if connection.vendor != "mysql":
        kwargs["unique_fields"] = unique_fields
    return kwargs


def load_file_obj(file_obj, **kwargs):
    filename = getattr(file_obj, "name", "").lower()
    if filename.endswith(".csv"):
        try:
            return pd.read_csv(file_obj, **kwargs)
        except UnicodeDecodeError:
            file_obj.seek(0)
            return pd.read_csv(file_obj, encoding="latin1", **kwargs)
    else:
        try:
            return pd.read_excel(file_obj, **kwargs)
        except Exception:
            file_obj.seek(0)
            return pd.read_excel(file_obj, engine="openpyxl", **kwargs)


CSV_CHUNK_SIZE = 20_000
DB_BATCH_SIZE = 10_000


def iter_file_chunks(file_obj, **kwargs):
    filename = getattr(file_obj, "name", "").lower()
    if filename.endswith(".csv"):
        read_kwargs = dict(kwargs)
        read_kwargs["chunksize"] = read_kwargs.get("chunksize", CSV_CHUNK_SIZE)
        try:
            yield from pd.read_csv(file_obj, **read_kwargs)
        except UnicodeDecodeError:
            file_obj.seek(0)
            yield from pd.read_csv(file_obj, encoding="latin1", **read_kwargs)
    else:
        yield load_file_obj(file_obj, **kwargs)


def _parse_numeric_report_date(value):
    """
    Parse numeric date cells robustly:
    - Excel serial days (e.g. 45816)
    - Unix timestamps (seconds / milliseconds)
    - YYYYMMDD integers
    """
    try:
        num = float(value)
    except (TypeError, ValueError):
        return None

    if pd.isna(num) or num <= 0:
        return None

    as_int = int(round(num))

    # YYYYMMDD
    if abs(num - as_int) < 1e-9 and 19000101 <= as_int <= 21001231:
        dt = pd.to_datetime(str(as_int), format="%Y%m%d", errors="coerce")
        if not pd.isna(dt):
            return dt.date()

    # Excel serial date (days since 1899-12-30)
    if 10_000 <= num <= 90_000:
        dt = pd.to_datetime(num, unit="D", origin="1899-12-30", errors="coerce")
        if not pd.isna(dt):
            return dt.date()

    # Unix timestamp
    if num >= 1_000_000_000_000:  # ms
        dt = pd.to_datetime(num, unit="ms", origin="unix", errors="coerce")
        if not pd.isna(dt):
            return dt.date()
    elif num >= 1_000_000_000:  # sec
        dt = pd.to_datetime(num, unit="s", origin="unix", errors="coerce")
        if not pd.isna(dt):
            return dt.date()

    return None


def parse_report_date(value, prefer_dayfirst=None):
    """
    Parse date values from CSV/XLSX cells with flexible handling for
    text dates, datetime values, and Excel numeric serials.
    """
    if pd.isna(value):
        raise ValueError("empty date")

    if isinstance(value, datetime.datetime):
        return value.date()
    if isinstance(value, datetime.date):
        return value

    # Numeric types from Excel/CSV
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        parsed = _parse_numeric_report_date(value)
        if parsed:
            return parsed
        raise ValueError(f"unsupported numeric date: {value}")

    raw = str(value).strip()
    if not raw:
        raise ValueError("empty date")

    # Numeric-looking strings
    raw_num = raw.replace(",", "")
    parsed = _parse_numeric_report_date(raw_num)
    if parsed:
        return parsed

    # Ambiguous numeric-string dates can be interpreted with an explicit
    # preference from the caller.
    if re.fullmatch(r"\d{1,2}[-/]\d{1,2}[-/]\d{4}", raw):
        if prefer_dayfirst is True:
            fmts = ("%d-%m-%Y", "%d/%m/%Y", "%m-%d-%Y", "%m/%d/%Y")
        elif prefer_dayfirst is False:
            fmts = ("%m-%d-%Y", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y")
        else:
            fmts = ("%m-%d-%Y", "%m/%d/%Y", "%d-%m-%Y", "%d/%m/%Y")
        for fmt in fmts:
            try:
                return datetime.datetime.strptime(raw, fmt).date()
            except ValueError:
                pass

    dt = pd.to_datetime(raw, errors="coerce", dayfirst=False)
    if pd.isna(dt):
        dt = pd.to_datetime(raw, errors="coerce", dayfirst=True)
    if pd.isna(dt):
        raise ValueError(f"unparseable date: {value}")
    return dt.date()


# ---------------------------------------------------------------------------
# Category
# ---------------------------------------------------------------------------


def process_category_file(file_obj, user):
    """
    Upsert category mappings scoped to the given user.
    - Uses bulk_create with update_conflicts to elegantly update existing records.
    """
    required_cols = ["ASIN", "Portfolio", "Category", "Subcategory", "Skus"]
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Category Mapping missing required columns: {', '.join(missing_cols)}"
            )

        new_mappings = []
        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            new_mappings.append(
                CategoryMapping(
                    user=user,
                    asin=asin,
                    portfolio=str(row.get("Portfolio", "")).strip(),
                    category=str(row.get("Category", "")).strip(),
                    subcategory=str(row.get("Subcategory", "")).strip(),
                )
            )

        if new_mappings:
            CategoryMapping.objects.bulk_create(
                new_mappings,
                **_get_upsert_kwargs(
                    unique_fields=["user", "asin"],
                    update_fields=["portfolio", "category", "subcategory"],
                ),
            )

    if not any_chunk:
        raise ValueError("Category Mapping file is empty.")


# ---------------------------------------------------------------------------
# Price
# ---------------------------------------------------------------------------


def process_price_file(file_obj, user):
    """
    Upsert price data scoped to the given user.
    - Uses bulk_create with update_conflicts to smartly update existing values.
    """
    required_cols = ["ASIN", "Price"]
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Pricing Data missing required columns: {', '.join(missing_cols)}"
            )

        new_prices = []
        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue
            new_prices.append(
                PriceData(user=user, asin=asin, price=clean_currency(row.get("Price", 0)))
            )

        if new_prices:
            PriceData.objects.bulk_create(
                new_prices,
                **_get_upsert_kwargs(
                    unique_fields=["user", "asin"], update_fields=["price"]
                ),
            )

    if not any_chunk:
        raise ValueError("Pricing Data file is empty.")


# ---------------------------------------------------------------------------
# Spend
# ---------------------------------------------------------------------------


def process_spend_file(file_obj, user):
    """
    Insert spend rows scoped to the user, or update existing records directly if they already exist.
    """
    required_cols = ["Date", "Ad Account", "Ad Type", "ASIN", "Spend"]
    total_spends = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Ads Spends missing required columns: {', '.join(missing_cols)}"
            )

        new_spends = []
        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            try:
                row_date = pd.to_datetime(row.get("Date")).date()
            except Exception:
                continue

            ad_type = str(row.get("Ad Type", "")).strip().upper()
            if ad_type in ("SPONSORED PRODUCTS", "SP"):
                ad_type = "SP"
            elif ad_type in ("SPONSORED BRANDS", "SB"):
                ad_type = "SB"
            elif ad_type in ("SPONSORED DISPLAY", "SD"):
                ad_type = "SD"
            else:
                ad_type = ad_type[:10]

            new_spends.append(
                SpendData(
                    user=user,
                    date=row_date,
                    asin=asin,
                    ad_account=str(row.get("Ad Account", "")).strip(),
                    ad_type=ad_type,
                    spend=clean_currency(row.get("Spend", 0)),
                )
            )

        total_spends += len(new_spends)
        if new_spends:
            for i in range(0, len(new_spends), DB_BATCH_SIZE):
                SpendData.objects.bulk_create(
                    new_spends[i : i + DB_BATCH_SIZE],
                    **_get_upsert_kwargs(
                        unique_fields=["user", "date", "asin", "ad_account", "ad_type"],
                        update_fields=["spend"],
                    ),
                )

    if not any_chunk:
        raise ValueError("Ads Spends file is empty.")

    print(
        f"[SpendData] Processed and upserted bulk batch of {total_spends} records."
    )


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------


def process_sales_file(file_obj, date_str, user):
    """
    Insert sales rows scoped to the user, or update existing records directly if they already exist.
    """
    try:
        date_obj = datetime.datetime.strptime(date_str, "%d-%m-%Y").date()
    except ValueError:
        raise ValueError(
            f"Invalid Date format '{date_str}' in Daily Sales filename. Please strictly use DD-MM-YYYY.csv format."
        )

    required_cols = [
        "(Child) ASIN",
        "Page Views - Total",
        "Units Ordered",
        "Ordered Product Sales",
        "Total Order Items",
    ]
    total_sales = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise ValueError(
                f"Daily Sales missing required columns: {', '.join(missing_cols)}"
            )

        new_sales = []
        for row in df.to_dict("records"):
            asin = str(row.get("(Child) ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            new_sales.append(
                SalesData(
                    user=user,
                    date=date_obj,
                    asin=asin,
                    pageviews=clean_number(row.get("Page Views - Total", 0)),
                    units=clean_number(row.get("Units Ordered", 0)),
                    orders=clean_number(row.get("Total Order Items", 0)),
                    revenue=float(clean_currency(row.get("Ordered Product Sales", 0))),
                )
            )

        total_sales += len(new_sales)
        if new_sales:
            for i in range(0, len(new_sales), DB_BATCH_SIZE):
                SalesData.objects.bulk_create(
                    new_sales[i : i + DB_BATCH_SIZE],
                    **_get_upsert_kwargs(
                        unique_fields=["user", "date", "asin"],
                        update_fields=["pageviews", "units", "orders", "revenue"],
                    ),
                )

    if not any_chunk:
        raise ValueError("Daily Sales file is empty.")

    print(
        f"[SalesData] date={date_obj}, Processed and upserted bulk batch of {total_sales} records."
    )


# ---------------------------------------------------------------------------
# FBA Stock
# ---------------------------------------------------------------------------


def process_fba_stock_file(file_obj, user):
    """
    Parse Amazon FBA Stock file.
    Required columns: Date, FNSKU, ASIN, MSKU, Title, Disposition,
    Starting Warehouse Balance, In Transit Between Warehouses, Receipts,
    Customer Shipments, Customer Returns, Vendor Returns,
    Warehouse Transfer In/Out, Found, Lost, Damaged, Disposed,
    Other Events, Ending Warehouse Balance, Unknown Events, Location.
    """
    required_cols = [
        "Date",
        "FNSKU",
        "ASIN",
        "MSKU",
        "Title",
        "Disposition",
        "Starting Warehouse Balance",
        "In Transit Between Warehouses",
        "Receipts",
        "Customer Shipments",
        "Customer Returns",
        "Vendor Returns",
        "Warehouse Transfer In/Out",
        "Found",
        "Lost",
        "Damaged",
        "Disposed",
        "Other Events",
        "Ending Warehouse Balance",
        "Unknown Events",
        "Location",
    ]
    total_records = 0
    any_chunk = False
    row_number = 1
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        col_lookup = {}
        for c in df.columns:
            key = str(c).replace("\ufeff", "").strip().lower()
            if key and key not in col_lookup:
                col_lookup[key] = c

        missing_cols = [c for c in required_cols if c.lower() not in col_lookup]
        if missing_cols:
            raise ValueError(
                f"FBA Stock file missing required columns: {', '.join(missing_cols)}"
            )

        records = []
        for row in df.to_dict("records"):
            row_number += 1
            asin = str(row.get(col_lookup["asin"], "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            try:
                row_date = parse_report_date(
                    row.get(col_lookup["date"]), prefer_dayfirst=False
                )
            except Exception as exc:
                raise ValueError(
                    f"Invalid Date value in FBA Stock at row {row_number}: {exc}"
                )

            records.append(
                FBAStockData(
                    user=user,
                    date=row_date,
                    fnsku=str(row.get(col_lookup["fnsku"], "") or "").strip(),
                    asin=asin,
                    msku=str(row.get(col_lookup["msku"], "") or "").strip(),
                    title=str(row.get(col_lookup["title"], "") or "").strip()[:500],
                    disposition=str(row.get(col_lookup["disposition"], "") or "").strip(),
                    starting_warehouse_balance=clean_number(
                        row.get(col_lookup["starting warehouse balance"], 0)
                    ),
                    in_transit_between_warehouses=clean_number(
                        row.get(col_lookup["in transit between warehouses"], 0)
                    ),
                    receipts=clean_number(row.get(col_lookup["receipts"], 0)),
                    customer_shipments=clean_number(
                        row.get(col_lookup["customer shipments"], 0)
                    ),
                    customer_returns=clean_number(
                        row.get(col_lookup["customer returns"], 0)
                    ),
                    vendor_returns=clean_number(row.get(col_lookup["vendor returns"], 0)),
                    warehouse_transfer_in_out=clean_number(
                        row.get(col_lookup["warehouse transfer in/out"], 0)
                    ),
                    found=clean_number(row.get(col_lookup["found"], 0)),
                    lost=clean_number(row.get(col_lookup["lost"], 0)),
                    damaged=clean_number(row.get(col_lookup["damaged"], 0)),
                    disposed=clean_number(row.get(col_lookup["disposed"], 0)),
                    other_events=clean_number(row.get(col_lookup["other events"], 0)),
                    ending_warehouse_balance=clean_number(
                        row.get(col_lookup["ending warehouse balance"], 0)
                    ),
                    unknown_events=clean_number(
                        row.get(col_lookup["unknown events"], 0)
                    ),
                    location=str(row.get(col_lookup["location"], "") or "").strip(),
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FBAStockData.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **_get_upsert_kwargs(
                        unique_fields=["user", "asin", "date", "disposition", "location"],
                        update_fields=[
                            "fnsku",
                            "msku",
                            "title",
                            "starting_warehouse_balance",
                            "in_transit_between_warehouses",
                            "receipts",
                            "customer_shipments",
                            "customer_returns",
                            "vendor_returns",
                            "warehouse_transfer_in_out",
                            "found",
                            "lost",
                            "damaged",
                            "disposed",
                            "other_events",
                            "ending_warehouse_balance",
                            "unknown_events",
                        ],
                    ),
                )

    if not any_chunk:
        raise ValueError("FBA Stock file is empty.")

    print(f"[FBAStockData] Processed {total_records} records.")


# ---------------------------------------------------------------------------
# Flex Stock
# ---------------------------------------------------------------------------


def process_flex_stock_file(file_obj, user):
    """
    Parse Amazon Flex Stock file.
    Required columns: Date, ASIN, Cluster, Qty.
    """
    required_cols = ["Date", "ASIN", "Cluster", "Qty"]
    total_records = 0
    any_chunk = False
    row_number = 1
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        col_lookup = {}
        for c in df.columns:
            key = str(c).replace("\ufeff", "").strip().lower()
            if key and key not in col_lookup:
                col_lookup[key] = c

        missing_cols = [c for c in required_cols if c.lower() not in col_lookup]
        if missing_cols:
            raise ValueError(
                f"Flex Stock file missing required columns: {', '.join(missing_cols)}"
            )

        records = []
        for row in df.to_dict("records"):
            row_number += 1
            asin = str(row.get(col_lookup["asin"], "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            try:
                row_date = parse_report_date(
                    row.get(col_lookup["date"]), prefer_dayfirst=False
                )
            except Exception as exc:
                raise ValueError(
                    f"Invalid Date value in Flex Stock at row {row_number}: {exc}"
                )

            records.append(
                FlexStockData(
                    user=user,
                    date=row_date,
                    asin=asin,
                    cluster=str(row.get(col_lookup["cluster"], "") or "").strip(),
                    qty=clean_number(row.get(col_lookup["qty"], 0)),
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FlexStockData.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **_get_upsert_kwargs(
                        unique_fields=["user", "asin", "date", "cluster"],
                        update_fields=["qty"],
                    ),
                )

    if not any_chunk:
        raise ValueError("Flex Stock file is empty.")

    print(f"[FlexStockData] Processed {total_records} records.")



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
    cat_qs = CategoryMapping.objects.filter(user=user).values()
    price_qs = PriceData.objects.filter(user=user).values()

    if not sales_qs and not spend_qs:
        return

    df_sales = pd.DataFrame(list(sales_qs))
    if not df_sales.empty:
        df_sales = df_sales[["date", "asin", "pageviews", "units", "orders", "revenue"]]
    else:
        df_sales = pd.DataFrame(
            columns=["date", "asin", "pageviews", "units", "orders", "revenue"]
        )

    df_spend = pd.DataFrame(list(spend_qs))
    if not df_spend.empty:
        df_spend = (
            df_spend.groupby(["date", "asin", "ad_type"])["spend"]
            .sum()
            .unstack("ad_type")
            .reset_index()
            .fillna(0)
        )
        col_map = {
            c: f"spend_{str(c).lower()}"
            for c in df_spend.columns
            if c not in ("date", "asin")
        }
        df_spend.rename(columns=col_map, inplace=True)
    else:
        df_spend = pd.DataFrame(columns=["date", "asin"])

    df_cat = pd.DataFrame(list(cat_qs))
    if df_cat.empty:
        df_cat = pd.DataFrame(columns=["asin", "portfolio", "category", "subcategory"])
    else:
        df_cat = df_cat[["asin", "portfolio", "category", "subcategory"]]

    df_price = pd.DataFrame(list(price_qs))
    if df_price.empty:
        df_price = pd.DataFrame(columns=["asin", "price"])
    else:
        df_price = df_price[["asin", "price"]]

    df_merged = pd.merge(df_sales, df_spend, on=["date", "asin"], how="outer")
    df_merged = pd.merge(df_merged, df_cat, on="asin", how="left")
    df_merged = pd.merge(df_merged, df_price, on="asin", how="left")

    fill_values = {
        "portfolio": "",
        "category": "",
        "subcategory": "",
        "price": 0.0,
        "pageviews": 0,
        "units": 0,
        "orders": 0,
        "revenue": 0.0,
        "spend_sp": 0.0,
        "spend_sb": 0.0,
        "spend_sd": 0.0,
    }
    for col, fill_val in fill_values.items():
        if col in df_merged.columns:
            df_merged[col] = df_merged[col].fillna(fill_val)

    records = []
    batch_size = 10_000
    
    for row in df_merged.itertuples(index=False):
        spend_sp = float(getattr(row, "spend_sp", 0))
        spend_sb = float(getattr(row, "spend_sb", 0))
        spend_sd = float(getattr(row, "spend_sd", 0))
        total_spend = spend_sp + spend_sb + spend_sd

        records.append(
            ProcessedDashboardData(
                user=user,
                date=getattr(row, "date"),
                asin=getattr(row, "asin"),
                portfolio=str(getattr(row, "portfolio", "")) or "",
                category=str(getattr(row, "category", "")) or "",
                subcategory=str(getattr(row, "subcategory", "")) or "",
                price=float(getattr(row, "price", 0)),
                pageviews=clean_number(str(getattr(row, "pageviews", 0))),
                units=clean_number(str(getattr(row, "units", 0))),
                orders=clean_number(str(getattr(row, "orders", 0))),
                revenue=float(getattr(row, "revenue", 0)),
                spend_sp=spend_sp,
                spend_sb=spend_sb,
                spend_sd=spend_sd,
                total_spend=total_spend,
            )
        )
        
        if len(records) >= batch_size:
            ProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
            records = []
            
    if records:
        ProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
    
    from django.core.cache import cache
    
    # Increment dashboard data version for caching
    data_version = cache.get(f"dashboard_data_version_{user.id}", 0)
    cache.set(f"dashboard_data_version_{user.id}", data_version + 1, timeout=None)
    
    for amz in (True, False):
        for flp in (True, False):
            cache.delete(f"dashboard_filters_{user.id}_{amz}_{flp}")


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
    required_cols = [
        "Listing Id",
        "SKU Id",
        "Vertical",
        "Impression Date",
        "Product Clicks",
        "Sales",
        "Revenue",
    ]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK Search Traffic missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            listing_id = str(row.get("Listing Id", "")).strip()
            if not listing_id or listing_id.lower() == "nan" or len(listing_id) < 19:
                continue

            fsn = listing_id[3:19]  # Mid(Listing Id, 4, 16)
            try:
                row_date = pd.to_datetime(row.get("Impression Date")).date()
            except Exception:
                continue

            sku = str(row.get("SKU Id", "") or "").strip().replace('"', "")
            sku = re.sub(r"(?i)^SKU:\s*", "", sku)
            records.append(
                FlipkartSearchTraffic(
                    user=user,
                    fsn=fsn,
                    sku=sku,
                    vertical=str(row.get("Vertical", "") or "").strip(),
                    date=row_date,
                    page_views=clean_number(row.get("Product Clicks", 0)),
                    product_clicks=clean_number(row.get("Product Clicks", 0)),
                    sales=clean_number(row.get("Sales", 0)),
                    revenue=float(clean_currency(row.get("Revenue", 0))),
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FlipkartSearchTraffic.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **_get_upsert_kwargs(
                        unique_fields=["user", "fsn", "date"],
                        update_fields=[
                            "sku",
                            "vertical",
                            "page_views",
                            "product_clicks",
                            "sales",
                            "revenue",
                        ],
                    ),
                )

    if not any_chunk:
        raise ValueError("FK Search Traffic file is empty.")

    print(f"[FK SearchTraffic] Processed {total_records} records.")


# ---------------------------------------------------------------------------
# FK Category Report
# ---------------------------------------------------------------------------


def process_fk_category(file_obj, user):
    """
    Parse Flipkart Category Dashboard (.xlsx).
    Columns: FSN ID, SKU, Portfolio, Cat, Subcat.
    """
    required_cols = ["FSN ID", "Portfolio", "Cat", "Subcat"]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK Category missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            fsn = str(row.get("FSN ID", "")).strip()
            if not fsn or fsn.lower() == "nan":
                continue

            records.append(
                FlipkartCategoryMap(
                    user=user,
                    fsn=fsn,
                    sku=str(row.get("SKU", "") or "").strip(),
                    portfolio=str(row.get("Portfolio", "") or "").strip(),
                    category=str(row.get("Cat", "") or "").strip(),
                    subcategory=str(row.get("Subcat", "") or "").strip(),
                )
            )

        total_records += len(records)
        if records:
            FlipkartCategoryMap.objects.bulk_create(
                records,
                **_get_upsert_kwargs(
                    unique_fields=["user", "fsn"],
                    update_fields=["sku", "portfolio", "category", "subcategory"],
                ),
            )

    if not any_chunk:
        raise ValueError("FK Category file is empty.")

    print(f"[FK Category] Processed {total_records} records.")


# ---------------------------------------------------------------------------
# FK Price Report
# ---------------------------------------------------------------------------


def process_fk_price(file_obj, user):
    """
    Parse Flipkart Price file (.xlsx).
    Columns: Flipkart Serial Number → fsn, Deal → price.
    """
    required_cols = ["Flipkart Serial Number", "Deal"]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK Price missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            fsn = str(row.get("Flipkart Serial Number", "")).strip().replace('"', "")
            if not fsn or fsn.lower() == "nan":
                continue
            records.append(
                FlipkartPrice(
                    user=user,
                    fsn=fsn,
                    price=float(clean_currency(row.get("Deal", 0))),
                )
            )

        total_records += len(records)
        if records:
            FlipkartPrice.objects.bulk_create(
                records,
                **_get_upsert_kwargs(
                    unique_fields=["user", "fsn"], update_fields=["price"]
                ),
            )

    if not any_chunk:
        raise ValueError("FK Price file is empty.")

    print(f"[FK Price] Processed {total_records} records.")


# ---------------------------------------------------------------------------
# FK PCA Attribution Report
# ---------------------------------------------------------------------------


def process_fk_pca(file_obj, user):
    """
    Parse Flipkart PCA Attribution (.csv).
    File has 2 metadata rows (Start Time, End Time) then the header row.
    Columns: campaign_id, campaign_name, Date, fsn_id.
    """
    required_cols = ["campaign_id", "campaign_name", "Date", "fsn_id"]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj, skiprows=2):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK PCA missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            campaign_id = str(row.get("campaign_id", "")).strip()
            fsn_id = str(row.get("fsn_id", "")).strip().replace('"', "")
            if not campaign_id or campaign_id.lower() == "nan":
                continue
            if not fsn_id or fsn_id.lower() == "nan":
                continue

            try:
                row_date = pd.to_datetime(row.get("Date")).date()
            except Exception:
                row_date = None

            records.append(
                FlipkartPCA(
                    user=user,
                    campaign_id=campaign_id,
                    campaign_name=str(row.get("campaign_name", "") or "").strip(),
                    date=row_date,
                    fsn_id=fsn_id,
                )
            )

        total_records += len(records)
        if records:
            FlipkartPCA.objects.bulk_create(
                records,
                **_get_upsert_kwargs(
                    unique_fields=["user", "campaign_id", "fsn_id", "date"],
                    update_fields=["campaign_name"],
                ),
            )

    if not any_chunk:
        raise ValueError("FK PCA file is empty.")

    print(f"[FK PCA] Processed {total_records} records.")


# ---------------------------------------------------------------------------
# FK PLA FSN Report
# ---------------------------------------------------------------------------


def process_fk_pla(file_obj, user):
    """
    Parse Flipkart PLA FSN Report (.csv).
    File has 2 metadata rows then the header row.
    Columns: Campaign ID, Advertised FSN ID, Ad Spend.
    """
    required_cols = ["Campaign ID", "Advertised FSN ID", "Ad Spend"]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj, skiprows=2):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK PLA missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            campaign_id = str(row.get("Campaign ID", "")).strip()
            fsn_id = str(row.get("Advertised FSN ID", "")).strip().replace('"', "")
            if not fsn_id or fsn_id.lower() == "nan":
                continue

            records.append(
                FlipkartPLA(
                    user=user,
                    campaign_id=campaign_id,
                    fsn_id=fsn_id,
                    ad_spend=float(clean_currency(row.get("Ad Spend", 0))),
                )
            )

        total_records += len(records)
        if records:
            FlipkartPLA.objects.bulk_create(
                records,
                **_get_upsert_kwargs(
                    unique_fields=["user", "campaign_id", "fsn_id"],
                    update_fields=["ad_spend"],
                ),
            )

    if not any_chunk:
        raise ValueError("FK PLA file is empty.")

    print(f"[FK PLA] Processed {total_records} records.")


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
    if "Sales Report" in xl.sheet_names:
        df_sales = pd.read_excel(xl, sheet_name="Sales Report")
        for row in df_sales.to_dict("records"):
            oid = str(row.get("Order Item ID", "")).strip().replace('"', "")
            fsn = str(row.get("FSN", "")).strip().replace('"', "")
            qty = clean_number(row.get("Item Quantity", 0))
            if oid and oid.lower() != "nan" and fsn and fsn.lower() != "nan":
                fsn_map[oid] = {"fsn": fsn, "qty": qty}

    # --- Cash Back Report sheet: taxable value & invoice amount ---
    records = []
    if "Cash Back Report" in xl.sheet_names:
        df_cb = pd.read_excel(xl, sheet_name="Cash Back Report")

        for row in df_cb.to_dict("records"):
            order_id = str(row.get("Order ID", "")).strip()
            order_item_id = str(row.get("Order Item ID", "")).strip()
            if not order_id or order_id.lower() == "nan":
                continue

            # Look up FSN from the Sales Report sheet
            info = fsn_map.get(order_item_id, {})
            fsn = info.get("fsn", "")
            qty = info.get("qty", 0)

            records.append(
                FlipkartSalesInvoice(
                    user=user,
                    order_id=order_id,
                    order_item_id=order_item_id,
                    fsn=fsn,
                    item_quantity=qty,
                    taxable_value=float(clean_currency(row.get("Taxable Value", 0))),
                    invoice_amount=float(clean_currency(row.get("Invoice Amount", 0))),
                )
            )

    if records:
        batch_size = 5_000
        for i in range(0, len(records), batch_size):
            FlipkartSalesInvoice.objects.bulk_create(
                records[i : i + batch_size],
                **_get_upsert_kwargs(
                    unique_fields=["user", "order_id", "order_item_id"],
                    update_fields=[
                        "fsn",
                        "item_quantity",
                        "taxable_value",
                        "invoice_amount",
                    ],
                ),
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
    required_cols = ["Flipkart Serial Number", "Coupon Value"]
    total_records = 0
    any_chunk = False
    for df in iter_file_chunks(file_obj, skiprows=2):
        any_chunk = True
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            raise ValueError(f"FK Coupon missing columns: {', '.join(missing)}")

        records = []
        for row in df.to_dict("records"):
            fsn = str(row.get("Flipkart Serial Number", "")).strip().replace('"', "")
            if not fsn or fsn.lower() == "nan":
                continue

            records.append(
                FlipkartCoupon(
                    user=user,
                    fsn=fsn,
                    coupon_value=float(clean_currency(row.get("Coupon Value", 0))),
                )
            )

        total_records += len(records)
        if records:
            FlipkartCoupon.objects.bulk_create(
                records,
                **_get_upsert_kwargs(
                    unique_fields=["user", "fsn"], update_fields=["coupon_value"]
                ),
            )

    if not any_chunk:
        raise ValueError("FK Coupon file is empty.")

    print(f"[FK Coupon] Processed {total_records} records.")


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
    df_traffic = df_traffic[
        [
            "fsn",
            "sku",
            "vertical",
            "date",
            "page_views",
            "product_clicks",
            "sales",
            "revenue",
        ]
    ]

    # 2) Category mapping
    cat_qs = FlipkartCategoryMap.objects.filter(user=user).values()
    df_cat = pd.DataFrame(list(cat_qs)) if cat_qs else pd.DataFrame()
    if not df_cat.empty:
        df_cat = df_cat[["fsn", "portfolio", "category", "subcategory"]]

    # 3) Price
    price_qs = FlipkartPrice.objects.filter(user=user).values()
    df_price = pd.DataFrame(list(price_qs)) if price_qs else pd.DataFrame()
    if not df_price.empty:
        df_price = df_price[["fsn", "price"]]

    # 4) Spend: PCA (campaign_id + fsn + date) + PLA (campaign_id + fsn → ad_spend)
    # Allocate PLA spend across dates to avoid multiplying full spend on every traffic day.
    pca_qs = FlipkartPCA.objects.filter(user=user).values()
    pla_qs = FlipkartPLA.objects.filter(user=user).values()

    df_spend = pd.DataFrame(columns=["fsn", "date", "total_spend"])
    if pca_qs and pla_qs:
        df_pca = pd.DataFrame(list(pca_qs))[["campaign_id", "fsn_id", "date"]]
        df_pla = pd.DataFrame(list(pla_qs))[["campaign_id", "fsn_id", "ad_spend"]]
        traffic_dates_by_fsn = df_traffic[["fsn", "date"]].drop_duplicates()

        if not df_pca.empty and not df_pla.empty:
            df_pca = df_pca.dropna(subset=["fsn_id"])
            df_pca["fsn_id"] = df_pca["fsn_id"].astype(str).str.strip()
            df_pca_dates = df_pca[df_pca["date"].notna()].drop_duplicates(
                subset=["campaign_id", "fsn_id", "date"]
            )
            pair_day_counts = (
                df_pca_dates.groupby(["campaign_id", "fsn_id"])
                .size()
                .reset_index(name="day_count")
            )

            df_pla = df_pla.dropna(subset=["fsn_id"])
            df_pla["fsn_id"] = df_pla["fsn_id"].astype(str).str.strip()
            df_pla["ad_spend"] = pd.to_numeric(df_pla["ad_spend"], errors="coerce").fillna(0.0)
            df_pla_pairs = (
                df_pla.groupby(["campaign_id", "fsn_id"], as_index=False)["ad_spend"].sum()
            )

            spend_parts = []

            pla_with_counts = pd.merge(
                df_pla_pairs, pair_day_counts, on=["campaign_id", "fsn_id"], how="left"
            )
            dated_pairs = pla_with_counts[pla_with_counts["day_count"].fillna(0) > 0]
            if not dated_pairs.empty and not df_pca_dates.empty:
                df_dated = pd.merge(
                    df_pca_dates,
                    dated_pairs[["campaign_id", "fsn_id", "ad_spend", "day_count"]],
                    on=["campaign_id", "fsn_id"],
                    how="inner",
                )
                if not df_dated.empty:
                    df_dated["total_spend"] = df_dated["ad_spend"] / df_dated["day_count"]
                    spend_parts.append(
                        df_dated[["fsn_id", "date", "total_spend"]].rename(
                            columns={"fsn_id": "fsn"}
                        )
                    )

            # Fallback: if PCA has no date rows for a PLA pair, spread spend across
            # available traffic dates for that FSN to preserve total spend correctly.
            undated_pairs = pla_with_counts[pla_with_counts["day_count"].fillna(0) <= 0]
            if not undated_pairs.empty and not traffic_dates_by_fsn.empty:
                fsn_day_counts = (
                    traffic_dates_by_fsn.groupby("fsn")
                    .size()
                    .reset_index(name="fsn_day_count")
                )
                fallback_pairs = undated_pairs.rename(columns={"fsn_id": "fsn"})
                fallback_pairs = pd.merge(
                    fallback_pairs, fsn_day_counts, on="fsn", how="left"
                )
                fallback_pairs = fallback_pairs[
                    fallback_pairs["fsn_day_count"].fillna(0) > 0
                ]
                if not fallback_pairs.empty:
                    fallback_pairs["per_day_spend"] = (
                        fallback_pairs["ad_spend"] / fallback_pairs["fsn_day_count"]
                    )
                    df_fallback = pd.merge(
                        fallback_pairs[["campaign_id", "fsn", "per_day_spend"]],
                        traffic_dates_by_fsn,
                        on="fsn",
                        how="inner",
                    )
                    if not df_fallback.empty:
                        spend_parts.append(
                            df_fallback.groupby(["fsn", "date"], as_index=False)[
                                "per_day_spend"
                            ]
                            .sum()
                            .rename(columns={"per_day_spend": "total_spend"})
                        )

            if spend_parts:
                df_spend = (
                    pd.concat(spend_parts, ignore_index=True)
                    .groupby(["fsn", "date"], as_index=False)["total_spend"]
                    .sum()
                )

    # 5) Sales Invoice (aggregated per FSN)
    inv_qs = FlipkartSalesInvoice.objects.filter(user=user).values()
    df_inv = pd.DataFrame(
        columns=["fsn", "taxable_value", "invoice_amount", "item_quantity"]
    )
    if inv_qs:
        df_inv_raw = pd.DataFrame(list(inv_qs))
        if not df_inv_raw.empty and "fsn" in df_inv_raw.columns:
            df_inv_raw = df_inv_raw[
                df_inv_raw["fsn"].notna() & (df_inv_raw["fsn"] != "")
            ]
            if not df_inv_raw.empty:
                df_inv = (
                    df_inv_raw.groupby("fsn")
                    .agg(
                        {
                            "taxable_value": "sum",
                            "invoice_amount": "sum",
                            "item_quantity": "sum",
                        }
                    )
                    .reset_index()
                )

    # 6) Coupon
    coupon_qs = FlipkartCoupon.objects.filter(user=user).values()
    df_coupon = pd.DataFrame(columns=["fsn", "coupon_value"])
    if coupon_qs:
        df_coupon = pd.DataFrame(list(coupon_qs))
        if not df_coupon.empty:
            df_coupon = df_coupon[["fsn", "coupon_value"]]

    # --- Merge everything onto traffic base ---
    df = df_traffic.copy()

    if not df_cat.empty:
        df = pd.merge(df, df_cat, on="fsn", how="left")
    else:
        df["portfolio"] = ""
        df["category"] = ""
        df["subcategory"] = ""

    if not df_price.empty:
        df = pd.merge(df, df_price, on="fsn", how="left")
    else:
        df["price"] = 0.0

    if not df_spend.empty:
        df = pd.merge(df, df_spend, on=["fsn", "date"], how="left")
    else:
        df["total_spend"] = 0.0

    if not df_inv.empty:
        df = pd.merge(df, df_inv, on="fsn", how="left")
    else:
        df["taxable_value"] = 0.0
        df["invoice_amount"] = 0.0
        df["item_quantity"] = 0

    if not df_coupon.empty:
        df = pd.merge(df, df_coupon, on="fsn", how="left")
    else:
        df["coupon_value"] = 0.0

    # Fill NaN
    fill = {
        "portfolio": "",
        "category": "",
        "subcategory": "",
        "price": 0.0,
        "total_spend": 0.0,
        "taxable_value": 0.0,
        "invoice_amount": 0.0,
        "item_quantity": 0,
        "coupon_value": 0.0,
    }
    for col, val in fill.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)

    # Coupon validation: coupon_value × item_quantity ≈ invoice_amount
    df["coupon_total"] = df["coupon_value"] * df["item_quantity"]
    df["coupon_error"] = False
    mask = (df["coupon_total"] > 0) & (df["invoice_amount"] > 0)
    df.loc[mask, "coupon_error"] = ~(
        (df.loc[mask, "coupon_total"] - df.loc[mask, "invoice_amount"]).abs()
        < 1.0  # tolerance of ₹1
    )

    records = []
    total_processed = 0
    batch_size = 10_000

    for row in df.itertuples(index=False):
        records.append(
            FlipkartProcessedDashboardData(
                user=user,
                date=getattr(row, "date"),
                fsn=getattr(row, "fsn"),
                platform="Flipkart",
                portfolio=str(getattr(row, "portfolio", "")) or "",
                category=str(getattr(row, "category", "")) or "",
                subcategory=str(getattr(row, "subcategory", "")) or "",
                price=float(getattr(row, "price", 0)),
                pageviews=clean_number(str(getattr(row, "page_views", 0))),
                units=clean_number(str(getattr(row, "sales", 0))),
                orders=0,  # No order data for Flipkart
                revenue=float(getattr(row, "revenue", 0)),
                total_spend=float(getattr(row, "total_spend", 0)),
                spend_sp=0.0,
                spend_sb=0.0,
                spend_sd=0.0,
                taxable_value=float(getattr(row, "taxable_value", 0)),
                invoice_amount=float(getattr(row, "invoice_amount", 0)),
                coupon_total=float(getattr(row, "coupon_total", 0)),
                coupon_error=bool(getattr(row, "coupon_error", False)),
            )
        )
        if len(records) >= batch_size:
            FlipkartProcessedDashboardData.objects.bulk_create(
                records, ignore_conflicts=True
            )
            total_processed += len(records)
            records = []

    if records:
        FlipkartProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
        total_processed += len(records)

    from django.core.cache import cache

    # Increment dashboard data version for caching
    data_version = cache.get(f"dashboard_data_version_{user.id}", 0)
    cache.set(f"dashboard_data_version_{user.id}", data_version + 1, timeout=None)

    for amz in (True, False):
        for flp in (True, False):
            cache.delete(f"dashboard_filters_{user.id}_{amz}_{flp}")

    print(f"[FK Dashboard] Generated {total_processed} processed records.")
