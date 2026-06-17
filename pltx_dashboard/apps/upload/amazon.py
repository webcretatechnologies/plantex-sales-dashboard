import logging

from apps.dashboard.models import (
    CategoryMapping,
    FBAStockData,
    FlexStockData,
    PriceData,
    SalesData,
    SpendData,
)
from apps.dashboard.utils import clean_currency, clean_number
from apps.upload.parsers import iter_file_chunks, parse_report_date
from apps.upload.schema import parse_sales_upload_date, require_columns

from .service_common import DB_BATCH_SIZE, get_upsert_kwargs

logger = logging.getLogger(__name__)


def _clean_optional_parent_asin(value):
    text = str(value or "").strip()
    if not text or text.lower() == "nan" or text in {"0", "0.0"}:
        return None
    return text


def _clean_optional_text(value):
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    return text


def _clean_optional_date(value):
    text = str(value or "").strip()
    if not text or text.lower() == "nan" or text in {"0", "0.0"}:
        return None
    try:
        return parse_report_date(value, prefer_dayfirst=True)
    except Exception:
        logger.warning("Ignoring invalid category Launch Date value: %r", value)
        return None


def process_category_file(file_obj, user):
    """
    Upsert category mappings scoped to the given user.
    - Uses bulk_create with update_conflicts to elegantly update existing records.
    """
    any_chunk = False
    touched_asins = set()
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "category")

        new_mappings = []
        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue
            touched_asins.add(asin)

            new_mappings.append(
                CategoryMapping(
                    user=user,
                    asin=asin,
                    portfolio=str(row.get("Portfolio", "")).strip(),
                    category=str(row.get("Category", "")).strip(),
                    subcategory=str(row.get("Subcategory", "")).strip(),
                    msku=str(row.get("Skus", "") or "").strip() or None,
                    parent_asin=_clean_optional_parent_asin(row.get("Parent ASIN")),
                    launch_date=_clean_optional_date(
                        row.get("Launch date", row.get("Launch Date"))
                    ),
                    category_manager=_clean_optional_text(row.get("RP")),
                    series_name=_clean_optional_text(row.get("Series")),
                    material=_clean_optional_text(row.get("Material")),
                    size=_clean_optional_text(row.get("Size")),
                    brand_name=_clean_optional_text(row.get("Brand name")),
                    ratings=_clean_optional_text(row.get("Ratings")),
                    finish=_clean_optional_text(row.get("Finish")),
                )
            )

        if new_mappings:
            for i in range(0, len(new_mappings), DB_BATCH_SIZE):
                CategoryMapping.objects.bulk_create(
                    new_mappings[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "asin"],
                        update_fields=[
                            "portfolio",
                            "category",
                            "subcategory",
                            "msku",
                            "parent_asin",
                            "launch_date",
                            "category_manager",
                            "series_name",
                            "material",
                            "size",
                            "brand_name",
                            "ratings",
                            "finish",
                        ],
                    ),
                )

    if not any_chunk:
        raise ValueError("Category Mapping file is empty.")
    return touched_asins


# ---------------------------------------------------------------------------
# Price
# ---------------------------------------------------------------------------


def process_price_file(file_obj, user):
    """
    Upsert price data scoped to the given user.
    - Uses bulk_create with update_conflicts to smartly update existing values.
    """
    any_chunk = False
    touched_asins = set()
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "price")

        new_prices = []
        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue
            touched_asins.add(asin)
            new_prices.append(
                PriceData(user=user, asin=asin, price=clean_currency(row.get("Price", 0)))
            )

        if new_prices:
            for i in range(0, len(new_prices), DB_BATCH_SIZE):
                PriceData.objects.bulk_create(
                    new_prices[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "asin"], update_fields=["price"]
                    ),
                )

    if not any_chunk:
        raise ValueError("Pricing Data file is empty.")
    return touched_asins


# ---------------------------------------------------------------------------
# Spend
# ---------------------------------------------------------------------------


def process_spend_file(file_obj, user):
    """
    Insert spend rows scoped to the user, or update existing records directly if they already exist.
    """
    import tempfile
    import csv
    from django.db import connection
    from apps.dashboard.models import SpendData
    import os
    
    total_spends = 0
    touched_dates = set()
    any_chunk = False
    row_number = 1
    _date_cache = {}
    
    db_table = SpendData._meta.db_table
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_csv:
        writer = csv.writer(tmp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Write headers based on SpendData fields
        writer.writerow(["user_id", "date", "asin", "ad_account", "ad_type", "spend"])
        
        for df in iter_file_chunks(file_obj):
            any_chunk = True
            require_columns(df, "spend")

            spend_agg = {}
            for row in df.to_dict("records"):
                row_number += 1
                asin = str(row.get("ASIN", "")).strip()
                if not asin or asin.lower() == "nan":
                    continue

                raw_date = row.get("Date")
                if raw_date not in _date_cache:
                    try:
                        _date_cache[raw_date] = parse_report_date(raw_date, prefer_dayfirst=False)
                    except Exception as exc:
                        import os
                        os.remove(tmp_csv.name)
                        raise ValueError(f"Invalid Date value in Ads Spends at row {row_number}: {exc}")
                row_date = _date_cache[raw_date]
                touched_dates.add(row_date)

                ad_type = str(row.get("Ad Type", "")).strip().upper()
                if ad_type in ("SPONSORED PRODUCTS", "SP"):
                    ad_type = "SP"
                elif ad_type in ("SPONSORED BRANDS", "SB"):
                    ad_type = "SB"
                elif ad_type in ("SPONSORED DISPLAY", "SD"):
                    ad_type = "SD"
                else:
                    ad_type = ad_type[:10]

                ad_account = str(row.get("Ad Account", "")).strip()
                spend_val = clean_currency(row.get("Spend", 0))

                key = (row_date, asin, ad_account, ad_type)
                spend_agg[key] = spend_agg.get(key, 0.0) + spend_val

            for (row_date, asin, ad_account, ad_type), spend_total in spend_agg.items():
                writer.writerow([
                    user.id,
                    row_date.isoformat(),
                    asin,
                    ad_account,
                    ad_type,
                    spend_total
                ])
                total_spends += 1
        
        tmp_csv_path = tmp_csv.name

    if not any_chunk:
        os.remove(tmp_csv_path)
        raise ValueError("Ads Spends file is empty.")

    if total_spends > 0:
        with connection.cursor() as cursor:
            cursor.execute("SET autocommit=0;")
            cursor.execute("SET unique_checks=0;")
            cursor.execute("SET foreign_key_checks=0;")
            
            query = f"""
            LOAD DATA LOCAL INFILE '{tmp_csv_path}'
            REPLACE INTO TABLE {db_table}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            IGNORE 1 LINES
            (user_id, date, asin, ad_account, ad_type, spend);
            """
            cursor.execute(query)
            
            cursor.execute("COMMIT;")
            cursor.execute("SET autocommit=1;")
            cursor.execute("SET unique_checks=1;")
            cursor.execute("SET foreign_key_checks=1;")

    os.remove(tmp_csv_path)

    logger.info("[SpendData] Processed and loaded %s records via INFILE.", total_spends)
    return touched_dates


# ---------------------------------------------------------------------------
# Sales
# ---------------------------------------------------------------------------


def process_sales_file(file_obj, date_str, user):
    """
    Insert sales rows scoped to the user, or update existing records directly if they already exist.
    """
    import tempfile
    import csv
    import os
    from django.db import connection
    from apps.dashboard.models import SalesData
    
    date_obj = parse_sales_upload_date(date_str)
    db_table = SalesData._meta.db_table

    total_sales = 0
    any_chunk = False
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_csv:
        writer = csv.writer(tmp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["user_id", "date", "asin", "pageviews", "units", "orders", "revenue"])

        for df in iter_file_chunks(file_obj):
            any_chunk = True
            require_columns(df, "sales")

            for row in df.to_dict("records"):
                asin = str(row.get("(Child) ASIN", "")).strip()
                if not asin or asin.lower() == "nan":
                    continue

                writer.writerow([
                    user.id,
                    date_obj.isoformat(),
                    asin,
                    clean_number(row.get("Page Views - Total", 0)),
                    clean_number(row.get("Units Ordered", 0)),
                    clean_number(row.get("Total Order Items", 0)),
                    float(clean_currency(row.get("Ordered Product Sales", 0)))
                ])
                total_sales += 1
                
        tmp_csv_path = tmp_csv.name

    if not any_chunk:
        os.remove(tmp_csv_path)
        raise ValueError("Daily Sales file is empty.")

    if total_sales > 0:
        with connection.cursor() as cursor:
            cursor.execute("SET autocommit=0;")
            cursor.execute("SET unique_checks=0;")
            cursor.execute("SET foreign_key_checks=0;")

            query = f"""
            LOAD DATA LOCAL INFILE '{tmp_csv_path}'
            REPLACE INTO TABLE {db_table}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            IGNORE 1 LINES
            (user_id, date, asin, pageviews, units, orders, revenue);
            """
            cursor.execute(query)

            cursor.execute("COMMIT;")
            cursor.execute("SET autocommit=1;")
            cursor.execute("SET unique_checks=1;")
            cursor.execute("SET foreign_key_checks=1;")

    os.remove(tmp_csv_path)

    logger.info("[SalesData] date=%s, processed and loaded %s records via INFILE.", date_obj, total_sales)
    return {date_obj}


# ---------------------------------------------------------------------------
# FBA Stock
# ---------------------------------------------------------------------------


def process_fba_stock_file(file_obj, user, id_columns=("ASIN",)):
    """
    Parse FBA stock file (Amazon/Flipkart).
    Required columns: Date, FNSKU, <product id>, MSKU, Title, Disposition,
    Starting Warehouse Balance, In Transit Between Warehouses, Receipts,
    Customer Shipments, Customer Returns, Vendor Returns,
    Warehouse Transfer In/Out, Found, Lost, Damaged, Disposed,
    Other Events, Ending Warehouse Balance, Unknown Events, Location.
    """
    required_cols = [
        "Date",
        "FNSKU",
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
    touched_dates = set()
    any_chunk = False
    row_number = 1
    _date_cache = {}
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        col_lookup = {}
        for c in df.columns:
            key = str(c).replace("\ufeff", "").strip().lower()
            if key and key not in col_lookup:
                col_lookup[key] = c

        id_col_key = None
        for candidate in id_columns:
            key = str(candidate).strip().lower()
            if key in col_lookup:
                id_col_key = key
                break

        missing_cols = [c for c in required_cols if c.lower() not in col_lookup]
        if id_col_key is None:
            missing_cols.append("/".join(id_columns))
        if missing_cols:
            raise ValueError(
                f"FBA Stock file missing required columns: {', '.join(missing_cols)}"
            )

        records = []
        for row in df.to_dict("records"):
            row_number += 1
            asin = str(row.get(col_lookup[id_col_key], "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            raw_date = row.get(col_lookup["date"])
            if raw_date not in _date_cache:
                try:
                    _date_cache[raw_date] = parse_report_date(
                        raw_date, prefer_dayfirst=False
                    )
                except Exception as exc:
                    raise ValueError(
                        f"Invalid Date value in FBA Stock at row {row_number}: {exc}"
                    )
            row_date = _date_cache[raw_date]
            touched_dates.add(row_date)

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
                    **get_upsert_kwargs(
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

    logger.info("[FBAStockData] Processed %s records.", total_records)
    return touched_dates


# ---------------------------------------------------------------------------
# Flex Stock
# ---------------------------------------------------------------------------


def process_flex_stock_file(file_obj, user, id_columns=("ASIN",)):
    """
    Parse Flex stock file (Amazon/Flipkart).
    Required columns: Date, <product id>, Cluster, Qty.
    """
    required_cols = ["Date", "Cluster", "Qty"]
    total_records = 0
    touched_dates = set()
    any_chunk = False
    row_number = 1
    _date_cache = {}
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        col_lookup = {}
        for c in df.columns:
            key = str(c).replace("\ufeff", "").strip().lower()
            if key and key not in col_lookup:
                col_lookup[key] = c

        id_col_key = None
        for candidate in id_columns:
            key = str(candidate).strip().lower()
            if key in col_lookup:
                id_col_key = key
                break

        missing_cols = [c for c in required_cols if c.lower() not in col_lookup]
        if id_col_key is None:
            missing_cols.append("/".join(id_columns))
        if missing_cols:
            raise ValueError(
                f"Flex Stock file missing required columns: {', '.join(missing_cols)}"
            )

        records = []
        for row in df.to_dict("records"):
            row_number += 1
            asin = str(row.get(col_lookup[id_col_key], "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            raw_date = row.get(col_lookup["date"])
            if raw_date not in _date_cache:
                try:
                    _date_cache[raw_date] = parse_report_date(
                        raw_date, prefer_dayfirst=False
                    )
                except Exception as exc:
                    raise ValueError(
                        f"Invalid Date value in Flex Stock at row {row_number}: {exc}"
                    )
            row_date = _date_cache[raw_date]
            touched_dates.add(row_date)

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
                    **get_upsert_kwargs(
                        unique_fields=["user", "asin", "date", "cluster"],
                        update_fields=["qty"],
                    ),
                )

    if not any_chunk:
        raise ValueError("Flex Stock file is empty.")

    logger.info("[FlexStockData] Processed %s records.", total_records)
    return touched_dates
