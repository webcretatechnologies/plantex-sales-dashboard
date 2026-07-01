import logging

import pandas as pd

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


def _clean_text(value):
    text = str(value or "").strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def _prefer_present(existing, new_value):
    return new_value if new_value else existing


def _clean_optional_date(value):
    if value is None or pd.isna(value):
        return None
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"} or text in {"0", "0.0"}:
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
    mapping_by_asin = {}
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "category")

        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue
            touched_asins.add(asin)

            payload = {
                "asin": asin,
                "portfolio": _clean_text(row.get("Portfolio", "")),
                "category": _clean_text(row.get("Category", "")),
                "subcategory": _clean_text(row.get("Subcategory", "")),
                "msku": _clean_text(row.get("Skus", "")) or None,
                "parent_asin": _clean_optional_parent_asin(row.get("Parent ASIN")),
                "launch_date": _clean_optional_date(
                    row.get("Launch date", row.get("Launch Date"))
                ),
                "category_manager": _clean_optional_text(row.get("RP")),
                "series_name": _clean_optional_text(row.get("Series")),
                "material": _clean_optional_text(row.get("Material")),
                "size": _clean_optional_text(row.get("Size")),
                "brand_name": _clean_optional_text(row.get("Brand name")),
                "ratings": _clean_optional_text(row.get("Ratings")),
                "finish": _clean_optional_text(row.get("Finish")),
            }

            if asin not in mapping_by_asin:
                mapping_by_asin[asin] = payload
            else:
                existing = mapping_by_asin[asin]
                for field, value in payload.items():
                    if field == "asin":
                        continue
                    if value not in (None, ""):
                        existing[field] = value

    new_mappings = [
        CategoryMapping(user=user, **payload)
        for payload in mapping_by_asin.values()
    ]
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
    price_by_asin = {}
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "price")

        for row in df.to_dict("records"):
            asin = str(row.get("ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue
            touched_asins.add(asin)
            price_by_asin[asin] = clean_currency(row.get("Price", 0))

    new_prices = [
        PriceData(user=user, asin=asin, price=price)
        for asin, price in price_by_asin.items()
    ]
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
    import uuid
    from django.db import connection
    from apps.dashboard.models import SpendData
    import os
    
    total_spends = 0
    touched_dates = set()
    any_chunk = False
    row_number = 1
    _date_cache = {}
    spend_by_key = {}
    
    db_table = SpendData._meta.db_table
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_csv:
        writer = csv.writer(tmp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # Write headers based on SpendData fields
        writer.writerow(["user_id", "date", "asin", "ad_account", "ad_type", "spend"])
        
        for df in iter_file_chunks(file_obj):
            any_chunk = True
            require_columns(df, "spend")

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
                spend_by_key[key] = spend_by_key.get(key, 0.0) + spend_val

        for (row_date, asin, ad_account, ad_type), spend_total in spend_by_key.items():
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
        temp_table = f"tmp_spend_upload_{uuid.uuid4().hex}"
        with connection.cursor() as cursor:
            cursor.execute("SET autocommit=0;")
            cursor.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    user_id BIGINT NOT NULL,
                    date DATE NOT NULL,
                    asin VARCHAR(50) NOT NULL,
                    ad_account VARCHAR(100) NOT NULL,
                    ad_type VARCHAR(10) NOT NULL,
                    spend DOUBLE NOT NULL DEFAULT 0
                ) ENGINE=InnoDB;
                """
            )
            cursor.execute(
                f"""
            LOAD DATA LOCAL INFILE '{tmp_csv_path}'
                INTO TABLE {temp_table}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            IGNORE 1 LINES
            (user_id, date, asin, ad_account, ad_type, spend);
            """
            )
            cursor.execute(
                f"""
                INSERT INTO {db_table}
                    (user_id, date, asin, ad_account, ad_type, spend)
                SELECT
                    user_id, date, asin, ad_account, ad_type, spend
                FROM {temp_table}
                ON DUPLICATE KEY UPDATE
                    spend = VALUES(spend);
                """
            )
            cursor.execute("COMMIT;")
            cursor.execute("SET autocommit=1;")

    os.remove(tmp_csv_path)

    logger.info(
        "[SpendData] Processed and loaded %s unique ASIN/ad-account/ad-type/date records via INFILE.",
        total_spends,
    )
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
    import uuid
    from django.db import connection
    from apps.dashboard.models import SalesData
    
    date_obj = parse_sales_upload_date(date_str)
    db_table = SalesData._meta.db_table

    total_sales = 0
    any_chunk = False
    sales_by_asin = {}
    row_count = 0

    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "sales")

        for row in df.to_dict("records"):
            asin = str(row.get("(Child) ASIN", "")).strip()
            if not asin or asin.lower() == "nan":
                continue

            item = sales_by_asin.setdefault(
                asin,
                {
                    "pageviews": 0,
                    "units": 0,
                    "orders": 0,
                    "revenue": 0.0,
                },
            )
            item["pageviews"] += int(clean_number(row.get("Page Views - Total", 0)) or 0)
            item["units"] += int(clean_number(row.get("Units Ordered", 0)) or 0)
            item["orders"] += int(clean_number(row.get("Total Order Items", 0)) or 0)
            item["revenue"] += float(clean_currency(row.get("Ordered Product Sales", 0)) or 0.0)
            row_count += 1
    
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv') as tmp_csv:
        writer = csv.writer(tmp_csv, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        writer.writerow(["user_id", "date", "asin", "pageviews", "units", "orders", "revenue"])

        for asin, data in sales_by_asin.items():
            writer.writerow([
                user.id,
                date_obj.isoformat(),
                asin,
                data["pageviews"],
                data["units"],
                data["orders"],
                data["revenue"],
            ])
            total_sales += 1
                
        tmp_csv_path = tmp_csv.name

    if not any_chunk:
        os.remove(tmp_csv_path)
        raise ValueError("Daily Sales file is empty.")

    if total_sales > 0:
        temp_table = f"tmp_sales_upload_{uuid.uuid4().hex}"
        with connection.cursor() as cursor:
            cursor.execute("SET autocommit=0;")
            cursor.execute(
                f"""
                CREATE TEMPORARY TABLE {temp_table} (
                    user_id BIGINT NOT NULL,
                    date DATE NOT NULL,
                    asin VARCHAR(50) NOT NULL,
                    pageviews INT NOT NULL DEFAULT 0,
                    units INT NOT NULL DEFAULT 0,
                    orders INT NOT NULL DEFAULT 0,
                    revenue DOUBLE NOT NULL DEFAULT 0
                ) ENGINE=InnoDB;
                """
            )
            cursor.execute(
                f"""
            LOAD DATA LOCAL INFILE '{tmp_csv_path}'
                INTO TABLE {temp_table}
            FIELDS TERMINATED BY ',' ENCLOSED BY '"'
            IGNORE 1 LINES
            (user_id, date, asin, pageviews, units, orders, revenue);
            """
            )
            cursor.execute(
                f"""
                INSERT INTO {db_table}
                    (user_id, date, asin, pageviews, units, orders, revenue)
                SELECT
                    user_id, date, asin, pageviews, units, orders, revenue
                FROM {temp_table}
                ON DUPLICATE KEY UPDATE
                    pageviews = VALUES(pageviews),
                    units = VALUES(units),
                    orders = VALUES(orders),
                    revenue = VALUES(revenue);
                """
            )
            cursor.execute("COMMIT;")
            cursor.execute("SET autocommit=1;")

    os.remove(tmp_csv_path)

    logger.info(
        "[SalesData] date=%s, processed %s rows into %s unique ASIN records via INFILE.",
        date_obj,
        row_count,
        total_sales,
    )
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
    stock_by_key = {}
    numeric_fields = [
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
    ]
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
            disposition = _clean_text(row.get(col_lookup["disposition"], ""))
            location = _clean_text(row.get(col_lookup["location"], ""))
            key = (asin, row_date, disposition, location)

            if key not in stock_by_key:
                stock_by_key[key] = {
                    "date": row_date,
                    "fnsku": _clean_text(row.get(col_lookup["fnsku"], "")),
                    "asin": asin,
                    "msku": _clean_text(row.get(col_lookup["msku"], "")),
                    "title": _clean_text(row.get(col_lookup["title"], ""))[:500],
                    "disposition": disposition,
                    "location": location,
                    **{field: 0 for field in numeric_fields},
                }
            else:
                stock_by_key[key]["fnsku"] = _prefer_present(
                    stock_by_key[key]["fnsku"],
                    _clean_text(row.get(col_lookup["fnsku"], "")),
                )
                stock_by_key[key]["msku"] = _prefer_present(
                    stock_by_key[key]["msku"],
                    _clean_text(row.get(col_lookup["msku"], "")),
                )
                stock_by_key[key]["title"] = _prefer_present(
                    stock_by_key[key]["title"],
                    _clean_text(row.get(col_lookup["title"], ""))[:500],
                )

            stock_by_key[key]["starting_warehouse_balance"] += clean_number(
                row.get(col_lookup["starting warehouse balance"], 0)
            )
            stock_by_key[key]["in_transit_between_warehouses"] += clean_number(
                row.get(col_lookup["in transit between warehouses"], 0)
            )
            stock_by_key[key]["receipts"] += clean_number(row.get(col_lookup["receipts"], 0))
            stock_by_key[key]["customer_shipments"] += clean_number(
                row.get(col_lookup["customer shipments"], 0)
            )
            stock_by_key[key]["customer_returns"] += clean_number(
                row.get(col_lookup["customer returns"], 0)
            )
            stock_by_key[key]["vendor_returns"] += clean_number(row.get(col_lookup["vendor returns"], 0))
            stock_by_key[key]["warehouse_transfer_in_out"] += clean_number(
                row.get(col_lookup["warehouse transfer in/out"], 0)
            )
            stock_by_key[key]["found"] += clean_number(row.get(col_lookup["found"], 0))
            stock_by_key[key]["lost"] += clean_number(row.get(col_lookup["lost"], 0))
            stock_by_key[key]["damaged"] += clean_number(row.get(col_lookup["damaged"], 0))
            stock_by_key[key]["disposed"] += clean_number(row.get(col_lookup["disposed"], 0))
            stock_by_key[key]["other_events"] += clean_number(row.get(col_lookup["other events"], 0))
            stock_by_key[key]["ending_warehouse_balance"] += clean_number(
                row.get(col_lookup["ending warehouse balance"], 0)
            )
            stock_by_key[key]["unknown_events"] += clean_number(
                row.get(col_lookup["unknown events"], 0)
            )
            total_records += 1

    if not any_chunk:
        raise ValueError("FBA Stock file is empty.")

    records = [
        FBAStockData(user=user, **payload)
        for payload in stock_by_key.values()
    ]
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

    logger.info(
        "[FBAStockData] Processed %s uploaded rows into %s unique ASIN/date/disposition/location records.",
        total_records,
        len(records),
    )
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
    stock_by_key = {}
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
            cluster = _clean_text(row.get(col_lookup["cluster"], ""))
            key = (asin, row_date, cluster)

            if key not in stock_by_key:
                stock_by_key[key] = {
                    "date": row_date,
                    "asin": asin,
                    "cluster": cluster,
                    "qty": 0,
                }
            stock_by_key[key]["qty"] += clean_number(row.get(col_lookup["qty"], 0))
            total_records += 1

    if not any_chunk:
        raise ValueError("Flex Stock file is empty.")

    records = [
        FlexStockData(user=user, **payload)
        for payload in stock_by_key.values()
    ]
    if records:
        for i in range(0, len(records), DB_BATCH_SIZE):
            FlexStockData.objects.bulk_create(
                records[i : i + DB_BATCH_SIZE],
                **get_upsert_kwargs(
                    unique_fields=["user", "asin", "date", "cluster"],
                    update_fields=["qty"],
                ),
            )

    logger.info(
        "[FlexStockData] Processed %s uploaded rows into %s unique ASIN/date/cluster records.",
        total_records,
        len(records),
    )
    return touched_dates
