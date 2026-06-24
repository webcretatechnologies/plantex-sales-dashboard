import logging
import re

import pandas as pd

from apps.dashboard.models import (
    Flipkartfba,
    FlipkartCategoryMap,
    FlipkartInventoryStock,
    FlipkartPLA,
    FlipkartPrice,
    FlipkartSearchTraffic,
)
from apps.dashboard.utils import clean_currency, clean_number
from apps.upload.parsers import (
    extract_fk_report_date_from_metadata as _extract_fk_report_date_from_metadata,
    iter_file_chunks,
    parse_report_date,
)
from apps.upload.schema import require_columns

from .service_common import DB_BATCH_SIZE, get_upsert_kwargs

logger = logging.getLogger(__name__)


def _clean_optional_text(value):
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return None
    # Reject Excel formula strings (e.g. =IFERROR(VLOOKUP(...)))
    if text.startswith("="):
        return None
    # Reject bare "0" which appears when a formula evaluates to 0 and is read as string
    if text == "0":
        return None
    return text


def _clean_optional_rating(value):
    """Like _clean_optional_text but keeps numeric rating values (e.g. 4.3).
    Returns None if value is 0, NaN, empty, or a formula string."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nan" or text.startswith("="):
        return None
    try:
        num = float(text)
        if num == 0:
            return None
        return str(num)
    except (ValueError, TypeError):
        pass
    if text == "0":
        return None
    return text


def _clean_optional_date(value):
    if value is None or pd.isna(value):
        return None
    text = str(value or "").strip()
    if not text or text.lower() in {"nan", "nat", "none", "null"} or text in {"0", "0.0"}:
        return None
    try:
        return parse_report_date(value, prefer_dayfirst=True)
    except Exception:
        logger.warning("Ignoring invalid FK category Launch Date value: %r", value)
        return None


def _column_key(value):
    return re.sub(r"[^a-z0-9]+", "", str(value or "").replace("\ufeff", "").strip().lower())


def _resolve_columns(df, required_columns, file_label):
    lookup = {}
    for col in df.columns:
        key = _column_key(col)
        if key and key not in lookup:
            lookup[key] = col

    resolved = {}
    missing = []
    for required in required_columns:
        col = lookup.get(_column_key(required))
        if col is None:
            missing.append(required)
        else:
            resolved[required] = col
    if missing:
        raise ValueError(f"{file_label} missing required columns: {', '.join(missing)}")
    return resolved


def process_fk_inventory_file(file_obj, user):
    """
    Parse FK Inventory file (FK.xlsx).
    Required columns: PRODUCTS STATUS, PRODUCTS TYPE, SKU, FSN, Qty.

    The FK file does not contain a Date column; the date is derived from:
      1. An explicit "Date" column if present, or
      2. The file's last-modified timestamp on disk, or
      3. The current date as a fallback.
    All records are tagged with this date in DD-MM-YYYY format.
    """
    import datetime
    import os

    required_cols = ["FSN", "Qty"]
    any_chunk = False
    total_records = 0
    # Derive date from the file
    file_date = None
    if hasattr(file_obj, "name") and os.path.exists(file_obj.name):
        try:
            mtime = os.path.getmtime(file_obj.name)
            file_date = datetime.date.fromtimestamp(mtime)
        except Exception:
            pass
    if file_date is None:
        file_date = datetime.date.today()

    touched_dates = set()
    _date_cache = {}

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
                f"FK Inventory file missing required columns: {', '.join(missing_cols)}"
            )

        # If the file has an explicit Date column, use it per-row
        has_date_col = "date" in col_lookup

        records = []
        
        fsn_arr = df[col_lookup.get("fsn", "FSN")].fillna("").astype(str).values if col_lookup.get("fsn", "FSN") in df.columns else [""] * len(df)
        date_arr = df[col_lookup["date"]].values if has_date_col else [None] * len(df)
        sku_arr = df[col_lookup.get("sku", "SKU")].fillna("").astype(str).values if col_lookup.get("sku", "SKU") in df.columns else [""] * len(df)
        status_arr = df[col_lookup.get("products status", "PRODUCTS STATUS")].fillna("").astype(str).values if col_lookup.get("products status", "PRODUCTS STATUS") in df.columns else [""] * len(df)
        type_arr = df[col_lookup.get("products type", "PRODUCTS TYPE")].fillna("").astype(str).values if col_lookup.get("products type", "PRODUCTS TYPE") in df.columns else [""] * len(df)
        qty_arr = df[col_lookup.get("qty", "Qty")].fillna(0).values if col_lookup.get("qty", "Qty") in df.columns else [0] * len(df)

        for fsn_val, raw_date, sku_val, status_val, type_val, qty_val in zip(
            fsn_arr, date_arr, sku_arr, status_arr, type_arr, qty_arr
        ):
            fsn = str(fsn_val).strip()
            if not fsn or fsn.lower() == "nan":
                continue

            # Determine row-level date
            if has_date_col:
                if raw_date is not None and str(raw_date).strip():
                    if raw_date not in _date_cache:
                        try:
                            _date_cache[raw_date] = parse_report_date(raw_date, prefer_dayfirst=True)
                        except Exception:
                            _date_cache[raw_date] = file_date
                    row_date = _date_cache[raw_date]
                else:
                    row_date = file_date
            else:
                row_date = file_date

            touched_dates.add(row_date)

            sku = str(sku_val).strip()
            product_status = str(status_val).strip()
            product_type = str(type_val).strip()
            qty = clean_number(qty_val)

            records.append(
                FlipkartInventoryStock(
                    user=user,
                    date=row_date,
                    fsn=fsn,
                    sku=sku if sku.lower() != "nan" else "",
                    product_status=product_status if product_status.lower() != "nan" else "",
                    product_type=product_type if product_type.lower() != "nan" else "",
                    qty=qty,
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FlipkartInventoryStock.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "fsn", "date"],
                        update_fields=["sku", "product_status", "product_type", "qty"],
                    ),
                )

    if not any_chunk:
        raise ValueError("FK Inventory file is empty.")

    logger.info("[FlipkartInventoryStock] Processed %s uploaded rows.", total_records)
    return touched_dates


def process_fk_fba_stock_file(file_obj, user):
    """
    Parse Flipkart FBA current inventory report.
    Required columns:
    - Date
    - FSN
    - Live on Website
    """
    required_cols = ["Date", "FSN", "Live on Website"]
    any_chunk = False
    total_records = 0
    touched_dates = set()
    row_number = 1
    _date_cache = {}

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
                "FK FBA Stock file missing required columns: "
                + ", ".join(missing_cols)
            )

        date_col = col_lookup["date"]
        fsn_col = col_lookup["fsn"]
        live_col = col_lookup["live on website"]
        warehouse_col = col_lookup.get("warehouse id")
        sku_col = col_lookup.get("sku")
        title_col = col_lookup.get("title")
        listing_col = col_lookup.get("listing id")
        brand_col = col_lookup.get("brand")
        price_col = col_lookup.get("flipkart selling price")

        records = []
        
        fsn_arr = df[fsn_col].fillna("").astype(str).values if fsn_col in df.columns else [""] * len(df)
        date_arr = df[date_col].values if date_col in df.columns else [None] * len(df)
        live_arr = df[live_col].fillna(0).values if live_col in df.columns else [0] * len(df)
        warehouse_arr = df[warehouse_col].fillna("").astype(str).values if warehouse_col in df.columns else [""] * len(df)
        sku_arr = df[sku_col].fillna("").astype(str).values if sku_col in df.columns else [""] * len(df)
        title_arr = df[title_col].fillna("").astype(str).values if title_col in df.columns else [""] * len(df)
        listing_arr = df[listing_col].fillna("").astype(str).values if listing_col in df.columns else [""] * len(df)
        brand_arr = df[brand_col].fillna("").astype(str).values if brand_col in df.columns else [""] * len(df)
        price_arr = df[price_col].fillna(0).values if price_col in df.columns else [0] * len(df)

        for fsn_val, raw_date, live_val, wh_val, sku_val, title_val, listing_val, brand_val, price_val in zip(
            fsn_arr, date_arr, live_arr, warehouse_arr, sku_arr, title_arr, listing_arr, brand_arr, price_arr
        ):
            row_number += 1
            fsn = str(fsn_val).strip()
            if not fsn or fsn.lower() == "nan":
                continue

            if raw_date is None or str(raw_date).strip() == "":
                raise ValueError(
                    f"Missing Date value in FK FBA Stock at row {row_number}."
                )
            if raw_date not in _date_cache:
                try:
                    _date_cache[raw_date] = parse_report_date(raw_date, prefer_dayfirst=True)
                except Exception as exc:
                    raise ValueError(
                        f"Invalid Date value in FK FBA Stock at row {row_number}: {exc}"
                    )
            row_date = _date_cache[raw_date]

            touched_dates.add(row_date)
            live_on_website_qty = clean_number(live_val)
            location = str(wh_val).strip() if warehouse_col else ""
            msku = str(sku_val).strip() if sku_col else ""
            title = str(title_val).strip() if title_col else ""
            
            records.append(
                Flipkartfba(
                    user=user,
                    date=row_date,
                    fsn=fsn,
                    warehouse_id=location,
                    sku=msku,
                    title=title[:500],
                    listing_id=str(listing_val).strip() if listing_col else "",
                    brand=str(brand_val).strip() if brand_col else "",
                    flipkart_selling_price=clean_currency(price_val) if price_col else 0,
                    live_on_website=live_on_website_qty,
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                Flipkartfba.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "date", "fsn", "warehouse_id"],
                        update_fields=[
                            "sku",
                            "title",
                            "listing_id",
                            "brand",
                            "flipkart_selling_price",
                            "live_on_website",
                        ],
                    ),
                )

    if not any_chunk:
        raise ValueError("FK FBA Stock file is empty.")

    logger.info("[FK FBA Stock] Processed %s records.", total_records)
    return touched_dates


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
    total_records = 0
    any_chunk = False
    touched_dates = set()
    all_key_totals = {}
    row_number = 1
    _date_cache = {}

    for df in iter_file_chunks(file_obj):
        any_chunk = True
        cols = _resolve_columns(
            df,
            [
                "Listing Id",
                "SKU Id",
                "Vertical",
                "Impression Date",
                "Product Clicks",
                "Sales",
                "Revenue",
            ],
            "FK Search Traffic",
        )

        # Pre-extract arrays for speed
        listing_ids = df[cols["Listing Id"]].fillna("").astype(str).values
        raw_dates = df[cols["Impression Date"]].values
        sku_ids = df[cols["SKU Id"]].fillna("").astype(str).values
        verticals = df[cols["Vertical"]].fillna("").astype(str).values
        product_clicks = df[cols["Product Clicks"]].fillna(0).values
        sales_arr = df[cols["Sales"]].fillna(0).values
        revenues = df[cols["Revenue"]].fillna(0).values

        for listing_id, raw_date, sku, vertical, clicks, sales, revenue in zip(
            listing_ids, raw_dates, sku_ids, verticals, product_clicks, sales_arr, revenues
        ):
            row_number += 1
            listing_id = str(listing_id).strip()
            if not listing_id or listing_id.lower() == "nan" or len(listing_id) < 19:
                continue

            fsn = listing_id[3:19]  # Mid(Listing Id, 4, 16)
            if raw_date not in _date_cache:
                try:
                    _date_cache[raw_date] = parse_report_date(raw_date, prefer_dayfirst=False)
                except Exception as exc:
                    raise ValueError(f"Invalid Impression Date in FK Search Traffic at row {row_number}: {exc}")
            row_date = _date_cache[raw_date]
            touched_dates.add(row_date)

            sku = str(sku).strip().replace('"', "")
            sku = re.sub(r"(?i)^SKU:\s*", "", sku)
            key = (fsn, row_date)
            vertical = str(vertical).strip()
            
            if key not in all_key_totals:
                all_key_totals[key] = {
                    "fsn": fsn,
                    "date": row_date,
                    "sku": sku,
                    "vertical": vertical,
                    "page_views": 0,
                    "product_clicks": 0,
                    "sales": 0,
                    "revenue": 0.0,
                }
            else:
                if not all_key_totals[key]["sku"] and sku:
                    all_key_totals[key]["sku"] = sku
                if not all_key_totals[key]["vertical"] and vertical:
                    all_key_totals[key]["vertical"] = vertical

            clicks_val = clean_number(clicks)
            all_key_totals[key]["page_views"] += clicks_val
            all_key_totals[key]["product_clicks"] += clicks_val
            all_key_totals[key]["sales"] += clean_number(sales)
            all_key_totals[key]["revenue"] += float(clean_currency(revenue))

    if not any_chunk:
        raise ValueError("FK Search Traffic file is empty.")

    records = [
        FlipkartSearchTraffic(
            user=user,
            fsn=payload["fsn"],
            sku=payload["sku"],
            vertical=payload["vertical"],
            date=payload["date"],
            page_views=payload["page_views"],
            product_clicks=payload["product_clicks"],
            sales=payload["sales"],
            revenue=payload["revenue"],
        )
        for payload in all_key_totals.values()
    ]
    total_records = len(records)
    if records:
        for i in range(0, len(records), DB_BATCH_SIZE):
            FlipkartSearchTraffic.objects.bulk_create(
                records[i : i + DB_BATCH_SIZE],
                **get_upsert_kwargs(
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

    logger.info("[FK SearchTraffic] Processed and upserted %s records.", total_records)
    return touched_dates


# ---------------------------------------------------------------------------
# FK Category Report
# ---------------------------------------------------------------------------


def process_fk_category(file_obj, user):
    """
    Parse Flipkart Category Dashboard (.xlsx).
    Expected columns:
    - FSN ID
    - asin (optional; ignored)
    - SKU
    - Portfolio
    - Category
    - Sub Category
    - Vertical (optional; ignored)
    - Product Status (optional)
    """
    total_records = 0
    any_chunk = False
    touched_fsns = set()
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        col_lookup = {}
        for original_col in df.columns:
            normalized = re.sub(r"[^a-z0-9]+", "", str(original_col).strip().lower())
            if normalized and normalized not in col_lookup:
                col_lookup[normalized] = original_col

        def resolve_col(*aliases):
            for alias in aliases:
                normalized = re.sub(r"[^a-z0-9]+", "", alias.strip().lower())
                if normalized in col_lookup:
                    return col_lookup[normalized]
            return None

        fsn_col = resolve_col("FSN ID")
        asin_col = resolve_col("asin", "ASIN")
        sku_col = resolve_col("SKU")
        portfolio_col = resolve_col("Portfolio")
        category_col = resolve_col("Category")
        subcategory_col = resolve_col("Sub Category", "Subcategory")
        _vertical_col = resolve_col("Vertical")
        product_status_col = resolve_col("Product Status")
        material_col = resolve_col("Material")
        finish_col = resolve_col("Finish")
        category_manager_col = resolve_col("RP")
        series_col = resolve_col("Series name", "Series")
        ratings_col = resolve_col("Ratings")
        brand_col = resolve_col("Brand name")
        size_col = resolve_col("Size")
        launch_date_col = resolve_col("Launch Date", "Launch date")

        missing = []
        if not fsn_col:
            missing.append("FSN ID")
        if not sku_col:
            missing.append("SKU")
        if not portfolio_col:
            missing.append("Portfolio")
        if not category_col:
            missing.append("Category")
        if not subcategory_col:
            missing.append("Sub Category")
        if missing:
            raise ValueError(f"FK Category missing columns: {', '.join(missing)}")

        records = []

        # Pre-extract arrays
        fsn_arr = df[fsn_col].fillna("").astype(str).values
        asin_arr = df[asin_col].values if asin_col in df.columns else [None] * len(df)
        sku_arr = df[sku_col].fillna("").astype(str).values
        portfolio_arr = df[portfolio_col].fillna("").astype(str).values
        cat_arr = df[category_col].fillna("").astype(str).values
        subcat_arr = df[subcategory_col].fillna("").astype(str).values
        status_arr = df[product_status_col].fillna("").astype(str).values if product_status_col in df.columns else [""] * len(df)
        launch_arr = df[launch_date_col].values if launch_date_col in df.columns else [None] * len(df)
        cm_arr = df[category_manager_col].values if category_manager_col in df.columns else [None] * len(df)
        series_arr = df[series_col].values if series_col in df.columns else [None] * len(df)
        mat_arr = df[material_col].values if material_col in df.columns else [None] * len(df)
        size_arr = df[size_col].values if size_col in df.columns else [None] * len(df)
        brand_arr = df[brand_col].values if brand_col in df.columns else [None] * len(df)
        rating_arr = df[ratings_col].values if ratings_col in df.columns else [None] * len(df)
        finish_arr = df[finish_col].values if finish_col in df.columns else [None] * len(df)

        for fsn_val, asin_val, sku_val, port_val, cat_val, subcat_val, status_val, launch_val, cm_val, series_val, mat_val, size_val, brand_val, rating_val, finish_val in zip(
            fsn_arr, asin_arr, sku_arr, portfolio_arr, cat_arr, subcat_arr, status_arr, launch_arr, cm_arr, series_arr, mat_arr, size_arr, brand_arr, rating_arr, finish_arr
        ):
            fsn = str(fsn_val).strip()
            if not fsn or fsn.lower() == "nan":
                continue
            touched_fsns.add(fsn)

            raw_status = str(status_val).strip()
            normalized_status = ""
            status_lower = raw_status.lower()
            if status_lower in ("continued", "continue", "continued/pack of not sales"):
                normalized_status = "Continued"
            elif status_lower in ("discontinued", "discontinue"):
                normalized_status = "Discontinued"

            records.append(
                FlipkartCategoryMap(
                    user=user,
                    fsn=fsn,
                    asin=_clean_optional_text(asin_val) if asin_col else None,
                    sku=str(sku_val).strip(),
                    portfolio=str(port_val).strip(),
                    category=str(cat_val).strip(),
                    subcategory=str(subcat_val).strip(),
                    product_status=normalized_status,
                    launch_date=_clean_optional_date(launch_val) if launch_date_col else None,
                    category_manager=_clean_optional_text(cm_val) if category_manager_col else None,
                    series_name=_clean_optional_text(series_val) if series_col else None,
                    material=_clean_optional_text(mat_val) if material_col else None,
                    size=_clean_optional_text(size_val) if size_col else None,
                    brand_name=_clean_optional_text(brand_val) if brand_col else None,
                    ratings=_clean_optional_rating(rating_val) if ratings_col else None,
                    finish=_clean_optional_text(finish_val) if finish_col else None,
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FlipkartCategoryMap.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "fsn"],
                        update_fields=[
                            "sku",
                            "portfolio",
                            "category",
                            "subcategory",
                            "product_status",
                            "asin",
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
        raise ValueError("FK Category file is empty.")

    logger.info("[FK Category] Processed %s records.", total_records)
    return touched_fsns


# ---------------------------------------------------------------------------
# FK Price Report
# ---------------------------------------------------------------------------


def process_fk_price(file_obj, user):
    """
    Parse Flipkart Price file (.xlsx).
    Columns: Flipkart Serial Number → fsn, Deal → price.
    """
    total_records = 0
    any_chunk = False
    touched_fsns = set()
    for df in iter_file_chunks(file_obj):
        any_chunk = True
        require_columns(df, "fk_price")

        records = []
        fsn_arr = df["Flipkart Serial Number"].fillna("").astype(str).values
        deal_arr = df["Deal"].fillna(0).values

        for fsn_val, deal_val in zip(fsn_arr, deal_arr):
            fsn = str(fsn_val).strip().replace('"', "")
            if not fsn or fsn.lower() == "nan":
                continue
            touched_fsns.add(fsn)
            records.append(
                FlipkartPrice(
                    user=user,
                    fsn=fsn,
                    price=float(clean_currency(deal_val)),
                )
            )

        total_records += len(records)
        if records:
            for i in range(0, len(records), DB_BATCH_SIZE):
                FlipkartPrice.objects.bulk_create(
                    records[i : i + DB_BATCH_SIZE],
                    **get_upsert_kwargs(
                        unique_fields=["user", "fsn"], update_fields=["price"]
                    ),
                )

    if not any_chunk:
        raise ValueError("FK Price file is empty.")

    logger.info("[FK Price] Processed %s records.", total_records)
    return touched_fsns


# ---------------------------------------------------------------------------
# FK PLA FSN Report
# ---------------------------------------------------------------------------


def process_fk_pla(file_obj, user):
    """
    Parse Flipkart PLA FSN Report (.csv).
    File has 2 metadata rows then the header row.
    Columns: Campaign ID, Advertised FSN ID, Ad Spend.
    """
    total_records = 0
    any_chunk = False
    all_key_spend = {}

    report_date = _extract_fk_report_date_from_metadata(file_obj)
    if report_date is None:
        raise ValueError("FK PLA metadata missing Start Time/End Time.")

    for df in iter_file_chunks(file_obj, skiprows=2):
        any_chunk = True
        cols = _resolve_columns(
            df,
            ["Campaign ID", "Advertised FSN ID", "Ad Spend"],
            "FK PLA",
        )

        campaign_arr = df[cols["Campaign ID"]].fillna("").astype(str).values
        fsn_arr = df[cols["Advertised FSN ID"]].fillna("").astype(str).values
        spend_arr = df[cols["Ad Spend"]].fillna(0).values

        for camp_val, fsn_val, spend_val in zip(campaign_arr, fsn_arr, spend_arr):
            campaign_id = str(camp_val or "").strip().replace('"', "")
            if campaign_id.lower() == "nan":
                campaign_id = ""
            fsn_id = str(fsn_val).strip().replace('"', "")
            if not fsn_id or fsn_id.lower() == "nan":
                continue

            key = (campaign_id, fsn_id, report_date)
            all_key_spend[key] = all_key_spend.get(key, 0.0) + float(
                clean_currency(spend_val)
            )

    if not any_chunk:
        raise ValueError("FK PLA file is empty.")

    records = [
        FlipkartPLA(
            user=user,
            campaign_id=campaign_id,
            fsn_id=fsn_id,
            date=row_date,
            ad_spend=spend,
        )
        for (campaign_id, fsn_id, row_date), spend in all_key_spend.items()
    ]
    total_records = len(records)
    if records:
        for i in range(0, len(records), DB_BATCH_SIZE):
            FlipkartPLA.objects.bulk_create(
                records[i : i + DB_BATCH_SIZE],
                **get_upsert_kwargs(
                    unique_fields=["user", "campaign_id", "fsn_id", "date"],
                    update_fields=["ad_spend"],
                ),
            )

    logger.info("[FK PLA] Processed and upserted %s records.", total_records)
    return {report_date}
