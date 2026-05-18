import datetime
import logging
import time

from django.db import connection, transaction
from django.db.models import Sum

from apps.dashboard.models import (
    CategoryMapping,
    FlipkartCategoryMap,
    FlipkartPLA,
    FlipkartPrice,
    FlipkartProcessedDashboardData,
    FlipkartSearchTraffic,
    PriceData,
    ProcessedDashboardData,
    SalesData,
    SpendData,
)
from apps.dashboard.services.invalidation import invalidate_dashboard_cache_for_user
from apps.dashboard.utils import clean_number

from .service_common import DB_BATCH_SIZE

logger = logging.getLogger(__name__)


def _notify_progress(progress_callback, message):
    if progress_callback:
        try:
            progress_callback(message)
        except Exception:
            pass


def _safe_float(value):
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value):
    try:
        return int(value or 0)
    except (TypeError, ValueError):
        return clean_number(value)


def _parse_target_dates(only_dates):
    target_dates = set()
    if only_dates:
        for value in only_dates:
            if isinstance(value, datetime.datetime):
                target_dates.add(value.date())
            elif isinstance(value, datetime.date):
                target_dates.add(value)
            elif isinstance(value, str) and value.strip():
                try:
                    parsed = datetime.datetime.strptime(value.strip(), "%Y-%m-%d").date()
                    target_dates.add(parsed)
                except ValueError:
                    pass
    return target_dates


def _invalidate_user_dashboard_cache(user_id):
    invalidate_dashboard_cache_for_user(user_id, clear_materialized=True)


def _mysql_insert_processed_dashboard_rows(user_id, target_dates):
    """
    Build ProcessedDashboardData fully in MySQL using INSERT...SELECT joins.
    This is materially faster than Python row-by-row object construction.
    """
    sales_table = SalesData._meta.db_table
    spend_table = SpendData._meta.db_table
    category_table = CategoryMapping._meta.db_table
    price_table = PriceData._meta.db_table
    processed_table = ProcessedDashboardData._meta.db_table

    target_dates = sorted(target_dates or [])
    date_placeholders = ", ".join(["%s"] * len(target_dates))
    sales_date_filter = f" AND s.date IN ({date_placeholders})" if target_dates else ""
    spend_date_filter = (
        f" AND date IN ({date_placeholders})"
        if target_dates
        else ""
    )

    spend_agg_sql = f"""
        SELECT
            date,
            asin,
            SUM(CASE WHEN UPPER(ad_type) = 'SP' THEN spend ELSE 0 END) AS spend_sp,
            SUM(CASE WHEN UPPER(ad_type) = 'SB' THEN spend ELSE 0 END) AS spend_sb,
            SUM(CASE WHEN UPPER(ad_type) = 'SD' THEN spend ELSE 0 END) AS spend_sd
        FROM {spend_table}
        WHERE user_id = %s {spend_date_filter}
        GROUP BY date, asin
    """

    insert_sales_sql = f"""
        INSERT INTO {processed_table} (
            user_id, date, asin, portfolio, category, subcategory, price,
            pageviews, units, orders, revenue, spend_sp, spend_sb, spend_sd, total_spend
        )
        SELECT
            %s AS user_id,
            s.date,
            s.asin,
            COALESCE(cm.portfolio, '') AS portfolio,
            COALESCE(cm.category, '') AS category,
            COALESCE(cm.subcategory, '') AS subcategory,
            COALESCE(pd.price, 0) AS price,
            SUM(COALESCE(s.pageviews, 0)) AS pageviews,
            SUM(COALESCE(s.units, 0)) AS units,
            SUM(COALESCE(s.orders, 0)) AS orders,
            SUM(COALESCE(s.revenue, 0)) AS revenue,
            COALESCE(sp.spend_sp, 0) AS spend_sp,
            COALESCE(sp.spend_sb, 0) AS spend_sb,
            COALESCE(sp.spend_sd, 0) AS spend_sd,
            COALESCE(sp.spend_sp, 0) + COALESCE(sp.spend_sb, 0) + COALESCE(sp.spend_sd, 0) AS total_spend
        FROM {sales_table} s
        LEFT JOIN ({spend_agg_sql}) sp
            ON sp.date = s.date AND sp.asin = s.asin
        LEFT JOIN {category_table} cm
            ON cm.user_id = %s AND cm.asin = s.asin
        LEFT JOIN {price_table} pd
            ON pd.user_id = %s AND pd.asin = s.asin
        WHERE s.user_id = %s {sales_date_filter}
        GROUP BY
            s.date, s.asin,
            cm.portfolio, cm.category, cm.subcategory, pd.price,
            sp.spend_sp, sp.spend_sb, sp.spend_sd
    """

    insert_spend_only_sql = f"""
        INSERT INTO {processed_table} (
            user_id, date, asin, portfolio, category, subcategory, price,
            pageviews, units, orders, revenue, spend_sp, spend_sb, spend_sd, total_spend
        )
        SELECT
            %s AS user_id,
            sp.date,
            sp.asin,
            COALESCE(cm.portfolio, '') AS portfolio,
            COALESCE(cm.category, '') AS category,
            COALESCE(cm.subcategory, '') AS subcategory,
            COALESCE(pd.price, 0) AS price,
            0 AS pageviews,
            0 AS units,
            0 AS orders,
            0 AS revenue,
            COALESCE(sp.spend_sp, 0) AS spend_sp,
            COALESCE(sp.spend_sb, 0) AS spend_sb,
            COALESCE(sp.spend_sd, 0) AS spend_sd,
            COALESCE(sp.spend_sp, 0) + COALESCE(sp.spend_sb, 0) + COALESCE(sp.spend_sd, 0) AS total_spend
        FROM ({spend_agg_sql}) sp
        LEFT JOIN {sales_table} s
            ON s.user_id = %s AND s.date = sp.date AND s.asin = sp.asin
        LEFT JOIN {category_table} cm
            ON cm.user_id = %s AND cm.asin = sp.asin
        LEFT JOIN {price_table} pd
            ON pd.user_id = %s AND pd.asin = sp.asin
        WHERE s.id IS NULL
    """

    sales_params = [user_id, user_id]
    sales_params.extend(target_dates)
    sales_params.extend([user_id, user_id, user_id])
    sales_params.extend(target_dates)

    spend_only_params = [user_id, user_id]
    spend_only_params.extend(target_dates)
    spend_only_params.extend([user_id, user_id, user_id])

    rows_written = 0
    with connection.cursor() as cursor:
        cursor.execute(insert_sales_sql, sales_params)
        rows_written += max(int(cursor.rowcount or 0), 0)
        cursor.execute(insert_spend_only_sql, spend_only_params)
        rows_written += max(int(cursor.rowcount or 0), 0)

    return rows_written


def _generate_dashboard_data_python(user, sales_qs, spend_qs, progress_callback):
    _notify_progress(progress_callback, "Loading category and price mappings...")
    category_by_asin = {}
    for row in CategoryMapping.objects.filter(user=user).values(
        "asin", "portfolio", "category", "subcategory"
    ).iterator(chunk_size=DB_BATCH_SIZE):
        asin = str(row.get("asin") or "").strip()
        if not asin:
            continue
        category_by_asin[asin] = (
            str(row.get("portfolio") or ""),
            str(row.get("category") or ""),
            str(row.get("subcategory") or ""),
        )

    price_by_asin = {}
    for row in PriceData.objects.filter(user=user).values("asin", "price").iterator(
        chunk_size=DB_BATCH_SIZE
    ):
        asin = str(row.get("asin") or "").strip()
        if not asin:
            continue
        price_by_asin[asin] = _safe_float(row.get("price"))

    _notify_progress(progress_callback, "Aggregating ad spend...")
    spend_by_key = {}
    spend_rows = (
        spend_qs.values("date", "asin", "ad_type")
        .annotate(spend_total=Sum("spend"))
        .iterator(chunk_size=DB_BATCH_SIZE)
    )
    for row in spend_rows:
        date = row.get("date")
        asin = str(row.get("asin") or "").strip()
        if not date or not asin:
            continue

        key = (date, asin)
        bucket = spend_by_key.setdefault(
            key, {"spend_sp": 0.0, "spend_sb": 0.0, "spend_sd": 0.0}
        )
        spend_total = _safe_float(row.get("spend_total"))
        ad_type = str(row.get("ad_type") or "").strip().upper()
        if ad_type == "SP":
            bucket["spend_sp"] += spend_total
        elif ad_type == "SB":
            bucket["spend_sb"] += spend_total
        elif ad_type == "SD":
            bucket["spend_sd"] += spend_total

    _notify_progress(progress_callback, "Building processed dashboard rows...")
    records = []
    total_rows = 0

    sales_rows = sales_qs.values(
        "date", "asin", "pageviews", "units", "orders", "revenue"
    ).iterator(chunk_size=DB_BATCH_SIZE)

    for row in sales_rows:
        date = row.get("date")
        asin = str(row.get("asin") or "").strip()
        if not date or not asin:
            continue

        spend_payload = spend_by_key.pop((date, asin), None)
        spend_sp = _safe_float(spend_payload.get("spend_sp")) if spend_payload else 0.0
        spend_sb = _safe_float(spend_payload.get("spend_sb")) if spend_payload else 0.0
        spend_sd = _safe_float(spend_payload.get("spend_sd")) if spend_payload else 0.0
        total_spend = spend_sp + spend_sb + spend_sd

        portfolio, category, subcategory = category_by_asin.get(asin, ("", "", ""))
        price = _safe_float(price_by_asin.get(asin, 0.0))

        records.append(
            ProcessedDashboardData(
                user=user,
                date=date,
                asin=asin,
                portfolio=portfolio,
                category=category,
                subcategory=subcategory,
                price=price,
                pageviews=_safe_int(row.get("pageviews")),
                units=_safe_int(row.get("units")),
                orders=_safe_int(row.get("orders")),
                revenue=_safe_float(row.get("revenue")),
                spend_sp=spend_sp,
                spend_sb=spend_sb,
                spend_sd=spend_sd,
                total_spend=total_spend,
            )
        )
        total_rows += 1

        if len(records) >= DB_BATCH_SIZE:
            ProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
            records = []

    # Keep spend-only rows (outer-join behavior).
    for (date, asin), spend_payload in spend_by_key.items():
        if not date or not asin:
            continue
        spend_sp = _safe_float(spend_payload.get("spend_sp"))
        spend_sb = _safe_float(spend_payload.get("spend_sb"))
        spend_sd = _safe_float(spend_payload.get("spend_sd"))
        total_spend = spend_sp + spend_sb + spend_sd
        portfolio, category, subcategory = category_by_asin.get(asin, ("", "", ""))
        price = _safe_float(price_by_asin.get(asin, 0.0))

        records.append(
            ProcessedDashboardData(
                user=user,
                date=date,
                asin=asin,
                portfolio=portfolio,
                category=category,
                subcategory=subcategory,
                price=price,
                pageviews=0,
                units=0,
                orders=0,
                revenue=0.0,
                spend_sp=spend_sp,
                spend_sb=spend_sb,
                spend_sd=spend_sd,
                total_spend=total_spend,
            )
        )
        total_rows += 1

        if len(records) >= DB_BATCH_SIZE:
            ProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
            records = []

    if records:
        ProcessedDashboardData.objects.bulk_create(records, ignore_conflicts=True)
    return total_rows


def generate_dashboard_data(user, progress_callback=None, only_dates=None):
    """
    Merges all independent Amazon tables for the given user and dumps them into
    ProcessedDashboardData to quickly serve the frontend.

    This implementation avoids loading giant DataFrames in memory, which keeps
    large historical uploads faster and more stable.
    """

    target_dates = _parse_target_dates(only_dates)

    _t0 = time.monotonic()
    mode = f"incremental ({len(target_dates)} dates)" if target_dates else "full rebuild"

    if target_dates:
        _notify_progress(progress_callback, "Refreshing dashboard aggregates for selected dates...")
    else:
        _notify_progress(progress_callback, "Refreshing dashboard aggregates...")

    processed_qs = ProcessedDashboardData.objects.filter(user=user)
    sales_qs = SalesData.objects.filter(user=user)
    spend_qs = SpendData.objects.filter(user=user)
    if target_dates:
        processed_qs = processed_qs.filter(date__in=target_dates)
        sales_qs = sales_qs.filter(date__in=target_dates)
        spend_qs = spend_qs.filter(date__in=target_dates)

    with transaction.atomic():
        processed_qs.delete()

        has_sales = sales_qs.exists()
        has_spend = spend_qs.exists()
        if not has_sales and not has_spend:
            _invalidate_user_dashboard_cache(user.id)
            return

        total_rows = 0
        if connection.vendor == "mysql":
            _notify_progress(progress_callback, "Building processed dashboard rows...")
            total_rows = _mysql_insert_processed_dashboard_rows(
                user.id, sorted(target_dates)
            )
        else:
            total_rows = _generate_dashboard_data_python(
                user, sales_qs, spend_qs, progress_callback
            )

    _notify_progress(progress_callback, f"Processed {total_rows} dashboard rows.")
    _invalidate_user_dashboard_cache(user.id)
    _elapsed = time.monotonic() - _t0
    logger.info(
        "[Dashboard] generate_dashboard_data user=%s mode=%s rows=%d elapsed=%.1fs",
        user.id, mode, total_rows, _elapsed,
    )


# ===========================================================================
# SLIM FLIPKART PROCESSING FUNCTIONS
# ===========================================================================

# ---------------------------------------------------------------------------
# FK Search Traffic Report
# ---------------------------------------------------------------------------



def generate_flipkart_dashboard_data(user, progress_callback=None, only_dates=None):
    """
    Merge Flipkart reports at FSN/date level into FlipkartProcessedDashboardData.

    The old implementation loaded every source row into pandas DataFrames before
    merging. Large Flipkart uploads can exhaust worker time and memory that way,
    so this version lets the database aggregate traffic/spend and streams the
    processed rows back in batches.
    """

    target_dates = _parse_target_dates(only_dates)

    _t0 = time.monotonic()
    mode = f"incremental ({len(target_dates)} dates)" if target_dates else "full rebuild"

    if target_dates:
        _notify_progress(
            progress_callback, "Refreshing Flipkart dashboard aggregates for selected dates..."
        )
    else:
        _notify_progress(progress_callback, "Refreshing Flipkart dashboard aggregates...")

    processed_qs = FlipkartProcessedDashboardData.objects.filter(user=user)
    traffic_qs = FlipkartSearchTraffic.objects.filter(user=user)
    pla_qs = FlipkartPLA.objects.filter(user=user)
    if target_dates:
        processed_qs = processed_qs.filter(date__in=target_dates)
        traffic_qs = traffic_qs.filter(date__in=target_dates)
        pla_qs = pla_qs.filter(date__in=target_dates)


    processed_qs.delete()

    if not traffic_qs.exists() and not pla_qs.exists():
        _invalidate_user_dashboard_cache(user.id)
        logger.info("[FK Dashboard] No search traffic or PLA data - skipping.")
        return

    _notify_progress(progress_callback, "Loading Flipkart category and price mappings...")
    category_by_fsn = {}
    for row in FlipkartCategoryMap.objects.filter(user=user).values(
        "fsn", "portfolio", "category", "subcategory"
    ).iterator(chunk_size=DB_BATCH_SIZE):
        fsn = str(row.get("fsn") or "").strip()
        if not fsn:
            continue
        category_by_fsn[fsn] = (
            str(row.get("portfolio") or ""),
            str(row.get("category") or ""),
            str(row.get("subcategory") or ""),
        )

    price_by_fsn = {}
    for row in FlipkartPrice.objects.filter(user=user).values("fsn", "price").iterator(
        chunk_size=DB_BATCH_SIZE
    ):
        fsn = str(row.get("fsn") or "").strip()
        if not fsn:
            continue
        price_by_fsn[fsn] = _safe_float(row.get("price"))

    _notify_progress(progress_callback, "Aggregating Flipkart ad spend...")
    spend_by_key = {}
    spend_rows = (
        pla_qs.values("fsn_id", "date")
        .annotate(total_spend=Sum("ad_spend"))
        .iterator(chunk_size=DB_BATCH_SIZE)
    )
    for row in spend_rows:
        date = row.get("date")
        fsn = str(row.get("fsn_id") or "").strip()
        if not date or not fsn:
            continue
        key = (date, fsn)
        spend_by_key[key] = spend_by_key.get(key, 0.0) + _safe_float(
            row.get("total_spend")
        )

    _notify_progress(progress_callback, "Building Flipkart processed dashboard rows...")
    records = []
    total_processed = 0

    def _flush_records():
        nonlocal records, total_processed
        if not records:
            return
        FlipkartProcessedDashboardData.objects.bulk_create(
            records, ignore_conflicts=True
        )
        total_processed += len(records)
        records = []

    def _append_processed_row(
        *,
        date,
        fsn,
        pageviews=0,
        units=0,
        revenue=0.0,
        total_spend=0.0,
    ):
        if not date or not fsn:
            return


        portfolio, category, subcategory = category_by_fsn.get(fsn, ("", "", ""))
        records.append(
            FlipkartProcessedDashboardData(
                user=user,
                date=date,
                fsn=fsn,
                platform="Flipkart",
                portfolio=portfolio,
                category=category,
                subcategory=subcategory,
                price=_safe_float(price_by_fsn.get(fsn, 0.0)),
                pageviews=_safe_int(pageviews),
                units=_safe_int(units),
                orders=0,
                revenue=_safe_float(revenue),
                total_spend=_safe_float(total_spend),
                spend_sp=0.0,
                spend_sb=0.0,
                spend_sd=0.0,
            )
        )
        if len(records) >= DB_BATCH_SIZE:
            _flush_records()

    traffic_rows = (
        traffic_qs.values("fsn", "date")
        .annotate(
            page_views=Sum("page_views"),
            sales=Sum("sales"),
            revenue_total=Sum("revenue"),
        )
        .iterator(chunk_size=DB_BATCH_SIZE)
    )
    for row in traffic_rows:
        date = row.get("date")
        fsn = str(row.get("fsn") or "").strip()
        if not date or not fsn:
            continue
        total_spend = spend_by_key.pop((date, fsn), 0.0)
        _append_processed_row(
            date=date,
            fsn=fsn,
            pageviews=row.get("page_views"),
            units=row.get("sales"),
            revenue=row.get("revenue_total"),
            total_spend=total_spend,
        )

    # Preserve PLA spend rows that have no matching traffic row for that FSN/date.
    for (date, fsn), total_spend in spend_by_key.items():
        _append_processed_row(date=date, fsn=fsn, total_spend=total_spend)

    _flush_records()
    _invalidate_user_dashboard_cache(user.id)
    _elapsed = time.monotonic() - _t0

    logger.info(
        "[FK Dashboard] Generated %s processed records. mode=%s elapsed=%.1fs",
        total_processed, mode, _elapsed,
    )
