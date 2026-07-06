import datetime

from django.db import connection, transaction

from apps.dashboard.models import (
    CategoryMapping,
    DashboardMappingDimensionDailySummary,
    DashboardMappingFilterDailySummary,
    DashboardMappingFilterMonthlyActivitySummary,
    DashboardProductDailySummary,
    FlipkartCategoryMap,
    FlipkartProcessedDashboardData,
    ProcessedDashboardData,
)


DIMENSION_FIELDS = (
    "category",
    "portfolio",
    "subcategory",
    "category_manager",
    "series_name",
    "material",
    "size",
    "brand_name",
    "ratings",
    "finish",
)

FILTER_SUMMARY_FIELDS = (
    "category_manager",
    "series_name",
    "material",
    "size",
    "brand_name",
    "finish",
)


def _delete_summary_rows(cursor, table_name, user_id, only_dates):
    params = [user_id]
    date_filter = ""
    if only_dates:
        placeholders = ", ".join(["%s"] * len(only_dates))
        date_filter = f" AND `date` IN ({placeholders})"
        params.extend(only_dates)
    cursor.execute(f"DELETE FROM `{table_name}` WHERE `user_id` = %s{date_filter}", params)


def _month_start(value):
    if isinstance(value, datetime.datetime):
        value = value.date()
    elif not isinstance(value, datetime.date):
        try:
            value = datetime.datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
        except Exception:
            try:
                year, month = [int(part) for part in str(value)[:7].split("-")]
                value = datetime.date(year, month, 1)
            except Exception:
                return None
    return value.replace(day=1)


def _month_starts_from_dates(values):
    months = set()
    for value in values or []:
        month_start = _month_start(value)
        if month_start:
            months.add(month_start)
    return sorted(months)


def _next_month(value):
    if value.month == 12:
        return datetime.date(value.year + 1, 1, 1)
    return datetime.date(value.year, value.month + 1, 1)


def _delete_monthly_activity_rows(cursor, table_name, user_id, only_months):
    params = [user_id]
    month_filter = ""
    if only_months:
        placeholders = ", ".join(["%s"] * len(only_months))
        month_filter = f" AND `year_month` IN ({placeholders})"
        params.extend(only_months)
    cursor.execute(f"DELETE FROM `{table_name}` WHERE `user_id` = %s{month_filter}", params)


def rebuild_mapping_dimension_daily_summary_for_user(user, *, only_dates=None):
    """
    Rebuild day-level aggregates for category-master filter dimensions.
    This keeps RP/series/material/size/finish style filters off SKU-level scans.
    """
    only_dates = sorted({str(d) for d in (only_dates or []) if str(d).strip()})

    with transaction.atomic():
        date_filter = ""
        if only_dates:
            placeholders = ", ".join(["%s"] * len(only_dates))
            date_filter = f" AND p.date IN ({placeholders})"

        tbl = DashboardMappingDimensionDailySummary._meta.db_table
        az_tbl = ProcessedDashboardData._meta.db_table
        fk_tbl = FlipkartProcessedDashboardData._meta.db_table
        az_map_tbl = CategoryMapping._meta.db_table
        fk_map_tbl = FlipkartCategoryMap._meta.db_table

        def dimension_hash_sql(prefix):
            parts = [
                f"COALESCE({prefix}.category, '')",
                f"COALESCE({prefix}.portfolio, '')",
                f"COALESCE({prefix}.subcategory, '')",
                "COALESCE(m.category_manager, '')",
                "COALESCE(m.series_name, '')",
                "COALESCE(m.material, '')",
                "COALESCE(m.size, '')",
                "COALESCE(m.brand_name, '')",
                "COALESCE(m.ratings, '')",
                "COALESCE(m.finish, '')",
            ]
            return f"LEFT(CONCAT_WS('|', {', '.join(parts)}), 255)"

        def common_insert_sql(source_table, mapping_table, sku_field, platform):
            return f"""
                INSERT INTO `{tbl}` (
                    `user_id`, `date`, `platform`, `dimension_hash`,
                    `category`, `portfolio`, `subcategory`,
                    `category_manager`, `series_name`, `material`, `size`,
                    `brand_name`, `ratings`, `finish`,
                    `revenue`, `orders`, `units`, `pageviews`,
                    `total_spend`, `spend_sp`, `spend_sb`, `spend_sd`
                )
                SELECT
                    p.user_id,
                    p.date,
                    '{platform}',
                    {dimension_hash_sql("p")},
                    COALESCE(p.category, ''),
                    COALESCE(p.portfolio, ''),
                    COALESCE(p.subcategory, ''),
                    COALESCE(m.category_manager, ''),
                    COALESCE(m.series_name, ''),
                    COALESCE(m.material, ''),
                    COALESCE(m.size, ''),
                    COALESCE(m.brand_name, ''),
                    COALESCE(m.ratings, ''),
                    COALESCE(m.finish, ''),
                    SUM(p.revenue),
                    SUM(p.orders),
                    SUM(p.units),
                    SUM(p.pageviews),
                    SUM(p.total_spend),
                    SUM(p.spend_sp),
                    SUM(p.spend_sb),
                    SUM(p.spend_sd)
                FROM `{source_table}` p
                LEFT JOIN `{mapping_table}` m
                    ON m.user_id = p.user_id AND m.{sku_field} = p.{sku_field}
                WHERE p.user_id = %s{date_filter}
                GROUP BY
                    p.user_id,
                    p.date,
                    p.category,
                    p.portfolio,
                    p.subcategory,
                    m.category_manager,
                    m.series_name,
                    m.material,
                    m.size,
                    m.brand_name,
                    m.ratings,
                    m.finish
            """

        params = [user.id, *only_dates]
        rows_written = 0
        with connection.cursor() as cursor:
            _delete_summary_rows(cursor, tbl, user.id, only_dates)
            cursor.execute(common_insert_sql(az_tbl, az_map_tbl, "asin", "Amazon"), params)
            rows_written += max(cursor.rowcount, 0)
            cursor.execute(common_insert_sql(fk_tbl, fk_map_tbl, "fsn", "Flipkart"), params)
            rows_written += max(cursor.rowcount, 0)

    return {
        "rows_written": rows_written,
        "dates_scoped": only_dates,
    }


def rebuild_mapping_filter_monthly_activity_summary_for_user(
    user,
    *,
    only_months=None,
    only_dates=None,
):
    """
    Rebuild monthly per-listing activity rows for common mapping filters.

    The table is intentionally tall: one row per
    (month, platform, filter field, filter value, sku, category tuple). That
    keeps request-time listing activity counts exact for all-time and full-month
    ranges while avoiding runtime joins to category master tables.
    """
    month_starts = _month_starts_from_dates(only_months)
    if only_dates and not month_starts:
        month_starts = _month_starts_from_dates(only_dates)
    month_strs = [month_start.strftime("%Y-%m-01") for month_start in month_starts]

    month_filter_sql = ""
    month_params = []
    if month_starts:
        clauses = []
        for month_start in month_starts:
            clauses.append("(p.date >= %s AND p.date < %s)")
            month_params.extend([month_start, _next_month(month_start)])
        month_filter_sql = f" AND ({' OR '.join(clauses)})"

    tbl = DashboardMappingFilterMonthlyActivitySummary._meta.db_table
    product_tbl = DashboardProductDailySummary._meta.db_table
    az_map_tbl = CategoryMapping._meta.db_table
    fk_map_tbl = FlipkartCategoryMap._meta.db_table

    def insert_sql(mapping_table, sku_field, product_sku_field, platform, filter_field):
        return f"""
            INSERT INTO `{tbl}` (
                `user_id`, `year_month`, `platform`, `filter_name`, `filter_value`,
                `sku`, `category`, `portfolio`, `subcategory`,
                `revenue`, `units`, `pageviews`, `total_spend`
            )
            SELECT
                p.user_id,
                DATE_FORMAT(p.date, '%%Y-%%m-01'),
                '{platform}',
                '{filter_field}',
                COALESCE(m.{filter_field}, ''),
                COALESCE(p.{product_sku_field}, ''),
                COALESCE(p.category, ''),
                COALESCE(p.portfolio, ''),
                COALESCE(p.subcategory, ''),
                SUM(p.revenue),
                SUM(p.units_sold),
                SUM(p.page_views),
                SUM(p.ad_spend)
            FROM `{product_tbl}` p
            INNER JOIN `{mapping_table}` m
                ON m.user_id = p.user_id AND m.{sku_field} = p.{product_sku_field}
            WHERE p.user_id = %s
                AND p.platform = '{platform}'
                AND p.{product_sku_field} IS NOT NULL
                AND p.{product_sku_field} != ''
                AND m.{filter_field} IS NOT NULL
                AND m.{filter_field} != ''
                {month_filter_sql}
            GROUP BY
                p.user_id,
                DATE_FORMAT(p.date, '%%Y-%%m-01'),
                m.{filter_field},
                p.{product_sku_field},
                p.category,
                p.portfolio,
                p.subcategory
        """

    rows_written = 0
    params = [user.id, *month_params]
    with transaction.atomic():
        with connection.cursor() as cursor:
            _delete_monthly_activity_rows(cursor, tbl, user.id, month_starts)
            for field in FILTER_SUMMARY_FIELDS:
                cursor.execute(insert_sql(az_map_tbl, "asin", "asin", "Amazon", field), params)
                rows_written += max(cursor.rowcount, 0)
                cursor.execute(insert_sql(fk_map_tbl, "fsn", "fsn", "Flipkart", field), params)
                rows_written += max(cursor.rowcount, 0)

    return {
        "rows_written": rows_written,
        "months_scoped": month_strs,
    }


def rebuild_mapping_filter_daily_summary_for_user(user, *, only_dates=None):
    """
    Rebuild compact one-filter-at-a-time daily aggregates for mapping dimensions.
    """
    only_dates = sorted({str(d) for d in (only_dates or []) if str(d).strip()})

    with transaction.atomic():
        date_filter = ""
        if only_dates:
            placeholders = ", ".join(["%s"] * len(only_dates))
            date_filter = f" AND p.date IN ({placeholders})"

        tbl = DashboardMappingFilterDailySummary._meta.db_table
        az_tbl = ProcessedDashboardData._meta.db_table
        fk_tbl = FlipkartProcessedDashboardData._meta.db_table
        az_map_tbl = CategoryMapping._meta.db_table
        fk_map_tbl = FlipkartCategoryMap._meta.db_table

        def insert_sql(source_table, mapping_table, sku_field, platform, filter_field):
            return f"""
                INSERT INTO `{tbl}` (
                    `user_id`, `date`, `platform`, `filter_name`, `filter_value`,
                    `category`, `portfolio`, `subcategory`,
                    `revenue`, `orders`, `units`, `pageviews`,
                    `total_spend`, `spend_sp`, `spend_sb`, `spend_sd`
                )
                SELECT
                    p.user_id,
                    p.date,
                    '{platform}',
                    '{filter_field}',
                    COALESCE(m.{filter_field}, ''),
                    '',
                    '',
                    '',
                    SUM(p.revenue),
                    SUM(p.orders),
                    SUM(p.units),
                    SUM(p.pageviews),
                    SUM(p.total_spend),
                    SUM(p.spend_sp),
                    SUM(p.spend_sb),
                    SUM(p.spend_sd)
                FROM `{source_table}` p
                INNER JOIN `{mapping_table}` m
                    ON m.user_id = p.user_id AND m.{sku_field} = p.{sku_field}
                WHERE p.user_id = %s{date_filter}
                    AND m.{filter_field} IS NOT NULL
                    AND m.{filter_field} != ''
                GROUP BY
                    p.user_id,
                    p.date,
                    m.{filter_field}
            """

        params = [user.id, *only_dates]
        rows_written = 0
        with connection.cursor() as cursor:
            _delete_summary_rows(cursor, tbl, user.id, only_dates)
            for field in FILTER_SUMMARY_FIELDS:
                cursor.execute(insert_sql(az_tbl, az_map_tbl, "asin", "Amazon", field), params)
                rows_written += max(cursor.rowcount, 0)
                cursor.execute(insert_sql(fk_tbl, fk_map_tbl, "fsn", "Flipkart", field), params)
                rows_written += max(cursor.rowcount, 0)

    return {
        "rows_written": rows_written,
        "dates_scoped": only_dates,
    }
