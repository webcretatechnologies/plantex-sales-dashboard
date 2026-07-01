from django.db import connection, transaction

from apps.dashboard.models import (
    DashboardProductDailySummary,
    FlipkartProcessedDashboardData,
    FlipkartSearchTraffic,
    ProcessedDashboardData,
)


def rebuild_product_daily_summary_for_user(user, *, only_dates=None):
    """
    Rebuild product/day pre-aggregates for a user from processed dashboard rows.

    Flipkart product clicks are preserved from FlipkartSearchTraffic because the
    processed dashboard table intentionally keeps only dashboard-facing metrics.
    """
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}

    with transaction.atomic():
        scoped = DashboardProductDailySummary.objects.filter(user=user)
        if only_dates:
            scoped = scoped.filter(date__in=only_dates)
        scoped.delete()

        date_filter = ""
        if only_dates:
            placeholders = ", ".join(["%s"] * len(only_dates))
            date_filter = f" AND date IN ({placeholders})"

        tbl = DashboardProductDailySummary._meta.db_table
        az_tbl = ProcessedDashboardData._meta.db_table
        fk_tbl = FlipkartProcessedDashboardData._meta.db_table
        traffic_tbl = FlipkartSearchTraffic._meta.db_table

        az_sql = f"""
            INSERT INTO `{tbl}` (
                `user_id`, `date`, `platform`, `sku`, `asin`, `fsn`,
                `portfolio`, `category`, `subcategory`,
                `revenue`, `units_sold`, `page_views`, `orders`, `ad_spend`,
                `spend_sp`, `spend_sb`, `spend_sd`,
                `product_clicks`, `sales`
            )
            SELECT
                user_id,
                date,
                'Amazon',
                asin,
                asin,
                NULL,
                MAX(COALESCE(portfolio, '')),
                MAX(COALESCE(category, '')),
                MAX(COALESCE(subcategory, '')),
                SUM(revenue),
                SUM(units),
                SUM(pageviews),
                SUM(orders),
                SUM(total_spend),
                SUM(spend_sp),
                SUM(spend_sb),
                SUM(spend_sd),
                0,
                SUM(units)
            FROM `{az_tbl}`
            WHERE user_id = %s{date_filter}
            GROUP BY
                user_id, date, asin
            ON DUPLICATE KEY UPDATE
                asin = VALUES(asin),
                fsn = VALUES(fsn),
                portfolio = VALUES(portfolio),
                category = VALUES(category),
                subcategory = VALUES(subcategory),
                revenue = VALUES(revenue),
                units_sold = VALUES(units_sold),
                page_views = VALUES(page_views),
                orders = VALUES(orders),
                ad_spend = VALUES(ad_spend),
                spend_sp = VALUES(spend_sp),
                spend_sb = VALUES(spend_sb),
                spend_sd = VALUES(spend_sd),
                product_clicks = VALUES(product_clicks),
                sales = VALUES(sales)
        """

        fk_processed_date_filter = date_filter.replace("date", "p.date")
        fk_traffic_date_filter = date_filter.replace("date", "date")
        fk_sql = f"""
            INSERT INTO `{tbl}` (
                `user_id`, `date`, `platform`, `sku`, `asin`, `fsn`,
                `portfolio`, `category`, `subcategory`,
                `revenue`, `units_sold`, `page_views`, `orders`, `ad_spend`,
                `spend_sp`, `spend_sb`, `spend_sd`,
                `product_clicks`, `sales`
            )
            SELECT
                p.user_id,
                p.date,
                'Flipkart',
                p.fsn,
                NULL,
                p.fsn,
                MAX(COALESCE(p.portfolio, '')),
                MAX(COALESCE(p.category, '')),
                MAX(COALESCE(p.subcategory, '')),
                SUM(p.revenue),
                SUM(p.units),
                SUM(p.pageviews),
                SUM(p.orders),
                SUM(p.total_spend),
                SUM(p.spend_sp),
                SUM(p.spend_sb),
                SUM(p.spend_sd),
                COALESCE(MAX(t.product_clicks), 0),
                COALESCE(MAX(t.sales), SUM(p.units))
            FROM `{fk_tbl}` p
            LEFT JOIN (
                SELECT
                    user_id,
                    date,
                    fsn,
                    SUM(product_clicks) AS product_clicks,
                    SUM(sales) AS sales
                FROM `{traffic_tbl}`
                WHERE user_id = %s{fk_traffic_date_filter}
                GROUP BY user_id, date, fsn
            ) t ON t.user_id = p.user_id AND t.date = p.date AND t.fsn = p.fsn
            WHERE p.user_id = %s{fk_processed_date_filter}
            GROUP BY
                p.user_id, p.date, p.fsn
            ON DUPLICATE KEY UPDATE
                asin = VALUES(asin),
                fsn = VALUES(fsn),
                portfolio = VALUES(portfolio),
                category = VALUES(category),
                subcategory = VALUES(subcategory),
                revenue = VALUES(revenue),
                units_sold = VALUES(units_sold),
                page_views = VALUES(page_views),
                orders = VALUES(orders),
                ad_spend = VALUES(ad_spend),
                spend_sp = VALUES(spend_sp),
                spend_sb = VALUES(spend_sb),
                spend_sd = VALUES(spend_sd),
                product_clicks = VALUES(product_clicks),
                sales = VALUES(sales)
        """

        az_params = [user.id]
        if only_dates:
            az_params.extend(list(only_dates))

        fk_params = [user.id]
        if only_dates:
            fk_params.extend(list(only_dates))
        fk_params.append(user.id)
        if only_dates:
            fk_params.extend(list(only_dates))

        rows_written = 0
        with connection.cursor() as cursor:
            cursor.execute(az_sql, az_params)
            rows_written += max(cursor.rowcount, 0)
            cursor.execute(fk_sql, fk_params)
            rows_written += max(cursor.rowcount, 0)

    return {
        "rows_written": rows_written,
        "dates_scoped": sorted(only_dates) if only_dates else [],
    }
