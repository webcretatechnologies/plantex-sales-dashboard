from django.db import transaction
from django.db.models import Sum, Value, CharField

from apps.dashboard.models import (
    DashboardDailySummary,
    ProcessedDashboardData,
    FlipkartProcessedDashboardData,
)


def rebuild_daily_summary_for_user(user, *, only_dates=None):
    """
    Rebuild day-level pre-aggregates for a user.
    If only_dates is provided, only those dates are rebuilt.
    """
    only_dates = {str(d) for d in (only_dates or []) if str(d).strip()}

    az_qs = ProcessedDashboardData.objects.filter(user=user)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=user)

    if only_dates:
        az_qs = az_qs.filter(date__in=only_dates)
        fk_qs = fk_qs.filter(date__in=only_dates)

    az_rows = az_qs.values("date", "category", "portfolio", "subcategory").annotate(
        revenue=Sum("revenue"),
        orders=Sum("orders"),
        units=Sum("units"),
        pageviews=Sum("pageviews"),
        total_spend=Sum("total_spend"),
        spend_sp=Sum("spend_sp"),
        spend_sb=Sum("spend_sb"),
        spend_sd=Sum("spend_sd"),
        platform=Value("Amazon", output_field=CharField()),
    )
    fk_rows = fk_qs.values("date", "category", "portfolio", "subcategory").annotate(
        revenue=Sum("revenue"),
        orders=Sum("orders"),
        units=Sum("units"),
        pageviews=Sum("pageviews"),
        total_spend=Sum("total_spend"),
        spend_sp=Sum("spend_sp"),
        spend_sb=Sum("spend_sb"),
        spend_sd=Sum("spend_sd"),
        platform=Value("Flipkart", output_field=CharField()),
    )

    with transaction.atomic():
        scoped = DashboardDailySummary.objects.filter(user=user)
        if only_dates:
            scoped = scoped.filter(date__in=only_dates)
        scoped.delete()

        inserts = []
        for row in list(az_rows) + list(fk_rows):
            inserts.append(
                DashboardDailySummary(
                    user=user,
                    date=row.get("date"),
                    platform=row.get("platform") or "",
                    category=row.get("category") or "",
                    portfolio=row.get("portfolio") or "",
                    subcategory=row.get("subcategory") or "",
                    revenue=float(row.get("revenue") or 0),
                    orders=int(row.get("orders") or 0),
                    units=int(row.get("units") or 0),
                    pageviews=int(row.get("pageviews") or 0),
                    total_spend=float(row.get("total_spend") or 0),
                    spend_sp=float(row.get("spend_sp") or 0),
                    spend_sb=float(row.get("spend_sb") or 0),
                    spend_sd=float(row.get("spend_sd") or 0),
                )
            )

        if inserts:
            DashboardDailySummary.objects.bulk_create(inserts, batch_size=2000)

    return {
        "rows_written": len(inserts),
        "dates_scoped": sorted(only_dates) if only_dates else [],
    }
