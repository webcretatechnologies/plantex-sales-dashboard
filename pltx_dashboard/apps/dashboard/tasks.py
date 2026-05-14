import logging

from celery import shared_task
from django.conf import settings

from apps.accounts.models import Users
from apps.dashboard.services.materialized_cache import cleanup_materialized_summaries
from apps.dashboard.services.warmup import prime_dashboard_payloads_for_user

logger = logging.getLogger(__name__)


@shared_task
def cleanup_dashboard_materialized_summaries_task(
    retention_days=None, max_rows_per_view=None, dry_run=False
):
    stats = cleanup_materialized_summaries(
        retention_days=retention_days
        if retention_days is not None
        else getattr(settings, "DASHBOARD_SUMMARY_RETENTION_DAYS", 14),
        max_rows_per_view=max_rows_per_view
        if max_rows_per_view is not None
        else getattr(settings, "DASHBOARD_SUMMARY_MAX_ROWS_PER_VIEW", 800),
        dry_run=dry_run,
    )
    logger.info("[DashboardSummaryCleanup] %s", stats)
    return stats


@shared_task
def warmup_dashboard_payloads_task(data_owner_id, filter_sets=None, view_types=None):
    try:
        user = Users.objects.get(pk=data_owner_id)
    except Users.DoesNotExist:
        logger.warning(
            "[DashboardWarmup] Skipping warmup; user %s not found.", data_owner_id
        )
        return {"computed": 0, "error": "user-not-found"}

    max_filter_sets = getattr(settings, "DASHBOARD_WARMUP_MAX_FILTER_SETS", 7)
    stats = prime_dashboard_payloads_for_user(
        user,
        filter_sets=filter_sets,
        view_types=view_types,
        max_filter_sets=max_filter_sets,
    )
    logger.info("[DashboardWarmup] user=%s stats=%s", data_owner_id, stats)
    return stats
