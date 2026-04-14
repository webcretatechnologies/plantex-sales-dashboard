"""
Materialized View Refresh Service
==================================
Pre-computes the full dashboard payload for a given user — both the
unfiltered "all time" view AND every standard date-range filter variant.

Called automatically at the end of generate_dashboard_data() and can also
be triggered manually via the 'refresh_dashboard_cache' management command.
"""

import json
import logging

import pandas as pd
from django.utils import timezone

from apps.dashboard.materialized_models import (
    BusinessDashboardCache,
    CategoryDashboardCache,
    CeoDashboardCache,
    DashboardFilterCache,
    STANDARD_DATE_RANGES,
)
from apps.dashboard.models import ProcessedDashboardData, SpendData
from apps.dashboard.services.analytics_services import get_dashboard_payload

logger = logging.getLogger(__name__)


from apps.dashboard.utils import serialize_payload


def refresh_materialized_views(user):
    """
    Compute the dashboard payload for *user* with NO filters and with
    every standard date-range filter, then store all results in cache.

    The data is loaded from the DB **once**, then reused across all
    filter variants — so each additional variant adds very little time.
    """
    data_owner = user.created_by if user.created_by else user

    qs = ProcessedDashboardData.objects.filter(user=data_owner).values()
    if not qs:
        # No data → clear all caches
        CeoDashboardCache.objects.filter(user=data_owner).delete()
        BusinessDashboardCache.objects.filter(user=data_owner).delete()
        CategoryDashboardCache.objects.filter(user=data_owner).delete()
        DashboardFilterCache.objects.filter(user=data_owner).delete()
        logger.info("[MaterializedViews] No data for user %s — caches cleared.", data_owner)
        return

    # ── Load data ONCE ──────────────────────────────────────
    df = pd.DataFrame(list(qs))
    spend_qs = SpendData.objects.filter(user=data_owner).values()
    spend_df = pd.DataFrame(list(spend_qs)) if spend_qs else pd.DataFrame()

    now = timezone.now()

    # ── 1. All-time (unfiltered) payload ────────────────────
    payload_all = get_dashboard_payload(df, spend_df, filters={}, user=data_owner)
    payload_all_clean = serialize_payload(payload_all)

    CeoDashboardCache.objects.update_or_create(
        user=data_owner,
        defaults={'payload_json': payload_all_clean, 'refreshed_at': now},
    )
    BusinessDashboardCache.objects.update_or_create(
        user=data_owner,
        defaults={'payload_json': payload_all_clean, 'refreshed_at': now},
    )
    CategoryDashboardCache.objects.update_or_create(
        user=data_owner,
        defaults={'payload_json': payload_all_clean, 'refreshed_at': now},
    )
    logger.info("[MaterializedViews] All-time cache refreshed for user %s.", data_owner)

    # ── 2. Standard date-range variants ─────────────────────
    for date_range in STANDARD_DATE_RANGES:
        try:
            filters = {'date_range': date_range}
            payload = get_dashboard_payload(df, spend_df, filters=filters, user=data_owner)
            payload_clean = serialize_payload(payload)

            DashboardFilterCache.objects.update_or_create(
                user=data_owner,
                filter_key=date_range,
                defaults={'payload_json': payload_clean, 'refreshed_at': now},
            )
            logger.info("[MaterializedViews]   ✓ %s cached for user %s.", date_range, data_owner)
        except Exception as exc:
            logger.warning(
                "[MaterializedViews]   ✗ %s failed for user %s: %s",
                date_range, data_owner, exc,
            )

    logger.info(
        "[MaterializedViews] All caches refreshed for user %s at %s.",
        data_owner, now.isoformat(),
    )
