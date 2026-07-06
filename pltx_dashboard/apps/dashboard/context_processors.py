import datetime

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone


def _today_bounds(today):
    start = datetime.datetime.combine(today, datetime.time.min)
    if getattr(settings, "USE_TZ", False):
        start = timezone.make_aware(start, timezone.get_current_timezone())
    return start, start + datetime.timedelta(days=1)


def stock_upload_alert(request):
    """
    Global banner context:
    show a warning when today's stock files required for inventory health
    are not yet uploaded for Amazon and/or Flipkart.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return {
            "show_stock_upload_banner": False,
            "stock_upload_banner_date": None,
        }

    try:
        from apps.accounts.models import Users
        from apps.dashboard.models import (
            FBAStockData,
            FlexStockData,
            FlipkartInventoryStock,
            Flipkartfba,
        )
        from apps.upload.models import UploadLog

        user = Users.objects.get(pk=user_id)
        data_owner = user.created_by if user.created_by else user
        today = timezone.localdate()
        cache_key = f"stock_upload_alert_v2_{data_owner.id}_{today.isoformat()}"
        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            return cached
        today_start, tomorrow_start = _today_bounds(today)

        def successful_upload_today(upload_type):
            return UploadLog.objects.filter(
                data_owner=data_owner,
                upload_type=upload_type,
                status=UploadLog.STATUS_SUCCESS,
                created_at__gte=today_start,
                created_at__lt=tomorrow_start,
            ).exists()

        # Amazon readiness (today's stock snapshot expected)
        has_fba_today = FBAStockData.objects.filter(user=data_owner, date=today).exists()
        has_flex_today = FlexStockData.objects.filter(user=data_owner, date=today).exists()
        has_fba_upload_today = successful_upload_today("FBA Stock File")
        has_flex_upload_today = successful_upload_today("Flex Stock File")
        amazon_ready = (has_fba_today or has_fba_upload_today) and (
            has_flex_today or has_flex_upload_today
        )

        # Flipkart readiness:
        # - FK FBA is date-based, so we can validate "today" from data or upload log.
        # - FK Inventory is snapshot-level (no date field), so we rely on
        #   successful upload logs for today.
        has_fk_fba_today = Flipkartfba.objects.filter(user=data_owner, date=today).exists()
        has_fk_inventory_rows = FlipkartInventoryStock.objects.filter(user=data_owner).exists()

        has_fk_fba_upload_today = successful_upload_today("FK FBA Stock File")
        has_fk_inventory_upload_today = successful_upload_today("FK Inventory File")

        flipkart_ready = (has_fk_fba_today or has_fk_fba_upload_today) and has_fk_inventory_upload_today

        result = {
            "show_stock_upload_banner": not (amazon_ready and flipkart_ready),
            "stock_upload_banner_date": today,
            "has_fba_stock_today": has_fba_today,
            "has_flex_stock_today": has_flex_today,
            "has_fba_upload_today": has_fba_upload_today,
            "has_flex_upload_today": has_flex_upload_today,
            "has_fk_fba_stock_today": has_fk_fba_today,
            "has_fk_inventory_rows": has_fk_inventory_rows,
            "has_fk_fba_upload_today": has_fk_fba_upload_today,
            "has_fk_inventory_upload_today": has_fk_inventory_upload_today,
        }
        cache.set(cache_key, result, timeout=60)
        return result
    except Exception:
        return {
            "show_stock_upload_banner": False,
            "stock_upload_banner_date": None,
        }
