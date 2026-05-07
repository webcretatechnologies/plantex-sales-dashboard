from django.utils import timezone


def stock_upload_alert(request):
    """
    Global banner context:
    show a warning when today's FBA/Flex stock files are not yet uploaded.
    """
    user_id = request.session.get("user_id")
    if not user_id:
        return {
            "show_stock_upload_banner": False,
            "stock_upload_banner_date": None,
        }

    try:
        from apps.accounts.models import Users
        from apps.dashboard.models import FBAStockData, FlexStockData
        from apps.upload.models import UploadLog

        user = Users.objects.get(pk=user_id)
        data_owner = user.created_by if user.created_by else user
        today = timezone.localdate()

        has_fba_today = FBAStockData.objects.filter(user=data_owner, date=today).exists()
        has_flex_today = FlexStockData.objects.filter(user=data_owner, date=today).exists()

        has_fba_upload_today = UploadLog.objects.filter(
            data_owner=data_owner,
            upload_type="FBA Stock File",
            status=UploadLog.STATUS_SUCCESS,
            created_at__date=today,
        ).exists()
        has_flex_upload_today = UploadLog.objects.filter(
            data_owner=data_owner,
            upload_type="Flex Stock File",
            status=UploadLog.STATUS_SUCCESS,
            created_at__date=today,
        ).exists()

        fba_ready = has_fba_today or has_fba_upload_today
        flex_ready = has_flex_today or has_flex_upload_today

        return {
            "show_stock_upload_banner": not (fba_ready and flex_ready),
            "stock_upload_banner_date": today,
            "has_fba_stock_today": has_fba_today,
            "has_flex_stock_today": has_flex_today,
            "has_fba_upload_today": has_fba_upload_today,
            "has_flex_upload_today": has_flex_upload_today,
        }
    except Exception:
        return {
            "show_stock_upload_banner": False,
            "stock_upload_banner_date": None,
        }
