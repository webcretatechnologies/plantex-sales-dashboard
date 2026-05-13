import os
import tempfile
from datetime import datetime
from uuid import uuid4

from django.conf import settings
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from celery.result import AsyncResult

from apps.accounts.utils import get_logged_in_user
from .models import UploadLog
from .tasks import process_upload_file_task


# Flipkart file types for pipeline detection
FK_FILE_TYPES = {
    "fk_search_traffic",
    "fk_category",
    "fk_price",
    "fk_pla",
    "fk_fba_stock",
    "fk_flex_stock",
    "fk_inventory",
}

UPLOAD_TYPE_LABELS = {
    "sales": "Daily Sales",
    "category": "Category Mapping",
    "spend": "Ads Spends",
    "price": "Pricing Data",
    "fba_stock": "FBA Stock File",
    "flex_stock": "Flex Stock File",
    "fk_search_traffic": "FK Search Traffic",
    "fk_category": "FK Category",
    "fk_price": "FK Price",
    "fk_pla": "FK PLA",
    "fk_fba_stock": "FK FBA Stock File",
    "fk_flex_stock": "FK Flex Stock File",
    "fk_inventory": "FK Inventory File",
}


UPLOAD_ROOT_DIR = os.getenv(
    "UPLOAD_ROOT_DIR", os.path.join(settings.BASE_DIR, "uploads")
)

UPLOAD_SUBDIRS = {
    "sales": "sales",
    "category": "category",
    "spend": "spend",
    "price": "price",
    "fba_stock": "fba_stock",
    "flex_stock": "flex_stock",
    "fk_search_traffic": "search_traffic",
    "fk_category": "category",
    "fk_price": "price",
    "fk_pla": "pla",
    "fk_fba_stock": "fba_stock",
    "fk_flex_stock": "flex_stock",
    "fk_inventory": "inventory",
}


def _get_upload_dir(file_type):
    platform_dir = "flipkart" if file_type in FK_FILE_TYPES else "amazon"
    category_dir = UPLOAD_SUBDIRS.get(file_type, "misc")
    upload_dir = os.path.join(UPLOAD_ROOT_DIR, platform_dir, category_dir)
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


def _save_upload_to_disk(file_obj, file_type):
    """
    Save an uploaded file to a shared directory on disk
    so it can be passed to a Celery worker by path.
    Returns the absolute path to the saved file.
    """
    upload_dir = _get_upload_dir(file_type)
    suffix = os.path.splitext(file_obj.name)[1] or ""
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
    unique_prefix = f"upload_{ts}_{uuid4().hex[:8]}_"
    fd, path = tempfile.mkstemp(suffix=suffix, prefix=unique_prefix, dir=upload_dir)
    with os.fdopen(fd, "wb") as f:
        for chunk in file_obj.chunks():
            f.write(chunk)
    return path


class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        user = get_logged_in_user(request)
        if not user:
            return Response({"error": "Not authenticated"}, status=401)

        # RBAC Check
        if not user.is_main_user:
            if (
                not user.role
                or not user.role.features.filter(code_name="upload_data").exists()
            ):
                return Response({"error": "Permission Denied"}, status=403)

        # Use data_owner (the main user) for data associations to avoid duplicates
        data_owner = user.created_by if user.created_by else user

        file_obj = request.FILES.get("file")
        file_type = request.data.get("file_type")  # 'sales', 'spend', 'category', etc.
        date_str = request.data.get("date", "")

        if not file_obj or not file_type:
            return Response({"error": "file and file_type are required"}, status=400)

        is_last = request.data.get("is_last") == "true"
        filename = os.path.basename(file_obj.name)

        if file_type == "sales" and not date_str:
            date_str = os.path.splitext(filename)[0][:10]

        is_flipkart = file_type in FK_FILE_TYPES

        # Save uploaded file to disk for Celery worker access
        try:
            file_path = _save_upload_to_disk(file_obj, file_type)
        except Exception as e:
            return Response({"error": f"Failed to save file: {str(e)}"}, status=500)

        upload_log = UploadLog.objects.create(
            data_owner=data_owner,
            uploaded_by=user,
            file_name=filename,
            upload_type=UPLOAD_TYPE_LABELS.get(file_type, file_type),
            status=UploadLog.STATUS_QUEUED,
            message="Queued for processing.",
        )

        # Dispatch Celery task
        try:
            task = process_upload_file_task.delay(
                file_path=file_path,
                file_type=file_type,
                user_id=user.id,
                data_owner_id=data_owner.id,
                upload_log_id=upload_log.id,
                date_str=date_str,
                is_last=is_last,
                is_flipkart=is_flipkart,
            )
        except Exception as exc:
            try:
                os.remove(file_path)
            except OSError:
                pass
            upload_log.status = UploadLog.STATUS_ERROR
            upload_log.message = f"Failed to queue task: {str(exc)}"
            upload_log.save(update_fields=["status", "message", "updated_at"])
            return Response({"error": "Failed to queue file for processing."}, status=500)

        return Response(
            {
                "message": "File queued for processing",
                "task_id": task.id,
            },
            status=202,
        )


class UploadTaskStatusView(APIView):
    """Poll Celery task state for an upload processing task."""

    authentication_classes = []

    def get(self, request, task_id, *args, **kwargs):
        task = AsyncResult(task_id)

        if task.state == "PENDING":
            return Response({"status": "processing", "state": "PENDING"})
        elif task.state == "SUCCESS":
            result = task.result or {}
            return Response(
                {
                    "status": result.get("status", "success"),
                    "file_type": result.get("file_type", ""),
                    "is_last": result.get("is_last", False),
                    "message": result.get("message", ""),
                }
            )
        elif task.state == "FAILURE":
            return Response(
                {
                    "status": "error",
                    "message": str(task.info),
                },
                status=500,
            )
        else:
            return Response({"status": "processing", "state": task.state})
