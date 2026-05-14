import os
import shutil
import tempfile
from datetime import datetime
from uuid import uuid4
import re

from django.conf import settings
from django.core.cache import cache
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from celery.result import AsyncResult

from apps.accounts.authentication import SessionUserIdAuthentication
from apps.accounts.utils import get_logged_in_user
from .models import UploadLog
from .schema import parse_sales_upload_date, validate_file_type
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
ALLOWED_UPLOAD_EXTENSIONS = {".csv", ".xlsx", ".xls", ".xlsm"}

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


def _track_upload_task(request, task_id):
    task_ids = request.session.get("upload_task_ids", [])
    if task_id not in task_ids:
        task_ids.append(task_id)
        request.session["upload_task_ids"] = task_ids[-500:]
        request.session.modified = True


def _is_allowed_upload_task(request, task_id):
    task_ids = request.session.get("upload_task_ids", [])
    return task_id in task_ids


def _get_upload_dir(file_type):
    platform_dir = "flipkart" if file_type in FK_FILE_TYPES else "amazon"
    category_dir = UPLOAD_SUBDIRS.get(file_type, "misc")
    upload_dir = os.path.join(UPLOAD_ROOT_DIR, platform_dir, category_dir)
    os.makedirs(upload_dir, exist_ok=True)
    return upload_dir


def _upload_batch_key(batch_id, suffix):
    return f"upload_batch_{batch_id}_{suffix}"


def _register_upload_batch(batch_id, *, batch_total, user_id, data_owner_id, is_flipkart):
    ttl = 86400
    meta_key = _upload_batch_key(batch_id, "meta")
    expected_key = _upload_batch_key(batch_id, "expected_total")
    completed_key = _upload_batch_key(batch_id, "completed_total")
    failed_key = _upload_batch_key(batch_id, "failed_total")
    finalized_key = _upload_batch_key(batch_id, "finalized")

    existing_expected = cache.get(expected_key)
    if existing_expected is not None and int(existing_expected) != int(batch_total):
        raise ValueError("Invalid batch_total for existing upload batch.")

    cache.set(
        meta_key,
        {
            "user_id": int(user_id),
            "data_owner_id": int(data_owner_id),
            "is_flipkart": bool(is_flipkart),
        },
        timeout=ttl,
    )
    cache.set(expected_key, int(batch_total), timeout=ttl)

    if existing_expected is None:
        cache.set(completed_key, 0, timeout=ttl)
        cache.set(failed_key, 0, timeout=ttl)
        cache.delete(finalized_key)


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
    os.close(fd)

    temp_path_getter = getattr(file_obj, "temporary_file_path", None)
    if callable(temp_path_getter):
        source_path = temp_path_getter()
        shutil.move(source_path, path)
        return path

    with open(path, "wb") as f:
        for chunk in file_obj.chunks():
            f.write(chunk)
    return path


def _is_allowed_tabular_extension(filename):
    ext = os.path.splitext(str(filename or ""))[1].lower()
    return ext in ALLOWED_UPLOAD_EXTENSIONS


class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    authentication_classes = [SessionUserIdAuthentication]

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
        batch_id = str(request.data.get("batch_id") or "").strip()
        batch_total_raw = request.data.get("batch_total")
        batch_total = None

        if not file_obj or not file_type:
            return Response({"error": "file and file_type are required"}, status=400)
        try:
            validate_file_type(file_type)
        except ValueError as exc:
            return Response({"error": str(exc)}, status=400)

        if batch_id:
            if not re.fullmatch(r"[A-Za-z0-9_-]{8,64}", batch_id):
                return Response({"error": "Invalid batch_id."}, status=400)
            try:
                batch_total = int(batch_total_raw)
            except (TypeError, ValueError):
                return Response({"error": "batch_total must be a positive integer."}, status=400)
            if batch_total <= 0:
                return Response({"error": "batch_total must be a positive integer."}, status=400)

        is_last = request.data.get("is_last") == "true"
        filename = os.path.basename(file_obj.name)
        if not _is_allowed_tabular_extension(filename):
            return Response(
                {"error": "Unsupported file format. Upload CSV or Excel (.xlsx/.xls/.xlsm)."},
                status=400,
            )

        if file_type == "sales" and not date_str:
            date_str = os.path.splitext(filename)[0][:10]
        if file_type == "sales":
            try:
                parse_sales_upload_date(date_str)
            except ValueError as exc:
                return Response({"error": str(exc)}, status=400)

        is_flipkart = file_type in FK_FILE_TYPES
        if batch_id:
            try:
                _register_upload_batch(
                    batch_id,
                    batch_total=batch_total,
                    user_id=user.id,
                    data_owner_id=data_owner.id,
                    is_flipkart=is_flipkart,
                )
            except ValueError as exc:
                return Response({"error": str(exc)}, status=400)

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
            timeout = max(getattr(settings, "UPLOAD_TASK_TIMEOUT_SECONDS", 1800), 60)
            soft_timeout = max(timeout - 30, 60)
            task = process_upload_file_task.apply_async(
                kwargs={
                    "file_path": file_path,
                    "file_type": file_type,
                    "user_id": user.id,
                    "data_owner_id": data_owner.id,
                    "upload_log_id": upload_log.id,
                    "date_str": date_str,
                    "is_last": is_last,
                    "is_flipkart": is_flipkart,
                    "batch_id": batch_id,
                    "batch_total": batch_total,
                },
                time_limit=timeout,
                soft_time_limit=soft_timeout,
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

        cache.set(f"upload_task_owner_{task.id}", data_owner.id, timeout=86400)
        _track_upload_task(request, task.id)

        return Response(
            {
                "message": "File queued for processing",
                "task_id": task.id,
            },
            status=202,
        )


class UploadTaskStatusView(APIView):
    """Poll Celery task state for an upload processing task."""

    authentication_classes = [SessionUserIdAuthentication]

    def get(self, request, task_id, *args, **kwargs):
        user = get_logged_in_user(request)
        if not user:
            return Response({"error": "Not authenticated"}, status=401)

        if not _is_allowed_upload_task(request, task_id):
            return Response({"error": "Permission Denied"}, status=403)

        data_owner = user.created_by if user.created_by else user
        task_owner_id = cache.get(f"upload_task_owner_{task_id}")
        if task_owner_id is None:
            return Response({"error": "Task not found or expired"}, status=404)
        if int(task_owner_id) != int(data_owner.id):
            return Response({"error": "Permission Denied"}, status=403)

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
