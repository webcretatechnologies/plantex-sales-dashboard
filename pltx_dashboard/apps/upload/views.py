import os
import tempfile
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.parsers import MultiPartParser, FormParser
from django.http import JsonResponse
from celery.result import AsyncResult

from apps.accounts.utils import get_logged_in_user
from .tasks import process_upload_file_task


# Flipkart file types for pipeline detection
FK_FILE_TYPES = {
    'fk_search_traffic', 'fk_category', 'fk_price',
    'fk_pca', 'fk_pla', 'fk_sales_invoice', 'fk_coupon',
}


# Shared temp directory for upload files — must be accessible by both
# the web process (which saves files) and the Celery worker (which reads them).
# In Docker, this is a shared volume mounted at /tmp/upload_queue.
UPLOAD_TEMP_DIR = os.path.join(tempfile.gettempdir(), 'upload_queue')


def _save_upload_to_disk(file_obj):
    """
    Save an in-memory uploaded file to a temporary location on disk
    so it can be passed to a Celery worker by path.
    Returns the absolute path to the saved file.
    """
    os.makedirs(UPLOAD_TEMP_DIR, exist_ok=True)
    suffix = os.path.splitext(file_obj.name)[1] or ''
    fd, path = tempfile.mkstemp(suffix=suffix, prefix='upload_', dir=UPLOAD_TEMP_DIR)
    with os.fdopen(fd, 'wb') as f:
        for chunk in file_obj.chunks():
            f.write(chunk)
    return path



class FileUploadView(APIView):
    parser_classes = (MultiPartParser, FormParser)
    authentication_classes = []

    def post(self, request, *args, **kwargs):
        user = get_logged_in_user(request)
        if not user:
            return Response({'error': 'Not authenticated'}, status=401)

        # RBAC Check
        if not user.is_main_user:
            if not user.role or not user.role.features.filter(code_name='upload_data').exists():
                return Response({'error': 'Permission Denied'}, status=403)

        # Use data_owner (the main user) for data associations to avoid duplicates
        data_owner = user.created_by if user.created_by else user

        file_obj = request.FILES.get('file')
        file_type = request.data.get('file_type')  # 'sales', 'spend', 'category', etc.
        date_str = request.data.get('date', '')

        if not file_obj or not file_type:
            return Response({'error': 'file and file_type are required'}, status=400)

        is_last = request.data.get('is_last') == 'true'
        filename = os.path.basename(file_obj.name)

        if file_type == 'sales' and not date_str:
            date_str = os.path.splitext(filename)[0][:10]

        is_flipkart = file_type in FK_FILE_TYPES

        # Save uploaded file to disk for Celery worker access
        try:
            file_path = _save_upload_to_disk(file_obj)
        except Exception as e:
            return Response({'error': f'Failed to save file: {str(e)}'}, status=500)

        # Dispatch Celery task
        task = process_upload_file_task.delay(
            file_path=file_path,
            file_type=file_type,
            user_id=user.id,
            data_owner_id=data_owner.id,
            date_str=date_str,
            is_last=is_last,
            is_flipkart=is_flipkart,
        )

        return Response({
            'message': 'File queued for processing',
            'task_id': task.id,
        }, status=202)


class UploadTaskStatusView(APIView):
    """Poll Celery task state for an upload processing task."""
    authentication_classes = []

    def get(self, request, task_id, *args, **kwargs):
        task = AsyncResult(task_id)

        if task.state == 'PENDING':
            return Response({'status': 'processing', 'state': 'PENDING'})
        elif task.state == 'SUCCESS':
            result = task.result or {}
            return Response({
                'status': result.get('status', 'success'),
                'file_type': result.get('file_type', ''),
                'is_last': result.get('is_last', False),
                'message': result.get('message', ''),
            })
        elif task.state == 'FAILURE':
            return Response({
                'status': 'error',
                'message': str(task.info),
            }, status=500)
        else:
            return Response({'status': 'processing', 'state': task.state})
