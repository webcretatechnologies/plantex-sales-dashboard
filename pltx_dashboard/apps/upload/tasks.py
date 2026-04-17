import os
import tempfile
import logging
from celery import shared_task
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)


def _send_ws(user_id, message, status):
    """Send a WebSocket progress message to the user's channel group."""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            f'user_{user_id}',
            {
                'type': 'upload_progress',
                'message': message,
                'status': status,
            }
        )
    except Exception as exc:
        logger.warning("[UploadTask] WebSocket send failed: %s", exc)


@shared_task(bind=True)
def process_upload_file_task(self, file_path, file_type, user_id, data_owner_id,
                             date_str='', is_last=False, is_flipkart=False):
    """
    Celery task that processes a single uploaded file.

    Parameters
    ----------
    file_path : str
        Absolute path to the uploaded file saved on disk.
    file_type : str
        One of: 'sales', 'spend', 'category', 'price',
        'fk_search_traffic', 'fk_category', 'fk_price',
        'fk_pca', 'fk_pla', 'fk_sales_invoice', 'fk_coupon'.
    user_id : int
        ID of the logged-in user (for WebSocket notifications).
    data_owner_id : int
        ID of the data-owner user (main user) for DB associations.
    date_str : str
        Date string for sales files (DD-MM-YYYY).
    is_last : bool
        Whether this is the last file in the batch — triggers dashboard
        data generation and materialized view refresh.
    is_flipkart : bool
        Whether this file belongs to the Flipkart pipeline.
    """
    from apps.accounts.models import Users  # noqa: late import to avoid AppRegistryNotReady
    from apps.upload.services import (
        process_category_file, process_price_file,
        process_spend_file, process_sales_file,
        generate_dashboard_data,
        process_fk_search_traffic, process_fk_category,
        process_fk_price, process_fk_pca, process_fk_pla,
        process_fk_sales_invoice, process_fk_coupon,
        generate_flipkart_dashboard_data,
    )

    _send_ws(user_id, f'Processing {file_type} file...', 'processing')

    try:
        data_owner = Users.objects.get(pk=data_owner_id)

        # Open the file from disk
        with open(file_path, 'rb') as fh:
            if file_type == 'category':
                process_category_file(fh, data_owner)
            elif file_type == 'price':
                process_price_file(fh, data_owner)
            elif file_type == 'spend':
                process_spend_file(fh, data_owner)
            elif file_type == 'sales':
                process_sales_file(fh, date_str, data_owner)
            elif file_type == 'fk_search_traffic':
                process_fk_search_traffic(fh, data_owner)
            elif file_type == 'fk_category':
                process_fk_category(fh, data_owner)
            elif file_type == 'fk_price':
                process_fk_price(fh, data_owner)
            elif file_type == 'fk_pca':
                process_fk_pca(fh, data_owner)
            elif file_type == 'fk_pla':
                process_fk_pla(fh, data_owner)
            elif file_type == 'fk_sales_invoice':
                process_fk_sales_invoice(fh, data_owner)
            elif file_type == 'fk_coupon':
                process_fk_coupon(fh, data_owner)

        # Clean up temp file after processing
        try:
            os.remove(file_path)
        except OSError:
            pass

        if is_last:
            _send_ws(user_id, 'Generating final dashboard data...', 'processing')
            if is_flipkart:
                generate_flipkart_dashboard_data(data_owner)
            else:
                generate_dashboard_data(data_owner)
            _send_ws(user_id, 'All files processed successfully!', 'complete')
        else:
            _send_ws(user_id, f'{file_type} processed successfully.', 'partial')

        return {
            'status': 'success',
            'file_type': file_type,
            'is_last': is_last,
        }

    except Exception as exc:
        logger.exception("[UploadTask] Error processing %s: %s", file_type, exc)
        _send_ws(user_id, f'Error processing file: {str(exc)}', 'error')

        # Clean up temp file on error too
        try:
            os.remove(file_path)
        except OSError:
            pass

        return {
            'status': 'error',
            'file_type': file_type,
            'message': str(exc),
        }


@shared_task(bind=True)
def generate_dashboard_task(self, user_id, data_owner_id, is_flipkart=False):
    """
    Standalone task to regenerate dashboard data and refresh
    materialized views. Can be called independently of file uploads
    (e.g., to force a cache refresh).
    """
    from apps.accounts.models import Users
    from apps.upload.services import (
        generate_dashboard_data,
        generate_flipkart_dashboard_data,
    )

    _send_ws(user_id, 'Regenerating dashboard data...', 'processing')

    try:
        data_owner = Users.objects.get(pk=data_owner_id)

        if is_flipkart:
            generate_flipkart_dashboard_data(data_owner)
        else:
            generate_dashboard_data(data_owner)

        _send_ws(user_id, 'Dashboard data regenerated successfully!', 'complete')

        return {'status': 'success'}

    except Exception as exc:
        logger.exception("[DashboardTask] Error: %s", exc)
        _send_ws(user_id, f'Error regenerating dashboard: {str(exc)}', 'error')
        return {'status': 'error', 'message': str(exc)}
