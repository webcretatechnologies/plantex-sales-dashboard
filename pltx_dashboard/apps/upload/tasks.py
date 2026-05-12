import os
import logging
import csv

from openpyxl import load_workbook
from celery import shared_task
from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer

logger = logging.getLogger(__name__)


def _send_ws(user_id, message, status):
    """Send a WebSocket progress message to the user's channel group."""
    try:
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            f"user_{user_id}",
            {
                "type": "upload_progress",
                "message": message,
                "status": status,
            },
        )
    except Exception as exc:
        logger.warning("[UploadTask] WebSocket send failed: %s", exc)


NON_CONVERTIBLE_EXCEL_TYPES = set()
EXCEL_TO_CSV_MIN_SIZE_BYTES = int(
    os.getenv("EXCEL_TO_CSV_MIN_SIZE_MB", "5")
) * 1024 * 1024


def _convert_excel_to_csv_if_possible(file_path, file_type):
    """
    Convert single-sheet Excel uploads to CSV for faster chunked ingestion.
    Returns the path to the file that should be processed.
    """
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in {".xlsx", ".xlsm"}:
        return file_path

    if file_type in NON_CONVERTIBLE_EXCEL_TYPES:
        return file_path

    if os.path.getsize(file_path) < EXCEL_TO_CSV_MIN_SIZE_BYTES:
        return file_path

    csv_path = f"{os.path.splitext(file_path)[0]}.csv"
    wb = load_workbook(file_path, read_only=True, data_only=True)
    ws = wb[wb.sheetnames[0]]

    with open(csv_path, "w", newline="", encoding="utf-8") as out_file:
        writer = csv.writer(out_file)
        for row in ws.iter_rows(values_only=True):
            writer.writerow(["" if val is None else val for val in row])

    wb.close()
    return csv_path


@shared_task(bind=True)
def process_upload_file_task(
    self,
    file_path,
    file_type,
    user_id,
    data_owner_id,
    upload_log_id=None,
    date_str="",
    is_last=False,
    is_flipkart=False,
):
    """
    Celery task that processes a single uploaded file.

    Parameters
    ----------
    file_path : str
        Absolute path to the uploaded file saved on disk.
    file_type : str
        One of: 'sales', 'spend', 'category', 'price',
        'fk_search_traffic', 'fk_category', 'fk_price',
        'fk_pla', 'fk_fba_stock', 'fk_flex_stock'.
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
    from apps.accounts.models import Users  # noqa: F401
    from apps.upload.models import UploadLog
    from apps.upload.services import (
        process_category_file,
        process_price_file,
        process_spend_file,
        process_sales_file,
        process_fba_stock_file,
        process_flex_stock_file,
        generate_dashboard_data,
        process_fk_search_traffic,
        process_fk_category,
        process_fk_price,
        process_fk_pla,
        generate_flipkart_dashboard_data,
    )

    _send_ws(user_id, f"Processing {file_type} file...", "processing")

    try:
        data_owner = Users.objects.get(pk=data_owner_id)
        files_to_cleanup = [file_path]
        upload_log = None
        if upload_log_id:
            upload_log = UploadLog.objects.filter(pk=upload_log_id).first()
            if upload_log:
                upload_log.status = UploadLog.STATUS_PROCESSING
                upload_log.message = "Processing started."
                upload_log.save(update_fields=["status", "message", "updated_at"])

        processing_path = _convert_excel_to_csv_if_possible(file_path, file_type)
        if processing_path != file_path:
            files_to_cleanup.append(processing_path)

        # Open the file from disk
        with open(processing_path, "rb") as fh:
            if file_type == "category":
                process_category_file(fh, data_owner)
            elif file_type == "price":
                process_price_file(fh, data_owner)
            elif file_type == "spend":
                process_spend_file(fh, data_owner)
            elif file_type == "sales":
                process_sales_file(fh, date_str, data_owner)
            elif file_type == "fba_stock":
                process_fba_stock_file(fh, data_owner)
            elif file_type == "flex_stock":
                process_flex_stock_file(fh, data_owner)
            elif file_type == "fk_search_traffic":
                process_fk_search_traffic(fh, data_owner)
            elif file_type == "fk_category":
                process_fk_category(fh, data_owner)
            elif file_type == "fk_price":
                process_fk_price(fh, data_owner)
            elif file_type == "fk_pla":
                process_fk_pla(fh, data_owner)
            elif file_type == "fk_fba_stock":
                process_fba_stock_file(
                    fh,
                    data_owner,
                    id_columns=("ASIN", "FSN", "FSN ID", "Flipkart Serial Number"),
                )
            elif file_type == "fk_flex_stock":
                process_flex_stock_file(
                    fh,
                    data_owner,
                    id_columns=("ASIN", "FSN", "FSN ID", "Flipkart Serial Number"),
                )

        # Clean up uploaded files after processing
        for path in files_to_cleanup:
            try:
                os.remove(path)
            except OSError:
                pass

        if is_last:
            _send_ws(user_id, "Generating final dashboard data...", "processing")
            if is_flipkart:
                from apps.dashboard.models import (
                    FBAStockData,
                    FlexStockData,
                    FlipkartCategoryMap,
                    FlipkartSearchTraffic,
                    FlipkartPLA,
                    FlipkartPrice,
                )

                has_fk_category = FlipkartCategoryMap.objects.filter(user=data_owner).exists()
                has_fk_traffic = FlipkartSearchTraffic.objects.filter(user=data_owner).exists()
                has_fk_pla = FlipkartPLA.objects.filter(user=data_owner).exists()
                has_fk_price = FlipkartPrice.objects.filter(user=data_owner).exists()
                if not (has_fk_category and has_fk_traffic and has_fk_pla and has_fk_price):
                    raise ValueError(
                        "Flipkart requires Search Traffic, Category, PLA, and Price reports."
                    )

                has_fba_stock = FBAStockData.objects.filter(user=data_owner).exists()
                has_flex_stock = FlexStockData.objects.filter(user=data_owner).exists()
                if not (has_fba_stock and has_flex_stock):
                    raise ValueError(
                        "Flipkart requires both FBA Stock and Flex Stock uploads."
                    )
                generate_flipkart_dashboard_data(data_owner)
            else:
                generate_dashboard_data(data_owner)
            _send_ws(user_id, "All files processed successfully!", "complete")
        else:
            _send_ws(user_id, f"{file_type} processed successfully.", "partial")

        if upload_log:
            upload_log.status = UploadLog.STATUS_SUCCESS
            upload_log.message = "Processed successfully."
            upload_log.save(update_fields=["status", "message", "updated_at"])

        return {
            "status": "success",
            "file_type": file_type,
            "is_last": is_last,
        }

    except Exception as exc:
        logger.exception("[UploadTask] Error processing %s: %s", file_type, exc)
        _send_ws(user_id, f"Error processing file: {str(exc)}", "error")

        if upload_log_id:
            try:
                upload_log = UploadLog.objects.filter(pk=upload_log_id).first()
                if upload_log:
                    upload_log.status = UploadLog.STATUS_ERROR
                    upload_log.message = str(exc)
                    upload_log.save(update_fields=["status", "message", "updated_at"])
            except Exception:
                logger.exception("[UploadTask] Failed updating UploadLog status.")

        # Clean up uploaded files on error too
        cleanup_candidates = [file_path, f"{os.path.splitext(file_path)[0]}.csv"]
        for path in cleanup_candidates:
            try:
                os.remove(path)
            except OSError:
                pass

        return {
            "status": "error",
            "file_type": file_type,
            "message": str(exc),
        }
