import os
import pandas as pd
import json
import tempfile
from datetime import datetime
from io import BytesIO
from django.shortcuts import render, redirect
from django.http import JsonResponse, FileResponse
from django.views.decorators.csrf import csrf_exempt
from django.conf import settings
from django.core.cache import cache
import uuid
from celery.result import AsyncResult
from apps.accounts.utils import get_logged_in_user
from apps.dashboard.utils import resolve_path
from apps.accounts.models import Feature
from django.core.files.storage import FileSystemStorage

# Logic imports
from .validation import validate_sales, validate_shipment, validate_stock, validate_lis
from .sales_processor import process_sales_report
from .shipment_processor import generate_shipment_report
from .fba_stock_processor import process_fba_stock
from .merger_sales_ship_stock import generate_master_report
from .tasks import validate_reports_celery, generate_master_celery

# Functions moved to utils.py
# resolve_path
# extract_days

from .utils import generate_master_data

from apps.accounts.decorators import require_feature

@require_feature('replenishment')
def index(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect('account-login')

    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = [f.code_name for f in user.role.features.all()] if user.role else []

    context = {
        'logged_user': user,
        'user_features': user_features,
        'page_title': 'Replenishment Report Generator',
        'payload': {
            'filters': {
                'platforms': ['Amazon', 'Flipkart'],
                'categories': [],
                'asins': []
            }
        },
        'selected_filters': {
            'categories': [],
            'asins': []
        }
    }
    return render(request, "replenishment/index.html", context)


def save_uploaded_files(request_files):
    """Save multi-part uploaded files to a system temporary directory."""
    temp_dir = tempfile.mkdtemp(prefix="repl_uploads_")
    saved_paths = {}
    for key, file in request_files.items():
        # Sanitize filename
        safe_name = "".join([c for c in file.name if c.isalnum() or c in "._- "]).strip()
        if not safe_name:
            safe_name = "uploaded_file"
        path = os.path.join(temp_dir, safe_name)
        with open(path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)
        saved_paths[key] = path
    return saved_paths




@csrf_exempt
def validate_api(request):
    try:
        if request.method != 'POST':
            return JsonResponse({"error": "Only POST allowed"}, status=405)
            
        if not request.FILES:
            return JsonResponse({"error": "No files uploaded"}, status=400)
        
        # Save files to a unique temporary folder
        files = save_uploaded_files(request.FILES)
        
        reports_to_validate = [
            ("Sales", files.get("Sales")),
            ("Shipment", files.get("Shipment")),
            ("Stock", files.get("Stock")),
            ("LIS", files.get("LIS"))
        ]
        
        # Needs Assortment for validation logic
        if not files.get("Assortment") or not os.path.exists(files["Assortment"]):
            return JsonResponse({"error": "Assortment Master file is required for validation"}, status=400)

        # Pass mapping file paths to Celery — master_data is generated inside the task
        mapping_files = {
            "FC_Cluster": files.get("FC_Cluster"),
            "Pincode_Cluster": files.get("Pincode_Cluster"),
            "Assortment": files.get("Assortment"),
            "Input_Sheet": files.get("Input_Sheet"),
        }
        
        task = validate_reports_celery.delay(reports_to_validate, mapping_files)
        
        return JsonResponse({'task_id': task.id, 'status': 'processing'})
    except Exception as e:
        return JsonResponse({"error": f"Validation Error: {str(e)}"}, status=500)





@csrf_exempt
def generate_master_api(request):
    import traceback
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        if request.method != 'POST':
            return JsonResponse({"error": "Only POST allowed"}, status=405)
            
        if not request.FILES:
            return JsonResponse({"error": "No files uploaded"}, status=400)

        logger.info(f"[generate_master_api] Received {len(request.FILES)} files: {list(request.FILES.keys())}")

        # Save files to a unique temporary folder
        files = save_uploaded_files(request.FILES)
        
        required = ["Sales", "Stock", "LIS", "Shipment", "Assortment", "FC_Cluster", "Pincode_Cluster", "Input_Sheet", "Business_Report"]
        missing = [req for req in required if not files.get(req) or not os.path.exists(files[req])]
        if missing:
            logger.warning(f"[generate_master_api] Missing files: {missing}")
            return JsonResponse({"error": f"Missing uploaded files for: {', '.join(missing)}"}, status=400)
        
        temp_dir = tempfile.mkdtemp()
        task = generate_master_celery.delay(files, temp_dir)
        logger.info(f"[generate_master_api] Celery task dispatched: {task.id}")
        
        return JsonResponse({'task_id': task.id, 'status': 'processing'})
    except Exception as e:
        logger.error(f"[generate_master_api] Error: {str(e)}\n{traceback.format_exc()}")
        return JsonResponse({"error": f"Generation Error: {str(e)}"}, status=500)



def check_task_status(request, task_id):
    """Poll Celery state and transfer result to session so downloads work"""
    task = AsyncResult(task_id)
    
    if task.state == 'PENDING':
        return JsonResponse({"status": "processing"})
    elif task.state == 'SUCCESS':
        task_data = task.result
        if not task_data:
            return JsonResponse({"error": "Task returned empty result"}, status=500)
            
        if task_data.get('status') == 'error':
            return JsonResponse({"status": "error", "message": task_data.get('message', 'Unknown processing error.')})
            
        task_type = task_data.get('task_type')
        if task_type == 'validation':
            if 'error_data_map' in task_data:
                request.session['validation_errors'] = task_data['error_data_map']
                request.session.modified = True
            return JsonResponse({
                "status": "success",
                "total_errors": task_data.get('total_errors', 0),
                "reports": task_data.get('reports', {})
            })
        elif task_type == 'generation':
            if 'csv_path' in task_data:
                request.session['master_report'] = {
                    'csv_path': task_data['csv_path'],
                    'excel_path': task_data.get('excel_path'),
                    'temp_dir': task_data['temp_dir']
                }
                request.session.modified = True
            return JsonResponse({"status": "success", "message": "Master report generated successfully."})
            
    elif task.state == 'FAILURE':
        return JsonResponse({"status": "error", "message": str(task.info)})
        
    return JsonResponse({"status": "processing", "state": task.state})

def download_validation_error(request, report_type, file_format):
    """Download validation error file on-demand"""
    if 'validation_errors' not in request.session or report_type not in request.session['validation_errors']:
        return JsonResponse({"error": "Validation data not found"}, status=404)
    
    error_data = request.session['validation_errors'][report_type]
    df_errors = pd.DataFrame(error_data['data'])
    
    if file_format == 'csv':
        output = BytesIO()
        df_errors.to_csv(output, index=False)
        output.seek(0)
        response = FileResponse(output, content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{report_type}_validation_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
        return response
    elif file_format == 'excel':
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df_errors.to_excel(writer, sheet_name='Errors', index=False)
        output.seek(0)
        response = FileResponse(output, content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        response['Content-Disposition'] = f'attachment; filename="{report_type}_validation_errors_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
        return response
    else:
        return JsonResponse({"error": "Invalid format"}, status=400)

def download_master_report(request, file_format):
    """Download master report file on-demand"""
    if 'master_report' not in request.session:
        return JsonResponse({"error": "Master report not found"}, status=404)
    
    master_data = request.session['master_report']
    
    try:
        if file_format == 'csv':
            if not os.path.exists(master_data['csv_path']):
                return JsonResponse({"error": "CSV file not found"}, status=404)
            response = FileResponse(open(master_data['csv_path'], 'rb'), content_type='text/csv')
            response['Content-Disposition'] = f'attachment; filename="Master_Merged_Report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv"'
            return response
        elif file_format == 'excel':
            if not master_data['excel_path'] or not os.path.exists(master_data['excel_path']):
                return JsonResponse({"error": "Excel file not found"}, status=404)
            response = FileResponse(open(master_data['excel_path'], 'rb'), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = f'attachment; filename="Master_Merged_Report_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx"'
            return response
        else:
            return JsonResponse({"error": "Invalid format"}, status=400)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)

def download_file(request):
    filepath = request.GET.get("path")
    if not filepath or not os.path.exists(filepath):
        return JsonResponse({"error": "File not found"}, status=404)
        
    return FileResponse(open(filepath, 'rb'), as_attachment=True)
