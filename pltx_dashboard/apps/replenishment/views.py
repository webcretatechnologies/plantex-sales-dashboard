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
from apps.dashboard.utils import resolve_path, extract_days
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

def generate_master_data(fc_mapping_path, pincode_path, product_details_path, input_sheet_path):
    try:
        fc_cluster_mapping_3 = pd.read_excel(fc_mapping_path)
        pincode_cluster = pd.read_csv(pincode_path)
        product_details = pd.read_excel(product_details_path)
        input_sheet_1 = pd.read_excel(input_sheet_path)
        
        # Clean columns
        fc_cluster_mapping_3.columns = fc_cluster_mapping_3.columns.str.strip()
        pincode_cluster.columns = pincode_cluster.columns.str.strip()
        product_details.columns = product_details.columns.str.strip()
        input_sheet_1.columns = input_sheet_1.columns.str.strip()
        
        # FC lists
        fc_list = fc_cluster_mapping_3[fc_cluster_mapping_3["FC TYPE"] == "AMAZON"]["FC CODE"].dropna().unique().tolist()
        flex_list = fc_cluster_mapping_3[fc_cluster_mapping_3["FC TYPE"] == "FLEX"]["FC CODE"].dropna().unique().tolist()
        
        fc_ixd_list = ["ISK3", "BLR4", "DED5", "DED3", "HBX1", "HBX2"]
        fc_local_list = list(set(fc_list) - set(fc_ixd_list))
        
        # Other lists
        asin_list = product_details["ASIN"].dropna().unique().tolist()
        cluster_list = pincode_cluster["Fulfilment Cluster"].dropna().unique().tolist()
        pincode_list = pincode_cluster["PIN CODE"].dropna().unique().tolist()
        
        # Date parameters
        p0_day = extract_days(input_sheet_1[input_sheet_1["Particular"] == "P0 Demand DOC"]["Value"].values[0])
        p1_day = extract_days(input_sheet_1[input_sheet_1["Particular"] == "P1 Demand DOC"]["Value"].values[0])
        p2_day = extract_days(input_sheet_1[input_sheet_1["Particular"] == "P2 Demand DOC"]["Value"].values[0])
        sales_day_count = extract_days(input_sheet_1[input_sheet_1["Particular"] == "Sale Report Days"]["Value"].values[0])
        
        return {
            "fc_list": fc_list,
            "flex_list": flex_list,
            "fc_ixd_list": fc_ixd_list,
            "fc_local_list": fc_local_list,
            "asin_list": asin_list,
            "cluster_list": cluster_list,
            "pincode_list": pincode_list,
            "p0_day": p0_day,
            "p1_day": p1_day,
            "p2_day": p2_day,
            "sales_day_count": sales_day_count
        }
    except Exception as e:
        raise Exception(f"Failed to generate master data from mapping files: {str(e)}")

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
                'platforms': ['Amazon', 'Flipkart', 'JioMart'],
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


def save_uploaded_files(request_files, subfolder):
    """Save multi-part uploaded files to a unique subfolder in MEDIA_ROOT."""
    base_path = os.path.join(settings.MEDIA_ROOT, 'replenishment', 'uploads', subfolder)
    if not os.path.exists(base_path):
        os.makedirs(base_path, exist_ok=True)
        
    fs = FileSystemStorage(location=base_path)
    saved_paths = {}
    for key, file in request_files.items():
        filename = fs.save(file.name, file)
        saved_paths[key] = fs.path(filename)
    return saved_paths




@csrf_exempt
def validate_api(request):
    if request.method != 'POST':
        return JsonResponse({"error": "Only POST allowed"}, status=405)
        
    if not request.FILES:
        return JsonResponse({"error": "No files uploaded"}, status=400)
    
    # Save files to a unique subfolder
    subfolder = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]
    files = save_uploaded_files(request.FILES, subfolder)
    
    reports_to_validate = [
        ("Sales", files.get("Sales")),
        ("Shipment", files.get("Shipment")),
        ("Stock", files.get("Stock")),
        ("LIS", files.get("LIS"))
    ]
    
    # Needs Assortment for validation logic
    if not files.get("Assortment") or not os.path.exists(files["Assortment"]):
        return JsonResponse({"error": "Assortment Master file is required for validation"}, status=400)
        
    try:
        master_data = generate_master_data(
            files.get("FC_Cluster"),
            files.get("Pincode_Cluster"),
            files.get("Assortment"),
            files.get("Input_Sheet")
        )
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)
    
    task = validate_reports_celery.delay(reports_to_validate, master_data)
    
    return JsonResponse({'task_id': task.id, 'status': 'processing'})




@csrf_exempt
def generate_master_api(request):
    if request.method != 'POST':
        return JsonResponse({"error": "Only POST allowed"}, status=405)
        
    if not request.FILES:
        return JsonResponse({"error": "No files uploaded"}, status=400)

    # Save files to a unique subfolder
    subfolder = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + str(uuid.uuid4())[:8]
    files = save_uploaded_files(request.FILES, subfolder)
    
    required = ["Sales", "Stock", "LIS", "Shipment", "Assortment", "FC_Cluster", "Pincode_Cluster", "Input_Sheet", "Business_Report"]
    missing = [req for req in required if not files.get(req) or not os.path.exists(files[req])]
    if missing:
        return JsonResponse({"error": f"Missing uploaded files for: {', '.join(missing)}"}, status=400)
    
    temp_dir = tempfile.mkdtemp()
    task = generate_master_celery.delay(files, temp_dir)
    
    return JsonResponse({'task_id': task.id, 'status': 'processing'})


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
