import os
import pandas as pd
import tempfile
from celery import shared_task
from django.core.cache import cache

# Logic imports
from .validation import validate_sales, validate_shipment, validate_stock, validate_lis
from .sales_processor import process_sales_report
from .shipment_processor import generate_shipment_report
from .fba_stock_processor import process_fba_stock
from .merger_sales_ship_stock import generate_master_report

@shared_task(bind=True)
def validate_reports_celery(self, reports_to_validate, master_data):
    task_id = self.request.id
    results = {}
    total_errors = 0
    error_data_map = {}
    
    try:
        for r_type, path in reports_to_validate:
            results[r_type] = {
                "error_count": 0,
                "has_errors": False
            }
            
            if path and os.path.exists(path):
                if r_type == "Sales":
                    errs = validate_sales(path, master_data)
                elif r_type == "Shipment":
                    errs = validate_shipment(path, master_data)
                elif r_type == "Stock":
                    errs = validate_stock(path, master_data)
                elif r_type == "LIS":
                    errs = validate_lis(path, master_data)
                else:
                    errs = []
                
                error_count = len(errs)
                results[r_type]["error_count"] = error_count
                total_errors += error_count
                
                if error_count > 0:
                    df_errors = pd.DataFrame(errs)
                    cols = ["Row", "Column", "Value", "Message", "Shipment_id", "ASIN"]
                    df_errors = df_errors[[c for c in cols if c in df_errors.columns]]
                    
                    error_records = df_errors.to_dict('records')
                    error_data_map[r_type] = {
                        'data': error_records,
                        'columns': df_errors.columns.tolist()
                    }
                    results[r_type]["has_errors"] = True

        return {
            "status": "success",
            "total_errors": total_errors,
            "reports": results,
            "error_data_map": error_data_map,
            "task_type": "validation"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "task_type": "validation"
        }

@shared_task(bind=True)
def generate_master_celery(self, files, temp_dir):
    try:
        sales_out = os.path.join(temp_dir, "Final_Sales_Report.csv")
        stock_out = os.path.join(temp_dir, "FBA_Mapped_Output.xlsx")
        shipment_out = os.path.join(temp_dir, "Shipment_Report_Final.xlsx")

        process_sales_report(files["Sales"], files["Pincode_Cluster"], sales_out)
        process_fba_stock(files["Stock"], files["FC_Cluster"], stock_out)
        generate_shipment_report(files["Shipment"], files["FC_Cluster"], shipment_out)
        
        master_out_csv = os.path.join(temp_dir, "Master_Merged_Report.csv")
        generate_master_report(
            sales_out, 
            stock_out, 
            shipment_out, 
            files["Assortment"], 
            files["Business_Report"], 
            files["LIS"], 
            files["Sales"], 
            files["Pincode_Cluster"], 
            files["FC_Cluster"],
            files["Input_Sheet"],
            master_out_csv
        )
        
        master_out_excel = os.path.join(temp_dir, "Master_Merged_Report.xlsx")
        try:
            df_master = pd.read_csv(master_out_csv, low_memory=False)
            annexure_data = [
                {"Header Name": "BSR", "Description / Formula": "Best Seller Rank of the product."},
                {"Header Name": "ASIN", "Description / Formula": "Amazon Standard Identification Number."},
                {"Header Name": "Ideal Cluster", "Description / Formula": "Optimized fulfillment cluster identified for this ASIN."},
                {"Header Name": "SKU", "Description / Formula": "Stock Keeping Unit from product details."},
                {"Header Name": "HSN CODE", "Description / Formula": "Harmonized System of Nomenclature code."},
                {"Header Name": "VENDOR NAME", "Description / Formula": "Name of the supplier or vendor."},
                {"Header Name": "PRODUCTS STATUS", "Description / Formula": "Current operational status of the product."},
                {"Header Name": "ACT WEIGHT", "Description / Formula": "Actual physical weight of the product."},
                {"Header Name": "VOLUMETRIC WEIGHT", "Description / Formula": "Volumetric weight based on product dimensions."},
                {"Header Name": "PRODUCT TYPE", "Description / Formula": "Classification type of the product."},
                {"Header Name": "PRODUCT SIZE", "Description / Formula": "Size category of the product."},
                {"Header Name": "Portfolio", "Description / Formula": "Product portfolio group."},
                {"Header Name": "Category", "Description / Formula": "Product category."},
                {"Header Name": "Zone", "Description / Formula": "Demand zone derived from sales data."},
                {"Header Name": "LIS_CLUSTER", "Description / Formula": "Mapped Local In-Stock cluster."},
                {"Header Name": "DRR", "Description / Formula": "Daily Run Rate (average daily sales)."},
                {"Header Name": "Sales Qty", "Description / Formula": "Total sales quantity for the period."},
                {"Header Name": "Stock Qty", "Description / Formula": "Current available stock quantity in warehouse."},
                {"Header Name": "Receiving Qty", "Description / Formula": "Quantity currently in 'Receiving' status across DFC and IXD shipments."},
                {"Header Name": "Stock Transfer Qty", "Description / Formula": "Stock in transit between warehouses (from stock report)."},
                {"Header Name": "Shipment Qty", "Description / Formula": "Total quantity in upcoming and pending DFC shipments (Pending + Upcoming + Intransit)."},
                {"Header Name": "P0", "Description / Formula": "Formula: DRR * 15 - Stock Qty - Receiving Qty - Stock Transfer Qty - Shipment Qty"},
                {"Header Name": "P1", "Description / Formula": "Formula: DRR * 30 - Stock Qty - Receiving Qty - Stock Transfer Qty - Shipment Qty - P0"},
                {"Header Name": "P2", "Description / Formula": "Formula: DRR * 60 - Stock Qty - Receiving Qty - Stock Transfer Qty - Shipment Qty - P1 - P0"},
                {"Header Name": "Stock Out Date", "Description / Formula": "Formula: IF(Today+((Stock Qty + Receiving Qty)/DRR) > Shipment Date, Today+((Stock Qty + Shipment Qty + Receiving Qty)/DRR), Today+((Stock Qty + Receiving Qty)/DRR))"},
                {"Header Name": "Stock cover alert", "Description / Formula": "Alert category based on Days of Cover (<30, Healthy, >120 Days, etc)."},
                {"Header Name": "Page Views - Total", "Description / Formula": "Total page views from the business report."},
                {"Header Name": "Units Ordered", "Description / Formula": "Total units ordered from the business report."},
                {"Header Name": "Total Order Items", "Description / Formula": "Total order items from the business report."},
                {"Header Name": "Conversion Pct", "Description / Formula": "Formula: (Total Order Items / Page Views - Total) * 100"},
                {"Header Name": "Ordered Product Sales", "Description / Formula": "Total sales value from orders."},
                {"Header Name": "LIS_LOCAL_QTY", "Description / Formula": "Local shipped units within the LIS cluster."},
                {"Header Name": "LIS_TOTAL_QTY", "Description / Formula": "Total shipped units within the LIS cluster."},
                {"Header Name": "DFC Appointment Pending Qty", "Description / Formula": "Quantity in DFC shipments pending appointment."},
                {"Header Name": "DFC Upcoming Shipment Qty", "Description / Formula": "Quantity in upcoming DFC shipments."},
                {"Header Name": "DFC Intransit Qty", "Description / Formula": "Quantity currently in transit for DFC shipments."},
                {"Header Name": "DFC Shipment ID", "Description / Formula": "Shipment IDs for DFC replenishment."},
                {"Header Name": "DFC Next Arrival Date", "Description / Formula": "Earliest arrival date for upcoming DFC shipments."},
                {"Header Name": "IXD Appointment Pending Qty", "Description / Formula": "Quantity in IXD shipments pending appointment."},
                {"Header Name": "IXD Upcoming Shipment Qty", "Description / Formula": "Quantity in upcoming IXD shipments."},
                {"Header Name": "IXD Receiving + Intransit Qty", "Description / Formula": "Quantity currently receiving or in transit for IXD shipments."},
                {"Header Name": "IXD Shipment ID", "Description / Formula": "Shipment IDs for IXD replenishment."}
            ]
            df_annexure = pd.DataFrame(annexure_data)
            with pd.ExcelWriter(master_out_excel) as writer:
                df_master.to_excel(writer, sheet_name='Master Report', index=False)
                df_annexure.to_excel(writer, sheet_name='Annexure', index=False)
        except Exception as e:
            master_out_excel = None
        
        return {
            "status": "success",
            "csv_path": master_out_csv,
            "excel_path": master_out_excel,
            "temp_dir": temp_dir,
            "task_type": "generation"
        }
    except Exception as e:
        return {
            "status": "error",
            "message": str(e),
            "task_type": "generation"
        }
