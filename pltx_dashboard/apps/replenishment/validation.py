import pandas as pd
import os
from datetime import datetime, timedelta

def load_data(filepath):
    _, ext = os.path.splitext(filepath)
    ext = ext.lower()
    
    if ext == '.csv':
        return pd.read_csv(filepath)
    elif ext in ['.xls', '.xlsx']:
        return pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file format '{ext}'. Only .csv, .xls, and .xlsx are supported.")

def validate_sales(filepath, master_data):
    try:
        df = load_data(filepath)
    except Exception as e:
        return [{"Row": "-", "Column": "-", "Value": "-", "Message": f"Failed to read file: {str(e)}"}]
    
    errors = []
    
    # Extract master lists
    fc_master = set(master_data['fc_list']).union(set(master_data['flex_list']))
    asin_master = set(master_data['asin_list'])
    pincode_master = set(master_data['pincode_list'])
    
    # Process Dates
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=120)
    
    for idx, row in df.iterrows():
        row_num = idx + 2  # 1-indexed, +1 for header
        
        # 1. FC validation
        fc = row.get('FC CODE', None)
        if pd.notna(fc) and fc not in fc_master:
            errors.append({"Row": row_num, "Column": "FC CODE", "Value": str(fc), "Message": "FC CODE not found in master", "ASIN": row.get('ASIN', '')})
        
        # 2. Pincode validation
        pincode = row.get('Shipment To Postal Code', None)
        if pd.notna(pincode):
            # Attempt to convert to float/int to match the numeric format
            try:
                pincode_val = float(pincode)
                if pincode_val not in pincode_master:
                    errors.append({"Row": row_num, "Column": "Shipment To Postal Code", "Value": str(pincode), "Message": "Pincode not found in master", "ASIN": row.get('ASIN', '')})
            except ValueError:
                errors.append({"Row": row_num, "Column": "Shipment To Postal Code", "Value": str(pincode), "Message": "Invalid Pincode format", "ASIN": row.get('ASIN', '')})

        # 3. ASIN validation
        asin = row.get('ASIN', None)
        if pd.notna(asin) and asin not in asin_master:
            errors.append({"Row": row_num, "Column": "ASIN", "Value": str(asin), "Message": "ASIN not found in master", "ASIN": str(asin)})
            
        # 4. Sales Report days: dates > Today+120 days
        date_str = row.get('Customer Shipment Date', None)
        if pd.notna(date_str):
            try:
                # Expected format: "2026-03-14T23:59:48+05:30" or "dd-mm-yyyy"
                parsed_date = pd.to_datetime(date_str, dayfirst=True, format='mixed').date()
                if parsed_date < cutoff_date:
                    errors.append({"Row": row_num, "Column": "Customer Shipment Date", "Value": str(date_str), "Message": f"Date is before Today - 120 days ({cutoff_date})", "ASIN": row.get('ASIN', '')})
            except Exception:
                pass # Ignore invalid date format for this specific rule if it doesn't parse
                
    return errors


def validate_shipment(filepath, master_data):
    try:
        df = load_data(filepath)
    except Exception as e:
        return [{"Row": "-", "Column": "-", "Value": "-", "Message": f"Failed to read file: {str(e)}"}]
    
    errors = []
    
    # Data cleaning: Remove rows where STATUS is "Closed", "STATUS" (header), or "(Blanks)"
    if 'STATUS' in df.columns:
        invalid_statuses = ["Closed", "STATUS", "(Blanks)"]
        df = df[~df['STATUS'].astype(str).str.strip().isin(invalid_statuses)]
        df = df.dropna(subset=['STATUS'])
        df = df.reset_index(drop=True)
    
    fc_master = set(master_data['fc_list']).union(set(master_data['flex_list']))
    asin_master = set(master_data['asin_list'])
    cluster_master = set(master_data['cluster_list'])
    
    today = datetime.now().date()
    cutoff_date = today - timedelta(days=15)
    
    for idx, row in df.iterrows():
        row_num = idx + 2
        
        # 1. ASIN validation
        asin = row.get('ASIN', None)
        if pd.notna(asin) and asin not in asin_master:
            errors.append({"Row": row_num, "Column": "ASIN", "Value": str(asin), "Message": "ASIN not found in master", "Shipment_id": row.get('ID', ''), "ASIN": str(asin)})
            
        # 2. FC validation
        fc = row.get('FC CODE', None)
        if pd.notna(fc) and fc not in fc_master:
            errors.append({"Row": row_num, "Column": "FC CODE", "Value": str(fc), "Message": "FC CODE not found in master", "Shipment_id": row.get('ID', ''), "ASIN": row.get('ASIN', '')})
            
        # 3. Cluster validation
        cluster = row.get('CLUSTER', None)
        if pd.notna(cluster) and cluster not in cluster_master:
            errors.append({"Row": row_num, "Column": "CLUSTER", "Value": str(cluster), "Message": "Cluster not found in master", "Shipment_id": row.get('ID', ''), "ASIN": row.get('ASIN', '')})
            
        # 4. Loading Date Flag
        date_str = row.get('LOADING DATE', None)
        if pd.notna(date_str):
            try:
                parsed_date = pd.to_datetime(date_str, dayfirst=True, format='mixed').date()
                if parsed_date < cutoff_date:
                    errors.append({
                        "Row": row_num,
                        "Column": "LOADING DATE",
                        "Value": str(parsed_date),
                        "Message": f"Loading Date is before Today-15 days ({cutoff_date}), flagged as too old",
                        "Shipment_id": row.get('ID', ''),
                        "ASIN": row.get('ASIN', '')
                    })
            except Exception:
                pass
                
    return errors


def validate_stock(filepath, master_data):
    try:
        df = load_data(filepath)
    except Exception as e:
        return [{"Row": "-", "Column": "-", "Value": "-", "Message": f"Failed to read file: {str(e)}"}]
    
    errors = []
    
    fc_master = set(master_data['fc_list']).union(set(master_data['flex_list']))
    asin_master = set(master_data['asin_list'])
    
    today = datetime.now().date()
    min_date = today - timedelta(days=5)
    max_date = today
    
    for idx, row in df.iterrows():
        row_num = idx + 2
        
        # 1. ASIN validation
        asin = row.get('ASIN', None)
        if pd.notna(asin) and asin not in asin_master:
            errors.append({"Row": row_num, "Column": "ASIN", "Value": str(asin), "Message": "ASIN not found in master"})
            
        # 2. FC (Location) validation - Support both 'FC CODE' and 'Location'
        fc = row.get('FC CODE', None)
        if pd.isna(fc):
            fc = row.get('Location', None)
            
        if pd.notna(fc) and fc not in fc_master:
            errors.append({"Row": row_num, "Column": "FC CODE/Location", "Value": str(fc), "Message": "FC CODE/Location not found in master"})
            
        # 3. Date Range validation - User specified MM-DD-YYYY format
        date_str = row.get('Date', None)
        if pd.notna(date_str):
            try:
                # Use dayfirst=False to prefer MM-DD-YYYY format
                parsed_date = pd.to_datetime(date_str, dayfirst=False, format='mixed').date()
                if not (min_date <= parsed_date <= max_date):
                    errors.append({"Row": row_num, "Column": "Date", "Value": str(parsed_date), "Message": f"Date not in range Today-5 to Today ({min_date} to {max_date})"})
            except Exception:
                pass
                
    return errors

def validate_lis(filepath, master_data):
    try:
        df = load_data(filepath)
    except Exception as e:
        return [{"Row": "-", "Column": "-", "Value": "-", "Message": f"Failed to read file: {str(e)}"}]
    
    errors = []
    
    asin_master = set(master_data['asin_list'])
    
    for idx, row in df.iterrows():
        row_num = idx + 2
        
        # 1. ASIN validation
        asin = row.get('ASIN', None)
        if pd.notna(asin) and str(asin).strip() not in asin_master:
            errors.append({"Row": row_num, "Column": "ASIN", "Value": str(asin), "Message": "ASIN from LIS Report not found in Assortment Master file"})
            
    return errors
