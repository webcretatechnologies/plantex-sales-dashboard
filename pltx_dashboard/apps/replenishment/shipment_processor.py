import pandas as pd
import os

def load_data(file_path):
    """Load data from either CSV or Excel format."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == '.csv':
        return pd.read_csv(file_path)
    else:
        return pd.read_excel(file_path)

def generate_shipment_report(shipment_file, mapping_file, output_file):
    print(f"Loading Shipment data from {shipment_file}...")
    try:
        df = load_data(shipment_file)
    except Exception as e:
        print(f"Error reading shipment file: {e}")
        return

    print(f"Loading FC Mapping from {mapping_file}...")
    try:
        mapping_df = load_data(mapping_file)
    except Exception as e:
        print(f"Error reading mapping file: {e}")
        return

    # Strip column names
    df.columns = df.columns.astype(str).str.strip()
    mapping_df.columns = mapping_df.columns.astype(str).str.strip()

    # Data cleaning: Remove all rows where Status is “Closed”, "(Blanks)", or "STATUS"
    if 'STATUS' in df.columns:
        invalid_statuses = ["Closed", r"\(Blanks\)", "STATUS", "Inbound"]
        df = df[~df['STATUS'].astype(str).str.strip().str.contains('|'.join(invalid_statuses), case=False, na=False)]
        df = df.dropna(subset=['STATUS'])
    
    # Check required columns
    required_cols = ['ASIN', 'CLUSTER', 'FC', 'STATUS', 'FINAL QTY', 'ID', 'APPOINTMENT DATE']
    for col in required_cols:
        if col not in df.columns:
            print(f"Error: Missing required column '{col}' in Shipment file.")
            print(f"Available columns: {list(df.columns)}")
            return

    # Build FC Replenishment Type lists from mapping
    fc_list = mapping_df[mapping_df["FC TYPE"] == "AMAZON"]["FC CODE"].dropna().unique().tolist()
    fc_ixd_list = ["ISK3", "BLR4", "DED5", "DED3", "HBX1", "HBX2"]
    fc_local_list = list(set(fc_list) - set(fc_ixd_list))

    def get_fc_replenishment_type(fc):
        if pd.isna(fc): return ''
        fc_str = str(fc).strip()
        if fc_str in fc_local_list:
            return 'DFC'
        elif fc_str in fc_ixd_list:
            return 'IXD'
        return ''

    df['FC Replenishment Type'] = df['FC'].apply(get_fc_replenishment_type)

    # Convert FINAL QTY to numeric before calculation
    df['FINAL QTY'] = pd.to_numeric(df['FINAL QTY'], errors='coerce').fillna(0)

    # Extract clean status
    df['STATUS_CLEAN'] = df['STATUS'].astype(str).str.strip()

    # Quantity calculations based on status
    df['Appointment Pending Qty'] = df.apply(
        lambda x: x['FINAL QTY'] if x['STATUS_CLEAN'] == 'Appointment Pending' else 0, axis=1)
    
    df['Upcoming Shipment Qty'] = df.apply(
        lambda x: x['FINAL QTY'] if x['STATUS_CLEAN'] == 'Upcoming' else 0, axis=1)
    
    inbound_statuses = ['Receiving', 'Intransit', 'In Transit']
    df['Receiving + Intransit Qty'] = df.apply(
        lambda x: x['FINAL QTY'] if x['STATUS_CLEAN'] in inbound_statuses else 0, axis=1)

    df['Receiving Qty'] = df.apply(
        lambda x: x['FINAL QTY'] if x['STATUS_CLEAN'] == 'Receiving' else 0, axis=1)

    intransit_statuses = ['Intransit', 'In Transit']
    df['Intransit Qty'] = df.apply(
        lambda x: x['FINAL QTY'] if x['STATUS_CLEAN'] in intransit_statuses else 0, axis=1)

    # Format Appointment Date
    df["APPOINTMENT DATE"] = df["APPOINTMENT DATE"].astype(str).replace('NaT', 'None').replace('', '')

    # Combine ID and Appointment Date
    def format_appt(id_val, date_val):
        id_str = str(id_val).strip() if pd.notna(id_val) else ""
        date_str = str(date_val).strip().split(' ')[0] if pd.notna(date_val) else ""
        return f'"{id_str}":"{date_str}"'

    df['Shipment_Detail'] = df.apply(lambda x: format_appt(x['ID'], x['APPOINTMENT DATE']), axis=1)

    # Convert APPOINTMENT DATE to actual datetime objects for calculation
    df['APPT_DATE_DT'] = pd.to_datetime(df['APPOINTMENT DATE'], errors='coerce')
    
    # Calculate Next DFC Arrival Date (Earliest Upcoming)
    today = pd.Timestamp.now().normalize()
    df['DFC Next Arrival Date'] = df.apply(
        lambda x: x['APPT_DATE_DT'] if (x['FC Replenishment Type'] == 'DFC' and pd.notna(x['APPT_DATE_DT']) and x['APPT_DATE_DT'] >= today) else pd.NaT, axis=1)

    # Grouping by ASIN
    # We will include CLUSTER and FC Replenishment Type in the group to maintain geographic accuracy
    groupby_cols = ['ASIN', 'CLUSTER', 'FC Replenishment Type']

    agg_funcs = {
        'Appointment Pending Qty': 'sum',
        'Upcoming Shipment Qty': 'sum',
        'Receiving + Intransit Qty': 'sum',
        'Receiving Qty': 'sum',
        'Intransit Qty': 'sum',
        'Shipment_Detail': lambda x: "[" + ", ".join(x.dropna().unique()) + "]",
        'DFC Next Arrival Date': 'min'
    }

    print("Aggregating grouped data...")
    report_df = df.groupby(groupby_cols, dropna=False).agg(agg_funcs).reset_index()

    # Format the DFC Next Arrival Date back to string
    report_df['DFC Next Arrival Date'] = report_df['DFC Next Arrival Date'].dt.strftime('%Y-%m-%d').fillna('')

    # Rename columns to match requested output
    report_df.rename(columns={'Shipment_Detail': 'Shipment ID', 'CLUSTER': 'Cluster'}, inplace=True)

    # Final Output columns
    final_cols = [
        'ASIN', 
        'Cluster', 
        'FC Replenishment Type', 
        'Appointment Pending Qty', 
        'Upcoming Shipment Qty', 
        'Receiving + Intransit Qty', 
        'Receiving Qty',
        'Intransit Qty',
        'Shipment ID',
        'DFC Next Arrival Date'
    ]
    report_df = report_df[final_cols]

    # Save output
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"Saving final report to {output_file}...")
    try:
        report_df.to_excel(output_file, index=False)
        print(f"Successfully generated final shipment report! Total rows: {len(report_df)}")
    except Exception as e:
        print(f"Error saving output file: {e}")

if __name__ == "__main__":
    # Dynamic path resolution for standalone testing
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    SHIPMENT_FILE = os.path.join(CURRENT_DIR, "Files", "Shipment 1.xlsx")
    MAPPING_FILE = os.path.join(CURRENT_DIR, "Files", "FC_Cluster_Mapping 3.xlsx")
    OUTPUT_FILE = os.path.join(CURRENT_DIR, "output", "Shipment_Report_Final.xlsx")
    
    if not os.path.exists(SHIPMENT_FILE):
        print(f"Please ensure {SHIPMENT_FILE} exists.")
    elif not os.path.exists(MAPPING_FILE):
        print(f"Please ensure {MAPPING_FILE} exists.")
    else:
        generate_shipment_report(SHIPMENT_FILE, MAPPING_FILE, OUTPUT_FILE)
