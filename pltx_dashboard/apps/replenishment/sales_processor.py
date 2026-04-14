import os
import pandas as pd

def process_sales_report(sales_file, pin_file, output_file):
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    if not os.path.exists(sales_file) or not os.path.exists(pin_file):
        print(f"Error: Required files not found.")
        return None

    print("Loading datasets...")
    # Load the datasets, treating postal codes as strings
    df_sales = pd.read_csv(sales_file, dtype={'Shipment To Postal Code': str})
    df_pin = pd.read_csv(pin_file, dtype={'PIN CODE': str})

    print("Processing Dates and Numerical Columns...")
    # Date parsing: Separate into proper date and time, ignoring time
    # The format is like 2026-03-14T23:59:48+05:30
    date_time_split = df_sales['Customer Shipment Date'].astype(str).str.split('T', n=1, expand=True)
    
    # Overwrite with only the proper Date part (ignoring time string)
    df_sales['Customer Shipment Date'] = pd.to_datetime(date_time_split[0], errors='coerce')

    # Calculate Total from amounts
    for col in ['Product Amount', 'Shipping Amount', 'Gift Amount']:
        if col in df_sales.columns:
            df_sales[col] = pd.to_numeric(df_sales[col], errors='coerce').fillna(0)
    
    df_sales['Total'] = df_sales.get('Product Amount', 0) + df_sales.get('Shipping Amount', 0) + df_sales.get('Gift Amount', 0)

    print("Preparing Pincodes for Merge...")
    # Clean '.0' trailing decimals from zip codes if they were inferred as floats originally
    df_sales['Shipment To Postal Code'] = df_sales['Shipment To Postal Code'].str.replace(r'\.0$', '', regex=True).str.strip()
    df_pin['PIN CODE'] = df_pin['PIN CODE'].str.replace(r'\.0$', '', regex=True).str.strip()

    # De-duplicate PIN file to ensure a clean 1:1 map
    df_pin_unique = df_pin.drop_duplicates(subset=['PIN CODE'])

    print("Merging Sales data with PIN mappings...")
    # Perform Left Join mapping Shipment To Postal Code to PIN CODE
    df_merged = pd.merge(
        df_sales,
        df_pin_unique[['PIN CODE', 'Fulfilment Cluster', 'Zone']],
        left_on='Shipment To Postal Code',
        right_on='PIN CODE',
        how='left'
    )

    # Handle Missing mappings
    df_merged['Fulfilment Cluster'] = df_merged['Fulfilment Cluster'].fillna('N/A')
    df_merged['Zone'] = df_merged['Zone'].fillna('N/A')

    print("Calculating file-level Sales Day Count...")
    # File-level: find max and min of Customer Shipment Date across the entire file
    file_max_date = df_merged['Customer Shipment Date'].max()
    file_min_date = df_merged['Customer Shipment Date'].min()
    file_sales_day_count = max((file_max_date - file_min_date).days, 1)  # At least 1 day
    print(f"  Date range: {file_min_date} to {file_max_date} = {file_sales_day_count} days")

    print("Grouping Data by ASIN and Cluster Name...")
    # Group by ASIN and Cluster
    grouped = df_merged.groupby(['ASIN', 'Fulfilment Cluster'])

    # Using Named Aggregation for clarity
    df_result = grouped.agg(
        Sales_Qty=('Quantity', 'sum'),
        Unique_Order_Count=('Amazon Order Id', 'nunique'),
        Unique_Pincode_Count=('Shipment To Postal Code', 'nunique'),
        Total_Sum=('Total', 'sum'),
        Demand_Zone=('Zone', 'first')
    ).reset_index()

    print("Calculating metrics...")
    # Rename cluster
    df_result.rename(columns={'Fulfilment Cluster': 'Cluster Name'}, inplace=True)

    # Sales Day Count = (max date - min date) across the entire file, applied to every row
    df_result['Sales Day Count'] = file_sales_day_count

    # DRR
    df_result['DRR'] = df_result['Sales_Qty'] / df_result['Sales Day Count']
    df_result['DRR'] = df_result['DRR'].round(2)
    df_result['Total'] = df_result['Total_Sum'].round(2)

    # Final Column Selection
    final_columns = [
        'ASIN',
        'Cluster Name',
        'Sales Qty',
        'Sales Day Count',
        'DRR',
        'Total',
        'Unique Order Count',
        'Unique Pincode Count',
        'Demand Zone'
    ]

    # Rename columns to match final expected output
    rename_mapping = {
        'Sales_Qty': 'Sales Qty',
        'Unique_Order_Count': 'Unique Order Count',
        'Unique_Pincode_Count': 'Unique Pincode Count',
        'Demand_Zone': 'Demand Zone'
    }
    df_result.rename(columns=rename_mapping, inplace=True)

    df_final = df_result[final_columns]

    print("Saving Final Report...")
    df_final.to_csv(output_file, index=False)
    print(f"Successfully saved {len(df_final)} clustered records to {output_file}")
    
    return df_final

if __name__ == "__main__":
    # Dynamic path resolution for standalone testing
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
    
    process_sales_report(
        sales_file=os.path.join(CURRENT_DIR, "Files", "Sales 1.csv"),
        pin_file=os.path.join(CURRENT_DIR, "Files", "PIN CODE and CLUSTER 1.csv"),
        output_file=os.path.join(CURRENT_DIR, "output", "Final_Sales_Report.csv")
    )
