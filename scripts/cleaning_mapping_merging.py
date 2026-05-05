import pandas as pd
import glob
import os

def clean_currency(x):
    """
    Cleans string representation of currency (e.g. '₹44,275.00') into float.
    """
    if isinstance(x, str):
        return float(x.replace(',', '').replace('₹', '').replace('$', '').strip())
    return float(x) if pd.notnull(x) else 0.0

def clean_number(x):
    """
    Cleans string representation of numbers (e.g. '2,559') into integer.
    """
    if isinstance(x, str):
        return int(x.replace(',', '').strip())
    return int(x) if pd.notnull(x) else 0

def process_reports(base_dir):
    print("Starting data ingestion process...")

    # ==========================
    # 1. LOAD & CLEAN SALES DATA
    # ==========================
    # The sales data is split across multiple daily CSV files (e.g., 01-03-2026.csv)
    # Columns expected: ['(Child) ASIN', 'Page Views - Total', 'Units Ordered', 'Ordered Product Sales', 'Total Order Items']
    sales_files = glob.glob(os.path.join(base_dir, '*-03-2026.csv'))
    
    if not sales_files:
        print("No daily sales CSV files found.")
        return

    print(f"Found {len(sales_files)} sales files. Reading and combining...")
    daily_sales_list = []
    for file in sales_files:
        df = pd.read_csv(file)
        daily_sales_list.append(df)
        
    # Concatenate all daily reports into one vertical stack
    sales_df = pd.concat(daily_sales_list, ignore_index=True)
    
    # Rename columns to standard simple names
    sales_df.rename(columns={
        '(Child) ASIN': 'ASIN',
        'Page Views - Total': 'Page Views',
        'Units Ordered': 'Units',
        'Ordered Product Sales': 'Revenue',
        'Total Order Items': 'Orders'
    }, inplace=True)
    
    # Clean up formatting (comma separators and currency signs)
    sales_df['Revenue'] = sales_df['Revenue'].apply(clean_currency)
    sales_df['Page Views'] = sales_df['Page Views'].apply(clean_number)
    sales_df['Units'] = sales_df['Units'].apply(clean_number)
    sales_df['Orders'] = sales_df['Orders'].apply(clean_number)
    
    # Aggregate sales data across all dates at the ASIN level
    aggregated_sales = sales_df.groupby('ASIN').agg({
        'Page Views': 'sum',
        'Units': 'sum',
        'Revenue': 'sum',
        'Orders': 'sum'
    }).reset_index()
    print("Sales data combined, cleaned, and aggregated successfully.")

    # ==========================
    # 2. LOAD AD SPEND DATA
    # ==========================
    ads_file = os.path.join(base_dir, 'Ads_Report_March.xlsx')
    ads_df = pd.read_excel(ads_file)
    # Columns expected: ['Date', 'Ad Account', 'Ad Type', 'ASIN', 'Spend']
    
    # Aggregate spend directly at ASIN level
    ads_aggregated = ads_df.groupby('ASIN')['Spend'].sum().reset_index()
    print("Ad spend data loaded and aggregated.")

    # ==========================
    # 3. LOAD CATEGORY / MAPPING DATA
    # ==========================
    category_file = os.path.join(base_dir, 'Category_dashboard.xlsx')
    category_df = pd.read_excel(category_file)
    # Columns expected: ['ASIN', 'Portfolio', 'Category', 'Subcategory', 'Skus']
    # Ensures only unique ASIN matches for a 1-to-1 merge later
    category_df = category_df.drop_duplicates(subset=['ASIN'])
    print("Category metadata loaded and de-duplicated.")

    # ==========================
    # 4. LOAD PRICING DATA
    # ==========================
    pricing_file = os.path.join(base_dir, 'March_live_prices.xlsx')
    pricing_df = pd.read_excel(pricing_file)
    # Columns expected: ['ASIN', 'Price']
    pricing_df = pricing_df.drop_duplicates(subset=['ASIN'])
    print("Pricing data loaded.")

    # ==========================
    # 5. MERGE ALL DATASETS
    # ==========================
    # Use 'outer' join to keep visibility on ASINs that might have had Spend but no Sales, or vice-versa.
    merged_df = pd.merge(aggregated_sales, category_df, on='ASIN', how='outer')
    merged_df = pd.merge(merged_df, ads_aggregated, on='ASIN', how='outer')
    merged_df = pd.merge(merged_df, pricing_df, on='ASIN', how='outer')
    
    # Fill missing values (NaN) that resulted from asymmetric merges
    merged_df['Spend'] = merged_df['Spend'].fillna(0)
    merged_df['Revenue'] = merged_df['Revenue'].fillna(0)
    merged_df['Orders'] = merged_df['Orders'].fillna(0)
    merged_df['Page Views'] = merged_df['Page Views'].fillna(0)
    merged_df['Units'] = merged_df['Units'].fillna(0)
    merged_df['Price'] = merged_df['Price'].fillna(0)
    merged_df['Category'] = merged_df['Category'].fillna('Unknown')
    merged_df['Subcategory'] = merged_df['Subcategory'].fillna('Unknown')
    merged_df['Portfolio'] = merged_df['Portfolio'].fillna('Unknown')
    print("All datasets merged with missing values padded correctly.")

    # ==========================
    # 6. CALCULATE CORE METRICS
    # ==========================
    # Based on the logic in 'dashboards_logic.md'
    
    # -- A. SALES EFFICIENCY --
    # ROAS (Return on Ad Spend) — uses Revenue * 0.7
    merged_df['ROAS'] = merged_df.apply(lambda row: (row['Revenue'] * 0.7 / row['Spend']) if row['Spend'] > 0 else 0, axis=1)
    
    # TACoS (Total Advertising Cost of Sale) — uses Revenue * 0.7
    merged_df['TACoS (%)'] = merged_df.apply(lambda row: (row['Spend'] / (row['Revenue'] * 0.7) * 100) if row['Revenue'] > 0 else 0, axis=1)
    
    # CVR (Conversion Rate)
    merged_df['CVR (%)'] = merged_df.apply(lambda row: (row['Orders'] / row['Page Views'] * 100) if row['Page Views'] > 0 else 0, axis=1)
    
    # -- B. PROFITABILITY --
    
    # We will assume 'Price' from pricing report represents our Cost factor (COGS) for this example
    merged_df['COGS (Total)'] = merged_df['Units'] * merged_df['Price']
    
    # Gross Margin 
    merged_df['Gross Margin'] = merged_df['Revenue'] - merged_df['COGS (Total)']
    
    # Gross Margin %
    merged_df['Gross Margin (%)'] = merged_df.apply(lambda row: (row['Gross Margin'] / row['Revenue'] * 100) if row['Revenue'] > 0 else 0, axis=1)
    
    # Net Profit
    merged_df['Net Profit'] = merged_df['Gross Margin'] - merged_df['Spend']
    
    # Contribution Margin % (Gross Margin % - TACoS %)
    merged_df['Contribution Margin (%)'] = merged_df['Gross Margin (%)'] - merged_df['TACoS (%)']
    
    # Round all metrics for a readable CSV output
    cols_to_round = ['ROAS', 'TACoS (%)', 'CVR (%)', 'Gross Margin', 'Gross Margin (%)', 'Net Profit', 'Contribution Margin (%)']
    merged_df[cols_to_round] = merged_df[cols_to_round].round(2)
    print("All core metrics computed based on dashboard logic.")

    # ==========================
    # 7. GENERATE ANNEXURE
    # ==========================
    annexure_data = [
        {"Metric": "ROAS", "Formula": "(Revenue × 0.7) / Spend", "Description": "Return on Ad Spend: For every ₹1 spent on ads, how much adjusted revenue was generated. Revenue is GST-exclusive and multiplied by 0.7."},
        {"Metric": "TACoS (%)", "Formula": "(Spend / (Revenue × 0.7)) * 100", "Description": "Total Advertising Cost of Sale: Percentage of adjusted revenue spent on advertising. Revenue is GST-exclusive and multiplied by 0.7."},
        {"Metric": "CVR (%)", "Formula": "(Orders / Page Views) * 100", "Description": "Conversion Rate: Percentage of people who viewed the product and actually bought it."},
        {"Metric": "COGS (Total)", "Formula": "Units * Price", "Description": "Cost of Goods Sold: Estimated total cost of products sold based on the pricing data report."},
        {"Metric": "Gross Margin", "Formula": "Revenue - COGS (Total)", "Description": "Gross Margin: Profit after accounting for product costs but before advertising costs."},
        {"Metric": "Gross Margin (%)", "Formula": "(Gross Margin / Revenue) * 100", "Description": "Gross Margin Percentage: Profitability percentage before advertising."},
        {"Metric": "Net Profit", "Formula": "Gross Margin - Spend", "Description": "Net Profit: The money left over after paying for products and ads."},
        {"Metric": "Contribution Margin (%)", "Formula": "Gross Margin (%) - TACoS (%)", "Description": "Contribution Margin Percentage: A health score for profitability; how much each sale contributes after ads."}
    ]
    annexure_df = pd.DataFrame(annexure_data)

    # ==========================
    # 8. GENERATE OUTPUT FILES
    # ==========================
    # Save as CSV
    csv_output_path = os.path.join(base_dir, 'final_mapped_merged_data.csv')
    merged_df.to_csv(csv_output_path, index=False)
    
    # Save as Excel with Annexure sheet
    excel_output_path = os.path.join(base_dir, 'final_mapped_merged_data.xlsx')
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        merged_df.to_excel(writer, sheet_name='Data', index=False)
        annexure_df.to_excel(writer, sheet_name='Annexure', index=False)
    
    print(f"\n==============================================")
    print(f"SUCCESS: Cleaning, mapping, and merging completed.")
    print(f"Total ASINs processed: {len(merged_df)}")
    print(f"Final dataset exported to CSV: \n{csv_output_path}")
    print(f"Final dataset with Annexure exported to Excel: \n{excel_output_path}")
    print(f"==============================================")


if __name__ == '__main__':
    # Define path based on the upload folder structure you specified
    base_uploads_dir = '/Users/apple/Documents/Devarth Nanavaty/Backup/Python/Django/Plantex Sales Dashboard/pltx_dashboard/media/uploads/1'
    process_reports(base_uploads_dir)
