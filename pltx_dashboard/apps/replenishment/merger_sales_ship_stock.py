import pandas as pd
import os
import itertools


def load_data(file_path):
    """Load data from either CSV or Excel format."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext == ".csv":
        try:
            return pd.read_csv(file_path)
        except UnicodeDecodeError:
            return pd.read_csv(file_path, encoding="latin1")
    else:
        try:
            return pd.read_excel(file_path)
        except ValueError:
            return pd.read_excel(file_path, engine="openpyxl")


def generate_master_report(
    sales_file,
    stock_file,
    shipment_file,
    product_details_file,
    business_report_file,
    database_file,
    raw_sales_file,
    pin_code_file,
    fc_mapping_file,
    input_sheet_file,
    output_file,
):
    print("Loading processed reports...")

    # --- Load Sales Data ---
    try:
        sales_df = pd.read_csv(sales_file)
        print(f" Loaded Sales Data: {len(sales_df)} rows")
    except Exception as e:
        print(f" Error reading sales file: {e}")
        sales_df = pd.DataFrame()

    # --- Load Stock Data ---
    try:
        stock_df = load_data(stock_file)
        print(f" Loaded Stock Data: {len(stock_df)} rows")
    except Exception as e:
        print(f" Error reading stock file: {e}")
        stock_df = pd.DataFrame()

    # --- Load Shipment Data ---
    try:
        ship_df = load_data(shipment_file)
        print(f" Loaded Shipment Data: {len(ship_df)} rows")
    except Exception as e:
        print(f" Error reading shipment file: {e}")
        ship_df = pd.DataFrame()

    # --- Load Product Details Data ---
    try:
        product_details_df = load_data(product_details_file)
        print(f" Loaded Product Details Data: {len(product_details_df)} rows")
    except Exception as e:
        print(f" Error reading product details file: {e}")
        product_details_df = pd.DataFrame()

    # --- Load Business Report Data ---
    try:
        business_df = pd.read_csv(business_report_file)
        print(f" Loaded Business Report Data: {len(business_df)} rows")
    except Exception as e:
        print(f" Error reading business report file: {e}")
        business_df = pd.DataFrame()

    # --- Load Database (LIS) Data ---
    try:
        database_df = load_data(database_file)
        print(f" Loaded Database (LIS) Data: {len(database_df)} rows")
    except Exception as e:
        print(f" Error reading database file: {e}")
        database_df = pd.DataFrame()

    # --- Generate Ideal Cluster Mapping ---
    try:
        raw_sales_df = pd.read_csv(
            raw_sales_file, dtype={"Shipment To Postal Code": str}
        )
        pin_code_df = pd.read_csv(pin_code_file, dtype={"PIN CODE": str})
        fc_mapping_df = load_data(fc_mapping_file)
        print(
            f" Loaded Raw Sales ({len(raw_sales_df)} rows), Pin Codes ({len(pin_code_df)} rows), and FC Mapping ({len(fc_mapping_df)} rows)"
        )

        # Clean postal codes
        raw_sales_df["Shipment To Postal Code"] = (
            raw_sales_df["Shipment To Postal Code"]
            .str.replace(r"\.0$", "", regex=True)
            .str.strip()
        )
        pin_code_df["PIN CODE"] = (
            pin_code_df["PIN CODE"].str.replace(r"\.0$", "", regex=True).str.strip()
        )

        # Merge to get Ideal Cluster
        raw_sales_merged = pd.merge(
            raw_sales_df,
            pin_code_df[["PIN CODE", "Fulfilment Cluster", "Ideal Cluster"]],
            left_on="Shipment To Postal Code",
            right_on="PIN CODE",
            how="left",
        )

        # Group by ASIN and Fulfilment Cluster -> get Ideal Cluster
        raw_sales_merged = raw_sales_merged.dropna(subset=["ASIN"])
        raw_sales_merged["ASIN"] = raw_sales_merged["ASIN"].astype(str)
        ideal_mapping = (
            raw_sales_merged.groupby(["ASIN", "Fulfilment Cluster"])["Ideal Cluster"]
            .first()
            .reset_index()
        )
        ideal_mapping.rename(
            columns={
                "Fulfilment Cluster": "Ideal Cluster",
                "Ideal Cluster": "Ideal Cluster name",
            },
            inplace=True,
        )
        ideal_mapping["Ideal Cluster"] = ideal_mapping["Ideal Cluster"].astype(str)
    except Exception as e:
        print(f" Error generating Ideal Cluster mapping: {e}")
        ideal_mapping = pd.DataFrame(
            columns=["ASIN", "Cluster Name", "Ideal Cluster name"]
        )

    # --- Load Input Sheet Parameters ---
    print("\nLoading Input Sheet Parameters...")
    try:
        input_sheet_df = pd.read_excel(input_sheet_file)
        input_sheet_df.columns = input_sheet_df.columns.str.strip()

        def get_input_val(particular, default):
            try:
                val = input_sheet_df[input_sheet_df["Particular"] == particular][
                    "Value"
                ].values[0]
                return val
            except Exception:
                return default

        p0_days = int(str(get_input_val("P0 Demand DOC", 15)).split()[0])
        p1_days = int(str(get_input_val("P1 Demand DOC", 30)).split()[0])
        p2_days = int(str(get_input_val("P2 Demand DOC", 60)).split()[0])

        # Try to get Stock Report Date
        stock_date_val = get_input_val(
            "Stock Report Date", pd.Timestamp.now().strftime("%Y-%m-%d")
        )
        stock_report_date = pd.to_datetime(stock_date_val)
        print(
            f"  Parameters: P0={p0_days}, P1={p1_days}, P2={p2_days}, Stock Date={stock_report_date.date()}"
        )
    except Exception as e:
        print(f" Error loading input sheet: {e}. Using defaults.")
        p0_days, p1_days, p2_days = 15, 30, 60
        stock_report_date = pd.Timestamp.now()

    print("\nExtracting ASINs and Clusters...")
    # Get all unique ASINs and Clusters across all three reports
    asin_set = set()
    cluster_set = set()

    for df, cluster_col in [
        (sales_df, "Cluster Name"),
        (stock_df, "Cluster Name"),
        (ship_df, "Cluster"),
    ]:
        if not df.empty:
            if "ASIN" in df.columns:
                asin_set.update(df["ASIN"].dropna().astype(str).unique())
            if cluster_col and cluster_col in df.columns:
                cluster_set.update(df[cluster_col].dropna().astype(str).unique())

    if not business_df.empty and "(Child) ASIN" in business_df.columns:
        asin_set.update(business_df["(Child) ASIN"].dropna().astype(str).unique())

    if not database_df.empty and "ASIN" in database_df.columns:
        asin_set.update(database_df["ASIN"].dropna().astype(str).unique())

    if not product_details_df.empty and "ASIN" in product_details_df.columns:
        asin_set.update(product_details_df["ASIN"].dropna().astype(str).unique())

    # Add clusters from mapping files to ensure all possible clusters are covered
    if "Fulfilment Cluster" in pin_code_df.columns:
        cluster_set.update(
            pin_code_df["Fulfilment Cluster"].dropna().astype(str).unique()
        )
    if "Ideal Cluster" in pin_code_df.columns:
        cluster_set.update(pin_code_df["Ideal Cluster"].dropna().astype(str).unique())
    if "Cluster Name" in fc_mapping_df.columns:
        cluster_set.update(fc_mapping_df["Cluster Name"].dropna().astype(str).unique())

    asin_list = sorted(list(asin_set))
    cluster_list = sorted(list(cluster_set))

    print(
        f" Found {len(asin_list)} unique ASINs and {len(cluster_list)} unique Clusters."
    )
    print(" Generating Master Cartesian Product...")

    # --- Create Cartesian Product ---
    combinations = list(itertools.product(asin_list, cluster_list))
    master_df = pd.DataFrame(combinations, columns=["ASIN", "Ideal Cluster"])

    # --- Merge Ideal Cluster name ---
    print("\nMerging Ideal Cluster Mapping...")
    master_df = pd.merge(
        master_df, ideal_mapping, on=["ASIN", "Ideal Cluster"], how="left"
    )
    master_df["Ideal Cluster name"] = master_df["Ideal Cluster name"].fillna("")

    # --- Merge Sales Data ---
    print("\nMerging Sales Data...")
    if not sales_df.empty:
        # Ensure correct data types before merge
        sales_cols_to_keep = ["ASIN", "Cluster Name", "DRR", "Sales Qty"]
        if "Demand Zone" in sales_df.columns:
            sales_cols_to_keep.append("Demand Zone")

        sales_subset = sales_df[sales_cols_to_keep].copy()
        sales_subset["ASIN"] = sales_subset["ASIN"].astype(str)
        sales_subset.rename(columns={"Cluster Name": "Ideal Cluster"}, inplace=True)
        sales_subset["Ideal Cluster"] = sales_subset["Ideal Cluster"].astype(str)
        if "Demand Zone" in sales_cols_to_keep:
            sales_subset.rename(columns={"Demand Zone": "Zone"}, inplace=True)

        master_df = pd.merge(
            master_df, sales_subset, on=["ASIN", "Ideal Cluster"], how="left"
        )
    else:
        master_df["DRR"] = 0
        master_df["Sales Qty"] = 0
        master_df["Zone"] = ""

    if "Zone" not in master_df.columns:
        master_df["Zone"] = ""

    # --- Merge Stock Data ---
    print("Merging Stock Data...")
    if not stock_df.empty:
        stock_df["ASIN"] = stock_df["ASIN"].astype(str)
        stock_df.rename(columns={"Cluster Name": "Ideal Cluster"}, inplace=True)
        stock_df["Ideal Cluster"] = stock_df["Ideal Cluster"].astype(str)
        # Group by in case multiple FCs belong to the same cluster mapping
        stock_cols = ["Ending Warehouse Balance"]
        if "In Transit Between Warehouses" in stock_df.columns:
            stock_cols.append("In Transit Between Warehouses")
        stock_agg = stock_df.groupby(["ASIN", "Ideal Cluster"], as_index=False)[
            stock_cols
        ].sum()
        stock_agg.rename(
            columns={"Ending Warehouse Balance": "Stock Qty"}, inplace=True
        )
        if "In Transit Between Warehouses" in stock_agg.columns:
            stock_agg.rename(
                columns={"In Transit Between Warehouses": "Stock Transfer Qty"},
                inplace=True,
            )
        master_df = pd.merge(
            master_df, stock_agg, on=["ASIN", "Ideal Cluster"], how="left"
        )
    else:
        master_df["Stock Qty"] = 0

    # --- Merge Shipment Data ---
    print("Merging Shipment Data...")
    if not ship_df.empty:
        ship_df["ASIN"] = ship_df["ASIN"].astype(str)
        ship_df.rename(columns={"Cluster": "Ideal Cluster"}, inplace=True)
        ship_df["Ideal Cluster"] = ship_df["Ideal Cluster"].astype(str)

        # Split into DFC and IXD
        dfc_df = ship_df[ship_df["FC Replenishment Type"] == "DFC"].copy()
        ixd_df = ship_df[ship_df["FC Replenishment Type"] == "IXD"].copy()

        # Rename DFC columns
        dfc_df.rename(
            columns={
                "Appointment Pending Qty": "DFC Appointment Pending Qty",
                "Upcoming Shipment Qty": "DFC Upcoming Shipment Qty",
                "Receiving + Intransit Qty": "DFC Receiving + Intransit Qty",
                "Receiving Qty": "DFC Receiving Qty",
                "Intransit Qty": "DFC Intransit Qty",
                "Shipment ID": "DFC Shipment ID",
                "DFC Next Arrival Date": "DFC Next Arrival Date",
            },
            inplace=True,
        )

        # Merge DFC
        dfc_cols = [
            "ASIN",
            "Ideal Cluster",
            "DFC Appointment Pending Qty",
            "DFC Upcoming Shipment Qty",
            "DFC Receiving + Intransit Qty",
            "DFC Receiving Qty",
            "DFC Intransit Qty",
            "DFC Shipment ID",
            "DFC Next Arrival Date",
        ]
        dfc_merge_subset = dfc_df[[c for c in dfc_cols if c in dfc_df.columns]]
        master_df = pd.merge(
            master_df, dfc_merge_subset, on=["ASIN", "Ideal Cluster"], how="left"
        )

        # Rename IXD columns
        ixd_df.rename(
            columns={
                "Appointment Pending Qty": "IXD Appointment Pending Qty",
                "Upcoming Shipment Qty": "IXD Upcoming Shipment Qty",
                "Receiving + Intransit Qty": "IXD Receiving + Intransit Qty",
                "Receiving Qty": "IXD Receiving Qty",
                "Intransit Qty": "IXD Intransit Qty",
                "Shipment ID": "IXD Shipment ID",
            },
            inplace=True,
        )

        # Merge IXD
        ixd_cols = [
            "ASIN",
            "Ideal Cluster",
            "IXD Appointment Pending Qty",
            "IXD Upcoming Shipment Qty",
            "IXD Receiving + Intransit Qty",
            "IXD Receiving Qty",
            "IXD Intransit Qty",
            "IXD Shipment ID",
        ]
        ixd_merge_subset = ixd_df[[c for c in ixd_cols if c in ixd_df.columns]]
        master_df = pd.merge(
            master_df, ixd_merge_subset, on=["ASIN", "Ideal Cluster"], how="left"
        )
    else:
        # Create missing columns
        shipment_empty_cols = [
            "DFC Appointment Pending Qty",
            "DFC Upcoming Shipment Qty",
            "DFC Receiving + Intransit Qty",
            "DFC Receiving Qty",
            "DFC Intransit Qty",
            "DFC Shipment ID",
            "DFC Next Arrival Date",
            "IXD Appointment Pending Qty",
            "IXD Upcoming Shipment Qty",
            "IXD Receiving + Intransit Qty",
            "IXD Receiving Qty",
            "IXD Intransit Qty",
            "IXD Shipment ID",
        ]
        for col in shipment_empty_cols:
            master_df[col] = 0

    # --- Merge Product Details Data ---
    print("Merging Product Details Data...")
    if not product_details_df.empty:
        product_details_df["ASIN"] = product_details_df["ASIN"].astype(str)
        prod_cols = [
            "ASIN",
            "SKU",
            "HSN CODE",
            "VENDOR NAME",
            "PRODUCTS STATUS",
            "ACT WEIGHT",
            "VOLUMETRIC WEIGHT",
            "PRODUCT TYPE",
            "PRODUCT SIZE",
            "Portfolio",
            "Category",
        ]
        prod_subset = product_details_df[
            [c for c in prod_cols if c in product_details_df.columns]
        ].drop_duplicates(subset=["ASIN"])
        master_df = pd.merge(master_df, prod_subset, on="ASIN", how="left")
    else:
        for col in [
            "SKU",
            "HSN CODE",
            "VENDOR NAME",
            "PRODUCTS STATUS",
            "ACT WEIGHT",
            "VOLUMETRIC WEIGHT",
            "PRODUCT TYPE",
            "PRODUCT SIZE",
            "Portfolio",
            "Category",
        ]:
            master_df[col] = ""

    # --- Merge Business Report Data ---
    print("Merging Business Report Data...")
    if not business_df.empty:
        business_df["(Child) ASIN"] = business_df["(Child) ASIN"].astype(str)
        business_cols = [
            "(Child) ASIN",
            "Page Views - Total",
            "Units Ordered",
            "Ordered Product Sales",
            "Total Order Items",
        ]
        b_subset = (
            business_df[[c for c in business_cols if c in business_df.columns]]
            .drop_duplicates(subset=["(Child) ASIN"])
            .copy()
        )

        # We need to map this to master_df by ASIN.
        # master_df['ASIN'] == business_df['(Child) ASIN']
        # We keep '(Child) ASIN' in b_subset, but create 'ASIN' for joining
        b_subset["ASIN"] = b_subset["(Child) ASIN"]
        master_df = pd.merge(master_df, b_subset, on="ASIN", how="left")
    else:
        for col in [
            "(Child) ASIN",
            "Page Views - Total",
            "Units Ordered",
            "Ordered Product Sales",
            "Total Order Items",
        ]:
            master_df[col] = ""

    # --- Merge Database (LIS) Data ---
    print("Merging Database (LIS) Data...")
    if not database_df.empty:
        database_df["ASIN"] = database_df["ASIN"].astype(str)
        if "Cluster" in database_df.columns:
            database_df["Cluster"] = database_df["Cluster"].astype(str)

        lis_mapping = {
            "CHN_CLUSTER": "CHENNAI",
            "NAG_CLUSTER": "NAGPUR",
            "GAA_CLUSTER": "GUWAHATI",
            "PAT_CLUSTER": "PATNA",
            "SAT_CLUSTER": "LUDHIANA",
            "PUN_CLUSTER": "PUNE",
            "AMD_CLUSTER": "AHMEDABAD",
            "HRA_CLUSTER": "DELHI",
            "BLR_CLUSTER": "BANGALORE",
            "IND_CLUSTER": "INDORE",
            "BOM_CLUSTER": "MUMBAI",
            "BHU_CLUSTER": "BHUBANESWAR",
            "JAI_CLUSTER": "JAIPUR",
            "COI_CLUSTER": "COIMBATORE",
            "HYD_CLUSTER": "HYDERABAD",
            "LKO_CLUSTER": "LUCKNOW",
            "KOL_CLUSTER": "KOLKATA",
            "HUB_CLUSTER": "HUBLI",
        }

        # Determine the LIS Cluster Name first!
        # Map LIS_CLUSTER based exclusively on the Ideal Cluster column
        master_df["LIS_CLUSTER"] = (
            master_df["Ideal Cluster"].map(lis_mapping).fillna("")
        )

        db_cols = [
            "ASIN",
            "Cluster",
            "Sum of Local Shipped Units",
            "Sum of Total Units",
        ]
        db_subset = database_df[[c for c in db_cols if c in database_df.columns]].copy()

        db_subset.rename(
            columns={
                "Cluster": "LIS_CLUSTER",
                "Sum of Local Shipped Units": "LIS_LOCAL_QTY",
                "Sum of Total Units": "LIS_TOTAL_QTY",
            },
            inplace=True,
        )

        db_agg = db_subset.groupby(["ASIN", "LIS_CLUSTER"], as_index=False)[
            ["LIS_LOCAL_QTY", "LIS_TOTAL_QTY"]
        ].sum()

        # Merge database data onto master_df using ASIN and LIS_CLUSTER
        # Ensure data types are consistent for merge
        db_agg["LIS_CLUSTER"] = db_agg["LIS_CLUSTER"].astype(str)
        master_df["LIS_CLUSTER"] = master_df["LIS_CLUSTER"].astype(str)
        master_df = pd.merge(master_df, db_agg, on=["ASIN", "LIS_CLUSTER"], how="left")

    else:
        master_df["LIS_LOCAL_QTY"] = 0
        master_df["LIS_TOTAL_QTY"] = 0
        master_df["LIS_CLUSTER"] = ""

    # --- Add BSR ---
    master_df["BSR"] = ""

    # --- Fill NaNs ---
    print("\nData Cleanup & Calculations...")
    numeric_columns = [
        "DRR",
        "Sales Qty",
        "Stock Qty",
        "Stock Transfer Qty",
        "Receiving Qty",
        "DFC Appointment Pending Qty",
        "DFC Upcoming Shipment Qty",
        "DFC Receiving + Intransit Qty",
        "DFC Receiving Qty",
        "DFC Intransit Qty",
        "IXD Appointment Pending Qty",
        "IXD Upcoming Shipment Qty",
        "IXD Receiving + Intransit Qty",
        "IXD Receiving Qty",
        "IXD Intransit Qty",
        "LIS_LOCAL_QTY",
        "LIS_TOTAL_QTY",
        "Page Views - Total",
        "Units Ordered",
        "Ordered Product Sales",
        "Total Order Items",
    ]

    for col in numeric_columns:
        if col in master_df.columns:
            master_df[col] = (
                pd.to_numeric(master_df[col], errors="coerce").fillna(0).round(2)
            )

    # Fill text NaNs
    for col in ["DFC Shipment ID", "IXD Shipment ID", "DFC Next Arrival Date"]:
        if col in master_df.columns:
            master_df[col] = master_df[col].fillna("")

    # --- Calculate Total Shipment Qty ---
    # As per requirements, Shipment Qty is the sum of all 3 DFC categories.
    master_df["Shipment Qty"] = (
        master_df.get("DFC Appointment Pending Qty", 0)
        + master_df.get("DFC Upcoming Shipment Qty", 0)
        + master_df.get("DFC Intransit Qty", 0)
    )

    # --- Add Conversion Pct ---
    print("Calculating Conversion Pct...")

    # Calculate Receiving Qty and Stock Transfer Qty (handle missing)
    master_df["Receiving Qty"] = master_df.get("DFC Receiving Qty", 0) + master_df.get(
        "IXD Receiving Qty", 0
    )

    # Stock Transfer Qty comes strictly from Stock Report ('In Transit Between Warehouses')
    master_df["Stock Transfer Qty"] = master_df.get(
        "Stock Transfer Qty", 0
    )  # if it already existed from stock_df merge

    master_df["Conversion Pct"] = (
        (master_df["Total Order Items"] / master_df["Page Views - Total"]) * 100
    ).replace([float("inf"), -float("inf")], 0).fillna(0).round(2).astype(str) + "%"

    # --- Calculate P0, P1, P2 ---
    master_df["P0"] = (
        (
            master_df["DRR"] * p0_days
            - master_df["Stock Qty"]
            - master_df["Receiving Qty"]
            - master_df["Stock Transfer Qty"]
            - master_df["Shipment Qty"]
        )
        .clip(lower=0)
        .round(2)
    )
    master_df["P1"] = (
        (
            master_df["DRR"] * p1_days
            - master_df["Stock Qty"]
            - master_df["Receiving Qty"]
            - master_df["Stock Transfer Qty"]
            - master_df["Shipment Qty"]
            - master_df["P0"]
        )
        .clip(lower=0)
        .round(2)
    )
    master_df["P2"] = (
        (
            master_df["DRR"] * p2_days
            - master_df["Stock Qty"]
            - master_df["Receiving Qty"]
            - master_df["Stock Transfer Qty"]
            - master_df["Shipment Qty"]
            - master_df["P1"]
            - master_df["P0"]
        )
        .clip(lower=0)
        .round(2)
    )

    # --- Intelligence Features ---
    print("Calculating Intelligence Features...")
    import numpy as np

    today = pd.Timestamp.now().normalize()

    # Days Diff between today and stock report date

    # Days of Cover
    master_df["Days of Cover"] = (
        (master_df["Stock Qty"] / master_df["DRR"])
        .replace([np.inf, -np.inf], 999)
        .fillna(0)
    )

    # Stock Out Date Calculation
    days_stock_recv = (
        ((master_df["Stock Qty"] + master_df["Receiving Qty"]) / master_df["DRR"])
        .replace([np.inf, -np.inf], 999)
        .fillna(999)
    )
    days_all = (
        (
            (
                master_df["Stock Qty"]
                + master_df["Receiving Qty"]
                + master_df["Shipment Qty"]
            )
            / master_df["DRR"]
        )
        .replace([np.inf, -np.inf], 999)
        .fillna(999)
    )

    expected_out_date_dt = today + pd.to_timedelta(days_stock_recv, unit="D")
    shipment_date_dt = pd.to_datetime(
        master_df.get("DFC Next Arrival Date", ""), errors="coerce"
    )

    condition = shipment_date_dt.notna() & (expected_out_date_dt > shipment_date_dt)
    final_days = np.where(condition, days_all, days_stock_recv)

    def get_stock_out_date(days):
        if pd.isna(days) or days >= 999:
            return "Never"
        return (today + pd.Timedelta(days=int(days))).strftime("%Y-%m-%d")

    master_df["Stock Out Date"] = pd.Series(final_days).apply(get_stock_out_date)

    # Stock Cover Alert
    def get_stock_alert(days):
        if days < 30:
            return "<30 Days"
        if days < 45:
            return "<45 Days"
        if 45 <= days <= 60:
            return "Healthy"
        if days > 120:
            return ">120 Days"
        if days > 90:
            return ">90 Days"
        if days > 60:
            return ">60 Days"
        return ""

    master_df["Stock cover alert"] = master_df["Days of Cover"].apply(get_stock_alert)

    # --- Finalize Dataframe Layout ---
    final_columns = [
        "BSR",
        "ASIN",
        "Ideal Cluster",
        "SKU",
        "HSN CODE",
        "VENDOR NAME",
        "PRODUCTS STATUS",
        "ACT WEIGHT",
        "VOLUMETRIC WEIGHT",
        "PRODUCT TYPE",
        "PRODUCT SIZE",
        "Portfolio",
        "Category",
        "Zone",
        "LIS_CLUSTER",
        "DRR",
        "Sales Qty",
        "Stock Qty",
        "Receiving Qty",
        "Stock Transfer Qty",
        "Shipment Qty",
        "P0",
        "P1",
        "P2",
        "Stock Out Date",
        "Stock cover alert",
        "Page Views - Total",
        "Units Ordered",
        "Total Order Items",
        "Conversion Pct",
        "Ordered Product Sales",
        "LIS_LOCAL_QTY",
        "LIS_TOTAL_QTY",
        "DFC Appointment Pending Qty",
        "DFC Upcoming Shipment Qty",
        "DFC Intransit Qty",
        "DFC Shipment ID",
        "DFC Next Arrival Date",
        "IXD Appointment Pending Qty",
        "IXD Upcoming Shipment Qty",
        "IXD Receiving + Intransit Qty",
        "IXD Shipment ID",
    ]

    # No additional renaming needed after restructuring

    # Reorder filtering only those that exist (to prevent key errors)
    ordered_cols = [col for col in final_columns if col in master_df.columns]
    master_df = master_df[ordered_cols]

    # --- Save Output ---
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    print(f"\nSaving Final Master Report to {output_file}...")
    master_df.to_csv(output_file, index=False)
    print(
        f"\n✅ Successfully generated Final Master Report with {len(master_df)} records!"
    )


if __name__ == "__main__":
    # Dynamic path resolution for standalone testing
    CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))

    # Input file locations (outputs of the previous processors)
    SALES_FILE = os.path.join(CURRENT_DIR, "output", "Final_Sales_Report.csv")
    STOCK_FILE = os.path.join(CURRENT_DIR, "output", "FBA_Mapped_Output.xlsx")
    SHIPMENT_FILE = os.path.join(CURRENT_DIR, "output", "Shipment_Report_Final.xlsx")
    PRODUCT_DETAILS_FILE = os.path.join(CURRENT_DIR, "Files", "Product Details 1.xlsx")
    BUSINESS_REPORT_FILE = os.path.join(
        CURRENT_DIR, "Files", "BusinessReport-02-04-26.csv"
    )
    DATABASE_FILE = os.path.join(CURRENT_DIR, "Files", "Database.xlsx")
    RAW_SALES_FILE = os.path.join(CURRENT_DIR, "Files", "Sales 1.csv")
    PIN_CODE_FILE = os.path.join(CURRENT_DIR, "Files", "PIN CODE and CLUSTER 1.csv")
    INPUT_SHEET_FILE = os.path.join(CURRENT_DIR, "Files", "Input Sheet 1.xlsx")

    # Final output file location
    OUTPUT_FILE = os.path.join(CURRENT_DIR, "output", "Master_Merged_Report.csv")
    FC_MAPPING_FILE = os.path.join(CURRENT_DIR, "Files", "FC_Cluster_Mapping 3.xlsx")

    generate_master_report(
        SALES_FILE,
        STOCK_FILE,
        SHIPMENT_FILE,
        PRODUCT_DETAILS_FILE,
        BUSINESS_REPORT_FILE,
        DATABASE_FILE,
        RAW_SALES_FILE,
        PIN_CODE_FILE,
        FC_MAPPING_FILE,
        INPUT_SHEET_FILE,
        OUTPUT_FILE,
    )
