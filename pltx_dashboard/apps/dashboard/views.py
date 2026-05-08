import csv
import datetime
import json
from copy import deepcopy
from io import BytesIO, StringIO

import pandas as pd
from django.http import FileResponse, JsonResponse
from django.shortcuts import render, redirect

from apps.accounts.decorators import require_feature, _first_allowed_dashboard_for
from apps.accounts.models import Feature
from apps.accounts.utils import get_logged_in_user
from apps.dashboard.models import (
    SpendData,
    ProcessedDashboardData,
    FlipkartProcessedDashboardData,
)
from apps.dashboard.utils import DashboardEncoder

DASHBOARD_PAYLOAD_CACHE_VERSION = 10


def _build_payload_json(payload):
    """
    Return full payload JSON for frontend consumers.
    """
    if not payload:
        return "null"
    return json.dumps(payload, cls=DashboardEncoder, separators=(",", ":"))


def _build_template_payload(payload):
    """
    Keep template payload separate from cached payload mutation.
    """
    return deepcopy(payload) if isinstance(payload, dict) else payload


def _payload_needs_refresh(payload):
    """
    Detect stale cached payloads from older schema versions that can
    cause oversized HTML responses and outdated calculations.
    """
    if not isinstance(payload, dict):
        return True

    inventory = payload.get("inventory")
    if not isinstance(inventory, dict):
        return True

    required_inventory_keys = {
        "details_total",
        "details_shown",
        "details_truncated",
        "has_stock_data",
        "num_sale_days",
    }
    if not required_inventory_keys.issubset(inventory.keys()):
        return True

    return False


def no_cache_for_htmx(view_func):
    """Decorator to prevent caching of HTMX requests"""

    def wrapper(request, *args, **kwargs):
        response = view_func(request, *args, **kwargs)

        # Set no-cache headers for HTMX requests
        if request.headers.get("HX-Request") == "true":
            response["Cache-Control"] = "no-cache, no-store, must-revalidate, max-age=0"
            response["Pragma"] = "no-cache"
            response["Expires"] = "0"

        return response

    return wrapper


def dashboard_view(request):
    # Redirect the user to the first dashboard they have access to.
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")
    route = _first_allowed_dashboard_for(user)
    return redirect(route)


def get_dashboard_context(request):
    user = get_logged_in_user(request)
    if not user:
        return None

    data_owner = user.created_by if user.created_by else user

    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = (
            [f.code_name for f in user.role.features.all()] if user.role else []
        )

    # Define which fields should be treated as lists (multi-selects)
    list_fields = ["category", "asin", "fsn", "portfolio", "subcategory"]

    # Build filters from QueryDict:
    # Logic: For lists, take all non-empty values. For single values, take the standard request.GET.get()
    # (which picks the last value if duplicates exist, and preserves "" for "All" options).
    filters = {}
    for k in request.GET.keys():
        if k in list_fields:
            # Filter out empty strings for lists to keep them clean
            filters[k] = [v for v in request.GET.getlist(k) if v]
        else:
            # Single value: standard Django GET behavior (takes the last one)
            # This is critical for allowing "All" choices (empty strings) to work
            filters[k] = request.GET.get(k, "")

    # selected_filters is used by templates to pre-select multi-select controls (always lists)
    selected_filters = {
        "categories": filters.get("category", []),
        "asins": filters.get("asin", []),
        "fsns": filters.get("fsn", []),
    }

    # Build the queryset with DB-level entity filters
    qs = ProcessedDashboardData.objects.filter(user=data_owner)
    fk_qs = FlipkartProcessedDashboardData.objects.filter(user=data_owner)

    # Apply platform filter
    platform = filters.get("platform")
    show_amazon = True
    show_flipkart = True
    if platform == "Amazon":
        show_flipkart = False
    elif platform == "Flipkart":
        show_amazon = False

    # Extract all available options BEFORE applying entity filters
    from apps.dashboard.services.analytics_services_orm_pipeline import (
        get_available_filters_orm_cached,
    )

    cached_filter_metadata = get_available_filters_orm_cached(
        qs if show_amazon else qs.none(), fk_qs if show_flipkart else fk_qs.none(), data_owner.id, show_amazon, show_flipkart
    )

    # Apply category filter at DB level
    category = filters.get("category")
    if category:
        if isinstance(category, (list, tuple)):
            qs = qs.filter(category__in=category)
            fk_qs = fk_qs.filter(category__in=category)
        else:
            qs = qs.filter(category=category)
            fk_qs = fk_qs.filter(category=category)

    # Apply ASIN filter at DB level
    asin_filter = filters.get("asin")
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            qs = qs.filter(asin__in=asin_filter)
        else:
            qs = qs.filter(asin=asin_filter)

    # Apply FSN filter at DB level
    fsn_filter = filters.get("fsn")
    if fsn_filter:
        if isinstance(fsn_filter, (list, tuple)):
            fk_qs = fk_qs.filter(fsn__in=fsn_filter)
        else:
            fk_qs = fk_qs.filter(fsn=fsn_filter)

    # If user selected an ASIN but no FSN, then empty the Flipkart query
    if asin_filter and not fsn_filter:
        fk_qs = fk_qs.none()
    # If user selected an FSN but no ASIN, then empty the Amazon query
    elif fsn_filter and not asin_filter:
        qs = qs.none()

    # Apply portfolio filter at DB level
    portfolio = filters.get("portfolio")
    if portfolio:
        qs = qs.filter(portfolio=portfolio)
        fk_qs = fk_qs.filter(portfolio=portfolio)

    # Apply subcategory filter at DB level
    subcategory = filters.get("subcategory")
    if subcategory:
        if isinstance(subcategory, (list, tuple)):
            qs = qs.filter(subcategory__in=subcategory)
            fk_qs = fk_qs.filter(subcategory__in=subcategory)
        else:
            qs = qs.filter(subcategory=subcategory)
            fk_qs = fk_qs.filter(subcategory=subcategory)

    if not show_amazon:
        qs = qs.none()
    if not show_flipkart:
        fk_qs = fk_qs.none()

    if not qs.exists() and not fk_qs.exists():
        return {
            "logged_user": user,
            "user_features": user_features,
            "payload": None,
            "payload_json": "null",
            "filters": filters,
            "selected_filters": selected_filters,
            "selected_filters_json": json.dumps(selected_filters),
        }

    # Apply same entity filters to spend data at DB level
    spend_qs = SpendData.objects.filter(user=data_owner)
    if asin_filter:
        if isinstance(asin_filter, (list, tuple)):
            spend_qs = spend_qs.filter(asin__in=asin_filter)
        else:
            spend_qs = spend_qs.filter(asin=asin_filter)

    # Use a versioned cache key to allow instantaneous clearing on upload
    from django.core.cache import cache
    from apps.dashboard.services.analytics_services_orm_pipeline import run_orm_computation
    import hashlib
    
    # Generate unique hash for these filters
    filter_key_str = json.dumps(filters, sort_keys=True)
    cache_hash = hashlib.md5(filter_key_str.encode("utf-8")).hexdigest()
    
    # Get current data version for this user
    data_version = cache.get(f"dashboard_data_version_{data_owner.id}", 0)
    cache_key = (
        f"dashboard_payload_v{DASHBOARD_PAYLOAD_CACHE_VERSION}_"
        f"{data_owner.id}_{data_version}_{cache_hash}"
    )
    
    payload = cache.get(cache_key)
    if payload and _payload_needs_refresh(payload):
        payload = None

    if not payload:
        payload = run_orm_computation(
            qs,
            fk_qs,
            spend_qs,
            filters,
            data_owner,
            cached_filter_metadata=cached_filter_metadata,
        )
        cache.set(cache_key, payload, timeout=3600 * 24)  # Cache for 24 hours

    template_payload = _build_template_payload(payload)

    return {
        "logged_user": user,
        "user_features": user_features,
        "payload": template_payload,
        "payload_json": _build_payload_json(payload),
        "filters": filters,
        "selected_filters": selected_filters,
        "selected_filters_json": json.dumps(selected_filters),
    }


def _inject_htmx(request, ctx):
    """
    Inject base_template into context.
    Ensures base_template is ALWAYS set to prevent extends tag errors.
    """
    # Ensure ctx is always a dict (not None)
    if ctx is None:
        ctx = {
            "logged_user": None,
            "user_features": [],
            "payload": None,
            "payload_json": "null",
            "filters": {},
            "selected_filters": {},
            "selected_filters_json": "{}",
        }

    # Determine which base template to use
    is_htmx_request = request.headers.get("HX-Request") == "true"
    ctx["base_template"] = (
        "dashboard/base_htmx.html"
        if is_htmx_request
        else "dashboard/base_dashboard.html"
    )

    return ctx


# ─────────────────────────────────────────────────────────
# Dashboard views
# ─────────────────────────────────────────────────────────


@require_feature("business_dashboard")
@no_cache_for_htmx
def business_dashboard_view(request):
    ctx = get_dashboard_context(request)
    if ctx is None:
        return redirect("account-login")
    return render(
        request, "dashboard/business_dashboard.html", _inject_htmx(request, ctx)
    )


@require_feature("ceo_dashboard")
@no_cache_for_htmx
def ceo_dashboard_view(request):
    ctx = get_dashboard_context(request)
    if ctx is None:
        return redirect("account-login")
    return render(request, "dashboard/ceo_dashboard.html", _inject_htmx(request, ctx))


@require_feature("category_dashboard")
@no_cache_for_htmx
def category_dashboard_view(request):
    ctx = get_dashboard_context(request)
    if ctx is None:
        return redirect("account-login")
    return render(
        request, "dashboard/category_dashboard.html", _inject_htmx(request, ctx)
    )


@require_feature("upload_data")
def upload_view(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")
    data_owner = user.created_by if user.created_by else user

    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = (
            [f.code_name for f in user.role.features.all()] if user.role else []
        )
    from apps.upload.models import UploadLog
    upload_logs = UploadLog.objects.filter(data_owner=data_owner).select_related(
        "uploaded_by"
    )[:100]

    return render(
        request,
        "dashboard/upload.html",
        {
            "logged_user": user,
            "user_features": user_features,
            "upload_logs": upload_logs,
            "payload_json": "null",
            "selected_filters_json": "{}",
        },
    )


def _demo_specs(today):
    day_ddmmyyyy = today.strftime("%d-%m-%Y")
    day_ymd = today.strftime("%Y-%m-%d")
    now_iso = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        # Upload Data (Amazon)
        "upload_sales": {
            "kind": "csv",
            "filename": f"{day_ddmmyyyy}.csv",
            "columns": [
                "(Child) ASIN",
                "Page Views - Total",
                "Units Ordered",
                "Ordered Product Sales",
                "Total Order Items",
            ],
            "rows": [["B0DEMOASIN1", 245, 18, "₹25,499.00", 17]],
        },
        "upload_category": {
            "kind": "csv",
            "filename": "category_mapping_demo.csv",
            "columns": ["ASIN", "Portfolio", "Category", "Subcategory", "Skus"],
            "rows": [["B0DEMOASIN1", "Home", "Storage", "Bins", "SKU-DEMO-1"]],
        },
        "upload_spend": {
            "kind": "csv",
            "filename": "ads_spend_demo.csv",
            "columns": ["Date", "Ad Account", "Ad Type", "ASIN", "Spend"],
            "rows": [[day_ymd, "Main Ads", "SP", "B0DEMOASIN1", 1250.50]],
        },
        "upload_price": {
            "kind": "csv",
            "filename": "pricing_data_demo.csv",
            "columns": ["ASIN", "Price"],
            "rows": [["B0DEMOASIN1", 1499]],
        },
        "upload_fba_stock": {
            "kind": "csv",
            "filename": "fba_stock_demo.csv",
            "columns": [
                "Date",
                "FNSKU",
                "ASIN",
                "MSKU",
                "Title",
                "Disposition",
                "Starting Warehouse Balance",
                "In Transit Between Warehouses",
                "Receipts",
                "Customer Shipments",
                "Customer Returns",
                "Vendor Returns",
                "Warehouse Transfer In/Out",
                "Found",
                "Lost",
                "Damaged",
                "Disposed",
                "Other Events",
                "Ending Warehouse Balance",
                "Unknown Events",
                "Location",
            ],
            "rows": [
                [
                    day_ymd,
                    "X000DEMOFNSKU",
                    "B0DEMOASIN1",
                    "MSKU-DEMO-1",
                    "Demo Product",
                    "SELLABLE",
                    120,
                    10,
                    15,
                    8,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    0,
                    137,
                    0,
                    "DEL4",
                ]
            ],
        },
        "upload_flex_stock": {
            "kind": "csv",
            "filename": "flex_stock_demo.csv",
            "columns": ["Date", "ASIN", "Cluster", "Qty"],
            "rows": [[day_ymd, "B0DEMOASIN1", "BANGALORE", 42]],
        },
        # Upload Data (Flipkart)
        "fk_search_traffic": {
            "kind": "csv",
            "filename": "fk_search_traffic_demo.csv",
            "columns": [
                "Listing Id",
                "SKU Id",
                "Vertical",
                "Impression Date",
                "Product Clicks",
                "Sales",
                "Revenue",
            ],
            "rows": [["ABCDEMOFSN00000001X", "FK-SKU-1", "Home Furnishing", day_ymd, 320, 22, 28765]],
        },
        "fk_category": {
            "kind": "csv",
            "filename": "fk_category_demo.csv",
            "columns": ["FSN ID", "SKU", "Portfolio", "Cat", "Subcat"],
            "rows": [["DEMOFSN00000001", "FK-SKU-1", "Home", "Storage", "Bins"]],
        },
        "fk_price": {
            "kind": "csv",
            "filename": "fk_price_demo.csv",
            "columns": ["Flipkart Serial Number", "Deal"],
            "rows": [["DEMOFSN00000001", 1349]],
        },
        "fk_pca": {
            "kind": "csv_with_metadata",
            "filename": "fk_pca_demo.csv",
            "metadata_rows": [[f"Start Time,{now_iso}"], [f"End Time,{now_iso}"]],
            "columns": ["campaign_id", "campaign_name", "Date", "fsn_id"],
            "rows": [["CMP-1001", "Summer Promo", day_ymd, "DEMOFSN00000001"]],
        },
        "fk_pla": {
            "kind": "csv_with_metadata",
            "filename": "fk_pla_demo.csv",
            "metadata_rows": [[f"Start Time,{now_iso}"], [f"End Time,{now_iso}"]],
            "columns": ["Campaign ID", "Advertised FSN ID", "Ad Spend"],
            "rows": [["CMP-1001", "DEMOFSN00000001", 842.75]],
        },
        "fk_coupon": {
            "kind": "csv_with_metadata",
            "filename": "fk_coupon_demo.csv",
            "metadata_rows": [[f"Start Time,{now_iso}"], [f"End Time,{now_iso}"]],
            "columns": ["Flipkart Serial Number", "Coupon Value"],
            "rows": [["DEMOFSN00000001", 75]],
        },
        "fk_sales_invoice": {
            "kind": "xlsx_multi",
            "filename": "fk_sales_invoice_demo.xlsx",
            "sheets": {
                "Sales Report": {
                    "columns": ["Order Item ID", "FSN", "Item Quantity"],
                    "rows": [["OI-10001", "DEMOFSN00000001", 2]],
                },
                "Cash Back Report": {
                    "columns": ["Order ID", "Order Item ID", "Taxable Value", "Invoice Amount"],
                    "rows": [["O-10001", "OI-10001", 1499, 1574]],
                },
            },
        },
        # Replenishment
        "repl_sales": {
            "kind": "csv",
            "filename": "replenishment_sales_demo.csv",
            "columns": [
                "FC CODE",
                "Shipment To Postal Code",
                "ASIN",
                "Customer Shipment Date",
                "Quantity",
                "Amazon Order ID",
                "Product Amount",
                "Shipping Amount",
                "Gift Amount",
            ],
            "rows": [["DEL4", "560001", "B0DEMOASIN1", f"{day_ymd}T10:10:00+05:30", 2, "AMZ-ORD-1001", 1499, 40, 0]],
        },
        "repl_stock": {
            "kind": "xlsx",
            "filename": "replenishment_stock_demo.xlsx",
            "columns": [
                "ASIN",
                "Location",
                "Disposition",
                "Ending Warehouse Balance",
                "In Transit Between Warehouses",
            ],
            "rows": [["B0DEMOASIN1", "DEL4", "sellable", 150, 12]],
        },
        "repl_lis": {
            "kind": "xlsx",
            "filename": "replenishment_lis_demo.xlsx",
            "columns": ["ASIN", "Cluster", "Sum of Local Shipped Units", "Sum of Total Units"],
            "rows": [["B0DEMOASIN1", "BANGALORE", 18, 30]],
        },
        "repl_shipment": {
            "kind": "xlsx",
            "filename": "replenishment_shipment_demo.xlsx",
            "columns": [
                "ASIN",
                "CLUSTER",
                "FC",
                "STATUS",
                "FINAL QTY",
                "ID",
                "APPOINTMENT DATE",
                "LOADING DATE",
            ],
            "rows": [["B0DEMOASIN1", "BANGALORE", "DEL4", "Upcoming", 45, "SHP-1001", day_ymd, day_ymd]],
        },
        "repl_assortment": {
            "kind": "xlsx",
            "filename": "replenishment_assortment_demo.xlsx",
            "columns": [
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
                "Brand",
            ],
            "rows": [["B0DEMOASIN1", "SKU-DEMO-1", "392490", "Demo Vendor", "Active", 0.75, 1.10, "Storage", "Medium", "Home", "Bins", "Plantex"]],
        },
        "repl_fc_cluster": {
            "kind": "xlsx",
            "filename": "replenishment_fc_cluster_demo.xlsx",
            "columns": ["FC CODE", "FC TYPE", "CLUSTER NAME", "ZONE"],
            "rows": [["DEL4", "AMAZON", "DELHI", "North"]],
        },
        "repl_pincode_cluster": {
            "kind": "csv",
            "filename": "replenishment_pincode_cluster_demo.csv",
            "columns": ["PIN CODE", "Fulfilment Cluster", "IDEAL CLUSTER", "ZONE"],
            "rows": [["560001", "BANGALORE", "BLR_CLUSTER", "South"]],
        },
        "repl_input_sheet": {
            "kind": "xlsx",
            "filename": "replenishment_input_sheet_demo.xlsx",
            "columns": ["Particular", "Value"],
            "rows": [
                ["P0 Demand DOC", "15 Days"],
                ["P1 Demand DOC", "30 Days"],
                ["P2 Demand DOC", "60 Days"],
                ["Sale Report Days", "7 Days"],
                ["Stock Report Date", day_ymd],
            ],
        },
        "repl_business_report": {
            "kind": "csv",
            "filename": "replenishment_business_report_demo.csv",
            "columns": [
                "(Child) ASIN",
                "Page Views - Total",
                "Units Ordered",
                "Ordered Product Sales",
                "Total Order Items",
            ],
            "rows": [["B0DEMOASIN1", 180, 14, "₹19,999.00", 13]],
        },
        "repl_flex_qty": {
            "kind": "csv",
            "filename": "replenishment_flex_qty_demo.csv",
            "columns": ["ASIN", "Cluster", "Qty"],
            "rows": [["B0DEMOASIN1", "BANGALORE", 20]],
        },
    }


def _table_to_dataframe(columns, rows):
    return pd.DataFrame(rows, columns=columns)


def download_demo_template(request):
    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")

    template_key = (request.GET.get("template") or "").strip()
    specs = _demo_specs(datetime.date.today())
    spec = specs.get(template_key)
    if not spec:
        return JsonResponse(
            {"error": "Invalid template key. Please provide a valid template."},
            status=400,
        )

    kind = spec["kind"]
    filename = spec["filename"]

    if kind in {"csv", "csv_with_metadata"}:
        output = StringIO()
        if kind == "csv_with_metadata":
            for row in spec.get("metadata_rows", []):
                output.write(",".join(str(v) for v in row) + "\n")
        writer = csv.writer(output)
        writer.writerow(spec["columns"])
        writer.writerows(spec["rows"])
        data = output.getvalue().encode("utf-8")
        buf = BytesIO(data)
        response = FileResponse(buf, content_type="text/csv")
    elif kind == "xlsx":
        buf = BytesIO()
        df = _table_to_dataframe(spec["columns"], spec["rows"])
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="Sheet1")
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    elif kind == "xlsx_multi":
        buf = BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as writer:
            for sheet_name, sheet in spec["sheets"].items():
                df = _table_to_dataframe(sheet["columns"], sheet["rows"])
                df.to_excel(writer, index=False, sheet_name=sheet_name)
        buf.seek(0)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    else:
        return JsonResponse({"error": "Unsupported template type."}, status=400)

    response["Content-Disposition"] = f'attachment; filename="{filename}"'
    return response


def download_calculated_data(request, file_format):
    """Download the calculated/merged dashboard data as CSV or Excel.

    Uses the same filters currently applied on the dashboard.
    The export mirrors the logic from scripts/cleaning_mapping_merging.py.
    """
    from apps.dashboard.services.export_services import export_csv, export_excel
    from datetime import datetime

    user = get_logged_in_user(request)
    if not user:
        return redirect("account-login")

    # Collect filters from query params (same as dashboard views)
    filters = {}
    for k in request.GET.keys():
        vals = request.GET.getlist(k)
        if len(vals) == 1:
            filters[k] = vals[0]
        else:
            filters[k] = vals

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if file_format == "csv":
        buf = export_csv(user, filters)
        response = FileResponse(buf, content_type="text/csv")
        response["Content-Disposition"] = (
            f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.csv"'
        )
        return response
    elif file_format == "excel":
        buf = export_excel(user, filters)
        response = FileResponse(
            buf,
            content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
        response["Content-Disposition"] = (
            f'attachment; filename="Calculated_Dashboard_Data_{timestamp}.xlsx"'
        )
        return response
    else:
        from django.http import JsonResponse

        return JsonResponse(
            {"error": "Invalid format. Use 'csv' or 'excel'."}, status=400
        )
