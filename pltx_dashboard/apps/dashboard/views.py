from django.shortcuts import render, redirect
from django.http import FileResponse
from apps.dashboard.models import (
    SpendData,
    ProcessedDashboardData,
    FlipkartProcessedDashboardData,
)
import json
from apps.accounts.decorators import require_feature, _first_allowed_dashboard_for
from apps.accounts.models import Feature
from apps.dashboard.utils import DashboardEncoder


from apps.accounts.utils import get_logged_in_user


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
    cache_key = f"dashboard_payload_{data_owner.id}_{data_version}_{cache_hash}"
    
    # Bypass cache temporarily to ensure new data structures (like inventory.details) are populated
    payload = None # cache.get(cache_key)
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


    return {
        "logged_user": user,
        "user_features": user_features,
        "payload": payload,
        "payload_json": json.dumps(payload, cls=DashboardEncoder),
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

    if user.is_main_user:
        user_features = [f.code_name for f in Feature.objects.all()]
    else:
        user_features = (
            [f.code_name for f in user.role.features.all()] if user.role else []
        )

    return render(
        request,
        "dashboard/upload.html",
        {
            "logged_user": user,
            "user_features": user_features,
            "payload_json": "null",
            "selected_filters_json": "{}",
        },
    )


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
